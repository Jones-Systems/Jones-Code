// @effect-diagnostics nodeBuiltinImport:off - CLI integration exercises Node HTTP and filesystem boundaries.
import * as NodeHttp from "node:http";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { ExecutionEnvironmentDescriptor } from "@t3tools/contracts";
import * as NetService from "@t3tools/shared/Net";
import { HostProcessEnvironment, HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { assert, describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Layer from "effect/Layer";
import * as Fiber from "effect/Fiber";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcessSpawner } from "effect/unstable/process";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";
import * as TestConsole from "effect/testing/TestConsole";
import { Command } from "effect/unstable/cli";

import { cli } from "../binCli.ts";
import {
  SERVICE_LAUNCHER_CONTEXT_ENV,
  SERVICE_LAUNCHER_PROTOCOL,
} from "../cloud/serviceProtocol.ts";
import * as ServiceLauncherClient from "../cloud/serviceLauncherClient.ts";
import {
  makePersistedServerRuntimeState,
  persistServerRuntimeState,
  type PersistedServerRuntimeState,
} from "../serverRuntimeState.ts";
import {
  DevServerNotProxiableError,
  resolveDirectPairingBaseUrl,
  resolveTailscaleLocalTarget,
  resolveTailscalePairingBase,
} from "./pair.ts";

import packageJson from "../../package.json" with { type: "json" };

const CliRuntimeLayer = Layer.mergeAll(NodeServices.layer, NetService.layer);

const baseState = {
  version: 1,
  pid: 123,
  port: 3_773,
  origin: "http://127.0.0.1:3773",
  startedAt: "2026-06-20T00:00:00.000Z",
} as const satisfies PersistedServerRuntimeState;

describe("pair base URL selection", () => {
  it("pairs through the dev web origin when the server fronts a dev server", () => {
    expect(resolveDirectPairingBaseUrl({ ...baseState, devUrl: "http://localhost:5733/" })).toBe(
      "http://localhost:5733/",
    );
  });

  it("pairs through the bound host when there is no dev server", () => {
    expect(resolveDirectPairingBaseUrl({ ...baseState, host: "100.64.0.7" })).toBe(
      "http://100.64.0.7:3773",
    );
    expect(resolveDirectPairingBaseUrl(baseState)).toBe("http://localhost:3773");
  });
});

describe("pair tailscale local target", () => {
  it("proxies the dev web port for dev servers", () => {
    expect(resolveTailscaleLocalTarget({ ...baseState, devUrl: "http://localhost:5733/" })).toEqual(
      { localPort: 5_733 },
    );
    // A dev server on a non-loopback interface must be proxied at that
    // interface; tailscale serve defaults to 127.0.0.1 otherwise.
    expect(
      resolveTailscaleLocalTarget({ ...baseState, devUrl: "http://192.168.1.10:5733/" }),
    ).toEqual({ localPort: 5_733, localHost: "192.168.1.10" });
    // URL.hostname keeps IPv6 brackets, so the serve target stays valid.
    expect(
      resolveTailscaleLocalTarget({ ...baseState, devUrl: "http://[fd7a:115c::1]:5733/" }),
    ).toEqual({ localPort: 5_733, localHost: "[fd7a:115c::1]" });
  });

  it("rejects HTTPS dev URLs, which tailscale serve cannot proxy", () => {
    expect(
      resolveTailscaleLocalTarget({ ...baseState, devUrl: "https://localhost:5733/" }),
    ).toBeInstanceOf(DevServerNotProxiableError);
  });

  it("proxies the backend port directly otherwise", () => {
    expect(resolveTailscaleLocalTarget(baseState)).toEqual({ localPort: 3_773 });
    expect(resolveTailscaleLocalTarget({ ...baseState, host: "0.0.0.0" })).toEqual({
      localPort: 3_773,
    });
    expect(resolveTailscaleLocalTarget({ ...baseState, host: "192.168.1.42" })).toEqual({
      localPort: 3_773,
      localHost: "192.168.1.42",
    });
  });
});

const runCli = (args: ReadonlyArray<string>) => Command.runWith(cli, { version: "0.0.0" })(args);

const provideCliTestLayers = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.provide(effect, Layer.mergeAll(CliRuntimeLayer, TestConsole.layer));

// Console output accumulates across CLI runs within a test, and each
// Console.log call is one entry — so the latest command's output is the last
// entry, even when it spans many lines.
const captureStdout = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  provideCliTestLayers(
    Effect.gen(function* () {
      yield* effect;
      return (
        (yield* TestConsole.logLines).findLast(
          (line): line is string => typeof line === "string",
        ) ?? ""
      );
    }),
  );

const testDescriptor = {
  environmentId: "pair-test-environment",
  label: "pair-test",
  platform: { os: "linux", arch: "x64" },
  serverVersion: "0.0.1",
  capabilities: { repositoryIdentity: true },
};

const withDescriptorServer = <A, E, R>(run: (origin: string) => Effect.Effect<A, E, R>) =>
  Effect.acquireUseRelease(
    Effect.callback<NodeHttp.Server>((resume) => {
      const server = NodeHttp.createServer((request, response) => {
        if (request.url === "/.well-known/t3/environment") {
          response.writeHead(200, { "content-type": "application/json" });
          response.end(JSON.stringify(testDescriptor));
          return;
        }
        response.writeHead(404);
        response.end();
      });
      server.listen(0, "127.0.0.1", () => resume(Effect.succeed(server)));
    }),
    (server) => {
      const address = server.address();
      if (address === null || typeof address === "string") {
        return Effect.die(new Error("Expected a TCP address"));
      }
      return run(`http://127.0.0.1:${String(address.port)}`);
    },
    (server) => Effect.sync(() => server.close()),
  );

describe("t3 pair", () => {
  it.effect("mints a token and prints a QR pairing URL for a live server", () =>
    withDescriptorServer((origin) =>
      Effect.gen(function* () {
        const baseDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-pair-test-"));
        const port = Number(new URL(origin).port);
        const statePath = NodePath.join(baseDir, "userdata", "server-runtime.json");
        yield* persistServerRuntimeState({
          path: statePath,
          state: yield* makePersistedServerRuntimeState({
            config: { host: "127.0.0.1", devUrl: undefined },
            port,
          }),
        });

        const output = yield* captureStdout(runCli(["pair", "--base-dir", baseDir]));

        assert.include(output, `Pairing with pair-test (${origin})`);
        assert.include(output, `Pairing URL: ${origin}/pair#token=`);
        assert.isTrue(output.includes("█") || output.includes("▀") || output.includes("▄"));
        // Loopback origins are not reachable from a phone; the output must say so.
        assert.include(output, "only reachable from this machine");

        const token = /#token=([A-Z2-9]+)/.exec(output)?.[1];
        assert.isString(token);

        // The token must be in the same store the running server reads.
        const listed = yield* captureStdout(
          runCli(["auth", "pairing", "list", "--base-dir", baseDir, "--json"]),
        );
        // @effect-diagnostics-next-line preferSchemaOverJson:off - CLI JSON output is decoded as a presentation DTO.
        const credentials = JSON.parse(listed) as ReadonlyArray<{ readonly label?: string }>;
        assert.equal(credentials.length, 1);
        assert.equal(credentials[0]?.label, "t3 pair");
      }),
    ).pipe(
      Effect.provide(NodeServices.layer),
      Effect.provideService(HostProcessEnvironment, {
        ...process.env,
        [SERVICE_LAUNCHER_CONTEXT_ENV]: JSON.stringify({
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          childVersion: packageJson.version,
        }),
      }),
      Effect.provideService(ServiceLauncherClient.ServiceLauncherHostProcess, {
        connected: false,
        send: () => false,
        on: () => undefined,
        off: () => undefined,
      }),
    ),
  );

  it.effect("pairs through the recorded dev web URL for dev servers", () =>
    withDescriptorServer((origin) =>
      Effect.gen(function* () {
        const baseDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-pair-dev-test-"));
        const port = Number(new URL(origin).port);
        const statePath = NodePath.join(baseDir, "dev", "server-runtime.json");
        yield* persistServerRuntimeState({
          path: statePath,
          state: yield* makePersistedServerRuntimeState({
            config: { host: undefined, devUrl: new URL("http://localhost:5733") },
            port,
          }),
        });

        const output = yield* captureStdout(runCli(["pair", "--base-dir", baseDir]));

        assert.include(output, "Pairing URL: http://localhost:5733/pair#token=");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("directs to t3 serve or t3 connect when no server is running", () =>
    Effect.gen(function* () {
      const baseDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-pair-none-test-"));

      const error = yield* provideCliTestLayers(
        runCli(["pair", "--base-dir", baseDir]).pipe(Effect.flip),
      );

      const rendered = String(
        typeof error === "object" && error !== null && "cause" in error ? error.cause : error,
      );
      assert.include(rendered, "No running T3 Code server found.");
      assert.include(rendered, "npx t3 serve");
      assert.include(rendered, "npx t3 connect");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("ignores runtime state whose recorded pid is no longer alive", () =>
    withDescriptorServer((origin) =>
      Effect.gen(function* () {
        const baseDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-pair-pid-test-"));
        const statePath = NodePath.join(baseDir, "userdata", "server-runtime.json");
        // The origin answers (another server reused the port), but the pid
        // that wrote this state file is dead — pairing must not mint a token
        // into the dead server's database.
        const state = yield* makePersistedServerRuntimeState({
          config: { host: "127.0.0.1", devUrl: undefined },
          port: Number(new URL(origin).port),
        });
        yield* persistServerRuntimeState({
          path: statePath,
          // pid 2**22 + 1 exceeds any default Linux/macOS pid range.
          state: { ...state, pid: 4_194_305 },
        });

        const error = yield* provideCliTestLayers(
          runCli(["pair", "--base-dir", baseDir]).pipe(Effect.flip),
        );

        const rendered = String(
          typeof error === "object" && error !== null && "cause" in error ? error.cause : error,
        );
        assert.include(rendered, "No running T3 Code server found.");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("ignores stale runtime state pointing at a dead server", () =>
    Effect.gen(function* () {
      const baseDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-pair-stale-test-"));
      const statePath = NodePath.join(baseDir, "userdata", "server-runtime.json");
      // A port from the dynamic range with nothing listening: the probe fails
      // fast with ECONNREFUSED and discovery moves on.
      yield* persistServerRuntimeState({
        path: statePath,
        state: yield* makePersistedServerRuntimeState({
          config: { host: "127.0.0.1", devUrl: undefined },
          port: 1,
        }),
      });

      const error = yield* provideCliTestLayers(
        runCli(["pair", "--base-dir", baseDir]).pipe(Effect.flip),
      );

      const rendered = String(
        typeof error === "object" && error !== null && "cause" in error ? error.cause : error,
      );
      assert.include(rendered, "No running T3 Code server found.");
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});

const guardedPairClaim = { servePort: 8443, localPort: baseState.port };
const guardedServeConfig = (
  localPort: number = guardedPairClaim.localPort,
  extraHandlers = false,
) =>
  JSON.stringify({
    TCP: { "8443": { HTTPS: true } },
    Web: {
      "pair.tail.ts.net:8443": {
        Handlers: {
          "/": { Proxy: `http://127.0.0.1:${String(localPort)}` },
          ...(extraHandlers ? { "/api": { Proxy: "http://127.0.0.1:1" } } : {}),
        },
      },
    },
  });
const pairEncoder = new TextEncoder();
const pairStatusArgs = ["serve", "status", "--json"];

function guardedPairLayers(input: {
  readonly serveReads: ReadonlyArray<string>;
  readonly probe: "same" | "different" | "502";
  readonly writeTimeout?: boolean;
}) {
  const commands: Array<ReadonlyArray<string>> = [];
  let readIndex = 0;
  const spawner = ChildProcessSpawner.make((command) => {
    const child = command as unknown as { readonly args: ReadonlyArray<string> };
    commands.push(child.args);
    const isStatus = child.args[0] === "status";
    const isServeRead = child.args[1] === "status";
    const stdout = isStatus
      ? '{"Self":{"DNSName":"pair.tail.ts.net."}}'
      : isServeRead
        ? input.serveReads[readIndex++]
        : "";
    if (stdout === undefined) return Effect.die(new Error("Unexpected Serve read"));
    return Effect.succeed(
      ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(1),
        exitCode:
          input.writeTimeout && child.args.includes("--bg")
            ? Effect.never
            : Effect.succeed(ChildProcessSpawner.ExitCode(0)),
        isRunning: Effect.succeed(false),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.drain,
        stdout: Stream.make(pairEncoder.encode(stdout)),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      }),
    );
  });
  let probes = 0;
  const client = HttpClient.make((request) => {
    probes += 1;
    const response =
      input.probe === "502" && probes === 1
        ? new Response("", { status: 502 })
        : new Response(
            JSON.stringify({
              ...testDescriptor,
              environmentId:
                input.probe === "different" ? "another-environment" : testDescriptor.environmentId,
            }),
            { headers: { "content-type": "application/json" } },
          );
    return Effect.succeed(HttpClientResponse.fromWeb(request, response));
  });
  return {
    commands,
    layer: Layer.mergeAll(
      Layer.succeed(HostProcessPlatform, "linux"),
      Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Layer.succeed(HttpClient.HttpClient, client),
    ),
  };
}

const decodePairDescriptor = Schema.decodeUnknownSync(ExecutionEnvironmentDescriptor);

const guardedPairTarget = (dev = false) => ({
  baseDir: "/synthetic-pair-base",
  variant: dev ? ("dev" as const) : ("userdata" as const),
  state: dev ? { ...baseState, devUrl: "http://localhost:5733/" } : baseState,
  descriptor: decodePairDescriptor(testDescriptor),
});

describe("guarded Tailscale pairing", () => {
  it.effect("refuses a conflicting route even when it returns 502", () => {
    const { commands, layer } = guardedPairLayers({
      serveReads: [guardedServeConfig(9999)],
      probe: "502",
    });
    return Effect.gen(function* () {
      const error = yield* resolveTailscalePairingBase({
        target: guardedPairTarget(),
        servePort: 8443,
      }).pipe(Effect.flip);
      assert.equal(error._tag, "ServePortClaimedError");
      assert.deepEqual(commands, [["status", "--json"], pairStatusArgs]);
    }).pipe(Effect.provide(layer));
  });

  it.effect(
    "writes an absent mapping and requires exact readback before returning a pairing URL",
    () => {
      const { commands, layer } = guardedPairLayers({
        serveReads: ["{}", guardedServeConfig()],
        probe: "502",
      });
      return Effect.gen(function* () {
        const resolved = yield* resolveTailscalePairingBase({
          target: guardedPairTarget(),
          servePort: 8443,
        });
        assert.equal(resolved.baseUrl, "https://pair.tail.ts.net:8443/");
        assert.deepEqual(commands, [
          ["status", "--json"],
          pairStatusArgs,
          ["serve", "--bg", "--https=8443", "http://127.0.0.1:3773"],
          pairStatusArgs,
        ]);
      }).pipe(Effect.provide(layer));
    },
  );

  it.effect("reuses an exact route without writing", () => {
    const { commands, layer } = guardedPairLayers({
      serveReads: [guardedServeConfig()],
      probe: "same",
    });
    return Effect.gen(function* () {
      yield* resolveTailscalePairingBase({ target: guardedPairTarget(), servePort: 8443 });
      assert.deepEqual(commands, [["status", "--json"], pairStatusArgs]);
    }).pipe(Effect.provide(layer));
  });

  it.effect("stops on unknown configuration without writing", () => {
    const { commands, layer } = guardedPairLayers({ serveReads: [""], probe: "same" });
    return Effect.gen(function* () {
      const error = yield* resolveTailscalePairingBase({
        target: guardedPairTarget(),
        servePort: 8443,
      }).pipe(Effect.flip);
      assert.equal(error._tag, "TailscaleServeStateUnknownError");
      assert.deepEqual(commands, [["status", "--json"], pairStatusArgs]);
    }).pipe(Effect.provide(layer));
  });

  it.effect(
    "returns unknown after mismatching postwrite readback and never removes the route",
    () => {
      const { commands, layer } = guardedPairLayers({
        serveReads: ["{}", guardedServeConfig(9999)],
        probe: "502",
      });
      return Effect.gen(function* () {
        const error = yield* resolveTailscalePairingBase({
          target: guardedPairTarget(),
          servePort: 8443,
        }).pipe(Effect.flip);
        assert.equal(error._tag, "TailscaleServeStateUnknownError");
        assert.equal(commands.length, 4);
        assert.isFalse(commands.some((args) => args.includes("off")));
      }).pipe(Effect.provide(layer));
    },
  );

  it.effect("returns unknown on ensure timeout without readback, cleanup or retry", () => {
    const { commands, layer } = guardedPairLayers({
      serveReads: ["{}"],
      probe: "502",
      writeTimeout: true,
    });
    return Effect.gen(function* () {
      const fiber = yield* resolveTailscalePairingBase({
        target: guardedPairTarget(),
        servePort: 8443,
      }).pipe(Effect.flip, Effect.forkChild);
      yield* TestClock.adjust("10 seconds");
      assert.equal((yield* Fiber.join(fiber))._tag, "TailscaleServeStateUnknownError");
      assert.equal(commands.length, 3);
      assert.isFalse(commands.some((args) => args.includes("off")));
    }).pipe(Effect.provide(layer));
  });

  it.effect("repoints only a dev target proved to be the same environment", () => {
    const { commands, layer } = guardedPairLayers({
      serveReads: [guardedServeConfig(), guardedServeConfig(5733)],
      probe: "same",
    });
    return Effect.gen(function* () {
      yield* resolveTailscalePairingBase({ target: guardedPairTarget(true), servePort: 8443 });
      assert.deepEqual(commands, [
        ["status", "--json"],
        pairStatusArgs,
        ["serve", "--bg", "--https=8443", "http://127.0.0.1:5733"],
        pairStatusArgs,
      ]);
    }).pipe(Effect.provide(layer));
  });

  it.effect.each([
    { label: "a different environment", probe: "different", extraHandlers: false },
    {
      label: "a route with extra handlers despite the same environment",
      probe: "same",
      extraHandlers: true,
    },
  ] as const)("does not repoint $label", ({ probe, extraHandlers }) => {
    const { commands, layer } = guardedPairLayers({
      serveReads: [guardedServeConfig(3773, extraHandlers)],
      probe,
    });
    return Effect.gen(function* () {
      const error = yield* resolveTailscalePairingBase({
        target: guardedPairTarget(true),
        servePort: 8443,
      }).pipe(Effect.flip);
      assert.equal(error._tag, "ServePortClaimedError");
      assert.deepEqual(commands, [["status", "--json"], pairStatusArgs]);
    }).pipe(Effect.provide(layer));
  });
});
