import * as NodeCrypto from "node:crypto";
import { expect, it } from "@effect/vitest";
import {
  HostProcessArchitecture,
  HostProcessPlatform,
  HostProcessUserId,
} from "@t3tools/shared/hostProcess";
import * as Net from "@t3tools/shared/Net";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";
import { ChildProcessSpawner } from "effect/unstable/process";
import { afterEach, vi } from "vite-plus/test";

import packageJson from "../../../package.json" with { type: "json" };
import * as BootService from "../../cloud/bootService.ts";
import * as ProcessRunner from "../../processRunner.ts";
import * as HostServiceConfig from "./HostServiceConfig.ts";
import * as HostService from "./HostService.ts";

afterEach(() => vi.restoreAllMocks());
const baseDir = "/task-owned/jones-host";
const claim = { schema: 1, port: 4321, tailscaleServePort: 8443 } as const;
const configJsonText = Schema.encodeSync(
  Schema.fromJsonString(HostServiceConfig.HostServiceConfigData),
);
const encodeUnknownJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const exact = JSON.stringify({
  TCP: { "8443": { HTTPS: true } },
  Web: { "host:8443": { Handlers: { "/": { Proxy: "http://127.0.0.1:4321" } } } },
});
const encode = (text: string) => new TextEncoder().encode(text);
const output = (stdout: string): ProcessRunner.ProcessRunOutput => ({
  stdout,
  stderr: "",
  code: ChildProcessSpawner.ExitCode(0),
  timedOut: false,
  stdoutTruncated: false,
  stderrTruncated: false,
  stdoutInvalidUtf8: false,
  stderrInvalidUtf8: false,
});

function fixture(
  options: {
    readonly serveReads?: ReadonlyArray<string>;
    readonly offFails?: boolean;
    readonly busy?: boolean;
    readonly linger?: string;
    readonly platform?: "linux" | "darwin";
    readonly foreignBase?: string;
    readonly foreground?: boolean;
    readonly unverified?: boolean;
    readonly initiallyInstalled?: boolean;
  } = {},
) {
  const files = new Map<string, string>();
  const commands: string[][] = [];
  const writes: string[] = [];
  const installs: Array<{ readonly start?: boolean }> = [];
  let persisted: HostServiceConfig.HostServiceConfigData = claim;
  files.set(`${baseDir}/jones/host-service.json`, JSON.stringify(claim));
  let installed = options.initiallyInstalled ?? true;
  let serveRead = 0;
  const platform = options.platform ?? "linux";
  const entry = "synthetic Jones executable";
  const versionDir = `${baseDir}/runtime/versions/${packageJson.version}`;
  files.set(`${versionDir}/t3`, entry);
  files.set(`${versionDir}/.install-complete`, packageJson.version);
  if (!options.unverified)
    files.set(
      `${versionDir}/.jones-provenance.json`,
      JSON.stringify({
        schema: 1,
        repository: "Jones-Systems/Jones-Code",
        source: "a".repeat(40),
        version: packageJson.version,
        platform,
        architecture: platform === "darwin" ? "arm64" : "x64",
        artifact: `t3-${packageJson.version}-${platform}-${platform === "darwin" ? "arm64" : "x64"}.tar.gz`,
        sha256: "b".repeat(64),
        entrySha256: NodeCrypto.createHash("sha256").update(entry).digest("hex"),
      }),
    );
  if (options.foreground)
    files.set(
      `${baseDir}/userdata/server-runtime.json`,
      JSON.stringify({
        version: 1,
        pid: process.pid,
        port: 4321,
        host: "127.0.0.1",
        origin: "http://127.0.0.1:4321",
        startedAt: "2026-10-07T00:00:00Z",
        serviceManaged: false,
      }),
    );
  const read = (name: string) =>
    files.has(name)
      ? Effect.succeed(files.get(name)!)
      : Effect.fail(
          PlatformError.systemError({
            module: "FileSystem",
            method: "readFileString",
            _tag: "NotFound",
            pathOrDescriptor: name,
          }),
        );
  const fs = FileSystem.makeNoop({
    readFileString: read,
    readFile: (name) => read(name).pipe(Effect.map(encode)),
    exists: (name) => Effect.succeed(files.has(name)),
    writeFileString: (name, text) =>
      Effect.sync(() => {
        writes.push(name);
        files.set(name, text);
      }),
  });
  const configLayer = Layer.succeed(
    HostServiceConfig.HostServiceConfig,
    HostServiceConfig.HostServiceConfig.of({
      read: () => Effect.succeed(Option.some(persisted)),
      write: (_base, data) =>
        Effect.sync(() => {
          writes.push("host-service.json");
          persisted = data;
          files.set(`${baseDir}/jones/host-service.json`, configJsonText(data));
        }),
    }),
  );
  const boot = BootService.BootService.of({
    status: Effect.sync(() => ({
      supported: true,
      installed,
      current: installed,
      ...(installed
        ? {
            installedVersion: packageJson.version,
            installedBaseDir: options.foreignBase ?? baseDir,
          }
        : {}),
      unitPath: "/task-owned/user/jones-code.service",
      logPath: `${baseDir}/userdata/logs/boot-service.log`,
    })),
    install: (input) =>
      Effect.sync(() => {
        installed = true;
        installs.push(input ?? {});
        return {
          baseDir,
          unitPath: "/task-owned/user/jones-code.service",
          logPath: `${baseDir}/userdata/logs/boot-service.log`,
          program: [`${versionDir}/t3`, "__service-launcher"],
        };
      }),
    restart: Effect.succeed(false),
    uninstall: Effect.succeed(false),
  });
  vi.spyOn(BootService, "layer").mockReturnValue(Layer.succeed(BootService.BootService, boot));
  const spawner = ChildProcessSpawner.make((command) => {
    const args = [...(command as unknown as { args: ReadonlyArray<string> }).args];
    commands.push(args);
    const statusRead = args.join(" ") === "serve status --json";
    const off = args.includes("off");
    const stdout = statusRead
      ? (options.serveReads?.[serveRead++] ?? "{}")
      : '{"Self":{"DNSName":"host.tail.ts.net."}}';
    return Effect.succeed(
      ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(1),
        exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(off && options.offFails ? 1 : 0)),
        isRunning: Effect.succeed(false),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.drain,
        stdout: Stream.make(encode(stdout)),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      }),
    );
  });
  const dependencies = Layer.mergeAll(
    configLayer,
    Layer.succeed(FileSystem.FileSystem, fs),
    Path.layer,
    Layer.succeed(HostProcessPlatform, platform),
    Layer.succeed(HostProcessArchitecture, platform === "darwin" ? "arm64" : "x64"),
    Layer.succeed(HostProcessUserId, 1000),
    Layer.succeed(
      Net.NetService,
      Net.NetService.of({
        isPortAvailableOnLoopback: () => Effect.succeed(!options.busy),
        canListenOnHost: () => Effect.succeed(!options.busy),
        hasListenerOnHost: () => Effect.succeed(options.busy === true),
        reserveLoopbackPort: () => Effect.succeed(4321),
        findAvailablePort: () => Effect.succeed(4321),
      }),
    ),
    Layer.succeed(
      ProcessRunner.ProcessRunner,
      ProcessRunner.ProcessRunner.of({
        run: (input) =>
          Effect.sync(() => {
            commands.push([input.command, ...input.args]);
            return output(
              input.command === "loginctl"
                ? (options.linger ?? "yes")
                : input.command === "stat"
                  ? "root"
                  : input.command === "pmset"
                    ? "AC Power:\n sleep 0\n disksleep 10\n"
                    : input.command === "defaults"
                      ? "synthetic-user"
                      : "state = running\npid = 42",
            );
          }),
      }),
    ),
    Layer.succeed(
      HttpClient.HttpClient,
      HttpClient.make((request) =>
        Effect.succeed(HttpClientResponse.fromWeb(request, new Response("{}", { status: 503 }))),
      ),
    ),
    Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
    ConfigProvider.layer(ConfigProvider.fromEnv({ env: { HOME: "/task-owned/user" } })),
  );
  return {
    files,
    commands,
    writes,
    installs,
    layer: HostService.layer.pipe(Layer.provide(dependencies)),
  };
}

it.effect("dry-run produces a bounded effect plan with no writes or process calls", () =>
  Effect.gen(function* () {
    const f = fixture();
    const result = yield* Effect.flatMap(HostService.HostService, (service) =>
      service.setup({ baseDir, port: 4321, dryRun: true }),
    ).pipe(Effect.provide(f.layer));
    expect(result.state).toBe("dry-run");
    expect(result.plan.nextStep).toContain("--tailscale-serve-port 8443");
    expect(f.writes).toEqual([]);
    expect(f.commands).toEqual([]);
  }),
);

it.effect.each([
  [{ foreignBase: "/another-base" }, "different or unknown base"],
  [{ foreground: true }, "not owned"],
  [{ busy: true }, "busy"],
  [{ linger: "no" }, "lingering is disabled"],
  [{ unverified: true }, "Operation failed"],
  [{ serveReads: [exact.replace("4321", "9000")] }, "conflicting mapping"],
] as const)("setup refuses preflight %j without config or unit writes", ([options, reason]) =>
  Effect.gen(function* () {
    const f = fixture(options);
    const error = yield* Effect.flatMap(HostService.HostService, (service) =>
      service.setup({ baseDir, port: 4321 }),
    ).pipe(Effect.provide(f.layer), Effect.flip);
    expect(error.message).toContain(reason);
    expect(f.writes).toEqual([]);
    expect(f.installs).toEqual([]);
    expect(f.commands.some((args) => args.includes("off") || args.includes("--bg"))).toBe(false);
  }),
);

it.effect("status is read-only and reports failed probes as unknown", () =>
  Effect.gen(function* () {
    const f = fixture({ unverified: true, serveReads: ["{broken"] });
    const result = yield* Effect.flatMap(HostService.HostService, (service) =>
      service.status({ baseDir }),
    ).pipe(Effect.provide(f.layer));
    expect(result.provenance).toBe("unverified");
    expect(result.serve).toMatchObject({ _tag: "unknown", reason: "decode-failed" });
    expect(result.serveEndpoint).toMatchObject({
      environmentMatch: "unknown",
      scope: "host-side-only",
    });
    expect(result.foreignUpstreamUnit).toEqual({ present: false, managed: false });
    expect(f.writes).toEqual([]);
    expect(f.installs).toEqual([]);
  }),
);

it.effect(
  "setup preserves unknown Serve state as a warning and reads back a verified install",
  () =>
    Effect.gen(function* () {
      const f = fixture({ serveReads: ["{bad", "{}"], initiallyInstalled: false, linger: "no" });
      const result = yield* Effect.flatMap(HostService.HostService, (service) =>
        service.setup({ baseDir, port: 4321, allowLingerEnable: true }),
      ).pipe(Effect.provide(f.layer));
      expect(result.state).toBe("installed");
      expect(result.warnings).toHaveLength(1);
      expect(result.status?.provenance).toBe("verified");
      expect(f.installs).toEqual([{ start: true }]);
      expect(f.writes).toEqual(["host-service.json"]);
      expect(f.commands.some((args) => args.includes("off") || args.includes("--bg"))).toBe(false);
    }),
);

it.effect("Mac without a GUI login installs without bootstrap and reports awaiting login", () =>
  Effect.gen(function* () {
    const f = fixture({ platform: "darwin", initiallyInstalled: false });
    const result = yield* Effect.flatMap(HostService.HostService, (service) =>
      service.setup({ baseDir, port: 4321 }),
    ).pipe(Effect.provide(f.layer));
    expect(result.state).toBe("installed-awaiting-gui-login");
    expect(f.installs).toEqual([{ start: false }]);
    expect(result.status?.mac).toMatchObject({
      consoleUser: "gui-login-required",
      autoLogin: "yes",
      sleep: { "AC Power": 0 },
    });
    const statusJson = yield* encodeUnknownJson(result.status);
    expect(statusJson).not.toContain("synthetic-user");
    expect(f.commands.some((args) => args.includes("bootstrap"))).toBe(false);
  }),
);

it.effect("removes only the exact persisted claim and confirms absence", () =>
  Effect.gen(function* () {
    const f = fixture({ serveReads: [exact, exact, "{}"] });
    const result = yield* Effect.flatMap(HostService.HostService, (service) =>
      service.routeRemove({ baseDir }),
    ).pipe(Effect.provide(f.layer));
    expect(result.state).toBe("removed");
    expect(f.commands).toEqual([
      ["serve", "status", "--json"],
      ["serve", "status", "--json"],
      ["serve", "--https=8443", "off"],
      ["serve", "status", "--json"],
    ]);
  }),
);

it.effect.each([exact.replace("4321", "9000"), "{bad", exact.replaceAll("8443", "443")])(
  "route remove dry-run %s reads only",
  (config) =>
    Effect.gen(function* () {
      const f = fixture({ serveReads: [config] });
      const result = yield* Effect.flatMap(HostService.HostService, (service) =>
        service.routeRemove({ baseDir, dryRun: true }),
      ).pipe(Effect.provide(f.layer));
      expect(result.state).toBe("dry-run");
      expect(f.commands).toEqual([["serve", "status", "--json"]]);
      expect(f.writes).toEqual([]);
    }),
);

it.effect.each([exact.replace("4321", "9000"), "{bad"])(
  "route remove refuses %s without off",
  (config) =>
    Effect.gen(function* () {
      const f = fixture({ serveReads: [config] });
      yield* Effect.flatMap(HostService.HostService, (service) =>
        service.routeRemove({ baseDir }),
      ).pipe(Effect.provide(f.layer), Effect.flip);
      expect(f.commands).toEqual([["serve", "status", "--json"]]);
    }),
);

it.effect("failed off and unchanged readback report unknown without retry", () =>
  Effect.gen(function* () {
    const f = fixture({ serveReads: [exact, exact, exact], offFails: true });
    const error = yield* Effect.flatMap(HostService.HostService, (service) =>
      service.routeRemove({ baseDir }),
    ).pipe(Effect.provide(f.layer), Effect.flip);
    expect(error.message).toContain("Effect unknown");
    expect(f.commands.filter((args) => args.includes("off"))).toHaveLength(1);
    expect(f.commands.at(-1)).toEqual(["serve", "status", "--json"]);
  }),
);

it.effect("rejects relative bases and incomplete artifact options before setup writes", () =>
  Effect.gen(function* () {
    const f = fixture();
    for (const input of [
      { baseDir: "relative", port: 4321 },
      { baseDir, port: 4321, artifactDir: "/artifact" },
    ]) {
      yield* Effect.flatMap(HostService.HostService, (service) => service.setup(input)).pipe(
        Effect.provide(f.layer),
        Effect.flip,
      );
    }
    expect(f.commands).toEqual([]);
    expect(f.writes).toEqual([]);
  }),
);
