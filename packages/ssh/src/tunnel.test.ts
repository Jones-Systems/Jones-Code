import * as NodeNet from "node:net";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Context from "effect/Context";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NetService from "@t3tools/shared/Net";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as PlatformError from "effect/PlatformError";
import * as Result from "effect/Result";
import * as Scope from "effect/Scope";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";
import { HttpClient, HttpClientResponse } from "effect/http";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import * as SshAuth from "./auth.ts";
import { SshCommandError } from "./errors.ts";
import * as SshTunnel from "./tunnel.ts";

const TEST_NODE_ENGINE_RANGE = "^22.16 || ^23.11 || >=24.10";

const HOST_MANAGED = { hostManagedRuntime: true } as const;

const NODE_SCRIPT = {
  nodeScriptPath: "/Users/julius/Development/Work/codething-mvp/apps/server/dist/bin.mjs",
} as const;

const runScript = (home: string, args: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const child = yield* spawner.spawn(
      ChildProcess.make("sh", args, {
        env: { PATH: process.env.PATH ?? "", HOME: home },
        extendEnv: false,
      }),
    );
    const [stdout, stderr, exitCode] = yield* Effect.all(
      [
        child.stdout.pipe(
          Stream.decodeText(),
          Stream.runFold(
            () => "",
            (acc, chunk) => acc + chunk,
          ),
        ),
        child.stderr.pipe(
          Stream.decodeText(),
          Stream.runFold(
            () => "",
            (acc, chunk) => acc + chunk,
          ),
        ),
        child.exitCode.pipe(Effect.map(Number)),
      ],
      { concurrency: "unbounded" },
    );
    return { stdout, stderr, exitCode };
  });

const makeSuccessfulProcess = (stdout: string) => {
  const stdoutStream = Stream.make(new TextEncoder().encode(stdout));
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(123),
    stdout: stdoutStream,
    stderr: Stream.empty,
    all: stdoutStream,
    exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
    isRunning: Effect.succeed(false),
    kill: () => Effect.void,
    stdin: Sink.drain,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
    unref: Effect.succeed(Effect.void),
  });
};

const makeRunningProcess = (onKill: () => void) => {
  let finish: ((exitCode: ChildProcessSpawner.ExitCode) => void) | null = null;
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(123),
    stdout: Stream.empty,
    stderr: Stream.empty,
    all: Stream.empty,
    exitCode: Effect.callback<ChildProcessSpawner.ExitCode>((resume) => {
      finish = (exitCode) => resume(Effect.succeed(exitCode));
      return Effect.sync(() => {
        finish = null;
      });
    }),
    isRunning: Effect.succeed(true),
    kill: () =>
      Effect.sync(() => {
        onKill();
        finish?.(ChildProcessSpawner.ExitCode(143));
      }),
    stdin: Sink.drain,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
    unref: Effect.succeed(Effect.void),
  });
};

const testHttpClient = HttpClient.make((request) =>
  Effect.succeed(HttpClientResponse.fromWeb(request, new Response("", { status: 200 }))),
);

const hangingHttpClient = HttpClient.make(() => Effect.never);

const testNetService = NetService.NetService.of({
  canListenOnHost: () => Effect.succeed(true),
  isPortAvailableOnLoopback: () => Effect.succeed(true),
  hasListenerOnHost: () => Effect.succeed(false),
  reserveLoopbackPort: () => Effect.succeed(41_773),
  findAvailablePort: (preferred) => Effect.succeed(preferred),
});

function commandArgs(command: ChildProcess.Command): ReadonlyArray<string> {
  return command._tag === "StandardCommand" ? command.args : [];
}

const ARCHIVE = { archiveVersion: "1.2.3-preview.20260911.4" } as const;

describe("ssh tunnel scripts", () => {
  it("rejects archive versions that are not a single exact version segment", () => {
    for (const archiveVersion of [
      "../other",
      "1.2.3/evil",
      "1.2.3\\evil",
      "1.2.3-preview.1 x",
      "1.2.3-preview.1\nrm -rf /",
      "v1.2.3",
    ]) {
      assert.throws(
        () => SshTunnel.buildRemoteT3RunnerScript({ archiveVersion }),
        SshTunnel.SshInvalidArchiveVersionError,
        undefined,
        archiveVersion,
      );
    }
    assert.include(
      SshTunnel.buildRemoteT3RunnerScript(ARCHIVE),
      "T3_ARCHIVE_VERSION='1.2.3-preview.20260911.4'",
    );
  });

  it.effect("accepts launch JSON after remote shell startup noise", () => {
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;
    const spawnedCommands: Array<ReadonlyArray<string>> = [];
    const spawner = ChildProcessSpawner.make((command) =>
      Effect.sync(() => {
        spawnedCommands.push(commandArgs(command));
        return makeSuccessfulProcess('loaded nvm default\n{"remotePort":3774}\n');
      }),
    );
    const layerSpawner = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner);
    const layerProcess = Layer.merge(NodeServices.layer, layerSpawner);

    return Effect.gen(function* () {
      const result = yield* SshTunnel.launchOrReuseRemoteServer(target, undefined, ARCHIVE);
      assert.equal(result.remotePort, 3774);
      assert.deepEqual(spawnedCommands[0]?.slice(-5, -1), ["sh", "-l", "-s", "--"]);
    }).pipe(Effect.provide(layerProcess));
  });

  it.effect("accepts pretty-printed pairing JSON after remote shell startup noise", () => {
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;
    const spawner = ChildProcessSpawner.make(() =>
      Effect.succeed(
        makeSuccessfulProcess(`loaded nvm default
{
  "id": "88941235-6ed5-4184-a2ff-5339e2075958",
  "credential": "LCL4R2TPHDKQ",
  "scopes": ["orchestration:read"],
  "expiresAt": "2026-04-29T01:01:20.994Z"
}

`),
      ),
    );
    const layerSpawner = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner);
    const layerProcess = Layer.merge(NodeServices.layer, layerSpawner);
    return Effect.gen(function* () {
      const result = yield* SshTunnel.issueRemotePairingToken(target, undefined, ARCHIVE);
      assert.equal(result.credential, "LCL4R2TPHDKQ");
    }).pipe(Effect.provide(layerProcess));
  });

  it.effect("retains a bounded diagnostic tail when the tunnel exits before readiness", () =>
    Effect.gen(function* () {
      const diagnostic = "Connection refused: café";
      const output = new TextEncoder().encode(
        "old diagnostics\n" + "x".repeat(32_768) + "\n" + diagnostic,
      );
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.succeed(
          commandArgs(command).includes("-N")
            ? {
                ...makeSuccessfulProcess(""),
                exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
                stderr: Stream.make(output.subarray(0, output.length - 1), output.subarray(-1)),
              }
            : makeSuccessfulProcess('{"remotePort":3773}\n'),
        ),
      );
      const layer = Layer.mergeAll(
        NodeServices.layer,
        Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Layer.succeed(HttpClient.HttpClient, hangingHttpClient),
        Layer.succeed(NetService.NetService, testNetService),
        SshAuth.SshPasswordPrompt.disabledLayer,
        SshTunnel.SshEnvironmentManager.layer({ resolveCliRunner: Effect.succeed(ARCHIVE) }),
      );
      yield* Effect.gen(function* () {
        const manager = yield* SshTunnel.SshEnvironmentManager;
        const result = yield* Effect.result(
          manager.ensureEnvironment({
            alias: "devbox",
            hostname: "devbox",
            username: null,
            port: null,
          }),
        );
        assert.isTrue(Result.isFailure(result));
        if (Result.isFailure(result)) {
          assert.instanceOf(result.failure, SshCommandError);
          if (result.failure instanceof SshCommandError) {
            assert.equal(result.failure.exitCode, 1);
            assert.equal(result.failure.stderr.length, 16_384);
            assert.isTrue(result.failure.stderr.endsWith(diagnostic));
            assert.equal(result.failure.message, result.failure.stderr.trim());
          }
        }
      }).pipe(Effect.provide(layer), Effect.scoped);
    }),
  );

  it.effect("reports the real exit when reading tunnel diagnostics fails", () =>
    Effect.gen(function* () {
      const diagnostic = "Permission denied (publickey)";
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.succeed(
          commandArgs(command).includes("-N")
            ? {
                ...makeSuccessfulProcess(""),
                exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(255)),
                stderr: Stream.make(new TextEncoder().encode(diagnostic)).pipe(
                  Stream.concat(
                    Stream.fail(
                      PlatformError.systemError({
                        _tag: "Unknown",
                        module: "Stream",
                        method: "read",
                        description: "stderr read failed",
                      }),
                    ),
                  ),
                ),
              }
            : makeSuccessfulProcess('{"remotePort":3773}\n'),
        ),
      );
      const layer = Layer.mergeAll(
        NodeServices.layer,
        Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Layer.succeed(HttpClient.HttpClient, hangingHttpClient),
        Layer.succeed(NetService.NetService, testNetService),
        SshAuth.SshPasswordPrompt.disabledLayer,
        SshTunnel.SshEnvironmentManager.layer({ resolveCliRunner: Effect.succeed(ARCHIVE) }),
      );
      yield* Effect.gen(function* () {
        const manager = yield* SshTunnel.SshEnvironmentManager;
        const result = yield* Effect.result(
          manager.ensureEnvironment({
            alias: "devbox",
            hostname: "devbox",
            username: null,
            port: null,
          }),
        );
        assert.isTrue(Result.isFailure(result));
        if (Result.isFailure(result)) {
          assert.instanceOf(result.failure, SshCommandError);
          if (result.failure instanceof SshCommandError) {
            assert.equal(result.failure.exitCode, 255);
            assert.equal(result.failure.stderr, diagnostic);
          }
        }
      }).pipe(Effect.provide(layer), Effect.scoped);
    }),
  );

  it.effect("waits for local tunnel shutdown before reconnecting the same target", () =>
    Effect.gen(function* () {
      const shutdownStarted = yield* Deferred.make<void>();
      const finishShutdown = yield* Deferred.make<void>();
      const reconnectsStarted = yield* Deferred.make<void>();
      const pauseShutdown = Deferred.succeed(shutdownStarted, undefined).pipe(
        Effect.andThen(Deferred.await(finishShutdown)),
      );
      let resolutions = 0;
      let launches = 0;
      let tunnels = 0;
      let stops = 0;
      let remoteRunning = false;
      const target = { alias: "devbox", hostname: "devbox", username: null, port: null };
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.gen(function* () {
          const args = commandArgs(command);
          const isTarget = args.includes(target.alias);
          if (args.includes("-G")) {
            if (isTarget && ++resolutions === 4) {
              yield* Deferred.succeed(reconnectsStarted, undefined);
            }
            return makeSuccessfulProcess("");
          }
          if (args.includes("-N")) {
            const tunnel = makeRunningProcess(() => undefined);
            if (isTarget && ++tunnels === 1) {
              return {
                ...tunnel,
                kill: (options?: ChildProcess.KillOptions) =>
                  pauseShutdown.pipe(Effect.andThen(tunnel.kill(options))),
              };
            }
            return tunnel;
          }
          if (args.some((arg, index) => arg === "-s" && args[index + 1] === "--")) {
            if (isTarget) {
              launches += 1;
              remoteRunning = true;
            }
            return makeSuccessfulProcess('{"remotePort":3773}\n');
          }
          if (isTarget) {
            stops += 1;
            remoteRunning = false;
          }
          return makeSuccessfulProcess("");
        }),
      );
      const layer = Layer.mergeAll(
        NodeServices.layer,
        Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Layer.succeed(HttpClient.HttpClient, testHttpClient),
        Layer.succeed(NetService.NetService, testNetService),
        SshAuth.SshPasswordPrompt.disabledLayer,
        SshTunnel.SshEnvironmentManager.layer({ resolveCliRunner: Effect.succeed(ARCHIVE) }),
      );
      yield* Effect.gen(function* () {
        const manager = yield* SshTunnel.SshEnvironmentManager;
        yield* manager.ensureEnvironment(target);
        const disconnect = yield* Effect.forkChild(manager.disconnectEnvironment(target));
        yield* Deferred.await(shutdownStarted);
        const firstReconnect = yield* Effect.forkChild(manager.ensureEnvironment(target));
        const secondReconnect = yield* Effect.forkChild(manager.ensureEnvironment(target));
        yield* Deferred.await(reconnectsStarted);

        yield* manager.ensureEnvironment({
          alias: "other",
          hostname: "other",
          username: null,
          port: null,
        });
        yield* TestClock.adjust(Duration.zero);
        const launchesBeforeShutdown = launches;
        yield* Deferred.succeed(finishShutdown, undefined);
        yield* Fiber.join(disconnect);
        const first = yield* Fiber.join(firstReconnect);
        const second = yield* Fiber.join(secondReconnect);

        assert.equal(launchesBeforeShutdown, 1);
        assert.equal(launches, 2);
        assert.equal(tunnels, 2);
        assert.isTrue(remoteRunning);
        assert.equal(stops, 0);
        assert.equal(first.httpBaseUrl, second.httpBaseUrl);
      }).pipe(
        Effect.ensuring(Deferred.succeed(finishShutdown, undefined)),
        Effect.provide(layer),
        Effect.scoped,
      );
    }),
  );

  it.effect("disconnect and scope closure preserve the remote server", () => {
    const spawnedCommands: Array<ReadonlyArray<string>> = [];
    let tunnelKillCount = 0;
    let stopCommandCount = 0;
    const spawner = ChildProcessSpawner.make((command) =>
      Effect.sync(() => {
        const args = commandArgs(command);
        spawnedCommands.push(args);
        if (args.includes("-N")) {
          return makeRunningProcess(() => {
            tunnelKillCount += 1;
          });
        }
        if (args.some((arg, index) => arg === "-s" && args[index + 1] === "--")) {
          return makeSuccessfulProcess('{"remotePort":3773}\n');
        }
        if (args.includes("sh")) {
          stopCommandCount += 1;
          return makeSuccessfulProcess('{"stopped":true}\n');
        }
        return makeSuccessfulProcess("\n");
      }),
    );
    const layer = Layer.mergeAll(
      NodeServices.layer,
      Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Layer.succeed(HttpClient.HttpClient, testHttpClient),
      Layer.succeed(NetService.NetService, testNetService),
      SshAuth.SshPasswordPrompt.disabledLayer,
      SshTunnel.SshEnvironmentManager.layer({ resolveCliRunner: Effect.succeed(ARCHIVE) }),
    );
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;

    return Effect.gen(function* () {
      const manager = yield* SshTunnel.SshEnvironmentManager;

      const first = yield* manager.ensureEnvironment(target);
      assert.equal(first.httpBaseUrl, "http://127.0.0.1:41773/");
      const firstTunnelArgs = spawnedCommands.find((args) => args.includes("-N"));
      assert.isDefined(firstTunnelArgs);
      assert.include(firstTunnelArgs, "ControlMaster=no");
      assert.include(firstTunnelArgs, "ControlPath=none");
      assert.include(firstTunnelArgs, "ControlPersist=no");
      assert.deepEqual(firstTunnelArgs?.slice(-2), ["--", "julius@devbox"]);

      const disconnected = yield* Effect.result(manager.disconnectEnvironment(target));
      assert.isTrue(Result.isSuccess(disconnected));
      assert.equal(tunnelKillCount, 1);
      assert.equal(stopCommandCount, 0);
      yield* manager.disconnectEnvironment(target);
      assert.equal(stopCommandCount, 0);

      yield* manager.ensureEnvironment(target);

      assert.equal(spawnedCommands.filter((args) => args.includes("-N")).length, 2);
      assert.equal(tunnelKillCount, 1);
    }).pipe(
      Effect.provide(layer),
      Effect.scoped,
      Effect.andThen(
        Effect.sync(() => {
          assert.equal(tunnelKillCount, 2);
          assert.equal(stopCommandCount, 0);
        }),
      ),
    );
  });

  it.effect("replaces a stale SSH forward without stopping the remote server", () =>
    Effect.gen(function* () {
      const probeStarted = yield* Deferred.make<void>();
      let stale = false;
      let forwards = 0;
      let stops = 0;
      let kills = 0;
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.sync(() => {
          const args = commandArgs(command);
          if (args.includes("-N")) {
            forwards += 1;
            stale = false;
            return makeRunningProcess(() => {
              kills += 1;
            });
          }
          if (args.some((arg, index) => arg === "-s" && args[index + 1] === "--"))
            return makeSuccessfulProcess('{"remotePort":3773}\n');
          if (args.includes("sh")) stops += 1;
          return makeSuccessfulProcess("");
        }),
      );
      const httpClient = HttpClient.make((request) =>
        stale
          ? Deferred.succeed(probeStarted, undefined).pipe(Effect.andThen(Effect.never))
          : Effect.succeed(HttpClientResponse.fromWeb(request, new Response("", { status: 200 }))),
      );
      const layer = Layer.mergeAll(
        NodeServices.layer,
        Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Layer.succeed(HttpClient.HttpClient, httpClient),
        Layer.succeed(NetService.NetService, testNetService),
        SshAuth.SshPasswordPrompt.disabledLayer,
        SshTunnel.SshEnvironmentManager.layer({ resolveCliRunner: Effect.succeed(ARCHIVE) }),
      );
      yield* Effect.gen(function* () {
        const manager = yield* SshTunnel.SshEnvironmentManager;
        const target = { alias: "devbox", hostname: "devbox", username: null, port: null };
        yield* manager.ensureEnvironment(target);
        stale = true;
        const reconnect = yield* Effect.forkChild(manager.ensureEnvironment(target));
        yield* Deferred.await(probeStarted);
        yield* TestClock.adjust("2 seconds");
        yield* Fiber.join(reconnect);
        assert.equal(forwards, 2);
        assert.equal(kills, 1);
        assert.equal(stops, 0);
        yield* manager.disconnectEnvironment(target);
        assert.equal(stops, 0);
        assert.equal(kills, 2);
      }).pipe(Effect.provide(layer), Effect.scoped);
    }),
  );

  it("uses the remote t3 runner for launch and pairing scripts", () => {
    const stateKey = "711bc738002d72fd";
    const launch = SshTunnel.buildRemoteLaunchScript(ARCHIVE);
    const devLaunch = SshTunnel.buildRemoteLaunchScript({
      ...NODE_SCRIPT,
      nodeEngineRange: TEST_NODE_ENGINE_RANGE,
    });

    assert.include(launch, 'elif [ -n "$REMOTE_PID" ] && server_running "$REMOTE_PID"; then');
    assert.notInclude(launch, "RUNNER_CHANGED=1");
    assert.include(launch, "ensure_remote_node_path()");
    assert.include(launch, "if ! ensure_remote_node_path; then");
    assert.include(devLaunch, `T3_NODE_ENGINE_RANGE='${TEST_NODE_ENGINE_RANGE}'`);
    assert.include(devLaunch, "does not satisfy required range ");
    assert.include(launch, 'kill "$REMOTE_PID" 2>/dev/null || true');
    assert.include(launch, "wait_ready");
    assert.include(launch, '"$RUNNER_FILE" serve --host 127.0.0.1');
    assert.include(launch, '--base-dir "$DEFAULT_SERVER_HOME"');
    assert.notInclude(launch, "server-home");
    assert.include(launch, "Remote T3 server did not become ready");
    assert.include(launch, 'wait_ready "60000"');
    assert.include(launch, 'if [ -s "$LOG_FILE" ]; then');
    assert.include(launch, "It wrote nothing to %s");
    assert.include(launch, "T3_ARCHIVE_VERSION='1.2.3-preview.20260911.4'");
    assert.include(
      SshTunnel.buildRemotePairingScript(stateKey, ARCHIVE),
      '"$RUNNER_FILE" auth pairing create --base-dir "$PAIRING_BASE_DIR" --json',
    );
    assert.include(
      SshTunnel.buildRemotePairingScript(stateKey, ARCHIVE),
      'PAIRING_BASE_DIR="$DEFAULT_SERVER_HOME"',
    );
    assert.notInclude(SshTunnel.buildRemotePairingScript(stateKey, ARCHIVE), "server-home");
    assert.include(
      SshTunnel.buildRemotePairingScript(stateKey, ARCHIVE),
      "T3_ARCHIVE_VERSION='1.2.3-preview.20260911.4'",
    );
    assert.include(
      launch,
      'DEFAULT_RUNTIME_FILE="$DEFAULT_SERVER_HOME/userdata/server-runtime.json"',
    );
    assert.include(launch, "resolve_default_runtime_port()");
    assert.include(launch, 'DEFAULT_RUNTIME_INFO="$(resolve_default_runtime_port');
    assert.include(launch, "if (!Number.isInteger(pid) || pid <= 0 || !Number.isInteger(port))");
    assert.include(launch, 'if [ -n "$DEFAULT_REMOTE_PORT" ]; then');
    assert.include(launch, 'REMOTE_PORT="$DEFAULT_REMOTE_PORT"');
    assert.include(
      launch,
      'if [ "$REMOTE_MANAGED" != "managed" ] || [ "$REMOTE_PID" != "$DEFAULT_RUNTIME_PID" ]; then',
    );
    assert.include(launch, 'if [ -z "$REMOTE_PORT" ]; then');
    assert.include(launch, "readiness failure never grants replacement");
    assert.isBelow(
      launch.indexOf('DEFAULT_RUNTIME_INFO="$(resolve_default_runtime_port'),
      launch.indexOf('elif [ -n "$REMOTE_PID" ]'),
    );
  });

  it("attaches Jones to the host runtime without downloading or starting a server", () => {
    const launch = SshTunnel.buildRemoteLaunchScript(HOST_MANAGED);
    assert.include(launch, "service-state.json");
    assert.include(launch, '"serverKind":"external"');
    assert.notInclude(launch, "nohup");
    assert.notInclude(launch, "t3_fetch");
    assert.notInclude(launch, "pick-port");
    const pairing = SshTunnel.buildRemotePairingScript("test-host", HOST_MANAGED);
    assert.include(pairing, "service-state.json");
    assert.notInclude(pairing, "t3_fetch");
    assert.notInclude(
      SshTunnel.buildRemoteT3RunnerScript({ ...HOST_MANAGED, ...ARCHIVE }),
      ARCHIVE.archiveVersion,
    );
  });
});

describe("client tunnel lifetime", () => {
  it.effect.each(["scope close", "disconnect", "stale reconnect"] as const)(
    "%s leaves a second client's host and tunnel usable",
    (action) =>
      Effect.gen(function* () {
        const killed: number[] = [];
        const remoteCommands: ReadonlyArray<string>[] = [];
        let stalePort: string | undefined;
        let port = 41773;
        const target = { alias: "fixture", hostname: "fixture", username: null, port: null };
        const spawner = ChildProcessSpawner.make((command) =>
          Effect.sync(() => {
            const args = commandArgs(command);
            if (args.includes("-N")) {
              const localPort = Number(args[args.indexOf("-L") + 1]?.split(":")[0]);
              return makeRunningProcess(() => killed.push(localPort));
            }
            if (args.includes("sh")) {
              remoteCommands.push(args);
              return makeSuccessfulProcess('{"remotePort":3773,"serverKind":"managed"}\n');
            }
            return makeSuccessfulProcess("");
          }),
        );
        const http = HttpClient.make((request) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              new Response("", { status: new URL(request.url).port === stalePort ? 503 : 200 }),
            ),
          ),
        );
        const services = Layer.mergeAll(
          NodeServices.layer,
          Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Layer.succeed(HttpClient.HttpClient, http),
          Layer.succeed(NetService.NetService, {
            ...testNetService,
            reserveLoopbackPort: () => Effect.sync(() => port++),
          }),
          SshAuth.SshPasswordPrompt.disabledLayer,
        );
        yield* Effect.gen(function* () {
          const firstScope = yield* Scope.make();
          const secondScope = yield* Scope.make();
          yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void));
          yield* Effect.addFinalizer(() => Scope.close(secondScope, Exit.void));
          const makeManager = (scope: Scope.Scope) =>
            Layer.buildWithScope(
              SshTunnel.SshEnvironmentManager.layer({ resolveCliRunner: Effect.succeed(ARCHIVE) }),
              scope,
            ).pipe(Effect.map((context) => Context.get(context, SshTunnel.SshEnvironmentManager)));
          const first = yield* makeManager(firstScope);
          const second = yield* makeManager(secondScope);
          const firstConnection = yield* first.ensureEnvironment(target);
          const secondConnection = yield* second.ensureEnvironment(target);
          if (action === "scope close") {
            yield* Scope.close(firstScope, Exit.void);
          } else if (action === "disconnect") {
            yield* first.disconnectEnvironment(target);
          } else {
            stalePort = new URL(firstConnection.httpBaseUrl).port;
            const reconnect = yield* Effect.forkChild(first.ensureEnvironment(target));
            yield* TestClock.adjust(Duration.seconds(2));
            const replacement = yield* Fiber.join(reconnect);
            assert.notEqual(replacement.httpBaseUrl, firstConnection.httpBaseUrl);
          }
          assert.deepEqual(killed, [41773]);
          assert.deepEqual(yield* second.ensureEnvironment(target), secondConnection);
          assert.equal(remoteCommands.length, action === "stale reconnect" ? 3 : 2);
          assert.isTrue(
            remoteCommands.every((args) =>
              args.some((arg, index) => arg === "-s" && args[index + 1] === "--"),
            ),
          );
        }).pipe(Effect.provide(services), Effect.scoped);
      }),
  );
});

describe("launch script", () => {
  const windowsHost = HostProcess.Platform.defaultValue() === "win32";
  const FAKE_SERVER = `const fs = require("node:fs");
const http = require("node:http");
const path = require("node:path");
const args = process.argv.slice(2);
const flag = (name) => args[args.indexOf(name) + 1];
// --answer-once stalls every request after the first, like a busy server;
// --exit-after-answer exits right after the first answer.
const answerOnce = args.includes("--answer-once");
const exitAfterAnswer = args.includes("--exit-after-answer");
let answered = false;
const server = http.createServer((_request, response) => {
  if (answerOnce && answered) return;
  answered = true;
  response.end("ok", () => {
    if (exitAfterAnswer) process.exit(0);
  });
});
server.listen(Number(flag("--port")), "127.0.0.1", () => {
  const port = server.address().port;
  const userdata = path.join(flag("--base-dir"), "userdata");
  fs.mkdirSync(userdata, { recursive: true });
  fs.writeFileSync(
    path.join(userdata, "server-runtime.json"),
    JSON.stringify({ version: 1, pid: process.pid, port, origin: "http://127.0.0.1:" + port }),
  );
  console.log(port);
});
`;

  const isAlive = (pid: number) => {
    try {
      process.kill(pid, 0);
      return true;
    } catch {
      return false;
    }
  };

  const freeLoopbackPort = Effect.callback<number>((resume) => {
    const server = NodeNet.createServer();
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      server.close(() =>
        resume(Effect.succeed(typeof address === "object" && address ? address.port : 0)),
      );
    });
  });

  const isListening = (port: number) =>
    Effect.callback<boolean>((resume) => {
      const socket = NodeNet.connect(port, "127.0.0.1");
      socket.once("connect", () => {
        socket.destroy();
        resume(Effect.succeed(true));
      });
      socket.once("error", () => resume(Effect.succeed(false)));
    });

  const launchResult = (stdout: string) => JSON.parse(stdout.trim().split("\n").at(-1) ?? "");

  const prepareLaunch = Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-launch-script-" });
    const home = `${root}/home`;
    const stateDir = `${home}/.t3/ssh-launch/test-state`;
    const fakeServer = `${root}/fake-t3.cjs`;
    const launch = `${root}/launch.sh`;
    yield* fs.makeDirectory(stateDir, { recursive: true });
    yield* fs.writeFileString(`${stateDir}/port`, `${yield* freeLoopbackPort}\n`);
    yield* fs.writeFileString(fakeServer, FAKE_SERVER);
    yield* fs.writeFileString(
      launch,
      SshTunnel.buildRemoteLaunchScript({ nodeScriptPath: fakeServer }),
    );
    const readManagedPid = fs
      .readFileString(`${stateDir}/pid`)
      .pipe(Effect.map((pid) => Number(pid.trim())));
    // Launched servers outlive the script, so stop each PID the script
    // recorded, including one it recorded just before a failed assertion.
    const launchedPids: Array<number> = [];
    yield* Effect.addFinalizer(() =>
      readManagedPid.pipe(
        Effect.map((pid) => [...launchedPids, pid]),
        Effect.orElseSucceed(() => launchedPids),
        Effect.map((pids) => {
          for (const pid of pids) {
            if (isAlive(pid)) process.kill(pid, "SIGKILL");
          }
        }),
      ),
    );
    const runLaunchScript = runScript(home, [launch, "test-state"]);
    const runLaunch = runLaunchScript.pipe(
      Effect.map((result) => {
        assert.equal(result.exitCode, 0, result.stderr);
        return launchResult(result.stdout);
      }),
    );
    const launchManaged = Effect.gen(function* () {
      const launched = yield* runLaunch;
      const pid = yield* readManagedPid;
      launchedPids.push(pid);
      assert.equal(launched.serverKind, "managed");
      return { launched, pid };
    });
    // An unreaped server runs under a parent that never waits on it, so once it
    // exits it lingers as a zombie.
    const startDefaultHomeServer = (
      flags: ReadonlyArray<string>,
      options?: { readonly unreaped?: boolean },
    ) =>
      Effect.gen(function* () {
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const args = [fakeServer, "serve", "--port", "0", "--base-dir", `${home}/.t3`, ...flags];
        const server = yield* spawner.spawn(
          options?.unreaped
            ? ChildProcess.make("sh", ["-c", 'node "$@" & exec sleep 600', "sh", ...args])
            : ChildProcess.make("node", args),
        );
        const port = yield* server.stdout.pipe(
          Stream.decodeText(),
          Stream.splitLines,
          Stream.runHead,
        );
        return Number(Option.getOrThrow(port));
      });
    return {
      home,
      stateDir,
      launch,
      readManagedPid,
      runLaunchScript,
      runLaunch,
      launchManaged,
      startDefaultHomeServer,
    };
  });

  it.effect.skipIf(windowsHost)(
    "reuses its managed server and attaches to a different default-home owner",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const { home, stateDir, readManagedPid, runLaunch, launchManaged, startDefaultHomeServer } =
          yield* prepareLaunch;
        const { launched, pid: managedPid } = yield* launchManaged;

        assert.deepEqual(yield* runLaunch, launched);
        assert.equal(yield* readManagedPid, managedPid);
        assert.isTrue(isAlive(managedPid));

        yield* fs.writeFileString(`${stateDir}/port`, `${yield* freeLoopbackPort}\n`);
        assert.deepEqual(yield* runLaunch, launched);
        assert.equal(yield* readManagedPid, managedPid);
        assert.equal(
          (yield* fs.readFileString(`${stateDir}/port`)).trim(),
          `${launched.remotePort}`,
        );

        const servicePort = yield* startDefaultHomeServer([]);
        assert.deepEqual(yield* runLaunch, {
          remotePort: servicePort,
          serverKind: "external",
        });
        assert.isTrue(yield* isListening(launched.remotePort));
        assert.equal(
          (yield* fs.readFileString(`${stateDir}/pid`)).trim(),
          JSON.parse(
            yield* fs.readFileString(`${home}/.t3/userdata/server-runtime.json`),
          ).pid.toString(),
        );
      }).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect.skipIf(windowsHost)(
    "waits for its busy managed server instead of replacing it",
    () =>
      Effect.gen(function* () {
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const { readManagedPid, runLaunch, launchManaged } = yield* prepareLaunch;
        const { launched, pid } = yield* launchManaged;

        process.kill(pid, "SIGSTOP");
        yield* spawner.spawn(ChildProcess.make("sh", ["-c", `sleep 1; kill -CONT ${pid}`]));
        assert.deepEqual(yield* runLaunch, launched);
        assert.equal(yield* readManagedPid, pid);
        assert.isTrue(isAlive(pid));
      }).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect.skipIf(windowsHost)(
    "fails without stopping a managed server that never becomes ready",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const { stateDir, launch, readManagedPid, runLaunchScript, launchManaged } =
          yield* prepareLaunch;
        const { launched, pid } = yield* launchManaged;

        const script = yield* fs.readFileString(launch);
        assert.include(script, 'wait_ready "2000"');
        yield* fs.writeFileString(
          launch,
          script.replaceAll('wait_ready "2000"', 'wait_ready "1500"'),
        );
        process.kill(pid, "SIGSTOP");
        const result = yield* runLaunchScript;
        assert.equal(result.exitCode, 1, result.stdout);
        assert.include(result.stderr, "already running");
        assert.isTrue(isAlive(pid));
        assert.equal(yield* readManagedPid, pid);
        assert.equal(
          (yield* fs.readFileString(`${stateDir}/port`)).trim(),
          `${launched.remotePort}`,
        );
        assert.equal((yield* fs.readFileString(`${stateDir}/managed`)).trim(), "managed");
      }).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect.skipIf(windowsHost)(
    "preserves its managed server when the client runner changes",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const { stateDir, readManagedPid, runLaunch, launchManaged } = yield* prepareLaunch;
        const { pid: previousPid } = yield* launchManaged;

        yield* fs.writeFileString(`${stateDir}/run-t3.sh`, "# previous runner\n");
        assert.equal((yield* runLaunch).serverKind, "managed");
        const replacementPid = yield* readManagedPid;
        assert.equal(replacementPid, previousPid);
        assert.isTrue(isAlive(previousPid));
        assert.isTrue(isAlive(replacementPid));
      }).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect.skipIf(windowsHost)(
    "replaces a managed server that exited but was not reaped",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const { home, stateDir, readManagedPid, runLaunch, launchManaged } = yield* prepareLaunch;
        const { launched, pid } = yield* launchManaged;
        process.kill(pid, "SIGKILL");

        // A crashed server whose parent never reaps it stays a zombie, which
        // kill -0 still reports. Its pid file and runtime record keep naming it.
        const parent = yield* spawner.spawn(
          ChildProcess.make("sh", ["-c", "sleep 600 & echo $!; exec sleep 600"]),
        );
        const zombie = Number(
          Option.getOrThrow(
            yield* parent.stdout.pipe(Stream.decodeText(), Stream.splitLines, Stream.runHead),
          ),
        );
        // The shell prints the PID before it execs and can still reap a child
        // that dies in between. Kill it only once the parent is sleep, which
        // never reaps. An empty image means the parent itself is gone.
        let parentImage = "";
        for (let attempt = 0; attempt < 20; attempt++) {
          parentImage = (yield* runScript(home, [
            "-c",
            `ps -o comm= -p ${parent.pid}`,
          ])).stdout.trim();
          if (parentImage === "" || /(^|\/)sleep$/.test(parentImage)) break;
        }
        assert.match(parentImage, /(^|\/)sleep$/);
        process.kill(zombie, "SIGKILL");
        assert.include((yield* runScript(home, ["-c", `ps -o stat= -p ${zombie}`])).stdout, "Z");
        yield* fs.writeFileString(`${stateDir}/pid`, `${zombie}\n`);
        yield* fs.writeFileString(
          `${home}/.t3/userdata/server-runtime.json`,
          JSON.stringify({
            version: 1,
            pid: zombie,
            port: launched.remotePort,
            origin: `http://127.0.0.1:${launched.remotePort}`,
          }),
        );

        assert.equal((yield* runLaunch).serverKind, "managed");
        const replacementPid = yield* readManagedPid;
        assert.notEqual(replacementPid, zombie);
        assert.isTrue(isAlive(replacementPid));
      }).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect.skipIf(windowsHost)(
    "adopts a server it has no managed record for instead of restarting it",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const { stateDir, launchManaged, runLaunch } = yield* prepareLaunch;
        const { launched, pid } = yield* launchManaged;

        yield* fs.remove(`${stateDir}/managed`);
        yield* fs.writeFileString(`${stateDir}/run-t3.sh`, "# superseded runner\n");
        assert.deepEqual(yield* runLaunch, {
          remotePort: launched.remotePort,
          serverKind: "external",
        });
        assert.isTrue(isAlive(pid));
      }).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect.skipIf(windowsHost)(
    "keeps an adopted default-home server without launching another beside it",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const { stateDir, runLaunch, startDefaultHomeServer } = yield* prepareLaunch;
        yield* fs.writeFileString(`${stateDir}/managed`, "managed\n");
        const servicePort = yield* startDefaultHomeServer(["--answer-once"]);

        assert.deepEqual(yield* runLaunch, { remotePort: servicePort, serverKind: "external" });
        assert.isTrue(yield* fs.exists(`${stateDir}/pid`));
      }).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect.skipIf(windowsHost)(
    "preserves the previous owner when the adopted server exits after readiness",
    () =>
      Effect.gen(function* () {
        const { readManagedPid, runLaunch, launchManaged, startDefaultHomeServer } =
          yield* prepareLaunch;
        const { pid: previousPid } = yield* launchManaged;
        yield* startDefaultHomeServer(["--exit-after-answer"], { unreaped: true });

        const result = yield* runLaunch;
        assert.equal(result.serverKind, "external");
        assert.notEqual(yield* readManagedPid, previousPid);
        assert.isTrue(isAlive(previousPid));
      }).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );
});

// On macOS, Node can bind 127.0.0.1:<port> while another process (such as the
// desktop app with network access on) listens on *:<port>. Linux refuses that
// bind unless both sockets set SO_REUSEPORT, so on Linux the preload adds it to
// the picker's probe and the holder sets it too.
