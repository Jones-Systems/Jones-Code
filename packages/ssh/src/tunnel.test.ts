import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NetService from "@t3tools/shared/Net";
import { HostProcessArchitecture, HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Context from "effect/Context";
import * as Exit from "effect/Exit";
import * as Scope from "effect/Scope";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";
import { HttpClient, HttpClientResponse } from "effect/http";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import * as SshAuth from "./auth.ts";
import * as SshTunnel from "./tunnel.ts";

const TEST_NODE_ENGINE_RANGE = "^22.16 || ^23.11 || >=24.10";

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

const makeDelayedSuccessfulProcess = (stdout: string, delayMs: number) => {
  const process = makeSuccessfulProcess(stdout);
  return {
    ...process,
    exitCode: Effect.sleep(Duration.millis(delayMs)).pipe(
      Effect.as(ChildProcessSpawner.ExitCode(0)),
    ),
  };
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
const NODE_SCRIPT = {
  nodeScriptPath: "/Users/julius/Development/Work/codething-mvp/apps/server/dist/bin.mjs",
} as const;

describe("ssh tunnel scripts", () => {
  it("installs and runs the release archive without Node, npm, or npx", () => {
    const script = SshTunnel.buildRemoteT3RunnerScript(ARCHIVE);

    assert.include(script, "T3_ARCHIVE_VERSION='1.2.3-preview.20260911.4'");
    assert.include(script, "T3_NODE_SCRIPT_PATH=''");
    assert.include(
      script,
      "T3_RELEASE_BASE_URL='https://github.com/pingdotgg/t3code/releases/download'",
    );
    assert.include(script, 'T3_RUNTIME_DIR="$HOME/.t3/runtime/versions/$T3_ARCHIVE_VERSION"');
    assert.include(script, 'T3_ARCHIVE="t3-$T3_ARCHIVE_VERSION-$T3_PLATFORM-$T3_ARCH.tar.gz"');
    assert.include(script, "SHA256SUMS");
    assert.include(script, 'exec "$T3_RUNTIME_DIR/t3" "$@"');
    assert.notInclude(script, "npx");
    assert.notInclude(script, "npm exec");
    assert.notInclude(script, "t3@latest");
    assert.notInclude(script, 'exec t3 "$@"');
    // Concurrent launches serialize on a per-version mkdir lock and recheck
    // the completion marker after acquiring it.
    assert.include(
      script,
      'T3_LOCK="$HOME/.t3/runtime/versions/.$T3_ARCHIVE_VERSION.install.lock"',
    );
    // mkdir is the exclusive create; the pid follows atomically. A dead owner
    // is reclaimed at once, a never-published owner after a short grace.
    assert.include(script, 'while ! mkdir "$T3_LOCK" 2>/dev/null; do');
    assert.include(script, 'mv "$T3_LOCK/pid.tmp" "$T3_LOCK/pid"');
    assert.include(script, 'if ! kill -0 "$T3_LOCK_OWNER" 2>/dev/null; then');
    assert.include(script, 'if [ "$T3_LOCK_UNOWNED" -ge 5 ]; then');
    assert.include(script, 'if [ "$T3_LOCK_WAITED" -ge 360 ]; then');
    assert.include(script, '"$T3_STAGING/SHA256SUMS" 30');
    assert.include(script, '"$T3_STAGING/$T3_ARCHIVE" 240');
    assert.notInclude(script, "T3_LOCK_CANDIDATE");
    assert.notInclude(script, "-mmin");
    assert.equal(script.split("if ! t3_runtime_ready; then").length - 1, 2);
    assert.isBelow(
      script.indexOf('"$T3_STAGING/t3" --version'),
      script.indexOf('> "$T3_STAGING/.install-complete"'),
    );
    // Node discovery is defined for the dev path but only ever invoked inside
    // the node-script branch, which the archive path skips entirely.
    assert.equal(script.split("ensure_remote_node_path || true").length - 1, 1);
    assert.isBelow(
      script.indexOf("ensure_remote_node_path || true"),
      script.indexOf('exec node "$T3_NODE_SCRIPT_PATH" "$@"'),
    );
    assert.isBelow(
      script.indexOf('exec node "$T3_NODE_SCRIPT_PATH" "$@"'),
      script.indexOf("T3_ARCHIVE_VERSION="),
    );

    const launch = SshTunnel.buildRemoteLaunchScript({
      ...ARCHIVE,
      releaseBaseUrl: "https://mirror.example/t3/",
    });
    assert.include(launch, "T3_ARCHIVE_MODE=1");
    assert.include(launch, "T3_RELEASE_BASE_URL='https://mirror.example/t3'");
    assert.include(launch, '"$RUNNER_FILE" __ssh-helper pick-port "$PORT_FILE"');
    assert.include(launch, '"$RUNNER_FILE" __ssh-helper wait-ready "$REMOTE_PORT"');
    assert.include(launch, '"$RUNNER_FILE" __ssh-helper runtime-port "$DEFAULT_RUNTIME_FILE"');
    assert.include(SshTunnel.buildRemoteLaunchScript(NODE_SCRIPT), "T3_ARCHIVE_MODE=0");
  });

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

  it("refuses to build a runner with neither an archive version nor a node script", () => {
    for (const input of [undefined, {}, { archiveVersion: "  " }, { nodeScriptPath: null }]) {
      assert.throws(
        () => SshTunnel.buildRemoteT3RunnerScript(input),
        SshTunnel.SshMissingRunnerError,
      );
    }
    assert.throws(() => SshTunnel.buildRemoteLaunchScript(), SshTunnel.SshMissingRunnerError);
  });

  it("does not hard-code a remote node engine range", () => {
    const script = SshTunnel.buildRemoteT3RunnerScript(NODE_SCRIPT);

    assert.include(script, "T3_NODE_ENGINE_RANGE=''");
    assert.notInclude(script, TEST_NODE_ENGINE_RANGE);
  });

  it("builds the remote t3 runner with a node script override", () => {
    const script = SshTunnel.buildRemoteT3RunnerScript({
      ...NODE_SCRIPT,
      nodeEngineRange: TEST_NODE_ENGINE_RANGE,
    });

    assert.include(
      script,
      "T3_NODE_SCRIPT_PATH='/Users/julius/Development/Work/codething-mvp/apps/server/dist/bin.mjs'",
    );
    assert.include(script, 'exec node "$T3_NODE_SCRIPT_PATH" "$@"');
    assert.include(script, "T3_ARCHIVE_VERSION=''");
    assert.include(script, 'prepend_path_if_dir "$HOME/.local/bin"');
    assert.include(script, `T3_NODE_ENGINE_RANGE='${TEST_NODE_ENGINE_RANGE}'`);
    assert.include(script, "remote_node_satisfies_engine()");
    assert.include(script, "function satisfiesSemverRange");
    assert.include(script, "satisfiesSemverRange(rawVersion, range)");
    assert.include(script, 'prepend_path_if_dir "/home/linuxbrew/.linuxbrew/bin"');
    assert.include(script, 'prepend_path_if_dir "$VOLTA_HOME/bin"');
    assert.include(script, 'prepend_path_if_dir "$HOME/.asdf/shims"');
    assert.include(script, 'prepend_path_if_dir "$HOME/.local/share/mise/shims"');
    assert.include(script, 'eval "$(fnm env --shell bash)"');
    assert.include(script, "fnm use --silent-if-unchanged");
    assert.include(script, "fnm use default");
    assert.include(script, 'prepend_path_if_dir "$HOME/.nodenv/shims"');
    assert.include(script, 'NVM_DIR="$HOME/.nvm"');
    assert.include(script, "nvm use --silent default");
    assert.include(script, 'for T3_NODE_BIN in "$NVM_DIR"/versions/node/*/bin');
    assert.notInclude(script, "ensure $NVM_DIR/nvm.sh is available");
    assert.notInclude(script, "npx");
  });

  it("uses the remote t3 runner for launch and pairing scripts", () => {
    const stateKey = "711bc738002d72fd";
    const launch = SshTunnel.buildRemoteLaunchScript(ARCHIVE);
    const devLaunch = SshTunnel.buildRemoteLaunchScript({
      ...NODE_SCRIPT,
      nodeEngineRange: TEST_NODE_ENGINE_RANGE,
    });

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

  it.effect("allows cold remote launches to exceed the default SSH command timeout", () => {
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;
    const spawner = ChildProcessSpawner.make(() =>
      Effect.succeed(makeDelayedSuccessfulProcess('{"remotePort":3774}\n', 75_000)),
    );
    const layerSpawner = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner);
    const layerProcess = Layer.mergeAll(NodeServices.layer, layerSpawner, TestClock.layer());

    return Effect.gen(function* () {
      const fiber = yield* Effect.forkChild(
        SshTunnel.launchOrReuseRemoteServer(target, undefined, NODE_SCRIPT),
      );
      yield* Effect.yieldNow;
      yield* TestClock.adjust(Duration.seconds(75));

      const result = yield* Fiber.join(fiber);
      assert.equal(result.remotePort, 3774);
    }).pipe(Effect.provide(layerProcess));
  });

  it.effect("gives cold archive launches a larger budget than node-script launches", () => {
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;
    const spawner = ChildProcessSpawner.make(() =>
      Effect.succeed(makeDelayedSuccessfulProcess('{"remotePort":3774}\n', 800_000)),
    );
    const layerSpawner = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner);
    const layerProcess = Layer.mergeAll(NodeServices.layer, layerSpawner, TestClock.layer());

    return Effect.gen(function* () {
      const fiber = yield* Effect.forkChild(
        SshTunnel.launchOrReuseRemoteServer(target, undefined, ARCHIVE),
      );
      yield* Effect.yieldNow;
      yield* TestClock.adjust(Duration.seconds(800));

      const result = yield* Fiber.join(fiber);
      assert.equal(result.remotePort, 3774);
    }).pipe(Effect.provide(layerProcess));
  });

  it("allows the remote port picker to run without a state file path", () => {
    assert.include(SshTunnel.REMOTE_PICK_PORT_SCRIPT, 'const filePath = process.argv[2] ?? "";');
  });

  it.effect("bounds each HTTP readiness probe so retries cannot hang on one request", () =>
    Effect.gen(function* () {
      const fiber = yield* Effect.forkChild(
        Effect.result(
          SshTunnel.waitForHttpReady({
            baseUrl: "http://127.0.0.1:41773/",
            timeoutMs: 1_000,
            intervalMs: 100,
            probeTimeoutMs: 250,
          }),
        ),
      );
      yield* Effect.yieldNow;
      yield* TestClock.adjust(Duration.millis(1_000));

      const result = yield* Fiber.join(fiber);

      assert.isTrue(Result.isFailure(result));
      if (Result.isFailure(result)) {
        assert.include(result.failure.message, "Timed out waiting 1000ms");
      }
    }).pipe(
      Effect.provide(
        Layer.merge(TestClock.layer(), Layer.succeed(HttpClient.HttpClient, hangingHttpClient)),
      ),
    ),
  );

  it("preserves primitive readiness reason values in diagnostic output", () => {
    assert.deepEqual(
      SshTunnel.describeReadinessCause({
        _tag: "HttpClientError",
        message: "Backend readiness probe failed.",
        reason: "authentication failed",
        cause: "upstream closed",
      }),
      {
        _tag: "HttpClientError",
        message: "Backend readiness probe failed.",
        reason: "authentication failed",
        cause: "upstream closed",
      },
    );
  });

  it.effect("accepts pretty-printed pairing JSON from the remote CLI", () => {
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;
    const spawner = ChildProcessSpawner.make(() =>
      Effect.succeed(
        makeSuccessfulProcess(`{
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
        if (args.includes("sh") && args.includes("--")) {
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
          if (args.includes("--")) {
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
          assert.isTrue(remoteCommands.every((args) => args.includes("--")));
        }).pipe(Effect.provide(services), Effect.scoped);
      }),
  );
});

// The archive runner is generated shell; string assertions cannot prove the
// lock excludes concurrent installers. Run the real script against a tiny
// fake archive served from a file:// mirror.
describe("archive runner script", () => {
  const hostPlatform = HostProcessPlatform.defaultValue();
  const hostArch = HostProcessArchitecture.defaultValue();
  const windowsHost = hostPlatform === "win32";
  const archiveVersion = "1.2.3-preview.20260911.4";

  const runRunner = (home: string, runner: string) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const child = yield* spawner.spawn(
        ChildProcess.make("sh", [runner, "--version"], {
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

  // A fake "executable" that answers --version, packed the way the release
  // workflow packs the real archive: one top-level directory named after the
  // stem, checksummed in SHA256SUMS.
  const makeMirror = Effect.fn("makeMirror")(function* (root: string) {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const platform = hostPlatform === "darwin" ? "darwin" : "linux";
    const arch = hostArch === "arm64" ? "arm64" : "x64";
    const stem = `t3-${archiveVersion}-${platform}-${arch}`;
    const stage = `${root}/stage/${stem}`;
    const release = `${root}/mirror/v${archiveVersion}`;
    const script = [
      "set -eu",
      `mkdir -p '${stage}' '${release}'`,
      `printf '#!/bin/sh\\necho t3 v${archiveVersion}\\n' > '${stage}/t3'`,
      `chmod +x '${stage}/t3'`,
      `tar -czf '${release}/${stem}.tar.gz' -C '${root}/stage' '${stem}'`,
      `cd '${release}' && (sha256sum '${stem}.tar.gz' 2>/dev/null || shasum -a 256 '${stem}.tar.gz') > SHA256SUMS`,
    ].join("\n");
    const child = yield* spawner.spawn(ChildProcess.make("sh", ["-c", script]));
    assert.equal(Number(yield* child.exitCode), 0);
    return `file://${root}/mirror`;
  });

  it.effect.skipIf(windowsHost)(
    "installs once when several launches race, and reclaims stale locks",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-archive-runner-" });
        const releaseBaseUrl = yield* makeMirror(root);
        const runner = `${root}/run-t3.sh`;
        yield* fs.writeFileString(
          runner,
          SshTunnel.buildRemoteT3RunnerScript({ archiveVersion, releaseBaseUrl }),
        );
        const home = `${root}/home`;
        yield* fs.makeDirectory(home, { recursive: true });

        const results = yield* Effect.all(
          [runRunner(home, runner), runRunner(home, runner), runRunner(home, runner)],
          { concurrency: "unbounded" },
        );
        for (const result of results) {
          assert.equal(result.exitCode, 0, result.stderr);
          assert.include(result.stdout, `t3 v${archiveVersion}`);
        }
        const versionsDir = `${home}/.t3/runtime/versions`;
        assert.deepEqual(yield* fs.readDirectory(versionsDir), [archiveVersion]);
        assert.equal(
          (yield* fs.readFileString(`${versionsDir}/${archiveVersion}/.install-complete`)).trim(),
          archiveVersion,
        );

        // A lock left by a crashed installer (dead pid) must not block the
        // next launch, and neither must one that never published a pid.
        const lock = `${versionsDir}/.${archiveVersion}.install.lock`;
        yield* fs.remove(`${versionsDir}/${archiveVersion}`, { recursive: true });
        yield* fs.makeDirectory(lock);
        yield* fs.writeFileString(`${lock}/pid`, "999999\n");
        const afterDead = yield* runRunner(home, runner);
        assert.equal(afterDead.exitCode, 0, afterDead.stderr);

        yield* fs.remove(`${versionsDir}/${archiveVersion}`, { recursive: true });
        yield* fs.makeDirectory(lock);
        const afterUnowned = yield* runRunner(home, runner);
        assert.equal(afterUnowned.exitCode, 0, afterUnowned.stderr);
        assert.isFalse(yield* fs.exists(lock));
      }).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );
});
