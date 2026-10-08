import {
  type DeviceHostSummary,
  DevicePlatformAvailability,
  DeviceToolVersions,
  deviceToolInstallMessage,
  type SshDeviceHostConfig,
} from "@t3tools/contracts";
import { runSshCommand, baseSshArgs, resolveSshCommand } from "@t3tools/ssh/command";
import * as NetService from "@t3tools/shared/Net";
import { waitForHttpReady } from "@t3tools/shared/httpReadiness";
import * as Crypto from "effect/Crypto";
import * as Exit from "effect/Exit";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Hex from "effect/encoding/Hex";
import * as HttpClient from "effect/http/HttpClient";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as ServerConfig from "../config.ts";
import * as DeviceHost from "./DeviceHost.ts";
import { DeviceDirectGrants } from "../jones/device/DeviceDirectGrants.ts";
import { registerDirectRetirement, directGatewayHealth } from "../jones/device/directLifecycle.ts";
import { openDirectAdmission } from "../jones/device/DeviceDirectAdmission.ts";
import { quoteRemoteArg, remoteDeviceEnvironment, remoteDeviceScript } from "./sshDeviceScript.ts";

const Probe = Schema.Struct({
  nodePath: Schema.String,
  tools: Schema.optional(DeviceToolVersions),
  platforms: Schema.Array(DevicePlatformAvailability),
});
const Started = Schema.Struct({
  ...Probe.fields,
  hubPort: Schema.Int,
  directMedia: Schema.optionalKey(
    Schema.Struct({
      gatewayPort: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 65535 })),
      admissionPort: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 65535 })),
      generation: Schema.String,
    }),
  ),
  daemonPort: Schema.optionalKey(Schema.Int),
  token: Schema.optionalKey(Schema.String),
  entryPath: Schema.optionalKey(Schema.String),
  helpers: Schema.Struct({
    serveSimAxSettings: Schema.NullOr(Schema.String),
    serveSimCli: Schema.NullOr(Schema.String),
  }),
});
const decodeProbe = Schema.decodeUnknownEffect(Schema.fromJsonString(Probe));
const decodeStarted = Schema.decodeUnknownEffect(Schema.fromJsonString(Started));
const targetFor = (config: SshDeviceHostConfig) => ({
  alias: config.target,
  hostname: config.target,
  username: null,
  port: config.port ?? null,
});
const identityArgs = (config: SshDeviceHostConfig) =>
  config.identityFile ? ["-i", config.identityFile] : [];
const commandArgs = (script: string) => [
  "sh",
  "-c",
  quoteRemoteArg(remoteDeviceEnvironment + script),
];
const bootstrap = (
  config: SshDeviceHostConfig,
  owner: string,
  mode: "probe" | "start" | "agent-start" | "stop-agent" | "stop" | "stop-direct",
  generation?: string,
) =>
  runSshCommand(targetFor(config), {
    preHostArgs: identityArgs(config),
    remoteCommandArgs: commandArgs(
      'command -v node >/dev/null 2>&1 || { echo "Node is missing from the non-interactive SSH PATH" >&2; exit 1; }; exec node',
    ),
    stdin: remoteDeviceScript(
      owner,
      mode,
      generation ? { hostId: config.id, generation } : undefined,
    ),
    timeoutMs: mode === "start" || mode === "agent-start" ? 1_300_000 : 45_000,
  }).pipe(
    Effect.mapError(
      (cause) => new DeviceHost.DeviceHostError({ hostId: config.id, step: mode, cause }),
    ),
  );

const ownerFor = Effect.fn("SshDeviceHost.ownerFor")(function* (hostId: string) {
  const fs = yield* FileSystem.FileSystem;
  const server = yield* ServerConfig.ServerConfig;
  const environmentId = yield* fs
    .readFileString(server.environmentIdPath)
    .pipe(Effect.orElseSucceed(() => server.stateDir));
  const crypto = yield* Crypto.Crypto;
  const owner = yield* crypto
    .digest("SHA-256", new TextEncoder().encode(`${environmentId}\0${server.stateDir}\0${hostId}`))
    .pipe(Effect.map(Hex.encode), Effect.orDie);
  return owner.slice(0, 24);
});

export const probe = Effect.fn("SshDeviceHost.probe")(function* (
  config: SshDeviceHostConfig,
  owner?: string,
) {
  const result = yield* bootstrap(config, owner ?? (yield* ownerFor(config.id)), "probe");
  const value = yield* decodeProbe(result.stdout.trim()).pipe(
    Effect.mapError(
      (cause) =>
        new DeviceHost.DeviceHostError({ hostId: config.id, step: "reading probe result", cause }),
    ),
  );
  return {
    id: config.id,
    label: config.label,
    kind: "ssh",
    tools: value.tools,
    hubInstalled:
      value.tools?.hub.installedVersions.includes(value.tools.hub.requiredVersion) ?? false,
    agentDeviceInstalled:
      value.tools?.agent.installedVersions.includes(value.tools.agent.requiredVersion) ?? false,
    platforms: value.platforms,
  } satisfies DeviceHostSummary;
});

type SshDeviceHostReady = DeviceHost.DeviceHostReady & {
  readonly agentDevice?: DeviceHost.DeviceHostAgentReady["agentDevice"];
};

export const make = Effect.fn("SshDeviceHost.make")(function* (
  config: SshDeviceHostConfig,
  onReady: (
    ready: DeviceHost.DeviceHostAgentReady,
  ) => Effect.Effect<void, DeviceHost.DeviceHostError> = () => Effect.void,
  onStatus: (
    status: "starting" | "ready" | "failed",
    detail?: string,
  ) => Effect.Effect<void> = () => Effect.void,
  currentEndpoint?: Effect.Effect<DeviceHost.DeviceDirectMediaEndpoint | null>,
) {
  const grants = yield* DeviceDirectGrants;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const server = yield* ServerConfig.ServerConfig;
  const net = yield* NetService.NetService;
  const http = yield* HttpClient.HttpClient;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const crypto = yield* Crypto.Crypto;
  const parentScope = yield* Scope.Scope;
  const ssh = yield* resolveSshCommand;
  const owner = yield* ownerFor(config.id);
  const provide = <A, E>(
    effect: Effect.Effect<
      A,
      E,
      | FileSystem.FileSystem
      | Path.Path
      | ChildProcessSpawner.ChildProcessSpawner
      | Crypto.Crypto
      | ServerConfig.ServerConfig
    >,
  ) =>
    effect.pipe(
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provideService(Path.Path, path),
      Effect.provideService(ServerConfig.ServerConfig, server),
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Effect.provideService(Crypto.Crypto, crypto),
    );
  const lock = yield* Semaphore.make(1);
  let stopped = false;
  let activated = false;
  let wantsAgent = false;
  let ready: SshDeviceHostReady | null = null;
  let connectionScope: Scope.Closeable | null = null;
  let retirementFailure: DeviceHost.DeviceHostError | null = null;
  const currentRetirementFailure = (): DeviceHost.DeviceHostError | null => retirementFailure;
  let summary: DeviceHostSummary = {
    id: config.id,
    label: config.label,
    kind: "ssh",
    hubInstalled: false,
    agentDeviceInstalled: false,
    platforms: [],
  };

  const run: DeviceHost.DeviceHostReady["run"] = (command, args, options) =>
    provide(
      runSshCommand(targetFor(config), {
        preHostArgs: identityArgs(config),
        remoteCommandArgs: commandArgs(`exec ${[command, ...args].map(quoteRemoteArg).join(" ")}`),
        ...(options?.stdin === undefined ? {} : { stdin: options.stdin }),
        ...(options?.timeoutMs === undefined ? {} : { timeoutMs: options.timeoutMs }),
      }),
    ).pipe(
      Effect.map((result) => ({ ...result, code: 0 })),
      Effect.catch((error) =>
        Effect.succeed({
          stdout: "stdout" in error ? (error.stdout ?? "") : "",
          stderr: error.message,
          code: "exitCode" in error ? (error.exitCode ?? 127) : 127,
        }),
      ),
    );

  const retire = (scope: Scope.Closeable) =>
    Effect.gen(function* () {
      if (connectionScope === scope) {
        connectionScope = null;
        ready = null;
      }
      yield* Scope.close(scope, Exit.void);
    });
  const connectOnce = Effect.fn("SshDeviceHost.connectOnce")(function* (
    direct: boolean,
  ): Effect.fn.Return<SshDeviceHostReady, DeviceHost.DeviceHostError> {
    activated = true;
    const scope = yield* Scope.make();
    connectionScope = scope;
    const generation = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
    let active = true;
    yield* registerDirectRetirement({
      scope,
      direct,
      retire: provide(bootstrap(config, owner, "stop-direct", generation)),
      deactivate: () => {
        active = false;
      },
      onFailure: (cause) => {
        retirementFailure = new DeviceHost.DeviceHostError({
          hostId: config.id,
          step: "retiring direct gateway",
          cause,
        });
      },
    });
    return yield* Effect.gen(function* () {
      const result = yield* provide(
        bootstrap(
          config,
          owner,
          wantsAgent ? "agent-start" : "start",
          direct ? generation : undefined,
        ),
      );
      yield* onStatus("starting");
      const remote = yield* decodeStarted(result.stdout.trim()).pipe(
        Effect.mapError(
          (cause) =>
            new DeviceHost.DeviceHostError({
              hostId: config.id,
              step: "reading host endpoints",
              cause,
            }),
        ),
      );
      summary = {
        ...summary,
        platforms: remote.platforms,
        tools: remote.tools,
        hubInstalled: true,
        agentDeviceInstalled: wantsAgent || summary.agentDeviceInstalled,
      };
      const reserve = (step: string) =>
        net
          .reserveLoopbackPort("127.0.0.1")
          .pipe(
            Effect.mapError(
              (cause) => new DeviceHost.DeviceHostError({ hostId: config.id, step, cause }),
            ),
          );
      const hubPort = yield* reserve("reserving hub port");
      const daemonPort =
        remote.daemonPort === undefined ? undefined : yield* reserve("reserving daemon port");
      if (direct && (!currentEndpoint || remote.directMedia?.generation !== generation))
        return yield* new DeviceHost.DeviceHostError({
          hostId: config.id,
          step: "reading direct gateway endpoints",
          cause: new Error("Missing direct gateway descriptor or readiness callback."),
        });
      const endpoint =
        direct && remote.directMedia && config.directSshTarget
          ? {
              target: config.directSshTarget,
              gatewayPort: remote.directMedia.gatewayPort,
              owner,
              generation,
            }
          : undefined;
      const gatewayProbePort = endpoint ? yield* reserve("reserving gateway port") : undefined;
      const admission =
        endpoint && currentEndpoint
          ? yield* openDirectAdmission({
              hostId: config.id,
              owner,
              generation,
              gatewayPort: endpoint.gatewayPort,
              currentEndpoint,
              isActive: () => active && !stopped && connectionScope === scope,
            }).pipe(
              Effect.provideService(DeviceDirectGrants, grants),
              Effect.provideService(Scope.Scope, scope),
            )
          : undefined;
      const child = yield* spawner
        .spawn(
          ChildProcess.make(
            ssh,
            [
              ...baseSshArgs(targetFor(config), { batchMode: "yes" }),
              ...identityArgs(config),
              "-o",
              "ControlMaster=no",
              "-o",
              "ControlPath=none",
              "-o",
              "ControlPersist=no",
              "-o",
              "ExitOnForwardFailure=yes",
              "-o",
              "ServerAliveInterval=10",
              "-o",
              "ServerAliveCountMax=3",
              "-N",
              "-L",
              `127.0.0.1:${hubPort}:127.0.0.1:${remote.hubPort}`,
              ...(remote.daemonPort !== undefined
                ? ["-L", `127.0.0.1:${daemonPort}:127.0.0.1:${remote.daemonPort}`]
                : []),
              ...(endpoint && remote.directMedia && admission && gatewayProbePort !== undefined
                ? [
                    "-R",
                    `127.0.0.1:${remote.directMedia.admissionPort}:127.0.0.1:${admission.port}`,
                    "-L",
                    `127.0.0.1:${gatewayProbePort}:127.0.0.1:${endpoint.gatewayPort}`,
                  ]
                : []),
              config.target,
            ],
            { stdin: "ignore", stdout: "ignore", stderr: "pipe" },
          ),
        )
        .pipe(
          Effect.provideService(Scope.Scope, scope),
          Effect.mapError(
            (cause) =>
              new DeviceHost.DeviceHostError({
                hostId: config.id,
                step: "forwarding ports",
                cause,
              }),
          ),
        );
      let stderr = "";
      yield* child.stderr.pipe(
        Stream.decodeText(),
        Stream.runForEach((chunk) =>
          Effect.sync(() => {
            stderr = (stderr + chunk).slice(-2000);
          }),
        ),
        Effect.forkIn(scope),
      );
      const next = {
        nodePath: remote.nodePath,
        hub: { origin: `http://127.0.0.1:${hubPort}` },
        ...(endpoint ? { directMedia: endpoint } : {}),
        ...(remote.daemonPort !== undefined &&
        remote.token !== undefined &&
        remote.entryPath !== undefined
          ? {
              agentDevice: {
                baseUrl: `http://127.0.0.1:${daemonPort}`,
                token: remote.token,
                entryPath: remote.entryPath,
              },
            }
          : {}),
        helpers: remote.helpers,
        run,
      };
      const gatewayHealth = directGatewayHealth(
        http,
        gatewayProbePort,
        owner,
        generation,
        config.id,
      );
      for (const [baseUrl, route] of [
        [next.hub.origin, "/readyz"],
        ...(gatewayProbePort !== undefined
          ? [[`http://127.0.0.1:${gatewayProbePort}`, "/readyz"]]
          : []),
        ...(next.agentDevice ? [[next.agentDevice.baseUrl, "/health"]] : []),
      ])
        yield* waitForHttpReady({
          baseUrl: baseUrl!,
          path: route!,
          timeoutMs: 15000,
          makeError: () =>
            new DeviceHost.DeviceHostError({
              hostId: config.id,
              step: "waiting for SSH forward",
              cause: new Error(stderr || "Forwarded endpoint did not answer."),
            }),
        }).pipe(Effect.provideService(HttpClient.HttpClient, http));
      if (!(yield* gatewayHealth))
        return yield* new DeviceHost.DeviceHostError({
          hostId: config.id,
          step: "checking direct gateway generation",
          cause: new Error("Gateway owner or generation mismatch."),
        });
      if (next.agentDevice) yield* onReady({ ...next, agentDevice: next.agentDevice });
      ready = next;
      yield* onStatus("ready");
      const unhealthy = Effect.gen(function* () {
        while (true) {
          yield* Effect.sleep("10 seconds");
          const alive = yield* http.get(`${next.hub.origin}/readyz`).pipe(
            Effect.timeout("5 seconds"),
            Effect.map((r) => r.status === 200),
            Effect.orElseSucceed(() => false),
          );
          const daemonAlive = next.agentDevice
            ? yield* http.get(`${next.agentDevice.baseUrl}/health`).pipe(
                Effect.timeout("5 seconds"),
                Effect.map((r) => r.status === 200),
                Effect.orElseSucceed(() => false),
              )
            : true;
          if (!alive || !daemonAlive || !(yield* gatewayHealth)) return;
        }
      });
      yield* Effect.gen(function* () {
        yield* Effect.raceFirst(child.exitCode.pipe(Effect.ignore), unhealthy);
        if (stopped || connectionScope !== scope) return;
        yield* lock.withPermit(
          Effect.gen(function* () {
            if (stopped || connectionScope !== scope) return;
            active = false;
            yield* retire(scope);
            yield* onStatus("starting", "Reconnecting to device host…");
          }),
        );
        let delay = 1000;
        const needsReconnect = () => !stopped && !ready;
        while (needsReconnect()) {
          yield* Effect.sleep(delay);
          const result = yield* lock
            .withPermit(
              Effect.suspend(() =>
                stopped || ready ? Effect.void : connect().pipe(Effect.asVoid),
              ),
            )
            .pipe(Effect.result);
          if (result._tag === "Success") return;
          yield* onStatus("failed", result.failure.message);
          if (retirementFailure) return;
          delay = Math.min(delay * 2, 30000);
        }
      }).pipe(Effect.forkIn(parentScope));
      return next;
    }).pipe(
      Effect.onExit((exit) =>
        exit._tag === "Failure"
          ? Effect.gen(function* () {
              active = false;
              yield* retire(scope);
            })
          : Effect.void,
      ),
    );
  });

  const connect = Effect.fn("SshDeviceHost.connect")(function* (): Effect.fn.Return<
    SshDeviceHostReady,
    DeviceHost.DeviceHostError
  > {
    const direct = Boolean(config.directSshTarget && currentEndpoint);
    const previousRetirementFailure = currentRetirementFailure();
    if (previousRetirementFailure) return yield* previousRetirementFailure;
    for (let attempt = 0; ; attempt++) {
      const result = yield* connectOnce(direct).pipe(Effect.result);
      if (result._tag === "Success") return result.success;
      const failedRetirement = currentRetirementFailure();
      if (failedRetirement) return yield* failedRetirement;
      // Reservation races can retry only after the captured generation is confirmed retired.
      if (
        attempt < 2 &&
        ["forwarding ports", "waiting for SSH forward"].includes(result.failure.step)
      )
        continue;
      if (direct) return yield* connectOnce(false);
      return yield* result.failure;
    }
  });

  const ensureReady: DeviceHost.DeviceHost["Service"]["ensureReady"] = (onPhase) =>
    lock.withPermit(
      Effect.gen(function* () {
        stopped = false;
        if (ready) return ready;
        summary = yield* provide(probe(config, owner));
        yield* onPhase(
          summary.hubInstalled ? "starting" : "installing",
          summary.hubInstalled
            ? undefined
            : deviceToolInstallMessage("device hub", summary.tools?.hub),
        );
        return yield* connect().pipe(
          Effect.tapError(() => (connectionScope ? retire(connectionScope) : Effect.void)),
        );
      }),
    );
  const stop = lock.withPermit(
    Effect.gen(function* () {
      stopped = true;
      ready = null;
      if (connectionScope) yield* retire(connectionScope);
      connectionScope = null;
      if (activated) yield* provide(bootstrap(config, owner, "stop")).pipe(Effect.ignore);
      activated = false;
      wantsAgent = false;
    }),
  );
  const changeAgent = (enabled: boolean) =>
    lock.withPermit(
      Effect.gen(function* () {
        wantsAgent = enabled;
        if (enabled && ready?.agentDevice) return { ...ready, agentDevice: ready.agentDevice };
        if (!enabled && !ready?.agentDevice) return null;
        ready = null;
        const previousScope = connectionScope;
        connectionScope = null;
        if (previousScope) yield* retire(previousScope);
        if (!enabled) yield* provide(bootstrap(config, owner, "stop-agent"));
        return yield* connect().pipe(
          Effect.onError(() =>
            Effect.gen(function* () {
              const failedScope = connectionScope;
              connectionScope = null;
              if (failedScope) yield* retire(failedScope);
              if (enabled)
                yield* provide(bootstrap(config, owner, "stop-agent")).pipe(Effect.ignore);
            }),
          ),
        );
      }),
    );
  yield* Effect.addFinalizer(() => stop);
  const host: DeviceHost.DeviceHost["Service"] = {
    id: config.id,
    summary: Effect.sync(() => summary),
    inspect: provide(probe(config, owner)).pipe(
      Effect.tap((value) =>
        Effect.sync(() => {
          summary = value;
        }),
      ),
    ),
    current: Effect.sync(() => ready),
    ensureReady,
    ensureAgentReady: (onPhase) =>
      ensureReady(onPhase).pipe(
        Effect.flatMap(() =>
          onPhase(
            summary.agentDeviceInstalled ? "starting" : "installing",
            summary.agentDeviceInstalled
              ? undefined
              : deviceToolInstallMessage("agent tools", summary.tools?.agent),
          ),
        ),
        Effect.flatMap(() => changeAgent(true)),
        Effect.flatMap((value) =>
          value?.agentDevice
            ? Effect.succeed({ ...value, agentDevice: value.agentDevice })
            : Effect.fail(
                new DeviceHost.DeviceHostError({
                  hostId: config.id,
                  step: "starting agent tools",
                  cause: new Error("Daemon endpoint missing"),
                }),
              ),
        ),
      ),
    stopAgent: changeAgent(false).pipe(Effect.asVoid, Effect.ignore),
    stop,
    platformAvailability: (platform) =>
      provide(probe(config, owner)).pipe(
        Effect.map((value) => {
          summary = { ...summary, platforms: value.platforms };
          return value.platforms.find((p) => p.platform === platform)!;
        }),
        Effect.orElseSucceed(() => ({
          platform,
          available: false,
          reason: "Cannot reach device host. Test its SSH connection in Settings.",
        })),
      ),
  };
  return host;
});
