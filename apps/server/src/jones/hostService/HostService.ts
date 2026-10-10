import { ExecutionEnvironmentDescriptor } from "@t3tools/contracts";
import {
  HostProcessArchitecture,
  HostProcessPlatform,
  HostProcessUserId,
} from "@t3tools/shared/hostProcess";
import * as Net from "@t3tools/shared/Net";
import {
  buildTailscaleHttpsBaseUrl,
  DEFAULT_TAILSCALE_SERVE_PORT,
  queryServeMapping,
  readTailscaleStatus,
  releaseServeMapping,
  type ServeMappingState,
} from "@t3tools/tailscale";
import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http";
import { ChildProcessSpawner } from "effect/process";

import packageJson from "../../../package.json" with { type: "json" };
import * as BootService from "../../cloud/bootService.ts";
import * as PinnedRuntime from "../../cloud/pinnedRuntime.ts";
import { serviceStateActiveVersion } from "../../cloud/serviceProtocol.ts";
import { jonesBootServiceLayer, reconcileService } from "../../cli/service.ts";
import * as ProcessRunner from "../../processRunner.ts";
import { isProcessAlive, PersistedServerRuntimeState } from "../../serverRuntimeState.ts";
import * as HostServiceConfig from "./HostServiceConfig.ts";
import * as HostAdoption from "./adoption.ts";
import { JONES_BOOT_SERVICE_IDENTITY } from "./identity.ts";
import {
  decodeJonesArtifactMetadata,
  type JonesRuntimeProvenance,
} from "./artifactVerification.ts";
import {
  assertPrivateServiceRuntimeOwnership,
  verifyPrivateServiceRuntimeCache,
} from "./privateRuntime.ts";

export class HostServiceError extends Schema.TaggedError<HostServiceError>()("HostServiceError", {
  operation: Schema.String,
  reason: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return `Jones host ${this.operation}: ${this.reason}`;
  }
}
const isHostServiceError = Schema.is(HostServiceError);
const decodeRuntimeState = Schema.decodeUnknownEffect(
  Schema.fromJsonString(PersistedServerRuntimeState),
);

const BaseInput = Schema.Struct({ baseDir: Schema.String });
const Port = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 65535 }));
const SetupInputSchema = Schema.Struct({
  ...BaseInput.fields,
  port: Port,
  artifactDir: Schema.optional(Schema.String),
  expectSourceCommit: Schema.optional(Schema.String),
  tailscaleServePort: Schema.optional(Port),
  allowLingerEnable: Schema.optional(Schema.Boolean),
  dryRun: Schema.optional(Schema.Boolean),
});
const decodeSetupInput = Schema.decodeUnknownEffect(SetupInputSchema);
export type SetupInput = typeof SetupInputSchema.Type;
export interface StageInput {
  readonly baseDir: string;
  readonly artifactDir: string;
  readonly expectSourceCommit: string;
}
export interface HostPlan {
  readonly baseDir: string;
  readonly config: HostServiceConfig.HostServiceConfigData;
  readonly effects: ReadonlyArray<string>;
  readonly nextStep: string;
}
export interface HostStatus {
  readonly baseDir: string;
  readonly activeVersion?: string;
  readonly provenance: "verified" | "unverified" | "missing";
  readonly runtimeProvenance?: JonesRuntimeProvenance;
  readonly config: HostServiceConfig.HostServiceConfigData | "missing" | "unknown";
  readonly service: BootService.BootServiceStatus | "unknown";
  readonly serviceProblems: ReadonlyArray<string>;
  readonly runtime:
    | (PersistedServerRuntimeState & { readonly environmentId?: string })
    | "missing"
    | "stopped"
    | "unknown";
  readonly serve: ServeMappingState | "not-configured";
  readonly serveEndpoint: {
    readonly url?: string;
    readonly environmentMatch: boolean | "unknown";
    readonly scope: "host-side-only";
  };
  readonly foreignUpstreamUnit: {
    readonly present: boolean | "unknown";
    readonly baseDir?: string;
    readonly managed: false;
  };
  readonly mac?: {
    readonly consoleUser: "gui-login-present" | "gui-login-required" | "unknown";
    readonly loaded: boolean | "unknown";
    readonly running: boolean | "unknown";
    readonly autoLogin: "yes" | "no" | "unknown";
    readonly sleep: Readonly<Record<string, number>> | "unknown";
  };
}

const ProcessAlive = Context.Reference<(pid: number) => boolean>("jones/hostService/ProcessAlive", {
  defaultValue: () => isProcessAlive,
});

export class HostService extends Context.Service<
  HostService,
  {
    readonly planAdoption: (
      input: HostAdoption.AdoptInput,
    ) => Effect.Effect<HostAdoption.AdoptionPlan, HostServiceError>;
    readonly adopt: (
      input: HostAdoption.AdoptInput,
    ) => Effect.Effect<
      { readonly state: "dry-run" | "adopted"; readonly plan: HostAdoption.AdoptionPlan },
      HostServiceError
    >;
    readonly stageRuntime: (
      input: StageInput,
    ) => Effect.Effect<PinnedRuntime.PinnedRuntimePaths, HostServiceError>;
    readonly plan: (input: SetupInput) => Effect.Effect<HostPlan, HostServiceError>;
    readonly setup: (input: SetupInput) => Effect.Effect<
      {
        readonly state: "dry-run" | "installed" | "installed-awaiting-gui-login";
        readonly plan: HostPlan;
        readonly warnings: ReadonlyArray<string>;
        readonly status?: HostStatus;
      },
      HostServiceError
    >;
    readonly status: (input: {
      readonly baseDir: string;
    }) => Effect.Effect<HostStatus, HostServiceError>;
    readonly routeRemove: (input: {
      readonly baseDir: string;
      readonly dryRun?: boolean;
    }) => Effect.Effect<
      {
        readonly state: "dry-run" | "absent" | "removed";
        readonly mapping: ServeMappingState;
      },
      HostServiceError
    >;
  }
>()("t3/jones/hostService/HostService") {}

const make = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const config = yield* HostServiceConfig.HostServiceConfig;
  const runner = yield* ProcessRunner.ProcessRunner;
  const net = yield* Net.NetService;
  const client = yield* HttpClient.HttpClient;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const platform = yield* HostProcessPlatform;
  const arch = yield* HostProcessArchitecture;
  const uid = yield* HostProcessUserId;
  const alive = yield* ProcessAlive;
  const home = yield* Config.String("HOME").pipe(Config.withDefault(""));
  const context = yield* Effect.context<
    FileSystem.FileSystem | Path.Path | ProcessRunner.ProcessRunner | HttpClient.HttpClient
  >();
  const bounded = (command: string, args: ReadonlyArray<string>) =>
    runner
      .run({
        command,
        args,
        timeout: "3 seconds",
        maxOutputBytes: 32 * 1024,
      })
      .pipe(Effect.option);
  const validOutput = (output: Option.Option<ProcessRunner.ProcessRunOutput>) =>
    Option.isSome(output) &&
    output.value.code === 0 &&
    !output.value.timedOut &&
    !output.value.stdoutTruncated &&
    !output.value.stdoutInvalidUtf8;
  const validateBase = (baseDir: string) =>
    path.isAbsolute(baseDir)
      ? Effect.void
      : Effect.fail(
          new HostServiceError({
            operation: "validate",
            reason: "--base-dir must be an explicit absolute directory.",
          }),
        );
  const wrap = <A, E>(operation: string, effect: Effect.Effect<A, E>) =>
    effect.pipe(
      Effect.mapError((cause) =>
        isHostServiceError(cause)
          ? cause
          : new HostServiceError({
              operation,
              reason: "Operation failed; inspect host status before retrying.",
              cause,
            }),
      ),
    );
  const bootLayer = (baseDir: string, allowEnableLinger = false) =>
    jonesBootServiceLayer({
      baseDir,
      logsDir: path.join(baseDir, "userdata", "logs"),
      cliVersion: packageJson.version,
      allowEnableLinger,
      runtimeMode: "verified-private-artifact",
    });
  const bootStatus = (baseDir: string) =>
    Effect.flatMap(BootService.BootService, (service) => service.status).pipe(
      Effect.provide(bootLayer(baseDir)),
      Effect.provide(context),
    );
  const readOptional = (filePath: string) =>
    fs.readFileString(filePath).pipe(
      Effect.map(Option.some),
      Effect.catchIf(
        (cause) => cause.reason._tag === "NotFound",
        () => Effect.succeed(Option.none<string>()),
      ),
    );
  const readRuntime = (baseDir: string) =>
    Effect.gen(function* () {
      let stopped = false;
      for (const stateDir of ["userdata", "dev"]) {
        const text = yield* readOptional(path.join(baseDir, stateDir, "server-runtime.json"));
        if (Option.isNone(text)) continue;
        const state = yield* decodeRuntimeState(text.value);
        if (alive(state.pid)) return { state: Option.some(state), stopped: false };
        stopped = true;
      }
      return { state: Option.none<PersistedServerRuntimeState>(), stopped };
    });
  const descriptor = (baseUrl: string) =>
    client
      .execute(HttpClientRequest.get(new URL("/.well-known/t3/environment", baseUrl).toString()))
      .pipe(
        Effect.flatMap(HttpClientResponse.filterStatusOk),
        Effect.flatMap(HttpClientResponse.schemaBodyJson(ExecutionEnvironmentDescriptor)),
        Effect.timeout("3 seconds"),
        Effect.option,
      );
  const claim = (data: HostServiceConfig.HostServiceConfigData) => ({
    servePort: data.tailscaleServePort!,
    expectedTarget: `http://127.0.0.1:${data.port}`,
  });
  const mapping = (data: HostServiceConfig.HostServiceConfigData) =>
    queryServeMapping(claim(data)).pipe(
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    );
  const consoleLogin = Effect.gen(function* () {
    const result = yield* bounded("stat", ["-f", "%Su", "/dev/console"]);
    if (!validOutput(result) || Option.isNone(result)) return "unknown" as const;
    const user = result.value.stdout.trim();
    return user === "root" || user === "loginwindow"
      ? ("gui-login-required" as const)
      : user.length > 0
        ? ("gui-login-present" as const)
        : ("unknown" as const);
  });
  const provenance = (baseDir: string, version: string) =>
    PinnedRuntime.verifyPinnedRuntimeProvenance({
      paths: PinnedRuntime.pinnedRuntimePaths(path, baseDir, version, platform),
      version,
      fs,
      path,
      platform,
      arch,
    });

  const adoptionHost = () =>
    Effect.gen(function* () {
      if (
        (platform !== "linux" && platform !== "darwin") ||
        (arch !== "x64" && arch !== "arm64") ||
        uid === undefined
      )
        return yield* new HostServiceError({
          operation: "adopt",
          reason: "Native host platform or service-user identity is unavailable.",
        });
      const runtimeContext = yield* Effect.context<never>();
      const runPromise = Effect.runPromiseWith(runtimeContext);
      return {
        platform,
        architecture: arch,
        home,
        uid,
        environmentPath: yield* Config.String("PATH").pipe(Config.withDefault("/usr/bin:/bin")),
        run: async (command: HostAdoption.AdoptionCommand) => {
          const result = await runPromise(
            runner.run({ ...command, timeout: "5 minutes", maxOutputBytes: 128 * 1024 }),
          );
          if (
            result.code !== 0 ||
            result.timedOut ||
            result.stdoutTruncated ||
            result.stdoutInvalidUtf8
          )
            throw new HostAdoption.HostAdoptionError(
              "A bounded host command failed or has an unknown effect; inspect before retry.",
            );
          return result.stdout;
        },
        readback: async (expected: {
          readonly baseDir: string;
          readonly activeVersion: string;
          readonly environmentId: string;
        }) => {
          return await runPromise(
            Effect.gen(function* () {
              const observed = yield* readRuntime(expected.baseDir);
              if (Option.isNone(observed.state))
                return yield* new HostServiceError({
                  operation: "adopt-readback",
                  reason: "New service-managed runtime has not produced readback.",
                });
              const state = observed.state.value;
              const environment = yield* descriptor(`http://127.0.0.1:${state.port}`);
              if (Option.isNone(environment))
                return yield* new HostServiceError({
                  operation: "adopt-readback",
                  reason: "Native environment descriptor is unavailable.",
                });
              return {
                processId: state.pid,
                serviceManaged: state.serviceManaged === true,
                port: state.port,
                serverVersion: environment.value.serverVersion,
                environmentId: environment.value.environmentId,
              };
            }),
          );
        },
      } satisfies HostAdoption.AdoptionHost;
    });
  const planAdoption: HostService["Service"]["planAdoption"] = (input) =>
    wrap(
      "adopt-plan",
      Effect.gen(function* () {
        const host = yield* adoptionHost();
        return yield* Effect.tryPromise({
          try: () => HostAdoption.planHostAdoption(input, host),
          catch: (cause) =>
            new HostServiceError({
              operation: "adopt-plan",
              reason: cause instanceof Error ? cause.message : "Qualification is unavailable.",
              cause,
            }),
        });
      }),
    );
  const adopt: HostService["Service"]["adopt"] = (input) =>
    wrap(
      "adopt",
      Effect.gen(function* () {
        const host = yield* adoptionHost();
        return yield* Effect.tryPromise({
          try: () => HostAdoption.adoptHost(input, host),
          catch: (cause) =>
            new HostServiceError({
              operation: "adopt",
              reason:
                cause instanceof Error
                  ? cause.message
                  : "Effect requires reconciliation before retry.",
              cause,
            }),
        });
      }),
    );

  const stageRuntime: HostService["Service"]["stageRuntime"] = (input) =>
    wrap(
      "stage-runtime",
      Effect.gen(function* () {
        yield* validateBase(input.baseDir);
        const metadata = yield* decodeJonesArtifactMetadata(
          yield* fs.readFileString(path.join(input.artifactDir, "ARTIFACT.json")),
        );
        const cacheInput = {
          baseDir: input.baseDir,
          version: metadata.version,
          fs,
          path,
          runner,
          platform,
          arch,
        };
        const activeVersion = yield* assertPrivateServiceRuntimeOwnership(cacheInput);
        if (activeVersion !== undefined) {
          yield* verifyPrivateServiceRuntimeCache({ ...cacheInput, version: activeVersion });
        }
        return yield* PinnedRuntime.installPinnedRuntimeFromLocalArchive({
          ...input,
          fs,
          path,
          runner,
          platform,
          arch,
          validate: () => Effect.void,
        });
      }),
    );

  const plan: HostService["Service"]["plan"] = (input) =>
    wrap(
      "plan",
      Effect.gen(function* () {
        yield* decodeSetupInput(input);
        yield* validateBase(input.baseDir);
        if ((input.artifactDir === undefined) !== (input.expectSourceCommit === undefined)) {
          return yield* new HostServiceError({
            operation: "plan",
            reason: "--artifact-dir and --expect-source-commit must be supplied together.",
          });
        }
        const persisted = yield* config.read(input.baseDir);
        const tailscaleServePort =
          input.tailscaleServePort ??
          Option.getOrUndefined(persisted)?.tailscaleServePort ??
          DEFAULT_TAILSCALE_SERVE_PORT;
        const data = { schema: 1, port: input.port, tailscaleServePort } as const;
        const quotedBase = `'${input.baseDir.replaceAll("'", "'\\''")}'`;
        return {
          baseDir: input.baseDir,
          config: data,
          effects: [
            ...(input.artifactDir === undefined
              ? []
              : [`Stage verified local Jones runtime ${packageJson.version}.`]),
            `Write ${path.join(input.baseDir, "jones", "host-service.json")} (loopback port ${input.port}, Serve claim ${tailscaleServePort}).`,
            `Reconcile ${platform === "darwin" ? JONES_BOOT_SERVICE_IDENTITY.launchdLabel : JONES_BOOT_SERVICE_IDENTITY.systemdUnitFile} for this base directory.`,
            ...(input.allowLingerEnable
              ? ["Allow enabling Linux user lingering if disabled."]
              : []),
            "Read back config and service status. Pairing and the Serve mapping are a separate operator step.",
          ],
          nextStep: `t3 pair --base-dir ${quotedBase} --tailscale --tailscale-serve-port ${tailscaleServePort}`,
        };
      }),
    );

  const status: HostService["Service"]["status"] = (input) =>
    wrap(
      "status",
      Effect.gen(function* () {
        yield* validateBase(input.baseDir);
        const persisted = yield* config.read(input.baseDir).pipe(Effect.option);
        const data = Option.isSome(persisted) ? Option.getOrUndefined(persisted.value) : undefined;
        const serviceResult = yield* bootStatus(input.baseDir).pipe(Effect.option);
        const service = Option.getOrUndefined(serviceResult);
        const stateText = yield* readOptional(
          path.join(input.baseDir, "runtime", "service-state.json"),
        ).pipe(Effect.option);
        const activeVersion =
          Option.isSome(stateText) && Option.isSome(stateText.value)
            ? serviceStateActiveVersion(stateText.value.value)
            : service?.installedVersion;
        const verification =
          activeVersion === undefined
            ? Option.none<JonesRuntimeProvenance>()
            : yield* provenance(input.baseDir, activeVersion).pipe(Effect.option);
        const runtimeResult = yield* readRuntime(input.baseDir).pipe(Effect.option);
        const runtimeState = Option.isSome(runtimeResult)
          ? Option.getOrUndefined(runtimeResult.value.state)
          : undefined;
        const localDescriptor =
          runtimeState === undefined
            ? Option.none<ExecutionEnvironmentDescriptor>()
            : yield* descriptor(`http://127.0.0.1:${runtimeState.port}`);
        const serve =
          data?.tailscaleServePort === undefined
            ? ("not-configured" as const)
            : yield* mapping(data);
        let url: string | undefined;
        let environmentMatch: boolean | "unknown" = "unknown";
        if (data?.tailscaleServePort !== undefined) {
          const tailscale = yield* readTailscaleStatus.pipe(
            Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
            Effect.option,
          );
          if (Option.isSome(tailscale) && tailscale.value.magicDnsName !== null) {
            url = buildTailscaleHttpsBaseUrl({
              magicDnsName: tailscale.value.magicDnsName,
              servePort: data.tailscaleServePort,
            });
            const remoteDescriptor = yield* descriptor(url);
            if (Option.isSome(localDescriptor) && Option.isSome(remoteDescriptor))
              environmentMatch =
                localDescriptor.value.environmentId === remoteDescriptor.value.environmentId;
          }
        }
        const foreignPath =
          platform === "darwin"
            ? path.join(home, "Library", "LaunchAgents", "com.t3tools.t3code.service.plist")
            : path.join(home, ".config", "systemd", "user", "t3code.service");
        const foreign = yield* readOptional(foreignPath).pipe(Effect.option);
        const foreignBase =
          Option.isSome(foreign) && Option.isSome(foreign.value)
            ? BootService.bootServiceBaseDirOf(foreign.value.value)
            : undefined;
        let mac: HostStatus["mac"];
        if (platform === "darwin") {
          const login = yield* consoleLogin;
          const result =
            uid === undefined
              ? Option.none<ProcessRunner.ProcessRunOutput>()
              : yield* bounded("launchctl", [
                  "print",
                  `gui/${uid}/${JONES_BOOT_SERVICE_IDENTITY.launchdLabel}`,
                ]);
          const autoLoginResult = yield* bounded("defaults", [
            "read",
            "/Library/Preferences/com.apple.loginwindow",
            "autoLoginUser",
          ]);
          const autoLogin =
            validOutput(autoLoginResult) && Option.isSome(autoLoginResult)
              ? autoLoginResult.value.stdout.trim().length > 0
                ? ("yes" as const)
                : ("no" as const)
              : Option.isSome(autoLoginResult) &&
                  !autoLoginResult.value.timedOut &&
                  !autoLoginResult.value.stderrTruncated &&
                  !autoLoginResult.value.stderrInvalidUtf8 &&
                  /does not exist/i.test(autoLoginResult.value.stderr)
                ? ("no" as const)
                : ("unknown" as const);
          const sleepResult = yield* bounded("pmset", ["-g", "custom"]);
          const sleepSettings: Record<string, number> = {};
          if (validOutput(sleepResult) && Option.isSome(sleepResult)) {
            let powerSource: string | undefined;
            for (const line of sleepResult.value.stdout.split("\n")) {
              const heading = /^\s*(AC Power|Battery Power|UPS Power):\s*$/.exec(line);
              if (heading?.[1] !== undefined) powerSource = heading[1];
              const setting = /^\s*sleep\s+(\d+)\s*$/.exec(line);
              if (setting?.[1] !== undefined && powerSource !== undefined)
                sleepSettings[powerSource] = Number(setting[1]);
            }
          }
          mac = {
            consoleUser: login,
            loaded: validOutput(result) ? true : "unknown",
            running:
              validOutput(result) && Option.isSome(result)
                ? /\bstate = running\b/.test(result.value.stdout) ||
                  /\bpid = \d+\b/.test(result.value.stdout)
                : "unknown",
            autoLogin,
            sleep: Object.keys(sleepSettings).length > 0 ? sleepSettings : "unknown",
          };
        }
        return {
          baseDir: input.baseDir,
          ...(activeVersion === undefined ? {} : { activeVersion }),
          provenance: Option.isSome(verification)
            ? "verified"
            : activeVersion === undefined
              ? "missing"
              : "unverified",
          ...(Option.isSome(verification) ? { runtimeProvenance: verification.value } : {}),
          config: data ?? (Option.isSome(persisted) ? "missing" : "unknown"),
          service: service ?? "unknown",
          serviceProblems: (service?.problems ?? []).map((problem) =>
            BootService.formatBootServiceProblem(problem).replaceAll(
              "t3code.service",
              "jones-code.service",
            ),
          ),
          runtime:
            runtimeState === undefined
              ? Option.isSome(runtimeResult)
                ? runtimeResult.value.stopped
                  ? "stopped"
                  : "missing"
                : "unknown"
              : {
                  ...runtimeState,
                  ...(Option.isSome(localDescriptor)
                    ? { environmentId: localDescriptor.value.environmentId }
                    : {}),
                },
          serve,
          serveEndpoint: {
            ...(url === undefined ? {} : { url }),
            environmentMatch,
            scope: "host-side-only",
          },
          foreignUpstreamUnit: {
            present: Option.isNone(foreign) ? "unknown" : Option.isSome(foreign.value),
            ...(foreignBase === undefined ? {} : { baseDir: foreignBase }),
            managed: false,
          },
          ...(mac === undefined ? {} : { mac }),
        };
      }),
    );

  const setup: HostService["Service"]["setup"] = (input) =>
    wrap(
      "setup",
      Effect.gen(function* () {
        const effectPlan = yield* plan(input);
        if (input.dryRun) return { state: "dry-run", plan: effectPlan, warnings: [] };
        const before = yield* bootStatus(input.baseDir);
        const privateRuntimeInput = {
          baseDir: input.baseDir,
          version: packageJson.version,
          activeVersion: before.installedVersion,
          fs,
          path,
          runner,
          platform,
          arch,
        };
        yield* assertPrivateServiceRuntimeOwnership(privateRuntimeInput);
        if (
          before.installedVersion !== undefined &&
          before.installedVersion !== packageJson.version
        ) {
          yield* verifyPrivateServiceRuntimeCache({
            ...privateRuntimeInput,
            version: before.installedVersion,
          });
        }
        if (!before.supported)
          return yield* new HostServiceError({
            operation: "setup",
            reason: "The Jones user service is unsupported on this host.",
          });
        if (
          before.installed &&
          (before.installedBaseDir === undefined ||
            path.resolve(before.installedBaseDir) !== path.resolve(input.baseDir))
        ) {
          return yield* new HostServiceError({
            operation: "setup",
            reason:
              "The installed Jones unit serves a different or unknown base directory; preserve it.",
          });
        }
        const live = yield* readRuntime(input.baseDir);
        const liveState = Option.getOrUndefined(live.state);
        if (liveState !== undefined && (liveState.serviceManaged !== true || !before.installed)) {
          return yield* new HostServiceError({
            operation: "setup",
            reason: "A live server on this base is not owned by this Jones service.",
          });
        }
        const free = yield* net.isPortAvailableOnLoopback(input.port);
        if (
          !free &&
          (liveState === undefined ||
            liveState.port !== input.port ||
            liveState.serviceManaged !== true ||
            Option.isNone(yield* descriptor(`http://127.0.0.1:${input.port}`)))
        ) {
          return yield* new HostServiceError({
            operation: "setup",
            reason: "The requested loopback port is busy or its owner is unverified.",
          });
        }
        if (platform === "linux") {
          const linger = yield* bounded("loginctl", [
            "show-user",
            ...(uid === undefined ? [] : [String(uid)]),
            "--property=Linger",
            "--value",
          ]);
          if (
            !validOutput(linger) ||
            Option.isNone(linger) ||
            !["yes", "no"].includes(linger.value.stdout.trim())
          ) {
            return yield* new HostServiceError({
              operation: "setup",
              reason: "Linux linger state is unknown; inspect it before setup.",
            });
          }
          if (linger.value.stdout.trim() === "no" && input.allowLingerEnable !== true) {
            return yield* new HostServiceError({
              operation: "setup",
              reason:
                "Linux lingering is disabled; use --allow-linger-enable only with approval for that effect.",
            });
          }
        }
        const warnings: string[] = [];
        const serve = yield* mapping(effectPlan.config);
        if (serve._tag === "conflicting")
          return yield* new HostServiceError({
            operation: "setup",
            reason: `The recorded Serve port has a conflicting mapping (${serve.reason}); preserve it.`,
          });
        if (serve._tag === "unknown")
          warnings.push(
            "Serve configuration is unknown. Setup makes no route change; inspect it before pairing.",
          );
        if (input.artifactDir !== undefined && input.expectSourceCommit !== undefined) {
          const artifact = yield* PinnedRuntime.readLocalJonesArtifact({
            artifactDir: input.artifactDir,
            expectSourceCommit: input.expectSourceCommit,
            fs,
            path,
            platform,
            arch,
          });
          if (artifact.metadata.version !== packageJson.version)
            return yield* new HostServiceError({
              operation: "setup",
              reason:
                "The artifact version must match this CLI. Run setup using the approved artifact's CLI.",
            });
          yield* stageRuntime({
            baseDir: input.baseDir,
            artifactDir: input.artifactDir,
            expectSourceCommit: input.expectSourceCommit,
          });
        } else {
          yield* provenance(input.baseDir, packageJson.version);
          const runtimePaths = PinnedRuntime.pinnedRuntimePaths(
            path,
            input.baseDir,
            packageJson.version,
            platform,
          );
          const sentinel = yield* fs.readFileString(runtimePaths.sentinelPath);
          if (sentinel.trim() !== packageJson.version)
            return yield* new HostServiceError({
              operation: "setup",
              reason:
                "Runtime installation is incomplete; run t3 jones host stage-runtime with an approved artifact.",
            });
        }
        yield* verifyPrivateServiceRuntimeCache(privateRuntimeInput);
        const awaitingLogin =
          platform === "darwin" && (yield* consoleLogin) === "gui-login-required";
        yield* config.write(input.baseDir, effectPlan.config);
        yield* reconcileService({ start: !awaitingLogin }).pipe(
          Effect.provide(bootLayer(input.baseDir, input.allowLingerEnable === true)),
          Effect.provide(context),
        );
        const readbackConfig = yield* config.read(input.baseDir);
        if (
          Option.isNone(readbackConfig) ||
          readbackConfig.value.port !== effectPlan.config.port ||
          readbackConfig.value.tailscaleServePort !== effectPlan.config.tailscaleServePort
        ) {
          return yield* new HostServiceError({
            operation: "setup",
            reason:
              "Config readback changed; effect unknown. Run t3 jones host status before retrying.",
          });
        }
        const readback = yield* status({ baseDir: input.baseDir });
        if (
          readback.service === "unknown" ||
          !readback.service.installed ||
          readback.service.installedBaseDir !== input.baseDir ||
          readback.provenance !== "verified"
        ) {
          return yield* new HostServiceError({
            operation: "setup",
            reason:
              "Service readback did not confirm this Jones install; effect unknown. Run t3 jones host status before retrying.",
          });
        }
        if (!awaitingLogin && !readback.service.current) {
          return yield* new HostServiceError({
            operation: "setup",
            reason:
              "Service readback reports an incomplete reconciliation. Run t3 jones host status before retrying.",
          });
        }
        return {
          state: awaitingLogin ? "installed-awaiting-gui-login" : "installed",
          plan: effectPlan,
          warnings,
          status: readback,
        };
      }),
    );

  const routeRemove: HostService["Service"]["routeRemove"] = (input) =>
    wrap(
      "route-remove",
      Effect.gen(function* () {
        yield* validateBase(input.baseDir);
        const data = yield* config.read(input.baseDir);
        if (Option.isNone(data) || data.value.tailscaleServePort === undefined) {
          return yield* new HostServiceError({
            operation: "route-remove",
            reason: "No persisted Serve claim exists for this base directory.",
          });
        }
        const before = yield* mapping(data.value);
        if (input.dryRun) return { state: "dry-run", mapping: before };
        if (before._tag === "absent") return { state: "absent", mapping: before };
        if (before._tag !== "exact")
          return yield* new HostServiceError({
            operation: "route-remove",
            reason: `Serve state is ${before._tag}; no mapping was removed. Run t3 jones host status before retrying.`,
          });
        const result = yield* releaseServeMapping({ ...claim(data.value), created: true }).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        );
        if (result !== "disabled")
          return yield* new HostServiceError({
            operation: "route-remove",
            reason: "Effect unknown; run t3 jones host status before retrying.",
          });
        return { state: "removed", mapping: { _tag: "absent" } };
      }),
    );

  return HostService.of({ planAdoption, adopt, stageRuntime, plan, setup, status, routeRemove });
});

export const layer = Layer.effect(HostService, make);
