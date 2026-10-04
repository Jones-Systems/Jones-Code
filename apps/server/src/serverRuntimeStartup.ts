import packageJson from "../package.json" with { type: "json" };
import { awaitJonesTrialCommit } from "./jonesUpdates/trialGate.ts";
import * as NodeCrypto from "node:crypto";
export const nativeCreationBootId = NodeCrypto.randomUUID();
import {
  CommandId,
  DEFAULT_MODEL,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_SERVER_SETTINGS,
  type ModelSelection,
  type Project,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import { HostProcessEnvironment } from "@t3tools/shared/hostProcess";
import * as Cause from "effect/Cause";
import * as Console from "effect/Console";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as HttpServer from "effect/unstable/http/HttpServer";

import * as ServerConfig from "./config.ts";
import * as ServiceLauncherClient from "./cloud/serviceLauncherClient.ts";
import { decodeServiceLauncherContext, SERVICE_LAUNCHER_CONTEXT_ENV } from "./cloud/serviceProtocol.ts";
import { flushCompileCache } from "./compileCache.ts";
import * as Keybindings from "./keybindings.ts";
import * as ExternalLauncher from "./process/externalLauncher.ts";
import * as EffectWorker from "./orchestration-v2/EffectWorker.ts";
import type { RestartContinuationMarkerV2 } from "./orchestration-v2/EventSink.ts";
import * as LegacyV1ThreadImporter from "./orchestration-v2/legacy/LegacyV1ThreadImporter.ts";
import * as ProviderRuntimeRecovery from "./orchestration-v2/ProviderRuntimeRecoveryService.ts";
import * as ProviderSessionManager from "./orchestration-v2/ProviderSessionManager.ts";
import * as ThreadLaunch from "./orchestration-v2/ThreadLaunchService.ts";
import * as ThreadDeletion from "./orchestration-v2/ThreadDeletion.ts";
import * as ThreadManagement from "./orchestration-v2/ThreadManagementService.ts";
import * as ProjectService from "./project/ProjectService.ts";
import * as GitVcsDriver from "./vcs/GitVcsDriver.ts";
import * as ProjectStore from "./orchestration-v2/ProjectStore.ts";
import * as AgentAwarenessRelay from "./relay/AgentAwarenessRelay.ts";
import * as ServerLifecycleEvents from "./serverLifecycleEvents.ts";
import * as ServerSettings from "./serverSettings.ts";
import { forkParked, forkParkedFiber } from "./serverActivation.ts";
import * as AnalyticsService from "./telemetry/AnalyticsService.ts";
import * as ServerEnvironment from "./environment/ServerEnvironment.ts";
import * as EnvironmentAuth from "./auth/EnvironmentAuth.ts";
import {
  formatHeadlessServeOutput,
  formatHostForUrl,
  isWildcardHost,
  issueHeadlessServeAccessInfo,
} from "./startupAccess.ts";

export class ServerRuntimeStartupError extends Schema.TaggedError<ServerRuntimeStartupError>()(
  "ServerRuntimeStartupError",
  {
    mode: ServerConfig.RuntimeMode,
    host: Schema.NullOr(Schema.String),
    port: Schema.Number,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return "Server runtime startup failed before command readiness.";
  }
}

export class ServerUpdateThreadContinuationError extends Schema.TaggedError<ServerUpdateThreadContinuationError>()(
  "ServerUpdateThreadContinuationError",
  { operation: Schema.Literals(["prepare", "clear"]), cause: Schema.Defect() },
) {}

export const markRunningProviderSessionsForContinuation = Effect.flatMap(
  ProviderRuntimeRecovery.ProviderRuntimeRecoveryService,
  (service) => service.prepareForServerUpdate({ respectProjectPreference: false }),
).pipe(Effect.mapError((cause) => new ServerUpdateThreadContinuationError({ operation: "prepare", cause })));

export const markOptedInProviderSessionsForContinuation = Effect.flatMap(
  ProviderRuntimeRecovery.ProviderRuntimeRecoveryService,
  (service) => service.prepareForServerUpdate({ respectProjectPreference: true }),
).pipe(Effect.mapError((cause) => new ServerUpdateThreadContinuationError({ operation: "prepare", cause })));

export const clearProviderSessionContinuationMarkers = (markers: ReadonlyArray<RestartContinuationMarkerV2>) =>
  Effect.flatMap(ProviderRuntimeRecovery.ProviderRuntimeRecoveryService,
    (service) => service.clearServerUpdatePreparation(markers),
  ).pipe(Effect.mapError((cause) => new ServerUpdateThreadContinuationError({ operation: "clear", cause })));

export class ServerRuntimeStartup extends Context.Service<
  ServerRuntimeStartup,
  {
    readonly awaitCommandReady: Effect.Effect<void, ServerRuntimeStartupError>;
    readonly markHttpListening: Effect.Effect<void>;
    readonly markRunningProviderSessionsForContinuation: Effect.Effect<
      ReadonlyArray<RestartContinuationMarkerV2>,
      ServerUpdateThreadContinuationError
    >;
    readonly markOptedInProviderSessionsForContinuation: Effect.Effect<
      ReadonlyArray<RestartContinuationMarkerV2>,
      ServerUpdateThreadContinuationError
    >;
    readonly clearProviderSessionContinuationMarkers: (
      markers: ReadonlyArray<RestartContinuationMarkerV2>,
    ) => Effect.Effect<void, ServerUpdateThreadContinuationError>;
    readonly enqueueCommand: <A, E>(
      effect: Effect.Effect<A, E>,
    ) => Effect.Effect<A, E | ServerRuntimeStartupError>;
  }
>()("t3/serverRuntimeStartup") {}

interface QueuedCommand {
  readonly run: Effect.Effect<void, never>;
}

type CommandReadinessState = "pending" | "ready" | ServerRuntimeStartupError;

interface CommandGate {
  readonly awaitCommandReady: Effect.Effect<void, ServerRuntimeStartupError>;
  readonly signalCommandReady: Effect.Effect<void>;
  readonly failCommandReady: (error: ServerRuntimeStartupError) => Effect.Effect<void>;
  readonly enqueueCommand: <A, E>(
    effect: Effect.Effect<A, E>,
  ) => Effect.Effect<A, E | ServerRuntimeStartupError>;
}

const settleQueuedCommand = <A, E>(deferred: Deferred.Deferred<A, E>, exit: Exit.Exit<A, E>) =>
  Exit.isSuccess(exit)
    ? Deferred.succeed(deferred, exit.value)
    : Deferred.failCause(deferred, exit.cause);

export const makeCommandGate = Effect.gen(function* () {
  const commandReady = yield* Deferred.make<void, ServerRuntimeStartupError>();
  const commandQueue = yield* Queue.unbounded<QueuedCommand>();
  const commandReadinessState = yield* Ref.make<CommandReadinessState>("pending");

  const commandWorker = Effect.forever(
    Queue.take(commandQueue).pipe(Effect.flatMap((command) => command.run)),
  );
  yield* Effect.forkScoped(commandWorker);

  return {
    awaitCommandReady: Deferred.await(commandReady),
    signalCommandReady: Effect.gen(function* () {
      yield* Ref.set(commandReadinessState, "ready");
      yield* Deferred.succeed(commandReady, undefined).pipe(Effect.orDie);
    }),
    failCommandReady: (error) =>
      Effect.gen(function* () {
        yield* Ref.set(commandReadinessState, error);
        yield* Deferred.fail(commandReady, error).pipe(Effect.orDie);
      }),
    enqueueCommand: <A, E>(effect: Effect.Effect<A, E>) =>
      Effect.gen(function* () {
        const readinessState = yield* Ref.get(commandReadinessState);
        if (readinessState === "ready") {
          return yield* effect;
        }
        if (readinessState !== "pending") {
          return yield* readinessState;
        }

        const result = yield* Deferred.make<A, E | ServerRuntimeStartupError>();
        yield* Queue.offer(commandQueue, {
          run: Deferred.await(commandReady).pipe(
            Effect.flatMap(() => effect),
            Effect.exit,
            Effect.flatMap((exit) => settleQueuedCommand(result, exit)),
          ),
        });
        return yield* Deferred.await(result);
      }),
  } satisfies CommandGate;
});

const recordStartupHeartbeat = Effect.gen(function* () {
  const analytics = yield* AnalyticsService.AnalyticsService;
  const projects = yield* ProjectService.ProjectService;
  const threads = yield* ThreadManagement.ThreadManagementService;

  const { threadCount, projectCount } = yield* Effect.all({
    projects: projects.snapshot,
    threads: threads.getShellSnapshot(),
  }).pipe(
    Effect.map(({ projects: projectSnapshot, threads: shellSnapshot }) => ({
      projectCount: projectSnapshot.projects.length,
      threadCount: shellSnapshot.threads.length + shellSnapshot.archivedThreads.length,
    })),
    Effect.catch((cause) =>
      Effect.logWarning("failed to gather V2 startup counts for telemetry", {
        cause,
      }).pipe(
        Effect.as({
          threadCount: 0,
          projectCount: 0,
        }),
      ),
    ),
  );

  yield* analytics.record("server.boot.heartbeat", {
    threadCount,
    projectCount,
  });
});

export const getAutoBootstrapThreadModelSelection = (): ModelSelection => ({
  instanceId: ProviderInstanceId.make("codex"),
  model: DEFAULT_MODEL,
});

interface AutoBootstrapWelcomeTargets {
  readonly bootstrapProjectId?: ProjectId;
  readonly bootstrapThreadId?: ThreadId;
}

export const autoPullProjects = Effect.fn("autoPullProjects")(function* (
  projects: ReadonlyArray<Pick<Project, "id" | "workspaceRoot" | "autoPull">>,
  settings = DEFAULT_SERVER_SETTINGS,
) {
  const git = yield* GitVcsDriver.GitVcsDriver;
  const workspaceRoots = [
    ...new Set(
      projects
        .filter((project) => resolveProjectSettings(settings, project.id).settings.defaultAutoPull)
        .map((project) => project.workspaceRoot),
    ),
  ];

  yield* Effect.forEach(
    workspaceRoots,
    (cwd) =>
      Effect.gen(function* () {
        const status = yield* git.statusDetails(cwd);
        if (
          !status.isRepo ||
          !status.isDefaultBranch ||
          !status.hasUpstream ||
          status.hasWorkingTreeChanges ||
          status.aheadCount > 0
        ) {
          yield* Effect.logDebug("Skipped automatic project pull", {
            cwd,
            reason: !status.isRepo
              ? "not-a-repository"
              : !status.isDefaultBranch
                ? "not-on-default-branch"
                : !status.hasUpstream
                  ? "no-upstream"
                  : status.hasWorkingTreeChanges
                    ? "working-tree-changes"
                    : "local-commits",
          });
          return;
        }

        if (status.behindCount <= 0) return;

        const result = yield* git.pullCurrentBranch(cwd);
        yield* Effect.logDebug("Automatic project pull completed", {
          cwd,
          status: result.status,
          refName: result.refName,
        });
      }).pipe(
        Effect.catch((cause) =>
          Effect.logWarning("Automatic project pull failed", {
            cwd,
            cause,
          }),
        ),
      ),
    { concurrency: 4, discard: true },
  );
});

export const resolveWelcomeBase = Effect.gen(function* () {
  const serverConfig = yield* ServerConfig.ServerConfig;
  const segments = serverConfig.cwd.split(/[/\\]/).filter(Boolean);
  const projectName = segments[segments.length - 1] ?? "project";

  return {
    cwd: serverConfig.cwd,
    projectName,
  } as const;
});

export const resolveAutoBootstrapWelcomeTargets = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const randomUUID = crypto.randomUUIDv4;
  const serverConfig = yield* ServerConfig.ServerConfig;
  const projects = yield* ProjectService.ProjectService;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const threadLaunch = yield* ThreadLaunch.ThreadLaunchService;
  const path = yield* Path.Path;

  let bootstrapProjectId: ProjectId | undefined;
  let bootstrapThreadId: ThreadId | undefined;

  if (serverConfig.autoBootstrapProjectFromCwd) {
    // Project creation has no user model choice; only the bootstrap thread
    // gets an automatic selection, and an explicit project default wins.
    const threadModelSelection = getAutoBootstrapThreadModelSelection();
    const { project } = yield* projects.bootstrap({
      commandId: CommandId.make(yield* randomUUID),
      projectId: ProjectId.make(yield* randomUUID),
      title: path.basename(serverConfig.cwd) || "project",
      workspaceRoot: serverConfig.cwd,
    });
    const shell = yield* threads.getShellSnapshot();
    const existingThread = shell.threads.find(
      (thread) =>
        thread.projectId === project.id && thread.lineage.relationshipToParent !== "subagent",
    );
    if (existingThread === undefined) {
      const serverSettings = yield* ServerSettings.ServerSettingsService;
      const settings = yield* serverSettings.getSettings;
      const launched = yield* threadLaunch.launch({
        commandId: CommandId.make(yield* randomUUID),
        projectId: project.id,
        title: "New thread",
        modelSelection:
          resolveProjectSettings(settings, project.id, project).settings.defaultModelSelection ??
          threadModelSelection,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: resolveProjectSettings(settings, project.id, project).settings
          .defaultRuntimeMode,
        workspaceStrategy: { type: "root" },
        createdBy: "system",
        creationSource: "server",
      });
      bootstrapProjectId = project.id;
      bootstrapThreadId = launched.threadId;
    } else {
      bootstrapProjectId = project.id;
      bootstrapThreadId = existingThread.id;
    }
  }

  return {
    ...(bootstrapProjectId ? { bootstrapProjectId } : {}),
    ...(bootstrapThreadId ? { bootstrapThreadId } : {}),
  } satisfies AutoBootstrapWelcomeTargets;
});

const resolveStartupBrowserTarget = Effect.gen(function* () {
  const serverConfig = yield* ServerConfig.ServerConfig;
  const serverAuth = yield* EnvironmentAuth.EnvironmentAuth;
  const localUrl = `http://localhost:${serverConfig.port}`;
  const bindUrl =
    serverConfig.host && !isWildcardHost(serverConfig.host)
      ? `http://${formatHostForUrl(serverConfig.host)}:${serverConfig.port}`
      : localUrl;
  const baseTarget = serverConfig.devUrl?.toString() ?? bindUrl;
  return serverConfig.mode === "desktop"
    ? baseTarget
    : yield* serverAuth.issueStartupPairingUrl(baseTarget);
});

const maybeOpenBrowser = (target: string) =>
  Effect.gen(function* () {
    const serverConfig = yield* ServerConfig.ServerConfig;
    if (serverConfig.noBrowser) {
      return;
    }
    const externalLauncher = yield* ExternalLauncher.ExternalLauncher;

    yield* externalLauncher.launchBrowser(target).pipe(
      Effect.catch(() =>
        Effect.logInfo("browser auto-open unavailable", {
          hint: `Open ${target} in your browser.`,
        }),
      ),
    );
  });

const runStartupPhase = <A, E, R>(phase: string, effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.annotateSpans({ "startup.phase": phase }),
    Effect.withSpan(`server.startup.${phase}`),
  );

interface StartupOptions {
  readonly activate?: Effect.Effect<void>;
  readonly awaitAuxiliaryParked?: Effect.Effect<void>;
  readonly abort?: (error: ServerRuntimeStartupError) => Effect.Effect<void>;
}

export const startEffectWorkerWithRelay = Effect.fn(
  "ServerRuntimeStartup.startEffectWorkerWithRelay",
)(function* <WorkerContext, RelayContext>(input: {
  readonly runWorker: Effect.Effect<void, never, WorkerContext>;
  readonly startRelay: Effect.Effect<void, never, RelayContext>;
  readonly workerFiberRef: Ref.Ref<Fiber.Fiber<void, never> | null>;
}) {
  const workerFiber = yield* forkParkedFiber(input.runWorker);
  yield* Ref.set(input.workerFiberRef, workerFiber);
  yield* input.startRelay.pipe(
    Effect.onExit((exit) => {
      if (Exit.isSuccess(exit)) {
        return Effect.void;
      }
      return Ref.getAndSet(input.workerFiberRef, null).pipe(
        Effect.flatMap((ownedWorkerFiber) =>
          ownedWorkerFiber === null
            ? Effect.void
            : Fiber.interrupt(ownedWorkerFiber).pipe(Effect.asVoid),
        ),
      );
    }),
  );
});

export function runOrderedV2StartupPhases<
  Import,
  Recovery,
  Bootstrap,
  ImportError,
  RecoveryError,
  WorkerError,
  BootstrapError,
  ImportContext,
  RecoveryContext,
  WorkerContext,
  BootstrapContext,
>(input: {
  readonly importLegacyShells: Effect.Effect<Import, ImportError, ImportContext>;
  readonly recover: Effect.Effect<Recovery, RecoveryError, RecoveryContext>;
  readonly startEffectWorker: Effect.Effect<void, WorkerError, WorkerContext>;
  readonly autoBootstrap: Effect.Effect<Bootstrap, BootstrapError, BootstrapContext>;
}) {
  return Effect.gen(function* () {
    yield* input.importLegacyShells;
    const recovery = yield* input.recover;
    yield* input.startEffectWorker;
    const bootstrap = yield* input.autoBootstrap;
    return { recovery, bootstrap } as const;
  });
}

export const prepareServiceLauncherTrial = Effect.gen(function* () {
  const config = yield* ServerConfig.ServerConfig;
  const launcher = yield* ServiceLauncherClient.ServiceLauncherClient;
  const environment = yield* HostProcessEnvironment;
  const path = yield* Path.Path;
  const rawContext = environment[SERVICE_LAUNCHER_CONTEXT_ENV];
  const context = rawContext === undefined ? undefined : decodeServiceLauncherContext(rawContext);
  if (
    (rawContext !== undefined && context === undefined) ||
    (context?.update?.status === "pending" && (
      path.resolve(context.update.dbPath) !== path.resolve(config.dbPath) ||
      environment.T3CODE_HOME === undefined ||
      path.resolve(environment.T3CODE_HOME) !== path.resolve(config.baseDir)
    ))
  ) return yield* new ServerRuntimeStartupError({
    mode: config.mode,
    host: config.host ?? null,
    port: config.port,
    cause: "The service trial does not bind the selected home and database.",
  });
  return yield* launcher.prepareTrial;
});

export const runOrderedV2ActivationPhases = <Prepared, E, R>(input: {
  readonly awaitHttpListening: Effect.Effect<void, E, R>;
  readonly awaitAuxiliaryParked: Effect.Effect<void, E, R>;
  readonly prepareTrial: Effect.Effect<Prepared, E, R>;
  readonly commitJonesTrial: Effect.Effect<void, E, R>;
  readonly reconcileAfterTrial: Effect.Effect<void, E, R>;
  readonly publishWelcome: Effect.Effect<void, E, R>;
  readonly activate: Effect.Effect<void, E, R>;
  readonly signalCommandReady: Effect.Effect<void, E, R>;
}) => Effect.gen(function* () {
  yield* input.awaitHttpListening;
  yield* input.awaitAuxiliaryParked;
  const prepared = yield* input.prepareTrial;
  yield* input.commitJonesTrial;
  yield* input.reconcileAfterTrial;
  yield* input.publishWelcome;
  yield* input.activate;
  yield* input.signalCommandReady;
  return prepared;
});

const make = (options?: StartupOptions) =>
  Effect.gen(function* () {
    const serverConfig = yield* ServerConfig.ServerConfig;
    const keybindings = yield* Keybindings.Keybindings;
    const legacyV1ThreadImporter = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
    const providerRuntimeRecovery = yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService;
    const leaseCleanup = yield* ThreadDeletion.ThreadDeletionLeaseCleanup;
    const providerSessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
    const agentAwarenessRelay = yield* AgentAwarenessRelay.AgentAwarenessRelay;
    const lifecycleEvents = yield* ServerLifecycleEvents.ServerLifecycleEvents;
    const serverSettings = yield* ServerSettings.ServerSettingsService;
    const serverEnvironment = yield* ServerEnvironment.ServerEnvironment;
    const crypto = yield* Crypto.Crypto;
    const launcher = yield* ServiceLauncherClient.ServiceLauncherClient;

    const commandGate = yield* makeCommandGate;
    const httpListening = yield* Deferred.make<void>();
    const effectWorkerFiber = yield* Ref.make<Fiber.Fiber<void, never> | null>(null);

    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        yield* commandGate.failCommandReady(
          new ServerRuntimeStartupError({
            mode: serverConfig.mode,
            host: serverConfig.host ?? null,
            port: serverConfig.port,
            cause: "Server runtime is shutting down.",
          }),
        );
        const workerFiber = yield* Ref.getAndSet(effectWorkerFiber, null);
        if (workerFiber !== null) {
          yield* Fiber.interrupt(workerFiber).pipe(Effect.ignore);
        }
        yield* providerRuntimeRecovery.prepareForShutdown.pipe(
          Effect.ensuring(providerSessions.shutdown),
        );
        const reconciliation = yield* providerRuntimeRecovery.reconcile("shutdown");
        yield* Effect.logInfo("V2 orchestration shutdown reconciliation completed", reconciliation);
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("V2 orchestration shutdown reconciliation failed", {
            cause: Cause.pretty(cause),
          }),
        ),
      ),
    );

    const startup = Effect.gen(function* () {
      yield* Effect.logDebug("startup phase: starting keybindings runtime");
      yield* runStartupPhase(
        "keybindings.start",
        keybindings.start.pipe(
          Effect.catch((error) =>
            Effect.logWarning("failed to start keybindings runtime", {
              path: error.configPath,
              detail: error.detail,
              cause: error.cause,
            }),
          ),
        ),
      );

      yield* Effect.logDebug("startup phase: starting server settings runtime");
      yield* runStartupPhase(
        "settings.start",
        serverSettings.start.pipe(
          Effect.catch((error) =>
            Effect.logWarning("failed to start server settings runtime", {
              path: error.settingsPath,
              operation: error.operation,
              providerInstanceId: error.providerInstanceId,
              environmentVariable: error.environmentVariable,
              cause: error.cause,
            }),
          ),
        ),
      );

      const welcomeBase = yield* resolveWelcomeBase;
      const environment = yield* serverEnvironment.getDescriptor;
      const legacyMigrationThreadCount = yield* legacyV1ThreadImporter.pendingThreadCount;
      if (legacyMigrationThreadCount > 0) {
        yield* lifecycleEvents.publish({
          version: 1,
          type: "legacyThreadMigration",
          payload: {
            status: "running",
            totalThreadCount: legacyMigrationThreadCount,
          },
        });
      }
      const { recovery, bootstrap: bootstrapTargets } = yield* runOrderedV2StartupPhases({
        importLegacyShells: runStartupPhase(
          "orchestration-v2.legacy-v1.import-shells",
          legacyV1ThreadImporter.reconcileShells.pipe(
            Effect.tap((summary) =>
              summary.importedThreadCount === 0
                ? Effect.void
                : Effect.logInfo("Imported legacy v1 thread shells", summary),
            ),
          ),
        ),
        recover: runStartupPhase("orchestration-v2.recovery.stage", providerRuntimeRecovery.stageStartupRecovery),
        startEffectWorker: runStartupPhase(
          "orchestration-v2.effect-worker.start",
          startEffectWorkerWithRelay({
            runWorker: EffectWorker.runDaemon,
            startRelay: agentAwarenessRelay.start(),
            workerFiberRef: effectWorkerFiber,
          }),
        ),
        autoBootstrap: (serverConfig.autoBootstrapProjectFromCwd
          ? runStartupPhase(
              "welcome.autobootstrap",
              resolveAutoBootstrapWelcomeTargets.pipe(Effect.provideService(Crypto.Crypto, crypto)),
            )
          : Effect.succeed({})
        ).pipe(Effect.map((targets): AutoBootstrapWelcomeTargets => targets)),
      });
      yield* Effect.logInfo("V2 orchestration recovery staged", { continuationMarkerCount: recovery.continuationMarkers.length });
      yield* runStartupPhase(
        "projects.auto-pull",
        Effect.gen(function* () {
          const projects = yield* (yield* ProjectStore.ProjectStoreV2).listShells();
          const settings = yield* serverSettings.getSettings;
          yield* autoPullProjects(projects, settings);
        }).pipe(forkParked),
      );

      const importPendingTranscripts = legacyV1ThreadImporter.importPendingTranscripts.pipe(
        Effect.tap((summary) =>
          summary.importedThreadCount === 0
            ? Effect.void
            : Effect.logInfo("Hydrated legacy v1 thread transcripts", summary),
        ),
      );
      yield* (
        legacyMigrationThreadCount > 0
          ? importPendingTranscripts.pipe(
              Effect.tap(() =>
                lifecycleEvents.publish({
                  version: 1,
                  type: "legacyThreadMigration",
                  payload: {
                    status: "complete",
                    totalThreadCount: legacyMigrationThreadCount,
                  },
                }),
              ),
            )
          : importPendingTranscripts
      ).pipe(forkParked);

      yield* forkParked(
        Effect.gen(function* () {
          yield* Effect.logDebug("startup phase: recording startup heartbeat");
          yield* recordStartupHeartbeat.pipe(
            Effect.annotateSpans({ "startup.phase": "heartbeat.record" }),
            Effect.withSpan("server.startup.heartbeat.record"),
            Effect.ignoreCause({ log: true }),
          );
          if (serverConfig.startupPresentation === "headless") {
            yield* Effect.logDebug("startup phase: headless access info");
            const accessInfo = yield* issueHeadlessServeAccessInfo();
            yield* runStartupPhase(
              "headless.output",
              Console.log(formatHeadlessServeOutput(accessInfo)),
            );
          } else {
            yield* Effect.logDebug("startup phase: browser open check");
            const startupBrowserTarget = yield* resolveStartupBrowserTarget;
            if (serverConfig.mode !== "desktop") {
              yield* Effect.logInfo(
                "Authentication required. Open T3 Code using the pairing URL.",
              ).pipe(Effect.annotateLogs({ pairingUrl: startupBrowserTarget }));
            }
            yield* runStartupPhase("browser.open", maybeOpenBrowser(startupBrowserTarget));
          }
        }),
      );

      const updateOutcome = yield* runOrderedV2ActivationPhases({
        awaitHttpListening: runStartupPhase("http.wait", Deferred.await(httpListening)),
        awaitAuxiliaryParked: runStartupPhase("auxiliary-roots.parked", options?.awaitAuxiliaryParked ?? Effect.void),
        prepareTrial: prepareServiceLauncherTrial,
        commitJonesTrial: Effect.gen(function* () {
          const address = (yield* HttpServer.HttpServer).address;
          if (typeof address === "string" || !("port" in address)) return yield* new ServerRuntimeStartupError({
            mode: serverConfig.mode,
            host: serverConfig.host ?? null,
            port: serverConfig.port,
            cause: "The Jones trial requires the actual HTTP listener address.",
          });
          return yield* Effect.tryPromise({
            try: (signal) => awaitJonesTrialCommit({
              descriptorPath: process.env.T3CODE_JONES_TRIAL_DESCRIPTOR,
              home: serverConfig.baseDir,
              databasePath: serverConfig.dbPath,
              profile: process.env.T3CODE_DESKTOP_USER_DATA_DIR,
              environmentId: environment.environmentId,
              version: packageJson.version,
              buildMetadata: packageJson,
              listener: `http://${formatHostForUrl(serverConfig.host ?? "127.0.0.1")}:${address.port}`,
              signal,
            }),
            catch: (cause) => new ServerRuntimeStartupError({
              mode: serverConfig.mode,
              host: serverConfig.host ?? null,
              port: serverConfig.port,
              cause,
            }),
          });
        }),
        reconcileAfterTrial: runStartupPhase(
          "orchestration-v2.recovery.after-trial",
          providerRuntimeRecovery.reconcileAfterStartupTrial(recovery).pipe(
            Effect.mapError((cause) => new ServerRuntimeStartupError({
              mode: serverConfig.mode,
              host: serverConfig.host ?? null,
              port: serverConfig.port,
              cause,
            })),
            Effect.tap((summary) => Effect.logInfo("V2 orchestration recovery completed", {
              terminalizedRuns: summary.terminalizedRuns,
              stoppedSessions: summary.stoppedSessions,
              closedRequests: summary.closedRequests,
              retiredEffects: summary.retiredEffects,
              requeuedEffects: summary.requeuedEffects,
              failedThreadCount: summary.failedThreadIds.length,
              releasedContinuationCount: summary.releasedContinuationMarkerIds.length,
              heldContinuationCount: summary.heldContinuationMarkers.length,
            })),
            Effect.asVoid,
            Effect.andThen(runStartupPhase(
              "orchestration-v2.lease-cleanup.after-trial",
              leaseCleanup.reconcileLeaseOwners.pipe(
                Effect.catch(() => Effect.logWarning("Retained worktree ownership inventory is unavailable.")),
                Effect.asVoid,
              ),
            )),
          ),
        ),
        publishWelcome: runStartupPhase("welcome.publish", lifecycleEvents.publish({
          version: 1,
          type: "welcome",
          payload: { environment, ...welcomeBase, ...bootstrapTargets },
        })),
        activate: options?.activate ?? Effect.void,
        signalCommandReady: commandGate.signalCommandReady,
      });
      yield* Effect.logDebug("startup phase: publishing ready event");
      yield* runStartupPhase(
        "ready.publish",
        lifecycleEvents.publish({
          version: 1,
          type: "ready",
          payload: {
            at: DateTime.formatIso(yield* DateTime.now),
            environment,
            ...(updateOutcome === undefined ? {} : { updateOutcome }),
          },
        }),
      );
      yield* Effect.logDebug("startup phase: complete");
      yield* flushCompileCache;
    }).pipe(
      Effect.annotateSpans({
        "server.mode": serverConfig.mode,
        "server.port": serverConfig.port,
        "server.host": serverConfig.host ?? "default",
      }),
      Effect.withSpan("server.startup", { kind: "server", root: true }),
    );

    yield* Effect.forkScoped(
      Effect.exit(startup).pipe(
        Effect.flatMap((startupExit) => {
          if (Exit.isSuccess(startupExit)) return Effect.void;
          const error = new ServerRuntimeStartupError({
            mode: serverConfig.mode,
            host: serverConfig.host ?? null,
            port: serverConfig.port,
            cause: startupExit.cause,
          });
          return Effect.logError("server runtime startup failed", {
            cause: Cause.pretty(startupExit.cause),
          }).pipe(
            Effect.andThen(commandGate.failCommandReady(error)),
            Effect.andThen(options?.abort?.(error) ?? Effect.void),
          );
        }),
      ),
    );

    return {
      awaitCommandReady: commandGate.awaitCommandReady,
      markHttpListening: Deferred.succeed(httpListening, undefined),
      markRunningProviderSessionsForContinuation: markRunningProviderSessionsForContinuation.pipe(
        Effect.provideService(ProviderRuntimeRecovery.ProviderRuntimeRecoveryService, providerRuntimeRecovery),
      ),
      markOptedInProviderSessionsForContinuation: markOptedInProviderSessionsForContinuation.pipe(
        Effect.provideService(ProviderRuntimeRecovery.ProviderRuntimeRecoveryService, providerRuntimeRecovery),
      ),
      clearProviderSessionContinuationMarkers: (markers) => clearProviderSessionContinuationMarkers(markers).pipe(
        Effect.provideService(ProviderRuntimeRecovery.ProviderRuntimeRecoveryService, providerRuntimeRecovery),
      ),
      enqueueCommand: commandGate.enqueueCommand,
    } satisfies ServerRuntimeStartup["Service"];
  });

export const layerWithOptions = (options?: StartupOptions) =>
  Layer.effect(ServerRuntimeStartup, make(options));

const layer = layerWithOptions();
