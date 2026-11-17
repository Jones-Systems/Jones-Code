import * as RuntimeStop from "../../jones/runtime/RuntimeStop.ts";
import * as AuthSessions from "../../persistence/AuthSessions.ts";
import * as RestartPreparation from "../../jones/orchestration/RestartPreparation.ts";
import * as ControlAdmission from "../../jones/orchestration/ControlAdmission.ts";
import * as NativeStartAdmission from "../../jones/nativeCreation/NativeStartAdmission.ts";
import * as StartRefusals from "../../jones/startRefusal/Store.ts";
import * as StartRefusalProbe from "../../jones/startRefusal/Probe.ts";
import * as GoalContinuityRuntime from "../../jones/goals/continuity/RuntimeIntegration.ts";
import * as ServerEnvironment from "../../environment/ServerEnvironment.ts";
import type * as CheckoutIntent from "../../jones/ordinaryCheckout/CheckoutIntent.ts";
import * as RuntimeLayer from "../runtimeLayer.ts";
import { resolveServerParallelismProfile } from "../../jones/serverParallelism/config.ts";
import * as OtelEnvironment from "@t3tools/shared/otelEnvironment";
import { DEFAULT_SIGNAL_EXPORT } from "@t3tools/shared/observability";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import type {
  ManagedPolicyDocument,
  ProviderDriverKind,
  ProviderReplayTranscript,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import type * as Scope from "effect/Scope";
import type * as SqlClient from "effect/sql/SqlClient";
import type { MigrationError } from "effect/sql/Migrator";
import type { SqlError } from "effect/sql/SqlError";

import * as CheckpointStore from "../../checkpointing/CheckpointStore.ts";
import * as ServerConfig from "../../config.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as ManagedPolicy from "../../policy/ManagedPolicy.ts";
import * as ThreadManagementService from "../ThreadManagementService.ts";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as McpSessionRegistryTestkit from "../../mcp/McpSessionRegistry.testkit.ts";
import * as VcsDriverRegistry from "../../vcs/VcsDriverRegistry.ts";
import * as GitVcsDriver from "../../vcs/GitVcsDriver.ts";
import * as VcsProcess from "../../vcs/VcsProcess.ts";
import * as CheckpointCaptureService from "../CheckpointCaptureService.ts";
import * as CheckpointService from "../CheckpointService.ts";
import * as CheckpointRollbackService from "../CheckpointRollbackService.ts";
import * as CommandPolicy from "../CommandPolicy.ts";
import * as CommandReceiptStore from "../CommandReceiptStore.ts";
import * as ContextHandoffService from "../ContextHandoffService.ts";
import * as EffectOutbox from "../EffectOutbox.ts";
import * as EffectWorker from "../EffectWorker.ts";
import * as EventSink from "../EventSink.ts";
import * as EventStore from "../EventStore.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Orchestrator from "../Orchestrator.ts";
import * as ProjectionStore from "../ProjectionStore.ts";
import * as ProjectStore from "../ProjectStore.ts";
import * as ProviderAdapterRegistry from "../ProviderAdapterRegistry.ts";
import * as ProviderAuthService from "../../provider/ProviderAuthService.ts";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as ProviderContinuationService from "../ProviderContinuationService.ts";
import * as ProviderEventIngestor from "../ProviderEventIngestor.ts";
import * as ProviderInstallationGate from "../../provider/ProviderInstallationGate.ts";
import * as ProviderInstanceRegistry from "../../provider/ProviderInstanceRegistry.ts";
import * as ProviderRuntimeRecoveryService from "../ProviderRuntimeRecoveryService.ts";
import * as ProviderSessionManager from "../ProviderSessionManager.ts";
import * as CommandOutputRecorder from "./CommandOutputRecorder.ts";
import * as ProviderSwitchService from "../ProviderSwitchService.ts";
import * as ProviderTurnControlService from "../ProviderTurnControlService.ts";
import * as ProviderTurnStartService from "../ProviderTurnStartService.ts";
import * as ProviderTurnStartServiceTestkit from "../ProviderTurnStartService.testkit.ts";
import * as McpAppModelContext from "../../mcpApps/McpAppModelContext.ts";
import * as PreviewAgentActivity from "../../preview/PreviewAgentActivity.ts";
import * as RunExecutionService from "../RunExecutionService.ts";
import * as RunFinalizationService from "../RunFinalizationService.ts";
import * as ThreadTitleRegenerationService from "../ThreadTitleRegenerationService.ts";
import * as RuntimePolicy from "../RuntimePolicy.ts";
import * as TurnItemPositionStore from "../TurnItemPositionStore.ts";
import * as RuntimeRequestService from "../RuntimeRequestService.ts";
import * as ThreadCommandExecutor from "../ThreadCommandExecutor.ts";
import * as ThreadForkService from "../ThreadForkService.ts";
import {
  runOrchestratorV2Scenario,
  type OrchestratorV2ScenarioStepError,
  type OrchestratorV2Scenario,
  type OrchestratorV2ScenarioResult,
} from "./OrchestratorScenario.ts";
import {
  makeProviderReplayGate,
  type ProviderReplayGate,
} from "@t3tools/provider-testing/replayGate";

/** A supplied database may be the real persistence open graph, including managed-open admission. */
type ReplayDatabaseError = Layer.Error<ReturnType<typeof SqlitePersistence.layerFromPath>>;
/** Replay also builds the server environment identity in its temporary state directory. */
type ReplayInfrastructureError =
  | ReplayDatabaseError
  | ServerEnvironment.ServerEnvironmentIdPersistenceError;

export function makeReplayServerConfig(
  scenario: string,
): Effect.Effect<
  ServerConfig.ServerConfig["Service"],
  PlatformError.PlatformError,
  FileSystem.FileSystem | Path.Path | Scope.Scope
> {
  const safeScenario = scenario.replace(/[^a-z0-9_-]+/gi, "-");
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const baseDir = yield* fs.makeTempDirectoryScoped({
      prefix: `t3-orchestration-v2-replay-${safeScenario}-`,
    });
    const stateDir = path.join(baseDir, "userdata");
    const logsDir = path.join(stateDir, "logs");
    const providerLogsDir = path.join(logsDir, "provider");
    const terminalLogsDir = path.join(logsDir, "terminals");
    const attachmentsDir = path.join(stateDir, "attachments");
    const environmentThemesDir = path.join(stateDir, "themes");
    const worktreesDir = path.join(baseDir, "worktrees");
    const providerStatusCacheDir = path.join(baseDir, "caches");

    for (const directory of [
      stateDir,
      logsDir,
      providerLogsDir,
      terminalLogsDir,
      attachmentsDir,
      environmentThemesDir,
      worktreesDir,
      providerStatusCacheDir,
    ]) {
      yield* fs.makeDirectory(directory, { recursive: true });
    }

    return {
      logLevel: "Error",
      traceMinLevel: "Info",
      traceTimingEnabled: true,
      traceBatchWindowMs: 200,
      traceMaxBytes: 10 * 1024 * 1024,
      traceMaxFiles: 10,
      otelEnvironment: OtelEnvironment.none,
      otlpTracesUrl: undefined,
      otlpMetricsUrl: undefined,
      otlpLogsUrl: undefined,
      otlpTracesExport: DEFAULT_SIGNAL_EXPORT,
      otlpMetricsExport: DEFAULT_SIGNAL_EXPORT,
      otlpLogsExport: DEFAULT_SIGNAL_EXPORT,
      mode: "web",
      port: 0,
      host: undefined,
      cwd: process.cwd(),
      baseDir,
      staticDir: undefined,
      devUrl: undefined,
      devAllowedOrigins: [],
      noBrowser: false,
      startupPresentation: "browser",
      tailscaleServeEnabled: false,
      tailscaleServePort: 443,
      desktopBootstrapToken: undefined,
      autoBootstrapProjectFromCwd: false,
      logWebSocketEvents: false,
      serverParallelism: resolveServerParallelismProfile(undefined),
      stateDir,
      authorityStateDir: path.join(baseDir, "native-store-authority"),
      dbPath: path.join(stateDir, "state.sqlite"),
      keybindingsConfigPath: path.join(stateDir, "keybindings.json"),
      settingsPath: path.join(stateDir, "settings.json"),
      providerStatusCacheDir,
      worktreesDir,
      worktreeCheckoutCacheDir: path.join(providerStatusCacheDir, "worktree-checkouts"),
      attachmentsDir,
      browserArtifactsDir: path.join(stateDir, "browser-artifacts"),
      environmentThemesDir,
      logsDir,
      serverLogPath: path.join(logsDir, "server.log"),
      serverTracePath: path.join(logsDir, "server.trace.ndjson"),
      providerLogsDir,
      providerEventLogPath: path.join(providerLogsDir, "events.log"),
      terminalLogsDir,
      anonymousIdPath: path.join(stateDir, "anonymous-id"),
      environmentIdPath: path.join(stateDir, "environment-id"),
      serverRuntimeStatePath: path.join(stateDir, "server-runtime.json"),
      secretsDir: path.join(stateDir, "secrets"),
    };
  });
}

export interface OrchestratorV2ProviderReplayScenario<
  Transcript extends ProviderReplayTranscript = ProviderReplayTranscript,
> extends OrchestratorV2Scenario {
  readonly transcript: Transcript;
  readonly runtimePolicyOverride?: RuntimePolicy.RuntimePolicyV2Override;
}

export interface OrchestratorV2ProviderReplayHarness<
  Transcript extends ProviderReplayTranscript = ProviderReplayTranscript,
  Error = never,
> {
  readonly driver: ProviderDriverKind;
  readonly decodeTranscript: (
    transcript: ProviderReplayTranscript,
  ) => Effect.Effect<Transcript, Error>;
  readonly makeProviderAdapterRegistryLayer: (
    transcript: Transcript,
    options?: { readonly replayGate?: ProviderReplayGate },
  ) => Layer.Layer<
    ProviderAdapterRegistry.ProviderAdapterRegistryV2,
    Error,
    McpProviderSessions.McpProviderSessions
  >;
}

export function runOrchestratorV2ProviderReplayScenario<
  Transcript extends ProviderReplayTranscript,
  Error,
>(
  scenario: OrchestratorV2ProviderReplayScenario<Transcript>,
  harness: OrchestratorV2ProviderReplayHarness<Transcript, Error>,
  options: {
    readonly databaseLayer?: Layer.Layer<SqlClient.SqlClient, ReplayDatabaseError>;
    readonly continuationRequestsLayer?: Layer.Layer<never>;
    readonly startAdmissionLayer?: Layer.Layer<ControlAdmission.ControlAdmission>;
    readonly configureMcp?: boolean;
    readonly onSessionDetached?: (
      input: Parameters<ProviderSessionManager.ProviderSessionManagerV2["Service"]["detach"]>[0],
    ) => Effect.Effect<void>;
    readonly runEffectWorker?: boolean;
    readonly runContinuationWorker?: boolean;
    readonly recoverOnStartup?: boolean;
    readonly continueThreadsAfterServerUpdate?: boolean;
    readonly managedPolicy?: ManagedPolicyDocument;
  } = {},
): Effect.Effect<
  OrchestratorV2ScenarioResult,
  | Orchestrator.OrchestratorV2Error
  | OrchestratorV2ScenarioStepError
  | Error
  | ReplayInfrastructureError,
  never
> {
  const replayGate = makeProviderReplayGate(
    scenario.steps?.flatMap((step) =>
      step.type === "release_replay_gate" || step.type === "release_replay_gate_after_waiting"
        ? [step.label]
        : [],
    ) ?? [],
  );
  const layer = layerProviderReplay(scenario, harness, {
    ...options,
    replayGate,
  });

  return Effect.gen(function* () {
    const result = yield* runOrchestratorV2Scenario(scenario, { replayGate });
    const recorder = yield* CommandOutputRecorder.CommandOutputRecorder;
    return { ...result, commandOutput: yield* recorder.snapshot };
  }).pipe(Effect.provide(layer));
}

export function layerProviderReplay<Transcript extends ProviderReplayTranscript, Error>(
  scenario: OrchestratorV2ProviderReplayScenario<Transcript>,
  harness: OrchestratorV2ProviderReplayHarness<Transcript, Error>,
  options: {
    readonly databaseLayer?: Layer.Layer<SqlClient.SqlClient, ReplayDatabaseError>;
    readonly continuationRequestsLayer?: Layer.Layer<never>;
    readonly startAdmissionLayer?: Layer.Layer<ControlAdmission.ControlAdmission>;
    readonly configureMcp?: boolean;
    readonly onSessionDetached?: (
      input: Parameters<ProviderSessionManager.ProviderSessionManagerV2["Service"]["detach"]>[0],
    ) => Effect.Effect<void>;
    readonly runEffectWorker?: boolean;
    readonly runContinuationWorker?: boolean;
    readonly recoverOnStartup?: boolean;
    readonly continueThreadsAfterServerUpdate?: boolean;
    readonly managedPolicy?: ManagedPolicyDocument;
    readonly replayGate?: ProviderReplayGate;
  } = {},
): Layer.Layer<
  | Orchestrator.OrchestratorV2
  | EffectWorker.OrchestrationEffectWorkerV2
  | EffectWorker.OrchestrationEffectExecutorV2
  | EffectOutbox.EffectOutboxV2
  | EventSink.EventSinkV2
  | CommandOutputRecorder.CommandOutputRecorder
  | ProviderSessionManager.ProviderSessionManagerV2,
  Error | ReplayInfrastructureError
> {
  const layerRegistry = harness.makeProviderAdapterRegistryLayer(
    scenario.transcript,
    options.replayGate === undefined ? {} : { replayGate: options.replayGate },
  );
  return layerWithRegistry(scenario, layerRegistry, options);
}

export function layerWithRegistry<Error>(
  ...args: Parameters<typeof layerWithRegistryAndServices<Error>>
): Layer.Layer<
  | Orchestrator.OrchestratorV2
  | EffectWorker.OrchestrationEffectWorkerV2
  | EffectWorker.OrchestrationEffectExecutorV2
  | EffectOutbox.EffectOutboxV2
  | EventSink.EventSinkV2
  | ProviderSessionManager.ProviderSessionManagerV2
  | CommandOutputRecorder.CommandOutputRecorder
  | ThreadCommandExecutor.ThreadCommandExecutor,
  Error | ReplayInfrastructureError
> {
  // Narrow the runtime context too: leaked mocks override consumers' real services.
  return Layer.mergeAll(
    Layer.effect(Orchestrator.OrchestratorV2, Orchestrator.OrchestratorV2),
    Layer.effect(
      EffectWorker.OrchestrationEffectWorkerV2,
      EffectWorker.OrchestrationEffectWorkerV2,
    ),
    Layer.effect(
      EffectWorker.OrchestrationEffectExecutorV2,
      EffectWorker.OrchestrationEffectExecutorV2,
    ),
    Layer.effect(EffectOutbox.EffectOutboxV2, EffectOutbox.EffectOutboxV2),
    Layer.effect(EventSink.EventSinkV2, EventSink.EventSinkV2),
    Layer.effect(
      ProviderSessionManager.ProviderSessionManagerV2,
      ProviderSessionManager.ProviderSessionManagerV2,
    ),
    Layer.effect(
      CommandOutputRecorder.CommandOutputRecorder,
      CommandOutputRecorder.CommandOutputRecorder,
    ),
    Layer.effect(
      ThreadCommandExecutor.ThreadCommandExecutor,
      ThreadCommandExecutor.ThreadCommandExecutor,
    ),
  ).pipe(Layer.provide(layerWithRegistryAndServices(...args)));
}

/** Exposes domain services for registered-tool tests without leaking workspace mocks. */
export function layerWithRegistryAndServices<Error>(
  scenario: Pick<OrchestratorV2ProviderReplayScenario, "name" | "runtimePolicyOverride">,
  registryLayer: Layer.Layer<
    ProviderAdapterRegistry.ProviderAdapterRegistryV2,
    Error,
    McpProviderSessions.McpProviderSessions
  >,
  options: {
    readonly eventStoreLayer?: Layer.Layer<EventStore.EventStoreV2, never, SqlClient.SqlClient>;
    readonly checkoutIntentLayer?: Layer.Layer<
      CheckoutIntent.CheckoutIntent,
      MigrationError | PlatformError.PlatformError | SqlError
    >;
    readonly serverConfigLayer?: Layer.Layer<ServerConfig.ServerConfig>;
    readonly databaseLayer?: Layer.Layer<SqlClient.SqlClient, ReplayDatabaseError>;
    readonly runEffectWorker?: boolean;
    readonly continuationRequestsLayer?: Layer.Layer<never>;
    readonly startAdmissionLayer?: Layer.Layer<ControlAdmission.ControlAdmission>;
    readonly configureMcp?: boolean;
    readonly beforeEffectExecution?: (
      effect: EffectOutbox.OrchestrationEffectV2,
    ) => Effect.Effect<void>;
    readonly onSessionDetached?: (
      input: Parameters<ProviderSessionManager.ProviderSessionManagerV2["Service"]["detach"]>[0],
    ) => Effect.Effect<void>;
    readonly runContinuationWorker?: boolean;
    readonly recoverOnStartup?: boolean;
    readonly continueThreadsAfterServerUpdate?: boolean;
    /** Lets a test hold a projection read open to order concurrent dispatches. */
    readonly wrapProjectionStore?: (
      store: ProjectionStore.ProjectionStoreV2Shape,
    ) => ProjectionStore.ProjectionStoreV2Shape;
    readonly managedPolicy?: ManagedPolicyDocument;
  } = {},
): Layer.Layer<
  | RuntimeStop.CurrentRuntimeStop
  | AuthSessions.AuthSessionRepository
  | ProjectionStore.ProjectionStoreV2
  | ProviderSwitchService.ProviderSwitchServiceV2
  | ProviderTurnStartService.ProviderTurnStartServiceV2
  | ServerSettings.ServerSettingsService
  | SqlClient.SqlClient
  | ThreadManagementService.ThreadManagementService
  | EffectWorker.OrchestrationEffectExecutorV2
  | Orchestrator.OrchestratorV2
  | EffectOutbox.EffectOutboxV2
  | EffectWorker.OrchestrationEffectWorkerV2
  | EventSink.EventSinkV2
  | ProviderSessionManager.ProviderSessionManagerV2
  | CommandOutputRecorder.CommandOutputRecorder
  | ThreadCommandExecutor.ThreadCommandExecutor,
  Error | ReplayInfrastructureError
> {
  const layerServerConfig =
    options.serverConfigLayer ??
    Layer.effect(
      ServerConfig.ServerConfig,
      makeReplayServerConfig(scenario.name).pipe(Effect.orDie),
    ).pipe(Layer.provide(NodeServices.layer));
  const layerRuntimeBase =
    scenario.runtimePolicyOverride === undefined
      ? RuntimePolicy.layer
      : RuntimePolicy.layerWithOverride(scenario.runtimePolicyOverride).pipe(
          Layer.provide(RuntimePolicy.layer),
        );
  const layerDatabase = options.databaseLayer ?? SqlitePersistence.layerMemory;
  // Share one continuation queue across adapters, orchestrator, and worker through layer memoization.
  const layerContinuationRequests =
    options.continuationRequestsLayer ??
    (options.runContinuationWorker === true ? ProviderContinuationRequests.layer : Layer.empty);
  const layerServerSettings = Layer.unwrap(
    Effect.map(ManagedPolicy.fromDocument("/test/policy.json", options.managedPolicy), (policy) =>
      ServerSettings.layerTest(
        {
          responseStreamingMode: "turn",
          ...(options.continueThreadsAfterServerUpdate === undefined
            ? {}
            : { continueThreadsAfterServerUpdate: options.continueThreadsAfterServerUpdate }),
        },
        policy.summary,
      ),
    ),
  ).pipe(Layer.orDie);
  const layerEffectOutboxProvided = EffectOutbox.layer.pipe(Layer.provide(layerDatabase));
  const wrapProjectionStore = options.wrapProjectionStore;
  const layerProjectionStore =
    wrapProjectionStore === undefined
      ? ProjectionStore.layer
      : Layer.effect(
          ProjectionStore.ProjectionStoreV2,
          Effect.gen(function* () {
            return wrapProjectionStore(yield* ProjectionStore.ProjectionStoreV2);
          }),
        ).pipe(Layer.provide(ProjectionStore.layer));
  const layerStores = Layer.mergeAll(
    options.eventStoreLayer ?? EventStore.layer,
    layerProjectionStore,
    ProjectStore.layer,
    AuthSessions.layer,
    CommandReceiptStore.layer,
    layerEffectOutboxProvided,
    TurnItemPositionStore.layer,
  ).pipe(Layer.provideMerge(layerDatabase));
  const layerStartRefusals = StartRefusals.layer.pipe(Layer.provide(layerDatabase));
  const layerControlAdmission = options.startAdmissionLayer ?? ControlAdmission.layer;
  const layerGoalContinuity = GoalContinuityRuntime.layer.pipe(Layer.provide(layerDatabase));
  const layerEnvironment = ServerEnvironment.layerIdentity.pipe(Layer.provide(layerServerConfig));
  const layerProvidedRegistry = registryLayer.pipe(
    Layer.provide(
      Layer.mergeAll(layerContinuationRequests, McpProviderSessions.layer, layerEnvironment),
    ),
  );
  const layerAdmissionProbe = StartRefusalProbe.layer.pipe(
    Layer.provide(
      Layer.mergeAll(layerStores, layerControlAdmission, layerGoalContinuity, layerEnvironment),
    ),
  );
  const layerEventSinkProvided = EventSink.layerFromStores.pipe(
    Layer.provide(
      Layer.mergeAll(layerStores, layerStartRefusals, options.checkoutIntentLayer ?? Layer.empty),
    ),
  );
  const layerRuntime = RuntimePolicy.layerEnforcingCeiling.pipe(
    Layer.provide(Layer.merge(layerRuntimeBase, layerServerSettings)),
  );
  const layerCommandReceiptStoreProvided = CommandReceiptStore.layer.pipe(
    Layer.provide(layerDatabase),
  );
  const layerProviderEventIngestorProvided = ProviderEventIngestor.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)({
          getInstance: () => Effect.undefined,
          getCatalogSource: () => undefined,
        }),
        layerStores,
        layerEventSinkProvided,
        IdAllocator.layer,
        ThreadCommandExecutor.layer,
        GitVcsDriver.layer.pipe(
          Layer.provide(layerServerConfig),
          Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
        ),
      ),
    ),
  );
  const layerVcsDriverRegistry = VcsDriverRegistry.layer.pipe(
    Layer.provide(VcsProcess.layer),
    Layer.provide(layerServerConfig),
    Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
  );
  const layerCheckpointStore = CheckpointStore.layer.pipe(
    Layer.provide(layerVcsDriverRegistry),
    Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
  );
  const layerCheckpointServiceProvided = CheckpointService.layer.pipe(
    Layer.provide(Layer.mergeAll(layerCheckpointStore, IdAllocator.layer)),
  );
  const layerContextHandoffServiceProvided = ContextHandoffService.layer.pipe(
    Layer.provide(IdAllocator.layer),
  );
  const layerPersistence = Layer.mergeAll(
    layerStores,
    layerEventSinkProvided,
    layerCommandReceiptStoreProvided,
    IdAllocator.layer,
    layerProviderEventIngestorProvided,
  );
  const layerCommandOutputHubProvided = CommandOutputRecorder.layer.pipe(
    Layer.provide(Layer.merge(layerEventSinkProvided, layerStores)),
  );
  const layerProviderSessionManagerBase = ProviderSessionManager.layerWithOptions({
    configureMcp: options.configureMcp ?? false,
  }).pipe(
    Layer.provide(
      Layer.mergeAll(
        layerCommandOutputHubProvided,
        layerProvidedRegistry,
        layerEventSinkProvided,
        IdAllocator.layer,
        McpProviderSessions.layer,
        McpSessionRegistryTestkit.layer,
        layerServerSettings,
        layerProviderEventIngestorProvided,
        layerStores,
        ProviderInstallationGate.layer,
      ),
    ),
  );
  const onSessionDetached = options.onSessionDetached;
  const layerProviderSessionManagerProvided =
    onSessionDetached === undefined
      ? layerProviderSessionManagerBase
      : Layer.effect(
          ProviderSessionManager.ProviderSessionManagerV2,
          Effect.gen(function* () {
            const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
            return ProviderSessionManager.ProviderSessionManagerV2.of({
              ...sessions,
              detach: (input) =>
                sessions.detach(input).pipe(Effect.tap(() => onSessionDetached(input))),
            });
          }),
        ).pipe(Layer.provide(layerProviderSessionManagerBase));
  const layerProviderSwitchServiceProvided = ProviderSwitchService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerProvidedRegistry,
        layerStores,
        layerEventSinkProvided,
        layerStartRefusals,
      ),
    ),
  );
  const layerRunExecutionServiceProvided = RunExecutionService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        McpAppModelContext.layerEmpty,
        PreviewAgentActivity.layer,
        layerCheckpointServiceProvided,
        layerEventSinkProvided,
        IdAllocator.layer,
        layerProviderEventIngestorProvided,
        layerServerSettings,
        layerStores,
        ThreadCommandExecutor.layer,
      ),
    ),
  );
  const layerRestartPreparation = RestartPreparation.layer.pipe(
    Layer.provide(Layer.merge(layerStores, layerEventSinkProvided)),
  );
  const layerNativeStartAdmission = NativeStartAdmission.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerStores,
        layerEventSinkProvided,
        layerControlAdmission,
        layerStartRefusals,
      ),
    ),
  );
  const layerProviderTurnStartServiceProvided = RuntimeLayer.layerCapturedProviderTurnStart.pipe(
    Layer.provide(ProviderTurnStartService.layer),
    Layer.provide(
      Layer.mergeAll(
        layerNativeStartAdmission,
        layerGoalContinuity,
        layerEnvironment,
        layerControlAdmission,
        layerStartRefusals,
        layerRestartPreparation,
        layerProviderSwitchServiceProvided,
      ),
    ),
    Layer.provide(
      Layer.mergeAll(
        layerContextHandoffServiceProvided,
        layerEventSinkProvided,
        IdAllocator.layer,
        layerStores,
        layerProviderSessionManagerProvided,
        Layer.mock(ProviderAuthService.ProviderAuthService)({
          tryHandlePromptCommand: () => Effect.succeed(false),
        }),
        layerRunExecutionServiceProvided,
        layerRuntime,
        layerServerConfig,
        layerServerSettings,
      ),
    ),
  );
  const layerProviderTurnControlServiceProvided = ProviderTurnControlService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(layerEventSinkProvided, layerStores, layerProviderSessionManagerProvided),
    ),
  );
  const layerRuntimeRequestServiceProvided = RuntimeRequestService.layer.pipe(
    Layer.provide(Layer.merge(layerStores, layerProviderSessionManagerProvided)),
  );
  const layerCheckpointRollbackServiceProvided = CheckpointRollbackService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerCheckpointServiceProvided,
        layerEventSinkProvided,
        IdAllocator.layer,
        layerStores,
        layerProviderSessionManagerProvided,
        ThreadCommandExecutor.layer,
        layerRuntime,
        layerServerConfig,
        layerServerSettings,
      ),
    ),
  );
  const layerCheckpointCaptureServiceProvided = CheckpointCaptureService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerCheckpointServiceProvided,
        layerEventSinkProvided,
        IdAllocator.layer,
        layerStores,
      ),
    ),
  );
  const layerRunFinalizationServiceProvided = RunFinalizationService.layer.pipe(
    Layer.provide(Layer.merge(layerCheckpointCaptureServiceProvided, layerStores)),
  );
  const layerThreadTitleRegenerationTest = Layer.succeed(
    ThreadTitleRegenerationService.ThreadTitleRegenerationService,
    ThreadTitleRegenerationService.ThreadTitleRegenerationService.of({
      execute: Effect.fn("ThreadTitleRegenerationService.execute")(function () {
        return Effect.void;
      }),
    }),
  );
  const layerOrchestratorProvided = Orchestrator.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerCheckpointServiceProvided,
        CommandPolicy.layer,
        layerContextHandoffServiceProvided,
        layerPersistence,
        layerStartRefusals,
        layerAdmissionProbe,
        options.checkoutIntentLayer ?? Layer.empty,
        layerProvidedRegistry,
        layerContinuationRequests,
        layerRuntime,
        layerProviderSessionManagerProvided,
        layerProviderSwitchServiceProvided,
        layerRunExecutionServiceProvided,
        layerServerSettings,
        ThreadForkService.layer,
      ),
    ),
  );
  const layerThreadManagementProvided = Layer.unwrap(
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      return Layer.mock(ThreadManagementService.ThreadManagementService)({
        dispatch: orchestrator.dispatch,
        getThreadShell: orchestrator.getThreadShell,
        getProjectThreadRecords: ({ threadId }, fields, filter) =>
          orchestrator.getThreadRecords(threadId, fields, filter).pipe(Effect.orDie),
        stopDelegatedTasks: ({ threadId }) =>
          orchestrator
            .getThreadRecords(threadId, ["subagents"])
            .pipe(
              Effect.flatMap(({ subagents }) =>
                subagents.length === 0
                  ? Effect.void
                  : Effect.die("Replay stop fixture requires explicit delegated-child handling"),
              ),
            ),
        getThreadRecords: orchestrator.getThreadRecords,
        getThreadProjection: orchestrator.getThreadProjection,
        recoverDelegatedTask: orchestrator.recoverDelegatedTask,
        cancelDelegatedTask: orchestrator.cancelDelegatedTask,
        delegatedTaskResultPending: orchestrator.delegatedTaskResultPending,
        streamStoredEventsFrom: orchestrator.streamStoredEventsFrom,
      });
    }),
  ).pipe(Layer.provide(layerOrchestratorProvided));
  const layerContinuationWorkerProvided =
    options.runContinuationWorker === true
      ? ProviderContinuationService.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              layerContinuationRequests,
              layerThreadManagementProvided,
              IdAllocator.layer,
            ),
          ),
        )
      : Layer.empty;
  const layerCurrentRuntimeStop = RuntimeStop.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerStores,
        layerEventSinkProvided,
        layerProviderSessionManagerProvided,
        layerRestartPreparation,
      ),
    ),
  );
  const layerEffectExecutorBase = EffectWorker.layerExecutor.pipe(
    Layer.provide(layerCurrentRuntimeStop),
    Layer.provide(layerRestartPreparation),
    Layer.provide(
      Layer.mergeAll(
        layerRunFinalizationServiceProvided,
        layerCheckpointRollbackServiceProvided,
        layerProviderSessionManagerProvided,
        layerProviderTurnControlServiceProvided,
        layerProviderTurnStartServiceProvided,
        layerRuntimeRequestServiceProvided,
        layerThreadTitleRegenerationTest,
        layerServerSettings,
        layerThreadManagementProvided,
        layerStores,
      ),
    ),
  );
  const beforeEffectExecution = options.beforeEffectExecution;
  const layerEffectExecutorProvided = Layer.effect(
    EffectWorker.OrchestrationEffectExecutorV2,
    Effect.gen(function* () {
      const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
      const runtimeStop = yield* RuntimeStop.CurrentRuntimeStop;
      return EffectWorker.OrchestrationEffectExecutorV2.of({
        ...executor,
        execute: (effect, executionOptions) =>
          (beforeEffectExecution?.(effect) ?? Effect.void).pipe(
            Effect.andThen(executor.execute(effect, executionOptions)),
            Effect.provideService(RuntimeStop.CurrentRuntimeStop, runtimeStop),
          ),
      });
    }),
  ).pipe(Layer.provide(Layer.merge(layerEffectExecutorBase, layerCurrentRuntimeStop)));
  const layerEffectWorkerProvided = EffectWorker.layer.pipe(
    Layer.provide(Layer.merge(layerStores, layerEffectExecutorProvided)),
  );
  const layerReplayRuntime = Layer.mergeAll(
    layerCurrentRuntimeStop,
    layerStores,
    layerServerSettings,
    layerThreadManagementProvided,
    layerProviderSwitchServiceProvided,
    layerProviderTurnStartServiceProvided,
    layerEffectExecutorProvided,
    layerEffectOutboxProvided,
    layerOrchestratorProvided,
    layerProviderSessionManagerProvided,
    layerEffectWorkerProvided,
    layerEventSinkProvided,
    layerContinuationWorkerProvided,
    layerCommandOutputHubProvided,
    ThreadCommandExecutor.layer,
  ).pipe(
    Layer.provide(ProviderTurnStartServiceTestkit.layerWorkspace),
    Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
  );

  if (options.runEffectWorker === false) {
    return layerReplayRuntime;
  }
  // Build recovery before runtime so it commits before the effect worker claims work.
  const layerStartupRecovery: Layer.Layer<never, ReplayInfrastructureError> =
    options.recoverOnStartup === true
      ? Layer.effectDiscard(
          ProviderRuntimeRecoveryService.ProviderRuntimeRecoveryService.use(
            (recovery) => recovery.recover,
          ).pipe(Effect.orDie),
        ).pipe(
          Layer.provide(ProviderRuntimeRecoveryService.layer),
          Layer.provide(
            // Environment identity needs the platform while it is built, not as a sibling.
            Layer.mergeAll(
              layerStores,
              layerEventSinkProvided,
              IdAllocator.layer,
              layerServerSettings,
              layerStartRefusals,
              layerRestartPreparation,
              layerControlAdmission,
              layerGoalContinuity,
              layerEnvironment,
            ).pipe(Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
          ),
        )
      : Layer.empty;
  return Layer.effect(
    Orchestrator.OrchestratorV2,
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      yield* orchestrator.recoverDelegatedTasks;
      yield* EffectWorker.runDaemon.pipe(Effect.forkScoped);
      return orchestrator;
    }),
  ).pipe(Layer.provideMerge(layerReplayRuntime), Layer.provide(layerStartupRecovery));
}
