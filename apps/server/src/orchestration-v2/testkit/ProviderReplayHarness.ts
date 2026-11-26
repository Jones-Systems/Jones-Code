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
  ModelSelection,
  OrchestrationV2ProviderThread,
  ProviderDriverKind,
  ProviderReplayTranscript,
  ThreadId,
} from "@t3tools/contracts";
import { ProviderRuntimeBinding } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import type * as SqlClient from "effect/sql/SqlClient";
import type { MigrationError } from "effect/sql/Migrator";
import type { SqlError } from "effect/sql/SqlError";

import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as CheckpointStore from "../../checkpointing/CheckpointStore.ts";
import * as ServerConfig from "../../config.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as ManagedPolicy from "../../policy/ManagedPolicy.ts";
import * as ThreadManagementService from "../ThreadManagementService.ts";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as McpSessionRegistryTestkit from "../../mcp/McpSessionRegistry.testkit.ts";
import type * as McpSessionRegistry from "../../mcp/McpSessionRegistry.ts";
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
import * as ThreadViewers from "../ThreadViewers.ts";
import * as EventStore from "../EventStore.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Orchestrator from "../Orchestrator.ts";
import * as ProjectionMaintenance from "../ProjectionMaintenance.ts";
import * as ProjectionStore from "../ProjectionStore.ts";
import * as ProjectStore from "../ProjectStore.ts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
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
import * as WorkspaceFolders from "../../workspace/WorkspaceFolders.ts";
import * as TaskFolderStore from "../../workspace/TaskFolderStore.ts";
import * as TurnItemPositionStore from "../TurnItemPositionStore.ts";
import * as RuntimeRequestService from "../RuntimeRequestService.ts";
import * as ThreadCommandExecutor from "../ThreadCommandExecutor.ts";
import * as ThreadForkService from "../ThreadForkService.ts";
import { ThreadWorktreeRestore } from "../../workspace/restoreMissingWorktree.ts";
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

interface ReplayWriter {
  readonly binding: typeof ProviderRuntimeBinding.Type;
  readonly pending: Set<Fiber.Fiber<void, ProviderAdapter.ProviderAdapterV2Error>>;
  retirement?: Deferred.Deferred<ProviderAdapter.RuntimeRetirementReceipt>;
}

const sameReplayBinding = Schema.toEquivalence(ProviderRuntimeBinding);

/** Let an in-memory adapter load synthetic native threads through the managed binding boundary. */
export const withReplayThreadPublication = Effect.fn(
  "ProviderReplayHarness.withReplayThreadPublication",
)(function* (
  input: ProviderAdapter.ProviderAdapterV2OpenSessionInput,
  runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime,
) {
  const lifecycle = input.runtimeLifecycle;
  if (lifecycle === undefined) {
    return yield* Effect.die("The replay adapter requires a managed runtime lifecycle.");
  }
  const runtimeGeneration = yield* lifecycle.reserve(input.threadId);
  yield* Effect.addFinalizer(() => lifecycle.abandon(runtimeGeneration).pipe(Effect.orDie));
  const ownsRetirement = runtime.retireThreadForTransfer === undefined;
  const writers = new Map<ReplayWriter["binding"]["providerThreadId"], ReplayWriter>();
  const producerStopped = yield* Deferred.make<void>();
  let producerStopping = false;
  const bindingError = (detail: string) =>
    new ProviderAdapter.ProviderRuntimeBindingError({ driver: runtime.driver, detail });
  const getWriter = (providerThread: OrchestrationV2ProviderThread) => {
    const writer = writers.get(providerThread.id);
    const binding = ProviderAdapter.runtimeBinding(providerThread, runtimeGeneration);
    return writer !== undefined &&
      binding !== undefined &&
      providerThread.runtimeIdentity?.runtimeGeneration === runtimeGeneration &&
      sameReplayBinding(writer.binding, binding)
      ? writer
      : undefined;
  };
  const admit = (threadId: ThreadId) =>
    Effect.suspend(() =>
      producerStopping
        ? bindingError("The synthetic producer has stopped.")
        : lifecycle.admit({ runtimeGeneration, threadId }),
    );
  const publish = (providerThread: OrchestrationV2ProviderThread, selection: ModelSelection) =>
    lifecycle
      .bind({
        providerThread,
        runtimeGeneration,
        requested: ProviderAdapter.requestedRuntimeIdentity(selection, runtime.driver),
        observed: ProviderAdapter.unobservedRuntimeIdentity(),
      })
      .pipe(
        Effect.tap((thread) =>
          Effect.sync(() => {
            if (!ownsRetirement) return;
            const binding = ProviderAdapter.runtimeBinding(thread, runtimeGeneration);
            if (binding !== undefined && getWriter(thread) === undefined) {
              writers.set(thread.id, {
                binding,
                pending: new Set(),
              });
            }
          }),
        ),
      );
  const withWriter = (
    thread: OrchestrationV2ProviderThread,
    action: Effect.Effect<void, ProviderAdapter.ProviderAdapterV2Error>,
  ) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const writer = getWriter(thread);
        if (producerStopping || writer === undefined || writer.retirement !== undefined)
          return yield* bindingError("The synthetic writer is no longer attached.");
        const ready = Deferred.makeUnsafe<void>();
        const operation = yield* Effect.forkChild(
          Deferred.await(ready).pipe(Effect.andThen(action), Effect.interruptible),
        );
        writer.pending.add(operation);
        return yield* Effect.gen(function* () {
          if (writer.retirement === undefined) yield* Deferred.succeed(ready, undefined);
          else yield* Fiber.interrupt(operation);
          const result = yield* restore(Fiber.await(operation));
          if (
            writer.retirement !== undefined &&
            Exit.isFailure(result) &&
            Cause.hasInterruptsOnly(result.cause)
          )
            return;
          return yield* result;
        }).pipe(
          Effect.ensuring(
            Fiber.interrupt(operation).pipe(
              Effect.andThen(Effect.sync(() => writer.pending.delete(operation))),
            ),
          ),
        );
      }),
    );
  const retireThreadForTransfer: NonNullable<
    ProviderAdapter.ProviderAdapterV2SessionRuntime["retireThreadForTransfer"]
  > = ({ providerThread, allowProducerRetirement }) =>
    Effect.gen(function* () {
      const writer = getWriter(providerThread);
      if (writer === undefined)
        return yield* bindingError("The captured synthetic writer binding changed.");
      if (writer.retirement !== undefined) return yield* Deferred.await(writer.retirement);
      const receipt = Deferred.makeUnsafe<ProviderAdapter.RuntimeRetirementReceipt>();
      writer.retirement = receipt;
      const exits =
        allowProducerRetirement &&
        !runtime.providerSession.capabilities.sessions.supportsMultipleProviderThreadsPerSession &&
        [...writers.values()].every((candidate) => candidate.retirement !== undefined);
      if (exits) producerStopping = true;
      yield* Fiber.interruptAll([...writer.pending]);
      if (exits) {
        yield* Effect.forEach(
          [...writers.values()].filter((candidate) => candidate !== writer),
          (candidate) => Deferred.await(candidate.retirement!),
          { discard: true },
        );
        yield* Deferred.succeed(producerStopped, undefined);
      }
      const result = {
        binding: writer.binding,
        evidence: exits ? "producer_exited" : "thread_closed",
      } satisfies ProviderAdapter.RuntimeRetirementReceipt;
      yield* Deferred.succeed(receipt, result);
      return result;
    }).pipe(Effect.uninterruptible);
  const stoppedEvent = Stream.unwrap(
    Effect.gen(function* () {
      if (!(yield* Deferred.isDone(producerStopped))) return Stream.empty;
      const updatedAt = yield* DateTime.now;
      return Stream.make({
        type: "provider_session.updated",
        driver: runtime.driver,
        providerSession: { ...runtime.providerSession, status: "stopped", updatedAt },
      } satisfies ProviderAdapter.ProviderAdapterV2Event);
    }),
  );
  return {
    ...runtime,
    ...(ownsRetirement
      ? {
          // A stopped marker lets the manager drain an exclusive synthetic producer gracefully.
          events: runtime.events.pipe(
            Stream.interruptWhen(Deferred.await(producerStopped)),
            Stream.concat(stoppedEvent),
          ),
          retireThreadForTransfer,
          startTurn: (turn: ProviderAdapter.ProviderAdapterV2TurnInput) =>
            withWriter(turn.providerThread, runtime.startTurn(turn)),
          ...(runtime.resumeTurn === undefined
            ? {}
            : {
                resumeTurn: (turn: ProviderAdapter.ProviderAdapterV2ResumeTurnInput) =>
                  withWriter(turn.providerThread, runtime.resumeTurn!(turn)),
              }),
          ...(runtime.compactThread === undefined
            ? {}
            : {
                compactThread: (turn: ProviderAdapter.ProviderAdapterV2TurnInput) =>
                  withWriter(turn.providerThread, runtime.compactThread!(turn)),
              }),
        }
      : {}),
    ensureThread: (load) =>
      admit(load.threadId).pipe(
        Effect.andThen(runtime.ensureThread(load)),
        Effect.flatMap((thread) =>
          publish(
            load.existingProviderThread === undefined
              ? thread
              : {
                  ...thread,
                  id: load.existingProviderThread.id,
                  ownerNodeId: load.existingProviderThread.ownerNodeId,
                  createdAt: load.existingProviderThread.createdAt,
                },
            load.modelSelection,
          ),
        ),
      ),
    resumeThread: (load) => {
      const threadId = load.threadId ?? load.providerThread.appThreadId;
      if (threadId === null) {
        return Effect.fail(
          new ProviderAdapter.ProviderAdapterProtocolError({
            driver: runtime.driver,
            detail: "The replay adapter requires an owning thread for a managed resume.",
          }),
        );
      }
      return admit(threadId).pipe(
        Effect.andThen(runtime.resumeThread(load)),
        Effect.flatMap((thread) =>
          publish(
            { ...thread, appThreadId: threadId, providerSessionId: runtime.providerSessionId },
            load.modelSelection ?? input.modelSelection,
          ),
        ),
      );
    },
  } satisfies ProviderAdapter.ProviderAdapterV2SessionRuntime;
});

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
      workflowsDir: path.join(baseDir, "workflows"),
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
    readonly providerInstances?: Partial<
      ProviderInstanceRegistry.ProviderInstanceRegistry["Service"]
    >;
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
    readonly providerInstances?: Partial<
      ProviderInstanceRegistry.ProviderInstanceRegistry["Service"]
    >;
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
    readonly mcpSessionRegistryLayer?: Layer.Layer<McpSessionRegistry.McpSessionRegistry>;
    readonly startRefusalProbeLayer?: Layer.Layer<StartRefusalProbe.Probe> | false;
    readonly isMcpCallerAttached?: ProviderSessionManager.ProviderSessionManagerV2["Service"]["isMcpCallerAttached"];
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
    /** The instances the ingestor reads snapshots from, such as an endpoint behind a hub. */
    readonly providerInstances?: Partial<
      ProviderInstanceRegistry.ProviderInstanceRegistry["Service"]
    >;
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
  const layerServerConfig = Layer.effect(
    ServerConfig.ServerConfig,
    makeReplayServerConfig(scenario.name).pipe(Effect.orDie),
  ).pipe(Layer.provide(NodeServices.layer));
  // A thread without a worktree would otherwise fall back to process.cwd(), so checkpoints
  // would run git against the checkout running the tests and race its other users.
  const layerScenarioWorkspacePolicy = Layer.effect(
    RuntimePolicy.RuntimePolicyV2,
    Effect.gen(function* () {
      const workspace = yield* checkpointWorkspace(
        scenario.name.replace(/[^a-z0-9_-]+/gi, "-"),
      ).pipe(Effect.orDie);
      return {
        resolve: (input) =>
          Effect.succeed({
            runtimeMode: input.thread.runtimeMode,
            interactionMode: input.thread.interactionMode,
            cwd: input.thread.worktreePath ?? workspace,
          }),
      };
    }),
  );
  const layerRuntimeBase =
    scenario.runtimePolicyOverride === undefined
      ? layerScenarioWorkspacePolicy
      : RuntimePolicy.layerWithOverride(scenario.runtimePolicyOverride).pipe(
          Layer.provide(layerScenarioWorkspacePolicy),
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
  const layerAdmissionProbe =
    options.startRefusalProbeLayer === false
      ? Layer.empty
      : (options.startRefusalProbeLayer ??
        StartRefusalProbe.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              layerStores,
              layerControlAdmission,
              layerGoalContinuity,
              layerEnvironment,
            ),
          ),
        ));
  const layerEventSinkProvided = EventSink.layerFromStores.pipe(
    Layer.provide(Layer.mergeAll(layerStores, layerDatabase, ThreadViewers.layer)),
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
        Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)(
          options.providerInstances ?? { getInstance: () => Effect.undefined },
        ),
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
    Layer.provide(
      Layer.mergeAll(
        layerCheckpointStore,
        IdAllocator.layer,
        WorkspaceFolders.layer.pipe(
          Layer.provide(layerStores),
          Layer.provide(TaskFolderStore.layer.pipe(Layer.provide(layerDatabase))),
        ),
      ),
    ),
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
        options.mcpSessionRegistryLayer ?? McpSessionRegistryTestkit.layer,
        layerServerSettings,
        layerProviderEventIngestorProvided,
        layerStores,
        ProviderInstallationGate.layer,
      ),
    ),
  );
  const { onSessionDetached, isMcpCallerAttached } = options;
  const layerProviderSessionManagerProvided =
    onSessionDetached === undefined && isMcpCallerAttached === undefined
      ? layerProviderSessionManagerBase
      : Layer.effect(
          ProviderSessionManager.ProviderSessionManagerV2,
          Effect.gen(function* () {
            const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
            return ProviderSessionManager.ProviderSessionManagerV2.of({
              ...sessions,
              ...(onSessionDetached === undefined
                ? {}
                : {
                    detach: (input) =>
                      sessions.detach(input).pipe(Effect.tap(() => onSessionDetached(input))),
                  }),
              ...(isMcpCallerAttached === undefined ? {} : { isMcpCallerAttached }),
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
        Layer.mock(ThreadWorktreeRestore)({ takeRecreated: () => Effect.undefined }),
      ),
    ),
  );
  const layerProviderTurnControlServiceProvided = ProviderTurnControlService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerEventSinkProvided,
        McpAppModelContext.layerEmpty,
        layerStores,
        layerProviderSessionManagerProvided,
      ),
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
        ThreadCommandExecutor.layer,
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
        options.mcpSessionRegistryLayer ?? Layer.empty,
        layerProviderSessionManagerProvided,
        layerProviderSwitchServiceProvided,
        layerRunExecutionServiceProvided,
        layerServerSettings,
        TaskFolderStore.layer.pipe(Layer.provide(layerDatabase)),
        ThreadForkService.layer,
      ),
    ),
  );
  const layerThreadManagementProvided = Layer.unwrap(
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      return Layer.mock(ThreadManagementService.ThreadManagementService)({
        getCommandReceipt: receipts.getByCommandId,
        dispatch: orchestrator.dispatch,
        getThreadShell: orchestrator.getThreadShell,
        listActiveShellIndex: orchestrator.listActiveShellIndex,
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
  ).pipe(Layer.provide(Layer.merge(layerOrchestratorProvided, layerCommandReceiptStoreProvided)));
  const layerContinuationWorkerProvided =
    options.runContinuationWorker === true
      ? ProviderContinuationService.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              layerContinuationRequests,
              layerStartRefusals,
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
        ProjectionMaintenance.layer.pipe(Layer.provide(layerDatabase)),
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
