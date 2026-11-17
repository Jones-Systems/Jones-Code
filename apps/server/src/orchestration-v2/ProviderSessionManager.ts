import * as DutyRelease from "../jones/workspaceRetirement/DutyRelease.ts";
import * as ConversationPolicy from "../jones/conversationWorkspace/ConversationPolicy.ts";
import * as DecisionSnapshotAccess from "../jones/mcp/decisionSnapshot/access.ts";
import { PROVIDER_SCOPE_CLOSE_TIMEOUT_MS } from "../jones/cloud/shutdownBudget.ts";
import { WorkspaceLifecycle as WorkspaceLifecycleService } from "../jones/workspaceLifecycle/WorkspaceLifecycle.ts";
import { withOperation } from "../jones/workspaceLifecycle/NativeOperation.ts";
import * as WorkspaceLifecycle from "../jones/workspaceLifecycle/NativeBoundary.ts";
import { OpenDutyClaim } from "../jones/workspaceRetirement/DutyLifetimeLink.ts";
import * as WorkspaceDuty from "../jones/workspaceRetirement/Duty.ts";
import type * as RuntimeAttachment from "../jones/runtime/CurrentThreadRuntimeAttachment.ts";
import type * as RuntimeObservation from "../jones/provider/observations/ProviderThreadRuntimeObservation.ts";
import { selectTransferBindings } from "../jones/providerRetirement/transferBindings.ts";
import { makeProviderRecoveryPublication } from "../jones/providerRetirement/providerRecoveryPublication.ts";
import { makeProviderRecoveryRetirements } from "../jones/providerRetirement/providerRecoveryRetirement.ts";
import * as RetirementLedger from "../jones/providerRetirement/RetirementLedger.ts";
import * as AccountTransfer from "../jones/providerRetirement/accountTransferRetirement.ts";
import * as PhysicalRuntimeEvidence from "../jones/runtime/PhysicalRuntimeEvidence.ts";
import * as NativeProvider from "../jones/nativeCreation/NativeCreationProviderGuard.ts";
import * as ProviderEventOrigin from "@t3tools/provider-core/server/jones/ProviderEventOrigin";
import * as EventStore from "./EventStore.ts";
import * as StartAdmission from "../jones/provider/StartAdmission.ts";
import * as RuntimeUseOpenGuard from "../jones/cleanup/RuntimeUseOpenGuard.ts";
import { RuntimeUseError } from "../jones/cleanup/RuntimeUseProtocol.ts";
import * as Deferred from "effect/Deferred";
import {
  runtimeBinding,
  unobservedRuntimeIdentity,
  ProviderRuntimeBindingError,
  type ProviderRuntimeLifecycle,
  type RuntimeRetirementReceipt,
  type ProviderAdapterV2SessionRuntime,
} from "@t3tools/provider-core/server/ProviderAdapter";
import { makeRetainedNativeLoads } from "../jones/provider/RetainedNativeLoads.ts";
import * as WorkModeRetention from "../jones/workMode/Retention.ts";
import { AgentScope } from "@t3tools/shared/AgentScope";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import { modelSelectionsEqual } from "@t3tools/shared/model";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import {
  isOrchestrationV2WorkActive,
  ModelSelection,
  type OrchestrationV2AppThread,
  OrchestrationV2DomainEvent,
  OrchestrationV2ProviderSession,
  type OrchestrationV2PromptSuggestion,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  OrchestrationV2RuntimeRequest,
  type OrchestrationV2Subagent,
  PhysicalProcessObservation,
  ProviderInstanceId,
  ProviderRuntimeBinding as ProviderAdapterBinding,
  type ProviderRuntimeBinding,
  ProviderSessionId,
  ThreadId,
  type OrchestrationV2TurnItem,
  type ProviderThreadId,
  type ProviderTurnId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FiberSet from "effect/FiberSet";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";

import { normalizeModelMetricLabel } from "../observability/Attributes.ts";
import {
  providerSessionsTotal,
  providerTurnDuration,
  providerTurnsTotal,
  withMetrics,
} from "../observability/Metrics.ts";
import { ProviderWorkspaceMissingError } from "../provider/Errors.ts";
import * as ProviderInstallationGate from "../provider/ProviderInstallationGate.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ServerSettings from "../serverSettings.ts";
import * as McpSessionRegistry from "../mcp/McpSessionRegistry.ts";
import * as EventSink from "./EventSink.ts";
import * as CommandOutputHub from "./CommandOutputHub.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderEventIngestor from "./ProviderEventIngestor.ts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import {
  endOrphanedNativeSubagent,
  endRunlessRootTurns,
} from "@t3tools/provider-core/server/subagentProjection";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

const isRuntimeUseError = Schema.is(RuntimeUseError);

const DEFAULT_IDLE_TIMEOUT_MS = 30 * 60 * 1000;
const DEFAULT_MAX_IDLE_PIN_MS = 4 * 60 * 60 * 1000;
// Jones: the shared shutdown budget derives the server child grace from this bound.
const RELEASE_SCOPE_CLOSE_TIMEOUT_MS = PROVIDER_SCOPE_CLOSE_TIMEOUT_MS;
/** Past adapters' own 10 s stop fallbacks, so they settle first when the provider answers at all. */
export const PROVIDER_INTERRUPT_TIMEOUT_MS = 15 * 1000;

const busyTurnPrefix = (providerThreadId: ProviderThreadId) => `${providerThreadId}#`;
const busyTurnKey = (providerThreadId: ProviderThreadId, runOrdinal: number) =>
  `${busyTurnPrefix(providerThreadId)}${runOrdinal}`;
interface OwnBusyTurns {
  readonly providerThreadIds: ReadonlyArray<ProviderThreadId>;
  readonly throughRunOrdinal: number;
}
const isOwnBusyTurn = (turnKey: string, own: OwnBusyTurns) =>
  own.providerThreadIds.some((id) => {
    const prefix = busyTurnPrefix(id);
    return (
      turnKey.startsWith(prefix) && Number(turnKey.slice(prefix.length)) <= own.throughRunOrdinal
    );
  });
const UNLOAD_THREAD_TIMEOUT_MS = 10 * 1000;
// Providers can announce a retry and never resume. Longer than any adapter's own stall handling.
const PROVIDER_RETRY_STALL_TIMEOUT = Duration.minutes(15);
type ProviderErrorItem = Extract<OrchestrationV2TurnItem, { readonly type: "error" }>;
type ProviderRetryItem = ProviderErrorItem & {
  readonly providerTurnId: ProviderTurnId;
  readonly providerThreadId: ProviderThreadId;
  readonly retry: NonNullable<ProviderErrorItem["retry"]>;
};
/** Suggestions outlive their sessions, so keep only the most recently updated threads. */
const MAX_PROMPT_SUGGESTIONS = 256;
const WORKING_RUN_STATUSES: ReadonlySet<string> = new Set([
  "preparing",
  "starting",
  "running",
  "waiting",
]);

/** Matches planDelegatedCompletionDelivery: true while a delegated task's result still wakes the thread. */
export const delegatedTaskWillWake = (records: {
  readonly thread: Pick<OrchestrationV2AppThread, "archivedAt" | "deletedAt">;
  readonly runs: ReadonlyArray<Pick<OrchestrationV2Run, "id" | "delegatedCompletion">>;
  readonly subagents: ReadonlyArray<
    Pick<OrchestrationV2Subagent, "origin" | "status" | "runId" | "completionDelivery">
  >;
}): boolean =>
  records.thread.archivedAt === null &&
  records.thread.deletedAt === null &&
  records.subagents.some((task) => {
    const run = records.runs.find((candidate) => candidate.id === task.runId);
    const delivery = task.completionDelivery?.state;
    return (
      task.origin === "app_owned" &&
      isOrchestrationV2WorkActive(task.status) &&
      run !== undefined &&
      (run.delegatedCompletion?.disposition ?? "open") === "open" &&
      delivery !== "acknowledged" &&
      delivery !== "delivered" &&
      delivery !== "disposed"
    );
  });

export const ProviderSessionReleaseReason = Schema.Literals([
  "idle_timeout",
  "runtime_error",
  "manual_shutdown",
  "server_shutdown",
]);
export type ProviderSessionReleaseReason = typeof ProviderSessionReleaseReason.Type;

// Tool calls that only a live provider process can end.
const releasedToolCallTypes: ReadonlyArray<OrchestrationV2TurnItem["type"]> = [
  "command_execution",
  "dynamic_tool",
];
const activeTurnItemStatuses: ReadonlyArray<OrchestrationV2TurnItem["status"]> = [
  "pending",
  "running",
  "waiting",
];

export class ProviderSessionOpenError extends Schema.TaggedError<ProviderSessionOpenError>()(
  "ProviderSessionOpenError",
  {
    instanceId: ProviderInstanceId,
    providerSessionId: ProviderSessionId,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to open provider instance ${this.instanceId} session ${this.providerSessionId}.`;
  }
}

export class ProviderSessionLookupError extends Schema.TaggedError<ProviderSessionLookupError>()(
  "ProviderSessionLookupError",
  {
    providerSessionId: ProviderSessionId,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to look up provider session ${this.providerSessionId}.`;
  }
}

export class ProviderSessionCloseError extends Schema.TaggedError<ProviderSessionCloseError>()(
  "ProviderSessionCloseError",
  {
    providerSessionId: ProviderSessionId,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to close provider session ${this.providerSessionId}.`;
  }
}

export class ProviderSessionReleaseError extends Schema.TaggedError<ProviderSessionReleaseError>()(
  "ProviderSessionReleaseError",
  {
    providerSessionId: ProviderSessionId,
    reason: ProviderSessionReleaseReason,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to release provider session ${this.providerSessionId}.`;
  }
}

export class ProviderSessionActivityError extends Schema.TaggedError<ProviderSessionActivityError>()(
  "ProviderSessionActivityError",
  {
    providerSessionId: ProviderSessionId,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to update provider session activity for ${this.providerSessionId}.`;
  }
}

export const ProviderSessionManagerV2Error = Schema.Union([
  ProviderSessionOpenError,
  ProviderWorkspaceMissingError,
  ProviderSessionLookupError,
  ProviderSessionCloseError,
  ProviderSessionReleaseError,
  ProviderSessionActivityError,
]);
export type ProviderSessionManagerV2Error = typeof ProviderSessionManagerV2Error.Type;

export interface ProviderSessionManagerV2Shape {
  readonly readCurrentThreadRuntimeAttachment?: (
    threadId: ThreadId,
  ) => Effect.Effect<RuntimeAttachment.CurrentThreadRuntimeAttachment>;
  readonly observeThreadActivity?: (
    threadId: ThreadId,
  ) => Effect.Effect<RuntimeObservation.ProviderRuntimeObservation>;
  readonly hasRetiredWriter?: (input: {
    readonly threadId: ThreadId;
    readonly providerSessionId: ProviderSessionId;
  }) => Effect.Effect<boolean>;
  readonly awaitThreadRetirement?: (input: {
    readonly threadId: ThreadId;
    readonly instanceId: ProviderInstanceId;
  }) => Effect.Effect<void, RetirementLedger.WriterRetirementError>;
  readonly reconcileThreadBindingPublication?: (input: {
    readonly run: OrchestrationV2Run;
    readonly providerThread: OrchestrationV2ProviderThread;
  }) => Effect.Effect<OrchestrationV2ProviderThread | null, ProviderSessionManagerV2Error>;
  readonly captureThreadRetirement?: (
    binding: ProviderRuntimeBinding,
  ) => Effect.Effect<
    ProviderAdapter.CapturedRuntimeRetirement | null,
    ProviderSessionManagerV2Error
  >;
  readonly shutdown: Effect.Effect<void>;
  readonly open: (input: {
    readonly threadId: ThreadId;
    readonly providerSessionId: ProviderSessionId;
    readonly modelSelection: ModelSelection;
    readonly runtimePolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy;
    readonly resumeFromSession?: OrchestrationV2ProviderSession;
    readonly revalidateStartAdmission?: Effect.Effect<void, ProviderAdapter.ProviderAdapterV2Error>;
    readonly nativeCreationGuard?: ProviderAdapter.ProviderAdapterV2TurnInput["nativeCreationGuard"];
    readonly initialNativeThreadId?: string;
    readonly initialProviderItemIdentityVersion?: 2;
  }) => Effect.Effect<
    ProviderAdapter.ProviderAdapterV2SessionRuntime,
    ProviderSessionManagerV2Error
  >;
  readonly get: (
    providerSessionId: ProviderSessionId,
    options?: { readonly touchActivity?: boolean; readonly controlScope?: Scope.Scope },
  ) => Effect.Effect<
    Option.Option<ProviderAdapter.ProviderAdapterV2SessionRuntime>,
    ProviderSessionManagerV2Error
  >;
  readonly captureCurrentThreadRuntimeStop?: (
    threadId: ThreadId,
  ) => Effect.Effect<
    ProviderAdapter.CapturedRuntimeStop | null,
    ProviderAdapter.ProviderAdapterV2Error
  >;
  readonly isMcpCallerAttached: (input: {
    readonly threadId: ThreadId;
    readonly providerSessionId: ProviderSessionId;
    readonly providerInstanceId: ProviderInstanceId;
    readonly mcpCredentialId: string;
  }) => Effect.Effect<boolean>;
  readonly settleHandoff?: <E, R>(
    input: HandoffSettlementInput<E, R>,
  ) => Effect.Effect<void, E | ProviderSessionLookupError, R>;
  readonly isLive: (providerSessionId: ProviderSessionId) => Effect.Effect<boolean>;
  readonly close: (
    providerSessionId: ProviderSessionId,
  ) => Effect.Effect<void, ProviderSessionManagerV2Error>;
  readonly closeInstance: (
    instanceId: ProviderInstanceId,
  ) => Effect.Effect<void, ProviderSessionManagerV2Error>;
  readonly release: (input: {
    readonly providerSessionId: ProviderSessionId;
    readonly reason: ProviderSessionReleaseReason;
    readonly detail?: string;
  }) => Effect.Effect<void, ProviderSessionManagerV2Error>;
  readonly detach: (input: {
    readonly providerSessionId: ProviderSessionId;
    readonly threadId: ThreadId;
    readonly detail?: string;
    readonly revokeMcpCredential?: boolean;
    readonly providerThreadId?: ProviderThreadId;
    readonly replacementProviderSessionId?: ProviderSessionId;
    readonly retireOnAccountTransfer?: boolean;
    readonly requireWriterRetirement?: boolean;
  }) => Effect.Effect<void, ProviderSessionManagerV2Error>;
  /**
   * Releases the session unless, at the moment of removal, a turn is busy on it other than one
   * of `ownProviderThreadIds` from a run up to `throughRunOrdinal`. Checked atomically with turn
   * starts. Succeeds with whether it was released.
   */
  readonly releaseUnlessBusy: (input: {
    readonly providerSessionId: ProviderSessionId;
    readonly ownProviderThreadIds: ReadonlyArray<ProviderThreadId>;
    readonly throughRunOrdinal: number;
    /** Keeps the session when it succeeds with true, checked atomically with the removal. */
    readonly keepIf?: Effect.Effect<boolean>;
    readonly reason: ProviderSessionReleaseReason;
    readonly detail?: string;
  }) => Effect.Effect<boolean, ProviderSessionManagerV2Error>;
  /**
   * Other threads with a run or background work on this session's shared process; empty when
   * it serves one thread.
   */
  readonly otherWorkingThreads: (input: {
    readonly providerSessionId: ProviderSessionId;
    readonly threadId: ThreadId;
  }) => Effect.Effect<ReadonlyArray<ThreadId>>;
  /** The thread's latest predicted next prompt, then each replacement. */
  readonly subscribePromptSuggestion: (
    threadId: ThreadId,
  ) => Stream.Stream<OrchestrationV2PromptSuggestion>;
}

export class ProviderSessionManagerV2 extends Context.Service<
  ProviderSessionManagerV2,
  ProviderSessionManagerV2Shape
>()("t3/orchestration-v2/ProviderSessionManager/ProviderSessionManagerV2") {}

interface CompletedThreadTransfer {
  readonly source: ProviderAdapterV2SessionRuntime;
  readonly bindings: ReadonlyArray<ProviderRuntimeBinding>;
  readonly providerThreadId: ProviderThreadId | undefined;
  readonly replacementProviderSessionId: ProviderSessionId | undefined;
}

interface HandoffSettlementInput<E, R> {
  readonly runtime: ProviderAdapterV2SessionRuntime;
  readonly threadId: ThreadId;
  readonly providerThreadId: ProviderThreadId;
  readonly providerTurnId: ProviderTurnId;
  readonly settle: Effect.Effect<unknown, E, R>;
}

interface ProviderSessionEventSubscriber {
  readonly queue: Queue.Queue<ProviderSessionEventSignal, Cause.Done>;
  readonly settled: Deferred.Deferred<void>;
}

interface LiveSessionEntry {
  readonly eventPumpSettled: Deferred.Deferred<void>;
  readonly failureOwnership: { readonly permit: Semaphore.Semaphore; failed: boolean };
  readonly completedThreadTransfers: ReadonlyMap<ThreadId, CompletedThreadTransfer>;
  readonly attachedThreadIds: ReadonlySet<ThreadId>;
  readonly loadedProviderThreadKeyByThread: ReadonlyMap<ThreadId, string>;
  /** null marks an unknown catalog; undefined means MCP refresh is not configured. */
  readonly openedWithLinkedEnvironments: boolean | null | undefined;
  readonly linkedEnvironmentsByThread: ReadonlyMap<ThreadId, boolean | null>;
  /** Scope revocation to recorded credential ids so superseded sessions cannot revoke replacements opened before detach. */
  readonly mcpCredentialIdByThread: ReadonlyMap<ThreadId, string>;
  readonly supportsMultipleProviderThreads: boolean;
  readonly runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime;
  readonly exposedRuntime: ProviderAdapter.ProviderAdapterV2SessionRuntime;
  readonly eventSubscribers: Ref.Ref<ReadonlyMap<number, ProviderSessionEventSubscriber>>;
  readonly requestEventPermit: Semaphore.Semaphore;
  readonly scope: Scope.Closeable;
  readonly idleGeneration: number;
  readonly busyTurns: ReadonlySet<string>;
  readonly idleFiber: Fiber.Fiber<void, never> | null;
  readonly pinnedSinceMs: number | null;
  readonly idleThreadUnloads: ReadonlyMap<ThreadId, IdleThreadUnload>;
}

interface IdleThreadUnload {
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly generation: number;
  readonly fiber: Fiber.Fiber<void, never> | null;
}

type ProviderSessionEventSignal =
  | { readonly type: "event"; readonly event: ProviderAdapter.ProviderAdapterV2Event }
  | {
      readonly type: "failure";
      readonly cause: Cause.Cause<ProviderAdapter.ProviderAdapterV2Error>;
    };

export interface ProviderSessionManagerV2LayerOptions {
  readonly idleTimeoutMs?: number;
  readonly maxIdlePinMs?: number;
  readonly configureMcp?: boolean;
}

function releaseStatusFor(
  reason: ProviderSessionReleaseReason,
): OrchestrationV2ProviderSession["status"] {
  return reason === "runtime_error" ? "error" : "stopped";
}

function releasedRuntimeRequestStatusFor(
  reason: ProviderSessionReleaseReason,
): OrchestrationV2RuntimeRequest["status"] {
  return reason === "manual_shutdown" || reason === "server_shutdown" ? "cancelled" : "expired";
}

function sessionKey(providerSessionId: ProviderSessionId): string {
  return String(providerSessionId);
}

function sessionScopedArtifactThreadId(
  event: ProviderAdapter.ProviderAdapterV2Event,
): ThreadId | undefined {
  switch (event.type) {
    case "provider_thread.updated":
      return event.sessionScoped === true
        ? (event.providerThread.appThreadId ?? undefined)
        : undefined;
    case "runtime_request.updated":
      return event.runtimeRequest.providerTurnId === null ? event.threadId : undefined;
    case "node.updated":
      return event.node.runId === null && event.node.runtimeRequestId !== null
        ? event.node.threadId
        : undefined;
    case "turn_item.updated":
      if (event.turnItem.type === "command_execution")
        return event.sessionScoped === true ? event.turnItem.threadId : undefined;
      return event.turnItem.runId === null &&
        (event.turnItem.type === "approval_request" || event.turnItem.type === "user_input_request")
        ? event.turnItem.threadId
        : undefined;
    default:
      return undefined;
  }
}

function providerThreadRuntimeKey(
  providerThread: Parameters<
    ProviderAdapter.ProviderAdapterV2SessionRuntime["resumeThread"]
  >[0]["providerThread"],
): string {
  const nativeThreadRef = providerThread.nativeThreadRef;
  return JSON.stringify([
    providerThread.id,
    providerThread.providerSessionId,
    providerThread.providerInstanceId,
    providerThread.runtimeIdentity?.runtimeGeneration ?? null,
    nativeThreadRef?.driver ?? null,
    nativeThreadRef?.nativeId ?? null,
  ]);
}

function providerThreadLoadKey(input: {
  readonly runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime;
  readonly providerThread: Parameters<
    ProviderAdapter.ProviderAdapterV2SessionRuntime["resumeThread"]
  >[0]["providerThread"];
  readonly modelSelection?: ModelSelection;
  readonly runtimePolicy?: ProviderAdapter.ProviderAdapterV2RuntimePolicy;
}): string {
  const modelSelection =
    input.modelSelection === undefined
      ? undefined
      : (input.runtime.normalizeThreadLoadModelSelection?.(input.modelSelection) ??
        input.modelSelection);
  return JSON.stringify({
    providerThread: providerThreadRuntimeKey(input.providerThread),
    modelSelection: modelSelection ?? null,
    runtimePolicy: input.runtimePolicy ?? null,
  });
}

export const layerWithOptions = (
  options: ProviderSessionManagerV2LayerOptions = {},
): Layer.Layer<
  ProviderSessionManagerV2,
  never,
  | CommandOutputHub.CommandOutputHub
  | EventSink.EventSinkV2
  | EventStore.EventStoreV2
  | FileSystem.FileSystem
  | IdAllocator.IdAllocatorV2
  | McpProviderSessions.McpProviderSessions
  | McpSessionRegistry.McpSessionRegistry
  | ProjectionStore.ProjectionStoreV2
  | ProviderEventIngestor.ProviderEventIngestorV2
  | ProviderAdapterRegistry.ProviderAdapterRegistryV2
  | ProviderInstallationGate.ProviderInstallationGate
> =>
  Layer.effect(
    ProviderSessionManagerV2,
    Effect.gen(function* () {
      const registry = yield* ProviderAdapterRegistry.ProviderAdapterRegistryV2;
      const fileSystem = yield* FileSystem.FileSystem;
      const mcpSessionRegistry = yield* McpSessionRegistry.McpSessionRegistry;
      const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
      /** Unreadable settings must preserve explicit browser-access opt-outs. */
      const serverSettings = yield* Effect.serviceOption(ServerSettings.ServerSettingsService);
      const projectService = yield* Effect.serviceOption(ProjectService.ProjectService);
      const eventSink = yield* EventSink.EventSinkV2;
      const eventStore = yield* EventStore.EventStoreV2;
      const physicalEvidence = yield* PhysicalRuntimeEvidence.make;
      const runtimeUseGuard = yield* RuntimeUseOpenGuard.make;
      const commandOutput = yield* CommandOutputHub.CommandOutputHub;
      const promptSuggestionStore = yield* SubscriptionRef.make<
        ReadonlyMap<ThreadId, NonNullable<OrchestrationV2PromptSuggestion>>
      >(new Map());
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const conversationPolicy = yield* Effect.serviceOption(ConversationPolicy.ConversationPolicy);
      const providerEventIngestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const agentScope = yield* AgentScope;
      const installationGate = yield* ProviderInstallationGate.ProviderInstallationGate;
      yield* WorkspaceDuty.startup;
      const workspaceDuty = yield* WorkspaceDuty.hooks;
      const dutyRelease = yield* DutyRelease.make;
      const workspaceDuties = new Map<
        Scope.Closeable,
        Effect.Success<ReturnType<typeof workspaceDuty.hold>>
      >();
      type OriginalThreadLoad = {
        target:
          | (Pick<
              OrchestrationV2ProviderThread,
              "id" | "appThreadId" | "providerSessionId" | "providerInstanceId" | "driver"
            > & {
              readonly nativeThreadId: string | null;
            })
          | undefined;
        readonly loadKey: string;
        readonly settings: {
          readonly modelSelection?: ModelSelection;
          readonly runtimePolicy?: ProviderAdapter.ProviderAdapterV2RuntimePolicy;
        };
        // A captured undefined means MCP refresh is unconfigured, not an unfinished read.
        linkedEnvironments: { readonly value: boolean | null | undefined } | undefined;
        binding: ProviderRuntimeBinding | undefined;
        publication:
          | {
              readonly handle: ProviderAdapter.CapturedRuntimeBindingPublication;
              readonly readCompleted: Effect.Effect<
                OrchestrationV2ProviderThread | null,
                ProviderAdapter.ProviderAdapterV2Error
              >;
            }
          | undefined;
      };
      const lifecycleOwners = new WeakMap<Scope.Closeable, { active: boolean }>();
      const originalThreadLoads = new WeakMap<
        ProviderAdapterV2SessionRuntime,
        Map<ThreadId, OriginalThreadLoad>
      >();
      const makeRuntimeLifecycle = (
        providerSessionId: ProviderSessionId,
        instanceId: ProviderInstanceId,
        driver: ProviderAdapterV2SessionRuntime["driver"],
        owner: { runtime: ProviderAdapterV2SessionRuntime | undefined; active: boolean },
        dutyHandle: Effect.Success<ReturnType<typeof workspaceDuty.hold>>,
      ): ProviderRuntimeLifecycle => {
        const reservations = new Map<
          string,
          {
            readonly previous: Map<string, string | null>;
            readonly bindings: Map<string, ThreadId>;
            readonly invalidated: Set<string>;
          }
        >();
        const publicationAuthorities = new WeakMap<
          ProviderAdapter.RuntimeBindingPublicationInput,
          {
            readonly revalidate: Effect.Effect<void, ProviderAdapter.ProviderAdapterV2Error>;
            readonly nativeGuard: NativeProvider.NativeProviderExecutionGuard | undefined;
          }
        >();
        const protocolError = (cause: unknown) =>
          new ProviderRuntimeBindingError({
            driver,
            detail:
              "Failed to establish the actual provider runtime binding; native effects must not be replayed.",
            cause,
          });
        const revalidateCurrent = Effect.fn("ProviderSessionManager.revalidateCurrent")(function* (
          threadId: ThreadId,
        ) {
          if (!owner.active) return yield* protocolError("The original runtime scope has closed.");
          if (owner.runtime !== undefined) {
            const live = (yield* Ref.get(sessions)).get(sessionKey(providerSessionId));
            if (live?.runtime !== owner.runtime || !live.attachedThreadIds.has(threadId))
              return yield* protocolError("The original native-load attachment changed.");
          }
          const admitted = yield* StartAdmission.CurrentRevalidation;
          if (admitted === undefined)
            return yield* protocolError("Original load admission is unavailable.");
          yield* admitted;
        });
        const readThread = (threadId: ThreadId, id: string) =>
          projectionStore
            .getThreadRecords(threadId, ["providerThreads"])
            .pipe(
              Effect.map((records) => records.providerThreads.find((thread) => thread.id === id)),
            );
        const publication = makeProviderRecoveryPublication({
          driver,
          instanceId,
          providerSessionId,
          reservations,
          allocate: (threadId) =>
            idAllocator.allocate
              .event({ threadId, providerSessionId })
              .pipe(Effect.mapError(protocolError)),
          readThread: (threadId, id) =>
            readThread(threadId, id).pipe(Effect.mapError(protocolError)),
          events: eventStore,
          sink: eventSink,
          ingestor: providerEventIngestor,
          physical: {
            bind: (binding, lease) =>
              physicalEvidence
                .bind(binding, lease)
                .pipe(
                  Effect.tap(() =>
                    workspaceDuty
                      .bindSession(dutyHandle, binding.providerThreadId, binding.runtimeGeneration)
                      .pipe(
                        Effect.catch((cause) =>
                          Effect.logDebug("Native session duty binding unavailable", { cause }),
                        ),
                      ),
                  ),
                ),
          },
          forgetTransfers: forgetCompletedThreadTransfers,
          revalidateGuard: Effect.fn("ProviderSessionManager.revalidatePublicationGuard")(
            (
              threadId: ThreadId,
              publication: Parameters<
                Parameters<typeof makeProviderRecoveryPublication>[0]["revalidateGuard"]
              >[1],
            ) =>
              Effect.gen(function* () {
                if (!owner.active)
                  return yield* protocolError("The original publication runtime scope has closed.");
                if (owner.runtime !== undefined) {
                  const entry = (yield* Ref.get(sessions)).get(sessionKey(providerSessionId));
                  if (entry?.runtime !== owner.runtime || !entry.attachedThreadIds.has(threadId))
                    return yield* protocolError(
                      "The original publication runtime attachment changed.",
                    );
                }
                const original = publicationAuthorities.get(publication.input);
                if (original === undefined)
                  return yield* protocolError("Original publication authority was lost.");
                yield* original.revalidate;
                yield* NativeProvider.revalidateNativeProviderGuard(original.nativeGuard, {
                  threadId,
                });
              }).pipe(
                Effect.provideService(StartAdmission.CurrentBindingPublication, publication),
                Effect.mapError(protocolError),
              ),
          ),
          bindGuard: Effect.fn("ProviderSessionManager.bindPublicationGuard")(
            (
              binding: ProviderRuntimeBinding,
              value: ProviderAdapter.RuntimeBindingPublicationInput,
            ) =>
              Effect.gen(function* () {
                const original = publicationAuthorities.get(value);
                if (original === undefined)
                  return yield* protocolError("Original native publication guard was lost.");
                if (original.nativeGuard !== undefined)
                  yield* NativeProvider.bindNativeProviderGuard(original.nativeGuard, binding);
              }).pipe(Effect.mapError(protocolError)),
          ),
        });
        const captureOriginalLoadBinding = Effect.fn(
          "ProviderSessionManager.captureOriginalLoadBinding",
        )(function* (value: Parameters<ProviderRuntimeLifecycle["bind"]>[0]) {
          const runtime = owner.runtime;
          const threadId = value.providerThread.appThreadId;
          if (
            runtime === undefined ||
            (yield* Ref.get(sessions)).get(sessionKey(providerSessionId))?.runtime !== runtime ||
            threadId === null
          )
            return;
          const original = originalThreadLoads.get(runtime)?.get(threadId);
          if (original === undefined) return;
          const target = original.target;
          if (
            target !== undefined &&
            (target.id !== value.providerThread.id ||
              target.appThreadId !== threadId ||
              target.providerSessionId !== providerSessionId ||
              target.providerInstanceId !== instanceId ||
              target.driver !== driver ||
              (target.nativeThreadId !== null &&
                target.nativeThreadId !== value.providerThread.nativeThreadRef?.nativeId))
          )
            return yield* protocolError("The native result changed the original load target.");
          original.target ??= {
            id: value.providerThread.id,
            appThreadId: threadId,
            providerSessionId,
            providerInstanceId: instanceId,
            driver,
            nativeThreadId: value.providerThread.nativeThreadRef?.nativeId ?? null,
          };
          if (original.binding === undefined)
            original.binding = runtimeBinding(value.providerThread, value.runtimeGeneration);
          return { runtime, threadId, original };
        });
        const prepareBindingPublication = Effect.fn(
          "ProviderSessionManager.prepareBindingPublication",
        )((value: Parameters<ProviderRuntimeLifecycle["bind"]>[0]) =>
          Effect.uninterruptibleMask((restore) =>
            Effect.gen(function* () {
              const admitted = yield* StartAdmission.CurrentRevalidation;
              const guard = yield* Effect.serviceOption(NativeProvider.CurrentNativeGuard);
              const actualBinding = runtimeBinding(value.providerThread, value.runtimeGeneration);
              if (admitted === undefined || actualBinding === undefined)
                return yield* protocolError(
                  "Original native load authority or binding is unavailable.",
                );
              if (!publicationAuthorities.has(value))
                publicationAuthorities.set(value, {
                  revalidate: admitted,
                  nativeGuard: Option.getOrUndefined(guard),
                });
              const captured = yield* restore(captureOriginalLoadBinding(value));
              const handle = yield* restore(publication.prepare(value));
              if (captured !== undefined) {
                const current = (yield* Ref.get(sessions)).get(sessionKey(providerSessionId));
                if (
                  current?.runtime !== captured.runtime ||
                  originalThreadLoads.get(captured.runtime)?.get(captured.threadId) !==
                    captured.original ||
                  (captured.original.publication !== undefined &&
                    captured.original.publication.handle !== handle)
                )
                  return yield* protocolError(
                    "The original load changed before publication preparation completed.",
                  );
                captured.original.publication ??= {
                  handle,
                  readCompleted: publication.readCompleted(handle),
                };
              }
              return handle;
            }),
          ),
        );
        return {
          reserve: Effect.fn("ProviderSessionManager.reserve")((threadId) =>
            Effect.gen(function* () {
              yield* revalidateCurrent(threadId);
              const rows = yield* projectionStore.getThreadRecords(threadId, ["providerThreads"]);
              const generation = String(
                yield* idAllocator.allocate.event({ threadId, providerSessionId }),
              );
              reservations.set(generation, {
                previous: new Map(
                  rows.providerThreads
                    .filter(
                      (thread) =>
                        thread.providerInstanceId === instanceId && thread.driver === driver,
                    )
                    .map((thread) => [
                      String(thread.id),
                      thread.runtimeIdentity?.runtimeGeneration ?? null,
                    ]),
                ),
                bindings: new Map(),
                invalidated: new Set(),
              });
              return generation;
            }).pipe(Effect.mapError(protocolError)),
          ),
          admit: Effect.fn("ProviderSessionManager.admit")(({ runtimeGeneration, threadId }) =>
            Effect.gen(function* () {
              yield* revalidateCurrent(threadId);
              const reservation = reservations.get(runtimeGeneration);
              if (reservation === undefined)
                return yield* protocolError("The launch reservation was lost.");
              yield* assertRetirementClear({
                threadId,
                providerSessionId,
                instanceId,
                source: (yield* Ref.get(sessions)).get(sessionKey(providerSessionId))?.runtime,
              });
              // Jones: launch admission resumes the workspace's native transition.
              yield* beforeSessionUse(
                threadId,
                providerSessionId,
                Effect.gen(function* () {
                  if (reservations.get(runtimeGeneration) !== reservation)
                    return yield* protocolError("The launch reservation changed.");
                  yield* revalidateCurrent(threadId);
                }),
              );
              const rows = yield* projectionStore.getThreadRecords(threadId, ["providerThreads"]);
              for (const thread of rows.providerThreads) {
                if (
                  thread.providerInstanceId === instanceId &&
                  thread.driver === driver &&
                  !reservation.bindings.has(thread.id)
                ) {
                  const predecessor = reservation.previous.get(thread.id) ?? null;
                  if ((thread.runtimeIdentity?.runtimeGeneration ?? null) !== predecessor)
                    return yield* protocolError(
                      "The original launch predecessor changed before admission.",
                    );
                }
              }
            }).pipe(Effect.mapError(protocolError)),
          ),
          prepareBindingPublication,
          bind: (value) =>
            prepareBindingPublication(value).pipe(Effect.flatMap((handle) => handle.complete)),
          abandon: Effect.fn("ProviderSessionManager.abandon")((generation) =>
            Effect.gen(function* () {
              const reservation = reservations.get(generation);
              reservations.delete(generation);
              const runtime = owner.runtime;
              const loads = runtime === undefined ? undefined : originalThreadLoads.get(runtime);
              if (loads !== undefined)
                for (const [threadId, original] of loads)
                  if (original.binding?.runtimeGeneration === generation) loads.delete(threadId);
              yield* physicalEvidence.abandon(generation);
              if (reservation === undefined) return;
              for (const [id, threadId] of reservation.bindings) {
                const current = yield* readThread(threadId, id);
                if (current?.runtimeIdentity?.runtimeGeneration !== generation) continue;
                yield* eventSink.write({
                  runtimeIdentityBoundary: {
                    expectedGeneration: generation,
                    expectedActiveProviderThreadId: current.id,
                  },
                  events: [
                    {
                      id: yield* idAllocator.allocate.event({ threadId, providerSessionId }),
                      type: "provider-thread.updated",
                      threadId,
                      occurredAt: yield* DateTime.now,
                      payload: {
                        ...current,
                        runtimeIdentity: {
                          requested: current.runtimeIdentity.requested,
                          observed: unobservedRuntimeIdentity(),
                        },
                      },
                    },
                  ],
                });
              }
            }).pipe(Effect.mapError(protocolError)),
          ),
          invalidate: Effect.fn("ProviderSessionManager.invalidate")((binding) =>
            Effect.gen(function* () {
              const runtime = owner.runtime;
              const loads = runtime === undefined ? undefined : originalThreadLoads.get(runtime);
              const original = loads?.get(binding.threadId);
              if (
                original?.binding !== undefined &&
                Schema.toEquivalence(ProviderAdapterBinding)(original.binding, binding)
              )
                loads?.delete(binding.threadId);
              yield* physicalEvidence.invalidate(binding);
              const current = yield* readThread(binding.threadId, binding.providerThreadId);
              if (
                current?.runtimeIdentity?.runtimeGeneration !== binding.runtimeGeneration ||
                current.nativeThreadRef?.nativeId !== binding.nativeThreadId
              )
                return;
              reservations
                .get(binding.runtimeGeneration)
                ?.invalidated.add(binding.providerThreadId);
              yield* eventSink.write({
                runtimeEvidence: binding,
                runtimeIdentityBoundary: {
                  expectedGeneration: binding.runtimeGeneration,
                  expectedActiveProviderThreadId: binding.providerThreadId,
                },
                events: [
                  {
                    id: yield* idAllocator.allocate.event({
                      threadId: binding.threadId,
                      providerSessionId,
                    }),
                    type: "provider-thread.updated",
                    threadId: binding.threadId,
                    occurredAt: yield* DateTime.now,
                    payload: {
                      ...current,
                      runtimeIdentity: {
                        requested: current.runtimeIdentity.requested,
                        observed: unobservedRuntimeIdentity(),
                      },
                    },
                  },
                ],
              });
            }).pipe(Effect.mapError(protocolError)),
          ),
        };
      };

      // Labs flag, read live: Codex per turn (its shared session outlives a toggle), Claude per query.
      // Unreadable settings keep it off.
      const promptSuggestionsEnabled = Option.isNone(serverSettings)
        ? Effect.succeed(false)
        : serverSettings.value.getSettings.pipe(
            Effect.map((settings) => settings.promptSuggestionsEnabled),
            Effect.orElseSucceed(() => false),
          );
      const agentAccessSettings = Effect.fn("ProviderSessionManagerV2.agentAccessSettings")(
        function* (threadId: ThreadId) {
          if (Option.isNone(serverSettings))
            return { browser: true, device: false, decisionSnapshot: false };
          return yield* Effect.gen(function* () {
            const settings = yield* serverSettings.value.getSettings;
            const thread = yield* projectionStore.getThread(threadId);
            const entries = Object.values(settings.projectSettingsOverrides);
            const browserOverridden = entries.some(
              (entry) => entry.enableAgentBrowserAccess !== undefined,
            );
            const deviceOverridden = entries.some(
              (entry) => entry.enableAgentDeviceAccess !== undefined,
            );
            // Jones: the decision-snapshot grant follows the same project override rule.
            const decisionSnapshotOverridden =
              DecisionSnapshotAccess.hasAgentDecisionSnapshotProjectOverride(settings);
            if (browserOverridden || deviceOverridden || decisionSnapshotOverridden) {
              const project = Option.isSome(projectService)
                ? yield* projectService.value.getById(thread.projectId)
                : Option.none();
              if (Option.isNone(project))
                return {
                  browser: browserOverridden ? false : settings.enableAgentBrowserAccess,
                  device:
                    settings.enableDeviceSupport &&
                    !deviceOverridden &&
                    settings.enableAgentDeviceAccess,
                  decisionSnapshot: DecisionSnapshotAccess.resolveAgentDecisionSnapshotAccess(
                    settings,
                    thread.projectId,
                    false,
                  ),
                };
            }
            const effective = resolveProjectSettings(settings, thread.projectId).settings;
            return {
              browser: effective.enableAgentBrowserAccess,
              device: settings.enableDeviceSupport && effective.enableAgentDeviceAccess,
              decisionSnapshot: DecisionSnapshotAccess.resolveAgentDecisionSnapshotAccess(
                settings,
                thread.projectId,
                true,
              ),
            };
          }).pipe(
            Effect.catch((cause) =>
              Effect.logWarning(
                "Could not resolve agent access; withholding browser, device and decision-snapshot tools.",
                { threadId, cause },
              ).pipe(Effect.as({ browser: false, device: false, decisionSnapshot: false })),
            ),
          );
        },
      );
      const readLinkedEnvironments = Effect.gen(function* () {
        if (options.configureMcp === false || Option.isNone(serverSettings)) return undefined;
        return yield* serverSettings.value.getSettings.pipe(
          Effect.map((settings) => settings.linkedEnvironmentsEnabled),
          Effect.orElseSucceed(() => null),
        );
      });
      const layerScope = yield* Effect.scope;
      // Group signals stop providers before restart capture; drop post-signal events so recovery owns that state.
      const shutdownSignal = { received: false };
      const onShutdownSignal = () => {
        shutdownSignal.received = true;
      };
      yield* Effect.acquireRelease(
        Effect.sync(() => {
          process.on("SIGINT", onShutdownSignal);
          process.on("SIGTERM", onShutdownSignal);
        }),
        () =>
          Effect.sync(() => {
            process.off("SIGINT", onShutdownSignal);
            process.off("SIGTERM", onShutdownSignal);
          }),
      );
      const sessions = yield* Ref.make(new Map<string, LiveSessionEntry>());
      const retirements = RetirementLedger.makeRetirementLedger();
      const recoveryRetirements = makeProviderRecoveryRetirements();
      const equivalentBinding = Schema.toEquivalence(ProviderAdapterBinding);
      const transferWorkers = new WeakSet<RetirementLedger.Retirement>();
      const forgetCompletedThreadTransfers = (threadId: ThreadId) =>
        Ref.update(sessions, (current) => {
          const next = new Map(current);
          for (const [key, entry] of current) {
            if (!entry.completedThreadTransfers.has(threadId)) continue;
            const completedThreadTransfers = new Map(entry.completedThreadTransfers);
            completedThreadTransfers.delete(threadId);
            next.set(key, { ...entry, completedThreadTransfers });
          }
          return next;
        });
      const readMetadata = Effect.fn("ProviderSessionManager.readMetadata")(function* (
        instanceId: ProviderInstanceId,
      ) {
        if (registry.getMetadata === undefined) return undefined;
        return yield* registry.getMetadata(instanceId).pipe(Effect.orElseSucceed(() => undefined));
      });
      const readDestinationKey = (instanceId: ProviderInstanceId) =>
        readMetadata(instanceId).pipe(Effect.map(AccountTransfer.retirementConversationKey));
      const assertRetirementClear = (input: {
        readonly threadId: ThreadId;
        readonly providerSessionId: ProviderSessionId;
        readonly instanceId: ProviderInstanceId;
        readonly source?: object | undefined;
      }) =>
        readDestinationKey(input.instanceId).pipe(
          Effect.flatMap((destinationKey) =>
            retirements.blocks({ ...input, destinationKey })
              ? Effect.fail(
                  new ProviderSessionOpenError({
                    instanceId: input.instanceId,
                    providerSessionId: input.providerSessionId,
                    cause: "The original native writer has not finished retiring.",
                  }),
                )
              : Effect.void,
          ),
        );

      // Retry per released entry; reusing ids must not lose cleanup for earlier threads.
      const releaseRecordRetries = yield* FiberSet.make();
      const nextSubscriberId = yield* Ref.make(0);
      const sessionOpen = yield* KeyedLock.make<ProviderSessionId>();
      const threadAttachment = yield* KeyedLock.make<string>();
      const threadAttachmentKey = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
      }) => `${input.providerSessionId}\u0000${input.threadId}`;
      const idleTimeoutMs = Math.max(1, options.idleTimeoutMs ?? DEFAULT_IDLE_TIMEOUT_MS);
      const maxIdlePinMs = Math.max(0, options.maxIdlePinMs ?? DEFAULT_MAX_IDLE_PIN_MS);
      interface PreparedMcpCredential {
        readonly mcpCredentialId: string | undefined;
        readonly issued: boolean;
      }
      /** ACP and OpenCode consume credentials during openSession; reserve them against revocation until publication. */
      const mcpCredentialReservations = new Map<string, number>();
      const mcpReservationKey = (threadId: ThreadId, mcpCredentialId: string) =>
        `${threadId}\0${mcpCredentialId}`;
      const reserveMcpCredential = (threadId: ThreadId, mcpCredentialId: string) => {
        const key = mcpReservationKey(threadId, mcpCredentialId);
        mcpCredentialReservations.set(key, (mcpCredentialReservations.get(key) ?? 0) + 1);
      };
      const dropMcpCredentialReservation = (threadId: ThreadId, mcpCredentialId: string) => {
        const key = mcpReservationKey(threadId, mcpCredentialId);
        const count = mcpCredentialReservations.get(key) ?? 0;
        if (count <= 1) {
          mcpCredentialReservations.delete(key);
        } else {
          mcpCredentialReservations.set(key, count - 1);
        }
      };
      const isMcpCredentialReserved = (threadId: ThreadId, mcpCredentialId: string) =>
        (mcpCredentialReservations.get(mcpReservationKey(threadId, mcpCredentialId)) ?? 0) > 0;
      const mcpPrepareLock = yield* KeyedLock.make<ThreadId>();
      /** Serialize prepares per thread so concurrent rotations cannot revoke each other's credentials. */
      const prepareMcpSession = (
        threadId: ThreadId,
        providerInstanceId: ProviderInstanceId,
      ): Effect.Effect<PreparedMcpCredential> =>
        options.configureMcp === false
          ? mcpSessions
              .clear(threadId)
              .pipe(Effect.as<PreparedMcpCredential>({ mcpCredentialId: undefined, issued: false }))
          : mcpPrepareLock.withLock(
              threadId,
              Effect.gen(function* () {
                // Live providers build MCP clients once; reuse credentials across workspace handoffs.
                const {
                  browser: browserToolsAvailable,
                  device: deviceToolsAvailable,
                  decisionSnapshot: decisionSnapshotAvailable,
                } = yield* agentAccessSettings(threadId);
                const capabilities = new Set<
                  import("../mcp/McpInvocationContext.ts").McpCapability
                >(["orchestration", "worktree", "pull-requests"]);
                if (browserToolsAvailable) capabilities.add("preview");
                if (deviceToolsAvailable) capabilities.add("device");
                const subagent = yield* projectionStore.getThread(threadId).pipe(
                  Effect.map((thread) => thread.lineage.relationshipToParent === "subagent"),
                  Effect.orElseSucceed(() => false),
                );
                if (decisionSnapshotAvailable)
                  capabilities.add(DecisionSnapshotAccess.DECISION_SNAPSHOT_CAPABILITY);
                const existing = yield* mcpSessions.read(threadId);
                if (existing !== undefined) {
                  // Reserve before async validation so release cannot revoke between validation and reservation.
                  reserveMcpCredential(threadId, existing.providerSessionId);
                  const rawToken = existing.authorizationHeader.replace(/^Bearer\s+/, "");
                  // Interrupted resolution must release the reservation before the caller learns of it.
                  const resolved = yield* mcpSessionRegistry
                    .resolve(rawToken)
                    .pipe(
                      Effect.onInterrupt(() =>
                        Effect.sync(() =>
                          dropMcpCredentialReservation(threadId, existing.providerSessionId),
                        ),
                      ),
                    );
                  if (
                    resolved !== undefined &&
                    resolved.thread.threadId === threadId &&
                    resolved.thread.providerInstanceId === providerInstanceId &&
                    // Jones: any optional capability change rotates the credential.
                    DecisionSnapshotAccess.mcpGrantMatches(resolved.capabilities, capabilities) &&
                    // A credential issued while the lineage read failed is replaced once it works.
                    (resolved.thread.subagent === true) === subagent
                  ) {
                    return { mcpCredentialId: existing.providerSessionId, issued: false };
                  }
                  dropMcpCredentialReservation(threadId, existing.providerSessionId);
                }
                yield* mcpSessionRegistry.revokeThread(threadId);
                const credential = yield* mcpSessionRegistry.issue({
                  threadId,
                  providerInstanceId,
                  browserToolsAvailable,
                  capabilities,
                  subagent,
                });
                yield* mcpSessions.set({
                  ...credential.config,
                  hasPendingWake: projectionStore
                    .getThreadRecords(threadId, ["runs", "subagents"])
                    .pipe(
                      Effect.map(delegatedTaskWillWake),
                      Effect.orElseSucceed(() => false),
                    ),
                });
                reserveMcpCredential(threadId, credential.config.providerSessionId);
                return { mcpCredentialId: credential.config.providerSessionId, issued: true };
              }),
            );
      const clearMcpSession = (threadId: ThreadId, mcpCredentialId?: string) =>
        mcpCredentialId === undefined
          ? mcpSessionRegistry
              .revokeThread(threadId)
              .pipe(Effect.tap(() => mcpSessions.clear(threadId)))
          : mcpSessionRegistry.revokeProviderSession(mcpCredentialId).pipe(
              Effect.tap(() =>
                Effect.gen(function* () {
                  if ((yield* mcpSessions.read(threadId))?.providerSessionId === mcpCredentialId) {
                    yield* mcpSessions.clear(threadId);
                  }
                }),
              ),
            );

      const drainingRuntimes = new WeakSet<ProviderAdapterV2SessionRuntime>();
      const scopeClosures = new WeakMap<Scope.Closeable, Deferred.Deferred<Exit.Exit<void>>>();
      const subscriberReleaseCleanup = new WeakMap<
        Queue.Queue<ProviderSessionEventSignal, Cause.Done>,
        Effect.Effect<void>
      >();

      const publishToSubscribers = (
        subscribers: Ref.Ref<ReadonlyMap<number, ProviderSessionEventSubscriber>>,
        signal: ProviderSessionEventSignal,
      ) =>
        Ref.get(subscribers).pipe(
          Effect.flatMap((current) =>
            Effect.forEach(current.values(), ({ queue }) => Queue.offer(queue, signal), {
              discard: true,
            }),
          ),
        );

      const failSubscribers = Effect.fn("ProviderSessionManager.failSubscribers")(function* (
        entry: LiveSessionEntry,
        detail: string,
        afterDrain: Effect.Effect<void>,
        cause?: Cause.Cause<ProviderAdapter.ProviderAdapterV2Error>,
        detachedThreadId?: ThreadId,
        drain = false,
      ) {
        const error = new ProviderAdapter.ProviderAdapterEventStreamError({
          driver: entry.runtime.driver,
          providerSessionId: entry.runtime.providerSessionId,
          raisedByT3: true,
          ...(detachedThreadId === undefined ? {} : { detachedThreadId }),
          cause: detail,
        });
        const subscribers = yield* Ref.getAndSet(entry.eventSubscribers, new Map());
        yield* Effect.forEach(
          subscribers.values(),
          ({ queue }) => {
            if (!drain) subscriberReleaseCleanup.set(queue, afterDrain);
            return Queue.offer(queue, {
              type: "failure",
              cause: cause ?? Cause.fail(error),
            });
          },
          { discard: true },
        );
        if (drain) {
          yield* Effect.forEach(subscribers.values(), ({ settled }) => Deferred.await(settled), {
            concurrency: "unbounded",
            discard: true,
          });
          yield* afterDrain;
        }
      });

      // Server shutdown interrupts subscribers rather than ending them: a
      // cleanly ended stream without a terminal fails its run, while an
      // interrupted one leaves run settlement to restart recovery.
      const interruptSubscribers = Effect.fn("ProviderSessionManager.interruptSubscribers")(
        function* (entry: LiveSessionEntry) {
          const subscribers = yield* Ref.getAndSet(entry.eventSubscribers, new Map());
          yield* Effect.forEach(
            subscribers.values(),
            ({ queue }) => Queue.clear(queue).pipe(Effect.andThen(Queue.interrupt(queue))),
            { discard: true },
          );
        },
      );

      // Server shutdown intentionally clears them; a provider-announced Stop
      // must let consumers drain them before the stream completes.
      const endSubscribers = Effect.fn("ProviderSessionManager.endSubscribers")(function* (
        entry: LiveSessionEntry,
        afterDrain: Effect.Effect<void>,
      ) {
        const subscribers = yield* Ref.getAndSet(entry.eventSubscribers, new Map());
        yield* Effect.forEach(subscribers.values(), ({ queue }) => Queue.end(queue), {
          discard: true,
        });
        yield* Effect.forEach(subscribers.values(), ({ settled }) => Deferred.await(settled), {
          concurrency: "unbounded",
          discard: true,
        });
        yield* afterDrain;
      });

      const cancelIdleFiber = (fiber: Fiber.Fiber<void, never> | null) =>
        fiber === null ? Effect.void : Fiber.interrupt(fiber).pipe(Effect.ignore);

      const writeProviderSessionEvents = Effect.fn(
        "ProviderSessionManager.writeProviderSessionEvents",
      )(function* (input: {
        readonly runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime;
        readonly threadIds: Iterable<ThreadId>;
        readonly type: "provider-session.attached" | "provider-session.updated";
        readonly payload: OrchestrationV2ProviderSession;
      }) {
        const now = yield* DateTime.now;
        const events = yield* Effect.forEach(input.threadIds, (threadId) =>
          Effect.gen(function* () {
            return {
              id: yield* idAllocator.allocate.event({
                threadId,
                providerSessionId: input.runtime.providerSessionId,
              }),
              type: input.type,
              threadId,
              driver: input.runtime.driver,
              providerInstanceId: input.runtime.instanceId,
              occurredAt: now,
              payload: input.payload,
            } satisfies OrchestrationV2DomainEvent;
          }),
        );
        if (events.length > 0) {
          yield* eventSink.write({ events });
        }
      });

      const writeReleasedSessionEvents = Effect.fn(
        "ProviderSessionManager.writeReleasedSessionEvents",
      )(function* (input: {
        readonly entry: LiveSessionEntry;
        readonly reason: ProviderSessionReleaseReason;
        readonly detail?: string;
      }) {
        const now = yield* DateTime.now;
        const payload: OrchestrationV2ProviderSession = {
          ...input.entry.runtime.providerSession,
          status: releaseStatusFor(input.reason),
          ...(input.entry.runtime.providerSession.extensionStatuses === undefined
            ? {}
            : { extensionStatuses: [] }),
          updatedAt: now,
          lastError:
            input.reason === "runtime_error" ? (input.detail ?? "Provider runtime failed.") : null,
        };
        yield* writeProviderSessionEvents({
          runtime: input.entry.runtime,
          threadIds: input.entry.attachedThreadIds,
          type: "provider-session.updated",
          payload,
        });
      });

      // A detach removes its thread before releasing, but that thread's records still need releasing.
      const releasedThreadIds = (input: {
        readonly entry: LiveSessionEntry;
        readonly detachedThreadId?: ThreadId;
      }) => {
        const threadIds = new Set(input.entry.attachedThreadIds);
        if (input.detachedThreadId !== undefined) threadIds.add(input.detachedThreadId);
        return threadIds;
      };

      const writeReleasedRuntimeRequestEvents = Effect.fn(
        "ProviderSessionManager.writeReleasedRuntimeRequestEvents",
      )(function* (input: {
        readonly entry: LiveSessionEntry;
        readonly reason: ProviderSessionReleaseReason;
        readonly releasedAt: DateTime.Utc;
        readonly detachedThreadId?: ThreadId;
      }) {
        const providerSessionId = input.entry.runtime.providerSessionId;
        const now = yield* DateTime.now;
        const status = releasedRuntimeRequestStatusFor(input.reason);
        const reason =
          input.reason === "runtime_error"
            ? "Provider session failed before this runtime request was resolved."
            : "Provider session was closed before this runtime request was resolved.";

        const events: Array<OrchestrationV2DomainEvent> = [];
        for (const threadId of releasedThreadIds(input)) {
          const projection = yield* projectionStore.getThreadRecords(
            threadId,
            ["runtimeRequests"],
            { runtimeRequestStatuses: ["pending"] },
          );
          const releasedRequests = projection.runtimeRequests.filter(
            (request) =>
              request.status === "pending" &&
              request.responseCapability.type === "live" &&
              request.responseCapability.providerSessionId === providerSessionId &&
              DateTime.isLessThanOrEqualTo(request.createdAt, input.releasedAt) &&
              // Shutdown reconciliation decides which questions can survive the lost callback.
              !(input.reason === "server_shutdown" && request.kind === "user_input"),
          );

          if (releasedRequests.length === 0) continue;
          const records = yield* projectionStore.getThreadRecords(
            threadId,
            ["nodes", "turnItems"],
            {
              nodeIds: releasedRequests.map((request) => request.nodeId),
              turnItemRequestIds: releasedRequests.map((request) => request.id),
              turnItemTypes: ["approval_request", "user_input_request"],
            },
          );
          for (const request of releasedRequests) {
            events.push({
              id: yield* idAllocator.allocate.event({
                threadId,
                providerSessionId,
              }),
              type: "runtime-request.updated",
              threadId,
              nodeId: request.nodeId,
              driver: input.entry.runtime.driver,
              occurredAt: now,
              payload: {
                ...request,
                status,
                responseCapability: {
                  type: "not_resumable",
                  reason,
                },
                resolvedAt: now,
              },
            });

            const requestNode = records.nodes.find((node) => node.id === request.nodeId);
            if (requestNode !== undefined) {
              events.push({
                id: yield* idAllocator.allocate.event({
                  threadId,
                  providerSessionId,
                }),
                type: "node.updated",
                threadId,
                ...(requestNode.runId === null ? {} : { runId: requestNode.runId }),
                nodeId: requestNode.id,
                driver: input.entry.runtime.driver,
                occurredAt: now,
                payload: {
                  ...requestNode,
                  status: input.reason === "runtime_error" ? "failed" : "cancelled",
                  completedAt: now,
                },
              });
            }

            const turnItem = records.turnItems.find(
              (item) =>
                (item.type === "approval_request" || item.type === "user_input_request") &&
                item.requestId === request.id,
            );
            if (turnItem !== undefined) {
              events.push({
                id: yield* idAllocator.allocate.event({
                  threadId,
                  providerSessionId,
                }),
                type: "turn-item.updated",
                threadId,
                ...(turnItem.runId === null ? {} : { runId: turnItem.runId }),
                ...(turnItem.nodeId === null ? {} : { nodeId: turnItem.nodeId }),
                driver: input.entry.runtime.driver,
                occurredAt: now,
                payload: {
                  ...turnItem,
                  status: input.reason === "runtime_error" ? "failed" : "cancelled",
                  completedAt: now,
                  updatedAt: now,
                },
              });
            }
          }
        }

        if (events.length > 0) {
          yield* eventSink.write({ events });
        }
      });

      // Read the session's provider threads as soon as it leaves the live map, before a
      // replacement reports its own tasks. Keep OpenCode's background work: its background
      // subagents share a server and can outlive the session.
      const readReleasedProviderThreads = Effect.fn(
        "ProviderSessionManager.readReleasedProviderThreads",
      )(
        function* (input: {
          readonly entry: LiveSessionEntry;
          readonly reason: ProviderSessionReleaseReason;
          readonly detachedThreadId?: ThreadId;
        }) {
          if (
            input.reason === "server_shutdown" ||
            input.entry.runtime.backgroundTasksEndWithProcess !== true
          ) {
            return [];
          }
          const providerSessionId = input.entry.runtime.providerSessionId;
          const released = [];
          for (const threadId of releasedThreadIds(input)) {
            const projection = yield* projectionStore.getThreadRecords(threadId, [
              "providerThreads",
            ]);
            for (const providerThread of projection.providerThreads) {
              if (providerThread.providerSessionId !== providerSessionId) continue;
              released.push({
                threadId,
                providerSessionId,
                providerThreadId: providerThread.id,
                driver: providerThread.driver,
                taskIds: new Set(
                  (providerThread.pendingBackgroundTasks ?? []).map((task) => task.taskId),
                ),
              });
            }
          }
          return released;
        },
        (effect, input) =>
          effect.pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("orchestration-v2.provider-session-release-roster-read-failed", {
                providerSessionId: input.entry.runtime.providerSessionId,
                cause,
              }).pipe(Effect.as([])),
            ),
          ),
      );
      type ReleasedProviderThread = Effect.Success<
        ReturnType<typeof readReleasedProviderThreads>
      >[number];

      // Dead processes cannot report task endings; stale rosters keep threads Waiting.
      const writeReleasedRosterEvents = Effect.fn(
        "ProviderSessionManager.writeReleasedRosterEvents",
      )(function* ({
        threadId,
        providerSessionId,
        providerThreadId,
        taskIds,
      }: ReleasedProviderThread) {
        if (taskIds.size === 0) return;
        for (let attempt = 0; attempt < 3; attempt++) {
          const projection = yield* projectionStore.getThreadRecords(threadId, ["providerThreads"]);
          const providerThread = projection.providerThreads.find(
            (candidate) => candidate.id === providerThreadId,
          );
          const tasks = providerThread?.pendingBackgroundTasks ?? [];
          const remaining = tasks.filter((task) => !taskIds.has(task.taskId));
          if (providerThread === undefined || remaining.length === tasks.length) return;
          const now = yield* DateTime.now;
          const { committed } = yield* eventSink.writeIfProviderThreadUnchanged({
            expected: providerThread,
            events: [
              {
                id: yield* idAllocator.allocate.event({ threadId, providerSessionId }),
                type: "provider-thread.updated",
                threadId,
                driver: providerThread.driver,
                providerInstanceId: providerThread.providerInstanceId,
                occurredAt: now,
                payload: {
                  ...providerThread,
                  pendingBackgroundTasks: remaining,
                  updatedAt: now,
                },
              },
            ],
          });
          if (committed) return;
        }
        yield* Effect.logWarning("orchestration-v2.provider-session-release-roster-conflict", {
          providerSessionId,
          providerThreadId,
        });
      });

      // Tool calls the dead process started never report an end either, and run finalization
      // keeps them open for a late result. Items started after the release belong to a
      // replacement session.
      const writeReleasedToolCallEvents = Effect.fn(
        "ProviderSessionManager.writeReleasedToolCallEvents",
      )(function* (released: ReleasedProviderThread, releasedAt: DateTime.Utc) {
        const { threadId, providerSessionId, providerThreadId, driver } = released;
        const readCutOff = projectionStore
          .getThreadRecords(threadId, ["turnItems"], {
            turnItemTypes: releasedToolCallTypes,
            turnItemStatuses: activeTurnItemStatuses,
          })
          .pipe(
            Effect.map((projection) =>
              projection.turnItems.filter(
                (item) =>
                  item.providerThreadId === providerThreadId &&
                  DateTime.isLessThanOrEqualTo(item.startedAt ?? item.updatedAt, releasedAt),
              ),
            ),
          );
        // One write per item: a late completion from the dead session's queued events rejects
        // only the interrupt of its own item, and that item then is no longer open. Read again
        // after each pass: queued events can also store more cut-off items.
        let pending = yield* readCutOff;
        for (let pass = 0; pass < 3 && pending.length > 0; pass++) {
          for (const item of pending) {
            const now = yield* DateTime.now;
            yield* eventSink.writeIfTurnItemUnchanged({
              expected: item,
              events: [
                {
                  id: yield* idAllocator.allocate.event({ threadId, providerSessionId }),
                  type: "turn-item.updated",
                  threadId,
                  ...(item.runId === null ? {} : { runId: item.runId }),
                  ...(item.nodeId === null ? {} : { nodeId: item.nodeId }),
                  driver,
                  occurredAt: now,
                  payload: { ...item, status: "interrupted", completedAt: now, updatedAt: now },
                },
              ],
            });
          }
          pending = yield* readCutOff;
        }
        if (pending.length === 0) return;
        yield* Effect.logWarning("orchestration-v2.provider-session-release-tool-call-conflict", {
          providerSessionId,
          providerThreadId,
          openToolCalls: pending.length,
        });
      });

      // Background native subagents die with the process too, often after their run completed,
      // so run finalization does not end them. One started or resumed after the release belongs
      // to a replacement session.
      const writeReleasedSubagentEvents = Effect.fn(
        "ProviderSessionManager.writeReleasedSubagentEvents",
      )(function* (released: ReleasedProviderThread, releasedAt: DateTime.Utc) {
        const { threadId, providerSessionId, providerThreadId, driver } = released;
        const readCutOff = Effect.gen(function* () {
          const { subagents, turnItems } = yield* projectionStore.getThreadRecords(
            threadId,
            ["subagents", "turnItems"],
            {
              omitSettledSubagents: true,
              turnItemTypes: ["subagent"],
              turnItemStatuses: activeTurnItemStatuses,
            },
          );
          const cutOff = subagents.filter((subagent) => {
            if (
              subagent.origin !== "provider_native" ||
              subagent.driver !== driver ||
              !isOrchestrationV2WorkActive(subagent.status) ||
              DateTime.isGreaterThan(subagent.startedAt ?? subagent.updatedAt, releasedAt)
            ) {
              return false;
            }
            // Record and item are written together; a record without a live item lost its end.
            const item = turnItems.find(
              (candidate) => candidate.type === "subagent" && candidate.subagentId === subagent.id,
            );
            return item === undefined || item.providerThreadId === providerThreadId;
          });
          return { subagents: cutOff, turnItems };
        });
        let pending = yield* readCutOff;
        for (let pass = 0; pass < 3 && pending.subagents.length > 0; pass++) {
          const { nodes } = yield* projectionStore.getThreadRecords(threadId, ["nodes"], {
            nodeIds: pending.subagents.map((subagent) => subagent.id),
          });
          const projection = { ...pending, nodes };
          for (const subagent of pending.subagents) {
            const now = yield* DateTime.now;
            const allocateEventId = () =>
              idAllocator.allocate.event({ threadId, providerSessionId });
            const events = yield* endOrphanedNativeSubagent({
              projection,
              subagentId: subagent.id,
              status: "interrupted",
              now,
              emitted: [],
              allocateEventId,
            });
            if (subagent.childThreadId !== null) {
              const child = yield* projectionStore
                .getThreadRecords(subagent.childThreadId, ["nodes", "providerTurns", "turnItems"], {
                  turnItemRunIds: [null],
                  turnItemStatuses: activeTurnItemStatuses,
                })
                .pipe(
                  Effect.catchTags({
                    ProjectionStoreThreadNotFoundError: () => Effect.succeed(null),
                  }),
                );
              if (child !== null) {
                events.push(
                  ...(yield* endRunlessRootTurns({
                    threadId: subagent.childThreadId,
                    providerInstanceId: subagent.providerInstanceId,
                    projection: child,
                    status: "interrupted",
                    now,
                    emitted: events,
                    allocateEventId,
                  })),
                );
              }
            }
            // A late update from the dead session's queued events or a resume rejects only this write.
            yield* eventSink.writeIfSubagentUnchanged({ expected: subagent, events });
          }
          pending = yield* readCutOff;
        }
        if (pending.subagents.length === 0) return;
        yield* Effect.logWarning("orchestration-v2.provider-session-release-subagent-conflict", {
          providerSessionId,
          providerThreadId,
          openSubagents: pending.subagents.length,
        });
      });

      const writeReleasedBackgroundWorkEvents = (
        released: ReadonlyArray<ReleasedProviderThread>,
        releasedAt: DateTime.Utc,
      ) =>
        Effect.forEach(
          released,
          (providerThread) =>
            writeReleasedRosterEvents(providerThread).pipe(
              Effect.andThen(writeReleasedToolCallEvents(providerThread, releasedAt)),
              Effect.andThen(writeReleasedSubagentEvents(providerThread, releasedAt)),
            ),
          { discard: true },
        );

      const writeReleaseRecords = (input: {
        readonly entry: LiveSessionEntry;
        readonly reason: ProviderSessionReleaseReason;
        readonly detail?: string;
        readonly releasedProviderThreads: ReadonlyArray<ReleasedProviderThread>;
        readonly releasedAt: DateTime.Utc;
        readonly detachedThreadId?: ThreadId;
        readonly replaced: boolean;
      }) =>
        Effect.all(
          [
            input.replaced
              ? Effect.succeed(Exit.void)
              : Effect.exit(writeReleasedSessionEvents(input)),
            Effect.exit(
              writeReleasedRuntimeRequestEvents(input).pipe(
                input.entry.requestEventPermit.withPermits(1),
              ),
            ),
            Effect.exit(
              writeReleasedBackgroundWorkEvents(input.releasedProviderThreads, input.releasedAt),
            ),
          ],
          { concurrency: 1 },
        ).pipe(Effect.flatMap(Exit.asVoidAll));

      // Retry after removal from the live map; later releases cannot clear stale ready sessions and approvals.
      const retryReleaseRecords = (
        input: Omit<Parameters<typeof writeReleaseRecords>[0], "replaced">,
      ) => {
        const providerSessionId = input.entry.runtime.providerSessionId;
        const attempt = Effect.gen(function* () {
          const exit = yield* Effect.exit(
            sessionOpen.withLock(
              providerSessionId,
              Effect.gen(function* () {
                const replaced = (yield* Ref.get(sessions)).has(sessionKey(providerSessionId));
                yield* writeReleaseRecords({ ...input, replaced });
              }),
            ),
          );
          if (Exit.isSuccess(exit) || Cause.hasInterruptsOnly(exit.cause)) return yield* exit;
          yield* Effect.logWarning("orchestration-v2.provider-session-release-records-failed", {
            providerSessionId,
            cause: exit.cause,
          });
          // SQL commit failures are defects; retry every cause except interruption.
          return yield* Effect.fail(exit.cause);
        });
        return attempt.pipe(
          Effect.retry({
            schedule: Schedule.exponential("1 second").pipe(
              Schedule.modifyDelay(({ duration }) =>
                Effect.succeed(Duration.min(duration, Duration.seconds(30))),
              ),
            ),
          }),
          Effect.delay("1 second"),
          FiberSet.run(releaseRecordRetries),
        );
      };

      const logReleaseFailure =
        (providerSessionId: ProviderSessionId) =>
        <E, R>(release: Effect.Effect<void, E, R>) =>
          release.pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("orchestration-v2.provider-session-release-failed", {
                providerSessionId,
                cause,
              }),
            ),
          );

      // A request the event pump is persisting for this runtime lands before
      // the cutoff, and once the entry is gone the pump persists no more for
      // it.
      const removeLiveEntry = Effect.fn("ProviderSessionManager.removeLiveEntry")(
        function* (input: {
          readonly providerSessionId: ProviderSessionId;
          readonly onlyIfIdleGeneration?: number;
          readonly onlyIfRuntime?: ProviderAdapterV2SessionRuntime;
          readonly drainSubscribers?: boolean;
          readonly keepIfBusyOtherThan?: OwnBusyTurns;
          readonly keepIf?: Effect.Effect<boolean>;
        }): Effect.fn.Return<readonly [Option.Option<LiveSessionEntry>, DateTime.Utc]> {
          const key = sessionKey(input.providerSessionId);
          const candidate = (yield* Ref.get(sessions)).get(key);
          if (candidate === undefined) {
            return [Option.none<LiveSessionEntry>(), yield* DateTime.now] as const;
          }
          const remove = (keepRequested: boolean) =>
            Effect.zip(
              Ref.modify(sessions, (current) => {
                const existing = current.get(key);
                if (existing !== candidate) {
                  return [existing === undefined ? "gone" : "changed", current] as const;
                }
                if (
                  (input.onlyIfIdleGeneration !== undefined &&
                    (existing.busyTurns.size > 0 ||
                      existing.idleGeneration !== input.onlyIfIdleGeneration)) ||
                  (input.onlyIfRuntime !== undefined && existing.runtime !== input.onlyIfRuntime)
                ) {
                  return ["kept", current] as const;
                }
                const own = input.keepIfBusyOtherThan;
                if (
                  keepRequested ||
                  (own !== undefined &&
                    [...existing.busyTurns].some((turnKey) => !isOwnBusyTurn(turnKey, own)))
                ) {
                  if (own === undefined) return ["kept", current] as const;
                  // The caller settled its own turns; one whose end never arrives must not keep
                  // the session from going idle once the turn it was kept for ends.
                  const kept = new Map(current);
                  kept.set(key, {
                    ...existing,
                    busyTurns: new Set(
                      [...existing.busyTurns].filter((turnKey) => !isOwnBusyTurn(turnKey, own)),
                    ),
                  });
                  return ["kept", kept] as const;
                }
                retirements.retain({
                  source: existing.runtime,
                  scope: existing.scope,
                  providerSessionId: input.providerSessionId,
                  threadIds: existing.attachedThreadIds,
                  kind: "session",
                  sessionWide: true,
                  bindings: [],
                });
                if (input.drainSubscribers === true) drainingRuntimes.add(existing.runtime);
                const updated = new Map(current);
                updated.delete(key);
                return ["removed", updated] as const;
              }),
              DateTime.now,
            );
          const [outcome, releasedAt] = yield* (input.keepIf ?? Effect.succeed(false)).pipe(
            Effect.flatMap(remove),
            candidate.requestEventPermit.withPermits(1),
          );
          if (outcome === "changed") return yield* removeLiveEntry(input);
          return [
            outcome === "removed" ? Option.some(candidate) : Option.none<LiveSessionEntry>(),
            releasedAt,
          ] as const;
        },
      );

      // Scope close can wedge on a misbehaving adapter finalizer (e.g. a
      // provider process that never yields its message stream).
      const closeScopeWithin = Effect.fn("ProviderSessionManager.closeScopeWithin")(function* (
        scope: Scope.Closeable,
        annotations: { readonly providerSessionId?: ProviderSessionId; readonly reason: string },
      ) {
        const owner = lifecycleOwners.get(scope);
        if (owner !== undefined) owner.active = false;
        const closeCompletion = yield* Effect.uninterruptible(
          Effect.gen(function* () {
            const existing = scopeClosures.get(scope);
            if (existing !== undefined) return existing;
            const completion = Deferred.makeUnsafe<Exit.Exit<void>>();
            const closed = scope === sessionScopes ? [...workspaceDuties.keys()] : [scope];
            const ownedHandles = closed.filter((owned) => workspaceDuties.has(owned));
            scopeClosures.set(scope, completion);
            yield* Scope.close(scope, Exit.void).pipe(
              Effect.tap(() => retirements.completeScope(scope)),
              // Late successful native completion drains the captured handles only.
              Effect.tap(() => dutyRelease.drained(ownedHandles)),
              Effect.exit,
              Effect.flatMap((exit) => Deferred.succeed(completion, exit)),
              Effect.forkDetach({ startImmediately: true }),
            );
            return completion;
          }),
        );
        yield* dutyRelease.retry;
        const closeExit = yield* Deferred.await(closeCompletion).pipe(
          Effect.timeoutOption(RELEASE_SCOPE_CLOSE_TIMEOUT_MS),
        );
        if (Option.isNone(closeExit)) {
          yield* Effect.logWarning("orchestration-v2.provider-session-scope-close-timeout", {
            ...annotations,
            timeoutMs: RELEASE_SCOPE_CLOSE_TIMEOUT_MS,
          });
          yield* Deferred.await(closeCompletion).pipe(
            Effect.flatMap((exit) =>
              Exit.isFailure(exit)
                ? Effect.logWarning("orchestration-v2.provider-session-scope-close-failed", {
                    ...annotations,
                    cause: exit.cause,
                  })
                : Effect.logInfo(
                    "orchestration-v2.provider-session-scope-close-completed-late",
                    annotations,
                  ),
            ),
            Effect.forkDetach,
          );
        }
        return closeExit;
      });

      const releaseEntry = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly reason: ProviderSessionReleaseReason;
        readonly detail?: string;
        readonly detachedThreadId?: ThreadId;
        readonly cancelIdleFiber?: boolean;
        readonly onlyIfIdleGeneration?: number;
        readonly onlyIfRuntime?: ProviderAdapterV2SessionRuntime;
        readonly keepIfBusyOtherThan?: OwnBusyTurns;
        readonly keepIf?: Effect.Effect<boolean>;
        readonly gracefulSubscribers?: boolean;
        readonly eventStreamCause?: Cause.Cause<ProviderAdapter.ProviderAdapterV2Error>;
      }) =>
        Effect.acquireUseRelease(
          removeLiveEntry({
            ...input,
            drainSubscribers:
              input.gracefulSubscribers === true ||
              (input.reason === "runtime_error" && input.eventStreamCause !== undefined),
          }),
          ([entry, releasedAt]) =>
            Effect.gen(function* () {
              // Jones: a release that finds no live entry still waits, within the
              // shared scope-close budget, for a pending session or opening retirement.
              if (Option.isNone(entry)) {
                const pending =
                  retirements.find(input.providerSessionId, undefined, "session") ??
                  retirements.find(input.providerSessionId, undefined, "opening");
                if (pending !== undefined)
                  yield* Deferred.await(pending.completion).pipe(
                    Effect.timeoutOption(PROVIDER_SCOPE_CLOSE_TIMEOUT_MS),
                    Effect.asVoid,
                  );
                return;
              }
              return yield* Option.match(entry, {
                onNone: () => Effect.void,
                onSome: (entry) =>
                  Effect.gen(function* () {
                    yield* runtimeUseGuard.sealRuntime(entry.runtime);
                    const detachedThread =
                      input.detachedThreadId === undefined
                        ? {}
                        : { detachedThreadId: input.detachedThreadId };
                    const releasedProviderThreads = yield* readReleasedProviderThreads({
                      entry,
                      reason: input.reason,
                      ...detachedThread,
                    });
                    if (input.cancelIdleFiber !== false) {
                      yield* cancelIdleFiber(entry.idleFiber);
                    }
                    // Queued events can persist tool calls after the release-time scan.
                    const afterDrain = Effect.forEach(
                      releasedProviderThreads,
                      (thread) =>
                        writeReleasedToolCallEvents(thread, releasedAt).pipe(
                          Effect.andThen(writeReleasedSubagentEvents(thread, releasedAt)),
                        ),
                      { discard: true },
                    ).pipe(logReleaseFailure(input.providerSessionId));
                    if (input.gracefulSubscribers === true) {
                      yield* endSubscribers(entry, afterDrain);
                    } else if (input.reason === "server_shutdown") {
                      yield* interruptSubscribers(entry);
                    } else {
                      yield* failSubscribers(
                        entry,
                        input.detail ?? `Provider session released: ${input.reason}.`,
                        afterDrain,
                        input.eventStreamCause,
                        input.detachedThreadId,
                        input.reason === "runtime_error" && input.eventStreamCause !== undefined,
                      );
                    }
                    const closeExit = yield* closeScopeWithin(entry.scope, {
                      providerSessionId: input.providerSessionId,
                      reason: input.reason,
                    });
                    const records = {
                      entry,
                      reason: input.reason,
                      ...(input.detail === undefined ? {} : { detail: input.detail }),
                      releasedProviderThreads,
                      releasedAt,
                      ...detachedThread,
                    };
                    const recorded = yield* Effect.exit(
                      writeReleaseRecords({ ...records, replaced: false }),
                    );
                    if (Exit.isFailure(recorded)) {
                      yield* retryReleaseRecords(records);
                      return yield* recorded;
                    }
                    if (Option.isSome(closeExit) && Exit.isFailure(closeExit.value)) {
                      return yield* Effect.failCause(closeExit.value.cause);
                    }
                  }).pipe(
                    Effect.ensuring(Effect.sync(() => drainingRuntimes.delete(entry.runtime))),
                    withMetrics({
                      counter: providerSessionsTotal,
                      attributes: {
                        provider: entry.runtime.driver,
                        operation: "release",
                        reason: input.reason,
                      },
                    }),
                  ),
              });
            }),
          ([entry]) =>
            Option.match(entry, {
              onNone: () => Effect.void,
              onSome: (entry) =>
                // Revoke recorded credentials after process death, except those held by live replacements.
                Ref.get(sessions).pipe(
                  Effect.flatMap((current) =>
                    Effect.forEach(
                      entry.mcpCredentialIdByThread,
                      ([threadId, mcpCredentialId]) => {
                        // Only holders of this credential may prevent its revocation; stale credentials must not leak it.
                        const heldElsewhere =
                          isMcpCredentialReserved(threadId, mcpCredentialId) ||
                          Array.from(current.values()).some(
                            (other) =>
                              other !== entry &&
                              (other.attachedThreadIds.has(threadId) ||
                                other.mcpCredentialIdByThread.get(threadId) === mcpCredentialId),
                          );
                        return heldElsewhere
                          ? Effect.void
                          : clearMcpSession(threadId, mcpCredentialId);
                      },
                      { discard: true },
                    ),
                  ),
                ),
            }),
        ).pipe(
          Effect.catchCause((cause) =>
            Effect.fail(
              new ProviderSessionReleaseError({
                providerSessionId: input.providerSessionId,
                reason: input.reason,
                cause,
              }),
            ),
          ),
        );

      const retainThreadForWorkMode = WorkModeRetention.makeWorkModeRetentionProbe({
        enabled: Option.isNone(serverSettings)
          ? Effect.succeed(false)
          : serverSettings.value.getSettings.pipe(
              Effect.map((settings) => settings.workModeEnabled),
            ),
        projection: (threadId) => projectionStore.getThreadProjection(threadId),
      });

      const releaseIfStillIdle = Effect.fn("ProviderSessionManager.releaseIfStillIdle")(
        function* (input: {
          readonly providerSessionId: ProviderSessionId;
          readonly generation: number;
          readonly onlyIfRuntime?: ProviderAdapterV2SessionRuntime;
        }): Effect.fn.Return<void> {
          const current = yield* Ref.get(sessions);
          const key = sessionKey(input.providerSessionId);
          const entry = current.get(key);
          if (
            entry === undefined ||
            (input.onlyIfRuntime !== undefined && entry.runtime !== input.onlyIfRuntime) ||
            entry.busyTurns.size > 0 ||
            entry.idleGeneration !== input.generation
          ) {
            return;
          }
          // Capture runtime identity before yielding; replacements can reuse the session id.
          const probedRuntime = entry.runtime;
          const hasPendingWork =
            (probedRuntime.hasPendingBackgroundWork === undefined
              ? false
              : yield* probedRuntime.hasPendingBackgroundWork.pipe(
                  Effect.catchCause(() => Effect.succeed(false)),
                )) ||
            // Closing now would skip the archive of a child whose parent has not taken its result.
            Option.isSome(
              yield* Effect.findFirst(entry.idleThreadUnloads.keys(), (threadId) =>
                delegatedChildArchive(threadId).pipe(
                  Effect.map((child) => child === "awaiting_delivery"),
                ),
              ),
            );
          if (hasPendingWork) {
            const now = yield* Clock.currentTimeMillis;
            const pinnedSinceMs = entry.pinnedSinceMs ?? now;
            if (now - pinnedSinceMs < maxIdlePinMs) {
              const shouldContinuePin = yield* Ref.modify(sessions, (latest) => {
                const latestEntry = latest.get(key);
                if (
                  latestEntry === undefined ||
                  latestEntry.busyTurns.size > 0 ||
                  latestEntry.idleGeneration !== input.generation ||
                  latestEntry.runtime !== probedRuntime
                ) {
                  return [false, latest] as const;
                }
                const updated = new Map(latest);
                updated.set(key, { ...latestEntry, pinnedSinceMs });
                return [true, updated] as const;
              });
              if (!shouldContinuePin) {
                return;
              }
              yield* Effect.logInfo("orchestration-v2.driver-session.idle-release-deferred", {
                providerSessionId: input.providerSessionId,
                pinnedForMs: now - pinnedSinceMs,
              });
              // Recheck on this fiber; scheduleIdleReleaseInternal would interrupt it and self-deadlock.
              yield* Effect.sleep(Duration.millis(idleTimeoutMs));
              return yield* releaseIfStillIdle(input);
            }
            yield* Effect.logWarning("orchestration-v2.driver-session.idle-release-pin-expired", {
              providerSessionId: input.providerSessionId,
              pinnedForMs: now - pinnedSinceMs,
            });
          }
          const retainForWorkMode = yield* Effect.findFirst(entry.attachedThreadIds, (threadId) =>
            retainThreadForWorkMode(threadId, input.providerSessionId, probedRuntime.instanceId),
          );
          if (Option.isSome(retainForWorkMode)) {
            const latest = (yield* Ref.get(sessions)).get(key);
            if (
              latest?.runtime !== probedRuntime ||
              latest.busyTurns.size > 0 ||
              latest.idleGeneration !== input.generation
            )
              return;
            yield* Effect.sleep(Duration.millis(WorkModeRetention.WORK_MODE_RETENTION_RECHECK_MS));
            return yield* releaseIfStillIdle(input);
          }
          // Closing skips the threads' own idle unloads, so archive delivered children first.
          yield* Effect.forEach(
            entry.idleThreadUnloads,
            ([threadId, pending]) =>
              unloadIdleThread({
                providerSessionId: input.providerSessionId,
                threadId,
                generation: pending.generation,
                archiveOnly: true,
                onlyIfRuntime: probedRuntime,
              }),
            { discard: true },
          );
          // Adapter probes yield; guard busyTurns and idleGeneration atomically when removing the entry.
          yield* releaseEntry({
            providerSessionId: input.providerSessionId,
            reason: "idle_timeout",
            cancelIdleFiber: false,
            onlyIfIdleGeneration: input.generation,
            onlyIfRuntime: probedRuntime,
          }).pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("orchestration-v2.driver-session.idle-release-failed", {
                providerSessionId: input.providerSessionId,
                cause,
              }),
            ),
          );
        },
      );

      const withActivityError = <A, E, R>(
        providerSessionId: ProviderSessionId,
        effect: Effect.Effect<A, E, R>,
      ): Effect.Effect<A, ProviderSessionActivityError, R> =>
        effect.pipe(
          Effect.catchCause((cause) =>
            Effect.fail(
              new ProviderSessionActivityError({
                providerSessionId,
                cause,
              }),
            ),
          ),
        );

      // Provider activity restarts the pin budget, so background work that keeps
      // reporting progress (a long Claude workflow) is never cut off; only work
      // that went silent for maxIdlePinMs is released. Progress events can be
      // frequent, so keep this untraced.
      const scheduleIdleReleaseInternal = Effect.fnUntraced(function* (
        providerSessionId: ProviderSessionId,
        options?: {
          readonly providerActivity?: boolean;
          readonly onlyIfRuntime?: ProviderAdapterV2SessionRuntime;
        },
      ) {
        const key = sessionKey(providerSessionId);
        const current = yield* Ref.get(sessions);
        const entry = current.get(key);
        if (
          entry === undefined ||
          entry.busyTurns.size > 0 ||
          (options?.onlyIfRuntime !== undefined && entry.runtime !== options.onlyIfRuntime)
        ) {
          return;
        }

        yield* cancelIdleFiber(entry.idleFiber);
        const generation = entry.idleGeneration + 1;
        const idleFiber = yield* Effect.sleep(Duration.millis(idleTimeoutMs)).pipe(
          Effect.andThen(
            releaseIfStillIdle({ providerSessionId, generation, onlyIfRuntime: entry.runtime }),
          ),
          Effect.forkIn(layerScope),
        );
        yield* Ref.update(sessions, (latest) => {
          const latestEntry = latest.get(key);
          if (
            latestEntry === undefined ||
            latestEntry.runtime !== entry.runtime ||
            latestEntry.busyTurns.size > 0
          ) {
            return latest;
          }
          const updated = new Map(latest);
          updated.set(key, {
            ...latestEntry,
            idleGeneration: generation,
            idleFiber,
            ...(options?.providerActivity === true ? { pinnedSinceMs: null } : {}),
          });
          return updated;
        });
      });

      const scheduleIdleRelease = (providerSessionId: ProviderSessionId) =>
        withActivityError(providerSessionId, scheduleIdleReleaseInternal(providerSessionId));

      const touchActivity = (
        providerSessionId: ProviderSessionId,
        options?: {
          readonly providerActivity?: boolean;
          readonly onlyIfRuntime?: ProviderAdapterV2SessionRuntime;
        },
      ) =>
        withActivityError(
          providerSessionId,
          scheduleIdleReleaseInternal(providerSessionId, options),
        );

      const attachThread = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
      }) =>
        withActivityError(
          input.providerSessionId,
          Ref.modify(sessions, (current) => {
            const entry = current.get(sessionKey(input.providerSessionId));
            if (
              entry === undefined ||
              entry.attachedThreadIds.has(input.threadId) ||
              retirements.retiredWriter(entry.runtime, input.threadId) ||
              retirements.blocks({ ...input, source: entry.runtime })
            ) {
              return [undefined, current] as const;
            }
            const updated = new Map(current);
            updated.set(sessionKey(input.providerSessionId), {
              ...entry,
              attachedThreadIds: new Set([...entry.attachedThreadIds, input.threadId]),
            });
            return [entry.runtime, updated] as const;
          }),
        );

      const removeThreadAttachment = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
        readonly runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime;
      }) =>
        Ref.update(sessions, (current) => {
          const key = sessionKey(input.providerSessionId);
          const entry = current.get(key);
          if (
            entry === undefined ||
            entry.runtime !== input.runtime ||
            !entry.attachedThreadIds.has(input.threadId)
          ) {
            return current;
          }
          const attachedThreadIds = new Set(entry.attachedThreadIds);
          attachedThreadIds.delete(input.threadId);
          const loadedProviderThreadKeyByThread = new Map(entry.loadedProviderThreadKeyByThread);
          loadedProviderThreadKeyByThread.delete(input.threadId);
          const linkedEnvironmentsByThread = new Map(entry.linkedEnvironmentsByThread);
          linkedEnvironmentsByThread.delete(input.threadId);
          const updated = new Map(current);
          updated.set(key, {
            ...entry,
            attachedThreadIds,
            loadedProviderThreadKeyByThread,
            linkedEnvironmentsByThread,
          });
          return updated;
        });

      const isProviderThreadLoaded = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
        readonly providerThreadKey: string;
      }) =>
        Ref.get(sessions).pipe(
          Effect.map(
            (current) =>
              current
                .get(sessionKey(input.providerSessionId))
                ?.loadedProviderThreadKeyByThread.get(input.threadId) === input.providerThreadKey,
          ),
        );

      const forgetProviderThreadLoaded = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
        readonly runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime;
      }) =>
        Ref.update(sessions, (current) => {
          const key = sessionKey(input.providerSessionId);
          const entry = current.get(key);
          if (
            entry?.runtime !== input.runtime ||
            !entry.loadedProviderThreadKeyByThread.has(input.threadId)
          ) {
            return current;
          }
          const loadedProviderThreadKeyByThread = new Map(entry.loadedProviderThreadKeyByThread);
          loadedProviderThreadKeyByThread.delete(input.threadId);
          return new Map(current).set(key, { ...entry, loadedProviderThreadKeyByThread });
        });

      /** Pass the setting read before a native load; cache hits keep the earlier record. */
      const markProviderThreadLoaded = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
        readonly providerThreadKey: string;
        readonly onlyIfRuntime?: ProviderAdapterV2SessionRuntime;
        readonly linkedEnvironments: boolean | null | undefined;
      }) =>
        Ref.update(sessions, (current) => {
          const key = sessionKey(input.providerSessionId);
          const entry = current.get(key);
          if (
            entry === undefined ||
            (input.onlyIfRuntime !== undefined && entry.runtime !== input.onlyIfRuntime)
          ) {
            return current;
          }
          const loadedProviderThreadKeyByThread = new Map(entry.loadedProviderThreadKeyByThread);
          loadedProviderThreadKeyByThread.set(input.threadId, input.providerThreadKey);
          const linkedEnvironmentsByThread = new Map(entry.linkedEnvironmentsByThread);
          if (
            input.linkedEnvironments !== undefined &&
            !linkedEnvironmentsByThread.has(input.threadId)
          ) {
            linkedEnvironmentsByThread.set(input.threadId, input.linkedEnvironments);
          }
          const updated = new Map(current);
          updated.set(key, {
            ...entry,
            loadedProviderThreadKeyByThread,
            linkedEnvironmentsByThread,
          });
          return updated;
        });

      const ensureThreadAttached = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
        readonly providerInstanceId: ProviderInstanceId;
        readonly onlyIfRuntime?: ProviderAdapterV2SessionRuntime;
      }) =>
        Effect.suspend(() => {
          let attachedTo: ProviderAdapter.ProviderAdapterV2SessionRuntime | undefined;
          let preparedForCleanup: PreparedMcpCredential | undefined;
          let reservationDropped = false;
          const dropReservation = () => {
            if (!reservationDropped && preparedForCleanup?.mcpCredentialId !== undefined) {
              reservationDropped = true;
              dropMcpCredentialReservation(input.threadId, preparedForCleanup.mcpCredentialId);
            }
          };
          // Lock attach through rollback so concurrent attaches cannot observe an attachment about to be undone.
          const attach = Effect.gen(function* () {
            const current = (yield* Ref.get(sessions)).get(sessionKey(input.providerSessionId));
            yield* assertRetirementClear({
              ...input,
              instanceId: input.providerInstanceId,
              source: current?.runtime,
            });
            if (current !== undefined && retirements.retiredWriter(current.runtime, input.threadId))
              return yield* new ProviderSessionOpenError({
                instanceId: input.providerInstanceId,
                providerSessionId: input.providerSessionId,
                cause: "The transferred writer cannot be reused.",
              });
            const capturedEntry = (yield* Ref.get(sessions)).get(
              sessionKey(input.providerSessionId),
            );
            if (
              capturedEntry === undefined ||
              (input.onlyIfRuntime !== undefined && capturedEntry.runtime !== input.onlyIfRuntime)
            )
              return yield* new ProviderSessionLookupError({
                providerSessionId: input.providerSessionId,
                cause: "The original attachment runtime is no longer live.",
              });
            const attached = yield* attachThread(input).pipe(
              // Record without interruption so cleanup undoes only this call's attachment.
              Effect.tap((runtime) => Effect.sync(() => (attachedTo = runtime))),
              Effect.uninterruptible,
            );
            const afterAttach = (yield* Ref.get(sessions)).get(sessionKey(input.providerSessionId));
            if (
              afterAttach?.runtime !== capturedEntry.runtime ||
              !afterAttach.attachedThreadIds.has(input.threadId) ||
              retirements.retiredWriter(capturedEntry.runtime, input.threadId)
            )
              return yield* new ProviderSessionLookupError({
                providerSessionId: input.providerSessionId,
                cause: "The original attachment was fenced during admission.",
              });
            if (attached !== undefined) {
              const prepared = yield* prepareMcpSession(input.threadId, input.providerInstanceId);
              preparedForCleanup = prepared;
              if (prepared.mcpCredentialId !== undefined) {
                const mcpCredentialId = prepared.mcpCredentialId;
                yield* Ref.update(sessions, (current) => {
                  const key = sessionKey(input.providerSessionId);
                  const entry = current.get(key);
                  if (
                    entry?.runtime !== capturedEntry.runtime ||
                    !entry.attachedThreadIds.has(input.threadId)
                  )
                    return current;
                  const mcpCredentialIdByThread = new Map(entry.mcpCredentialIdByThread);
                  mcpCredentialIdByThread.set(input.threadId, mcpCredentialId);
                  const updated = new Map(current);
                  updated.set(key, { ...entry, mcpCredentialIdByThread });
                  return updated;
                });
              }
              const entry = (yield* Ref.get(sessions)).get(sessionKey(input.providerSessionId));
              if (entry !== undefined) {
                yield* withActivityError(
                  input.providerSessionId,
                  writeProviderSessionEvents({
                    runtime: entry.runtime,
                    threadIds: [input.threadId],
                    type: "provider-session.attached",
                    payload: entry.runtime.providerSession,
                  }),
                );
              }
            }
          }).pipe(
            Effect.onError(() =>
              attachedTo === undefined
                ? Effect.void
                : removeThreadAttachment({ ...input, runtime: attachedTo }).pipe(
                    Effect.andThen(
                      Effect.suspend(() => {
                        dropReservation();
                        // Other live processes hold reused MCP credentials; revocation would break their clients.
                        if (preparedForCleanup?.issued !== true) return Effect.void;
                        const mcpCredentialId = preparedForCleanup.mcpCredentialId;
                        const attachedRuntime = attachedTo;
                        return Ref.get(sessions).pipe(
                          Effect.flatMap((current) =>
                            (mcpCredentialId !== undefined &&
                              isMcpCredentialReserved(input.threadId, mcpCredentialId)) ||
                            Array.from(current.values()).some(
                              (other) =>
                                other.runtime !== attachedRuntime &&
                                (other.attachedThreadIds.has(input.threadId) ||
                                  (mcpCredentialId !== undefined &&
                                    other.mcpCredentialIdByThread.get(input.threadId) ===
                                      mcpCredentialId)),
                            )
                              ? Effect.void
                              : clearMcpSession(input.threadId, mcpCredentialId),
                          ),
                        );
                      }),
                    ),
                  ),
            ),
          );
          return threadAttachment
            .withLock(threadAttachmentKey(input), attach)
            .pipe(Effect.ensuring(Effect.sync(dropReservation)));
        });

      // Marks only the entry this runtime belongs to: a released or replaced session must not
      // gain a busy turn that never ends. Succeeds with whether the turn was marked.
      const markBusy = (
        providerSessionId: ProviderSessionId,
        turnKey: string,
        runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime,
      ) =>
        withActivityError(
          providerSessionId,
          Effect.gen(function* () {
            const key = sessionKey(providerSessionId);
            const marked = yield* Ref.modify(sessions, (current) => {
              const entry = current.get(key);
              if (entry === undefined || entry.runtime !== runtime) {
                return [undefined, current] as const;
              }
              const updated = new Map(current);
              updated.set(key, {
                ...entry,
                busyTurns: new Set(entry.busyTurns).add(turnKey),
                idleFiber: null,
                pinnedSinceMs: null,
              });
              return [{ idleFiber: entry.idleFiber }, updated] as const;
            });
            if (marked === undefined) return false;
            yield* cancelIdleFiber(marked.idleFiber);
            return true;
          }),
        );

      const markIdle = (
        providerSessionId: ProviderSessionId,
        providerThreadId: ProviderThreadId,
        runOrdinal: number,
        onlyIfRuntime: ProviderAdapterV2SessionRuntime,
        activityKey?: string,
      ) =>
        withActivityError(
          providerSessionId,
          Effect.gen(function* () {
            const key = sessionKey(providerSessionId);
            yield* Ref.update(sessions, (current) => {
              const entry = current.get(key);
              if (entry?.runtime !== onlyIfRuntime) {
                return current;
              }
              const busyTurns = new Set(entry.busyTurns);
              busyTurns.delete(activityKey ?? busyTurnKey(providerThreadId, runOrdinal));
              const updated = new Map(current);
              updated.set(key, {
                ...entry,
                busyTurns,
              });
              return updated;
            });
            yield* scheduleIdleReleaseInternal(providerSessionId, { onlyIfRuntime });
            yield* scheduleThreadUnload(providerSessionId, providerThreadId, onlyIfRuntime);
          }),
        );

      const hasBusyTurn = (entry: LiveSessionEntry, providerThreadId: ProviderThreadId) => {
        const prefix = busyTurnPrefix(providerThreadId);
        for (const turnKey of entry.busyTurns) {
          if (turnKey.startsWith(prefix)) return true;
        }
        return false;
      };

      const updateIdleThreadUnload = (
        providerSessionId: ProviderSessionId,
        threadId: ThreadId,
        update: (current: IdleThreadUnload | undefined) => IdleThreadUnload | undefined,
        onlyIfRuntime: ProviderAdapterV2SessionRuntime,
      ) =>
        Ref.modify(sessions, (current) => {
          const key = sessionKey(providerSessionId);
          const entry = current.get(key);
          if (entry?.runtime !== onlyIfRuntime) return [undefined, current] as const;
          const previous = entry.idleThreadUnloads.get(threadId);
          const next = update(previous);
          const idleThreadUnloads = new Map(entry.idleThreadUnloads);
          if (next === undefined) idleThreadUnloads.delete(threadId);
          else idleThreadUnloads.set(threadId, next);
          const updated = new Map(current);
          updated.set(key, { ...entry, idleThreadUnloads });
          return [previous, updated] as const;
        });

      /**
       * Called before the thread is resumed or given a turn, so an unload
       * cannot land between a resume that found the thread loaded and the turn
       * that relies on it.
       */
      const holdThreadLoaded = Effect.fn("ProviderSessionManager.holdThreadLoaded")(
        function* (input: {
          readonly providerSessionId: ProviderSessionId;
          readonly threadId: ThreadId;
          readonly providerThread: OrchestrationV2ProviderThread;
          readonly onlyIfRuntime: ProviderAdapterV2SessionRuntime;
        }) {
          const entry = (yield* Ref.get(sessions)).get(sessionKey(input.providerSessionId));
          if (
            entry?.runtime !== input.onlyIfRuntime ||
            !entry.supportsMultipleProviderThreads ||
            entry.exposedRuntime.unloadThread === undefined ||
            input.providerThread.nativeThreadRef === null
          ) {
            return;
          }
          const previous = yield* updateIdleThreadUnload(
            input.providerSessionId,
            input.threadId,
            (current) => ({
              providerThread: input.providerThread,
              generation: (current?.generation ?? 0) + 1,
              fiber: null,
            }),
            input.onlyIfRuntime,
          );
          yield* cancelIdleFiber(previous?.fiber ?? null);
        },
      );

      const scheduleThreadUnload = Effect.fn("ProviderSessionManager.scheduleThreadUnload")(
        function* (
          providerSessionId: ProviderSessionId,
          providerThreadId: ProviderThreadId,
          onlyIfRuntime: ProviderAdapterV2SessionRuntime,
        ) {
          const entry = (yield* Ref.get(sessions)).get(sessionKey(providerSessionId));
          if (entry?.runtime !== onlyIfRuntime || hasBusyTurn(entry, providerThreadId)) return;
          const tracked = Array.from(entry.idleThreadUnloads).find(
            ([, pending]) => pending.providerThread.id === providerThreadId,
          );
          if (tracked === undefined) return;
          const [threadId, pending] = tracked;
          const generation = pending.generation + 1;
          const fiber = yield* Effect.sleep(Duration.millis(idleTimeoutMs)).pipe(
            Effect.andThen(
              unloadIdleThread({ providerSessionId, threadId, generation, onlyIfRuntime }),
            ),
            Effect.forkIn(layerScope),
          );
          const previous = yield* updateIdleThreadUnload(
            providerSessionId,
            threadId,
            (current) =>
              current?.generation === pending.generation
                ? { ...current, generation, fiber }
                : current,
            onlyIfRuntime,
          );
          yield* cancelIdleFiber(
            previous?.generation === pending.generation ? previous.fiber : fiber,
          );
        },
      );

      // Provider clients list a delegated child as an ordinary conversation, so a
      // finished one is archived once its parent has the result.
      const delegatedChildArchive = Effect.fn("ProviderSessionManager.delegatedChildArchive")(
        function* (threadId: ThreadId) {
          const { thread, runs } = yield* projectionStore.getThreadRecords(threadId, ["runs"], {
            runStatuses: [],
            includeLatestRun: true,
          });
          // Follow-up tasks name their run; the initial task is the node the child forked from.
          const taskId =
            runs.at(-1)?.delegatedTaskId ??
            (thread.forkedFrom?.type === "node" ? thread.forkedFrom.nodeId : undefined);
          const parentThreadId = thread.lineage.parentThreadId;
          if (parentThreadId === null || taskId === undefined) return undefined;
          const task = (yield* projectionStore.getThreadRecords(parentThreadId, ["subagents"], {
            subagentIds: [taskId],
          })).subagents[0];
          if (
            task?.origin !== "app_owned" ||
            task.childThreadId !== threadId ||
            task.status !== "completed"
          ) {
            return undefined;
          }
          const delivery = task.completionDelivery?.state;
          if (delivery === "pending" || delivery === "claimed") return "awaiting_delivery";
          return delivery === "delivered" || delivery === "acknowledged" ? "ready" : undefined;
        },
        (effect) => effect.pipe(Effect.orElseSucceed(() => undefined)),
      );

      const unloadIdleThread = Effect.fn("ProviderSessionManager.unloadIdleThread")(
        function* (input: {
          readonly providerSessionId: ProviderSessionId;
          readonly threadId: ThreadId;
          readonly generation: number;
          /** Session release unloads only the threads it would archive, and never waits. */
          readonly archiveOnly?: boolean;
          readonly onlyIfRuntime?: ProviderAdapterV2SessionRuntime;
        }): Effect.fn.Return<void> {
          const outcome = yield* threadAttachment.withLock(
            threadAttachmentKey(input),
            Effect.gen(function* () {
              const key = sessionKey(input.providerSessionId);
              const entry = (yield* Ref.get(sessions)).get(key);
              const pending = entry?.idleThreadUnloads.get(input.threadId);
              const unloadThread = entry?.exposedRuntime.unloadThread;
              if (
                entry === undefined ||
                (input.onlyIfRuntime !== undefined && entry.runtime !== input.onlyIfRuntime) ||
                pending === undefined ||
                pending.generation !== input.generation ||
                unloadThread === undefined ||
                !entry.attachedThreadIds.has(input.threadId) ||
                hasBusyTurn(entry, pending.providerThread.id)
              ) {
                return "skipped" as const;
              }
              // Unloading kills background terminals; keep threads with pending work loaded.
              const hasPendingWork =
                entry.runtime.hasPendingBackgroundWorkForThread === undefined
                  ? false
                  : yield* entry.runtime
                      .hasPendingBackgroundWorkForThread(pending.providerThread)
                      .pipe(Effect.catchCause(() => Effect.succeed(false)));
              if (
                yield* retainThreadForWorkMode(
                  input.threadId,
                  input.providerSessionId,
                  entry.runtime.instanceId,
                )
              )
                return "work-mode-deferred" as const;
              if (hasPendingWork) return "deferred" as const;
              const child = yield* delegatedChildArchive(input.threadId);
              // Unloading now would miss the archive once the parent takes the result.
              if (child === "awaiting_delivery") return "deferred" as const;
              const archive = child === "ready";
              if (input.archiveOnly === true && !archive) return "skipped" as const;
              const unloading = yield* Ref.modify(sessions, (current) => {
                const latest = current.get(key);
                if (
                  latest?.runtime !== entry.runtime ||
                  latest.idleThreadUnloads.get(input.threadId)?.generation !== input.generation
                ) {
                  return [false, current] as const;
                }
                const loadedProviderThreadKeyByThread = new Map(
                  latest.loadedProviderThreadKeyByThread,
                );
                loadedProviderThreadKeyByThread.delete(input.threadId);
                const idleThreadUnloads = new Map(latest.idleThreadUnloads);
                idleThreadUnloads.delete(input.threadId);
                const updated = new Map(current);
                updated.set(key, {
                  ...latest,
                  loadedProviderThreadKeyByThread,
                  idleThreadUnloads,
                });
                return [true, updated] as const;
              });
              if (!unloading) return "skipped" as const;
              const unload = (archiveThread: boolean) =>
                unloadThread({
                  providerThread: pending.providerThread,
                  archive: archiveThread,
                }).pipe(Effect.timeout(UNLOAD_THREAD_TIMEOUT_MS));
              yield* unload(archive).pipe(
                // A failed or timed-out archive may have left the thread loaded.
                Effect.catch((cause) =>
                  archive
                    ? Effect.logWarning("orchestration-v2.driver-session.idle-archive-failed", {
                        providerSessionId: input.providerSessionId,
                        threadId: input.threadId,
                        providerThreadId: pending.providerThread.id,
                        cause,
                      }).pipe(Effect.andThen(unload(false)))
                    : Effect.fail(cause),
                ),
                Effect.tap(() =>
                  Ref.update(sessions, (current) => {
                    const latest = current.get(key);
                    if (latest?.runtime !== entry.runtime) return current;
                    const linkedEnvironmentsByThread = new Map(latest.linkedEnvironmentsByThread);
                    linkedEnvironmentsByThread.delete(input.threadId);
                    return new Map(current).set(key, { ...latest, linkedEnvironmentsByThread });
                  }),
                ),
                Effect.catchCause((cause) =>
                  Effect.logWarning("orchestration-v2.driver-session.idle-unload-failed", {
                    providerSessionId: input.providerSessionId,
                    threadId: input.threadId,
                    providerThreadId: pending.providerThread.id,
                    cause,
                  }),
                ),
              );
              return "unloaded" as const;
            }),
          );
          // Wait outside the lock so the next attach is not blocked.
          if (
            (outcome === "deferred" || outcome === "work-mode-deferred") &&
            input.archiveOnly !== true
          ) {
            yield* Effect.sleep(
              Duration.millis(
                outcome === "work-mode-deferred"
                  ? WorkModeRetention.WORK_MODE_RETENTION_RECHECK_MS
                  : idleTimeoutMs,
              ),
            );
            return yield* unloadIdleThread(input);
          }
        },
      );

      /** A shared runtime reloads only this idle thread so the others keep their processes. */
      const unloadForLinkedEnvironments = Effect.fn(
        "ProviderSessionManager.unloadForLinkedEnvironments",
      )(function* (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
        readonly providerThread: OrchestrationV2ProviderThread;
      }) {
        const key = sessionKey(input.providerSessionId);
        const entry = (yield* Ref.get(sessions)).get(key);
        const loadedWith = entry?.linkedEnvironmentsByThread.get(input.threadId);
        const unloadThread = entry?.runtime.unloadThread;
        if (
          entry === undefined ||
          !entry.supportsMultipleProviderThreads ||
          unloadThread === undefined ||
          loadedWith === undefined ||
          input.providerThread.nativeThreadRef === null
        ) {
          return;
        }
        const linkedEnvironments = yield* readLinkedEnvironments;
        if (typeof linkedEnvironments !== "boolean" || linkedEnvironments === loadedWith) return;
        yield* threadAttachment.withLock(
          threadAttachmentKey(input),
          Effect.gen(function* () {
            if (hasBusyTurn(entry, input.providerThread.id)) return;
            // Unloading ends the thread's background work; a failed probe keeps it loaded too.
            const hasPendingWork =
              entry.runtime.hasPendingBackgroundWorkForThread === undefined
                ? false
                : yield* entry.runtime
                    .hasPendingBackgroundWorkForThread(input.providerThread, {
                      includeSubagents: true,
                    })
                    .pipe(Effect.catchCause(() => Effect.succeed(true)));
            if (hasPendingWork) return;
            // Forget the load first so a timed-out unload still resumes natively next turn.
            const unloading = yield* Ref.modify(sessions, (current) => {
              const latest = current.get(key);
              if (
                latest?.runtime !== entry.runtime ||
                latest.linkedEnvironmentsByThread.get(input.threadId) !== loadedWith ||
                hasBusyTurn(latest, input.providerThread.id)
              ) {
                return [false, current] as const;
              }
              const loadedProviderThreadKeyByThread = new Map(
                latest.loadedProviderThreadKeyByThread,
              );
              loadedProviderThreadKeyByThread.delete(input.threadId);
              return [
                true,
                new Map(current).set(key, { ...latest, loadedProviderThreadKeyByThread }),
              ] as const;
            });
            if (!unloading) return;
            yield* unloadThread({ providerThread: input.providerThread }).pipe(
              Effect.timeout(UNLOAD_THREAD_TIMEOUT_MS),
              Effect.tap(() =>
                Ref.update(sessions, (current) => {
                  const latest = current.get(key);
                  if (latest?.runtime !== entry.runtime) return current;
                  const linkedEnvironmentsByThread = new Map(latest.linkedEnvironmentsByThread);
                  linkedEnvironmentsByThread.delete(input.threadId);
                  return new Map(current).set(key, { ...latest, linkedEnvironmentsByThread });
                }),
              ),
              Effect.catchCause((cause) =>
                Effect.logWarning("orchestration-v2.driver-session.links-unload-failed", {
                  providerSessionId: input.providerSessionId,
                  threadId: input.threadId,
                  cause,
                }),
              ),
            );
          }),
        );
      });

      /** An exclusive runtime configures MCP at open, so it reopens once nothing would be lost. */
      const releaseForLinkedEnvironments = Effect.fn(
        "ProviderSessionManager.releaseForLinkedEnvironments",
      )(function* (entry: LiveSessionEntry) {
        if (
          entry.supportsMultipleProviderThreads ||
          entry.runtime.canResumeAfterRestart === false ||
          entry.openedWithLinkedEnvironments === undefined ||
          entry.busyTurns.size > 0
        ) {
          return;
        }
        const linkedEnvironments = yield* readLinkedEnvironments;
        if (
          typeof linkedEnvironments !== "boolean" ||
          linkedEnvironments === entry.openedWithLinkedEnvironments
        ) {
          return;
        }
        const providerSessionId = entry.runtime.providerSessionId;
        // Any activity during these reads bumps the generation and keeps the runtime.
        const generation = entry.idleGeneration;
        const hasPendingWork =
          entry.runtime.hasPendingBackgroundWork === undefined
            ? false
            : yield* entry.runtime.hasPendingBackgroundWork.pipe(
                Effect.catchCause(() => Effect.succeed(true)),
              );
        if (hasPendingWork) return;
        // Checked under the request permit so a runless request mid-persist is not cancelled.
        const hasUnfinishedRecords = Effect.forEach(entry.attachedThreadIds, (threadId) =>
          projectionStore.getThreadRecords(threadId, ["runtimeRequests", "providerThreads"], {
            runtimeRequestStatuses: ["pending"],
          }),
        ).pipe(
          Effect.map((projections) =>
            projections.some(
              (projection) =>
                projection.runtimeRequests.some(
                  (request) =>
                    request.responseCapability.type === "live" &&
                    request.responseCapability.providerSessionId === providerSessionId,
                ) ||
                projection.providerThreads.some(
                  (providerThread) =>
                    providerThread.providerSessionId === providerSessionId &&
                    (providerThread.pendingBackgroundTasks ?? []).length > 0,
                ),
            ),
          ),
          Effect.orElseSucceed(() => true),
        );
        yield* releaseEntry({
          providerSessionId,
          reason: "idle_timeout",
          detail: "Linked environments changed; reopening the provider session.",
          onlyIfIdleGeneration: generation,
          keepIf: hasUnfinishedRecords,
        }).pipe(logReleaseFailure(providerSessionId));
      });

      const observeActivity = (
        providerSessionId: ProviderSessionId,
        activity: Effect.Effect<void, ProviderSessionActivityError>,
      ) =>
        activity.pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("orchestration-v2.driver-session.activity-failed", {
              providerSessionId,
              cause,
            }),
          ),
        );

      const validateProviderEvent = Effect.fn("ProviderSessionManager.validateProviderEvent")(
        (runtime: ProviderAdapterV2SessionRuntime, event: ProviderAdapter.ProviderAdapterV2Event) =>
          Effect.gen(function* () {
            const current = (yield* Ref.get(sessions)).get(sessionKey(runtime.providerSessionId));
            if (!drainingRuntimes.has(runtime) && current?.runtime !== runtime) return false;
            yield* ProviderEventOrigin.revalidateProviderEventOrigin(event, runtime);
            const latest = (yield* Ref.get(sessions)).get(sessionKey(runtime.providerSessionId));
            return drainingRuntimes.has(runtime) || latest?.runtime === runtime;
          }).pipe(Effect.catchCause(() => Effect.succeed(false))),
      );

      const makeEventSubscription = Effect.fn("ProviderSessionManager.makeEventSubscription")(
        (
          runtime: ProviderAdapterV2SessionRuntime,
          subscribers: Ref.Ref<ReadonlyMap<number, ProviderSessionEventSubscriber>>,
        ): Effect.Effect<ProviderAdapter.ProviderAdapterV2EventSubscription> =>
          Effect.gen(function* () {
            const entry = (yield* Ref.get(sessions)).get(sessionKey(runtime.providerSessionId));
            if (entry?.runtime !== runtime)
              return {
                events: Stream.fail(
                  new ProviderAdapter.ProviderAdapterEventStreamError({
                    driver: runtime.driver,
                    providerSessionId: runtime.providerSessionId,
                    raisedByT3: true,
                    cause: "The captured provider runtime is no longer live.",
                  }),
                ),
                close: Effect.void,
              };
            return yield* entry.requestEventPermit.withPermits(1)(
              Effect.gen(function* () {
                const current = (yield* Ref.get(sessions)).get(
                  sessionKey(runtime.providerSessionId),
                );
                if (current?.runtime !== runtime)
                  return {
                    events: Stream.fail(
                      new ProviderAdapter.ProviderAdapterEventStreamError({
                        driver: runtime.driver,
                        providerSessionId: runtime.providerSessionId,
                        raisedByT3: true,
                        cause: "The captured provider runtime was released before subscription.",
                      }),
                    ),
                    close: Effect.void,
                  };
                const queue = yield* Queue.unbounded<ProviderSessionEventSignal, Cause.Done>();
                const settled = yield* Deferred.make<void>();
                const subscriberId = yield* Ref.getAndUpdate(
                  nextSubscriberId,
                  (value) => value + 1,
                );
                yield* Ref.update(subscribers, (current) =>
                  new Map(current).set(subscriberId, { queue, settled }),
                );
                const close = Ref.update(subscribers, (current) => {
                  const next = new Map(current);
                  next.delete(subscriberId);
                  return next;
                }).pipe(
                  Effect.andThen(Queue.clear(queue)),
                  Effect.andThen(Queue.end(queue)),
                  Effect.asVoid,
                  Effect.ensuring(Deferred.succeed(settled, undefined)),
                  Effect.andThen(
                    Effect.suspend(() => {
                      const cleanup = subscriberReleaseCleanup.get(queue);
                      subscriberReleaseCleanup.delete(queue);
                      return cleanup === undefined ? Effect.void : Effect.forkDetach(cleanup);
                    }),
                  ),
                );
                const events = Stream.fromQueue(queue).pipe(
                  Stream.mapEffect((signal) =>
                    signal.type === "event"
                      ? Effect.succeed(signal.event)
                      : Effect.failCause(signal.cause),
                  ),
                  Stream.filterEffect((event) => validateProviderEvent(runtime, event)),
                  Stream.ensuring(close),
                );
                return {
                  events,
                  close,
                } satisfies ProviderAdapter.ProviderAdapterV2EventSubscription;
              }),
            );
          }),
      );

      // Jones: a lifecycle-managed workspace resumes its original native transition
      // before provider use; with lifecycle configured the resume runs inside one
      // issued provider-session operation whose check revalidates the caller.
      const beforeSessionUse = Effect.fn("ProviderSessionManager.beforeWorkspaceUse")(function* <C>(
        threadId: ThreadId,
        providerSessionId: ProviderSessionId,
        check: Effect.Effect<unknown, C>,
        cwd?: string | null,
      ) {
        // Conversation bindings have no checkout lifecycle row, so their exact
        // assignment still needs this recheck when checkout admission selects out.
        const conversation =
          Option.isSome(conversationPolicy) &&
          (conversationPolicy.value.ownsPath(cwd ?? null) ||
            (yield* conversationPolicy.value.lookup(threadId))?.state === "conversation");
        if (conversation) yield* check;
        if (Option.isNone(yield* Effect.serviceOption(WorkspaceLifecycleService))) {
          yield* WorkspaceLifecycle.beforeUse({ threadId, boundary: "resume" });
          if (conversation) yield* check;
          return;
        }
        const invocation = yield* idAllocator.allocate.event({ threadId, providerSessionId });
        yield* withOperation(
          {
            id: `provider-session:${providerSessionId}:${invocation}`,
            kind: "provider-session",
            threadId,
            boundaries: ["resume"],
            check: check.pipe(Effect.asVoid),
          },
          WorkspaceLifecycle.inheritOriginalTransition(
            `provider-session:${invocation}`,
            { threadId, providerSessionId },
            WorkspaceLifecycle.beforeUse({ threadId, boundary: "resume" }),
          ),
        );
        if (conversation) yield* check;
      });

      const withSessionInvocation = Effect.fn("ProviderSessionManager.withWorkspaceInvocation")(
        function* <A, E, R, C>(
          threadId: ThreadId,
          providerSessionId: ProviderSessionId,
          effect: Effect.Effect<A, E, R>,
          allocationError: (cause: unknown) => C,
        ) {
          if (
            Option.isNone(yield* Effect.serviceOption(WorkspaceLifecycleService)) ||
            Option.isSome(yield* Effect.serviceOption(WorkspaceLifecycle.OriginalNativeTransition))
          )
            return yield* effect;
          const invocation = yield* idAllocator.allocate
            .event({ threadId, providerSessionId })
            .pipe(Effect.mapError(allocationError));
          return yield* WorkspaceLifecycle.withOriginalTransition(
            `provider-session:${invocation}`,
            { threadId, providerSessionId },
            effect,
          );
        },
      );

      const decorateRuntime = (
        runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime,
        eventSubscribers: Ref.Ref<ReadonlyMap<number, ProviderSessionEventSubscriber>>,
      ): ProviderAdapter.ProviderAdapterV2SessionRuntime => {
        const providerSessionId = runtime.providerSessionId;
        const withRuntimeOperation = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
          runtimeUseGuard.withRuntimeOperation(runtime, effect).pipe(
            Effect.mapError((cause) =>
              isRuntimeUseError(cause)
                ? new ProviderAdapter.ProviderAdapterProtocolError({
                    driver: runtime.driver,
                    detail: "The original workspace runtime use is unavailable.",
                    cause,
                  })
                : cause,
            ),
          );
        // Jones: each native operation runs under one workspace invocation that
        // resumes (or inherits) the original native transition of its thread.
        const withRuntimeInvocation = <A, E, R>(
          threadId: ThreadId,
          effect: Effect.Effect<A, E, R>,
        ) =>
          withSessionInvocation(
            threadId,
            providerSessionId,
            withRuntimeOperation(effect),
            (cause) =>
              new ProviderAdapter.ProviderAdapterProtocolError({
                driver: runtime.driver,
                detail: "Cannot identify the native session operation.",
                cause,
              }),
          );
        const assertRuntimeAttached = Effect.fn("ProviderSessionManager.assertRuntimeAttached")(
          function* (threadId: ThreadId) {
            const entry = (yield* Ref.get(sessions)).get(sessionKey(providerSessionId));
            if (entry?.runtime !== runtime || !entry.attachedThreadIds.has(threadId))
              return yield* new ProviderRuntimeBindingError({
                driver: runtime.driver,
                detail: "The native runtime attachment changed before workspace use.",
              });
          },
        );
        const beforeRuntimeUse = (threadId: ThreadId | null, requestedCwd?: string | null) =>
          runtimeUseGuard
            .beforeUse(runtime, threadId, requestedCwd)
            .pipe(
              Effect.andThen(
                threadId === null
                  ? Effect.void
                  : assertRuntimeAttached(threadId).pipe(
                      Effect.andThen(
                        beforeSessionUse(
                          threadId,
                          providerSessionId,
                          assertRuntimeAttached(threadId),
                          requestedCwd,
                        ),
                      ),
                    ),
              ),
            );
        const withBoundThreadInvocation = <A, E, R>(
          providerThread: OrchestrationV2ProviderThread,
          effect: Effect.Effect<A, E, R>,
          requestedCwd?: string | null,
        ) => {
          const threadId = providerThread.appThreadId;
          const guarded = runtimeUseGuard.assertBoundThread(runtime, providerThread).pipe(
            Effect.andThen(beforeRuntimeUse(threadId, requestedCwd)),
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterProtocolError({
                  driver: runtime.driver,
                  detail: "Cannot validate the bound native workspace operation.",
                  cause,
                }),
            ),
            Effect.andThen(effect),
            withRuntimeOperation,
          );
          // Jones: one native transition identity covers this whole operation.
          return threadId === null
            ? guarded
            : withSessionInvocation(
                threadId,
                providerSessionId,
                guarded,
                (cause) =>
                  new ProviderAdapter.ProviderAdapterProtocolError({
                    driver: runtime.driver,
                    detail: "Cannot identify the native session operation.",
                    cause,
                  }),
              );
        };
        const ensureRuntimeThreadAttached = Effect.fn(
          "ProviderSessionManager.ensureRuntimeThreadAttached",
        )((threadId: ThreadId) =>
          ensureThreadAttached({
            providerSessionId,
            threadId,
            providerInstanceId: runtime.instanceId,
            onlyIfRuntime: runtime,
          }).pipe(
            Effect.catchTags({
              ProviderSessionActivityError: (error) =>
                Effect.logWarning("orchestration-v2.driver-session.activity-failed", {
                  providerSessionId,
                  cause: Cause.fail(error),
                }),
              ProviderSessionLookupError: (cause) =>
                Effect.fail(
                  new ProviderRuntimeBindingError({
                    driver: runtime.driver,
                    detail: "The captured native runtime attachment is no longer live.",
                    cause,
                  }),
                ),
              ProviderSessionOpenError: (cause) =>
                Effect.fail(
                  new ProviderRuntimeBindingError({
                    driver: runtime.driver,
                    detail: "The captured native writer cannot be attached.",
                    cause,
                  }),
                ),
            }),
          ),
        );
        const subscribeEvents = makeEventSubscription(runtime, eventSubscribers);
        type NativeLoad = {
          readonly loaded: OrchestrationV2ProviderThread;
          readonly linkedEnvironments: boolean | null | undefined;
        };
        type CompletedNativeLoad = NativeLoad & { readonly complete: Effect.Effect<void> };
        const originalLoads = makeRetainedNativeLoads<
          NativeLoad,
          ProviderAdapter.ProviderAdapterV2Error
        >(
          layerScope,
          () =>
            new ProviderAdapter.ProviderRuntimeBindingError({
              driver: runtime.driver,
              detail: "An original native load still owns unpublished or uncertain work.",
            }),
        );
        const retainedLoad = Effect.fn("ProviderSessionManager.retainedLoad")(function* (
          threadId: ThreadId,
          key: string,
          load: Effect.Effect<
            OrchestrationV2ProviderThread,
            ProviderAdapter.ProviderAdapterV2Error
          >,
          target?: OrchestrationV2ProviderThread,
          settings: OriginalThreadLoad["settings"] = {},
        ) {
          yield* assertLoadAdmission;
          const loads = originalThreadLoads.get(runtime) ?? new Map<ThreadId, OriginalThreadLoad>();
          originalThreadLoads.set(runtime, loads);
          let original = loads.get(threadId);
          if (original === undefined) {
            original = {
              target:
                target === undefined
                  ? undefined
                  : {
                      id: target.id,
                      appThreadId: target.appThreadId,
                      providerSessionId: target.providerSessionId,
                      providerInstanceId: target.providerInstanceId,
                      driver: target.driver,
                      nativeThreadId: target.nativeThreadRef?.nativeId ?? null,
                    },
              loadKey: key,
              settings,
              linkedEnvironments: undefined,
              binding: undefined,
              publication: undefined,
            };
            loads.set(threadId, original);
          }
          const captured = original;
          const recover = Effect.fn("ProviderSessionManager.recoverRetainedLoad")(function* (
            cause: ProviderAdapter.ProviderAdapterV2Error,
          ) {
            const admitted = yield* StartAdmission.CurrentRevalidation;
            if (
              admitted === undefined ||
              captured?.publication === undefined ||
              captured.target === undefined
            )
              return yield* cause;
            const revalidate = Effect.gen(function* () {
              const live = (yield* Ref.get(sessions)).get(sessionKey(providerSessionId));
              if (
                live?.runtime !== runtime ||
                !live.attachedThreadIds.has(threadId) ||
                loads.get(threadId) !== captured
              )
                return yield* new ProviderRuntimeBindingError({
                  driver: runtime.driver,
                  detail: "The retained native result lost its original runtime attachment.",
                });
              yield* admitted;
            });
            yield* revalidate;
            yield* captured.publication.handle.complete;
            yield* revalidate;
            // This port reconciles the adapter's retained local publication only.
            // It never resumes a native thread or injects history.
            if (runtime.reconcileBindingPublication !== undefined) {
              const reconciled = yield* runtime.reconcileBindingPublication({
                threadId,
                providerThreadId: captured.publication.handle.binding.providerThreadId,
                nativeThreadId: captured.publication.handle.binding.nativeThreadId,
                revalidate,
              });
              const reconciledGeneration = reconciled?.runtimeIdentity?.runtimeGeneration;
              const reconciledBinding =
                reconciled === null || reconciledGeneration === undefined
                  ? undefined
                  : runtimeBinding(reconciled, reconciledGeneration);
              if (
                reconciled !== null &&
                (reconciledBinding === undefined ||
                  !equivalentBinding(captured.publication.handle.binding, reconciledBinding))
              )
                return yield* new ProviderRuntimeBindingError({
                  driver: runtime.driver,
                  detail: "The adapter reconciled another native binding.",
                });
            }
            yield* revalidate;
            const current = yield* captured.publication.readCompleted;
            if (current === null)
              return yield* new ProviderRuntimeBindingError({
                driver: runtime.driver,
                detail: "The exact original native publication has not completed.",
              });
            if (captured.linkedEnvironments === undefined)
              return yield* new ProviderRuntimeBindingError({
                driver: runtime.driver,
                detail: "The original native load settings were not captured.",
              });
            return { loaded: current, linkedEnvironments: captured.linkedEnvironments.value };
          });
          if (captured !== undefined && captured.loadKey !== key) {
            if (captured.publication !== undefined) {
              const recovered = yield* originalLoads(
                threadId,
                captured.loadKey,
                Effect.fail(
                  new ProviderRuntimeBindingError({
                    driver: runtime.driver,
                    detail: "The original native result is unavailable.",
                  }),
                ),
                recover,
              );
              yield* markProviderThreadLoaded({
                providerSessionId,
                threadId,
                onlyIfRuntime: runtime,
                providerThreadKey: providerThreadLoadKey({
                  runtime,
                  providerThread: recovered.value.loaded,
                  ...captured.settings,
                }),
                linkedEnvironments: recovered.value.linkedEnvironments,
              });
              yield* recovered.complete;
              if (loads.get(threadId) === captured) loads.delete(threadId);
            }
            return yield* new ProviderRuntimeBindingError({
              driver: runtime.driver,
              detail:
                "The original native publication was retained; requested settings require a separate admitted load.",
            });
          }
          const result = yield* originalLoads(
            threadId,
            key,
            readLinkedEnvironments.pipe(
              Effect.flatMap((linkedEnvironments) => {
                captured.linkedEnvironments = { value: linkedEnvironments };
                return load.pipe(Effect.map((loaded) => ({ loaded, linkedEnvironments })));
              }),
            ),
            recover,
          );
          return {
            ...result.value,
            complete: result.complete.pipe(
              Effect.andThen(
                Effect.sync(() => {
                  if (loads.get(threadId) === captured) loads.delete(threadId);
                }),
              ),
            ),
          };
        });
        const assertLoadAdmission = Effect.gen(function* () {
          const admitted = yield* StartAdmission.CurrentRevalidation;
          if (admitted === undefined)
            return yield* new ProviderRuntimeBindingError({
              driver: runtime.driver,
              detail: "The original native-load admission is unavailable.",
            });
          yield* admitted;
          const live = (yield* Ref.get(sessions)).get(sessionKey(providerSessionId));
          if (live?.runtime !== runtime)
            return yield* new ProviderRuntimeBindingError({
              driver: runtime.driver,
              detail: "The original native-load runtime was replaced.",
            });
        });
        const guardTurnInput = (input: ProviderAdapter.ProviderAdapterV2TurnInput) => {
          const expected = input.providerThread;
          const expectedGeneration = expected.runtimeIdentity?.runtimeGeneration;
          let admittedSuccessor: typeof ProviderAdapterBinding.Type | undefined;
          return {
            ...input,
            revalidateStartAdmission: Effect.gen(function* () {
              if (input.revalidateStartAdmission === undefined)
                return yield* new ProviderAdapter.ProviderRuntimeBindingError({
                  driver: runtime.driver,
                  detail: "The original start admission callback is missing.",
                });
              yield* input.revalidateStartAdmission;
              const entry = (yield* Ref.get(sessions)).get(sessionKey(providerSessionId));
              if (entry?.runtime !== runtime || !entry.attachedThreadIds.has(input.threadId))
                return yield* new ProviderAdapter.ProviderRuntimeBindingError({
                  driver: runtime.driver,
                  detail: "The captured runtime no longer owns this attachment.",
                });
              const records = yield* projectionStore.getThreadRecords(input.threadId, [
                "providerThreads",
              ]);
              const current = records.providerThreads.find((row) => row.id === expected.id);
              const generation = current?.runtimeIdentity?.runtimeGeneration;
              const binding =
                current === undefined || generation === undefined
                  ? undefined
                  : runtimeBinding(current, generation);
              const publication = yield* StartAdmission.CurrentBindingPublication;
              const publishedSuccessor =
                binding !== undefined &&
                publication !== undefined &&
                publication.previousGeneration === (expectedGeneration ?? null) &&
                equivalentBinding(binding, publication.binding) &&
                (expected.nativeThreadRef === null ||
                  binding.nativeThreadId === expected.nativeThreadRef.nativeId);
              const retainedSuccessor =
                binding !== undefined &&
                admittedSuccessor !== undefined &&
                equivalentBinding(binding, admittedSuccessor);
              if (
                current?.providerSessionId !== providerSessionId ||
                current.providerInstanceId !== runtime.instanceId ||
                (!publishedSuccessor &&
                  !retainedSuccessor &&
                  (generation !== expectedGeneration ||
                    current.nativeThreadRef?.nativeId !== expected.nativeThreadRef?.nativeId))
              )
                return yield* new ProviderAdapter.ProviderRuntimeBindingError({
                  driver: runtime.driver,
                  detail: "The captured native producer binding changed.",
                });
              yield* input.revalidateStartAdmission;
              if (publishedSuccessor) admittedSuccessor = binding;
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderRuntimeBindingError({
                    driver: runtime.driver,
                    detail: "Managed dispatch admission was withdrawn.",
                    cause,
                  }),
              ),
            ),
          };
        };
        const assertControlTarget = Effect.fn("ProviderSessionManager.assertControlTarget")(
          (
            provider: OrchestrationV2ProviderThread,
            threadId?: ThreadId,
            strictProjection = false,
          ) =>
            Effect.gen(function* () {
              const live = (yield* Ref.get(sessions)).get(sessionKey(providerSessionId));
              const ownerThreadId = threadId ?? provider.appThreadId;
              if (
                live?.runtime !== runtime ||
                provider.providerSessionId !== providerSessionId ||
                provider.providerInstanceId !== runtime.instanceId ||
                provider.driver !== runtime.driver ||
                ownerThreadId === null ||
                !live.attachedThreadIds.has(ownerThreadId)
              )
                return yield* new ProviderRuntimeBindingError({
                  driver: runtime.driver,
                  detail: "The captured control runtime attachment changed.",
                });
              if (strictProjection) {
                const records = yield* projectionStore.getThreadRecords(ownerThreadId, [
                  "providerThreads",
                ]);
                const current = records.providerThreads.find((row) => row.id === provider.id);
                if (
                  current?.providerSessionId !== providerSessionId ||
                  current.providerInstanceId !== runtime.instanceId ||
                  current.driver !== runtime.driver ||
                  current.nativeThreadRef?.nativeId !== provider.nativeThreadRef?.nativeId ||
                  current.runtimeIdentity?.runtimeGeneration !==
                    provider.runtimeIdentity?.runtimeGeneration ||
                  current.runtimeIdentity?.evidenceRevision !==
                    provider.runtimeIdentity?.evidenceRevision ||
                  (yield* Ref.get(sessions)).get(sessionKey(providerSessionId))?.runtime !== runtime
                )
                  return yield* new ProviderRuntimeBindingError({
                    driver: runtime.driver,
                    detail: "The captured steering binding changed before dispatch.",
                  });
              }
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderRuntimeBindingError({
                    driver: runtime.driver,
                    detail: "The original control target is unavailable.",
                    cause,
                  }),
              ),
            ),
        );
        const turnMetrics = (operation: string, model?: string) =>
          withMetrics({
            counter: providerTurnsTotal,
            ...(operation === "send" ? { timer: providerTurnDuration } : {}),
            attributes: {
              provider: runtime.driver,
              operation,
              modelFamily: normalizeModelMetricLabel(model),
            },
          });
        return {
          ...runtime,
          ...(runtime.injectHistory === undefined
            ? {}
            : {
                injectHistory: (input: Parameters<NonNullable<typeof runtime.injectHistory>>[0]) =>
                  withBoundThreadInvocation(input.providerThread, runtime.injectHistory!(input)),
              }),
          rollbackThread: (input) =>
            withBoundThreadInvocation(input.providerThread, runtime.rollbackThread(input)),
          ...(runtime.mcpApps === undefined
            ? {}
            : {
                mcpApps: {
                  ...runtime.mcpApps,
                  callTool: (
                    input: Parameters<NonNullable<typeof runtime.mcpApps>["callTool"]>[0],
                  ) =>
                    withBoundThreadInvocation(
                      input.providerThread,
                      runtime.mcpApps!.callTool(input),
                    ),
                },
              }),
          ...(runtime.controlGoal === undefined
            ? {}
            : {
                controlGoal: (input: Parameters<NonNullable<typeof runtime.controlGoal>>[0]) =>
                  input.action === "replace"
                    ? withBoundThreadInvocation(
                        input.providerThread,
                        runtime.controlGoal!(input),
                      ).pipe(
                        Effect.orElseSucceed(() => ({
                          outcome: "unavailable" as const,
                          goal: null,
                          reason: "workspace-runtime-use-unavailable",
                        })),
                      )
                    : runtime.controlGoal!(input),
              }),
          // Pi replaces its session snapshot as metadata changes.
          get providerSession() {
            return runtime.providerSession;
          },
          subscribeEvents,
          ...(runtime.captureRuntimeStop === undefined
            ? {}
            : {
                captureRuntimeStop: (provider: OrchestrationV2ProviderThread) =>
                  provider.appThreadId === null
                    ? Effect.succeed(null)
                    : captureCurrentThreadRuntimeStop(provider.appThreadId, { runtime, provider }),
              }),
          events: Stream.unwrap(
            subscribeEvents.pipe(Effect.map((subscription) => subscription.events)),
          ),
          ensureThread: (input) =>
            assertLoadAdmission.pipe(
              Effect.andThen(ensureRuntimeThreadAttached(input.threadId)),
              Effect.andThen(
                beforeRuntimeUse(input.threadId, input.runtimePolicy.cwd).pipe(
                  Effect.mapError(
                    (cause) =>
                      new ProviderAdapter.ProviderAdapterEnsureThreadError({
                        driver: runtime.driver,
                        threadId: input.threadId,
                        cause,
                      }),
                  ),
                ),
              ),
              Effect.andThen(
                retainedLoad(
                  input.threadId,
                  JSON.stringify([
                    "ensure",
                    input.threadId,
                    input.existingProviderThread?.id,
                    input.existingProviderThread?.nativeThreadRef,
                    input.modelSelection,
                    input.runtimePolicy,
                    input.forceNewNativeThread,
                  ]),
                  runtime.ensureThread(input),
                  input.existingProviderThread,
                  input,
                ),
              ),
              Effect.tap(({ loaded: providerThread, linkedEnvironments }) =>
                markProviderThreadLoaded({
                  providerSessionId,
                  onlyIfRuntime: runtime,
                  threadId: input.threadId,
                  providerThreadKey: providerThreadLoadKey({
                    runtime,
                    providerThread,
                    modelSelection: input.modelSelection,
                    runtimePolicy: input.runtimePolicy,
                  }),
                  linkedEnvironments,
                }),
              ),
              Effect.tap(({ complete }) => complete),
              Effect.map(({ loaded }) => loaded),
              (effect) => withRuntimeInvocation(input.threadId, effect),
            ),
          resumeThread: (input) => {
            const threadId = input.threadId ?? input.providerThread.appThreadId;
            if (threadId === null || threadId === undefined) {
              return Effect.fail(
                new ProviderRuntimeBindingError({
                  driver: runtime.driver,
                  detail: "A native load requires its captured app-thread owner.",
                }),
              );
            }
            const providerThreadKey = providerThreadLoadKey({
              runtime,
              providerThread: input.providerThread,
              ...(input.modelSelection === undefined
                ? {}
                : { modelSelection: input.modelSelection }),
              ...(input.runtimePolicy === undefined ? {} : { runtimePolicy: input.runtimePolicy }),
            });
            return assertLoadAdmission.pipe(
              Effect.andThen(ensureRuntimeThreadAttached(threadId)),
              Effect.andThen(
                holdThreadLoaded({
                  providerSessionId,
                  onlyIfRuntime: runtime,
                  threadId,
                  providerThread: input.providerThread,
                }),
              ),
              Effect.andThen(
                unloadForLinkedEnvironments({
                  providerSessionId,
                  threadId,
                  providerThread: input.providerThread,
                }),
              ),
              Effect.andThen(
                isProviderThreadLoaded({ providerSessionId, threadId, providerThreadKey }),
              ),
              Effect.flatMap((loaded) =>
                beforeRuntimeUse(threadId, input.runtimePolicy?.cwd).pipe(
                  Effect.mapError(
                    (cause) =>
                      new ProviderAdapter.ProviderAdapterResumeThreadError({
                        driver: runtime.driver,
                        providerSessionId,
                        providerThreadId: input.providerThread.id,
                        cause,
                      }),
                  ),
                  Effect.andThen(
                    loaded
                      ? Effect.succeed<CompletedNativeLoad>({
                          loaded: input.providerThread,
                          linkedEnvironments: undefined,
                          complete: Effect.void,
                        })
                      : retainedLoad(
                          threadId,
                          JSON.stringify([
                            "resume",
                            providerThreadKey,
                            input.modelSelection,
                            input.runtimePolicy,
                          ]),
                          runtime.resumeThread(input),
                          input.providerThread,
                          input,
                        ),
                  ),
                ),
              ),
              Effect.tap(({ loaded: providerThread, linkedEnvironments }) =>
                markProviderThreadLoaded({
                  providerSessionId,
                  onlyIfRuntime: runtime,
                  threadId,
                  providerThreadKey: providerThreadLoadKey({
                    runtime,
                    providerThread,
                    ...(input.modelSelection === undefined
                      ? {}
                      : { modelSelection: input.modelSelection }),
                    ...(input.runtimePolicy === undefined
                      ? {}
                      : { runtimePolicy: input.runtimePolicy }),
                  }),
                  linkedEnvironments,
                }),
              ),
              Effect.tap(({ complete }) => complete),
              Effect.map(({ loaded }) => loaded),
              (effect) => withRuntimeInvocation(threadId, effect),
            );
          },
          forkThread: (input) =>
            ensureRuntimeThreadAttached(input.targetThreadId).pipe(
              Effect.andThen(
                beforeRuntimeUse(input.targetThreadId, input.runtimePolicy?.cwd).pipe(
                  Effect.mapError(
                    (cause) =>
                      new ProviderAdapter.ProviderAdapterForkThreadError({
                        driver: runtime.driver,
                        providerThreadId: input.sourceProviderThread.id,
                        cause,
                      }),
                  ),
                ),
              ),
              Effect.andThen(
                retainedLoad(
                  input.targetThreadId,
                  JSON.stringify([
                    "fork",
                    input.targetThreadId,
                    input.sourceProviderThread,
                    input.modelSelection,
                    input.runtimePolicy,
                  ]),
                  runtime.forkThread(input),
                ),
              ),
              Effect.tap(({ loaded: providerThread, linkedEnvironments }) =>
                markProviderThreadLoaded({
                  providerSessionId,
                  onlyIfRuntime: runtime,
                  threadId: input.targetThreadId,
                  providerThreadKey: providerThreadLoadKey({
                    runtime,
                    providerThread,
                    ...(input.modelSelection === undefined
                      ? {}
                      : { modelSelection: input.modelSelection }),
                    ...(input.runtimePolicy === undefined
                      ? {}
                      : { runtimePolicy: input.runtimePolicy }),
                  }),
                  linkedEnvironments,
                }),
              ),
              Effect.tap(({ complete }) => complete),
              Effect.map(({ loaded }) => loaded),
              (effect) => withRuntimeInvocation(input.targetThreadId, effect),
            ),
          startTurn: (value) => {
            const input = guardTurnInput(value);
            return ensureRuntimeThreadAttached(input.threadId).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterTurnStartError({
                    driver: runtime.driver,
                    threadId: input.threadId,
                    providerThreadId: input.providerThread.id,
                    runId: input.runId,
                    cause,
                  }),
              ),
              // Failed or stopped starts may omit turn.terminal; clear their turns so sessions can idle.
              Effect.andThen(
                holdThreadLoaded({
                  providerSessionId,
                  onlyIfRuntime: runtime,
                  threadId: input.threadId,
                  providerThread: input.providerThread,
                }),
              ),
              Effect.andThen(
                Effect.acquireUseRelease(
                  markBusy(
                    providerSessionId,
                    busyTurnKey(input.providerThread.id, input.runOrdinal),
                    runtime,
                  ).pipe(
                    Effect.catchCause((cause) =>
                      Effect.logWarning("orchestration-v2.driver-session.activity-failed", {
                        providerSessionId,
                        cause,
                      }).pipe(Effect.as(true)),
                    ),
                    // A release between attach and markBusy leaves nothing to mark: fail the
                    // start instead of running it on a runtime being torn down.
                    Effect.flatMap((marked) =>
                      marked
                        ? Effect.void
                        : Effect.fail(
                            new ProviderAdapter.ProviderAdapterTurnStartError({
                              driver: runtime.driver,
                              threadId: input.threadId,
                              providerThreadId: input.providerThread.id,
                              runId: input.runId,
                              cause: "The provider session was released while the turn started.",
                            }),
                          ),
                    ),
                  ),
                  () =>
                    beforeRuntimeUse(input.threadId, input.runtimePolicy.cwd).pipe(
                      Effect.mapError(
                        (cause) =>
                          new ProviderAdapter.ProviderAdapterTurnStartError({
                            driver: runtime.driver,
                            threadId: input.threadId,
                            providerThreadId: input.providerThread.id,
                            runId: input.runId,
                            cause,
                          }),
                      ),
                      Effect.andThen(input.revalidateStartAdmission),
                      Effect.andThen(
                        runtime
                          .startTurn(input)
                          .pipe(
                            Effect.provideService(
                              StartAdmission.CurrentRevalidation,
                              input.revalidateStartAdmission,
                            ),
                          ),
                      ),
                      turnMetrics("send", input.modelSelection.model),
                    ),
                  (_, exit) =>
                    Exit.isFailure(exit)
                      ? observeActivity(
                          providerSessionId,
                          markIdle(
                            providerSessionId,
                            input.providerThread.id,
                            input.runOrdinal,
                            runtime,
                          ),
                        ).pipe(
                          // A failed start can leave its native thread gone (Claude rejecting a
                          // resume), so the next start must ask the adapter to resume it again.
                          Effect.andThen(
                            forgetProviderThreadLoaded({
                              providerSessionId,
                              threadId: input.threadId,
                              runtime,
                            }),
                          ),
                        )
                      : Effect.void,
                ),
              ),
              (effect) => withRuntimeInvocation(input.threadId, effect),
            );
          },
          ...(runtime.resumeTurn === undefined
            ? {}
            : {
                resumeTurn: (value: ProviderAdapter.ProviderAdapterV2ResumeTurnInput) => {
                  const guarded = guardTurnInput(ProviderAdapter.resumeTurnInput(value));
                  const { message: _message, resume: _resume, ...rest } = guarded;
                  return withBoundThreadInvocation(
                    value.providerThread,
                    guarded.revalidateStartAdmission.pipe(
                      Effect.andThen(
                        runtime.resumeTurn!({
                          ...rest,
                          messageId: value.messageId,
                        }).pipe(
                          Effect.provideService(
                            StartAdmission.CurrentRevalidation,
                            guarded.revalidateStartAdmission,
                          ),
                        ),
                      ),
                    ),
                    guarded.runtimePolicy.cwd,
                  );
                },
              }),
          ...(runtime.compactThread === undefined
            ? {}
            : {
                compactThread: (value: ProviderAdapter.ProviderAdapterV2TurnInput) => {
                  const guarded = guardTurnInput(value);
                  return withBoundThreadInvocation(
                    guarded.providerThread,
                    guarded.revalidateStartAdmission.pipe(
                      Effect.andThen(
                        runtime.compactThread!(guarded).pipe(
                          Effect.provideService(
                            StartAdmission.CurrentRevalidation,
                            guarded.revalidateStartAdmission,
                          ),
                        ),
                      ),
                    ),
                    guarded.runtimePolicy.cwd,
                  );
                },
              }),
          ...(runtime.compactForContextHandoff === undefined
            ? {}
            : {
                compactForContextHandoff: (value: ProviderAdapter.ProviderAdapterV2TurnInput) => {
                  const input = guardTurnInput(value);
                  return withBoundThreadInvocation(
                    input.providerThread,
                    Effect.gen(function* () {
                      const activityKey = `${busyTurnKey(input.providerThread.id, input.runOrdinal)}:handoff:${input.attemptId}`;
                      yield* ensureRuntimeThreadAttached(input.threadId);
                      yield* holdThreadLoaded({
                        providerSessionId,
                        threadId: input.threadId,
                        providerThread: input.providerThread,
                        onlyIfRuntime: runtime,
                      });
                      // The caller scope spans compaction, injection and prompt dispatch.
                      // Its separate lease must never clear the original turn's activity.
                      yield* Effect.acquireRelease(
                        markBusy(providerSessionId, activityKey, runtime).pipe(
                          Effect.mapError(
                            (cause) =>
                              new ProviderRuntimeBindingError({
                                driver: runtime.driver,
                                detail: "Cannot retain the original handoff compaction activity.",
                                cause,
                              }),
                          ),
                          Effect.flatMap((marked) =>
                            marked
                              ? Effect.void
                              : Effect.fail(
                                  new ProviderRuntimeBindingError({
                                    driver: runtime.driver,
                                    detail:
                                      "The captured runtime was released before handoff compaction.",
                                  }),
                                ),
                          ),
                        ),
                        () =>
                          observeActivity(
                            providerSessionId,
                            markIdle(
                              providerSessionId,
                              input.providerThread.id,
                              input.runOrdinal,
                              runtime,
                              activityKey,
                            ),
                          ),
                      );
                      yield* input.revalidateStartAdmission;
                      return yield* runtime.compactForContextHandoff!(input).pipe(
                        Effect.provideService(
                          StartAdmission.CurrentRevalidation,
                          input.revalidateStartAdmission,
                        ),
                      );
                    }),
                    input.runtimePolicy.cwd,
                  );
                },
              }),
          steerTurn: (input) =>
            observeActivity(
              providerSessionId,
              touchActivity(providerSessionId, { onlyIfRuntime: runtime }),
            ).pipe(
              Effect.andThen(assertControlTarget(input.providerThread, input.threadId, true)),
              Effect.andThen(
                beforeRuntimeUse(input.threadId).pipe(
                  Effect.mapError(
                    (cause) =>
                      new ProviderAdapter.ProviderAdapterProtocolError({
                        driver: runtime.driver,
                        detail: "Cannot validate the original workspace before steering.",
                        cause,
                      }),
                  ),
                ),
              ),
              Effect.andThen(runtime.steerTurn(input).pipe(turnMetrics("steer"))),
              (effect) => withRuntimeInvocation(input.threadId, effect),
            ),
          // A provider starved of memory can stop answering, so no caller may wait on it unbounded.
          interruptTurn: (input) =>
            observeActivity(
              providerSessionId,
              touchActivity(providerSessionId, { onlyIfRuntime: runtime }),
            ).pipe(
              Effect.andThen(assertControlTarget(input.providerThread)),
              Effect.andThen(
                runtime.interruptTurn(input).pipe(
                  Effect.timeoutOrElse({
                    duration: PROVIDER_INTERRUPT_TIMEOUT_MS,
                    orElse: () =>
                      Effect.fail(
                        new ProviderAdapter.ProviderAdapterInterruptTimeoutError({
                          driver: runtime.driver,
                          providerThreadId: input.providerThread.id,
                          providerTurnId: input.providerTurnId,
                          timeoutMs: PROVIDER_INTERRUPT_TIMEOUT_MS,
                        }),
                      ),
                  }),
                  turnMetrics("interrupt"),
                ),
              ),
            ),
          respondToRuntimeRequest: (input) =>
            observeActivity(
              providerSessionId,
              touchActivity(providerSessionId, { onlyIfRuntime: runtime }),
            ).pipe(
              Effect.andThen(
                runtimeUseGuard.beforeResponse(runtime, input).pipe(
                  Effect.mapError(
                    (cause) =>
                      new ProviderAdapter.ProviderAdapterProtocolError({
                        driver: runtime.driver,
                        detail: "Cannot validate the original runtime request before response.",
                        cause,
                      }),
                  ),
                ),
              ),
              Effect.andThen(
                runtime
                  .respondToRuntimeRequest(input)
                  .pipe(turnMetrics("runtime-request-response")),
              ),
              withRuntimeOperation,
            ),
        };
      };

      const persistProviderSessionUpdate = Effect.fn(
        "ProviderSessionManager.persistProviderSessionUpdate",
      )(
        function* (
          entry: LiveSessionEntry,
          event: Extract<
            ProviderAdapter.ProviderAdapterV2Event,
            { readonly type: "provider_session.updated" }
          >,
        ) {
          const current = (yield* Ref.get(sessions)).get(
            sessionKey(entry.runtime.providerSessionId),
          );
          if (current?.runtime !== entry.runtime) {
            return;
          }
          yield* writeProviderSessionEvents({
            runtime: entry.runtime,
            threadIds: current.attachedThreadIds,
            type: "provider-session.updated",
            payload: event.providerSession,
          });
        },
        (effect, entry, _event) =>
          effect.pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("orchestration-v2.driver-session.status-persist-failed", {
                providerSessionId: entry.runtime.providerSessionId,
                cause,
              }),
            ),
          ),
      );

      const makeRetryStallWatch = (entry: LiveSessionEntry) => {
        const runningRetries = new Map<ProviderTurnId, ProviderRetryItem>();
        const stalledRetries = new Map<ProviderTurnId, ProviderRetryItem>();
        const stallTimers = new Map<ProviderTurnId, Fiber.Fiber<void>>();
        // Claude and Pi keep the retry row running while a recovered reply streams, so any
        // event on the retry's thread counts as the provider being alive.
        const activeSinceCheck = new Set<ProviderTurnId>();
        const noteThreadActivity = (event: ProviderAdapter.ProviderAdapterV2Event) => {
          if (runningRetries.size === 0) return;
          const threadId =
            event.type === "turn_item.updated"
              ? event.turnItem.threadId
              : event.type === "message.updated"
                ? event.message.threadId
                : event.type === "node.updated"
                  ? event.node.threadId
                  : event.type === "plan.updated"
                    ? event.plan.threadId
                    : undefined;
          if (threadId === undefined) return;
          for (const item of runningRetries.values()) {
            if (item.threadId === threadId) activeSinceCheck.add(item.providerTurnId);
          }
        };
        // Stops the stalled turn; its interrupted terminal is then reported as the retry's failure.
        const stopStalledRetry = Effect.fn("ProviderSessionManager.stopStalledRetry")(
          function* (item: ProviderRetryItem) {
            if (runningRetries.get(item.providerTurnId) !== item) return;
            // The terminal this stop causes must not interrupt the stop itself.
            stallTimers.delete(item.providerTurnId);
            const current = (yield* Ref.get(sessions)).get(
              sessionKey(entry.runtime.providerSessionId),
            );
            if (current?.runtime !== entry.runtime) return;
            const { providerThread } = yield* projectionStore.getProviderControlContext(
              item.threadId,
              { providerThreadId: item.providerThreadId, providerTurnId: item.providerTurnId },
            );
            if (providerThread === undefined || runningRetries.get(item.providerTurnId) !== item)
              return;
            stalledRetries.set(item.providerTurnId, item);
            yield* entry.exposedRuntime
              .interruptTurn({ providerThread, providerTurnId: item.providerTurnId })
              .pipe(
                // A slow stop can still end the turn later, so that terminal keeps the retry failure.
                Effect.catchTags({
                  ProviderAdapterInterruptTimeoutError: (cause) =>
                    Effect.logWarning("orchestration-v2.provider-retry-stall-stop-timed-out", {
                      providerSessionId: entry.runtime.providerSessionId,
                      cause,
                    }),
                }),
              );
          },
          (effect, item) =>
            effect.pipe(
              Effect.catchCause((cause) =>
                Effect.sync(() => stalledRetries.delete(item.providerTurnId)).pipe(
                  Effect.andThen(
                    Effect.logWarning("orchestration-v2.provider-retry-stall-stop-failed", {
                      providerSessionId: entry.runtime.providerSessionId,
                      cause,
                    }),
                  ),
                ),
              ),
            ),
        );
        const stopStallTimer = (providerTurnId: ProviderTurnId) => {
          const fiber = stallTimers.get(providerTurnId);
          stallTimers.delete(providerTurnId);
          activeSinceCheck.delete(providerTurnId);
          return fiber === undefined ? Effect.void : Fiber.interrupt(fiber);
        };
        const trackProviderRetry = (
          event: ProviderAdapter.ProviderAdapterV2Event,
        ): {
          readonly event: ProviderAdapter.ProviderAdapterV2Event;
          readonly armed?: ProviderRetryItem;
          readonly settled?: ProviderTurnId;
        } => {
          noteThreadActivity(event);
          if (event.type === "turn_item.updated") {
            const item = event.turnItem;
            if (
              item.type !== "error" ||
              item.retry === undefined ||
              item.providerTurnId === null ||
              item.providerThreadId === null
            )
              return { event };
            const retryItem = {
              ...item,
              providerTurnId: item.providerTurnId,
              providerThreadId: item.providerThreadId,
              retry: item.retry,
            };
            if (item.status === "running") {
              runningRetries.set(retryItem.providerTurnId, retryItem);
              return { event, armed: retryItem };
            }
            // A stall marker is set only once the stop is sent, so only the terminal clears it.
            runningRetries.delete(retryItem.providerTurnId);
            return { event, settled: retryItem.providerTurnId };
          }
          if (event.type === "provider_turn.updated") {
            if (event.providerTurn.status === "running") return { event };
            runningRetries.delete(event.providerTurn.id);
            const settled = event.providerTurn.id;
            return stalledRetries.has(settled) &&
              (event.providerTurn.status === "interrupted" ||
                event.providerTurn.status === "cancelled")
              ? {
                  event: { ...event, providerTurn: { ...event.providerTurn, status: "failed" } },
                  settled,
                }
              : { event, settled };
          }
          if (event.type !== "turn.terminal") return { event };
          runningRetries.delete(event.providerTurnId);
          const stalled = stalledRetries.get(event.providerTurnId);
          stalledRetries.delete(event.providerTurnId);
          const settled = event.providerTurnId;
          if (
            stalled === undefined ||
            (event.status !== "interrupted" && event.status !== "cancelled")
          )
            return { event, settled };
          return {
            settled,
            event: {
              type: "turn.terminal",
              driver: event.driver,
              providerThreadId: event.providerThreadId,
              providerTurnId: event.providerTurnId,
              runOrdinal: event.runOrdinal,
              failureItemOrdinal: stalled.ordinal,
              status: "failed",
              failure: { ...stalled.failure, retryable: false },
              retry: stalled.retry,
              ...(stalled.startedAt === null ? {} : { retryStartedAt: stalled.startedAt }),
              threadDisposition: event.threadDisposition,
              ...(event.retainedRuntimeRequestIds === undefined
                ? {}
                : { retainedRuntimeRequestIds: event.retainedRuntimeRequestIds }),
            },
          };
        };
        return {
          track: trackProviderRetry,
          // Runs for every provider event; spans would be per-event noise. Timers are children
          // of the pump fiber, so a released session takes them down.
          update: Effect.fnUntraced(function* (tracked: ReturnType<typeof trackProviderRetry>) {
            const item = tracked.armed;
            if (item === undefined) {
              if (tracked.settled !== undefined) yield* stopStallTimer(tracked.settled);
              return;
            }
            yield* stopStallTimer(item.providerTurnId);
            const window = Duration.sum(
              PROVIDER_RETRY_STALL_TIMEOUT,
              Duration.millis(item.retry.retryDelayMs ?? 0),
            );
            const timer = yield* Effect.gen(function* () {
              // Only a thread that stayed silent for a whole window has stalled.
              do {
                activeSinceCheck.delete(item.providerTurnId);
                yield* Effect.sleep(window);
              } while (activeSinceCheck.has(item.providerTurnId));
              yield* stopStalledRetry(item);
            }).pipe(Effect.forkChild({ startImmediately: true }));
            stallTimers.set(item.providerTurnId, timer);
          }),
        };
      };

      const startEventPump = (entry: LiveSessionEntry) => {
        let stoppedByProvider = false;
        let lastSessionStatus = entry.runtime.providerSession.status;
        const retryStalls = makeRetryStallWatch(entry);
        return entry.runtime.events.pipe(
          Stream.filterEffect((event) => validateProviderEvent(entry.runtime, event)),
          Stream.runForEach((nativeEvent) => {
            if (shutdownSignal.received) return Effect.void;
            // Live output bypasses persistence and subscribers so floods cannot delay domain events.
            if (nativeEvent.type === "command_output.delta") {
              return commandOutput.append(nativeEvent);
            }
            if (nativeEvent.type === "prompt_suggestion.updated") {
              return SubscriptionRef.update(promptSuggestionStore, (current) => {
                const next = new Map(current);
                next.delete(nativeEvent.threadId);
                next.set(nativeEvent.threadId, {
                  runId: nativeEvent.runId,
                  text: nativeEvent.text,
                });
                for (const threadId of next.keys()) {
                  if (next.size <= MAX_PROMPT_SUGGESTIONS) break;
                  next.delete(threadId);
                }
                return next;
              });
            }
            const tracked = retryStalls.track(nativeEvent);
            const event = ProviderEventOrigin.copyProviderEventOrigin(nativeEvent, tracked.event);
            const armRetryStall = retryStalls.update(tracked);
            const releaseLiveOutput =
              event.type === "turn_item.updated" &&
              event.turnItem.type === "command_execution" &&
              !isOrchestrationV2WorkActive(event.turnItem.status)
                ? commandOutput.finish({
                    threadId: event.turnItem.threadId,
                    itemId: event.turnItem.id,
                  })
                : Effect.void;
            // Metadata can change while idle without extending the runtime's lifetime.
            const metadataOnly =
              event.type === "provider_session.updated" &&
              event.providerSession.status === lastSessionStatus;
            if (event.type === "provider_session.updated") {
              lastSessionStatus = event.providerSession.status;
              if (lastSessionStatus === "stopped") stoppedByProvider = true;
            }
            return observeActivity(
              entry.runtime.providerSessionId,
              event.type === "turn.terminal"
                ? markIdle(
                    entry.runtime.providerSessionId,
                    event.providerThreadId,
                    event.runOrdinal,
                    entry.runtime,
                  )
                : metadataOnly
                  ? Effect.void
                  : touchActivity(entry.runtime.providerSessionId, {
                      providerActivity: true,
                      onlyIfRuntime: entry.runtime,
                    }),
            ).pipe(
              Effect.andThen(armRetryStall),
              Effect.andThen(
                event.type === "provider_session.updated"
                  ? persistProviderSessionUpdate(entry, event)
                  : Effect.void,
              ),
              Effect.andThen(
                Effect.gen(function* () {
                  if (
                    event.type === "provider_thread.updated" &&
                    event.providerThread.status === "error"
                  ) {
                    const threadId = event.providerThread.appThreadId;
                    yield* Ref.update(sessions, (sessions) => {
                      const key = sessionKey(entry.runtime.providerSessionId);
                      const latest = sessions.get(key);
                      if (latest?.runtime !== entry.runtime || threadId === null) return sessions;
                      const loadedProviderThreadKeyByThread = new Map(
                        latest.loadedProviderThreadKeyByThread,
                      );
                      loadedProviderThreadKeyByThread.delete(threadId);
                      return new Map(sessions).set(key, {
                        ...latest,
                        loadedProviderThreadKeyByThread,
                      });
                    });
                  }
                  // Requests and recovered background commands can arrive before a run subscribes.
                  const threadId = sessionScopedArtifactThreadId(event);
                  if (threadId !== undefined) {
                    yield* Effect.gen(function* () {
                      const current = (yield* Ref.get(sessions)).get(
                        sessionKey(entry.runtime.providerSessionId),
                      );
                      if (
                        current?.runtime !== entry.runtime ||
                        ((event.type === "provider_thread.updated" ||
                          (event.type === "turn_item.updated" &&
                            event.turnItem.type === "command_execution")) &&
                          !current.attachedThreadIds.has(threadId))
                      )
                        return;
                      yield* providerEventIngestor
                        .ingestNormalized({
                          providerSessionId: entry.runtime.providerSessionId,
                          providerInstanceId: entry.runtime.instanceId,
                          threadId,
                          event,
                        })
                        .pipe(
                          Effect.mapError(
                            (cause) =>
                              new ProviderAdapter.ProviderAdapterEventStreamError({
                                driver: entry.runtime.driver,
                                providerSessionId: entry.runtime.providerSessionId,
                                raisedByT3: true,
                                cause,
                              }),
                          ),
                        );
                    }).pipe(entry.requestEventPermit.withPermits(1));
                    yield* releaseLiveOutput;
                    return;
                  }
                  yield* publishToSubscribers(entry.eventSubscribers, { type: "event", event });
                  yield* releaseLiveOutput;
                }),
              ),
            );
          }),
          Effect.exit,
          Effect.flatMap((exit) =>
            Effect.gen(function* () {
              if (shutdownSignal.received) return;
              const current = (yield* Ref.get(sessions)).get(
                sessionKey(entry.runtime.providerSessionId),
              );
              if (current?.runtime !== entry.runtime) {
                return;
              }
              if (stoppedByProvider && Exit.isSuccess(exit)) {
                yield* releaseEntry({
                  providerSessionId: entry.runtime.providerSessionId,
                  onlyIfRuntime: entry.runtime,
                  reason: "manual_shutdown",
                  gracefulSubscribers: true,
                }).pipe(logReleaseFailure(entry.runtime.providerSessionId));
                return;
              }
              // Subscribers read an interrupt as server shutdown, so keep it for that.
              const cause =
                Exit.isFailure(exit) && !Cause.hasInterruptsOnly(exit.cause)
                  ? exit.cause
                  : Cause.fail(
                      new ProviderAdapter.ProviderAdapterEventStreamError({
                        driver: entry.runtime.driver,
                        providerSessionId: entry.runtime.providerSessionId,
                        raisedByT3: true,
                        cause: "Provider event stream ended unexpectedly.",
                      }),
                    );
              // lastError is the thread banner, so it gets the same bounded
              // message as a failed turn; the raw cause survives only in the log.
              yield* entry.failureOwnership.permit.withPermits(1)(
                Effect.sync(() => {
                  entry.failureOwnership.failed = true;
                }),
              );
              yield* Effect.logWarning("orchestration-v2.provider-session-runtime-failed", {
                providerSessionId: entry.runtime.providerSessionId,
                cause,
              });
              yield* releaseEntry({
                providerSessionId: entry.runtime.providerSessionId,
                onlyIfRuntime: entry.runtime,
                reason: "runtime_error",
                detail: makeProviderFailure({ cause }).message,
                eventStreamCause: cause,
              }).pipe(logReleaseFailure(entry.runtime.providerSessionId));
            }),
          ),
          Effect.ensuring(Deferred.succeed(entry.eventPumpSettled, undefined)),
          Effect.forkIn(layerScope),
        );
      };

      // Shutdown must fit the launcher's 5 s deadline with up to 15 sessions.
      const sessionScopes = yield* Scope.make("parallel");
      const shutdown = Effect.gen(function* () {
        const activeSessions = [...(yield* Ref.get(sessions)).values()];
        yield* Effect.forEach(
          activeSessions,
          (entry) =>
            releaseEntry({
              providerSessionId: entry.runtime.providerSessionId,
              reason: "server_shutdown",
            }).pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("orchestration-v2.driver-session.shutdown-release-failed", {
                  providerSessionId: entry.runtime.providerSessionId,
                  cause,
                }),
              ),
            ),
          { concurrency: "unbounded", discard: true },
        );
      }).pipe(Effect.withSpan("ProviderSessionManager.shutdown"));
      yield* Effect.addFinalizer(() =>
        shutdown.pipe(
          Effect.ensuring(closeScopeWithin(sessionScopes, { reason: "server_shutdown" })),
        ),
      );

      const equivalentPhysicalObservation = Schema.toEquivalence(PhysicalProcessObservation);
      const readCurrentThreadRuntimeAttachment = Effect.fn(
        "ProviderSessionManager.readCurrentThreadRuntimeAttachment",
      )(function* (threadId: ThreadId) {
        const observedAt = DateTime.formatIso(yield* DateTime.now);
        const unknown = (reason: string): RuntimeAttachment.CurrentThreadRuntimeAttachment => ({
          status: "unknown",
          reason,
          observedAt,
        });
        return yield* Effect.gen(function* () {
          const before = yield* projectionStore.getThreadRecords(threadId, ["providerThreads"]);
          if (before.thread.archivedAt !== null || before.thread.deletedAt !== null)
            return unknown("thread_unavailable");
          const entries = yield* Ref.get(sessions);
          const noResident = Ref.get(sessions).pipe(
            Effect.map(
              (current) =>
                ![...current.values()].some((entry) => entry.attachedThreadIds.has(threadId)),
            ),
          );
          if (![...entries.values()].some((entry) => entry.attachedThreadIds.has(threadId)))
            return { status: "stopped", observedAt, isCurrent: noResident } as const;
          const provider = before.providerThreads.find(
            (thread) => thread.id === before.thread.activeProviderThreadId,
          );
          const generation = provider?.runtimeIdentity?.runtimeGeneration;
          const binding =
            provider === undefined || generation === undefined
              ? undefined
              : runtimeBinding(provider, generation);
          if (binding === undefined || binding.threadId !== threadId)
            return unknown("native_binding_unavailable");
          const key = sessionKey(binding.providerSessionId);
          const entry = entries.get(key);
          if (
            entry === undefined ||
            !entry.attachedThreadIds.has(threadId) ||
            entry.runtime.instanceId !== binding.providerInstanceId ||
            entry.runtime.driver !== binding.driver ||
            entry.runtime.providerSessionId !== binding.providerSessionId
          )
            return unknown("runtime_binding_changed");
          const runtime = entry.runtime;
          const loadedKey = entry.loadedProviderThreadKeyByThread.get(threadId);
          if (loadedKey === undefined) return unknown("runtime_not_loaded");
          const attachedThreadIds = entry.attachedThreadIds;
          const evidenceRevision = provider?.runtimeIdentity?.evidenceRevision ?? null;
          const isCurrent = Effect.gen(function* () {
            const after = yield* projectionStore.getThreadRecords(threadId, ["providerThreads"]);
            const currentProvider = after.providerThreads.find(
              (thread) => thread.id === after.thread.activeProviderThreadId,
            );
            const currentGeneration = currentProvider?.runtimeIdentity?.runtimeGeneration;
            const currentBinding =
              currentProvider === undefined || currentGeneration === undefined
                ? undefined
                : runtimeBinding(currentProvider, currentGeneration);
            const current = (yield* Ref.get(sessions)).get(key);
            return (
              currentBinding !== undefined &&
              equivalentBinding(binding, currentBinding) &&
              (currentProvider?.runtimeIdentity?.evidenceRevision ?? null) === evidenceRevision &&
              current?.runtime === runtime &&
              current.attachedThreadIds === attachedThreadIds &&
              current.attachedThreadIds.has(threadId) &&
              current.loadedProviderThreadKeyByThread.get(threadId) === loadedKey &&
              DateTime.toEpochMillis(after.thread.createdAt) ===
                DateTime.toEpochMillis(before.thread.createdAt) &&
              after.thread.archivedAt === null &&
              after.thread.deletedAt === null
            );
          }).pipe(Effect.catchCause(() => Effect.succeed(false)));
          if (!(yield* isCurrent)) return unknown("runtime_binding_changed");
          const physicalIncarnation = yield* physicalEvidence.read(binding);
          if (!(yield* isCurrent)) return unknown("runtime_binding_changed");
          return {
            status: "attached",
            binding: Object.freeze({ ...binding }),
            evidenceRevision,
            observedAt,
            isCurrent,
            physicalIncarnation,
            physicalIsCurrent: Effect.gen(function* () {
              if (!(yield* isCurrent) || physicalIncarnation.status !== "observed") return false;
              const fresh = yield* physicalEvidence.read(binding);
              return (
                fresh.status === "observed" &&
                equivalentPhysicalObservation(fresh, physicalIncarnation) &&
                (yield* isCurrent)
              );
            }),
          } as const;
        }).pipe(Effect.catchCause(() => Effect.succeed(unknown("runtime_binding_unavailable"))));
      });
      const observeThreadActivity = Effect.fn("ProviderSessionManager.observeThreadActivity")(
        function* (threadId: ThreadId) {
          const unavailable = (reason: string): RuntimeObservation.ProviderRuntimeObservation => ({
            status: "unknown",
            reason,
          });
          const read = Effect.gen(function* () {
            const before = yield* projectionStore.getThreadRecords(threadId, ["providerThreads"]);
            const provider = before.providerThreads.find(
              (thread) => thread.id === before.thread.activeProviderThreadId,
            );
            if (provider?.providerSessionId == null)
              return unavailable("native_binding_unavailable");
            const key = sessionKey(provider.providerSessionId);
            const entry = (yield* Ref.get(sessions)).get(key);
            if (entry === undefined) return unavailable("runtime_not_resident");
            const runtime = entry.runtime;
            if (
              !entry.attachedThreadIds.has(threadId) ||
              runtime.providerSessionId !== provider.providerSessionId ||
              runtime.instanceId !== provider.providerInstanceId ||
              runtime.driver !== provider.driver
            )
              return unavailable("runtime_binding_changed");
            if (runtime.readThreadActivity === undefined)
              return unavailable("native_activity_unsupported");
            const generation = provider.runtimeIdentity?.runtimeGeneration;
            const binding =
              generation === undefined ? undefined : runtimeBinding(provider, generation);
            if (binding === undefined || binding.threadId !== threadId)
              return unavailable("native_binding_unavailable");
            const loadedKey = entry.loadedProviderThreadKeyByThread.get(threadId);
            if (loadedKey === undefined) return unavailable("runtime_not_loaded");
            const sampledAt = yield* Clock.currentTimeMillis;
            const observation = yield* runtime
              .readThreadActivity(provider)
              .pipe(Effect.timeout("3 seconds"));
            const after = yield* projectionStore.getThreadRecords(threadId, ["providerThreads"]);
            const current = after.providerThreads.find(
              (thread) => thread.id === after.thread.activeProviderThreadId,
            );
            const currentGeneration = current?.runtimeIdentity?.runtimeGeneration;
            const currentBinding =
              current === undefined || currentGeneration === undefined
                ? undefined
                : runtimeBinding(current, currentGeneration);
            const currentEntry = (yield* Ref.get(sessions)).get(key);
            if (
              currentBinding === undefined ||
              !equivalentBinding(binding, currentBinding) ||
              currentEntry?.runtime !== runtime ||
              !currentEntry.attachedThreadIds.has(threadId) ||
              currentEntry.loadedProviderThreadKeyByThread.get(threadId) !== loadedKey ||
              after.thread.archivedAt !== null ||
              after.thread.deletedAt !== null
            )
              return unavailable("runtime_binding_changed");
            if (observation.status === "unknown") return observation;
            if (!equivalentBinding(binding, observation.binding))
              return unavailable("runtime_binding_changed");
            const observedAt = Date.parse(observation.observedAt);
            if (!Number.isFinite(observedAt) || observedAt < sampledAt)
              return unavailable("native_activity_stale");
            return observation;
          });
          return yield* read.pipe(
            Effect.catchCause(() => Effect.succeed(unavailable("native_activity_unavailable"))),
          );
        },
      );

      // Jones: a new run may finish the adapter's retained local publication of its
      // exact binding before admission. It never opens, resumes or stops a runtime.
      const reconcileThreadBindingPublication = Effect.fn(
        "ProviderSessionManager.reconcileThreadBindingPublication",
      )(
        function* (input: {
          readonly run: OrchestrationV2Run;
          readonly providerThread: OrchestrationV2ProviderThread;
        }) {
          const { run, providerThread } = input;
          const providerSessionId = providerThread.providerSessionId;
          if (providerSessionId === null) return null;
          const entry = (yield* Ref.get(sessions)).get(sessionKey(providerSessionId));
          const reconcile = entry?.runtime.reconcileBindingPublication;
          if (
            entry === undefined ||
            reconcile === undefined ||
            !entry.attachedThreadIds.has(run.threadId)
          )
            return null;
          const nativeThreadId = providerThread.nativeThreadRef?.nativeId ?? null;
          const refused = (cause: unknown) =>
            new ProviderRuntimeBindingError({
              driver: providerThread.driver,
              detail: "The new run no longer owns this retained binding publication.",
              cause,
            });
          const revalidate = Effect.gen(function* () {
            const latestEntry = (yield* Ref.get(sessions)).get(sessionKey(providerSessionId));
            const current = yield* projectionStore.getTurnStartContext(run.threadId, run.id);
            const currentRun = current.runs.find((candidate) => candidate.id === run.id);
            const target = current.providerThreads.find(
              (candidate) => candidate.id === providerThread.id,
            );
            if (
              run.activeAttemptId === null ||
              run.status !== "starting" ||
              run.threadId !== providerThread.appThreadId ||
              run.providerThreadId !== providerThread.id ||
              run.providerInstanceId !== providerThread.providerInstanceId ||
              latestEntry === undefined ||
              latestEntry.runtime !== entry.runtime ||
              !latestEntry.attachedThreadIds.has(run.threadId) ||
              entry.runtime.driver !== providerThread.driver ||
              entry.runtime.instanceId !== providerThread.providerInstanceId ||
              entry.runtime.providerSessionId !== providerSessionId ||
              currentRun?.status !== "starting" ||
              currentRun.threadId !== run.threadId ||
              currentRun.activeAttemptId !== run.activeAttemptId ||
              currentRun.providerThreadId !== providerThread.id ||
              currentRun.providerInstanceId !== run.providerInstanceId ||
              !modelSelectionsEqual(currentRun.modelSelection, run.modelSelection) ||
              !current.attempts.some(
                (attempt) =>
                  attempt.id === run.activeAttemptId &&
                  attempt.runId === run.id &&
                  attempt.status === "pending" &&
                  attempt.providerThreadId === providerThread.id &&
                  attempt.providerInstanceId === run.providerInstanceId,
              ) ||
              target?.appThreadId !== run.threadId ||
              target.driver !== providerThread.driver ||
              target.providerInstanceId !== providerThread.providerInstanceId ||
              target.providerSessionId !== providerSessionId ||
              (target.nativeThreadRef !== null &&
                target.nativeThreadRef.driver !== providerThread.driver) ||
              (nativeThreadId !== null && target.nativeThreadRef?.nativeId !== nativeThreadId) ||
              current.thread.deletedAt !== null ||
              current.thread.archivedAt !== null
            )
              return yield* refused(
                "The captured run, attempt, target, or existing runtime changed.",
              );
          }).pipe(Effect.mapError(refused));
          yield* revalidate;
          let recovered = yield* reconcile({
            threadId: run.threadId,
            providerThreadId: providerThread.id,
            nativeThreadId,
            revalidate,
          });
          yield* revalidate;
          const loads = originalThreadLoads.get(entry.runtime);
          const original = loads?.get(run.threadId);
          if (recovered === null) {
            if (original?.publication === undefined) return null;
            recovered = yield* original.publication.readCompleted;
            if (recovered === null)
              return yield* refused("The retained original publication is not complete.");
            yield* revalidate;
          }
          const records = yield* projectionStore.getThreadRecords(run.threadId, [
            "providerThreads",
          ]);
          const current = records.providerThreads.find(
            (candidate) => candidate.id === providerThread.id,
          );
          const generation = recovered.runtimeIdentity?.runtimeGeneration;
          const recoveredBinding =
            generation === undefined ? undefined : runtimeBinding(recovered, generation);
          const currentBinding =
            current === undefined || generation === undefined
              ? undefined
              : runtimeBinding(current, generation);
          if (
            recovered.id !== providerThread.id ||
            recovered.appThreadId !== run.threadId ||
            recovered.providerSessionId !== providerSessionId ||
            recovered.providerInstanceId !== providerThread.providerInstanceId ||
            recovered.driver !== providerThread.driver ||
            recovered.nativeThreadRef?.driver !== providerThread.driver ||
            current === undefined ||
            generation === undefined ||
            recoveredBinding === undefined ||
            currentBinding === undefined ||
            current.runtimeIdentity?.runtimeGeneration !== generation ||
            !equivalentBinding(recoveredBinding, currentBinding)
          )
            return yield* refused(
              "Recovered publication does not match the current exact binding.",
            );
          if (original !== undefined) {
            // Jones: an ensure load captures no prior target; its binding still must match.
            const target = original.target;
            if (
              (target !== undefined &&
                (target.id !== current.id ||
                  target.appThreadId !== run.threadId ||
                  target.driver !== current.driver ||
                  target.providerSessionId !== providerSessionId ||
                  target.providerInstanceId !== current.providerInstanceId ||
                  (target.nativeThreadId !== null &&
                    target.nativeThreadId !== current.nativeThreadRef?.nativeId))) ||
              original.binding === undefined ||
              !equivalentBinding(original.binding, currentBinding)
            )
              return yield* refused("The original load metadata belongs to another binding.");
            const linkedEnvironments = original.linkedEnvironments;
            if (linkedEnvironments === undefined)
              return yield* refused("The original native load settings were not captured.");
            // The recovered key is the key the original load records on success.
            const { modelSelection, runtimePolicy } = original.settings;
            const loadedKey = providerThreadLoadKey({
              runtime: entry.runtime,
              providerThread: current,
              ...(modelSelection === undefined ? {} : { modelSelection }),
              ...(runtimePolicy === undefined ? {} : { runtimePolicy }),
            });
            yield* revalidate;
            const installed = yield* Ref.modify(sessions, (entries) => {
              const latest = entries.get(sessionKey(providerSessionId));
              if (
                latest?.runtime !== entry.runtime ||
                !latest.attachedThreadIds.has(run.threadId) ||
                loads?.get(run.threadId) !== original
              )
                return [false, entries] as const;
              const loadedProviderThreadKeyByThread = new Map(
                latest.loadedProviderThreadKeyByThread,
              );
              loadedProviderThreadKeyByThread.set(run.threadId, loadedKey);
              const linkedEnvironmentsByThread = new Map(latest.linkedEnvironmentsByThread);
              if (
                linkedEnvironments.value !== undefined &&
                !linkedEnvironmentsByThread.has(run.threadId)
              )
                linkedEnvironmentsByThread.set(run.threadId, linkedEnvironments.value);
              loads?.delete(run.threadId);
              return [
                true,
                new Map(entries).set(sessionKey(providerSessionId), {
                  ...latest,
                  loadedProviderThreadKeyByThread,
                  linkedEnvironmentsByThread,
                }),
              ] as const;
            });
            if (!installed)
              return yield* refused(
                "The runtime changed before the recovered load could be recorded.",
              );
          }
          return current;
        },
        (effect, input) =>
          effect.pipe(
            Effect.mapError(
              (cause) =>
                new ProviderSessionLookupError({
                  providerSessionId: input.providerThread.providerSessionId!,
                  cause,
                }),
            ),
          ),
      );

      const detachForTransfer = Effect.fn("ProviderSessionManager.detachForTransfer")(
        function* (
          input: Parameters<ProviderSessionManagerV2Shape["detach"]>[0],
          options?: {
            readonly expectedLoadedKey?: string;
            readonly conversationKey?: string;
            readonly awaitCompletion?: boolean;
          },
        ) {
          const prior = retirements.find(input.providerSessionId, input.threadId);
          if (prior !== undefined) {
            if (options?.awaitCompletion !== false)
              yield* Deferred.await(prior.completion).pipe(
                Effect.timeout(UNLOAD_THREAD_TIMEOUT_MS),
              );
            return;
          }
          const entry = (yield* Ref.get(sessions)).get(sessionKey(input.providerSessionId));
          if (entry === undefined) return;
          const reused = yield* threadAttachment.withLock(
            threadAttachmentKey(input),
            Effect.gen(function* () {
              const before = (yield* Ref.get(sessions)).get(sessionKey(input.providerSessionId));
              const proof = before?.completedThreadTransfers.get(input.threadId);
              if (
                proof === undefined ||
                proof.source !== entry.runtime ||
                before?.runtime !== entry.runtime ||
                before.attachedThreadIds.has(input.threadId) ||
                before.loadedProviderThreadKeyByThread.has(input.threadId) ||
                (input.providerThreadId !== undefined &&
                  input.providerThreadId !== proof.providerThreadId) ||
                (input.replacementProviderSessionId !== undefined &&
                  input.replacementProviderSessionId !== proof.replacementProviderSessionId)
              )
                return false;
              const fresh = yield* projectionStore.getThreadRecords(input.threadId, [
                "providerThreads",
              ]);
              const omittedSelector =
                (proof.providerThreadId !== undefined && input.providerThreadId === undefined) ||
                (proof.replacementProviderSessionId !== undefined &&
                  input.replacementProviderSessionId === undefined);
              const selected = selectTransferBindings(
                omittedSelector
                  ? { providerSessionId: input.providerSessionId, threadId: input.threadId }
                  : input,
                entry.runtime,
                fresh.providerThreads,
              );
              const current = (yield* Ref.get(sessions)).get(sessionKey(input.providerSessionId));
              return (
                current?.runtime === entry.runtime &&
                current.completedThreadTransfers.get(input.threadId) === proof &&
                !current.attachedThreadIds.has(input.threadId) &&
                !current.loadedProviderThreadKeyByThread.has(input.threadId) &&
                selected.complete &&
                selected.bindings.length === proof.bindings.length &&
                selected.bindings.every((binding, index) =>
                  equivalentBinding(binding, proof.bindings[index]!),
                )
              );
            }),
          );
          if (reused) return;
          const projection = yield* projectionStore.getThreadRecords(input.threadId, [
            "providerThreads",
            "providerTurns",
          ]);
          const { providerThreads, bindings, complete } = selectTransferBindings(
            input,
            entry.runtime,
            projection.providerThreads,
          );
          const loadedKey =
            options?.expectedLoadedKey ?? entry.loadedProviderThreadKeyByThread.get(input.threadId);
          const providerThreadsById = new Map(providerThreads.map((thread) => [thread.id, thread]));
          yield* Effect.forEach(
            projection.providerTurns.filter(
              (turn) => turn.status === "running" && providerThreadsById.has(turn.providerThreadId),
            ),
            (turn) =>
              entry.exposedRuntime.interruptTurn({
                providerThread: providerThreadsById.get(turn.providerThreadId)!,
                providerTurnId: turn.id,
              }),
            { concurrency: 1, discard: true },
          );
          // Capture exclusivity and publish its attachment fence in the same synchronous transition.
          const captured = yield* threadAttachment.withLock(
            threadAttachmentKey(input),
            Ref.modify(sessions, (current) => {
              const pending = retirements.find(input.providerSessionId, input.threadId);
              if (pending !== undefined)
                return [{ record: pending, idleFiber: null }, current] as const;
              const latest = current.get(sessionKey(input.providerSessionId));
              if (
                latest?.runtime !== entry.runtime ||
                !latest.attachedThreadIds.has(input.threadId) ||
                latest.loadedProviderThreadKeyByThread.get(input.threadId) !== loadedKey
              )
                return [undefined, current] as const;
              const attachedThreadIds = new Set(latest.attachedThreadIds);
              attachedThreadIds.delete(input.threadId);
              originalThreadLoads.get(entry.runtime)?.delete(input.threadId);
              const record = retirements.retain({
                source: latest.runtime,
                scope: latest.scope,
                providerSessionId: input.providerSessionId,
                threadIds: new Set([input.threadId]),
                kind: "transfer",
                sessionWide: attachedThreadIds.size === 0,
                ...(options?.conversationKey === undefined
                  ? {}
                  : { conversationKey: options.conversationKey }),
                bindings,
              });
              const loadedProviderThreadKeyByThread = new Map(
                latest.loadedProviderThreadKeyByThread,
              );
              loadedProviderThreadKeyByThread.delete(input.threadId);
              const idleThreadUnloads = new Map(latest.idleThreadUnloads);
              idleThreadUnloads.delete(input.threadId);
              return [
                { record, idleFiber: latest.idleThreadUnloads.get(input.threadId)?.fiber ?? null },
                new Map(current).set(sessionKey(input.providerSessionId), {
                  ...latest,
                  attachedThreadIds,
                  loadedProviderThreadKeyByThread,
                  idleThreadUnloads,
                }),
              ] as const;
            }),
          );
          if (captured === undefined) {
            const pending = retirements.find(input.providerSessionId, input.threadId);
            if (pending !== undefined) {
              if (options?.awaitCompletion !== false)
                yield* Deferred.await(pending.completion).pipe(
                  Effect.timeout(UNLOAD_THREAD_TIMEOUT_MS),
                );
              return;
            }
            return yield* Effect.fail("The source runtime changed before retirement was captured.");
          }
          const { record } = captured;
          const nativeRetired = Deferred.makeUnsafe<void>();
          const receipts: Array<RuntimeRetirementReceipt> = [];
          let producerExited = false;
          let cleaned = false;
          const cleanupPermit = Semaphore.makeUnsafe(1);
          const finishRetirement = Effect.gen(function* () {
            yield* Deferred.await(nativeRetired);
            if (cleaned) return;
            if (producerExited) {
              yield* Deferred.await(entry.eventPumpSettled);
              yield* releaseEntry({
                providerSessionId: input.providerSessionId,
                onlyIfRuntime: entry.runtime,
                reason: "manual_shutdown",
                detachedThreadId: input.threadId,
              });
            }
            yield* threadAttachment.withLock(
              threadAttachmentKey(input),
              Ref.update(sessions, (current) => {
                const latest = current.get(sessionKey(input.providerSessionId));
                if (
                  latest?.runtime !== entry.runtime ||
                  latest.attachedThreadIds.has(input.threadId) ||
                  latest.loadedProviderThreadKeyByThread.has(input.threadId)
                )
                  return current;
                const completedThreadTransfers = new Map(latest.completedThreadTransfers);
                completedThreadTransfers.set(input.threadId, {
                  source: entry.runtime,
                  bindings: record.bindings,
                  providerThreadId: input.providerThreadId,
                  replacementProviderSessionId: input.replacementProviderSessionId,
                });
                return new Map(current).set(sessionKey(input.providerSessionId), {
                  ...latest,
                  completedThreadTransfers,
                });
              }),
            );
            for (const receipt of receipts) yield* physicalEvidence.invalidate(receipt.binding);
            yield* retirements.complete(record);
            if (input.revokeMcpCredential === true) yield* clearMcpSession(input.threadId);
            yield* scheduleIdleRelease(input.providerSessionId);
            cleaned = true;
          }).pipe(
            cleanupPermit.withPermits(1),
            Effect.mapError(
              (cause) =>
                new ProviderRuntimeBindingError({
                  driver: entry.runtime.driver,
                  detail: "The captured writer retired, but its manager cleanup is incomplete.",
                  cause,
                }),
            ),
          );
          for (const binding of record.bindings)
            recoveryRetirements.retain(binding, finishRetirement);
          yield* Effect.uninterruptible(
            Effect.gen(function* () {
              if (transferWorkers.has(record)) return;
              transferWorkers.add(record);
              const retire = entry.runtime.retireThreadForTransfer;
              yield* Effect.gen(function* () {
                yield* cancelIdleFiber(captured.idleFiber);
                if (
                  retire === undefined ||
                  !complete ||
                  record.bindings.length !== providerThreads.length
                )
                  return yield* Effect.fail("The captured Codex writer cannot prove retirement.");
                for (const providerThread of providerThreads) {
                  const expected = record.bindings.find(
                    (binding) => binding.providerThreadId === providerThread.id,
                  );
                  if (expected === undefined)
                    return yield* Effect.fail("The source binding was not captured.");
                  const receipt = yield* retire({
                    providerThread,
                    allowProducerRetirement: record.sessionWide,
                  });
                  if (!RetirementLedger.receiptMatches(expected, receipt.binding))
                    return yield* Effect.fail(
                      "The writer retirement receipt does not match the captured source.",
                    );
                  producerExited ||= receipt.evidence === "producer_exited";
                  receipts.push(receipt);
                }
                yield* Deferred.succeed(nativeRetired, undefined);
                for (const receipt of receipts) yield* recoveryRetirements.complete(receipt);
                yield* finishRetirement;
              }).pipe(
                Effect.catchCause((cause) =>
                  Deferred.fail(
                    record.completion,
                    new RetirementLedger.WriterRetirementError({
                      providerSessionId: input.providerSessionId,
                      cause,
                    }),
                  ),
                ),
                Effect.forkDetach({ startImmediately: true }),
              );
            }),
          );
          if (options?.awaitCompletion !== false)
            yield* Deferred.await(record.completion).pipe(Effect.timeout(UNLOAD_THREAD_TIMEOUT_MS));
        },
        (effect) =>
          effect.pipe(Effect.forkDetach({ startImmediately: true }), Effect.flatMap(Fiber.join)),
      );

      const captureCurrentThreadRuntimeStop = Effect.fn(
        "ProviderSessionManager.captureCurrentThreadRuntimeStop",
      )(
        function* (
          threadId: ThreadId,
          expected?: {
            readonly runtime: ProviderAdapterV2SessionRuntime;
            readonly provider: OrchestrationV2ProviderThread;
          },
        ) {
          const before = yield* projectionStore.getThreadRecords(threadId, ["providerThreads"]);
          const provider = before.providerThreads.find(
            (row) => row.id === before.thread.activeProviderThreadId,
          );
          if (provider?.providerSessionId == null) return null;
          const key = sessionKey(provider.providerSessionId);
          const entry = (yield* Ref.get(sessions)).get(key);
          if (
            !entry?.attachedThreadIds.has(threadId) ||
            entry.runtime.captureRuntimeStop === undefined
          )
            return null;
          if (
            expected !== undefined &&
            (expected.runtime !== entry.runtime ||
              expected.provider.id !== provider.id ||
              expected.provider.providerSessionId !== provider.providerSessionId ||
              expected.provider.providerInstanceId !== provider.providerInstanceId ||
              expected.provider.nativeThreadRef?.nativeId !== provider.nativeThreadRef?.nativeId ||
              expected.provider.runtimeIdentity?.runtimeGeneration !==
                provider.runtimeIdentity?.runtimeGeneration ||
              expected.provider.runtimeIdentity?.evidenceRevision !==
                provider.runtimeIdentity?.evidenceRevision)
          )
            return null;
          const captured = yield* entry.runtime.captureRuntimeStop(provider);
          if (captured === null) return null;
          const equivalent = Schema.toEquivalence(ProviderAdapterBinding);
          const isCurrent = Effect.gen(function* () {
            if (!(yield* captured.isCurrent)) return false;
            const fresh = yield* projectionStore.getThreadRecords(threadId, ["providerThreads"]);
            const current = fresh.providerThreads.find(
              (row) => row.id === fresh.thread.activeProviderThreadId,
            );
            const currentGeneration = current?.runtimeIdentity?.runtimeGeneration;
            const binding =
              current === undefined || currentGeneration === undefined
                ? undefined
                : ProviderAdapter.runtimeBinding(current, currentGeneration);
            const live = (yield* Ref.get(sessions)).get(key);
            return (
              binding !== undefined &&
              equivalent(binding, captured.binding) &&
              current?.runtimeIdentity?.evidenceRevision === captured.evidenceRevision &&
              live?.runtime === entry.runtime &&
              live.attachedThreadIds.has(threadId) &&
              fresh.thread.deletedAt === null &&
              fresh.thread.archivedAt === null
            );
          }).pipe(Effect.catchCause(() => Effect.succeed(false)));
          if (!(yield* isCurrent)) return null;
          const retirement = captured.retirement;
          if (retirement !== undefined && !equivalent(retirement.binding, captured.binding))
            return null;
          let retirementFiber:
            | Fiber.Fiber<
                ProviderAdapter.RuntimeRetirementReceipt,
                ProviderAdapter.ProviderAdapterV2Error
              >
            | undefined;
          const observeRetirement =
            retirement === undefined
              ? undefined
              : retirement.awaitRetired.pipe(
                  Effect.flatMap((receipt) => {
                    if (!equivalent(receipt.binding, captured.binding))
                      return Effect.fail(
                        new ProviderAdapter.ProviderRuntimeBindingError({
                          driver: entry.runtime.driver,
                          detail: "Retirement did not match the captured runtime.",
                        }),
                      );
                    if (
                      receipt.evidence === "thread_closed" &&
                      entry.supportsMultipleProviderThreads
                    )
                      return Effect.gen(function* () {
                        const capturedIdle = yield* threadAttachment.withLock(
                          threadAttachmentKey({
                            providerSessionId: captured.binding.providerSessionId,
                            threadId,
                          }),
                          Ref.modify(sessions, (all) => {
                            const current = all.get(key);
                            if (current?.runtime !== entry.runtime) return [null, all] as const;
                            const retired = retirements.retain({
                              source: entry.runtime,
                              scope: entry.scope,
                              providerSessionId: captured.binding.providerSessionId,
                              threadIds: new Set([threadId]),
                              kind: "transfer",
                              sessionWide: false,
                              bindings: [captured.binding],
                            });
                            const attachedThreadIds = new Set(current.attachedThreadIds);
                            attachedThreadIds.delete(threadId);
                            const loadedProviderThreadKeyByThread = new Map(
                              current.loadedProviderThreadKeyByThread,
                            );
                            loadedProviderThreadKeyByThread.delete(threadId);
                            const idleThreadUnloads = new Map(current.idleThreadUnloads);
                            const idle = idleThreadUnloads.get(threadId)?.fiber ?? null;
                            idleThreadUnloads.delete(threadId);
                            originalThreadLoads.get(entry.runtime)?.delete(threadId);
                            return [
                              { idle, retired },
                              new Map(all).set(key, {
                                ...current,
                                attachedThreadIds,
                                loadedProviderThreadKeyByThread,
                                idleThreadUnloads,
                              }),
                            ] as const;
                          }),
                        );
                        if (capturedIdle !== null) {
                          yield* cancelIdleFiber(capturedIdle.idle);
                          yield* physicalEvidence.invalidate(captured.binding);
                          yield* retirements.complete(capturedIdle.retired);
                        }
                        return receipt;
                      });
                    // Native exit is not subscriber ingestion. The original pump owns
                    // its queued terminal batch and acknowledges its drain before disposal.
                    return Effect.gen(function* () {
                      yield* Deferred.await(entry.eventPumpSettled);
                      yield* releaseEntry({
                        providerSessionId: captured.binding.providerSessionId,
                        onlyIfRuntime: entry.runtime,
                        reason: "manual_shutdown",
                        detachedThreadId: threadId,
                      });
                      yield* physicalEvidence.invalidate(captured.binding);
                      return receipt;
                    }).pipe(
                      Effect.mapError(
                        (cause) =>
                          new ProviderAdapter.ProviderRuntimeBindingError({
                            driver: entry.runtime.driver,
                            detail: "Captured runtime cleanup failed.",
                            cause,
                          }),
                      ),
                    );
                  }),
                );
          return {
            ...captured,
            isCurrent,
            ...(retirement === undefined
              ? {}
              : {
                  retirement: {
                    ...retirement,
                    awaitRetired: Effect.uninterruptibleMask((restore) =>
                      Effect.gen(function* () {
                        retirementFiber ??= yield* restore(observeRetirement!).pipe(
                          Effect.forkIn(layerScope),
                        );
                        return yield* restore(Fiber.join(retirementFiber));
                      }),
                    ),
                  },
                }),
          } satisfies ProviderAdapter.CapturedRuntimeStop;
        },
        Effect.catchCause(() => Effect.succeed(null)),
      );

      return ProviderSessionManagerV2.of({
        shutdown,
        hasRetiredWriter: (input) =>
          Ref.get(sessions).pipe(
            Effect.map((entries) => {
              const entry = entries.get(sessionKey(input.providerSessionId));
              return (
                entry !== undefined && retirements.retiredWriter(entry.runtime, input.threadId)
              );
            }),
          ),
        awaitThreadRetirement: (input) =>
          readDestinationKey(input.instanceId).pipe(
            Effect.flatMap((destinationKey) =>
              Effect.forEach(
                retirements.pendingConversation(input.threadId, destinationKey),
                (record) =>
                  Deferred.await(record.completion).pipe(
                    Effect.timeout(UNLOAD_THREAD_TIMEOUT_MS),
                    Effect.mapError(
                      (cause) =>
                        new RetirementLedger.WriterRetirementError({
                          providerSessionId: record.providerSessionId,
                          cause,
                        }),
                    ),
                  ),
                { concurrency: 1, discard: true },
              ),
            ),
          ),
        captureThreadRetirement: (binding) =>
          Effect.sync(() => recoveryRetirements.capture(binding)),
        captureCurrentThreadRuntimeStop,
        reconcileThreadBindingPublication,
        readCurrentThreadRuntimeAttachment,
        observeThreadActivity,
        isMcpCallerAttached: (input) =>
          Ref.get(sessions).pipe(
            Effect.map((entries) => {
              const entry = entries.get(sessionKey(input.providerSessionId));
              return (
                entry !== undefined &&
                !entry.failureOwnership.failed &&
                entry.runtime.instanceId === input.providerInstanceId &&
                entry.attachedThreadIds.has(input.threadId) &&
                entry.mcpCredentialIdByThread.get(input.threadId) === input.mcpCredentialId
              );
            }),
          ),
        open: (input) =>
          sessionOpen
            .withLock(
              input.providerSessionId,
              Effect.gen(function* () {
                if (input.revalidateStartAdmission === undefined)
                  return yield* new ProviderSessionOpenError({
                    instanceId: input.modelSelection.instanceId,
                    providerSessionId: input.providerSessionId,
                    cause: "Original admission is required before managed session use.",
                  });
                yield* input.revalidateStartAdmission.pipe(
                  Effect.mapError(
                    (cause) =>
                      new ProviderSessionOpenError({
                        instanceId: input.modelSelection.instanceId,
                        providerSessionId: input.providerSessionId,
                        cause,
                      }),
                  ),
                );
                yield* assertRetirementClear({
                  ...input,
                  instanceId: input.modelSelection.instanceId,
                  source: (yield* Ref.get(sessions)).get(sessionKey(input.providerSessionId))
                    ?.runtime,
                });
                const cwd = input.runtimePolicy.cwd;
                if (cwd !== null) {
                  const workspaceIsDirectory = yield* fileSystem.stat(cwd).pipe(
                    Effect.map((stat) => stat.type === "Directory"),
                    Effect.catch((error) => Effect.succeed(error.reason._tag !== "NotFound")),
                  );
                  if (!workspaceIsDirectory) {
                    return yield* new ProviderWorkspaceMissingError({
                      threadId: input.threadId,
                      cwd,
                    });
                  }
                }
                const key = sessionKey(input.providerSessionId);
                const live = (yield* Ref.get(sessions)).get(key);
                if (live !== undefined) yield* releaseForLinkedEnvironments(live);
                const existing = (yield* Ref.get(sessions)).get(key);
                const beforeOpen = Effect.gen(function* () {
                  yield* input.revalidateStartAdmission!;
                  if ((yield* Ref.get(sessions)).get(key)?.runtime !== existing?.runtime)
                    return yield* new ProviderSessionOpenError({
                      instanceId: input.modelSelection.instanceId,
                      providerSessionId: input.providerSessionId,
                      cause: "The captured native session changed before opening.",
                    });
                  yield* assertRetirementClear({
                    ...input,
                    instanceId: input.modelSelection.instanceId,
                    source: existing?.runtime,
                  });
                  yield* NativeProvider.revalidateNativeProviderGuard(input.nativeCreationGuard, {
                    threadId: input.threadId,
                    cwd: input.runtimePolicy.cwd,
                  });
                  // Jones: conversation bindings and lifecycle-owned checkouts recheck
                  // the thread's exact checkout (or conversation) assignment around the
                  // native workspace transition; ordinary checkouts select out.
                  const checkOpen = Effect.gen(function* () {
                    const thread = yield* projectionStore.getThreadShell(input.threadId);
                    if (
                      thread === null ||
                      thread.deletedAt !== null ||
                      (thread.worktreePath !== input.runtimePolicy.cwd &&
                        !(
                          thread.worktreePath === null &&
                          Option.isSome(conversationPolicy) &&
                          (yield* conversationPolicy.value.acceptsCwd(
                            input.threadId,
                            input.runtimePolicy.cwd,
                          ))
                        ))
                    )
                      return yield* new ProviderSessionOpenError({
                        instanceId: input.modelSelection.instanceId,
                        providerSessionId: input.providerSessionId,
                        cause: "The native checkout assignment changed before opening.",
                      });
                    yield* NativeProvider.revalidateNativeProviderGuard(input.nativeCreationGuard, {
                      threadId: input.threadId,
                      cwd: input.runtimePolicy.cwd,
                    });
                  });
                  yield* beforeSessionUse(
                    input.threadId,
                    input.providerSessionId,
                    checkOpen,
                    input.runtimePolicy.cwd,
                  );
                });
                yield* beforeOpen.pipe(
                  Effect.mapError(
                    (cause) =>
                      new ProviderSessionOpenError({
                        instanceId: input.modelSelection.instanceId,
                        providerSessionId: input.providerSessionId,
                        cause,
                      }),
                  ),
                );
                if (existing !== undefined) {
                  if (existing.runtime.instanceId !== input.modelSelection.instanceId)
                    return yield* new ProviderSessionOpenError({
                      instanceId: input.modelSelection.instanceId,
                      providerSessionId: input.providerSessionId,
                      cause: "The requested session id belongs to another runtime instance.",
                    });
                  yield* input.revalidateStartAdmission.pipe(
                    Effect.mapError(
                      (cause) =>
                        new ProviderSessionOpenError({
                          instanceId: input.modelSelection.instanceId,
                          providerSessionId: input.providerSessionId,
                          cause,
                        }),
                    ),
                  );
                  const attached = existing.attachedThreadIds.has(input.threadId);
                  if (!attached && !existing.supportsMultipleProviderThreads) {
                    return yield* new ProviderSessionOpenError({
                      instanceId: input.modelSelection.instanceId,
                      providerSessionId: input.providerSessionId,
                      cause: `Provider ${existing.runtime.driver} does not support attaching multiple app threads to one session.`,
                    });
                  }
                  yield* runtimeUseGuard
                    .withRuntimeOperation(
                      existing.runtime,
                      runtimeUseGuard.withAttachment(
                        existing.runtime,
                        input.threadId,
                        input.runtimePolicy.cwd,
                        Effect.gen(function* () {
                          if (!attached) yield* agentScope.clear(input.threadId);
                          yield* ensureThreadAttached({
                            providerSessionId: input.providerSessionId,
                            threadId: input.threadId,
                            providerInstanceId: existing.runtime.instanceId,
                            onlyIfRuntime: existing.runtime,
                          });
                          yield* touchActivity(input.providerSessionId);
                        }),
                      ),
                    )
                    .pipe(
                      Effect.catchTags({
                        RuntimeUseError: (cause) =>
                          Effect.fail(
                            new ProviderSessionOpenError({
                              instanceId: input.modelSelection.instanceId,
                              providerSessionId: input.providerSessionId,
                              cause,
                            }),
                          ),
                      }),
                    );
                  return existing.exposedRuntime;
                }

                const adapter = yield* registry.get(input.modelSelection.instanceId).pipe(
                  Effect.mapError(
                    (cause) =>
                      new ProviderSessionOpenError({
                        instanceId: input.modelSelection.instanceId,
                        providerSessionId: input.providerSessionId,
                        cause,
                      }),
                  ),
                );
                const openedWithLinkedEnvironments = yield* readLinkedEnvironments;
                const prepared = yield* prepareMcpSession(
                  input.threadId,
                  input.modelSelection.instanceId,
                );
                const mcpCredentialId = prepared.mcpCredentialId;
                let reservationDropped = mcpCredentialId === undefined;
                const dropReservation = Effect.sync(() => {
                  if (!reservationDropped && mcpCredentialId !== undefined) {
                    reservationDropped = true;
                    dropMcpCredentialReservation(input.threadId, mcpCredentialId);
                  }
                });
                const sessionScope = yield* Scope.fork(sessionScopes);
                const runtimeOwner: {
                  runtime: ProviderAdapterV2SessionRuntime | undefined;
                  active: boolean;
                } = {
                  runtime: undefined,
                  active: true,
                };
                lifecycleOwners.set(sessionScope, runtimeOwner);
                yield* Effect.addFinalizer(() =>
                  Effect.sync(() => {
                    runtimeOwner.active = false;
                  }),
                ).pipe(Effect.provideService(Scope.Scope, sessionScope));
                const runtime = yield* DutyRelease.withRegisteredOpen(
                  workspaceDuty.hold(cwd, "session", input.providerSessionId).pipe(
                    Effect.mapError(
                      (cause) =>
                        new ProviderSessionOpenError({
                          instanceId: input.modelSelection.instanceId,
                          providerSessionId: input.providerSessionId,
                          cause,
                        }),
                    ),
                  ),
                  (releaseWorkspaceDuty) => {
                    workspaceDuties.set(sessionScope, releaseWorkspaceDuty);
                    dutyRelease.register(
                      sessionScope,
                      releaseWorkspaceDuty.pipe(
                        Effect.tap(() => Effect.sync(() => workspaceDuties.delete(sessionScope))),
                      ),
                    );
                    return installationGate
                      .execute(
                        input.modelSelection.instanceId,
                        beforeOpen.pipe(
                          Effect.andThen(agentScope.clear(input.threadId)),
                          Effect.andThen(
                            adapter.openSession({
                              threadId: input.threadId,
                              providerSessionId: input.providerSessionId,
                              modelSelection: input.modelSelection,
                              runtimePolicy: input.runtimePolicy,
                              promptSuggestions: promptSuggestionsEnabled,
                              runtimeLifecycle: makeRuntimeLifecycle(
                                input.providerSessionId,
                                input.modelSelection.instanceId,
                                adapter.driver,
                                runtimeOwner,
                                releaseWorkspaceDuty,
                              ),
                              ...(input.nativeCreationGuard === undefined
                                ? {}
                                : {
                                    nativeCreationGuard: input.nativeCreationGuard,
                                  }),
                              ...(input.resumeFromSession === undefined
                                ? {}
                                : { resumeFromSession: input.resumeFromSession }),
                              ...(input.initialNativeThreadId === undefined
                                ? {}
                                : { initialNativeThreadId: input.initialNativeThreadId }),
                              ...(input.initialProviderItemIdentityVersion === undefined
                                ? {}
                                : {
                                    initialProviderItemIdentityVersion:
                                      input.initialProviderItemIdentityVersion,
                                  }),
                            }),
                          ),
                        ),
                      )
                      .pipe(
                        (effect) =>
                          runtimeUseGuard.guardOpen(
                            {
                              threadId: input.threadId,
                              providerSessionId: input.providerSessionId,
                              instanceId: input.modelSelection.instanceId,
                              cwd: input.runtimePolicy.cwd,
                            },
                            adapter.driver,
                            effect,
                          ),
                        Effect.provideService(
                          StartAdmission.CurrentRevalidation,
                          input.revalidateStartAdmission,
                        ),
                        Effect.provideService(
                          OpenDutyClaim,
                          workspaceDuty.openClaim(releaseWorkspaceDuty),
                        ),
                        Effect.provideService(Scope.Scope, sessionScope),
                        Effect.tap((opened) =>
                          Effect.gen(function* () {
                            yield* input.revalidateStartAdmission!;
                            if (
                              opened.instanceId !== input.modelSelection.instanceId ||
                              opened.providerSessionId !== input.providerSessionId ||
                              opened.driver !== adapter.driver
                            )
                              return yield* new ProviderRuntimeBindingError({
                                driver: adapter.driver,
                                detail:
                                  "The opened runtime does not match the captured provider instance and session.",
                              });
                          }),
                        ),
                        Effect.mapError(
                          (cause) =>
                            new ProviderSessionOpenError({
                              instanceId: input.modelSelection.instanceId,
                              providerSessionId: input.providerSessionId,
                              cause,
                            }),
                        ),
                        withMetrics({
                          counter: providerSessionsTotal,
                          attributes: { provider: adapter.driver, operation: "open" },
                        }),
                      );
                  },
                  () =>
                    Effect.gen(function* () {
                      retirements.retain({
                        source: sessionScope,
                        scope: sessionScope,
                        providerSessionId: input.providerSessionId,
                        threadIds: new Set([input.threadId]),
                        kind: "opening",
                        sessionWide: true,
                        bindings: [],
                      });
                      yield* dropReservation.pipe(
                        Effect.andThen(
                          prepared.issued
                            ? clearMcpSession(input.threadId, mcpCredentialId)
                            : Effect.void,
                        ),
                        Effect.ensuring(
                          closeScopeWithin(sessionScope, {
                            providerSessionId: input.providerSessionId,
                            reason: "open_failed",
                          }),
                        ),
                      );
                    }),
                );
                const eventSubscribers = yield* Ref.make<
                  ReadonlyMap<number, ProviderSessionEventSubscriber>
                >(new Map());
                runtimeOwner.runtime = runtime;
                const exposedRuntime = decorateRuntime(runtime, eventSubscribers);
                const entry: LiveSessionEntry = {
                  eventPumpSettled: yield* Deferred.make<void>(),
                  failureOwnership: { permit: yield* Semaphore.make(1), failed: false },
                  attachedThreadIds: new Set([input.threadId]),
                  completedThreadTransfers: new Map(),
                  loadedProviderThreadKeyByThread: new Map(),
                  openedWithLinkedEnvironments,
                  linkedEnvironmentsByThread: new Map(),
                  mcpCredentialIdByThread:
                    mcpCredentialId === undefined
                      ? new Map()
                      : new Map([[input.threadId, mcpCredentialId]]),
                  supportsMultipleProviderThreads:
                    runtime.providerSession.capabilities.sessions
                      .supportsMultipleProviderThreadsPerSession,
                  runtime,
                  exposedRuntime,
                  eventSubscribers,
                  requestEventPermit: yield* Semaphore.make(1),
                  scope: sessionScope,
                  idleGeneration: 0,
                  busyTurns: new Set(),
                  idleFiber: null,
                  pinnedSinceMs: null,
                  idleThreadUnloads: new Map(),
                };
                yield* Ref.update(sessions, (current) => {
                  const updated = new Map(current);
                  updated.set(key, entry);
                  return updated;
                });
                yield* dropReservation;
                yield* withActivityError(
                  input.providerSessionId,
                  writeProviderSessionEvents({
                    runtime,
                    threadIds: [input.threadId],
                    type: "provider-session.attached",
                    payload: runtime.providerSession,
                  }),
                ).pipe(
                  // Release on interruption too; no event pump or idle timer exists to clean up yet.
                  Effect.onError((cause) =>
                    releaseEntry(
                      Cause.hasInterruptsOnly(cause)
                        ? {
                            providerSessionId: input.providerSessionId,
                            reason: "manual_shutdown",
                            detail: "The provider session start was interrupted.",
                          }
                        : {
                            providerSessionId: input.providerSessionId,
                            reason: "runtime_error",
                            detail: "Failed to persist the provider-session attachment.",
                          },
                    ).pipe(logReleaseFailure(input.providerSessionId)),
                  ),
                );
                yield* startEventPump(entry);
                yield* scheduleIdleRelease(input.providerSessionId);
                return exposedRuntime;
              }).pipe((effect) =>
                withSessionInvocation(
                  input.threadId,
                  input.providerSessionId,
                  effect,
                  (cause) =>
                    new ProviderSessionActivityError({
                      providerSessionId: input.providerSessionId,
                      cause,
                    }),
                ),
              ),
            )
            .pipe((effect) =>
              runtimeUseGuard
                .withWorkspace(
                  {
                    threadId: input.threadId,
                    providerSessionId: input.providerSessionId,
                    instanceId: input.modelSelection.instanceId,
                    cwd: input.runtimePolicy.cwd,
                  },
                  effect,
                )
                .pipe(
                  Effect.mapError((cause) =>
                    isRuntimeUseError(cause)
                      ? new ProviderSessionOpenError({
                          instanceId: input.modelSelection.instanceId,
                          providerSessionId: input.providerSessionId,
                          cause,
                        })
                      : cause,
                  ),
                ),
            ),
        isLive: (providerSessionId) =>
          Ref.get(sessions).pipe(
            Effect.map((current) => current.has(sessionKey(providerSessionId))),
          ),
        get: Effect.fn("ProviderSessionManager.get")(
          function* (providerSessionId, options) {
            const entry = (yield* Ref.get(sessions)).get(sessionKey(providerSessionId));
            if (entry === undefined) {
              return Option.none<ProviderAdapter.ProviderAdapterV2SessionRuntime>();
            }
            if (options?.controlScope !== undefined) {
              yield* Effect.acquireRelease(entry.requestEventPermit.take(1), () =>
                entry.requestEventPermit.release(1),
              ).pipe(Scope.provide(options.controlScope));
              const current = (yield* Ref.get(sessions)).get(sessionKey(providerSessionId));
              if (current?.runtime !== entry.runtime || entry.failureOwnership.failed)
                return Option.none<ProviderAdapter.ProviderAdapterV2SessionRuntime>();
            }
            if (options?.touchActivity !== false) yield* touchActivity(providerSessionId);
            return Option.some(entry.exposedRuntime);
          },
          (effect, providerSessionId) =>
            effect.pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderSessionLookupError({
                    providerSessionId,
                    cause,
                  }),
              ),
            ),
        ),
        settleHandoff: Effect.fn("ProviderSessionManager.settleHandoff")(function* <E, R>(
          input: HandoffSettlementInput<E, R>,
        ) {
          const entry = (yield* Ref.get(sessions)).get(sessionKey(input.runtime.providerSessionId));
          if (entry?.exposedRuntime !== input.runtime) return;
          yield* entry.failureOwnership.permit.withPermits(1)(
            Effect.gen(function* () {
              if (entry.failureOwnership.failed) return;
              const context = yield* projectionStore.getRunningTurnContext(input.threadId).pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderSessionLookupError({
                      providerSessionId: input.runtime.providerSessionId,
                      cause,
                    }),
                ),
              );
              if (
                context.providerThread?.id !== input.providerThreadId ||
                context.providerThread.providerSessionId !== input.runtime.providerSessionId ||
                context.providerTurn?.id !== input.providerTurnId ||
                context.providerTurn.status !== "running" ||
                context.providerTurn.runAttemptId === null ||
                context.providerTurn.providerThreadId !== input.providerThreadId ||
                context.providerTurn.runAttemptId !== context.run?.activeAttemptId ||
                context.run?.status !== "running"
              )
                return;
              // Outcome admission and durable settlement share this runtime/attempt gate.
              yield* input.settle;
            }),
          );
        }),
        close: (providerSessionId) =>
          releaseEntry({ providerSessionId, reason: "manual_shutdown" }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderSessionCloseError({
                  providerSessionId,
                  cause,
                }),
            ),
          ),
        closeInstance: Effect.fn("ProviderSessionManager.closeInstance")(
          function* (instanceId) {
            const active = [...(yield* Ref.get(sessions)).values()].filter(
              (entry) => entry.runtime.instanceId === instanceId,
            );
            const outcomes = yield* Effect.forEach(
              active,
              (entry) =>
                releaseEntry({
                  providerSessionId: entry.runtime.providerSessionId,
                  onlyIfRuntime: entry.runtime,
                  reason: "manual_shutdown",
                  detail: `Provider instance ${instanceId} logged out.`,
                }).pipe(Effect.exit),
              { concurrency: "unbounded" },
            );
            const failure = outcomes.find(Exit.isFailure);
            if (failure !== undefined && Exit.isFailure(failure)) {
              return yield* Effect.failCause(failure.cause);
            }
          },
          (effect, instanceId) =>
            effect.pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderSessionCloseError({
                    providerSessionId: ProviderSessionId.make(
                      `provider-session:provider-instance:${instanceId}`,
                    ),
                    cause,
                  }),
              ),
            ),
        ),
        release: releaseEntry,
        releaseUnlessBusy: Effect.fn("ProviderSessionManager.releaseUnlessBusy")(function* (input) {
          const key = sessionKey(input.providerSessionId);
          const before = (yield* Ref.get(sessions)).get(key);
          yield* releaseEntry({
            providerSessionId: input.providerSessionId,
            reason: input.reason,
            ...(input.detail === undefined ? {} : { detail: input.detail }),
            keepIfBusyOtherThan: {
              providerThreadIds: input.ownProviderThreadIds,
              throughRunOrdinal: input.throughRunOrdinal,
            },
            ...(input.keepIf === undefined ? {} : { keepIf: input.keepIf }),
          });
          // Scope, not presence: open can install a replacement under the same id meanwhile, and
          // a kept entry is rewritten with the same scope.
          const released =
            before !== undefined && (yield* Ref.get(sessions)).get(key)?.scope !== before.scope;
          // The stopped turn's markBusy cancelled the idle timer and its markIdle may never come.
          if (!released) yield* scheduleIdleReleaseInternal(input.providerSessionId);
          return released;
        }),
        otherWorkingThreads: Effect.fn("ProviderSessionManager.otherWorkingThreads")(
          function* (input) {
            const entry = (yield* Ref.get(sessions)).get(sessionKey(input.providerSessionId));
            if (entry === undefined || !entry.supportsMultipleProviderThreads) return [];
            return yield* Effect.filter(
              [...entry.attachedThreadIds].filter((threadId) => threadId !== input.threadId),
              (threadId) =>
                // Runs, not the shell's activeRunId: the shell omits silent runs such as idle
                // compaction. Background work outlives its run and still needs the process.
                Effect.zipWith(
                  projectionStore.getThreadRecords(threadId, ["runs"]),
                  projectionStore.getThreadShell(threadId),
                  ({ runs }, shell) =>
                    runs.some((run) => WORKING_RUN_STATUSES.has(run.status)) ||
                    (shell?.pendingBackgroundTasks?.length ?? 0) > 0,
                ).pipe(
                  // An unreadable thread may still be working; never kill it unseen.
                  Effect.orElseSucceed(() => true),
                ),
            );
          },
        ),
        detach: Effect.fn("ProviderSessionManager.detach")(
          function* (input) {
            const key = sessionKey(input.providerSessionId);
            const currentEntry = (yield* Ref.get(sessions)).get(key);
            if (input.requireWriterRetirement === true) {
              yield* detachForTransfer(input);
              return;
            }
            if (currentEntry !== undefined) {
              const projection = yield* projectionStore
                .getThreadRecords(input.threadId, ["runs", "providerThreads"])
                .pipe(Effect.option);
              const sourceMetadata = yield* readMetadata(currentEntry.runtime.instanceId);
              const destinationMetadata = Option.isNone(projection)
                ? undefined
                : yield* readMetadata(AccountTransfer.accountTransferDestination(projection.value));
              const decision = AccountTransfer.decideAccountTransfer({
                optedIn: input.retireOnAccountTransfer === true,
                revokeMcpCredential: input.revokeMcpCredential === true,
                projection: Option.getOrUndefined(projection),
                sourceInstanceId: currentEntry.runtime.instanceId,
                sourceDriver: currentEntry.runtime.driver,
                supportsRetirement: currentEntry.runtime.retireThreadForTransfer !== undefined,
                attached: currentEntry.attachedThreadIds.has(input.threadId),
                loadedKey: currentEntry.loadedProviderThreadKeyByThread.get(input.threadId),
                complete:
                  Option.isSome(projection) &&
                  selectTransferBindings(
                    input,
                    currentEntry.runtime,
                    projection.value.providerThreads,
                  ).complete,
                sourceMetadata,
                destinationMetadata,
              });
              if (decision.type === "prove") {
                yield* detachForTransfer(input, {
                  expectedLoadedKey: decision.loadedKey,
                  conversationKey: decision.conversationKey,
                  awaitCompletion: false,
                });
                return;
              }
            }
            let detachedProviderThreads: ReadonlyArray<OrchestrationV2ProviderThread> = [];
            if (currentEntry?.supportsMultipleProviderThreads === true) {
              const projection = yield* Effect.option(
                projectionStore.getThreadRecords(input.threadId, [
                  "providerThreads",
                  "providerTurns",
                ]),
              );
              if (Option.isSome(projection)) {
                const sessionProviderThreads = projection.value.providerThreads.filter(
                  (thread) => thread.providerSessionId === input.providerSessionId,
                );
                // Interrupt only this owner; unload orphaned native children later
                // under each child's attachment lock and live-attachment check.
                const providerThreads = new Map(
                  sessionProviderThreads
                    .filter(
                      (thread) =>
                        thread.appThreadId === null || thread.appThreadId === input.threadId,
                    )
                    .map((thread) => [thread.id, thread] as const),
                );
                detachedProviderThreads = sessionProviderThreads;
                const activeTurns = projection.value.providerTurns.filter(
                  (turn) => turn.status === "running" && providerThreads.has(turn.providerThreadId),
                );
                yield* Effect.forEach(
                  activeTurns,
                  (turn) =>
                    currentEntry.exposedRuntime
                      .interruptTurn({
                        providerThread: providerThreads.get(turn.providerThreadId)!,
                        providerTurnId: turn.id,
                      })
                      .pipe(
                        Effect.catchCause((cause) =>
                          Effect.logWarning(
                            "orchestration-v2.driver-session.detach-interrupt-failed",
                            {
                              providerSessionId: input.providerSessionId,
                              threadId: input.threadId,
                              providerTurnId: turn.id,
                              cause,
                            },
                          ),
                        ),
                      ),
                  { concurrency: 1, discard: true },
                );
              }
            }
            const detachResult = yield* Ref.modify(sessions, (current) => {
              const entry = current.get(key);
              if (entry === undefined || !entry.attachedThreadIds.has(input.threadId)) {
                return [
                  Option.none<{
                    readonly entry: LiveSessionEntry;
                    readonly idleUnloadFiber: Fiber.Fiber<void, never> | null;
                  }>(),
                  current,
                ] as const;
              }
              const attachedThreadIds = new Set(entry.attachedThreadIds);
              attachedThreadIds.delete(input.threadId);
              const loadedProviderThreadKeyByThread = new Map(
                entry.loadedProviderThreadKeyByThread,
              );
              loadedProviderThreadKeyByThread.delete(input.threadId);
              const linkedEnvironmentsByThread = new Map(entry.linkedEnvironmentsByThread);
              linkedEnvironmentsByThread.delete(input.threadId);
              const mcpCredentialIdByThread =
                input.revokeMcpCredential === true
                  ? (() => {
                      const pruned = new Map(entry.mcpCredentialIdByThread);
                      pruned.delete(input.threadId);
                      return pruned;
                    })()
                  : entry.mcpCredentialIdByThread;
              const idleThreadUnloads = new Map(entry.idleThreadUnloads);
              idleThreadUnloads.delete(input.threadId);
              const updatedEntry = {
                ...entry,
                attachedThreadIds,
                loadedProviderThreadKeyByThread,
                linkedEnvironmentsByThread,
                mcpCredentialIdByThread,
                idleThreadUnloads,
              };
              const updated = new Map(current);
              updated.set(key, updatedEntry);
              return [
                Option.some({
                  entry: updatedEntry,
                  idleUnloadFiber: entry.idleThreadUnloads.get(input.threadId)?.fiber ?? null,
                }),
                updated,
              ] as const;
            });
            if (Option.isSome(detachResult)) {
              yield* cancelIdleFiber(detachResult.value.idleUnloadFiber);
            }
            const detached = Option.map(detachResult, (result) => result.entry);
            // Plain detaches preserve credentials held by live shared providers across workspace handoffs.
            if (input.revokeMcpCredential === true) {
              yield* clearMcpSession(input.threadId);
            }
            if (Option.isNone(detached)) {
              return;
            }
            if (
              detached.value.attachedThreadIds.size === 0 &&
              !detached.value.supportsMultipleProviderThreads
            ) {
              yield* releaseEntry({
                providerSessionId: input.providerSessionId,
                reason: "manual_shutdown",
                ...(input.detail === undefined ? {} : { detail: input.detail }),
                detachedThreadId: input.threadId,
              });
              return;
            }
            const unloadThread = detached.value.exposedRuntime.unloadThread;
            if (detached.value.supportsMultipleProviderThreads && unloadThread !== undefined) {
              // The projection includes native children owned by other app threads;
              // lock each owner so an attached child stays loaded and later attaches wait.
              for (const providerThread of detachedProviderThreads) {
                if (providerThread.nativeThreadRef === null) continue;
                const ownerThreadId = providerThread.appThreadId ?? input.threadId;
                yield* threadAttachment.withLock(
                  threadAttachmentKey({
                    providerSessionId: input.providerSessionId,
                    threadId: ownerThreadId,
                  }),
                  Effect.gen(function* () {
                    const entry = (yield* Ref.get(sessions)).get(key);
                    if (
                      entry?.runtime !== detached.value.runtime ||
                      entry.attachedThreadIds.has(ownerThreadId)
                    ) {
                      return;
                    }
                    yield* unloadThread({ providerThread }).pipe(
                      Effect.timeout(UNLOAD_THREAD_TIMEOUT_MS),
                      Effect.catchCause((cause) =>
                        Effect.logWarning("orchestration-v2.driver-session.detach-unload-failed", {
                          providerSessionId: input.providerSessionId,
                          threadId: ownerThreadId,
                          providerThreadId: providerThread.id,
                          cause,
                        }),
                      ),
                    );
                  }),
                );
              }
            }
            yield* scheduleIdleRelease(input.providerSessionId);
          },
          (effect, input) =>
            effect.pipe(
              Effect.catchCause((cause) =>
                Effect.fail(
                  new ProviderSessionReleaseError({
                    providerSessionId: input.providerSessionId,
                    reason: "manual_shutdown",
                    cause,
                  }),
                ),
              ),
            ),
        ),
        // Stored suggestions can outlive a deleted thread, so check the thread before serving one.
        subscribePromptSuggestion: (threadId) =>
          SubscriptionRef.changes(promptSuggestionStore).pipe(
            Stream.map((suggestions) => suggestions.get(threadId) ?? null),
            Stream.changes,
            Stream.mapEffect((suggestion) =>
              suggestion === null
                ? Effect.succeed(null)
                : projectionStore.getThread(threadId).pipe(
                    Effect.map((thread) => (thread.deletedAt === null ? suggestion : null)),
                    Effect.orElseSucceed(() => null),
                  ),
            ),
            Stream.changes,
          ),
      } satisfies ProviderSessionManagerV2Shape);
    }),
  );

export const layer = layerWithOptions();
