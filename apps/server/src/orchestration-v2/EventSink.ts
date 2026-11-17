import {
  type RecordedLifecycleEvent as OrchestrationV2DomainEvent,
  type RecordedStoredLifecycleEvent as OrchestrationV2StoredEvent,
} from "./RecordedTypes.ts";
import type { OrchestrationV2PrivateEvent } from "@t3tools/contracts";
import {
  LegacyStorageError,
  type LegacyPreflightReceipt,
} from "../jones/legacyBootstrap/StorageSchema.ts";
import * as LegacyStorage from "../jones/legacyBootstrap/Storage.ts";
import * as LegacyCleanup from "../jones/legacyBootstrap/TerminalCleanupWorker.ts";
import * as LegacyTransaction from "../jones/legacyBootstrap/Transaction.ts";
import * as LegacyDecision from "../jones/legacyBootstrap/Decision.ts";
import {
  canonicalLegacyPayload,
  legacyBootstrapBirth,
  sameLegacyBootstrapPolicy,
} from "./LegacyBootstrap.ts";
import { makeDispatchGuard, DispatchGuardRejected } from "./DispatchGuard.ts";
const isDispatchGuardRejected = Schema.is(Schema.suspend(() => DispatchGuardRejected));
import * as IdleWorktreeAdmission from "../jones/orchestration/IdleWorktreeAdmission.ts";
import * as ImportedChoice from "../jones/importedHistory/ImportedHistoryChoice.ts";
import * as ImportedHistoryCommit from "../jones/importedHistory/ImportedHistoryCommit.ts";
import { captureNativeProviderSettlementProof } from "../jones/workstreams/nativeProvider/settlementProof.ts";
import type { TaskContinuationCommitContext } from "../jones/taskContinuation/commitContext.ts";
import type { ProviderConversationReselectionCommitContext } from "../jones/runtime/ProviderConversationReselection.ts";
import {
  makeDeletionAdmission,
  type DeletionAdmissionInput,
  type DeletionPolicyRead,
  type DeletionLiveRead,
} from "../jones/cleanup/DeletionAdmission.ts";
import type {
  DeletionWorktreeRemovalTargetV1,
  DeletionWorktreeRemovalStartV1,
} from "../jones/cleanup/DeletionWorktreeRemovalTypes.ts";
import type { DeletionWorktreeRemovalObservationV1 } from "../jones/cleanup/DeletionWorktreeRemoval.ts";
import * as NodePath from "@effect/platform-node/NodePath";
import * as CheckoutIntent from "../jones/ordinaryCheckout/CheckoutIntent.ts";
import * as CheckoutCommit from "../jones/ordinaryCheckout/CheckoutCommit.ts";
import * as SharedAdmission from "../jones/cleanup/sharedAdmission/Protocol.ts";
import { CheckoutError } from "../jones/ordinaryCheckout/CheckoutProtocol.ts";
import {
  decodedCommand,
  planDigests,
} from "../jones/ordinaryCheckout/CheckoutPreparationSchema.ts";
import { makeOwnerSelectionStore } from "../jones/threadConfiguration/ownerSelectionStore.ts";
import type { ThreadSelfConfigurationCommitContext } from "../jones/threadConfiguration/ThreadSelfConfiguration.ts";
import * as CodexFailoverStore from "../jones/codexFailover/CodexFailoverStore.ts";
import type { ModelSelection, MessageId } from "@t3tools/contracts";
import type { HoldReservation, RefusalReason } from "../jones/startRefusal/model.ts";
import {
  bindRecreatedCheckout,
  type ExpectedCheckout,
} from "../jones/workspaceRecreation/BindingCommit.ts";
import { makeJonesTablePresence } from "../jones/persistence/JonesTablePresence.ts";
import { makeDependencyStore } from "../jones/threadResultDependencies/DependencyStore.ts";
import * as DependencyChanges from "../jones/threadResultDependencies/DependencyChanges.ts";
import { ChangedThreads } from "../jones/threadResultDependencies/ChangeTracking.ts";
import { acceptEvents as acceptDependencyEvents } from "../jones/threadResultDependencies/TransactionHooks.ts";
import type {
  OpenRequest,
  WakeCommit,
} from "../jones/threadResultDependencies/DependencyCommit.ts";
import type { ThreadTitleCommitContext } from "../jones/threadTitle/commitContext.ts";
import type { CodexFailoverCommitContext } from "../jones/codexFailover/CodexFailover.ts";
import { makeSettlementRestoreStore } from "../jones/settlementRestore/store.ts";
import {
  makeQueueHoldStore,
  acceptQueueHoldEvents,
  type QueueHoldOrigin,
  type QueueHoldIntent,
} from "../jones/orchestration/QueueHoldStore.ts";
import * as RuntimeStop from "../jones/runtime/RuntimeStopSqlite.ts";
import * as StartRefusal from "../jones/startRefusal/Store.ts";
import * as RemoteSendReceipts from "../jones/orchestration/RemoteSendReceiptStore.ts";
import {
  recordSelfSettlementProjectRemaining,
  type SelfSettlementProjectRemaining,
} from "../jones/settlementOrigins/projectRemaining.ts";
import * as RuntimeEvidenceGuard from "../jones/orchestration/RuntimeEvidenceGuard.ts";
import * as HistoricalTerminalHistory from "../jones/orchestration/HistoricalTerminalHistory.ts";
import * as SupersededTerminalTrace from "../jones/orchestration/SupersededTerminalTrace.ts";
import {
  revalidateRuntimeUseAttachmentBeforePublication,
  validateRuntimeUseAttachmentPublication,
} from "../jones/cleanup/RuntimeUseAttachment.ts";
import * as RetirementEvents from "../jones/workspaceRetirement/EventIntegration.ts";
import * as WorkspaceLifecycle from "../jones/workspaceLifecycle/NativeBoundary.ts";
import * as WorkspaceProvenance from "../jones/workspaceLifecycle/Provenance.ts";
import * as WorkspaceProvenanceStore from "../jones/workspaceLifecycle/TransitionStore.ts";
import * as UpdateAdmissionFence from "../jones/updates/UpdateAdmissionFence.ts";
import * as NativeCreationRepository from "../jones/nativeCreation/NativeCreationRepository.ts";
import { identityForRequest } from "@t3tools/provider-core/server/ProviderAdapter";
import {
  CommandId,
  type OrchestrationV2Run,
  type OrchestrationV2ProviderThread,
  OrchestrationV2ProviderRef,
  type ProviderRuntimeEvidenceCapture,
  type RequestedRuntimeIdentity,
  type OrchestrationV2Subagent,
  type OrchestrationV2TurnItem,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  RuntimeRequestId,
  NodeId,
  type ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import * as ProviderCommitBatch from "./ProviderCommitBatch.ts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Semaphore from "effect/Semaphore";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlError from "effect/sql/SqlError";

import { replayAndBufferProjectedLiveEvents } from "./LiveStreamBudget.ts";
import type { UnsequencedProjectEvent } from "../persistence/OrchestrationEventStore.ts";
import { isPublicStoredOrchestrationEvent, projectStoredEventForWire } from "./WireProjection.ts";
import { makeRunOutcomeRecorder, type RunOutcome } from "./RunMetrics.ts";

import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as TurnItemPositionStore from "./TurnItemPositionStore.ts";

const isSharedAdmissionError = Schema.is(SharedAdmission.AdmissionError);

export class EventSinkWriteError extends Schema.TaggedError<EventSinkWriteError>()(
  "EventSinkWriteError",
  {
    eventCount: Schema.Number,
    commandId: Schema.optional(CommandId),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to write ${this.eventCount} orchestration V2 event(s).`;
  }
}

export class EventSinkStreamError extends Schema.TaggedError<EventSinkStreamError>()(
  "EventSinkStreamError",
  {
    threadId: Schema.optional(ThreadId),
    afterSequence: Schema.optional(Schema.Number),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.threadId === undefined
      ? "Failed to stream orchestration V2 events."
      : `Failed to stream orchestration V2 events for thread ${this.threadId}.`;
  }
}

export const EventSinkV2Error = Schema.Union([EventSinkWriteError, EventSinkStreamError]);
export type EventSinkV2Error = typeof EventSinkV2Error.Type;

interface ProviderBatchPublication {
  readonly publish: Array<Effect.Effect<void>>;
  eventCount: number;
}
const CurrentProviderBatch = Context.Reference<ProviderBatchPublication | undefined>(
  "t3/orchestration-v2/EventSink/CurrentProviderBatch",
  { defaultValue: () => undefined },
);
interface RuntimeWriteContext {
  readonly skipUnchangedProviderNodes?: boolean;
  readonly historicalTerminal?: HistoricalTerminalHistory.HistoricalTerminalInput;
  readonly runtimeIdentityRequest?: RequestedRuntimeIdentity;
  readonly runtimeIdentityPreviousRequest?: RequestedRuntimeIdentity;
  readonly runtimeEvidence?: ProviderRuntimeEvidenceCapture;
  readonly runtimeIdentityObservation?: RequestedRuntimeIdentity;
  readonly runtimeIdentityBoundary?: {
    readonly expectedGeneration: string | null;
    readonly expectedActiveProviderThreadId?: ProviderThreadId;
    readonly expectedProviderThreadIdentity?: Pick<
      OrchestrationV2ProviderThread,
      | "id"
      | "appThreadId"
      | "driver"
      | "providerInstanceId"
      | "providerSessionId"
      | "nativeThreadRef"
    >;
  };
  readonly settleRunOnLostRuntimeEvidence?: boolean;
}

export interface EventSinkV2Shape {
  readonly idleWorktreeAdmission?: IdleWorktreeAdmission.Store;
  readonly readDeletionWorktreeRemovalStartForTarget?: (
    threadId: ThreadId,
    target: DeletionWorktreeRemovalTargetV1,
  ) => Effect.Effect<
    { readonly start: DeletionWorktreeRemovalStartV1; readonly ordinal: number } | null,
    EventSinkV2Error
  >;
  readonly startDeletionWorktreeRemoval?: (input: DeletionAdmissionInput) => Effect.Effect<
    {
      readonly status: "start_now" | "observe_only";
      readonly start: DeletionWorktreeRemovalStartV1;
      readonly ordinal: number;
    },
    EventSinkV2Error
  >;
  readonly readDeletionWorktreeRemovalStart?: (
    effectId: string,
  ) => Effect.Effect<
    { readonly start: DeletionWorktreeRemovalStartV1; readonly ordinal: number } | null,
    EventSinkV2Error
  >;
  readonly revalidateDeletionWorktreeRemovalStart?: (
    start: DeletionWorktreeRemovalStartV1,
    ordinal: number,
    currentRules?: DeletionPolicyRead,
    currentLive?: DeletionLiveRead,
  ) => Effect.Effect<void, EventSinkV2Error>;
  readonly checkRetainedDeletionWorktreeRemovalStart?: (
    start: DeletionWorktreeRemovalStartV1,
    ordinal: number,
    currentRules?: DeletionPolicyRead,
    currentLive?: DeletionLiveRead,
  ) => Effect.Effect<void, EventSinkV2Error>;
  readonly qualifyDeletionWorktreeRemovalObservation?: (
    observation: DeletionWorktreeRemovalObservationV1,
  ) => Effect.Effect<void, EventSinkV2Error>;
  readonly reviewImportedHistory?: (
    input: Parameters<typeof ImportedChoice.reviewImportedHistory>[0],
  ) => Effect.Effect<import("@t3tools/contracts").ImportedHistoryReview, EventSinkV2Error>;
  readonly readImportedHistoryChoice?: (
    input: Parameters<typeof ImportedChoice.readImportedHistoryChoice>[0],
  ) => Effect.Effect<import("@t3tools/contracts").ImportedHistoryOutcome | null, EventSinkV2Error>;
  readonly assertImportedHistoryStartAllowed?: (
    input: Parameters<typeof ImportedChoice.claimImportedHistoryStart>[0],
  ) => Effect.Effect<void, EventSinkV2Error>;
  readonly readStoppedRuntime: (
    provider: OrchestrationV2ProviderThread,
  ) => Effect.Effect<import("@t3tools/contracts").ProviderRuntimeBinding | null, EventSinkV2Error>;
  readonly hasStoppedRuntime: (
    binding: import("@t3tools/contracts").ProviderRuntimeBinding,
  ) => Effect.Effect<boolean, EventSinkV2Error>;
  readonly supersededTerminalTrace: ReturnType<typeof SupersededTerminalTrace.make>;
  readonly writeHistoricalTerminal: (
    input: HistoricalTerminalHistory.HistoricalTerminalInput,
  ) => Effect.Effect<
    {
      readonly handled: boolean;
      readonly persisted?: boolean;
      readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
    },
    EventSinkV2Error
  >;
  /** Items that are not provider snapshots, such as a consumer's own control items, never batch. */
  readonly assertRuntimeStopStartAllowed: (
    input: Parameters<
      ReturnType<typeof RuntimeStop.makeRuntimeStopMethods>["assertStartAllowed"]
    >[0],
  ) => Effect.Effect<void, EventSinkV2Error>;
  readonly readRuntimeStop: (
    commandId: CommandId,
  ) => Effect.Effect<
    Effect.Success<ReturnType<ReturnType<typeof RuntimeStop.makeRuntimeStopMethods>["readState"]>>,
    EventSinkV2Error
  >;
  readonly startRuntimeStop: (
    commandId: CommandId,
    target: import("@t3tools/contracts").CurrentRuntimeStopTarget,
    revalidate: Effect.Effect<void, RuntimeStop.RuntimeStopError>,
  ) => Effect.Effect<boolean, EventSinkV2Error>;
  readonly completeRuntimeStop: (
    commandId: CommandId,
    result: "stopped" | "unknown",
  ) => Effect.Effect<void, EventSinkV2Error>;
  readonly consumeProviderEvents: <A extends { readonly type: string }, E, R, E2, R2>(
    source: Stream.Stream<A, E, R>,
    process: (event: A) => Effect.Effect<void, E2, R2>,
    after: Effect.Effect<boolean, E2, R2>,
    canBatch: Effect.Effect<boolean, never, R2>,
  ) => Effect.Effect<
    void,
    E | E2 | ProviderCommitBatch.CommitBatchError | Schema.SchemaError,
    R | R2
  >;

  /** Normalize and persist an ordered group against one transaction; publish after its commit. */
  readonly withProviderBatch: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | EventSinkV2Error, R>;

  readonly write: (
    input: RuntimeWriteContext & {
      readonly startRefusalHold?: HoldReservation;
      /** Return no events if another thread owns this native session at commit time. */
      readonly unlessNativeThreadOwned?: Pick<
        OrchestrationV2ProviderThread,
        "driver" | "providerInstanceId" | "appThreadId" | "nativeThreadRef"
      >;
      readonly guardPendingUserInputCancellations?: boolean;
      readonly commandId?: CommandId;
      readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
    },
  ) => Effect.Effect<ReadonlyArray<OrchestrationV2StoredEvent>, EventSinkV2Error>;
  readonly writeWithEffects: (
    input: RuntimeWriteContext & {
      readonly startRefusalHold?: HoldReservation;
      readonly queueHold?: QueueHoldIntent;
      /**
       * Rebuilds each run.updated from the stored row with only its delegatedCompletion replaced.
       * The batch must not also change that run's lifecycle.
       */
      readonly updateDelegatedCompletionOnly?: boolean;
      readonly unlessNativeThreadOwned?: Pick<
        OrchestrationV2ProviderThread,
        "driver" | "providerInstanceId" | "appThreadId" | "nativeThreadRef"
      >;
      readonly guardPendingUserInputCancellations?: boolean;
      readonly commandId?: CommandId;
      readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
      readonly effects: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2>;
    },
  ) => Effect.Effect<ReadonlyArray<OrchestrationV2StoredEvent>, EventSinkV2Error>;
  readonly writeIfRunCurrent: (
    input: RuntimeWriteContext & {
      readonly startRefusalReason?: RefusalReason;
      /** Jones: commit only while the thread still records this checkout; the events must bind it. */
      readonly checkoutRecreation?: ExpectedCheckout;
      readonly guardPendingUserInputCancellations?: boolean;
      readonly commandId?: CommandId;
      readonly threadId: ThreadId;
      readonly runId: RunId;
      readonly activeAttemptId: RunAttemptId;
      readonly expectedStatus: OrchestrationV2Run["status"];
      /** Also refuse while this run interrupt request is stored without its result. */
      readonly unlessInterruptRequested?: {
        readonly requestId: OrchestrationV2TurnItem["id"];
        readonly resultId: OrchestrationV2TurnItem["id"];
      };
      readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
      readonly effects?: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2>;
    },
  ) => Effect.Effect<
    {
      readonly committed: boolean;
      readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
    },
    EventSinkV2Error
  >;
  /** Commits only if the expected attempt and ordinal still own the provider thread, protecting newer attempts. */
  readonly writeIfProviderThreadOwner: (
    input: RuntimeWriteContext & {
      readonly guardPendingUserInputCancellations?: boolean;
      readonly commandId?: CommandId;
      readonly providerThreadId: ProviderThreadId;
      readonly runId: RunId;
      readonly activeAttemptId: RunAttemptId;
      readonly expectedLastRunOrdinal: number;
      readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
    },
  ) => Effect.Effect<
    {
      readonly committed: boolean;
      readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
    },
    EventSinkV2Error
  >;
  /** Commits only if the full provider row equals expected; some writers do not advance updatedAt. */
  readonly writeIfProviderThreadUnchanged: (input: {
    readonly expected: OrchestrationV2ProviderThread;
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
  }) => Effect.Effect<
    {
      readonly committed: boolean;
      readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
    },
    EventSinkV2Error
  >;
  /** Commits only if the stored turn item still equals expected. */
  readonly writeIfTurnItemUnchanged: (input: {
    readonly expected: OrchestrationV2TurnItem;
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
  }) => Effect.Effect<
    {
      readonly committed: boolean;
      readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
    },
    EventSinkV2Error
  >;
  /** Commits only if the stored subagent still equals expected. */
  readonly writeIfSubagentUnchanged: (input: {
    readonly expected: OrchestrationV2Subagent;
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
  }) => Effect.Effect<
    {
      readonly committed: boolean;
      readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
    },
    EventSinkV2Error
  >;
  readonly commitCommand: (input: {
    readonly legacyBootstrap?: LegacyDecision.LegacyCommitContext;
    readonly idleWorktreeAdmission?: IdleWorktreeAdmission.Intent | undefined;
    readonly taskContinuation?: TaskContinuationCommitContext | undefined;
    readonly providerConversationReselection?: ProviderConversationReselectionCommitContext;
    readonly importedHistory?: ImportedChoice.ImportedHistoryContext;
    readonly ordinaryCheckoutCommand?: CheckoutIntent.IntentCommand;
    readonly ordinaryCheckout?: CheckoutCommit.PreparationContext & {
      readonly prepared?: CheckoutIntent.PreparedIntent;
    };
    readonly nativeCreation?: {
      readonly claimId: string;
      readonly command: import("@t3tools/contracts").OrchestrationV2Command;
    };
    readonly selfSettlementRevalidate?: Effect.Effect<void, unknown>;
    readonly selfSettlementProjectRemaining?: SelfSettlementProjectRemaining;
    readonly remoteSend?: RemoteSendReceipts.SendCommitContext;
    readonly runtimeStop?: RuntimeStop.RuntimeStopCommitContext;
    readonly resultRequest?: OpenRequest;
    readonly dependencyWake?: WakeCommit;
    readonly threadTitle?: ThreadTitleCommitContext;
    readonly codexFailover?: CodexFailoverCommitContext;
    readonly threadSelfConfiguration?: ThreadSelfConfigurationCommitContext;
    readonly ownerSelection?: ModelSelection | undefined;
    readonly settlementRestore?: { readonly settleCommandId: CommandId };
    readonly commandId: CommandId;
    readonly threadId: ThreadId;
    readonly commandType: string;
    readonly acceptedAt: DateTime.Utc;
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
    readonly effects: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2>;
    readonly cancelUnsettledEffects?: {
      readonly effectTypes: ReadonlyArray<EffectOutbox.OrchestrationEffectRequestV2["type"]>;
      readonly reason: string;
    };
    /** Like writeWithEffects' updateDelegatedCompletionOnly, for this run's run.updated only. */
    readonly updateDelegatedCompletionOnlyRunId?: RunId;
  }) => Effect.Effect<
    {
      readonly receipt: CommandReceiptStore.CommandReceiptV2;
      readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
      readonly committed: boolean;
      readonly cancelledEffectCount: number;
    },
    EventSinkV2Error
  >;
  readonly commitLegacyPreflight: (input: {
    readonly commandId: CommandId;
    readonly event: OrchestrationV2PrivateEvent;
  }) => Effect.Effect<
    { readonly committed: boolean; readonly receipt: LegacyPreflightReceipt },
    EventSinkV2Error
  >;
  readonly readLegacyPreflight: (commandId: CommandId) => Effect.Effect<
    Option.Option<{
      readonly receipt: LegacyPreflightReceipt;
      readonly event: OrchestrationV2PrivateEvent;
    }>,
    EventSinkV2Error
  >;
  readonly readLifecycleByCommandId: (input: {
    readonly commandId: CommandId;
  }) => Stream.Stream<OrchestrationV2StoredEvent, EventSinkV2Error>;
  readonly commitRejectedCommand: (input: {
    readonly legacyGuardRejection?: {
      readonly rejection: DispatchGuardRejected;
      readonly events: readonly OrchestrationV2DomainEvent[];
      readonly context: LegacyDecision.LegacyCommitContext;
    };

    readonly commandId: CommandId;
    readonly threadId: ThreadId;
    readonly commandType: string;
    readonly rejectedAt: DateTime.Utc;
    readonly error: string;
  }) => Effect.Effect<CommandReceiptStore.CommandReceiptV2, EventSinkV2Error>;
  readonly commitProjectCommand: (input: {
    readonly commandId: CommandId;
    readonly projectId: ProjectId;
    readonly commandType: string;
    readonly acceptedAt: DateTime.Utc;
    readonly event: UnsequencedProjectEvent;
  }) => Effect.Effect<
    { readonly receipt: CommandReceiptStore.ProjectCommandReceiptV2; readonly committed: boolean },
    EventSinkV2Error
  >;
  readonly commitRejectedProjectCommand: (input: {
    readonly commandId: CommandId;
    readonly projectId: ProjectId;
    readonly commandType: string;
    readonly rejectedAt: DateTime.Utc;
    readonly error: string;
  }) => Effect.Effect<CommandReceiptStore.ProjectCommandReceiptV2, EventSinkV2Error>;
  readonly stream: (input?: {
    readonly threadId?: ThreadId;
    readonly afterSequence?: number;
    readonly eventType?: OrchestrationV2DomainEvent["type"];
    /** Bound RPC subscribers; internal workers must not drop their subscription under load. */
    readonly bounded?: boolean;
  }) => Stream.Stream<OrchestrationV2StoredEvent, EventSinkV2Error>;
  readonly latestSequence: (input?: {
    readonly threadId?: ThreadId;
  }) => Effect.Effect<number, EventSinkV2Error>;
  readonly canPromoteQueuedAtToolBoundary: (input: {
    readonly threadId: ThreadId;
    readonly queuedRunId: RunId;
    readonly activeRunId: RunId;
    readonly messageId: MessageId;
    readonly boundary: OrchestrationV2StoredEvent;
    readonly runtimeMode: string;
    readonly interactionMode: string;
    readonly births?: Map<
      RunId,
      {
        readonly sequence: number;
        readonly runtimeMode: string;
        readonly interactionMode: string;
        readonly queuedToolBoundaryEligible: boolean;
      } | null
    >;
  }) => Effect.Effect<boolean, EventSinkV2Error>;
  readonly readByCommandId: (input: {
    readonly commandId: CommandId;
  }) => Stream.Stream<OrchestrationV2StoredEvent, EventSinkV2Error>;
}

export class EventSinkV2 extends Context.Service<EventSinkV2, EventSinkV2Shape>()(
  "t3/orchestration-v2/EventSink/EventSinkV2",
) {}

const layerBase: Layer.Layer<
  EventSinkV2,
  never,
  | CommandReceiptStore.CommandReceiptStoreV2
  | EffectOutbox.EffectOutboxV2
  | EventStore.EventStoreV2
  | ProjectionStore.ProjectionStoreV2
  | ProjectStore.ProjectStoreV2
  | SqlClient.SqlClient
  | TurnItemPositionStore.TurnItemPositionStoreV2
> = Layer.effect(
  EventSinkV2,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const legacyStorage = LegacyStorage.make(sql);
    const validateLegacyGuard = (
      command: Parameters<Effect.Success<ReturnType<typeof makeDispatchGuard>>>[0],
    ) =>
      makeDispatchGuard().pipe(
        Effect.provideService(EventSinkV2, eventSinkService),
        Effect.provideService(ProjectionStore.ProjectionStoreV2, projectionStore),
        Effect.flatMap((validate) => validate(command)),
      );
    const deletionAdmission = yield* makeDeletionAdmission.pipe(Effect.provide(NodePath.layer));
    const idleWorktreeAdmission = IdleWorktreeAdmission.makeIdleWorktreeAdmission(sql);
    const checkoutIntent = yield* Effect.serviceOption(CheckoutIntent.CheckoutIntent);
    const captureCommittedProvenance = yield* WorkspaceProvenance.optionalCommittedCapture;
    const provenance = yield* Effect.serviceOption(WorkspaceProvenanceStore.ProvenanceStore);
    const admission = yield* Effect.serviceOption(UpdateAdmissionFence.UpdateAdmissionFence);
    if (Option.isSome(admission)) yield* admission.value.attach;
    const nativeCreation = yield* Effect.serviceOption(
      NativeCreationRepository.NativeCreationRepository,
    );
    const captureSettlementProof = (
      receipt: CommandReceiptStore.CommandReceiptV2,
      storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>,
    ) =>
      receipt.commandType === "thread.settle" || receipt.commandType === "thread.unsettle"
        ? captureNativeProviderSettlementProof({
            receipt: {
              commandId: receipt.commandId,
              aggregateKind: "thread",
              aggregateId: receipt.threadId,
              commandType: receipt.commandType,
              acceptedAt: DateTime.formatIso(receipt.acceptedAt),
              resultSequence: receipt.resultSequence,
              status: receipt.status,
            },
            events: storedEvents.map((stored) => ({
              eventId: stored.event.id,
              commandId: stored.commandId,
              aggregateKind: "thread" as const,
              aggregateId: stored.event.threadId,
              sequence: stored.sequence,
              type: stored.event.type,
              occurredAt: DateTime.formatIso(stored.event.occurredAt),
              applicationEventVersion: 2,
            })),
          }).pipe(Effect.provideService(SqlClient.SqlClient, sql))
        : Effect.void;
    const commandReceipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
    const effectOutbox = yield* EffectOutbox.EffectOutboxV2;
    const eventStore = yield* EventStore.EventStoreV2;
    const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
    const legacyCleanup = yield* Effect.serviceOption(LegacyCleanup.TerminalCleanupWorker);
    const historicalTerminals = yield* HistoricalTerminalHistory.make;
    const supersededTerminalTrace = SupersededTerminalTrace.make();
    const runtimeEvidenceGuard = RuntimeEvidenceGuard.makeRuntimeEvidenceGuard(sql);
    const startRefusals = yield* Effect.serviceOption(StartRefusal.StartRefusals);
    const tables = makeJonesTablePresence(sql);
    const importedHistoryCommit = yield* ImportedHistoryCommit.make;
    const ownerSelections = makeOwnerSelectionStore(sql, tables);
    const codexFailoverStore = yield* CodexFailoverStore.make;
    const dependencies = makeDependencyStore(
      sql,
      tables,
      Option.isSome(startRefusals) ? startRefusals.value.establish : undefined,
    );
    const dependencyChanges = yield* Effect.serviceOption(DependencyChanges.DependencyChanges);
    const settlementRestores = makeSettlementRestoreStore(sql, tables);
    const queueHolds = makeQueueHoldStore(sql, tables);
    const remoteSendReceipts = RemoteSendReceipts.makeRemoteSendReceiptStore(sql, tables);
    const runtimeStops = RuntimeStop.makeRuntimeStopMethods(sql, tables);
    const stopError = (cause: unknown) => new EventSinkWriteError({ eventCount: 0, cause });
    const projectStore = yield* ProjectStore.ProjectStoreV2;
    const turnItemPositions = yield* TurnItemPositionStore.TurnItemPositionStoreV2;
    const enqueueEffects = Effect.fnUntraced(function* (
      effects: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2>,
      events: ReadonlyArray<OrchestrationV2DomainEvent>,
    ) {
      if (Option.isSome(admission)) yield* admission.value.admit(effects, events);
      for (const effect of effects)
        if (effect.request.type === "provider-turn.start")
          yield* runtimeStops.assertStartAllowed({
            threadId: effect.threadId,
            runId: effect.request.runId,
          });
      yield* effectOutbox.enqueue(effects);
    });
    const liveEvents = yield* PubSub.unbounded<OrchestrationV2StoredEvent>();
    const liveEventsByType = new Map<
      OrchestrationV2DomainEvent["type"],
      PubSub.PubSub<OrchestrationV2StoredEvent>
    >();
    const publishLiveEvents =
      /* This helper runs for individual orchestration events; the enclosing operation owns the span. */
      Effect.fnUntraced(function* (events: ReadonlyArray<OrchestrationV2StoredEvent>) {
        yield* PubSub.publishAll(liveEvents, events);
        for (const [type, pubsub] of liveEventsByType) {
          yield* PubSub.publishAll(
            pubsub,
            events.filter((stored) => stored.event.type === type),
          );
        }
      });
    const runOutcomes = makeRunOutcomeRecorder((threadId) =>
      // One payload row; unlike the shell, it still resolves a deleted child thread.
      projectionStore
        .getThread(threadId)
        .pipe(Effect.map((thread) => thread.lineage.relationshipToParent === "subagent")),
    );
    // Recorded before publication, so an interrupted publish cannot lose a committed outcome.
    const publishStoredEvents = (
      events: ReadonlyArray<OrchestrationV2StoredEvent>,
      outcomes: ReadonlyArray<RunOutcome>,
    ) =>
      runOutcomes
        .record(outcomes)
        .pipe(
          Effect.andThen(eventStore.publishCommitted(events)),
          Effect.andThen(publishLiveEvents(events)),
        );

    // Serialize commit and publish; reordered publication makes clients drop earlier sequences.
    const publishLane = yield* Semaphore.make(1);
    const commitThenPublishNow = <A, E, R>(
      transaction: Effect.Effect<A, E, R>,
      publish: (committed: A) => Effect.Effect<void>,
      local?: {
        readonly acceptance: import("../jones/ordinaryCheckout/CheckoutPreparation.ts").LocalAcceptance;
        readonly commandId: CommandId;
        readonly authorityRevision: string;
      },
    ) =>
      Effect.uninterruptibleMask((restore) => {
        const changedThreadIds = new Set<ThreadId>();
        let dependencySequence = 0;
        let holdsLane = false;
        const takeLane = publishLane.take(1).pipe(
          Effect.andThen(
            Effect.sync(() => {
              holdsLane = true;
            }),
          ),
          Effect.uninterruptible,
        );
        // Cancellation may roll back the transaction's work. Once it commits,
        // every subscriber must hear about it before this writer can exit.
        // Otherwise a failed child run can leave its parent's task running.
        const body = restore(transaction).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
          Effect.provideService(ChangedThreads, changedThreadIds),
          Effect.tap(() =>
            changedThreadIds.size === 0
              ? Effect.void
              : dependencies.sequence.pipe(
                  Effect.tap((sequence) =>
                    Effect.sync(() => {
                      dependencySequence = sequence;
                    }),
                  ),
                ),
          ),
          Effect.tap(() => takeLane),
        );
        // Shared acceptance owns the outer local transaction, including sidecars.
        // Journal acknowledgement and every publication occur after its commit.
        const commit =
          local === undefined
            ? sql.withTransaction(body)
            : SharedAdmission.commitLocalAcceptance(
                local.acceptance.live,
                { receiptId: local.commandId, authorityRevision: local.authorityRevision },
                body,
              ).pipe(Effect.map(({ value }) => value));
        return commit.pipe(
          Effect.tap(publish),
          Effect.tap(() =>
            changedThreadIds.size > 0 && Option.isSome(dependencyChanges)
              ? dependencyChanges.value.publish({
                  threadIds: [...changedThreadIds],
                  sequence: dependencySequence,
                })
              : Effect.void,
          ),
          Effect.ensuring(Effect.suspend(() => (holdsLane ? publishLane.release(1) : Effect.void))),
        );
      });

    // A user can answer after terminal normalization reads the pending request.
    // Recheck inside the write transaction so stale cleanup cannot erase answers.
    const guardUserInputCancellations =
      /* This helper runs for individual orchestration events; the enclosing operation owns the span. */
      Effect.fnUntraced(function* (events: ReadonlyArray<OrchestrationV2DomainEvent>) {
        const staleRequests = new Set<RuntimeRequestId>();
        const staleNodes = new Set<NodeId>();
        for (const event of events) {
          if (
            event.type !== "runtime-request.updated" ||
            event.payload.kind !== "user_input" ||
            event.payload.status !== "cancelled"
          )
            continue;
          const current = yield* projectionStore.getRuntimeRequest(
            event.threadId,
            event.payload.id,
          );
          if (
            current?.status !== "pending" ||
            current.kind !== "user_input" ||
            current.providerTurnId !== event.payload.providerTurnId ||
            current.responseCapability.type === "message"
          ) {
            staleRequests.add(event.payload.id);
            staleNodes.add(event.payload.nodeId);
          }
        }
        return events.filter((event) => {
          switch (event.type) {
            case "runtime-request.updated":
              return event.payload.status !== "cancelled" || !staleRequests.has(event.payload.id);
            case "node.updated":
              return event.payload.status !== "cancelled" || !staleNodes.has(event.payload.id);
            case "turn-item.updated":
              return (
                event.payload.type !== "user_input_request" ||
                event.payload.status !== "cancelled" ||
                !staleRequests.has(event.payload.requestId)
              );
            default:
              return true;
          }
        });
      });

    const guardRuntimeIdentity = Effect.fn("EventSink.guardRuntimeIdentity")(function* (
      input: Pick<
        Parameters<EventSinkV2Shape["write"]>[0],
        | "events"
        | "historicalTerminal"
        | "runtimeEvidence"
        | "runtimeIdentityObservation"
        | "runtimeIdentityBoundary"
        | "runtimeIdentityRequest"
        | "runtimeIdentityPreviousRequest"
      > & {
        readonly effects?: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2>;
        readonly settleRunOnLostRuntimeEvidence?: boolean;
      },
    ) {
      if (input.historicalTerminal !== undefined) {
        if (
          input.runtimeIdentityBoundary !== undefined ||
          input.runtimeIdentityObservation !== undefined ||
          input.runtimeIdentityRequest !== undefined ||
          input.runtimeIdentityPreviousRequest !== undefined ||
          (input.effects?.length ?? 0) > 0
        )
          return null;
        const historical = yield* historicalTerminals.select(input.historicalTerminal);
        if (HistoricalTerminalHistory.isHistoricalTerminalRejection(historical)) {
          yield* SupersededTerminalTrace.recordTerminalRejection(
            supersededTerminalTrace,
            input.historicalTerminal.event,
            historical.reason,
          );
          return null;
        }
        if (historical !== "ordinary") return [historical];
      }
      const capture = input.runtimeEvidence;
      const update = input.events.find((event) => event.type === "provider-thread.updated");
      if (input.runtimeIdentityBoundary !== undefined) {
        if (update?.type !== "provider-thread.updated") return null;
        const records = yield* projectionStore.getThreadRecords(update.threadId, [
          "providerThreads",
        ]);
        const current =
          records.providerThreads.find((thread) => thread.id === update.payload.id) ?? null;
        const expectedIdentity = input.runtimeIdentityBoundary.expectedProviderThreadIdentity;
        if (
          expectedIdentity !== undefined &&
          (expectedIdentity.id !== update.payload.id ||
            expectedIdentity.appThreadId !== update.threadId ||
            current === null ||
            current.id !== expectedIdentity.id ||
            current.appThreadId !== expectedIdentity.appThreadId ||
            current.driver !== expectedIdentity.driver ||
            current.providerInstanceId !== expectedIdentity.providerInstanceId ||
            current.providerSessionId !== expectedIdentity.providerSessionId ||
            (current.nativeThreadRef === null || expectedIdentity.nativeThreadRef === null
              ? current.nativeThreadRef !== expectedIdentity.nativeThreadRef
              : !Schema.toEquivalence(OrchestrationV2ProviderRef)(
                  current.nativeThreadRef,
                  expectedIdentity.nativeThreadRef,
                )))
        )
          return null;
        if (
          (input.runtimeIdentityBoundary.expectedActiveProviderThreadId !== undefined &&
            (input.runtimeIdentityBoundary.expectedActiveProviderThreadId !== update.payload.id ||
              records.thread.activeProviderThreadId !==
                input.runtimeIdentityBoundary.expectedActiveProviderThreadId)) ||
          (capture !== undefined &&
            RuntimeEvidenceGuard.runtimeEvidenceMismatch(current, capture) !== undefined) ||
          (current?.runtimeIdentity?.runtimeGeneration ?? null) !==
            input.runtimeIdentityBoundary.expectedGeneration ||
          (current !== null &&
            (current.appThreadId !== update.payload.appThreadId ||
              current.providerInstanceId !== update.payload.providerInstanceId ||
              current.driver !== update.payload.driver))
        )
          return null;
        return input.events.map((event) =>
          event.type === "provider-thread.updated" && event.payload.id === update.payload.id
            ? {
                ...event,
                payload: {
                  ...event.payload,
                  runtimeIdentity:
                    event.payload.runtimeIdentity === undefined
                      ? undefined
                      : {
                          ...event.payload.runtimeIdentity,
                          evidenceRevision: (current?.runtimeIdentity?.evidenceRevision ?? 0) + 1,
                        },
                },
              }
            : event,
        );
      }
      if (capture === undefined)
        return yield* runtimeEvidenceGuard.retainStoredIdentity(input.events);
      const current =
        (yield* projectionStore.getThreadRecords(capture.threadId, [
          "providerThreads",
        ])).providerThreads.find((thread) => thread.id === capture.providerThreadId) ?? null;
      const mismatch = RuntimeEvidenceGuard.runtimeEvidenceMismatch(current, capture);
      if (mismatch !== undefined || current === null)
        return yield* runtimeEvidenceGuard.rejected({
          capture,
          current,
          reason: mismatch ?? "provider_thread_missing",
          events: input.events,
          settleRunOnLostEvidence:
            input.settleRunOnLostRuntimeEvidence === true &&
            input.runtimeIdentityRequest === undefined &&
            input.runtimeIdentityObservation === undefined,
        });
      const identity = current.runtimeIdentity!;
      if (
        input.runtimeIdentityRequest !== undefined &&
        capture.evidenceRevision !== identity.evidenceRevision
      ) {
        const previous = input.runtimeIdentityPreviousRequest;
        if (
          previous === undefined ||
          identity.requested.providerInstanceId !== previous.providerInstanceId ||
          identity.requested.providerDriver !== previous.providerDriver ||
          identity.requested.model !== previous.model ||
          identity.requested.serviceTier !== previous.serviceTier
        ) {
          yield* runtimeEvidenceGuard.reportRejection({
            capture,
            reason: "requested_identity_changed",
            events: input.events,
          });
          return null;
        }
      }
      if (input.runtimeIdentityObservation !== undefined) {
        const requested = input.runtimeIdentityObservation;
        if (
          identity.requested.providerInstanceId !== requested.providerInstanceId ||
          identity.requested.providerDriver !== requested.providerDriver ||
          identity.requested.model !== requested.model ||
          identity.requested.serviceTier !== requested.serviceTier ||
          identity.evidenceRevision !== capture.evidenceRevision
        ) {
          yield* runtimeEvidenceGuard.reportRejection({
            capture,
            reason: "observation_stale",
            events: input.events,
          });
          return null;
        }
      }
      return input.events.map((event) =>
        event.type === "provider-thread.updated" && event.payload.id === current.id
          ? {
              ...event,
              payload: {
                ...event.payload,
                runtimeIdentity:
                  input.runtimeIdentityObservation === undefined
                    ? input.runtimeIdentityRequest === undefined
                      ? identity
                      : {
                          ...identityForRequest(input.runtimeIdentityRequest, identity),
                          evidenceRevision: (identity.evidenceRevision ?? 0) + 1,
                        }
                    : {
                        ...identity,
                        observed: event.payload.runtimeIdentity!.observed,
                        evidenceRevision: (identity.evidenceRevision ?? 0) + 1,
                      },
              },
            }
          : event,
      );
    });

    // Compare inside the guarded writer transaction, after runtime evidence validation.
    const filterUnchangedProviderNodes = Effect.fnUntraced(function* (
      events: ReadonlyArray<OrchestrationV2DomainEvent>,
    ) {
      const changed: Array<OrchestrationV2DomainEvent> = [];
      for (const event of events) {
        if (event.type !== "node.updated" || !(yield* projectionStore.isNodeStored(event.payload)))
          changed.push(event);
      }
      return changed;
    });

    const normalizeEvents = (events: ReadonlyArray<OrchestrationV2DomainEvent>) => {
      const runOrdinals = new Map(
        events.flatMap((event) =>
          event.type === "run.created" || event.type === "run.updated"
            ? [[event.payload.id, event.payload.ordinal] as const]
            : [],
        ),
      );
      return Effect.forEach(
        events,
        (event): Effect.Effect<OrchestrationV2DomainEvent, unknown> =>
          event.type === "turn-item.updated"
            ? turnItemPositions
                .normalize(
                  event.payload,
                  event.payload.runId === null ? undefined : runOrdinals.get(event.payload.runId),
                )
                .pipe(Effect.map((payload) => ({ ...event, payload })))
            : Effect.succeed(event),
        { concurrency: 1 },
      );
    };

    const applyStoredEvents =
      /* This helper runs for individual orchestration events; the enclosing operation owns the span. */
      Effect.fnUntraced(function* (
        storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>,
        origin?: QueueHoldOrigin,
      ) {
        for (const { event } of storedEvents) {
          if (event.type === "provider-session.attached")
            yield* validateRuntimeUseAttachmentPublication(event.threadId, event.payload.id).pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
            );
        }
        yield* RetirementEvents.events(
          sql,
          storedEvents.map((stored) => stored.event),
        );
        yield* settlementRestores.capture(storedEvents, projectionStore.getThread);
        yield* Effect.forEach(storedEvents, (stored) => projectionStore.apply(stored.event), {
          concurrency: 1,
        });
        if (Option.isSome(provenance))
          yield* provenance.value
            .events(storedEvents.map((stored) => stored.event))
            .pipe(
              Effect.catch((cause) =>
                Effect.logWarning("Workspace provenance facts held", { reason: cause.reason }),
              ),
            );
        if (Option.isSome(startRefusals)) {
          for (const stored of storedEvents) {
            const event = stored.event;
            if (
              (event.type !== "run.created" && event.type !== "run.updated") ||
              event.payload.activeAttemptId === null ||
              (event.payload.status !== "starting" && event.payload.status !== "preparing")
            )
              continue;
            const message = yield* projectionStore.getRunMessage(event.threadId, event.payload.id);
            if (message?.delegatedCompletion !== undefined)
              yield* startRefusals.value.establish(
                {
                  threadId: event.threadId,
                  runId: event.payload.id,
                  attemptId: event.payload.activeAttemptId,
                },
                "delegated_completion",
                stored.sequence,
              );
          }
          yield* startRefusals.value.events(storedEvents);
        }
        yield* dependencies.wakes.events(storedEvents);
        yield* acceptDependencyEvents(
          dependencies,
          storedEvents.map((stored) => stored.event),
        );
        yield* acceptQueueHoldEvents(
          queueHolds,
          storedEvents.map((stored) => stored.event),
          origin,
        );
        const sequence = storedEvents.at(-1)?.sequence;
        if (sequence !== undefined) {
          const now = DateTime.formatIso(yield* DateTime.now);
          yield* sql`
            INSERT INTO orchestration_v2_projection_metadata (
              projection_name,
              schema_version,
              last_sequence,
              updated_at
            )
            VALUES (
              'thread-projections',
              ${ProjectionStore.ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION},
              ${sequence},
              ${now}
            )
            ON CONFLICT(projection_name)
            DO UPDATE SET
              schema_version = excluded.schema_version,
              last_sequence = excluded.last_sequence,
              updated_at = excluded.updated_at
          `;
        }
        // Read lineage here, on the connection this transaction holds; see makeRunOutcomeRecorder.
        return yield* runOutcomes.prepare(storedEvents);
      });

    // The enclosing write or commit span already traces this per-event projection merge.
    const updateDelegatedCompletionOnly = Effect.fnUntraced(function* (
      events: ReadonlyArray<OrchestrationV2DomainEvent>,
      includesRun: (runId: RunId) => boolean,
    ) {
      return yield* Effect.forEach(events, (event) =>
        Effect.gen(function* () {
          if (event.type !== "run.updated" || !includesRun(event.payload.id)) return event;
          const { runs } = yield* projectionStore.getThreadRecords(event.threadId, ["runs"], {
            runIds: [event.payload.id],
          });
          const current = runs[0];
          return current === undefined
            ? event
            : {
                ...event,
                payload: { ...current, delegatedCompletion: event.payload.delegatedCompletion },
              };
        }),
      );
    });

    const writeEffect = Effect.fn("orchestrationV2.EventSink.write")(function* (
      input: Parameters<EventSinkV2Shape["writeWithEffects"]>[0],
    ) {
      yield* Effect.annotateCurrentSpan({
        "orchestration_v2.command_id": input.commandId ?? null,
        "orchestration_v2.event_count": input.events.length,
        "orchestration_v2.thread_id": input.events[0]?.threadId ?? null,
      });

      for (const event of input.events) {
        if (event.type === "provider-session.attached")
          yield* revalidateRuntimeUseAttachmentBeforePublication(
            event.threadId,
            event.payload.id,
          ).pipe(Effect.provideService(SqlClient.SqlClient, sql));
      }

      const batch = yield* CurrentProviderBatch;
      if (batch !== undefined) batch.eventCount += input.events.length;
      const { committed } = yield* commitThenPublish(
        Effect.gen(function* () {
          if (
            input.startRefusalHold !== undefined &&
            (Option.isNone(startRefusals) ||
              !(yield* startRefusals.value.validateReservation(input.startRefusalHold)))
          )
            return { accepted: false, committed: [], outcomes: [] };
          // Check ownership in the same transaction that claims the native session.
          const imported = input.unlessNativeThreadOwned;
          if (imported?.nativeThreadRef != null) {
            const owners = yield* sql`
              SELECT provider_thread_id
              FROM orchestration_v2_projection_provider_threads
              WHERE driver = ${imported.driver}
                AND provider_instance_id = ${imported.providerInstanceId}
                AND thread_id IS NOT ${imported.appThreadId}
                AND json_extract(payload_json, '$.nativeThreadRef.nativeId') = ${imported.nativeThreadRef.nativeId}
              LIMIT 1
            `;
            if (owners.length > 0) return { accepted: false, committed: [], outcomes: [] };
          }
          // Checkpoint capture can finish after a cohort writer reads its parent run.
          // Merge in this transaction so the event log and projection keep that completion.
          const events = input.updateDelegatedCompletionOnly
            ? yield* updateDelegatedCompletionOnly(input.events, () => true)
            : input.events;
          const identityEvents = yield* guardRuntimeIdentity({ ...input, events });
          if (identityEvents === null) return { accepted: false, committed: [], outcomes: [] };
          const normalized = yield* normalizeEvents(
            input.guardPendingUserInputCancellations === true
              ? yield* guardUserInputCancellations(identityEvents)
              : identityEvents,
          );
          const committed = yield* eventStore
            .append({
              ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
              events:
                input.skipUnchangedProviderNodes === true
                  ? yield* filterUnchangedProviderNodes(normalized)
                  : normalized,
            })
            .pipe(Effect.map((stored) => stored.filter(isPublicStoredOrchestrationEvent)));
          const outcomes = yield* applyStoredEvents(committed, { queueHold: input.queueHold });
          if (input.startRefusalHold !== undefined && Option.isSome(startRefusals))
            yield* startRefusals.value.reserve(input.startRefusalHold, committed.at(-1)!.sequence);
          yield* enqueueEffects(
            input.effects,
            committed.map((stored) => stored.event),
          );
          if (Option.isSome(checkoutIntent))
            yield* CheckoutCommit.withDerived(
              sql,
              { storedEvents: committed, effects: input.effects },
              checkoutIntent.value.linkEffects({ storedEvents: committed, effects: input.effects }),
            );
          return { accepted: true, committed, outcomes };
        }).pipe(Effect.withTracerEnabled(false)),
        ({ accepted, committed, outcomes }) =>
          Effect.gen(function* () {
            if (!accepted) return;
            historicalTerminals.rememberCommitted(input.effects, committed);
            if (input.effects.length > 0) {
              yield* effectOutbox.notifyAvailable(input.effects.length);
            }
            yield* publishStoredEvents(committed, outcomes);
          }),
      );
      return committed;
    });

    const writeIfRunCurrentEffect = Effect.fn("orchestrationV2.EventSink.writeIfRunCurrent")(
      function* (input: Parameters<EventSinkV2Shape["writeIfRunCurrent"]>[0]) {
        yield* Effect.annotateCurrentSpan({
          "orchestration_v2.command_id": input.commandId ?? null,
          "orchestration_v2.event_count": input.events.length,
          "orchestration_v2.run_id": input.runId,
          "orchestration_v2.thread_id": input.threadId,
        });

        return yield* commitThenPublish(
          Effect.gen(function* () {
            const rows = yield* sql<{
              readonly status: string;
              readonly active_attempt_id: string | null;
            }>`
            SELECT
              status,
              json_extract(payload_json, '$.activeAttemptId') AS active_attempt_id
            FROM orchestration_v2_projection_runs
            WHERE run_id = ${input.runId}
              AND thread_id = ${input.threadId}
            LIMIT 1
          `;
            const current = rows[0];
            if (
              current === undefined ||
              current.status !== input.expectedStatus ||
              current.active_attempt_id !== input.activeAttemptId
            ) {
              return {
                committed: false as const,
                storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent>,
              };
            }
            if (input.unlessInterruptRequested !== undefined) {
              const { requestId, resultId } = input.unlessInterruptRequested;
              const pending = yield* sql<{ readonly pending: number }>`
                SELECT EXISTS (
                  SELECT 1 FROM orchestration_v2_projection_turn_items
                  WHERE thread_id = ${input.threadId} AND turn_item_id = ${requestId}
                ) AND NOT EXISTS (
                  SELECT 1 FROM orchestration_v2_projection_turn_items
                  WHERE thread_id = ${input.threadId} AND turn_item_id = ${resultId}
                ) AS pending
              `;
              if (pending[0]?.pending === 1) {
                return {
                  committed: false as const,
                  storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent>,
                };
              }
            }

            const guardedEvents = yield* guardRuntimeIdentity(input);
            // Jones hook: a recreated checkout binds only against the current projection, in this transaction.
            const identityEvents =
              input.checkoutRecreation === undefined || guardedEvents === null
                ? guardedEvents
                : bindRecreatedCheckout(
                    yield* projectionStore.getThread(input.threadId),
                    input.checkoutRecreation,
                    guardedEvents,
                  );
            if (identityEvents === null)
              return {
                committed: false as const,
                storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent>,
              };
            const normalized = yield* normalizeEvents(
              input.guardPendingUserInputCancellations === true
                ? yield* guardUserInputCancellations(identityEvents)
                : identityEvents,
            );
            const storedEvents = yield* eventStore
              .append({
                ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
                events:
                  input.skipUnchangedProviderNodes === true
                    ? yield* filterUnchangedProviderNodes(normalized)
                    : normalized,
              })
              .pipe(Effect.map((stored) => stored.filter(isPublicStoredOrchestrationEvent)));
            const outcomes = yield* applyStoredEvents(storedEvents);
            if (
              input.startRefusalReason !== undefined &&
              input.expectedStatus === "starting" &&
              Option.isSome(startRefusals)
            )
              yield* startRefusals.value.certify(
                { threadId: input.threadId, runId: input.runId, attemptId: input.activeAttemptId },
                input.startRefusalReason,
                storedEvents,
                { certified: dependencies.wakes.certified },
              );
            const effects = input.effects ?? [];
            yield* enqueueEffects(
              effects,
              storedEvents.map((stored) => stored.event),
            );
            if (Option.isSome(checkoutIntent))
              yield* CheckoutCommit.withDerived(
                sql,
                { storedEvents, effects },
                checkoutIntent.value.linkEffects({ storedEvents, effects }),
              );
            return { committed: true as const, storedEvents, outcomes };
          }),
          (result) =>
            Effect.gen(function* () {
              if (!result.committed) return;
              historicalTerminals.rememberCommitted(input.effects ?? [], result.storedEvents);
              if (input.effects !== undefined && input.effects.length > 0) {
                yield* effectOutbox.notifyAvailable(input.effects.length);
              }
              yield* publishStoredEvents(result.storedEvents, result.outcomes);
            }),
        );
      },
    );

    const writeIfProviderThreadOwnerEffect = Effect.fn(
      "orchestrationV2.EventSink.writeIfProviderThreadOwner",
    )(function* (input: Parameters<EventSinkV2Shape["writeIfProviderThreadOwner"]>[0]) {
      yield* Effect.annotateCurrentSpan({
        "orchestration_v2.command_id": input.commandId ?? null,
        "orchestration_v2.event_count": input.events.length,
        "orchestration_v2.provider_thread_id": input.providerThreadId,
        "orchestration_v2.run_id": input.runId,
        "orchestration_v2.active_attempt_id": input.activeAttemptId,
        "orchestration_v2.expected_last_run_ordinal": input.expectedLastRunOrdinal,
      });

      return yield* commitThenPublish(
        Effect.gen(function* () {
          const rows = yield* sql<{
            readonly active_attempt_id: string | null;
            readonly last_run_ordinal: number | null;
          }>`
            SELECT
              json_extract(r.payload_json, '$.activeAttemptId') AS active_attempt_id,
              p.last_run_ordinal
            FROM orchestration_v2_projection_provider_threads p
            JOIN orchestration_v2_projection_runs r
              ON r.run_id = ${input.runId}
             AND r.thread_id = p.thread_id
            WHERE p.provider_thread_id = ${input.providerThreadId}
            LIMIT 1
          `;
          const current = rows[0];
          if (
            current === undefined ||
            current.active_attempt_id !== input.activeAttemptId ||
            current.last_run_ordinal !== input.expectedLastRunOrdinal
          ) {
            return {
              committed: false as const,
              storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent>,
            };
          }

          const identityEvents = yield* guardRuntimeIdentity(input);
          if (identityEvents === null)
            return {
              committed: false as const,
              storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent>,
            };
          const normalized = yield* normalizeEvents(
            input.guardPendingUserInputCancellations === true
              ? yield* guardUserInputCancellations(identityEvents)
              : identityEvents,
          );
          const storedEvents = yield* eventStore
            .append({
              ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
              events:
                input.skipUnchangedProviderNodes === true
                  ? yield* filterUnchangedProviderNodes(normalized)
                  : normalized,
            })
            .pipe(Effect.map((stored) => stored.filter(isPublicStoredOrchestrationEvent)));
          const outcomes = yield* applyStoredEvents(storedEvents);
          return { committed: true as const, storedEvents, outcomes };
        }),
        (result) =>
          result.committed
            ? publishStoredEvents(result.storedEvents, result.outcomes)
            : Effect.void,
      );
    });

    // Appends the events only if the stored payload still matches, in one transaction.
    const writeIfStoredPayloadMatches = <E>(
      readStoredPayload: Effect.Effect<ReadonlyArray<{ readonly payload_json: string }>, E>,
      expectedJson: string,
      events: ReadonlyArray<OrchestrationV2DomainEvent>,
    ) =>
      commitThenPublish(
        Effect.gen(function* () {
          const rows = yield* readStoredPayload;
          if (rows[0]?.payload_json !== expectedJson) {
            return {
              committed: false as const,
              storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent>,
            };
          }

          const normalized = yield* normalizeEvents(events);
          const storedEvents = yield* eventStore
            .append({ events: normalized })
            .pipe(Effect.map((stored) => stored.filter(isPublicStoredOrchestrationEvent)));
          const outcomes = yield* applyStoredEvents(storedEvents);
          return { committed: true as const, storedEvents, outcomes };
        }),
        (result) =>
          result.committed
            ? publishStoredEvents(result.storedEvents, result.outcomes)
            : Effect.void,
      );

    const writeIfProviderThreadUnchangedEffect = Effect.fn(
      "orchestrationV2.EventSink.writeIfProviderThreadUnchanged",
    )(function* (input: Parameters<EventSinkV2Shape["writeIfProviderThreadUnchanged"]>[0]) {
      yield* Effect.annotateCurrentSpan({
        "orchestration_v2.event_count": input.events.length,
        "orchestration_v2.provider_thread_id": input.expected.id,
      });
      const expectedJson = yield* ProjectionStore.encodeProviderThreadPayload(input.expected);
      return yield* writeIfStoredPayloadMatches(
        sql<{ readonly payload_json: string }>`
          SELECT payload_json
          FROM orchestration_v2_projection_provider_threads
          WHERE provider_thread_id = ${input.expected.id}
          LIMIT 1
        `,
        expectedJson,
        input.events,
      );
    });

    const writeIfTurnItemUnchangedEffect = Effect.fn(
      "orchestrationV2.EventSink.writeIfTurnItemUnchanged",
    )(function* (input: Parameters<EventSinkV2Shape["writeIfTurnItemUnchanged"]>[0]) {
      yield* Effect.annotateCurrentSpan({
        "orchestration_v2.event_count": input.events.length,
        "orchestration_v2.turn_item_id": input.expected.id,
      });
      const expectedJson = yield* ProjectionStore.encodeTurnItemPayload(input.expected);
      return yield* writeIfStoredPayloadMatches(
        sql<{ readonly payload_json: string }>`
          SELECT payload_json
          FROM orchestration_v2_projection_turn_items
          WHERE turn_item_id = ${input.expected.id}
          LIMIT 1
        `,
        expectedJson,
        input.events,
      );
    });

    const writeIfSubagentUnchangedEffect = Effect.fn(
      "orchestrationV2.EventSink.writeIfSubagentUnchanged",
    )(function* (input: Parameters<EventSinkV2Shape["writeIfSubagentUnchanged"]>[0]) {
      yield* Effect.annotateCurrentSpan({
        "orchestration_v2.event_count": input.events.length,
        "orchestration_v2.subagent_id": input.expected.id,
      });
      const expectedJson = yield* ProjectionStore.encodeSubagentPayload(input.expected);
      return yield* writeIfStoredPayloadMatches(
        sql<{ readonly payload_json: string }>`
          SELECT payload_json
          FROM orchestration_v2_projection_subagents
          WHERE subagent_id = ${input.expected.id}
          LIMIT 1
        `,
        expectedJson,
        input.events,
      );
    });

    const existingCommandResult =
      /* This helper runs for individual orchestration events; the enclosing operation owns the span. */
      Effect.fnUntraced(function* (commandId: CommandId) {
        const existing = yield* commandReceipts.getByCommandId(commandId);
        if (Option.isNone(existing)) {
          return yield* Effect.die(
            new Error(`Command receipt ${commandId} disappeared during its transaction.`),
          );
        }
        const storedEvents = yield* eventStore.readByCommandId({ commandId }).pipe(
          Stream.filter(isPublicStoredOrchestrationEvent),
          Stream.runCollect,
          Effect.map((events): ReadonlyArray<OrchestrationV2StoredEvent> => Array.from(events)),
        );
        return { receipt: existing.value, storedEvents };
      });

    const commitCommandEffect = Effect.fn("orchestrationV2.EventSink.commitCommand")(function* (
      input: Parameters<EventSinkV2Shape["commitCommand"]>[0],
    ) {
      if (
        (input.commandType === "legacy-bootstrap.guard-rejection-delete" ||
          input.commandType === "legacy-bootstrap.failure-delete") &&
        input.legacyBootstrap?.mutation.kind !== "delete"
      )
        return yield* new LegacyStorageError({ reason: "private_delete_owner_required" });
      const ordinaryCommand = input.ordinaryCheckoutCommand ?? input.importedHistory?.command;
      if (
        input.importedHistory !== undefined &&
        input.ordinaryCheckoutCommand !== undefined &&
        (yield* decodedCommand(input.ordinaryCheckoutCommand)).digest !==
          (yield* decodedCommand(input.importedHistory.command)).digest
      )
        return yield* new CheckoutError({ reason: "identity" });
      if (
        input.ordinaryCheckout !== undefined &&
        (Option.isNone(checkoutIntent) ||
          ordinaryCommand === undefined ||
          input.nativeCreation !== undefined)
      )
        return yield* new CheckoutError({ reason: "unavailable" });
      let ordinary: CheckoutIntent.PreparedIntent | undefined;
      if (Option.isSome(checkoutIntent) && input.nativeCreation === undefined) {
        if (ordinaryCommand === undefined)
          return yield* new CheckoutError({ reason: "unavailable" });
        if (
          ordinaryCommand.commandId !== input.commandId ||
          ordinaryCommand.type !== input.commandType
        )
          return yield* new CheckoutError({ reason: "identity" });
        yield* checkoutIntent.value.replay(ordinaryCommand, input.threadId);
        const existing = yield* commandReceipts.getByCommandId(input.commandId);
        if (Option.isNone(existing)) {
          const batch = yield* CurrentProviderBatch;
          const transaction = yield* Effect.serviceOption(sql.transactionService);
          const preprepared = input.ordinaryCheckout?.prepared;
          // A batch can consume same-fiber, already captured SQL-only evidence.
          // Shared acceptance must instead own a fresh outer local transaction.
          if (
            (Option.isSome(transaction) || batch !== undefined) &&
            (batch === undefined ||
              preprepared === undefined ||
              input.ordinaryCheckout?.localAcceptance !== undefined)
          )
            return yield* new CheckoutError({ reason: "unavailable" });
          const plan: CheckoutIntent.IntentPlan = {
            command: ordinaryCommand,
            threadId: input.threadId,
            events: input.events,
            effects: input.effects,
            ...(input.ordinaryCheckout?.preparation === undefined
              ? {}
              : { preparation: input.ordinaryCheckout.preparation }),
            ...(input.ordinaryCheckout?.localAcceptance === undefined
              ? {}
              : { localAcceptance: input.ordinaryCheckout.localAcceptance }),
          };
          ordinary = preprepared ?? (yield* checkoutIntent.value.prepare(plan));
          yield* CheckoutCommit.validatePlan(ordinary, plan);
        }
      }

      const result = yield* commitThenPublish(
        Effect.gen(function* () {
          const legacy = input.legacyBootstrap;
          const privateDelete = legacy?.mutation.kind === "delete";
          if (
            privateDelete &&
            input.effects.some((effect) => effect.request.type === "terminal.cleanup")
          )
            return yield* new CheckoutError({ reason: "identity" });
          if (legacy !== undefined) {
            const previous = yield* legacyStorage.readLegacyReceipt(input.commandId);
            if (previous !== undefined) {
              yield* legacyStorage.assertBinding(legacy.binding);
              if (legacy.mutation.kind === "delete")
                yield* LegacyDecision.assertDeleteReplay(legacyStorage, legacy.mutation.command);
              else if (previous.domain !== "lifecycle")
                return yield* new LegacyStorageError({ reason: "legacy_command_domain_conflict" });
              return {
                receipt: previous.receipt,
                storedEvents: yield* legacyStorage.readLifecycleByCommandId(input.commandId),
                outcomes: [],
                committed: false as const,
                cancelledEffectIds: [],
              };
            }
          }
          if (legacy === undefined) yield* legacyStorage.assertUnreserved(input.commandId);
          else {
            yield* LegacyTransaction.validate(legacyStorage, legacy, input.events);
            if (
              legacy.mutation.kind === "release" &&
              legacy.mutation.policy.dispatchGuard !== undefined
            ) {
              const runId = legacy.mutation.policy.runId;
              const run = runId === undefined ? undefined : yield* legacyStorage.readRun(runId);
              if (run === undefined)
                return yield* new LegacyStorageError({ reason: "legacy_release_run_unavailable" });
              yield* validateLegacyGuard({
                type: "prepared-run.release",
                commandId: input.commandId,
                threadId: input.threadId,
                runId: run.run.id,
                dispatchGuard: legacy.mutation.policy.dispatchGuard,
                modelSelection: run.run.modelSelection,
              });
            }
            if (legacy.mutation.kind === "delete") {
              const projection = yield* LegacyDecision.withRetainedRecords(
                projectionStore,
                legacyStorage,
              ).getThreadProjection(input.threadId);
              const effects = yield* effectOutbox.listByThreadId(input.threadId);
              if (
                legacy.deletionTarget === undefined ||
                legacy.deletionTarget !== canonicalLegacyPayload({ projection, effects })
              )
                return yield* new LegacyStorageError({ reason: "legacy_delete_target_changed" });
            }
          }
          let originalRemoteSend: Effect.Success<ReturnType<typeof remoteSendReceipts.forCommit>>;
          if (input.remoteSend !== undefined) {
            const capture = input.remoteSend;
            if (
              input.commandType !== "message.dispatch" ||
              capture.commandId !== input.commandId ||
              capture.threadId !== input.threadId
            )
              return yield* new RemoteSendReceipts.RemoteSendReceiptError({
                reason: "acceptance_envelope_conflict",
              });
            originalRemoteSend = yield* remoteSendReceipts.forCommit(capture);
            const target = yield* projectionStore.getThread(input.threadId);
            yield* RemoteSendReceipts.assertTargetBirth(capture, target.createdAt);
          }
          yield* runtimeStops.assertIdentity(input.commandId, input.runtimeStop);
          if (input.runtimeStop !== undefined) {
            if (
              input.commandType !== "provider-session.detach" ||
              input.threadId !== input.runtimeStop.identity.request.threadId ||
              input.effects.length !== 1 ||
              input.effects[0]?.request.type !== "provider-session.detach" ||
              input.effects[0].request.runtimeStopCommandId !== input.commandId
            )
              return yield* new RuntimeStop.RuntimeStopError({
                reason: "stop_acceptance_envelope_conflict",
              });
            yield* input.runtimeStop.revalidate;
          }
          if (
            input.effects.some(
              (effect) =>
                effect.nativeCreationExecutionReference !== undefined &&
                (input.nativeCreation === undefined ||
                  effect.commandId !== input.commandId ||
                  effect.threadId !== input.threadId ||
                  effect.nativeCreationExecutionReference.claimId !==
                    input.nativeCreation.claimId ||
                  effect.nativeCreationExecutionReference.stageCommandId !== input.commandId ||
                  effect.nativeCreationExecutionReference.effectId !== effect.id),
            ) ||
            (input.nativeCreation !== undefined &&
              input.effects.some(
                (effect) =>
                  effect.request.type === "provider-turn.start" &&
                  effect.nativeCreationExecutionReference === undefined,
              ))
          )
            return yield* new NativeCreationRepository.NativeCreationRepositoryError({
              code: "unresolved_claim",
              message: "Native queued effect differs from its acceptance envelope.",
            });
          const choice = yield* importedHistoryCommit.reserve(input);
          const reservation = privateDelete
            ? undefined
            : yield* CheckoutCommit.reserveCommand(
                sql,
                commandReceipts,
                {
                  commandId: input.commandId,
                  threadId: input.threadId,
                  commandType: input.commandType,
                  acceptedAt: input.acceptedAt,
                  resultSequence: 0,
                  status: "accepted",
                  error: null,
                },
                ordinary,
              );
          if (!privateDelete && reservation === undefined) {
            // A raced new preparation must roll back its bridge, never acknowledge
            // a second local acceptance under somebody else's existing receipt.
            if (ordinary !== undefined) return yield* new CheckoutError({ reason: "replay" });
            if (input.remoteSend !== undefined && originalRemoteSend === undefined)
              return yield* new RemoteSendReceipts.RemoteSendReceiptError({
                reason: "accepted_receipt_unavailable",
              });
            const existing = yield* existingCommandResult(input.commandId);
            if (input.providerConversationReselection !== undefined)
              yield* input.providerConversationReselection.revalidate(input, existing);
            yield* importedHistoryCommit.replay(input, choice, existing.receipt);
            if (input.nativeCreation !== undefined) {
              const boundary = existing.storedEvents.at(-1);
              if (
                Option.isNone(nativeCreation) ||
                nativeCreation.value.recordExecutionAcceptance === undefined ||
                boundary === undefined ||
                existing.receipt.threadId !== input.threadId ||
                existing.receipt.commandType !== input.commandType ||
                existing.receipt.status !== "accepted"
              )
                return yield* new NativeCreationRepository.NativeCreationRepositoryError({
                  code: "unresolved_claim",
                  message: "Native receipt retry has no original native acceptance owner.",
                });
              yield* nativeCreation.value.recordExecutionAcceptance({
                ...input.nativeCreation,
                eventId: boundary.event.id,
                sequence: boundary.sequence,
              });
            }
            return { ...existing, committed: false as const, cancelledEffectIds: [] };
          }
          if (originalRemoteSend !== undefined)
            return yield* new RemoteSendReceipts.RemoteSendReceiptError({
              reason: "accepted_receipt_replay_required",
            });
          if (choice !== undefined && choice.rejectionReason !== null) {
            const receipt: CommandReceiptStore.CommandReceiptV2 = {
              commandId: input.commandId,
              threadId: input.threadId,
              commandType: input.commandType,
              acceptedAt: input.acceptedAt,
              resultSequence: 0,
              status: "rejected",
              error: choice.rejectionReason,
            };
            yield* commandReceipts.upsert(receipt);
            yield* importedHistoryCommit.reject(input, choice);
            return {
              receipt,
              storedEvents: [],
              outcomes: [],
              committed: true as const,
              cancelledEffectIds: [],
            };
          }
          if (input.selfSettlementRevalidate !== undefined) yield* input.selfSettlementRevalidate;

          // Planning does not lock the source. Once deletion commits, maintenance
          // can remove its checkpoints, so a new fork must not revive that history.
          if (input.commandType === "thread.fork") {
            for (const event of input.events) {
              if (event.type !== "thread.created" || event.payload.forkedFrom?.type !== "run")
                continue;
              const sourceId = event.payload.forkedFrom.threadId;
              const source = yield* sql`
                SELECT 1 FROM orchestration_v2_projection_threads
                WHERE thread_id = ${sourceId} AND deleted_at IS NULL
              `;
              if (source.length === 0) {
                const receipt: CommandReceiptStore.CommandReceiptV2 = {
                  commandId: input.commandId,
                  threadId: input.threadId,
                  commandType: input.commandType,
                  acceptedAt: input.acceptedAt,
                  resultSequence: yield* eventStore.latestSequence({ threadId: input.threadId }),
                  status: "rejected",
                  error: "Cannot fork a missing or deleted thread.",
                };
                yield* commandReceipts.upsert(receipt);
                return {
                  receipt,
                  storedEvents: [],
                  committed: false as const,
                  cancelledEffectIds: [],
                };
              }
            }
          }

          if (input.threadSelfConfiguration !== undefined)
            input = { ...input, events: yield* input.threadSelfConfiguration.revalidate() };
          if (input.codexFailover?.revalidate)
            input = { ...input, events: yield* input.codexFailover.revalidate(input) };
          if (input.providerConversationReselection !== undefined)
            yield* input.providerConversationReselection.revalidate(input);
          if (input.taskContinuation !== undefined) yield* input.taskContinuation.accept(input);
          yield* WorkspaceLifecycle.validateCommand(input.ordinaryCheckoutCommand, input.events);
          if (
            (input.commandType === "message.dispatch" || input.commandType === "thread.create") &&
            Option.isSome(startRefusals)
          )
            yield* startRefusals.value.admit(input.events);
          const runId = input.updateDelegatedCompletionOnlyRunId;
          const normalized = yield* normalizeEvents(
            runId === undefined
              ? input.events
              : yield* updateDelegatedCompletionOnly(input.events, (id) => id === runId),
          );
          if (ordinary !== undefined && Option.isSome(checkoutIntent)) {
            if (
              planDigests({ events: normalized, effects: input.effects }).authority !==
              planDigests(ordinary.plan).authority
            )
              return yield* new CheckoutError({ reason: "stale" });
            yield* checkoutIntent.value.validate(ordinary);
          }
          const storedEvents = yield* eventStore
            .append({
              commandId: input.commandId,
              events: normalized,
            })
            .pipe(Effect.map((stored) => stored.filter(isPublicStoredOrchestrationEvent)));
          // A Stop that changes nothing can still queue the stop of its delegated tasks.
          const sequence =
            storedEvents.at(-1)?.sequence ??
            (yield* eventStore.latestSequence({ threadId: input.threadId }));
          if (sequence === undefined) {
            return yield* Effect.die(
              new Error(`Command ${input.commandId} produced no orchestration events.`),
            );
          }
          const outcomes = yield* applyStoredEvents(storedEvents, {
            commandType: input.commandType,
          });
          if (
            input.resultRequest !== undefined &&
            ((input.commandType !== "message.dispatch" && input.commandType !== "thread.create") ||
              input.resultRequest.commandId !== input.commandId ||
              input.resultRequest.targetThreadId !== input.threadId ||
              !storedEvents.some(
                ({ event }) =>
                  event.type === "message.updated" &&
                  event.payload.id === input.resultRequest!.messageId,
              ))
          )
            return yield* new EventSinkWriteError({
              commandId: input.commandId,
              eventCount: storedEvents.length,
              cause: new Error("Result request does not belong to this accepted message."),
            });
          if (
            input.dependencyWake !== undefined &&
            ((input.commandType !== "message.dispatch" && input.commandType !== "thread.create") ||
              input.dependencyWake.waiterThreadId !== input.threadId ||
              !storedEvents.some(
                ({ event }) =>
                  event.type === "message.updated" &&
                  event.payload.id === input.dependencyWake!.messageId,
              ))
          )
            return yield* new EventSinkWriteError({
              commandId: input.commandId,
              eventCount: storedEvents.length,
              cause: new Error("Dependency wake does not belong to this accepted message."),
            });
          if (input.resultRequest !== undefined) yield* dependencies.open(input.resultRequest);
          if (input.dependencyWake !== undefined)
            yield* dependencies.admitWake(input.dependencyWake);
          // A completed child can accept Stop without producing another event. Resolve its
          // original child binding inside the same receipt transaction before releasing duties.
          if (
            storedEvents.length === 0 &&
            input.commandType === "subagent.stop" &&
            ordinaryCommand?.type === "subagent.stop" &&
            ordinaryCommand.commandId === input.commandId
          ) {
            const stopCommand = (yield* decodedCommand(ordinaryCommand)).command;
            if (stopCommand.type === "subagent.stop" && stopCommand.threadId === input.threadId) {
              const records = yield* projectionStore.getThreadRecords(input.threadId, [
                "subagents",
              ]);
              const childThreadId = records.subagents.find(
                (task) => task.id === stopCommand.subagentId,
              )?.childThreadId;
              if (childThreadId != null) yield* dependencies.release(childThreadId);
            }
          }
          yield* acceptDependencyEvents(
            dependencies,
            storedEvents.map((stored) => stored.event),
            input,
            false,
          );
          if (input.threadTitle !== undefined) yield* input.threadTitle.accept;
          if (input.codexFailover?.accept) yield* input.codexFailover.accept(input);
          if (input.selfSettlementProjectRemaining !== undefined)
            yield* recordSelfSettlementProjectRemaining(
              sql,
              tables,
              input,
              input.selfSettlementProjectRemaining,
            );
          if (input.remoteSend !== undefined) {
            const records = yield* projectionStore.getThreadRecords(
              input.threadId,
              ["runs", "turnItems"],
              { turnItemTypes: ["user_message"] },
            );
            let sent = yield* RemoteSendReceipts.acceptedResult(input.remoteSend, {
              events: storedEvents.map(({ event }) => event),
              ...records,
            });
            const dependency =
              input.resultRequest?.localDependency === undefined
                ? undefined
                : yield* dependencies.get(input.resultRequest.localDependency.dependencyId);
            if (
              input.remoteSend.dependency !== undefined &&
              (dependency === undefined ||
                input.remoteSend.dependency.dependencyId !== dependency.dependencyId ||
                input.remoteSend.dependency.requestId !== dependency.requestId ||
                input.remoteSend.dependency.state !== dependency.state)
            )
              return yield* new RemoteSendReceipts.RemoteSendReceiptError({
                reason: "accepted_dependency_binding_conflict",
              });
            if (dependency !== undefined)
              sent = {
                ...sent,
                dependency: {
                  dependencyId: dependency.dependencyId,
                  requestId: dependency.requestId,
                  state: dependency.state,
                },
              };
            yield* remoteSendReceipts.record(input.remoteSend, sent);
          }
          yield* enqueueEffects(
            input.effects,
            storedEvents.map((stored) => stored.event),
          );
          const receipt: CommandReceiptStore.CommandReceiptV2 = {
            commandId: input.commandId,
            threadId: input.threadId,
            commandType: input.commandType,
            acceptedAt: input.acceptedAt,
            resultSequence: sequence,
            status: "accepted",
            error: null,
          };
          if (privateDelete && ordinary === undefined)
            return yield* new CheckoutError({ reason: "unavailable" });
          const acceptance =
            legacy?.mutation.kind === "delete"
              ? yield* CheckoutCommit.reserveLegacyDeleteCommand(
                  sql,
                  legacy.binding,
                  receipt,
                  ordinary!,
                  { storedEvents, effects: input.effects },
                )
              : reservation;
          if (privateDelete && acceptance === undefined)
            return yield* new LegacyStorageError({
              reason: "legacy_delete_reservation_unavailable",
            });
          if (!privateDelete) {
            yield* commandReceipts.upsert(receipt);
            yield* captureSettlementProof(receipt, storedEvents);
          }
          if (legacy !== undefined)
            yield* LegacyTransaction.record(legacyStorage, legacy, receipt, storedEvents);
          if (
            legacy?.mutation.kind === "delete" &&
            legacy.mutation.command.type === "legacy-bootstrap.guard-rejection-delete" &&
            legacy.mutation.command.legacyOwnedControl !== undefined
          ) {
            const writer = yield* legacyStorage.forTransaction;
            yield* writer.createTerminalCleanup({
              effectId: `effect:${input.commandId}:terminal.cleanup`,
              commandId: input.commandId,
              threadId: input.threadId,
              control: legacy.mutation.command.legacyOwnedControl,
            });
          }
          if (input.idleWorktreeAdmission !== undefined)
            yield* idleWorktreeAdmission.accept({
              ...input,
              intent: input.idleWorktreeAdmission,
            });
          if (ordinary !== undefined && Option.isSome(checkoutIntent))
            yield* acceptance!.withAuthority(
              { receipt, storedEvents, effects: input.effects },
              checkoutIntent.value.accept(ordinary, {
                receipt,
                storedEvents,
                effects: input.effects,
              }),
            );
          yield* importedHistoryCommit.accept(input, choice);
          if (input.nativeCreation !== undefined) {
            const boundary = storedEvents.at(-1);
            if (
              Option.isNone(nativeCreation) ||
              nativeCreation.value.recordExecutionAcceptance === undefined ||
              input.nativeCreation.command.commandId !== input.commandId ||
              !("threadId" in input.nativeCreation.command) ||
              input.nativeCreation.command.threadId !== input.threadId ||
              input.nativeCreation.command.type !== input.commandType ||
              boundary === undefined
            )
              return yield* new NativeCreationRepository.NativeCreationRepositoryError({
                code: "unresolved_claim",
                message:
                  "Native acceptance owner is unavailable or differs from the accepted command.",
              });
            yield* nativeCreation.value.recordExecutionAcceptance({
              ...input.nativeCreation,
              eventId: boundary.event.id,
              sequence,
            });
          }
          if (input.runtimeStop !== undefined) yield* runtimeStops.record(input.runtimeStop);
          yield* ownerSelections.commit({
            threadId: input.threadId,
            commandId: input.commandId,
            sequence: receipt.resultSequence,
            events: input.events,
            record: input.ownerSelection,
            recordedAt: input.acceptedAt,
          });
          if (input.ownerSelection !== undefined)
            yield* codexFailoverStore.clearQueuedOverrides(
              input.threadId,
              DateTime.formatIso(input.acceptedAt),
            );

          if (input.settlementRestore !== undefined)
            yield* settlementRestores.recordRestore({
              threadId: input.threadId,
              settleCommandId: input.settlementRestore.settleCommandId,
              restoreCommandId: input.commandId,
              restoredAt: input.acceptedAt,
              sequence,
            });
          const cancelledEffectIds =
            input.cancelUnsettledEffects === undefined
              ? []
              : yield* effectOutbox.cancelUnsettled({
                  threadId: input.threadId,
                  ...input.cancelUnsettledEffects,
                });
          return { receipt, storedEvents, outcomes, committed: true as const, cancelledEffectIds };
        }),
        (result) =>
          Effect.gen(function* () {
            if (result.committed && result.receipt.status === "accepted")
              historicalTerminals.rememberCommitted(input.effects, result.storedEvents);
            // This publication callback runs only after the outer SQL commit.
            if (
              result.committed &&
              result.receipt.status === "accepted" &&
              input.legacyBootstrap?.mutation.kind === "delete" &&
              input.legacyBootstrap.mutation.command.type ===
                "legacy-bootstrap.guard-rejection-delete" &&
              input.legacyBootstrap.mutation.command.legacyOwnedControl !== undefined &&
              Option.isSome(legacyCleanup)
            )
              yield* legacyCleanup.value.wake;
            yield* effectOutbox.signalCancellations(result.cancelledEffectIds);
            if (
              result.committed &&
              result.receipt.status === "accepted" &&
              input.effects.length > 0
            ) {
              yield* effectOutbox.notifyAvailable(input.effects.length);
            }
            if (result.committed) {
              yield* publishStoredEvents(result.storedEvents, result.outcomes);
              yield* captureCommittedProvenance(result.storedEvents.map((stored) => stored.event));
            }
          }),
        ordinary?.plan.localAcceptance === undefined
          ? undefined
          : {
              acceptance: ordinary.plan.localAcceptance,
              commandId: input.commandId,
              authorityRevision: planDigests(ordinary.plan).authority,
            },
        ordinary !== undefined,
      );
      return {
        receipt: result.receipt,
        storedEvents: result.storedEvents,
        committed: result.committed,
        cancelledEffectCount: result.cancelledEffectIds.length,
      };
    });

    const commitLegacyPreflight: EventSinkV2Shape["commitLegacyPreflight"] = (input) =>
      commitThenPublish(
        Effect.gen(function* () {
          const writer = yield* legacyStorage.forTransaction;
          return yield* writer.commitPreflight(input);
        }),
        () => Effect.void,
      ).pipe(
        Effect.mapError(
          (cause) => new EventSinkWriteError({ commandId: input.commandId, eventCount: 1, cause }),
        ),
      );

    const commitRejectedCommandEffect = Effect.fn(
      "orchestrationV2.EventSink.commitRejectedCommand",
    )(function* (input: Parameters<EventSinkV2Shape["commitRejectedCommand"]>[0]) {
      const result = yield* commitThenPublish(
        Effect.gen(function* () {
          if (input.legacyGuardRejection === undefined)
            yield* legacyStorage.assertUnreserved(input.commandId);
          const existing = yield* commandReceipts.getByCommandId(input.commandId);
          if (Option.isSome(existing))
            return {
              receipt: existing.value,
              storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent>,
              outcomes: [] as ReadonlyArray<RunOutcome>,
            };
          const legacy = input.legacyGuardRejection;
          if (legacy !== undefined)
            yield* LegacyTransaction.validate(
              legacyStorage,
              legacy.context,
              legacy.events,
              "rejected",
            );
          let events: ReadonlyArray<OrchestrationV2DomainEvent> = [];
          if (legacy !== undefined) {
            const last = legacy.events.at(-1);
            const decision =
              last?.type === "run.updated" ? last.payload.legacyReleaseDecision : undefined;
            const policy = decision?.policy;
            const invalid = () =>
              new EventSinkWriteError({
                commandId: input.commandId,
                eventCount: legacy.events.length,
                cause: "Legacy guard rejection has no exact authenticated birth and current run.",
              });
            if (
              !isDispatchGuardRejected(legacy.rejection) ||
              legacy.rejection.observed === undefined ||
              input.commandType !== "prepared-run.release" ||
              last?.type !== "run.updated" ||
              decision === undefined ||
              policy === undefined ||
              input.commandId !== policy.releaseCommandId ||
              input.threadId !== policy.threadId ||
              last.threadId !== input.threadId ||
              last.runId !== policy.runId ||
              last.payload.id !== policy.runId ||
              last.payload.userMessageId !== policy.messageId ||
              last.id !== decision.evidenceEventId ||
              decision.reason !== legacy.rejection.reason ||
              canonicalLegacyPayload(decision.observed) !==
                canonicalLegacyPayload(legacy.rejection.observed) ||
              canonicalLegacyPayload(decision.guard) !==
                canonicalLegacyPayload(policy.dispatchGuard)
            )
              return yield* invalid();
            const collect = (commandId: CommandId) =>
              Stream.fromEffect(legacyStorage.readLifecycleByCommandId(commandId)).pipe(
                Stream.flatMap(Stream.fromIterable),
                Stream.filter(isPublicStoredOrchestrationEvent),
                Stream.runCollect,
                Effect.map((stored) => Array.from(stored)),
              );
            const proof = legacyBootstrapBirth({
              policy,
              claimEvents: yield* collect(policy.createCommandId),
              birthEvents: yield* collect(policy.birthCommandId),
            });
            const claim = yield* commandReceipts.getByCommandId(policy.createCommandId);
            const birth = yield* commandReceipts.getByCommandId(policy.birthCommandId);
            const projection = yield* LegacyDecision.withRetainedRecords(
              projectionStore,
              legacyStorage,
            ).getThreadRecords(input.threadId, ["runs", "attempts", "nodes", "turnItems"]);
            const current = projection.runs.find((run) => run.id === policy.runId);
            if (
              proof.type !== "valid" ||
              proof.claimEventId !== decision.claimEventId ||
              proof.claimSequence !== decision.claimSequence ||
              proof.birthEventId !== decision.birthEventId ||
              proof.sequence !== decision.birthSequence ||
              Option.isNone(claim) ||
              Option.isNone(birth) ||
              claim.value.status !== "accepted" ||
              birth.value.status !== "accepted" ||
              claim.value.threadId !== policy.threadId ||
              birth.value.threadId !== policy.threadId ||
              claim.value.commandType !==
                (policy.ownsNewThread ? "thread.create" : "thread.metadata.update") ||
              birth.value.commandType !== "message.dispatch" ||
              claim.value.resultSequence !== decision.claimReceiptSequence ||
              birth.value.resultSequence !== decision.birthReceiptSequence ||
              current?.status !== "preparing" ||
              current.legacyBootstrap === undefined ||
              !sameLegacyBootstrapPolicy(current.legacyBootstrap, policy) ||
              canonicalLegacyPayload(current) !==
                canonicalLegacyPayload({
                  ...last.payload,
                  legacyReleaseDecision: current.legacyReleaseDecision,
                  ...(policy.ownsNewThread
                    ? { status: current.status, completedAt: current.completedAt }
                    : {}),
                })
            )
              return yield* invalid();
            if (policy.ownsNewThread) {
              const [attemptEvent, nodeEvent, itemEvent] = legacy.events;
              const attempt = projection.attempts.find(
                (record) => record.id === current.activeAttemptId,
              );
              const node = projection.nodes.find((record) => record.id === current.rootNodeId);
              const item =
                itemEvent?.type === "turn-item.updated"
                  ? projection.turnItems.find(
                      (record) =>
                        record.id === itemEvent.payload.id &&
                        record.type === "command_execution" &&
                        record.input === "Preparing workspace" &&
                        record.runId === current.id,
                    )
                  : undefined;
              if (
                legacy.events.length !== 4 ||
                new Set(legacy.events.map((event) => event.id)).size !== 4 ||
                attemptEvent?.type !== "run-attempt.updated" ||
                nodeEvent?.type !== "node.updated" ||
                itemEvent?.type !== "turn-item.updated" ||
                itemEvent.payload.type !== "command_execution" ||
                attempt === undefined ||
                node === undefined ||
                item === undefined ||
                last.payload.status !== "failed" ||
                canonicalLegacyPayload(last.payload.completedAt) !==
                  canonicalLegacyPayload(input.rejectedAt) ||
                canonicalLegacyPayload(attemptEvent.payload) !==
                  canonicalLegacyPayload({
                    ...attempt,
                    status: "failed",
                    completedAt: input.rejectedAt,
                  }) ||
                canonicalLegacyPayload(nodeEvent.payload) !==
                  canonicalLegacyPayload({
                    ...node,
                    status: "failed",
                    completedAt: input.rejectedAt,
                  }) ||
                canonicalLegacyPayload(itemEvent.payload) !==
                  canonicalLegacyPayload({
                    ...item,
                    status: "failed",
                    title: "Dispatch guard rejected",
                    output: legacy.rejection.reason,
                    exitCode: undefined,
                    completedAt: input.rejectedAt,
                    updatedAt: input.rejectedAt,
                  }) ||
                legacy.events.some(
                  (event) =>
                    event.threadId !== policy.threadId ||
                    event.runId !== policy.runId ||
                    event.providerInstanceId !== current.providerInstanceId ||
                    canonicalLegacyPayload(event.occurredAt) !==
                      canonicalLegacyPayload(input.rejectedAt),
                )
              )
                return yield* invalid();
            } else if (legacy.events.length !== 1) return yield* invalid();
            events = legacy.events;
          }
          const reserved: CommandReceiptStore.CommandReceiptV2 = {
            commandId: input.commandId,
            threadId: input.threadId,
            commandType: input.commandType,
            acceptedAt: input.rejectedAt,
            resultSequence: 0,
            status: "rejected",
            error: input.error,
          };
          if (!(yield* commandReceipts.insertIfAbsent(reserved))) {
            return {
              receipt: (yield* existingCommandResult(input.commandId)).receipt,
              storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent>,
              outcomes: [] as ReadonlyArray<RunOutcome>,
            };
          }
          const storedEvents =
            events.length === 0
              ? []
              : (yield* eventStore.append({
                  commandId: input.commandId,
                  events,
                })).filter(isPublicStoredOrchestrationEvent);
          const outcomes = yield* applyStoredEvents(storedEvents);
          const receipt = {
            ...reserved,
            resultSequence:
              storedEvents.at(-1)?.sequence ??
              (yield* eventStore.latestSequence({ threadId: input.threadId })),
          };
          yield* commandReceipts.upsert(receipt);
          yield* captureSettlementProof(receipt, storedEvents);
          if (legacy !== undefined)
            yield* LegacyTransaction.record(legacyStorage, legacy.context, receipt, storedEvents);
          return { receipt, storedEvents, outcomes };
        }),
        (committed) => publishStoredEvents(committed.storedEvents, committed.outcomes),
      );
      return result.receipt;
    });

    const existingProjectReceipt = (commandId: CommandId) =>
      commandReceipts.getProjectByCommandId(commandId).pipe(
        Effect.flatMap(
          Option.match({
            onNone: () =>
              Effect.fail(`Command ${commandId} was already used by a thread command.` as const),
            onSome: Effect.succeed,
          }),
        ),
      );

    const commitProjectCommandEffect = Effect.fn("orchestrationV2.EventSink.commitProjectCommand")(
      function* (input: Parameters<EventSinkV2Shape["commitProjectCommand"]>[0]) {
        const result = yield* commitThenPublish(
          Effect.gen(function* () {
            yield* legacyStorage.assertUnreserved(input.commandId);
            const reserved: CommandReceiptStore.ProjectCommandReceiptV2 = {
              commandId: input.commandId,
              projectId: input.projectId,
              commandType: input.commandType,
              acceptedAt: input.acceptedAt,
              resultSequence: 0,
              status: "accepted",
              error: null,
            };
            if (!(yield* commandReceipts.insertIfAbsent(reserved))) {
              return { receipt: yield* existingProjectReceipt(input.commandId), event: undefined };
            }
            const event = yield* eventStore.appendProjectEvent(input.event);
            yield* projectStore.apply(event);
            const receipt = { ...reserved, resultSequence: event.sequence };
            yield* commandReceipts.upsert(receipt);
            return { receipt, event };
          }),
          (result) =>
            result.event === undefined ? Effect.void : eventStore.publishCommitted([result.event]),
        );
        return { receipt: result.receipt, committed: result.event !== undefined };
      },
    );

    const commitRejectedProjectCommandEffect = Effect.fn(
      "orchestrationV2.EventSink.commitRejectedProjectCommand",
    )(function* (input: Parameters<EventSinkV2Shape["commitRejectedProjectCommand"]>[0]) {
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* legacyStorage.assertUnreserved(input.commandId);
          const receipt: CommandReceiptStore.ProjectCommandReceiptV2 = {
            commandId: input.commandId,
            projectId: input.projectId,
            commandType: input.commandType,
            acceptedAt: input.rejectedAt,
            resultSequence: yield* eventStore.latestApplicationSequence,
            status: "rejected",
            error: input.error,
          };
          return (yield* commandReceipts.insertIfAbsent(receipt))
            ? receipt
            : yield* existingProjectReceipt(input.commandId);
        }),
      );
    });

    const catchUp = (input: {
      readonly afterSequence: number;
      readonly throughSequence: number;
      readonly threadId?: ThreadId;
      readonly eventType?: OrchestrationV2DomainEvent["type"];
    }): Stream.Stream<OrchestrationV2StoredEvent, unknown> => {
      const pageSize = 256;
      const loop = (afterSequence: number): Stream.Stream<OrchestrationV2StoredEvent, unknown> =>
        Stream.unwrap(
          eventStore
            .read({
              afterSequence,
              throughSequence: input.throughSequence,
              ...(input.threadId === undefined ? {} : { threadId: input.threadId }),
              ...(input.eventType === undefined ? {} : { eventType: input.eventType }),
              limit: pageSize,
            })
            .pipe(
              Stream.runCollect,
              Effect.map((chunk) => Array.from(chunk)),
              Effect.map((events) => {
                if (events.length === 0) {
                  return Stream.empty;
                }
                const current = Stream.fromIterable(
                  events.filter(isPublicStoredOrchestrationEvent),
                );
                const last = events.at(-1)?.sequence ?? input.throughSequence;
                return events.length < pageSize || last >= input.throughSequence
                  ? current
                  : Stream.concat(current, loop(last));
              }),
            ),
        );
      return loop(input.afterSequence);
    };

    const stream = (input?: Parameters<EventSinkV2Shape["stream"]>[0]) => {
      const afterSequence = input?.afterSequence ?? 0;
      const matches = (stored: OrchestrationV2StoredEvent) =>
        (input?.threadId === undefined || stored.event.threadId === input.threadId) &&
        (input?.eventType === undefined || stored.event.type === input.eventType);
      const replay = (throughSequence: number) =>
        catchUp({
          afterSequence,
          throughSequence,
          ...(input?.threadId === undefined ? {} : { threadId: input.threadId }),
          ...(input?.eventType === undefined ? {} : { eventType: input.eventType }),
        }).pipe(Stream.filter(matches));
      return Stream.unwrap(
        Effect.gen(function* () {
          let pubsub = liveEvents;
          if (input?.eventType !== undefined) {
            const existing = liveEventsByType.get(input.eventType);
            if (existing !== undefined) {
              pubsub = existing;
            } else {
              const created = yield* PubSub.unbounded<OrchestrationV2StoredEvent>();
              pubsub = liveEventsByType.get(input.eventType) ?? created;
              liveEventsByType.set(input.eventType, pubsub);
            }
          }
          if (input?.bounded === true) {
            return replayAndBufferProjectedLiveEvents({
              subscribe: PubSub.subscribe(pubsub),
              latestSequence: eventStore.latestSequence(),
              afterSequence,
              filter: matches,
              replay,
              project: projectStoredEventForWire,
            });
          }
          const subscription = yield* PubSub.subscribe(pubsub);
          const highWater = yield* eventStore.latestSequence();
          const live = Stream.fromSubscription(subscription).pipe(
            Stream.filter((stored) => stored.sequence > Math.max(highWater, afterSequence)),
            Stream.filter(matches),
          );
          return Stream.concat(replay(highWater), live);
        }),
      );
    };

    const commitThenPublish = Effect.fn("orchestrationV2.EventSink.commitThenPublish")(function* <
      A,
      E,
      R,
    >(
      transaction: Effect.Effect<A, E, R>,
      publish: (committed: A) => Effect.Effect<void>,
      local?: {
        readonly acceptance: import("../jones/ordinaryCheckout/CheckoutPreparation.ts").LocalAcceptance;
        readonly commandId: CommandId;
        readonly authorityRevision: string;
      },
      preserveFiber = false,
    ) {
      const batch = yield* CurrentProviderBatch;
      if (batch !== undefined) {
        if (local !== undefined) return yield* new CheckoutError({ reason: "unavailable" });
        const committed = yield* sql.withTransaction(
          transaction.pipe(Effect.provideService(SqlClient.SqlClient, sql)),
        );
        batch.publish.push(publish(committed));
        return committed;
      }
      // Only our batch owns a deferred publication lane for an existing transaction.
      // A savepoint cannot acknowledge a command before somebody else's outer COMMIT.
      if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
        return yield* new CheckoutError({ reason: "unavailable" });
      if (preserveFiber) {
        yield* coordinator.barrier(Effect.void);
        return yield* commitThenPublishNow(transaction, publish, local);
      }
      return yield* coordinator.barrier(commitThenPublishNow(transaction, publish, local));
    });

    const withProviderBatch: EventSinkV2Shape["withProviderBatch"] = Effect.fn(
      "orchestrationV2.EventSink.providerBatch",
    )(function* <A, E, R>(effect: Effect.Effect<A, E, R>) {
      if ((yield* CurrentProviderBatch) !== undefined) return yield* effect;
      if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
        return yield* new EventSinkWriteError({
          eventCount: 0,
          cause: new CheckoutError({ reason: "unavailable" }),
        });
      const batch: ProviderBatchPublication = { publish: [], eventCount: 0 };
      let started = false;
      const result = yield* commitThenPublishNow(
        Effect.sync(() => {
          started = true;
        }).pipe(Effect.andThen(Effect.provideService(effect, CurrentProviderBatch, batch))),
        () => Effect.forEach(batch.publish, (publish) => publish, { discard: true }),
      ).pipe(
        // Normalization mutates run-local state, so only retry before its transaction body starts.
        Effect.retry({
          while: (cause) =>
            !started && SqlError.isSqlError(cause) && cause.reason._tag === "LockTimeoutError",
          times: 2,
        }),
        Effect.mapError((cause) =>
          SqlError.isSqlError(cause) || isSharedAdmissionError(cause)
            ? new EventSinkWriteError({ eventCount: batch.eventCount, cause })
            : cause,
        ),
      );
      yield* Effect.annotateCurrentSpan({
        "orchestration_v2.event_count": batch.eventCount,
        "orchestration_v2.batch_writes": batch.publish.length,
      });
      return result;
    });

    const coordinator = yield* ProviderCommitBatch.make(
      (work) =>
        withProviderBatch(work).pipe(
          Effect.mapError((cause) =>
            ProviderCommitBatch.isCommitBatchError(cause)
              ? cause
              : new ProviderCommitBatch.CommitBatchError({ cause }),
          ),
        ),
      yield* ProviderCommitBatch.enabled,
      // Each snapshot owns a savepoint and its deferred publications within the shared commit.
      Effect.fnUntraced(function* (
        work: Effect.Effect<void, ProviderCommitBatch.CommitBatchError>,
      ) {
        const batch = yield* CurrentProviderBatch;
        const publicationCount = batch?.publish.length ?? 0;
        const eventCount = batch?.eventCount ?? 0;
        return yield* sql.withTransaction(work).pipe(
          Effect.onError(() =>
            Effect.sync(() => {
              if (batch !== undefined) {
                batch.publish.length = publicationCount;
                batch.eventCount = eventCount;
              }
            }),
          ),
          Effect.mapError((cause) =>
            ProviderCommitBatch.isCommitBatchError(cause)
              ? cause
              : new ProviderCommitBatch.CommitBatchError({ cause }),
          ),
        );
      }),
    );

    const eventSinkService: EventSinkV2Shape = EventSinkV2.of({
      idleWorktreeAdmission,
      supersededTerminalTrace,
      writeHistoricalTerminal: (input) => {
        const attempt = commitThenPublish(
          Effect.gen(function* () {
            const committed = yield* historicalTerminals.readCommitted(input);
            if (committed === "unavailable")
              return yield* new EventSinkWriteError({ eventCount: 1 });
            // Return durable proof for the exact-object terminal drain without
            // publishing the original writer's committed event a second time.
            if (committed !== undefined)
              return {
                handled: true,
                persisted: true,
                deferred: false,
                readback: true,
                storedEvents: [committed].filter(isPublicStoredOrchestrationEvent),
                outcomes: [],
              };
            const selected = yield* historicalTerminals.select(input);
            if (selected === "ordinary")
              return { handled: false, deferred: false, storedEvents: [], outcomes: [] };
            if (HistoricalTerminalHistory.isHistoricalTerminalRejection(selected)) {
              yield* SupersededTerminalTrace.recordTerminalRejection(
                supersededTerminalTrace,
                input.event,
                selected.reason,
              );
              return {
                handled: true,
                deferred: selected.type === "deferred",
                storedEvents: [],
                outcomes: [],
              };
            }
            const storedEvents = (yield* eventStore.append({ events: [selected] })).filter(
              isPublicStoredOrchestrationEvent,
            );
            const outcomes = yield* applyStoredEvents(storedEvents);
            const key = SupersededTerminalTrace.terminalKey(input.event);
            if (key !== undefined) supersededTerminalTrace.clear(key);
            return { handled: true, persisted: true, deferred: false, storedEvents, outcomes };
          }),
          (result) =>
            "readback" in result
              ? Effect.void
              : publishStoredEvents(result.storedEvents, result.outcomes),
        );
        // Even a no-write commit traverses the publication lane. Retry the DB
        // admission once after an earlier commit has installed its locator.
        return attempt.pipe(
          Effect.filterOrElse(
            (result) => !result.deferred,
            () => attempt,
          ),
          Effect.tapError(() =>
            SupersededTerminalTrace.recordTerminalRejection(
              supersededTerminalTrace,
              input.event,
              "write_error",
            ),
          ),
          Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 1, cause })),
        );
      },

      readDeletionWorktreeRemovalStartForTarget: (threadId, target) =>
        deletionAdmission.readTarget(threadId, target).pipe(Effect.mapError(stopError)),
      startDeletionWorktreeRemoval: (input) =>
        deletionAdmission.start(input).pipe(Effect.mapError(stopError)),
      readDeletionWorktreeRemovalStart: (effectId) =>
        deletionAdmission.read(effectId).pipe(Effect.mapError(stopError)),
      revalidateDeletionWorktreeRemovalStart: (start, ordinal, rules, live) =>
        deletionAdmission.revalidate(start, ordinal, rules, live).pipe(Effect.mapError(stopError)),
      checkRetainedDeletionWorktreeRemovalStart: (start, ordinal, rules, live) =>
        deletionAdmission
          .checkRetained(start, ordinal, rules, live)
          .pipe(Effect.mapError(stopError)),
      qualifyDeletionWorktreeRemovalObservation: (observation) =>
        deletionAdmission.qualify(observation).pipe(Effect.mapError(stopError)),
      readStoppedRuntime: (provider) =>
        runtimeStops.readStoppedRuntime(provider).pipe(Effect.mapError(stopError)),
      hasStoppedRuntime: (binding) =>
        runtimeStops.hasStoppedRuntime(binding).pipe(Effect.mapError(stopError)),
      assertRuntimeStopStartAllowed: (input) =>
        runtimeStops.assertStartAllowed(input).pipe(Effect.mapError(stopError)),
      reviewImportedHistory: (input) =>
        sql
          .withTransaction(
            ImportedChoice.reviewImportedHistory(input).pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
            ),
          )
          .pipe(
            Effect.map((facts) => facts.review),
            Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause })),
          ),
      readImportedHistoryChoice: (input) =>
        sql
          .withTransaction(
            ImportedChoice.readImportedHistoryChoice(input).pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
            ),
          )
          .pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      assertImportedHistoryStartAllowed: (input) =>
        sql
          .withTransaction(
            ImportedChoice.claimImportedHistoryStart(input, tables).pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
            ),
          )
          .pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      readRuntimeStop: (commandId) =>
        runtimeStops.readState(commandId).pipe(Effect.mapError(stopError)),
      startRuntimeStop: (commandId, target, revalidate) =>
        runtimeStops.start(commandId, target, revalidate).pipe(Effect.mapError(stopError)),
      completeRuntimeStop: (commandId, result) =>
        runtimeStops.complete(commandId, result).pipe(Effect.mapError(stopError)),
      consumeProviderEvents: coordinator.consume,
      withProviderBatch,
      write: (input) =>
        writeEffect({ ...input, effects: [] }).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({
                eventCount: input.events.length,
                ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
                cause,
              }),
          ),
        ),
      writeWithEffects: (input) =>
        writeEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({
                eventCount: input.events.length,
                ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
                cause,
              }),
          ),
        ),
      writeIfRunCurrent: (input) =>
        writeIfRunCurrentEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({
                eventCount: input.events.length,
                ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
                cause,
              }),
          ),
        ),
      writeIfProviderThreadOwner: (input) =>
        writeIfProviderThreadOwnerEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({
                eventCount: input.events.length,
                ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
                cause,
              }),
          ),
        ),
      writeIfProviderThreadUnchanged: (input) =>
        writeIfProviderThreadUnchangedEffect(input).pipe(
          Effect.mapError(
            (cause) => new EventSinkWriteError({ eventCount: input.events.length, cause }),
          ),
        ),
      writeIfTurnItemUnchanged: (input) =>
        writeIfTurnItemUnchangedEffect(input).pipe(
          Effect.mapError(
            (cause) => new EventSinkWriteError({ eventCount: input.events.length, cause }),
          ),
        ),
      writeIfSubagentUnchanged: (input) =>
        writeIfSubagentUnchangedEffect(input).pipe(
          Effect.mapError(
            (cause) => new EventSinkWriteError({ eventCount: input.events.length, cause }),
          ),
        ),
      commitLegacyPreflight,
      readLegacyPreflight: (commandId) =>
        legacyStorage.readLegacyPreflight(commandId).pipe(
          Effect.map(Option.fromUndefinedOr),
          Effect.mapError((cause) => new EventSinkWriteError({ commandId, eventCount: 0, cause })),
        ),
      readLifecycleByCommandId: ({ commandId }) =>
        Stream.fromEffect(legacyStorage.readLifecycleByCommandId(commandId)).pipe(
          Stream.flatMap(Stream.fromIterable),
          Stream.mapError((cause) => new EventSinkWriteError({ commandId, eventCount: 0, cause })),
        ),
      commitCommand: (input) =>
        commitCommandEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({
                commandId: input.commandId,
                eventCount: input.events.length,
                cause,
              }),
          ),
        ),
      commitRejectedCommand: (input) =>
        commitRejectedCommandEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({
                commandId: input.commandId,
                eventCount: 0,
                cause,
              }),
          ),
        ),
      commitProjectCommand: (input) =>
        commitProjectCommandEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({ commandId: input.commandId, eventCount: 1, cause }),
          ),
        ),
      commitRejectedProjectCommand: (input) =>
        commitRejectedProjectCommandEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({ commandId: input.commandId, eventCount: 0, cause }),
          ),
        ),
      stream: (input) =>
        stream(input).pipe(
          Stream.mapError(
            (cause) =>
              new EventSinkStreamError({
                ...(input?.threadId === undefined ? {} : { threadId: input.threadId }),
                ...(input?.afterSequence === undefined
                  ? {}
                  : { afterSequence: input.afterSequence }),
                cause,
              }),
          ),
        ),
      latestSequence: (input) =>
        eventStore.latestSequence(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkStreamError({
                ...(input?.threadId === undefined ? {} : { threadId: input.threadId }),
                cause,
              }),
          ),
        ),
      canPromoteQueuedAtToolBoundary: Effect.fn("EventSink.canPromoteQueuedAtToolBoundary")(
        (input) =>
          Effect.gen(function* () {
            if (input.boundary.event.type !== "node.updated") return false;
            const node = input.boundary.event.payload;
            if (
              input.boundary.event.threadId !== input.threadId ||
              node.runId !== input.activeRunId ||
              node.kind !== "tool_call" ||
              node.status !== "completed" ||
              node.nativeItemRef?.strength !== "strong" ||
              node.nativeItemRef.nativeId == null
            )
              return false;
            const persisted = yield* sql`SELECT 1 FROM orchestration_events
              WHERE sequence = ${input.boundary.sequence} AND event_id = ${input.boundary.event.id}
                AND application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ${input.threadId}
                AND event_type = 'node.updated' AND json_extract(payload_json, '$.id') = ${node.id}
                AND json_extract(payload_json, '$.runId') = ${input.activeRunId}
                AND json_extract(payload_json, '$.providerTurnId') = ${node.providerTurnId}
                AND json_extract(payload_json, '$.providerThreadId') = ${node.providerThreadId}
                AND json_extract(payload_json, '$.rootNodeId') = ${node.rootNodeId}
                AND json_extract(payload_json, '$.kind') = 'tool_call'
                AND json_extract(payload_json, '$.nativeItemRef.strength') = 'strong'
                AND json_extract(payload_json, '$.nativeItemRef.nativeId') = ${node.nativeItemRef.nativeId}
                AND json_extract(payload_json, '$.status') = 'completed'`;
            if (persisted.length !== 1) return false;
            const births = input.births ?? new Map();
            for (const runId of [input.activeRunId, input.queuedRunId]) {
              if (births.has(runId)) continue;
              const rows = yield* sql<{
                sequence: number;
                eligible: number;
                command_id: CommandId | null;
              }>`SELECT sequence, command_id,
              CASE WHEN json_type(payload_json, '$.queuedToolBoundaryEligible') = 'true' THEN 1 ELSE 0 END AS eligible FROM orchestration_events
            WHERE application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ${input.threadId}
              AND event_type = 'run.created' AND json_extract(payload_json, '$.id') = ${runId}
            ORDER BY sequence DESC LIMIT 1`;
              if (rows[0] === undefined) {
                births.set(runId, null);
                continue;
              }
              const receipt =
                rows[0].command_id === null
                  ? Option.none()
                  : yield* commandReceipts.getByCommandId(rows[0].command_id);
              if (
                Option.isNone(receipt) ||
                receipt.value.status !== "accepted" ||
                receipt.value.threadId !== input.threadId ||
                receipt.value.resultSequence < rows[0].sequence
              ) {
                births.set(runId, null);
                continue;
              }
              const settings = yield* sql<{ runtime_mode: string; interaction_mode: string }>`SELECT
            json_extract(payload_json, '$.runtimeMode') AS runtime_mode, json_extract(payload_json, '$.interactionMode') AS interaction_mode
            FROM orchestration_events WHERE application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ${input.threadId}
              AND event_type LIKE 'thread.%' AND sequence < ${rows[0].sequence} AND json_type(payload_json, '$.runtimeMode') = 'text'
            ORDER BY sequence DESC LIMIT 1`;
              births.set(
                runId,
                settings[0] === undefined
                  ? null
                  : {
                      sequence: rows[0].sequence,
                      queuedToolBoundaryEligible: rows[0].eligible === 1,
                      runtimeMode: settings[0].runtime_mode,
                      interactionMode: settings[0].interaction_mode,
                    },
              );
            }
            const queued = births.get(input.queuedRunId);
            const active = births.get(input.activeRunId);
            if (
              queued == null ||
              queued.queuedToolBoundaryEligible !== true ||
              active == null ||
              queued.sequence >= input.boundary.sequence ||
              queued.runtimeMode !== input.runtimeMode ||
              queued.interactionMode !== input.interactionMode ||
              active.runtimeMode !== input.runtimeMode ||
              active.interactionMode !== input.interactionMode
            )
              return false;
            const updates = yield* sql<{
              sequence: number;
            }>`SELECT sequence FROM orchestration_events
          WHERE application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ${input.threadId}
            AND sequence > ${active.sequence} AND event_type IN ('message.created', 'message.updated') AND json_extract(payload_json, '$.id') = ${input.messageId}
          ORDER BY sequence DESC LIMIT 1`;
            if (updates[0] === undefined || updates[0].sequence >= input.boundary.sequence)
              return false;
            const completions = yield* sql<{
              sequence: number;
            }>`SELECT sequence FROM orchestration_events
          WHERE application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ${input.threadId}
            AND sequence > ${active.sequence} AND event_type = 'node.updated' AND json_extract(payload_json, '$.id') = ${node.id}
            AND json_extract(payload_json, '$.providerTurnId') = ${node.providerTurnId}
            AND json_extract(payload_json, '$.status') = 'completed' ORDER BY sequence ASC LIMIT 1`;
            if (completions[0]?.sequence !== input.boundary.sequence) return false;
            const fences = yield* sql`SELECT 1 FROM orchestration_v2_projection_runs
          WHERE thread_id = ${input.threadId} AND run_id = ${input.queuedRunId}
            AND json_extract(payload_json, '$.queueHeld') = 1 LIMIT 1`;
            return fences.length === 0;
          }).pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      ),
      readByCommandId: (input) =>
        eventStore.readByCommandId(input).pipe(
          Stream.filter(isPublicStoredOrchestrationEvent),
          Stream.mapError(
            (cause) =>
              new EventSinkStreamError({
                cause,
              }),
          ),
        ),
    } satisfies EventSinkV2Shape);
    return eventSinkService;
  }),
);

/** Share the worker's outbox instance: enqueue wakeups are in-memory. */
export const layerFromStores = layerBase;

export const layer: Layer.Layer<
  EventSinkV2,
  never,
  EventStore.EventStoreV2 | ProjectionStore.ProjectionStoreV2 | SqlClient.SqlClient
> = layerBase.pipe(
  Layer.provide(
    Layer.mergeAll(
      CommandReceiptStore.layer,
      EffectOutbox.layer,
      ProjectStore.layer,
      TurnItemPositionStore.layer,
    ),
  ),
);
