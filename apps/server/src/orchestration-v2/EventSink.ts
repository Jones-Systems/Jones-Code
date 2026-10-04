import {
  AgentSessionImportSource,
  CommandId,
  EventId,
  MessageId,
  ModelSelection,
  RuntimeMode,
  ProviderInteractionMode,
  OrchestrationV2ImportedHistoryDelivery,
  OrchestrationV2ImportedHistoryReviewBasis,
  OrchestrationV2ImportedHistoryReviewResult,
  OrchestrationV2StartWithImportedHistoryCommand,
  OrchestrationV2ImportedHistoryStartReceipt,
  OrchestrationV2ThreadDeletionWorktreeRemoval,
  OrchestrationV2ThreadDeletionCleanupObservation,
  WorktreeCleanupRules,
  WorktreeOwnershipConflictError,
  AuthSessionId,
  NativeCommandIdentityV2,
  NativeThreadIncarnationV2,
  OrchestrationV2Command,
  OrchestrationV2ProviderSessionJson,
  OrchestrationV2ProviderTurnJson,
  OrchestrationV2ExecutionNodeJson,
  OrchestrationV2ConversationMessageJson,
  OrchestrationV2TurnItemJson,
  OrchestrationV2RunJson,
  TrustedT3PlacementEnvironment,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2AppThread,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  type OrchestrationV2Run,
  OrchestrationV2DomainEvent,
  OrchestrationV2DomainEventJson,
  OrchestrationV2StoredEvent,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  RuntimeRequestId,
  NodeId,
  ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import * as NodePath from "node:path";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlError from "effect/unstable/sql/SqlError";

import { replayAndBufferProjectedLiveEvents } from "./LiveStreamBudget.ts";
import type { OrchestrationCommandEventMetadata, UnsequencedProjectEvent } from "../persistence/Services/OrchestrationEventStore.ts";
import { projectDomainEventForWire } from "./WireProjection.ts";

import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as TurnItemPositionStore from "./TurnItemPositionStore.ts";
import * as OrdinaryCheckout from "./OrdinaryCheckoutOwnership.ts";
import { ProviderRuntimeBinding, ProviderRuntimeObservation, ProviderContinuationSourceIdentity, ProviderNativeOperationContext, ProviderNativeEffectEvidence } from "./ProviderAdapter.ts";
import { LegacyProviderContinuationEvidenceV1 } from "../persistence/ProviderSessionRuntime.ts";
import type { ProviderContinuationQualification } from "./ProviderContinuationQualification.ts";
import { nativeCreationCanonicalJson, nativeCreationSha256 } from "./NativeCreationPreparation.ts";
import { authorizeNativeCreationThreadRecovery, getNativeCreationThreadRecoveryReference,
  type NativeCreationThreadRecoveryContextV2 } from "./NativeCreationAuthority.ts";
import { queuedRunsInDeliveryOrder } from "./QueuedRunOrder.ts";
import * as NativeCreationRepository from "../persistence/Services/NativeCreationRepository.ts";
import { layer as NativeCreationRepositoryLayer } from "../persistence/Layers/NativeCreationRepository.ts";
import type { ProviderHandoffDeliveryDescriptor } from "./ProviderAdapterRegistry.ts";
import type { WorktreeOwnershipLease } from "./WorktreeOwnershipLease.ts";
import type { DeletionWorktreeRemovalObservationV1 } from "../git/DeletionWorktreeRemoval.ts";
import type { ProviderPinnedRuntimeStopResultV1 } from "./ProviderSessionManager.ts";
import type { ApplicationThreadBirthV2, CurrentThreadRuntimeStopIntentV2, CurrentThreadRuntimeStopCommitContextV2,
  QueuedRunContinuationBasisV2, QueuedRunExecutionIntentV2, QueuedRunRuntimeStopFenceV2,
  QueuedRunStartReservationInputV2, QueuedRunStartReservationResultV2, ImportedHistoryStartExecutionReferenceV2 } from "./Orchestrator.ts";


/**
 * ERRORS
 */
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

export class NativeCommandPreconditionError extends Schema.TaggedError<NativeCommandPreconditionError>()(
  "NativeCommandPreconditionError", {
    commandId: CommandId,
    reason: Schema.Literals(["identity_conflict", "unbound_receipt", "missing_target", "stale_target", "unknown_evidence", "authority_changed"]),
  },
) {}

export const EventSinkV2Error = Schema.Union([EventSinkWriteError, EventSinkStreamError, NativeCommandPreconditionError]);
export type EventSinkV2Error = typeof EventSinkV2Error.Type;

export interface OrdinaryCheckoutSqlCaptureV1 extends OrdinaryCheckout.OrdinaryCheckoutOwnershipTransactionContractV1 {
  readonly source: { readonly projectWorkspaceRoot: string; readonly worktreePath: string | null };
}
export interface OrdinaryCheckoutCommitContextV1 {
  readonly command: OrchestrationV2Command;
  /** SQL-only capture after actual projection writes; failures roll back the entire acceptance. */
  readonly captureAfterProjection: (storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>) => Effect.Effect<
    ReadonlyArray<OrdinaryCheckoutSqlCaptureV1>, unknown>;
}
export type OrdinaryCheckoutCommitErrorV1 = EventSinkV2Error | WorktreeOwnershipConflictError | OrdinaryCheckout.OrdinaryCheckoutOwnershipError;

export const OrdinaryCheckoutUseSubjectV1 = Schema.Struct({
  schema: Schema.Literal("t3.ordinary-checkout-use/v1"), use: OrdinaryCheckout.OrdinaryCheckoutUseV1,
  source: Schema.Struct({ projectWorkspaceRoot: Schema.NonEmptyString, worktreePath: Schema.NullOr(Schema.NonEmptyString) }),
});
export type OrdinaryCheckoutUseSubjectV1 = typeof OrdinaryCheckoutUseSubjectV1.Type;
export interface OrdinaryCheckoutUseRecordV1 {
  readonly subject: OrdinaryCheckoutUseSubjectV1;
  readonly state: "reserved" | "started" | "unknown" | "completed" | "no_effect" | "released";
  readonly startedAt: string | null;
}
export interface OrdinaryCheckoutUseInputV1 {
  readonly operationId: string;
  readonly admission: OrdinaryCheckout.OrdinaryCheckoutAdmissionRefV1;
  readonly source: OrdinaryCheckout.OrdinaryCheckoutUseSourceV1;
  readonly targetSource: OrdinaryCheckoutSqlCaptureV1["source"];
}

export interface NativeCommandAuthorityReadV2 {
  readonly actorSessionId?: AuthSessionId;
  readonly claimId?: string;
  readonly projectId?: ProjectId;
  readonly resourcePaths?: ReadonlyArray<string>;
}
export interface NativeCommandTargetSnapshotV2 {
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly targetEventSequence: number;
  readonly incarnation: NativeThreadIncarnationV2 | null;
  readonly creationProvenance: "native_created" | "legacy_import" | "unavailable";
  readonly records: Readonly<Record<string, ReadonlyArray<Readonly<Record<string, unknown>>>>>;
  readonly authority: NativeCommandAuthorityReadV2;
  readonly authorityRecords: Readonly<Record<string, ReadonlyArray<Readonly<Record<string, unknown>>>>>;
}
export interface NativeCommandFactsV2 {
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly receipt: CommandReceiptStore.CommandReceiptV2 | null;
  readonly identity: NativeCommandIdentityV2 | null;
  readonly events: ReadonlyArray<OrchestrationV2StoredEvent>;
  readonly eventMetadata: ReadonlyArray<OrchestrationCommandEventMetadata>;
  readonly eventMetadataOverflow: boolean;
  readonly snapshotSequence: number;
  readonly targetEventSequence: number;
  readonly incarnation: NativeThreadIncarnationV2 | null;
  readonly creationProvenance: NativeCommandTargetSnapshotV2["creationProvenance"];
  readonly projection: OrchestrationV2ThreadProjection | null;
  readonly commitSnapshot: NativeCommandTargetSnapshotV2;
  readonly creationHistory: ReadonlyArray<Readonly<Record<string, unknown>>>;
  readonly nativeCreationHistory: NativeCreationRepository.NativeCreationBoundedHistoryV2 | null;
  readonly workstreamWitness: NativeWorkstreamSettlementWitnessV2 | null;
}
export interface NativeCommandCommitContextV2 {
  readonly identity: NativeCommandIdentityV2;
  readonly snapshot: NativeCommandTargetSnapshotV2;
  /** Current AUTH resolver/qualified-store proof must be checked inside acceptance. */
  readonly revalidateAuthority: Effect.Effect<void, unknown>;
  readonly workstreamWitness?: NativeWorkstreamSettlementWitnessV2;
}
const LowerSha256 = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
const ProviderBindingExpectationSchemaV2 = Schema.Struct({ threadId: ThreadId, providerThreadId: ProviderThreadId,
  providerSessionId: ProviderSessionId, instanceId: ProviderInstanceId, driver: ProviderDriverKind,
  nativeThreadId: Schema.NullOr(Schema.String), runtimeGeneration: Schema.NullOr(Schema.NonEmptyString) });
export const NativeWorkstreamSettlementWitnessV2 = Schema.Struct({
  version: Schema.Literal(2), command: OrchestrationV2Command,
  attemptKey: Schema.Struct({ owner_id: Schema.NonEmptyString, principal_id: Schema.NonEmptyString, command_id: Schema.NonEmptyString }),
  dispatchStartedAt: Schema.NonEmptyString, actorSessionId: AuthSessionId,
  enrollmentSha256: LowerSha256, requestBytesSha256: LowerSha256, authority: TrustedT3PlacementEnvironment,
  incarnation: NativeThreadIncarnationV2, targetEventSequence: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  provider: Schema.NullOr(Schema.Struct({ binding: ProviderBindingExpectationSchemaV2,
    evidenceRevision: Schema.Int.check(Schema.isGreaterThan(0)) })),
});
export type NativeWorkstreamSettlementWitnessV2 = typeof NativeWorkstreamSettlementWitnessV2.Type;
export const nativeWorkstreamSettlementWitnessBindingDigestV2 = (witness: NativeWorkstreamSettlementWitnessV2): string =>
  nativeCreationSha256(nativeCreationCanonicalJson({ schema: "t3.workstream-settlement-binding/v2",
    witness: Schema.encodeSync(NativeWorkstreamSettlementWitnessV2)(witness) }));
export const queuedRunContinuationBasisDigestV2 = (basis: Omit<QueuedRunContinuationBasisV2, "basisDigest"> | QueuedRunContinuationBasisV2): string => {
  const { basisDigest: _ignored, ...fields } = basis as QueuedRunContinuationBasisV2;
  return nativeCreationSha256(nativeCreationCanonicalJson(fields));
};
const ApplicationBirthSchemaV2 = Schema.Struct({ kind: Schema.Literal("application_v2_thread_birth"),
  threadId: ThreadId, eventId: EventId, sequence: Schema.Int.check(Schema.isGreaterThan(0)) });
const ExecutionIntentFieldsV2 = { commandId: CommandId, runId: RunId, runAttemptId: RunAttemptId, effectId: Schema.NonEmptyString };
const ExecutionIntentSchemaV2 = Schema.Union([
  Schema.Struct({ ...ExecutionIntentFieldsV2, kind: Schema.Literal("queued"), reviewedBasis: Schema.Null }),
  Schema.Struct({ ...ExecutionIntentFieldsV2, kind: Schema.Literal("imported_history_choice"), reviewedBasis: OrchestrationV2ImportedHistoryReviewBasis }),
]);
const SwitchPlanSchemaV2 = Schema.Struct({ instanceChanged: Schema.Boolean, modelChanged: Schema.Boolean,
  targetProviderThreadId: Schema.NullOr(ProviderThreadId), releaseProviderSessionIds: Schema.Array(ProviderSessionId),
  transition: Schema.Union([Schema.Struct({ type: Schema.Literals(["reuse", "switch_model_in_session", "restart_and_resume", "create_with_handoff"]) }),
    Schema.Struct({ type: Schema.Literal("reject"), reason: Schema.String })]) });
const RegisteredSourceSchemaV2 = Schema.Struct({ threadId: ThreadId, providerThreadId: ProviderThreadId,
  providerSessionId: ProviderSessionId, instanceId: ProviderInstanceId, driver: ProviderDriverKind,
  nativeThreadId: Schema.NullOr(Schema.String), runtimeGeneration: Schema.NonEmptyString });
const BasisFieldsV2 = { runId: RunId, messageId: MessageId, queuedProviderThreadId: Schema.NullOr(ProviderThreadId),
  runAttemptId: Schema.NullOr(RunAttemptId), executionIntent: Schema.NullOr(ExecutionIntentSchemaV2),
  switchPlan: Schema.NullOr(SwitchPlanSchemaV2), basisDigest: LowerSha256 };
const ContinuationBasisSchemaV2 = Schema.Union([
  Schema.Struct({ ...BasisFieldsV2, sourceMode: Schema.Literals(["queued_thread", "active_native_copy"]),
    sourceBinding: RegisteredSourceSchemaV2, sourceEvidenceRevision: Schema.Int.check(Schema.isGreaterThan(0)) }),
  Schema.Struct({ ...BasisFieldsV2, sourceMode: Schema.Literal("new_context"), sourceBinding: Schema.Null, sourceEvidenceRevision: Schema.Null }),
  Schema.Struct({ ...BasisFieldsV2, sourceMode: Schema.Literal("unknown"), sourceBinding: Schema.NullOr(ProviderBindingExpectationSchemaV2),
    sourceEvidenceRevision: Schema.NullOr(Schema.Int.check(Schema.isGreaterThan(0))), reason: Schema.NonEmptyString }),
]);
const StopIntentSchemaV2 = Schema.Struct({ commandId: CommandId, threadId: ThreadId, incarnation: ApplicationBirthSchemaV2,
  canonicalRequestDigest: LowerSha256, actorBindingDigest: LowerSha256, targetBinding: RegisteredSourceSchemaV2,
  targetEvidenceRevision: Schema.Int.check(Schema.isGreaterThan(0)), stopEventId: EventId,
  stopEventSequence: Schema.Int.check(Schema.isGreaterThan(0)), affectedRunIds: Schema.Array(RunId),
  queuedBases: Schema.Array(ContinuationBasisSchemaV2) });
const StopFenceSchemaV2 = Schema.Struct({ stopCommandId: CommandId, threadId: ThreadId, incarnation: ApplicationBirthSchemaV2,
  runId: RunId, queuedProviderThreadId: ProviderThreadId, sourceBinding: RegisteredSourceSchemaV2,
  sourceEvidenceRevision: Schema.Int.check(Schema.isGreaterThan(0)), switchPlan: Schema.NullOr(SwitchPlanSchemaV2),
  sourceMode: Schema.Literals(["queued_thread", "active_native_copy"]), executionIntent: ExecutionIntentSchemaV2, basisDigest: LowerSha256 });
export interface LegacyContinuationDispositionV1 {
  readonly threadId: ThreadId;
  readonly provenance: "legacy_row" | "native_import";
  readonly qualification: ProviderContinuationQualification;
  readonly evidence: LegacyProviderContinuationEvidenceV1 | null;
  readonly importedAt: string;
}
export interface NativeImportTranscriptSealV1 {
  readonly version: 1;
  readonly threadId: ThreadId;
  readonly projectId: ProjectId;
  readonly provenance: "native_import";
  readonly source: AgentSessionImportSource;
  readonly parserPolicy: "agent_session_visible_messages_v1";
  readonly messageCount: number;
  readonly eventsSha256: string;
  readonly eventBasis: ReadonlyArray<{ readonly eventId: EventId; readonly sequence: number }>;
  readonly birth: { readonly eventId: EventId; readonly sequence: number };
  readonly importedAt: string;
}
export interface LegacyImportTranscriptSnapshotV1 {
  readonly version: 1;
  readonly threadId: ThreadId;
  readonly policy: "legacy_user_assistant_rows_v1";
  readonly sourceUpdatedAt: string;
  readonly messageCount: number;
  readonly sourceRowsSha256: string;
  readonly eventsSha256: string;
  readonly eventBasis: ReadonlyArray<{ readonly eventId: EventId; readonly sequence: number }>;
}
export interface ImportedHistoryStartReviewContextV2 {
  readonly actorSessionId: AuthSessionId;
  readonly threadId: ThreadId;
  readonly delivery: OrchestrationV2ImportedHistoryDelivery;
  readonly readTargetCapability: (instanceId: ProviderInstanceId) => Effect.Effect<ProviderHandoffDeliveryDescriptor | null, unknown>;
  readonly readLegacyTranscript?: Effect.Effect<LegacyImportTranscriptSnapshotV1 | null, unknown>;
}
export interface ImportedHistoryStartReviewFactsV2 {
  readonly review: OrchestrationV2ImportedHistoryReviewResult;
  readonly snapshot: NativeCommandTargetSnapshotV2;
  readonly basis: Readonly<Record<string, unknown>>;
}
export interface ImportedHistoryStartOutcomeV2 {
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly actorSessionId: AuthSessionId;
  readonly commandDigest: string;
  readonly command: OrchestrationV2StartWithImportedHistoryCommand;
  readonly basis: Readonly<Record<string, unknown>>;
  readonly receipt: CommandReceiptStore.CommandReceiptV2;
  readonly runId: RunId | null;
  readonly messageId: MessageId | null;
  readonly effectId: string | null;
  readonly rejectionReason: string | null;
}
export type ImportedHistoryStartPreparationResultV2 =
  | { readonly status: "prepared"; readonly choice: ImportedHistoryStartOutcomeV2; readonly executionIntent: QueuedRunExecutionIntentV2;
      readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent> }
  | { readonly status: "already_prepared"; readonly executionIntent: QueuedRunExecutionIntentV2 }
  | { readonly status: "rejected"; readonly reason: string };
export interface InFlightQueuedRunStartBasisV2 {
  readonly incarnation: ApplicationThreadBirthV2;
  readonly basis: QueuedRunContinuationBasisV2;
  readonly executionIntent: QueuedRunExecutionIntentV2;
}
const ContinuationQualification = Schema.Union([
  Schema.Struct({ type: Schema.Literal("qualified"), nativeThreadId: Schema.NonEmptyString, continuationKey: Schema.NonEmptyString }),
  Schema.Struct({ type: Schema.Literals(["unknown", "unsupported"]), reason: Schema.NonEmptyString }),
]);

export interface ProviderBindingExpectationV2 {
  readonly threadId: ThreadId;
  readonly providerThreadId: ProviderThreadId;
  readonly providerSessionId: ProviderSessionId;
  readonly instanceId: ProviderInstanceId;
  readonly driver: ProviderDriverKind;
  readonly nativeThreadId: string | null;
  readonly runtimeGeneration: string | null;
}
export interface ProviderRuntimeEvidenceV2 {
  readonly binding: ProviderBindingExpectationV2;
  readonly evidenceRevision: number;
  readonly observation: ProviderRuntimeObservation | null;
  readonly registeredAt: string;
}
export interface ConfirmedImportedHistoryContinuationV1 {
  readonly confirmation: NativeCreationRepository.NativeEffectConfirmationV1;
  readonly historicalSource: ProviderContinuationSourceIdentity;
  readonly currentSource: ProviderContinuationSourceIdentity;
}
export type ProviderBindingWriteResultV2 =
  | { readonly committed: true; readonly evidenceRevision: number; readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent> }
  | { readonly committed: false; readonly rejection: "binding_mismatch" | "evidence_revision_mismatch" | "unregistered_generation" | "attempt_mismatch"; readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent> };

export interface RestartContinuationMarkerV2 {
  readonly markerId: string;
  readonly threadId: ThreadId;
  readonly projectId: ProjectId;
  readonly sourceRunId: RunId;
  readonly sourceRunAttemptId: RunAttemptId;
  readonly binding: ProviderBindingExpectationV2;
  readonly evidenceRevision: number;
  readonly createdAt: string;
}
const CapturedRestartIsoTimestampV1 = Schema.String.check(Schema.isPattern(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/));
const RestartContinuationMarkerSchemaV2 = Schema.Struct({ markerId: Schema.NonEmptyString, threadId: ThreadId,
  projectId: ProjectId, sourceRunId: RunId, sourceRunAttemptId: RunAttemptId, binding: ProviderBindingExpectationSchemaV2,
  evidenceRevision: Schema.Int.check(Schema.isGreaterThan(0)), createdAt: CapturedRestartIsoTimestampV1 });
export interface CapturedRestartDispatchContextV2 {
  readonly effectId: string;
  readonly marker: RestartContinuationMarkerV2;
  readonly workerId: string;
  readonly expectedAttempt: number;
}
export interface CapturedRestartCommandOriginV1 {
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly effectId: string;
  readonly marker: RestartContinuationMarkerV2;
  readonly command: Extract<OrchestrationV2Command, { readonly type: "message.dispatch" }>;
  readonly commandDigest: string;
  readonly originalClaim: { readonly workerId: string; readonly expectedAttempt: number; readonly leaseExpiresAt: string };
  readonly recordedAt: string;
}
export const capturedRestartContinuationIdsV1 = (input: { readonly effectId: string; readonly marker: RestartContinuationMarkerV2 }) => {
  const digest = nativeCreationSha256(nativeCreationCanonicalJson({ version: 1, effectId: input.effectId, marker: input.marker }));
  return { commandId: CommandId.make(`command:restart-continuation:captured:${digest}`),
    messageId: MessageId.make(`message:restart-continuation:${input.marker.sourceRunId}`) };
};
const CapturedRestartOriginalClaimSchemaV1 = Schema.Struct({ workerId: Schema.NonEmptyString,
  expectedAttempt: Schema.Int.check(Schema.isGreaterThan(0)), leaseExpiresAt: CapturedRestartIsoTimestampV1 });
const CleanupLeaseSchemaV2 = Schema.Struct({ resourcePath: Schema.NonEmptyString, leaseId: Schema.NonEmptyString,
  ownerThreadId: ThreadId, ownerIncarnation: Schema.NonEmptyString, branch: Schema.NullOr(Schema.String),
  acquiredAtMs: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)), renewedAtMs: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  expiresAtMs: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)) });
const CleanupDeletionSchemaV2 = Schema.Struct({ commandId: CommandId, eventId: EventId, sequence: Schema.Int.check(Schema.isGreaterThan(0)) });
const CleanupTerminalTargetSchemaV2 = Schema.Struct({ threadId: Schema.NonEmptyString, terminalId: Schema.NonEmptyString,
  handleId: Schema.NonEmptyString, ownerBirth: ApplicationBirthSchemaV2 });
const CleanupTerminalCaptureSchemaV2 = Schema.Struct({ managerId: Schema.NonEmptyString,
  threadId: Schema.NonEmptyString, ownerBirth: ApplicationBirthSchemaV2, status: Schema.Literal("captured"),
  managedTargetsOnly: Schema.Literal(true), targets: Schema.Array(CleanupTerminalTargetSchemaV2) });
export const LeaseCleanupTaskV2 = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("provider"), expectedBinding: RegisteredSourceSchemaV2,
    evidenceRevision: Schema.Int.check(Schema.isGreaterThan(0)) }),
  Schema.Struct({ kind: Schema.Literal("terminal"), capture: CleanupTerminalCaptureSchemaV2 }),
  Schema.Struct({ kind: Schema.Literal("attachment"), attachmentIds: Schema.Array(Schema.NonEmptyString) }),
]);
export type LeaseCleanupTaskV2 = typeof LeaseCleanupTaskV2.Type;
export const LeaseCleanupTaskBindingV2 = Schema.Struct({ version: Schema.Literal(2), effectId: Schema.NonEmptyString,
  threadId: ThreadId, lease: CleanupLeaseSchemaV2, ownerBirth: ApplicationBirthSchemaV2,
  deletion: CleanupDeletionSchemaV2, task: LeaseCleanupTaskV2, bindingSha256: LowerSha256, recordedAt: CapturedRestartIsoTimestampV1 });
export type LeaseCleanupTaskBindingV2 = typeof LeaseCleanupTaskBindingV2.Type;
export const UnleasedDeletionCleanupTaskBindingV1 = Schema.Struct({ version: Schema.Literal(1), effectId: Schema.NonEmptyString,
  threadId: ThreadId, leaseInventory: Schema.Struct({ status: Schema.Literal("absent"), resourcePath: Schema.NonEmptyString }),
  ownerBirth: ApplicationBirthSchemaV2, deletion: CleanupDeletionSchemaV2, task: LeaseCleanupTaskV2,
  bindingSha256: LowerSha256, recordedAt: CapturedRestartIsoTimestampV1 });
export type UnleasedDeletionCleanupTaskBindingV1 = typeof UnleasedDeletionCleanupTaskBindingV1.Type;
export const DeletionCleanupTaskBindingV1 = Schema.Union([LeaseCleanupTaskBindingV2, UnleasedDeletionCleanupTaskBindingV1]);
export type DeletionCleanupTaskBindingV1 = typeof DeletionCleanupTaskBindingV1.Type;
export const deletionCleanupTaskBindingDigestV1 = (input: Omit<DeletionCleanupTaskBindingV1, "bindingSha256" | "recordedAt">) =>
  nativeCreationSha256(nativeCreationCanonicalJson(input));
export const LeaseCleanupTaskOutcomeV2 = Schema.Struct({ taskId: Schema.NonEmptyString,
  result: Schema.NullOr(Schema.Literals(["succeeded", "failed"])),
  effect: Schema.Literals(["confirmed", "absent", "no_effect", "unknown"]) });
export type LeaseCleanupTaskOutcomeV2 = typeof LeaseCleanupTaskOutcomeV2.Type;
export interface LeaseCleanupStoreBasisV2 {
  readonly lease: WorktreeOwnershipLease;
  readonly leaseCurrent: boolean;
  readonly ownerPresence: "absent" | "current" | "unavailable";
  readonly historicalOwnerBirth: ApplicationThreadBirthV2 | null;
  readonly currentApplicationBirth: ApplicationThreadBirthV2 | null;
  readonly deletion: typeof CleanupDeletionSchemaV2.Type | null;
  readonly pendingOwnerEffectIds: ReadonlyArray<string>;
  readonly inventoryComplete: boolean;
  readonly tasks: ReadonlyArray<LeaseCleanupTaskBindingV2>;
  readonly outcomes: ReadonlyArray<{ readonly ordinal: number; readonly outcome: LeaseCleanupTaskOutcomeV2;
    readonly correlation: { readonly workerId: string; readonly expectedAttempt: number; readonly bindingSha256: string; readonly evidence: Readonly<Record<string, unknown>> };
    readonly recordedAt: string }>;
}
export const leaseCleanupTaskBindingDigestV2 = (input: Omit<LeaseCleanupTaskBindingV2, "bindingSha256" | "recordedAt">) =>
  nativeCreationSha256(nativeCreationCanonicalJson(input));
export const DeletionWorktreeLeaseInventoryV1 = Schema.Union([
  Schema.Struct({ status: Schema.Literal("original"), lease: CleanupLeaseSchemaV2 }),
  Schema.Struct({ status: Schema.Literal("absent") }),
  Schema.Struct({ status: Schema.Literal("conflict"), leases: Schema.Array(CleanupLeaseSchemaV2) }),
  Schema.Struct({ status: Schema.Literal("unavailable") }),
]);
export const DeletionWorktreeCleanupRequestV1 = Schema.Union([
  Schema.Struct({ origin: Schema.Literal("explicit"), consent: OrchestrationV2ThreadDeletionWorktreeRemoval }),
  Schema.Struct({ origin: Schema.Literal("policy"), projectId: ProjectId, path: Schema.NonEmptyString,
    branch: Schema.NullOr(Schema.String), force: Schema.Literal(false), rules: WorktreeCleanupRules }),
]);
export type DeletionWorktreeCleanupRequestV1 = typeof DeletionWorktreeCleanupRequestV1.Type;
export type DeletionWorktreePolicyCaptureV1 =
  | { readonly status: "captured"; readonly request: Extract<DeletionWorktreeCleanupRequestV1, { readonly origin: "policy" }>;
      readonly revalidate: Effect.Effect<WorktreeCleanupRules, unknown> }
  | { readonly status: "unavailable" };
export const DeletionWorktreeTaskBindingV1 = Schema.Struct({
  version: Schema.Literal(1), effectId: Schema.NonEmptyString, threadId: ThreadId,
  leaseInventory: DeletionWorktreeLeaseInventoryV1, ownerBirth: Schema.NullOr(ApplicationBirthSchemaV2),
  deletion: CleanupDeletionSchemaV2,
  task: Schema.Struct({ kind: Schema.Literal("worktree"), canonicalCommand: OrchestrationV2Command,
    commandDigest: LowerSha256, consent: Schema.optionalKey(OrchestrationV2ThreadDeletionWorktreeRemoval),
    request: Schema.optionalKey(DeletionWorktreeCleanupRequestV1),
    worktree: Schema.Struct({ projectId: ProjectId, path: Schema.NullOr(Schema.NonEmptyString), branch: Schema.NullOr(Schema.String) }),
    projectRoot: Schema.NullOr(Schema.NonEmptyString), prerequisiteEffectIds: Schema.Array(Schema.NonEmptyString),
    captureStatus: Schema.Literals(["captured", "retained"]), reason: Schema.NullOr(Schema.NonEmptyString) }),
  bindingSha256: LowerSha256, recordedAt: CapturedRestartIsoTimestampV1,
});
export type DeletionWorktreeTaskBindingV1 = typeof DeletionWorktreeTaskBindingV1.Type;
export const deletionWorktreeCleanupRequestV1 = (binding: DeletionWorktreeTaskBindingV1): DeletionWorktreeCleanupRequestV1 | null =>
  binding.task.request ?? (binding.task.consent === undefined ? null : { origin: "explicit", consent: binding.task.consent });
export const deletionWorktreeTaskBindingDigestV1 = (input: Omit<DeletionWorktreeTaskBindingV1, "bindingSha256" | "recordedAt">) =>
  nativeCreationSha256(nativeCreationCanonicalJson(Schema.encodeSync(Schema.Struct({
    version: DeletionWorktreeTaskBindingV1.fields.version, effectId: DeletionWorktreeTaskBindingV1.fields.effectId,
    threadId: DeletionWorktreeTaskBindingV1.fields.threadId, leaseInventory: DeletionWorktreeTaskBindingV1.fields.leaseInventory,
    ownerBirth: DeletionWorktreeTaskBindingV1.fields.ownerBirth, deletion: DeletionWorktreeTaskBindingV1.fields.deletion,
    task: DeletionWorktreeTaskBindingV1.fields.task,
  }))(input)));
export const deletionWorktreeEffectIdV1 = (commandId: CommandId, threadId: ThreadId) => `effect:${commandId}:worktree.cleanup:${threadId}`;
export interface DeletionWorktreePathAdmissionV1 {
  readonly path: string;
  readonly status: "available" | "reserved" | "unavailable";
  readonly reservations: ReadonlyArray<DeletionWorktreeTaskBindingV1>;
  readonly admissions: ReadonlyArray<WorktreePathAdmissionV1>;
}
export const WorktreePathAdmissionV1 = Schema.Struct({
  operationId: Schema.NonEmptyString, path: Schema.NonEmptyString, kind: Schema.Literals(["worktree_removal", "native_operation"]),
  subject: Schema.Record(Schema.String, Schema.Unknown), state: Schema.Literals(["reserved", "started", "unknown", "completed", "no_effect", "released"]),
  startedAt: Schema.NullOr(CapturedRestartIsoTimestampV1), outcome: Schema.NullOr(Schema.Record(Schema.String, Schema.Unknown)),
  recordedAt: CapturedRestartIsoTimestampV1, updatedAt: CapturedRestartIsoTimestampV1,
});
export type WorktreePathAdmissionV1 = typeof WorktreePathAdmissionV1.Type;
export const DeletionWorktreeRemovalTargetV1 = Schema.Struct({ projectId: ProjectId, projectRoot: Schema.NonEmptyString,
  path: Schema.NonEmptyString, branch: Schema.NullOr(Schema.String), force: Schema.Boolean });
export type DeletionWorktreeRemovalTargetV1 = typeof DeletionWorktreeRemovalTargetV1.Type;
export const DeletionWorktreeRemovalStartV1 = Schema.Struct({ schema: Schema.Literal("t3.deletion-worktree-removal-start/v1"),
  effectId: Schema.NonEmptyString, bindingSha256: LowerSha256, workerId: Schema.NonEmptyString,
  expectedAttempt: Schema.Int.check(Schema.isGreaterThan(0)), target: DeletionWorktreeRemovalTargetV1,
  startedAt: CapturedRestartIsoTimestampV1 });
export type DeletionWorktreeRemovalStartV1 = typeof DeletionWorktreeRemovalStartV1.Type;
const DeletionWorktreeReadbackSchemaV1 = Schema.Struct({ registration: Schema.Union([
  Schema.Struct({ status: Schema.Literal("complete"), projectRoot: Schema.NonEmptyString, gitCommonDirectory: Schema.NonEmptyString,
    entries: Schema.Array(Schema.Struct({ path: Schema.NonEmptyString, head: Schema.NullOr(Schema.String),
      branch: Schema.NullOr(Schema.String), bare: Schema.Boolean })) }),
  Schema.Struct({ status: Schema.Literal("unavailable"), reason: Schema.NonEmptyString }),
]), filesystem: Schema.Union([
  Schema.Struct({ status: Schema.Literals(["present", "absent"]), path: Schema.NonEmptyString }),
  Schema.Struct({ status: Schema.Literal("unavailable"), path: Schema.NonEmptyString, reason: Schema.NonEmptyString }),
]) });
export const DeletionWorktreeRemovalObservationSchemaV1 = Schema.Struct({ version: Schema.Literal(1),
  start: DeletionWorktreeRemovalStartV1, startOrdinal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  operation: Schema.Union([
    Schema.Struct({ kind: Schema.Literal("executed"), exitCode: Schema.NullOr(Schema.Int), completion: Schema.Literals(["exited", "unknown"]) }),
    Schema.Struct({ kind: Schema.Literal("reconciled"), completion: Schema.Literal("unknown") }),
    Schema.Struct({ kind: Schema.Literal("already_absent"), completion: Schema.Literal("not_invoked") }),
  ]), before: DeletionWorktreeReadbackSchemaV1, after: DeletionWorktreeReadbackSchemaV1, observedAt: CapturedRestartIsoTimestampV1 });
export const ManagedTerminalDeletionObservationV1 = Schema.Struct({ version: Schema.Literal(1), kind: Schema.Literal("managed_terminal"),
  effectId: Schema.NonEmptyString, bindingSha256: LowerSha256, workerId: Schema.NonEmptyString,
  expectedAttempt: Schema.Int.check(Schema.isGreaterThan(0)), capture: CleanupTerminalCaptureSchemaV2,
  result: Schema.Struct({ status: Schema.Literals(["closed", "observed_absent", "mismatch", "unknown"]), managedTargetsOnly: Schema.Literal(true),
    processExitObserved: Schema.Boolean, descendantsQuiescence: Schema.Literal("unavailable"), futureWakeClosure: Schema.Literal("unavailable") }),
  observedAt: CapturedRestartIsoTimestampV1 });
export type ManagedTerminalDeletionObservationV1 = typeof ManagedTerminalDeletionObservationV1.Type;
export const ManagedProviderDeletionObservationV1 = Schema.Struct({ version: Schema.Literal(1), kind: Schema.Literal("managed_provider"),
  effectId: Schema.NonEmptyString, bindingSha256: LowerSha256, workerId: Schema.NonEmptyString,
  expectedAttempt: Schema.Int.check(Schema.isGreaterThan(0)), binding: RegisteredSourceSchemaV2,
  evidenceRevision: Schema.Int.check(Schema.isGreaterThan(0)), nativeOperation: ProviderNativeOperationContext,
  result: Schema.Union([
    Schema.Struct({ status: Schema.Literal("stopped"), operationId: Schema.NonEmptyString, binding: RegisteredSourceSchemaV2,
      cancelledPendingStart: Schema.Boolean, interruptedProviderTurnIds: Schema.Array(ProviderTurnId),
      readback: Schema.Struct({ threadAttached: Schema.Literal(false) }) }),
    Schema.Struct({ status: Schema.Literal("unknown"), reason: Schema.String }),
  ]), observedAt: CapturedRestartIsoTimestampV1 });
export type ManagedProviderDeletionObservationV1 = Omit<typeof ManagedProviderDeletionObservationV1.Type, "result"> & {
  readonly result: ProviderPinnedRuntimeStopResultV1;
};
export const DeletionCleanupObservationV1 = Schema.Union([DeletionWorktreeRemovalObservationSchemaV1,
  ManagedTerminalDeletionObservationV1, ManagedProviderDeletionObservationV1]);
export type DeletionCleanupObservationV1 = DeletionWorktreeRemovalObservationV1 | ManagedTerminalDeletionObservationV1 | ManagedProviderDeletionObservationV1;
export interface ObservedDeletionCleanupOutcomeV1 {
  readonly ordinal: number;
  readonly outcome: LeaseCleanupTaskOutcomeV2;
  readonly bindingSha256: string;
  readonly evidence: { readonly version: 1; readonly schema: "t3.deletion-cleanup-observation/v1";
    readonly producer: "worktree" | "managed_terminal" | "managed_provider"; readonly observation: DeletionCleanupObservationV1;
    readonly coveredHolds: ReadonlyArray<EffectOutbox.UnknownEffectHoldV2> };
}
export interface DeletionCleanupTaskOutcomeRowV1 {
  readonly ordinal: number;
  readonly outcome: LeaseCleanupTaskOutcomeV2;
  readonly correlation: { readonly workerId: string; readonly expectedAttempt: number; readonly bindingSha256: string;
    readonly evidence: Readonly<Record<string, unknown>> };
  readonly recordedAt: string;
}
export interface DeletionCleanupTaskOwnerBirthV1 {
  readonly latestApplicationBirth: ApplicationThreadBirthV2;
  readonly matchesOriginal: boolean;
}
export const deletionWorktreeRemovalTargetV1 = (binding: DeletionWorktreeTaskBindingV1): DeletionWorktreeRemovalTargetV1 | null =>
  binding.task.worktree.path === null || binding.task.projectRoot === null || deletionWorktreeCleanupRequestV1(binding) === null ? null : { ...binding.task.worktree,
    path: binding.task.worktree.path, projectRoot: binding.task.projectRoot,
    force: deletionWorktreeCleanupRequestV1(binding)!.origin === "explicit" };
export interface DeletionWorktreeExecutionBasisV1 {
  readonly binding: DeletionWorktreeTaskBindingV1;
  readonly effect: EffectOutbox.OrchestrationEffectV2;
  readonly admission: WorktreePathAdmissionV1 | null;
  readonly currentLease: "original" | "absent" | "replacement" | "unavailable";
  readonly inventoryComplete: boolean;
  readonly prerequisitesReady: boolean;
  readonly prerequisites: ReadonlyArray<{
    readonly effectId: string;
    readonly binding: DeletionCleanupTaskBindingV1 | null;
    readonly latestOutcome: { readonly ordinal: number; readonly outcome: LeaseCleanupTaskOutcomeV2 } | null;
    readonly held: boolean;
    readonly ready: boolean;
  }>;
  readonly latestOutcome: { readonly ordinal: number; readonly outcome: LeaseCleanupTaskOutcomeV2 } | null;
  readonly start: { readonly ordinal: number; readonly evidence: DeletionWorktreeRemovalStartV1 } | null;
  readonly held: boolean;
  readonly reason: string | null;
}
export type DeletionWorktreeRemovalStartResultV1 =
  | { readonly status: "start_now"; readonly start: DeletionWorktreeRemovalStartV1; readonly basis: DeletionWorktreeExecutionBasisV1 }
  | { readonly status: "observe_only" | "retained"; readonly reason: string; readonly basis: DeletionWorktreeExecutionBasisV1 | null };
const DeletionWorktreeInventorySchemaV1 = Schema.Struct({
  worktree: DeletionWorktreeTaskBindingV1.fields.task.fields.worktree,
  projectRoot: DeletionWorktreeTaskBindingV1.fields.task.fields.projectRoot,
  leaseInventory: DeletionWorktreeLeaseInventoryV1,
  prerequisiteEffectIds: Schema.Array(Schema.NonEmptyString),
  request: Schema.optionalKey(DeletionWorktreeCleanupRequestV1),
  captureStatus: Schema.Literals(["captured", "retained"]), reason: Schema.NullOr(Schema.NonEmptyString),
});
const ThreadDeletionCommandRecordSchemaV1 = Schema.Struct({
  command: OrchestrationV2Command, commandDigest: LowerSha256, ownerBirth: Schema.NullOr(ApplicationBirthSchemaV2),
  inventory: DeletionWorktreeInventorySchemaV1, deletion: CleanupDeletionSchemaV2, recordedAt: CapturedRestartIsoTimestampV1,
});
export type ThreadDeletionCommandRecordV1 = Omit<typeof ThreadDeletionCommandRecordSchemaV1.Type, "command"> & {
  readonly command: Extract<OrchestrationV2Command, { readonly type: "thread.delete" }>;
};
export class RestartContinuationMarkerError extends Schema.TaggedError<RestartContinuationMarkerError>()(
  "RestartContinuationMarkerError", { reason: Schema.Literals(["binding_changed", "marker_conflict", "source_changed", "unknown_effect", "qualification_unavailable"]) },
) {}

const CommitPublications = Context.Reference<Array<Effect.Effect<void>> | undefined>(
  "t3/orchestration-v2/EventSink/CommitPublications", { defaultValue: () => undefined },
);
// Only successful writes in this outer transaction can establish a new import seal.
const TransactionWrittenEvents = Context.Reference<Map<EventId, OrchestrationV2StoredEvent> | undefined>(
  "t3/orchestration-v2/EventSink/TransactionWrittenEvents", { defaultValue: () => undefined },
);
const ImportedHistoryReservation = Context.Reference<string | undefined>(
  "t3/orchestration-v2/EventSink/ImportedHistoryReservation", { defaultValue: () => undefined },
);

/**
 * SERVICE DEFINITION
 */
export interface EventSinkV2Shape {
  readonly readThreadDeletionCommand: (commandId: CommandId) => Effect.Effect<ThreadDeletionCommandRecordV1 | null, EventSinkV2Error>;
  readonly readDeletionWorktreeTask: (effectId: string) => Effect.Effect<DeletionWorktreeTaskBindingV1 | null, EventSinkV2Error>;
  readonly readDeletionWorktreeExecutionBasis: (effectId: string) => Effect.Effect<DeletionWorktreeExecutionBasisV1 | null, EventSinkV2Error>;
  readonly startDeletionWorktreeRemoval: (input: { readonly effectId: string; readonly bindingSha256: string;
    readonly workerId: string; readonly expectedAttempt: number; readonly target: DeletionWorktreeRemovalTargetV1;
    readonly revalidatePolicy?: Effect.Effect<WorktreeCleanupRules, unknown> }) =>
    Effect.Effect<DeletionWorktreeRemovalStartResultV1, EventSinkV2Error>;
  readonly revalidateDeletionWorktreeRemovalStart: (start: DeletionWorktreeRemovalStartV1, startOrdinal: number,
    revalidatePolicy?: Effect.Effect<WorktreeCleanupRules, unknown>) =>
    Effect.Effect<void, EventSinkV2Error>;
  readonly readDeletionCleanupTask: (effectId: string) => Effect.Effect<DeletionCleanupTaskBindingV1 | null, EventSinkV2Error>;
  readonly readDeletionCleanupTaskOwnerBirth: (effectId: string) => Effect.Effect<DeletionCleanupTaskOwnerBirthV1 | null, EventSinkV2Error>;
  readonly readDeletionCleanupTaskOutcome: (effectId: string) => Effect.Effect<DeletionCleanupTaskOutcomeRowV1 | null, EventSinkV2Error>;
  readonly prepareDeletionCleanupTaskBindings: (input: { readonly commandId: CommandId;
    readonly terminalCapture: Extract<LeaseCleanupTaskV2, { readonly kind: "terminal" }>["capture"] | null }) =>
    Effect.Effect<ReadonlyArray<DeletionCleanupTaskBindingV1>, EventSinkV2Error>;
  readonly recordObservedDeletionCleanupOutcome: (input: { readonly effectId: string; readonly bindingSha256: string;
    readonly expectedLatestOrdinal: number; readonly observation: DeletionCleanupObservationV1;
    readonly coveredHolds: ReadonlyArray<EffectOutbox.UnknownEffectHoldV2> }) => Effect.Effect<ObservedDeletionCleanupOutcomeV1, EventSinkV2Error>;
  readonly readUnresolvedDeletionCleanupHolds: (threadId: ThreadId) =>
    Effect.Effect<ReadonlyArray<EffectOutbox.UnknownEffectHoldV2>, EventSinkV2Error>;
  readonly completeObservedDeletionCleanup: (input: { readonly effectId: string; readonly bindingSha256: string;
    readonly expectedLatestOrdinal: number }) => Effect.Effect<boolean, EventSinkV2Error>;
  readonly finalizeDeletionWorktreeCleanup: <E, R>(input: { readonly effectId: string; readonly bindingSha256: string;
    readonly expectedLatestOrdinal: number; readonly releaseOriginalLease?: (lease: WorktreeOwnershipLease) => Effect.Effect<void, E, R> }) =>
    Effect.Effect<{ readonly status: "completed" | "retained"; readonly reason: string | null }, E | EventSinkV2Error, R>;
  readonly observeThreadDeletionCleanup: (input: { readonly threadId: ThreadId; readonly commandId: CommandId }) =>
    Effect.Effect<OrchestrationV2ThreadDeletionCleanupObservation, EventSinkV2Error>;
  readonly readDeletionWorktreePathAdmission: (input: { readonly path: string }) =>
    Effect.Effect<DeletionWorktreePathAdmissionV1, EventSinkV2Error>;
  /** SQL mutation only; native Git work must not run inside this transaction. */
  readonly withDeletionWorktreeSqlMutation: <A, E, R>(input: { readonly path: string; readonly ordinaryMutation?: OrdinaryCheckout.OrdinaryCheckoutOwnMutationV1 }, mutation: Effect.Effect<A, E, R>) =>
    Effect.Effect<A, E | EventSinkV2Error, R>;
  readonly readLeaseCleanupStoreBasis: (lease: WorktreeOwnershipLease) => Effect.Effect<LeaseCleanupStoreBasisV2, EventSinkV2Error>;
  readonly readLeaseCleanupTask: (effectId: string) => Effect.Effect<LeaseCleanupTaskBindingV2 | null, EventSinkV2Error>;
  readonly prepareLeaseCleanupTaskBindings: (input: { readonly lease: WorktreeOwnershipLease;
    readonly terminalCapture: Extract<LeaseCleanupTaskV2, { readonly kind: "terminal" }>["capture"] | null }) =>
    Effect.Effect<LeaseCleanupStoreBasisV2, EventSinkV2Error>;
  readonly recordLeaseCleanupTaskOutcome: (input: { readonly effectId: string; readonly workerId: string;
    readonly expectedAttempt: number; readonly outcome: LeaseCleanupTaskOutcomeV2;
    readonly evidence: Readonly<Record<string, unknown>> }) => Effect.Effect<LeaseCleanupTaskOutcomeV2, EventSinkV2Error>;
  readonly prepareImportedHistoryStartExecution: (input: {
    readonly reference: ImportedHistoryStartExecutionReferenceV2;
    readonly workerId: string;
    readonly expectedAttempt: number;
    readonly currentSnapshot: NativeCommandTargetSnapshotV2;
    readonly reviewSource: Pick<ImportedHistoryStartReviewContextV2, "readTargetCapability" | "readLegacyTranscript">;
    readonly revalidateAuthority: (choice: ImportedHistoryStartOutcomeV2) => Effect.Effect<void, unknown>;
    /** SQL preparation only, after reservation; provider allocation occurs after the owning commit. */
    readonly prepare: (choice: ImportedHistoryStartOutcomeV2) => Effect.Effect<ReadonlyArray<OrchestrationV2DomainEvent>, unknown>;
  }) => Effect.Effect<ImportedHistoryStartPreparationResultV2, EventSinkV2Error>;
  readonly readCurrentThreadRuntimeStopIntent: (input: { readonly threadId: ThreadId; readonly commandId: CommandId }) => Effect.Effect<CurrentThreadRuntimeStopIntentV2 | null, EventSinkV2Error>;
  readonly readQueuedRunRuntimeStopFences: (input: { readonly threadId: ThreadId; readonly runId: RunId; readonly incarnation: ApplicationThreadBirthV2 }) => Effect.Effect<ReadonlyArray<QueuedRunRuntimeStopFenceV2>, EventSinkV2Error>;
  readonly readInFlightQueuedRunStartBases: (input: { readonly threadId: ThreadId; readonly incarnation: ApplicationThreadBirthV2 }) =>
    Effect.Effect<ReadonlyArray<InFlightQueuedRunStartBasisV2>, EventSinkV2Error>;
  readonly readClaimedQueuedRunStart: (input: { readonly effectId: string; readonly threadId: ThreadId; readonly runId: RunId;
    readonly workerId: string; readonly expectedAttempt: number }) => Effect.Effect<InFlightQueuedRunStartBasisV2 | null, EventSinkV2Error>;
  readonly reserveQueuedRunStart: (input: QueuedRunStartReservationInputV2 & {
    readonly pendingEffect?: EffectOutbox.PendingOrchestrationEffectV2;
  }) => Effect.Effect<QueuedRunStartReservationResultV2, EventSinkV2Error>;
  readonly readApplicationThreadBirth: (threadId: ThreadId) => Effect.Effect<ApplicationThreadBirthV2 | null, EventSinkV2Error>;
  /** Application identity only; source eligibility and runtime ownership are separate commit predicates. */
  readonly readApplicationBirthRecord: (threadId: ThreadId) => Effect.Effect<ApplicationThreadBirthV2 | null, EventSinkV2Error>;
  readonly readImportedHistoryStartReview: (input: ImportedHistoryStartReviewContextV2) => Effect.Effect<ImportedHistoryStartReviewFactsV2, EventSinkV2Error>;
  readonly readImportedHistoryStartChoice: (input: { readonly threadId: ThreadId; readonly commandId: CommandId }) => Effect.Effect<ImportedHistoryStartOutcomeV2 | null, EventSinkV2Error>;
  readonly observeImportedHistoryStart: (input: { readonly threadId: ThreadId; readonly commandId: CommandId }) => Effect.Effect<OrchestrationV2ImportedHistoryStartReceipt, EventSinkV2Error>;
  readonly commitImportedHistoryStart: (input: {
    readonly command: OrchestrationV2StartWithImportedHistoryCommand;
    readonly reviewContext: ImportedHistoryStartReviewContextV2;
    readonly revalidateAuthority: Effect.Effect<void, unknown>;
    readonly ordinaryCheckoutContext?: OrdinaryCheckoutCommitContextV1;
    /** Invoked only after the exact choice reservation; it plans SQL events and never opens a provider. */
    readonly plan: (facts: ImportedHistoryStartReviewFactsV2) => Effect.Effect<{
      readonly runId: RunId;
      readonly messageId: MessageId;
      readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
      readonly effects: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2>;
    }, unknown>;
  }) => Effect.Effect<{ readonly committed: boolean; readonly outcome: ImportedHistoryStartOutcomeV2 }, OrdinaryCheckoutCommitErrorV1>;
  readonly recordNativeImportTranscriptSeal: (input: {
    readonly threadId: ThreadId;
    readonly projectId: ProjectId;
    readonly source: AgentSessionImportSource;
    readonly parserPolicy: "agent_session_visible_messages_v1";
    readonly messageEvents: ReadonlyArray<OrchestrationV2DomainEvent>;
    readonly importedAt: string;
  }) => Effect.Effect<NativeImportTranscriptSealV1, EventSinkV2Error>;
  /** Validates the stored imported snapshot; it does not inspect the current native file. */
  readonly readNativeImportTranscriptSeal: (threadId: ThreadId) => Effect.Effect<NativeImportTranscriptSealV1 | null, EventSinkV2Error>;
  readonly readCommandReceiptIdentity: (commandId: CommandId) => Effect.Effect<{
    readonly receipt: CommandReceiptStore.CommandReceiptV2 | null;
    readonly projectReceipt: CommandReceiptStore.ProjectCommandReceiptV2 | null;
    readonly identity: NativeCommandIdentityV2 | null;
    readonly nativeCreationReservation: NativeCreationRepository.NativeCreationReservedCommandIdentity | null;
    readonly importedHistoryChoiceIdentity: { readonly commandDigest: string; readonly threadId: ThreadId; readonly actorSessionId: AuthSessionId } | null;
    readonly currentRuntimeStopIdentity: { readonly threadId: ThreadId; readonly canonicalRequestDigest: string; readonly actorBindingDigest: string } | null;
    readonly capturedRestartOrigin: CapturedRestartCommandOriginV1 | null;
    readonly threadRecovery: NativeCreationRepository.NativeCreationThreadRecoveryCommandV2 | null;
    readonly threadDeletion: ThreadDeletionCommandRecordV1 | null;
    readonly ordinaryCheckoutAdmissions: ReadonlyArray<OrdinaryCheckout.OrdinaryCheckoutAdmissionV1>;
    readonly ordinaryCheckoutEffectLinks: ReadonlyArray<OrdinaryCheckout.OrdinaryCheckoutEffectLinkV1>;
  }, EventSinkV2Error>;
  readonly readOrdinaryCheckoutAdmission: (input: {
    readonly commandId: CommandId; readonly threadId: ThreadId;
  }) => Effect.Effect<OrdinaryCheckout.OrdinaryCheckoutAdmissionV1 | null, EventSinkV2Error>;
  readonly readOrdinaryCheckoutAdmissionForRun: (input: {
    readonly threadId: ThreadId; readonly runId: RunId;
  }) => Effect.Effect<OrdinaryCheckout.OrdinaryCheckoutAdmissionV1 | null, EventSinkV2Error>;
  readonly readOrdinaryCheckoutEffectLink: (effectId: string) => Effect.Effect<OrdinaryCheckout.OrdinaryCheckoutEffectLinkV1 | null, EventSinkV2Error>;
  readonly captureOrdinaryPreparedLaunch: (input: {
    readonly command: Extract<OrchestrationV2Command, { readonly type: "thread.create" | "thread.metadata.update" }>;
    readonly preparationEvent: OrdinaryCheckout.OrdinaryAcceptedEventV1;
    readonly capture: () => Effect.Effect<OrdinaryCheckoutSqlCaptureV1, unknown>;
  }) => Effect.Effect<OrdinaryCheckout.OrdinaryCheckoutAdmissionV1, OrdinaryCheckoutCommitErrorV1>;
  readonly readOrdinaryCheckoutUse: (operationId: string) => Effect.Effect<OrdinaryCheckoutUseRecordV1 | null, OrdinaryCheckoutCommitErrorV1>;
  readonly beginOrdinaryCheckoutUse: (input: OrdinaryCheckoutUseInputV1) => Effect.Effect<{
    readonly status: "use_now" | "observe_only"; readonly record: OrdinaryCheckoutUseRecordV1;
  }, OrdinaryCheckoutCommitErrorV1>;
  readonly revalidateOrdinaryCheckoutUse: (use: OrdinaryCheckout.OrdinaryCheckoutUseV1) => Effect.Effect<OrdinaryCheckoutUseRecordV1, OrdinaryCheckoutCommitErrorV1>;
  readonly endOrdinaryCheckoutOutboxUse: (use: OrdinaryCheckout.OrdinaryCheckoutUseV1) => Effect.Effect<boolean, OrdinaryCheckoutCommitErrorV1>;
  readonly abortUnstartedOrdinaryCheckoutUse: (use: OrdinaryCheckout.OrdinaryCheckoutUseV1) => Effect.Effect<boolean, OrdinaryCheckoutCommitErrorV1>;
  readonly holdOrdinaryCheckoutUseUnknown: (input: {
    readonly use: OrdinaryCheckout.OrdinaryCheckoutUseV1; readonly reason: string;
  }) => Effect.Effect<OrdinaryCheckoutUseRecordV1, OrdinaryCheckoutCommitErrorV1>;
  readonly readNativeThreadRecovery: (input: {
    readonly command: Extract<OrchestrationV2Command, { readonly type: "thread.delete" }>;
    readonly context: NativeCreationThreadRecoveryContextV2;
  }) => Effect.Effect<NativeCreationRepository.NativeCreationThreadRecoveryCommandV2, EventSinkV2Error>;
  /** Rechecks the current claim before replay; the retained original claim is historical correlation. */
  readonly readCapturedRestartCommandOrigin: (input: {
    readonly command: Extract<OrchestrationV2Command, { readonly type: "message.dispatch" }>;
    readonly context: CapturedRestartDispatchContextV2;
  }) => Effect.Effect<CapturedRestartCommandOriginV1 | null, EventSinkV2Error>;
  readonly prepareRestartContinuation: (input: {
    readonly markerId: string | Effect.Effect<string>;
    readonly threadId: ThreadId;
    readonly projectId: ProjectId;
    readonly sourceRunId: RunId;
    readonly sourceRunAttemptId: RunAttemptId;
    readonly expectedBinding: ProviderBindingExpectationV2;
    readonly expectedEvidenceRevision: number;
  }) => Effect.Effect<RestartContinuationMarkerV2, EventSinkV2Error | RestartContinuationMarkerError>;
  readonly findDormantRestartContinuation: (input: {
    readonly threadId: ThreadId;
    readonly projectId: ProjectId;
    readonly sourceRunId: RunId;
    readonly sourceRunAttemptId: RunAttemptId;
    readonly expectedBinding: ProviderBindingExpectationV2;
    readonly expectedEvidenceRevision: number;
  }) => Effect.Effect<RestartContinuationMarkerV2 | null, EventSinkV2Error | RestartContinuationMarkerError>;
  readonly readDormantRestartContinuations: Effect.Effect<ReadonlyArray<RestartContinuationMarkerV2>, EventSinkV2Error>;
  readonly readReleasedRestartContinuation: (input: {
    readonly effectId: string; readonly threadId: ThreadId; readonly sourceRunId: RunId;
  }) => Effect.Effect<RestartContinuationMarkerV2 | null, EventSinkV2Error>;
  readonly clearRestartContinuation: (marker: RestartContinuationMarkerV2) => Effect.Effect<boolean, EventSinkV2Error>;
  readonly releaseRestartContinuation: (input: {
    readonly marker: RestartContinuationMarkerV2;
    readonly currentSnapshot: NativeCommandTargetSnapshotV2;
    readonly revalidateAfterTrial: Effect.Effect<void, unknown>;
  }) => Effect.Effect<boolean, EventSinkV2Error | RestartContinuationMarkerError>;
  readonly onCommit: (effect: Effect.Effect<void>) => Effect.Effect<void, EventSinkWriteError>;
  readonly withWorktreeOwnershipTransaction: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E | EventSinkWriteError, R>;
  readonly withTransaction: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E | EventSinkWriteError, R>;
  readonly getThreadIncarnation: (threadId: ThreadId) => Effect.Effect<NativeThreadIncarnationV2 | null, EventSinkV2Error>;
  readonly readNativeCommandFacts: (input: {
    readonly threadId: ThreadId;
    readonly commandId: CommandId;
    readonly authority?: NativeCommandAuthorityReadV2;
  }) => Effect.Effect<NativeCommandFactsV2, EventSinkV2Error>;
  readonly readProviderRuntimeEvidence: (threadId: ThreadId) => Effect.Effect<ProviderRuntimeEvidenceV2 | null, EventSinkV2Error>;
  readonly readCurrentProviderRuntimeOwner: (threadId: ThreadId) => Effect.Effect<ProviderRuntimeEvidenceV2 | null, EventSinkV2Error>;
  readonly readConfirmedImportedHistoryBinding: (input: {
    readonly expectedBinding: ProviderBindingExpectationV2;
    readonly expectedEvidenceRevision: number;
  }) => Effect.Effect<NativeCreationRepository.NativeEffectConfirmationV1 | null, EventSinkV2Error>;
  readonly readConfirmedImportedHistoryContinuation: (input: {
    readonly expectedBinding: ProviderBindingExpectationV2;
    readonly expectedEvidenceRevision: number;
  }) => Effect.Effect<ConfirmedImportedHistoryContinuationV1 | null, EventSinkV2Error>;
  readonly readProviderContinuationSourceIdentity: (binding: ProviderBindingExpectationV2) => Effect.Effect<ProviderContinuationSourceIdentity | null, EventSinkV2Error>;
  readonly registerProviderRuntime: (input: {
    readonly expectedBinding: ProviderBindingExpectationV2;
    readonly expectedRegisteredBinding?: ProviderBindingExpectationV2 | null;
    readonly expectedEvidenceRevision: number;
    readonly actualBinding: ProviderRuntimeBinding;
    readonly actualContinuationSourceIdentity?: ProviderContinuationSourceIdentity;
    readonly expectedRunId?: RunId;
    readonly expectedRunAttemptId?: RunAttemptId;
  }) => Effect.Effect<ProviderBindingWriteResultV2, EventSinkV2Error>;
  readonly writeIfProviderBindingCurrent: (input: {
    readonly expectedBinding: ProviderBindingExpectationV2;
    readonly expectedEvidenceRevision: number;
    readonly expectedRunId?: RunId;
    readonly expectedRunAttemptId?: RunAttemptId;
    readonly observation?: ProviderRuntimeObservation;
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
  }) => Effect.Effect<ProviderBindingWriteResultV2, EventSinkV2Error>;
  readonly writeIfCurrentProviderRuntimeOwner: (input: Parameters<EventSinkV2Shape["writeIfProviderBindingCurrent"]>[0] & {
    readonly revalidateCurrentOwner: Effect.Effect<void, unknown>;
  }) => Effect.Effect<ProviderBindingWriteResultV2, EventSinkV2Error>;
  readonly writeIfCurrentProviderRuntimeOutputOwner: (input: {
    readonly expectedBinding: ProviderBindingExpectationV2;
    readonly expectedEvidenceRevision: number;
    readonly expectedRunId: RunId;
    readonly expectedRunAttemptId: RunAttemptId;
    readonly expectedProviderTurnId: ProviderTurnId;
    readonly revalidateCurrentOwner: Effect.Effect<void, unknown>;
    readonly companionNodes?: ReadonlyArray<Extract<OrchestrationV2DomainEvent, { readonly type: "node.updated" }>>;
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
  }) => Effect.Effect<ProviderBindingWriteResultV2, EventSinkV2Error>;
  readonly readLegacyContinuationDisposition: (threadId: ThreadId) => Effect.Effect<LegacyContinuationDispositionV1 | null, EventSinkV2Error>;
  readonly recordLegacyContinuationDisposition: (input: {
    readonly threadId: ThreadId;
    readonly provenance: "legacy_row" | "native_import";
    readonly qualification: ProviderContinuationQualification;
    readonly evidence: LegacyProviderContinuationEvidenceV1 | null;
    readonly importedAt: string;
  }) => Effect.Effect<void, EventSinkV2Error>;
  readonly write: (input: {
    readonly guardPendingUserInputCancellations?: boolean;
    readonly commandId?: CommandId;
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
  }) => Effect.Effect<ReadonlyArray<OrchestrationV2StoredEvent>, EventSinkV2Error>;
  readonly writeWithEffects: (input: {
    readonly guardPendingUserInputCancellations?: boolean;
    readonly commandId?: CommandId;
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
    readonly effects: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2>;
  }) => Effect.Effect<ReadonlyArray<OrchestrationV2StoredEvent>, EventSinkV2Error>;
  readonly writeIfRunCurrent: (input: {
    readonly guardPendingUserInputCancellations?: boolean;
    readonly commandId?: CommandId;
    readonly threadId: ThreadId;
    readonly runId: RunId;
    readonly activeAttemptId: RunAttemptId;
    readonly expectedStatus: OrchestrationV2Run["status"];
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
  }) => Effect.Effect<
    {
      readonly committed: boolean;
      readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
    },
    EventSinkV2Error
  >;
  /**
   * Atomically commit only when the provider thread is still owned by the
   * expected run attempt and ordinal. Used for late post-terminal
   * provider_thread updates so a completed or superseded attempt cannot clobber
   * a newer attempt that already claimed the thread.
   */
  readonly writeIfProviderThreadOwner: (input: {
    readonly guardPendingUserInputCancellations?: boolean;
    readonly commandId?: CommandId;
    readonly providerThreadId: ProviderThreadId;
    readonly runId: RunId;
    readonly activeAttemptId: RunAttemptId;
    readonly expectedLastRunOrdinal: number;
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
  }) => Effect.Effect<
    {
      readonly committed: boolean;
      readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
    },
    EventSinkV2Error
  >;
  readonly commitCommand: (input: {
    readonly commandId: CommandId;
    readonly threadId: ThreadId;
    readonly commandType: string;
    readonly acceptedAt: DateTime.Utc;
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
    readonly effects: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2>;
    readonly deletionCommand?: Extract<OrchestrationV2Command, { readonly type: "thread.delete" }>;
    readonly deletionWorktreePolicy?: DeletionWorktreePolicyCaptureV1;
    readonly ordinaryCheckoutContext?: OrdinaryCheckoutCommitContextV1;
    readonly nativeContext?: NativeCommandCommitContextV2;
    readonly recoveryContext?: NativeCreationThreadRecoveryContextV2;
    readonly stopContext?: CurrentThreadRuntimeStopCommitContextV2;
    readonly capturedRestartContext?: CapturedRestartDispatchContextV2 & {
      readonly command: Extract<OrchestrationV2Command, { readonly type: "message.dispatch" }>;
    };
    readonly cancelUnsettledEffects?: {
      readonly effectTypes: ReadonlyArray<EffectOutbox.OrchestrationEffectRequestV2["type"]>;
      readonly reason: string;
    };
  }) => Effect.Effect<
    {
      readonly receipt: CommandReceiptStore.CommandReceiptV2;
      readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
      readonly committed: boolean;
      readonly cancelledEffectCount: number;
    },
    OrdinaryCheckoutCommitErrorV1
  >;
  readonly commitRejectedCommand: (input: {
    readonly commandId: CommandId;
    readonly threadId: ThreadId;
    readonly commandType: string;
    readonly rejectedAt: DateTime.Utc;
    readonly error: string;
    readonly nativeIdentity?: NativeCommandIdentityV2;
    readonly nativeContext?: NativeCommandCommitContextV2;
    readonly workstreamWitness?: NativeWorkstreamSettlementWitnessV2;
    readonly capturedRestartContext?: CapturedRestartDispatchContextV2 & {
      readonly command: Extract<OrchestrationV2Command, { readonly type: "message.dispatch" }>;
    };
  }) => Effect.Effect<CommandReceiptStore.CommandReceiptV2, EventSinkV2Error>;
  /**
   * Append a project event, fold it into its row and record the receipt in one
   * transaction. A reused command id commits nothing and returns its receipt.
   */
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
  /** Record a rejected project command, or return the receipt its command id already has. */
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
    /** Filter before queuing live events so a busy worker retains only the events it handles. */
    readonly eventType?: OrchestrationV2DomainEvent["type"];
    /** Bound RPC subscribers; internal workers must not drop their subscription under load. */
    readonly bounded?: boolean;
  }) => Stream.Stream<OrchestrationV2StoredEvent, EventSinkV2Error>;
  readonly latestSequence: (input?: {
    readonly threadId?: ThreadId;
  }) => Effect.Effect<number, EventSinkV2Error>;
  readonly readByCommandId: (input: {
    readonly commandId: CommandId;
  }) => Stream.Stream<OrchestrationV2StoredEvent, EventSinkV2Error>;
}

export class EventSinkV2 extends Context.Service<EventSinkV2, EventSinkV2Shape>()(
  "t3/orchestration-v2/EventSink/EventSinkV2",
) {}

/**
 * IMPLEMENTATIONS
 */
const baseLayer: Layer.Layer<
  EventSinkV2,
  never,
  | CommandReceiptStore.CommandReceiptStoreV2
  | EffectOutbox.EffectOutboxV2
  | EventStore.EventStoreV2
  | ProjectionStore.ProjectionStoreV2
  | ProjectStore.ProjectStoreV2
  | SqlClient.SqlClient
  | TurnItemPositionStore.TurnItemPositionStoreV2
  | NativeCreationRepository.NativeCreationRepository
> = Layer.effect(
  EventSinkV2,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const commandReceipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
    const effectOutbox = yield* EffectOutbox.EffectOutboxV2;
    const eventStore = yield* EventStore.EventStoreV2;
    const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
    const projectStore = yield* ProjectStore.ProjectStoreV2;
    const turnItemPositions = yield* TurnItemPositionStore.TurnItemPositionStoreV2;
    const nativeCreationRepository = yield* NativeCreationRepository.NativeCreationRepository;
    const liveEvents = yield* PubSub.unbounded<OrchestrationV2StoredEvent>();
    const liveEventsByType = new Map<
      OrchestrationV2DomainEvent["type"],
      PubSub.PubSub<OrchestrationV2StoredEvent>
    >();
    const publishLiveEvents = (events: ReadonlyArray<OrchestrationV2StoredEvent>) =>
      Effect.gen(function* () {
        yield* PubSub.publishAll(liveEvents, events);
        for (const [type, pubsub] of liveEventsByType) {
          yield* PubSub.publishAll(
            pubsub,
            events.filter((stored) => stored.event.type === type),
          );
        }
      });

    const assertPublicationScope = Effect.gen(function* () {
      if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)) &&
          (yield* CommitPublications) === undefined)
        return yield* new EventSinkWriteError({ eventCount: 0, cause: "Use EventSink.withTransaction for an enclosing write transaction" });
    });
    const afterCommit = (effect: Effect.Effect<void>) => Effect.gen(function* () {
      const publications = yield* CommitPublications;
      if (publications === undefined) yield* effect;
      else publications.push(effect);
    });
    const withTransaction: EventSinkV2Shape["withTransaction"] = (effect) =>
      Effect.uninterruptibleMask((restore) => Effect.gen(function* () {
        yield* assertPublicationScope;
        const parent = yield* CommitPublications;
        const parentEvents = yield* TransactionWrittenEvents;
        const publications: Array<Effect.Effect<void>> = [];
        const writtenEvents = new Map<EventId, OrchestrationV2StoredEvent>();
        const result = yield* restore(sql.withTransaction(effect.pipe(
          Effect.provideService(CommitPublications, publications),
          Effect.provideService(TransactionWrittenEvents, writtenEvents),
        )));
        if (parentEvents !== undefined) for (const [id, stored] of writtenEvents) parentEvents.set(id, stored);
        if (parent !== undefined) parent.push(...publications);
        else yield* Effect.forEach(publications, (publication) => publication, { concurrency: 1, discard: true });
        return result;
      })).pipe(Effect.mapError((cause) => SqlError.isSqlError(cause)
        ? new EventSinkWriteError({ eventCount: 0, cause }) : cause));

    const withWorktreeOwnershipTransaction: EventSinkV2Shape["withWorktreeOwnershipTransaction"] = (effect) =>
      withTransaction(Effect.gen(function* () {
        // Reserve the SQLite writer before birth/path reads can pin a stale WAL snapshot.
        // The empty update changes no lease and also works before the first lease exists.
        yield* sql`UPDATE worktree_ownership_leases SET lease_id = lease_id WHERE 0`.pipe(
          Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause })),
        );
        return yield* effect;
      }));

    const readIncarnation = Effect.fnUntraced(function* (threadId: ThreadId) {
      const rows = yield* sql<{ readonly eventId: string; readonly sequence: number; readonly payload: string; readonly birthPayload: string }>`
        SELECT event.event_id AS "eventId", event.sequence, projection.payload_json AS payload, event.payload_json AS "birthPayload"
        FROM orchestration_events event
        JOIN orchestration_v2_projection_threads projection ON projection.thread_id = event.stream_id
        WHERE event.application_event_version = 2 AND event.aggregate_kind = 'thread'
          AND event.stream_id = ${threadId} AND event.event_type = 'thread.created'
        ORDER BY event.sequence ASC
      `;
      if (rows.length !== 1) return { incarnation: null, creationProvenance: "unavailable" as const };
      const row = rows[0]!;
      const decodeBirthIdentity = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Struct({
        id: ThreadId, historyOrigin: Schema.optional(Schema.String),
      })));
      const payload = yield* decodeBirthIdentity(row.payload);
      const birthPayload = yield* decodeBirthIdentity(row.birthPayload);
      if (payload.id !== threadId || birthPayload.id !== threadId) return { incarnation: null, creationProvenance: "unavailable" as const };
      const imports = yield* sql`SELECT thread_id FROM orchestration_v2_legacy_imports WHERE thread_id = ${threadId}`;
      if (payload.historyOrigin === "v1_import" || birthPayload.historyOrigin === "v1_import" || imports.length > 0 || row.eventId.startsWith("migration:v1:"))
        return { incarnation: null, creationProvenance: "legacy_import" as const };
      return {
        incarnation: yield* Schema.decodeUnknownEffect(NativeThreadIncarnationV2)({ eventId: row.eventId, sequence: row.sequence }),
        creationProvenance: "native_created" as const,
      };
    });
    const readIdentity = Effect.fnUntraced(function* (commandId: CommandId) {
      const rows = yield* sql`
        SELECT command_id AS "commandId", kind, version, command_type AS "commandType",
          aggregate_kind AS "aggregateKind", aggregate_id AS "aggregateId",
          normalized_command_digest AS "normalizedCommandDigest", binding_digest AS "bindingDigest"
        FROM orchestration_v2_native_command_identities WHERE command_id = ${commandId}
      `;
      return rows.length === 0 ? null : yield* Schema.decodeUnknownEffect(NativeCommandIdentityV2)(rows[0]);
    });
    const readWorkstreamWitness = Effect.fnUntraced(function* (commandId: CommandId) {
      const rows = yield* sql<{ readonly thread_id: string; readonly witness_json: string }>`
        SELECT thread_id, witness_json FROM orchestration_v2_workstream_settlement_witnesses WHERE command_id = ${commandId}`;
      if (rows.length === 0) return null;
      const witness = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(NativeWorkstreamSettlementWitnessV2))(rows[0]!.witness_json, { onExcessProperty: "error" });
      if ((witness.command.type !== "thread.settle" && witness.command.type !== "thread.unsettle") ||
          witness.command.commandId !== commandId || witness.command.threadId !== rows[0]!.thread_id ||
          (witness.provider !== null && (witness.provider.binding.threadId !== witness.command.threadId || witness.provider.binding.runtimeGeneration === null)))
        return yield* new NativeCommandPreconditionError({ commandId, reason: "unknown_evidence" });
      const identity = yield* readIdentity(commandId);
      const receipt = Option.getOrNull(yield* commandReceipts.getByCommandId(commandId));
      if (identity?.kind !== "workstream_settlement" || receipt === null || receipt.threadId !== witness.command.threadId ||
          receipt.commandType !== witness.command.type || identity.aggregateId !== witness.command.threadId || identity.commandType !== witness.command.type ||
          identity.normalizedCommandDigest !== nativeCreationSha256(nativeCreationCanonicalJson(witness.command)) ||
          identity.bindingDigest !== nativeWorkstreamSettlementWitnessBindingDigestV2(witness))
        return yield* new NativeCommandPreconditionError({ commandId, reason: "unknown_evidence" });
      return witness;
    });
    const readApplicationBirthRecordEffect = Effect.fnUntraced(function* (threadId: ThreadId) {
      const births = yield* sql<{ readonly event_id: string; readonly sequence: number; readonly payload_json: string }>`
        SELECT event_id, sequence, payload_json FROM orchestration_events WHERE application_event_version = 2
          AND aggregate_kind = 'thread' AND stream_id = ${threadId} AND event_type = 'thread.created'
        ORDER BY sequence DESC LIMIT 1`;
      if (births.length === 0) return null;
      const current = yield* sql<{ readonly payload_json: string }>`SELECT payload_json FROM orchestration_v2_projection_threads
        WHERE thread_id = ${threadId} AND json_extract(payload_json, '$.deletedAt') IS NULL`;
      if (current.length !== 1) return null;
      const identity = Schema.fromJsonString(Schema.Struct({ id: ThreadId, projectId: ProjectId, createdAt: Schema.String }));
      const birth = yield* Schema.decodeUnknownEffect(identity)(births[0]!.payload_json);
      const projection = yield* Schema.decodeUnknownEffect(identity)(current[0]!.payload_json);
      if (birth.id !== threadId || projection.id !== threadId || birth.projectId !== projection.projectId || birth.createdAt !== projection.createdAt) return null;
      return { kind: "application_v2_thread_birth", threadId, eventId: EventId.make(births[0]!.event_id), sequence: births[0]!.sequence } satisfies ApplicationThreadBirthV2;
    });
    const readApplicationThreadBirth: EventSinkV2Shape["readApplicationThreadBirth"] = (threadId) => sql.withTransaction(Effect.gen(function* () {
      return (yield* readCurrentProviderRuntimeOwnerEffect(threadId)) === null ? null : yield* readApplicationBirthRecordEffect(threadId);
    })).pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause })));
    const readThreadDeletionCommandEffect = Effect.fnUntraced(function* (commandId: CommandId) {
      const rows = yield* sql<{ readonly thread_id: string; readonly canonical_command_json: string; readonly command_digest: string;
        readonly owner_birth_json: string; readonly worktree_inventory_json: string; readonly deletion_event_id: string;
        readonly deletion_event_sequence: number; readonly recorded_at: string }>`
        SELECT * FROM orchestration_v2_thread_deletion_commands WHERE command_id = ${commandId}`;
      if (rows.length === 0) return null;
      const row = rows[0]!;
      const record = yield* Schema.decodeUnknownEffect(ThreadDeletionCommandRecordSchemaV1)({ command: JSON.parse(row.canonical_command_json),
        commandDigest: row.command_digest, ownerBirth: JSON.parse(row.owner_birth_json).birth, inventory: JSON.parse(row.worktree_inventory_json),
        deletion: { commandId, eventId: row.deletion_event_id, sequence: row.deletion_event_sequence }, recordedAt: row.recorded_at }, { onExcessProperty: "error" });
      const command = record.command;
      const receipt = Option.getOrNull(yield* commandReceipts.getByCommandId(commandId));
      const events = yield* sql<{ readonly payload_json: string }>`SELECT payload_json FROM orchestration_events
        WHERE event_id = ${record.deletion.eventId} AND sequence = ${record.deletion.sequence} AND command_id = ${commandId}
          AND application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ${row.thread_id} AND event_type = 'thread.deleted'`;
      if (command.type !== "thread.delete" || command.commandId !== commandId || command.threadId !== row.thread_id ||
          record.commandDigest !== nativeCreationSha256(nativeCreationCanonicalJson(Schema.encodeSync(OrchestrationV2Command)(command))) ||
          receipt?.status !== "accepted" || receipt.commandType !== command.type || receipt.threadId !== command.threadId ||
          receipt.resultSequence < record.deletion.sequence || events.length !== 1)
        return yield* new EventSinkWriteError({ eventCount: 0, commandId, cause: "Original deletion command lost its accepted event association" });
      const deleted = JSON.parse(events[0]!.payload_json);
      if (deleted.id !== command.threadId || deleted.projectId !== record.inventory.worktree.projectId ||
          deleted.branch !== record.inventory.worktree.branch ||
          (deleted.worktreePath === null || record.inventory.projectRoot === null ? null : NodePath.resolve(record.inventory.projectRoot, deleted.worktreePath)) !== record.inventory.worktree.path)
        return yield* new EventSinkWriteError({ eventCount: 0, commandId, cause: "Original deletion inventory differs from its accepted application path" });
      return { ...record, command } satisfies ThreadDeletionCommandRecordV1;
    });
    const captureDeletionWorktreeInventory = Effect.fnUntraced(function* (
      command: Extract<OrchestrationV2Command, { readonly type: "thread.delete" }>,
      deleted: Extract<OrchestrationV2DomainEvent, { readonly payload: OrchestrationV2AppThread }>,
      effects: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2>,
      policy: DeletionWorktreePolicyCaptureV1 | undefined,
    ) {
      const ownerBirth = yield* readApplicationBirthRecordEffect(command.threadId);
      const current = yield* projectionStore.getThreadProjection(command.threadId).pipe(Effect.option, Effect.map(Option.getOrNull));
      const projectResult = yield* projectStore.get(deleted.payload.projectId).pipe(Effect.result);
      const project = projectResult._tag === "Success" ? Option.getOrNull(projectResult.success) : null;
      const projectRoot = project === null || !NodePath.isAbsolute(project.workspaceRoot) ? null : NodePath.resolve(project.workspaceRoot);
      const path = deleted.payload.worktreePath === null || projectRoot === null || deleted.payload.worktreePath.includes("\0") ? null :
        NodePath.resolve(projectRoot, deleted.payload.worktreePath);
      const leaseRows = path === null ? null : yield* sql`SELECT resource_path AS "resourcePath", lease_id AS "leaseId", owner_thread_id AS "ownerThreadId",
        owner_incarnation AS "ownerIncarnation", branch, acquired_at_ms AS "acquiredAtMs", renewed_at_ms AS "renewedAtMs", expires_at_ms AS "expiresAtMs"
        FROM worktree_ownership_leases WHERE resource_path = ${path}`.pipe(Effect.option);
      const leases = leaseRows === null || Option.isNone(leaseRows) ? null : yield* Schema.decodeUnknownEffect(Schema.Array(CleanupLeaseSchemaV2))(leaseRows.value)
        .pipe(Effect.option, Effect.map(Option.getOrNull));
      const incarnation = ownerBirth === null ? null : JSON.stringify(["t3.orchestration-v2.thread-birth/v1", ownerBirth.eventId, ownerBirth.sequence]);
      const matchingLease = leases?.length === 1 && leases[0]!.ownerThreadId === command.threadId && leases[0]!.ownerIncarnation === incarnation &&
        (leases[0]!.branch === null || leases[0]!.branch === deleted.payload.branch) ? leases[0]! : null;
      const leaseInventory: typeof DeletionWorktreeLeaseInventoryV1.Type = leases === null ? { status: "unavailable" } : leases.length === 0 ?
        { status: "absent" } : matchingLease === null ? { status: "conflict", leases } : { status: "original", lease: matchingLease };
      let reason: string | null = ownerBirth === null || current === null ? "original_application_birth_unavailable" :
        current.thread.id !== deleted.payload.id || current.thread.projectId !== deleted.payload.projectId ||
        current.thread.worktreePath !== deleted.payload.worktreePath || current.thread.branch !== deleted.payload.branch ? "deletion_path_changed" :
        projectRoot === null || path === null ? "project_worktree_inventory_unavailable" :
        path === projectRoot ? "project_root_retained" : leaseInventory.status === "unavailable" || leaseInventory.status === "conflict" ? "original_lease_inventory_unavailable" : null;
      const consent = command.worktreeRemoval;
      if (consent !== undefined && (consent.projectId !== deleted.payload.projectId || consent.branch !== deleted.payload.branch ||
          consent.path.includes("\0") || projectRoot === null || NodePath.resolve(projectRoot, consent.path) !== path)) reason = "worktree_consent_mismatch";
      let request: DeletionWorktreeCleanupRequestV1 | undefined = consent === undefined ? undefined : { origin: "explicit", consent };
      if (consent === undefined && policy !== undefined) {
        if (policy.status === "unavailable") reason = "worktree_policy_capture_unavailable";
        else {
          request = yield* Schema.decodeUnknownEffect(DeletionWorktreeCleanupRequestV1)(policy.request, { onExcessProperty: "error" });
          const currentRules = yield* policy.revalidate.pipe(Effect.result);
          if (request.origin !== "policy" || request.projectId !== deleted.payload.projectId || request.branch !== deleted.payload.branch ||
              request.path.includes("\0") || projectRoot === null || NodePath.resolve(projectRoot, request.path) !== path)
            reason = "worktree_policy_target_mismatch";
          else if (currentRules._tag === "Failure" || !request.rules.worktreeOnDelete ||
              nativeCreationCanonicalJson(currentRules.success) !== nativeCreationCanonicalJson(request.rules))
            reason = "worktree_policy_capture_unavailable";
        }
      }
      if (path !== null) {
        const threads = yield* sql<{ readonly thread_id: string; readonly payload_json: string }>`
          SELECT thread_id, payload_json FROM orchestration_v2_projection_threads WHERE thread_id <> ${command.threadId}`;
        for (const row of threads) {
          const other = JSON.parse(row.payload_json);
          if (other.deletedAt === null && typeof other.worktreePath === "string") {
            const otherProject = Option.getOrNull(yield* projectStore.get(ProjectId.make(other.projectId)));
            if (otherProject !== null && NodePath.resolve(otherProject.workspaceRoot, other.worktreePath) === path) reason = "shared_worktree_retained";
          }
        }
        for (const other of yield* projectStore.list()) if (NodePath.resolve(other.workspaceRoot) === path) reason = "project_root_retained";
      }
      const pending = yield* sql<{ readonly effect_id: string }>`SELECT effect_id FROM orchestration_v2_effect_outbox
        WHERE thread_id = ${command.threadId} AND status IN ('pending', 'running') ORDER BY effect_id`;
      const prerequisiteEffectIds = [...new Set([...effects.map((effect) => effect.id), ...pending.map((effect) => effect.effect_id)])].sort();
      const inventory: typeof DeletionWorktreeInventorySchemaV1.Type = { worktree: { projectId: deleted.payload.projectId, path, branch: deleted.payload.branch },
        projectRoot, leaseInventory, prerequisiteEffectIds, ...(request === undefined ? {} : { request }),
        captureStatus: reason === null ? "captured" : "retained", reason };
      return { ownerBirth, inventory };
    });
    const readDeletionWorktreeTaskEffect = Effect.fnUntraced(function* (effectId: string) {
      const rows = yield* sql<{ readonly thread_id: string; readonly lease_json: string; readonly owner_birth_json: string;
        readonly deletion_json: string; readonly task_json: string; readonly binding_sha256: string; readonly recorded_at: string }>`
        SELECT * FROM orchestration_v2_lease_cleanup_task_bindings WHERE effect_id = ${effectId}`;
      if (rows.length === 0) return null;
      const row = rows[0]!;
      const task = JSON.parse(row.task_json);
      if (task.kind !== "worktree") return null;
      const binding = yield* Schema.decodeUnknownEffect(DeletionWorktreeTaskBindingV1)({ version: 1, effectId, threadId: row.thread_id,
        leaseInventory: JSON.parse(row.lease_json), ownerBirth: JSON.parse(row.owner_birth_json).birth,
        deletion: JSON.parse(row.deletion_json), task, bindingSha256: row.binding_sha256, recordedAt: row.recorded_at }, { onExcessProperty: "error" });
      const { bindingSha256, recordedAt: _recordedAt, ...subject } = binding;
      const command = binding.task.canonicalCommand;
      const original = yield* readThreadDeletionCommandEffect(binding.deletion.commandId);
      const request = deletionWorktreeCleanupRequestV1(binding);
      const originalRequest = original?.inventory.request ?? (original?.command.worktreeRemoval === undefined ? null :
        { origin: "explicit", consent: original.command.worktreeRemoval });
      const effect = Option.getOrNull(yield* effectOutbox.get(effectId));
      const receipt = Option.getOrNull(yield* commandReceipts.getByCommandId(binding.deletion.commandId));
      const events = yield* sql<{ readonly payload_json: string }>`SELECT payload_json FROM orchestration_events
        WHERE event_id = ${binding.deletion.eventId} AND sequence = ${binding.deletion.sequence}
          AND application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ${binding.threadId}
          AND event_type = 'thread.deleted' AND command_id = ${binding.deletion.commandId}`;
      if (original === null || nativeCreationCanonicalJson(original.command) !== nativeCreationCanonicalJson(command) ||
          nativeCreationCanonicalJson(original.ownerBirth) !== nativeCreationCanonicalJson(binding.ownerBirth) ||
          nativeCreationCanonicalJson(original.inventory.leaseInventory) !== nativeCreationCanonicalJson(binding.leaseInventory) ||
          nativeCreationCanonicalJson(original.deletion) !== nativeCreationCanonicalJson(binding.deletion) ||
          bindingSha256 !== deletionWorktreeTaskBindingDigestV1(subject) || command.type !== "thread.delete" ||
          command.commandId !== binding.deletion.commandId || command.threadId !== binding.threadId || request === null ||
          nativeCreationCanonicalJson(request) !== nativeCreationCanonicalJson(originalRequest) ||
          (request.origin === "explicit" ? command.worktreeRemoval === undefined ||
            nativeCreationCanonicalJson(command.worktreeRemoval) !== nativeCreationCanonicalJson(request.consent) ||
            (binding.task.consent !== undefined && nativeCreationCanonicalJson(binding.task.consent) !== nativeCreationCanonicalJson(request.consent)) :
            command.worktreeRemoval !== undefined || binding.task.consent !== undefined) ||
          nativeCreationSha256(nativeCreationCanonicalJson(Schema.encodeSync(OrchestrationV2Command)(command))) !== binding.task.commandDigest ||
          effect === null || effect.id !== deletionWorktreeEffectIdV1(command.commandId, binding.threadId) ||
          effect.commandId !== command.commandId || effect.threadId !== binding.threadId || effect.request.type !== "worktree.cleanup" ||
          receipt?.status !== "accepted" || receipt.commandType !== "thread.delete" || receipt.threadId !== binding.threadId ||
          receipt.resultSequence < binding.deletion.sequence || events.length !== 1)
        return yield* new EventSinkWriteError({ eventCount: 0, cause: "Worktree cleanup lost its original command/deletion/task association" });
      const deleted = JSON.parse(events[0]!.payload_json);
      const expectedPath = deleted.worktreePath === null || binding.task.projectRoot === null ? null :
        NodePath.resolve(binding.task.projectRoot, deleted.worktreePath);
      if (deleted.id !== binding.threadId || deleted.projectId !== binding.task.worktree.projectId ||
          expectedPath !== binding.task.worktree.path || deleted.branch !== binding.task.worktree.branch)
        return yield* new EventSinkWriteError({ eventCount: 0, cause: "Worktree cleanup differs from its original application path" });
      if (binding.ownerBirth !== null) {
        const births = yield* sql`SELECT event_id FROM orchestration_events WHERE event_id = ${binding.ownerBirth.eventId}
          AND sequence = ${binding.ownerBirth.sequence} AND application_event_version = 2 AND aggregate_kind = 'thread'
          AND stream_id = ${binding.threadId} AND event_type = 'thread.created' AND sequence < ${binding.deletion.sequence}
          AND json_extract(payload_json, '$.id') = ${binding.threadId}
          AND json_extract(payload_json, '$.projectId') = ${binding.task.worktree.projectId}
          AND NOT EXISTS (SELECT 1 FROM orchestration_events next WHERE next.application_event_version = 2
            AND next.aggregate_kind = 'thread' AND next.stream_id = ${binding.threadId} AND next.event_type = 'thread.created'
            AND next.sequence > ${binding.ownerBirth.sequence} AND next.sequence <= ${binding.deletion.sequence})`;
        if (binding.ownerBirth.threadId !== binding.threadId || births.length !== 1)
          return yield* new EventSinkWriteError({ eventCount: 0, cause: "Worktree cleanup lost its original application birth" });
      } else if (binding.task.captureStatus !== "retained")
        return yield* new EventSinkWriteError({ eventCount: 0, cause: "An unavailable application birth cannot authorize worktree cleanup" });
      if (binding.leaseInventory.status === "original" && (binding.ownerBirth === null ||
          binding.leaseInventory.lease.ownerThreadId !== binding.threadId || binding.leaseInventory.lease.resourcePath !== binding.task.worktree.path ||
          binding.leaseInventory.lease.ownerIncarnation !== JSON.stringify(["t3.orchestration-v2.thread-birth/v1",
            binding.ownerBirth.eventId, binding.ownerBirth.sequence])))
        return yield* new EventSinkWriteError({ eventCount: 0, cause: "Worktree cleanup lost its full original lease" });
      return binding;
    });
    const qualifyDeletionCleanupObservation = Effect.fnUntraced(function* (
      binding: DeletionWorktreeTaskBindingV1 | DeletionCleanupTaskBindingV1, observation: DeletionCleanupObservationV1,
    ) {
      let result: LeaseCleanupTaskOutcomeV2 = { taskId: binding.effectId, result: null, effect: "unknown" };
      if ("start" in observation) {
        if (binding.task.kind !== "worktree") return yield* new EventSinkWriteError({ eventCount: 0, cause: "Git observation belongs to a different cleanup task" });
        const worktreeBinding = yield* Schema.decodeUnknownEffect(DeletionWorktreeTaskBindingV1)(binding, { onExcessProperty: "error" });
        const rows = yield* sql<{ readonly ordinal: number; readonly correlation_json: string }>`SELECT ordinal, correlation_json
          FROM orchestration_v2_lease_cleanup_task_outcomes WHERE effect_id = ${binding.effectId}
          AND json_extract(correlation_json, '$.evidence.schema') = 't3.deletion-worktree-removal-start/v1'`;
        const admission = yield* sql<{ readonly state: string; readonly started_at: string | null; readonly subject_json: string }>`
          SELECT state, started_at, subject_json FROM orchestration_v2_worktree_path_admissions WHERE operation_id = ${binding.effectId}`;
        const start = observation.start;
        if (rows.length !== 1 || rows[0]!.ordinal !== observation.startOrdinal || admission.length !== 1 ||
            !["started", "unknown", "completed", "released"].includes(admission[0]!.state) || admission[0]!.started_at !== start.startedAt ||
            nativeCreationCanonicalJson(JSON.parse(rows[0]!.correlation_json).evidence) !== nativeCreationCanonicalJson(start) ||
            JSON.parse(admission[0]!.subject_json).bindingSha256 !== binding.bindingSha256 ||
            start.effectId !== binding.effectId || start.bindingSha256 !== binding.bindingSha256 ||
            nativeCreationCanonicalJson(start.target) !== nativeCreationCanonicalJson(deletionWorktreeRemovalTargetV1(worktreeBinding)) ||
            !Number.isFinite(Date.parse(observation.observedAt)) || Date.parse(observation.observedAt) < Date.parse(start.startedAt))
          return yield* new EventSinkWriteError({ eventCount: 0, cause: "Git observation lost its persisted original removal start" });
        const target = start.target;
        for (const readback of [observation.before, observation.after]) {
          if (readback.filesystem.path !== target.path || (readback.registration.status === "complete" &&
              (readback.registration.projectRoot !== target.projectRoot || !NodePath.isAbsolute(readback.registration.gitCommonDirectory) ||
                readback.registration.entries.some((entry) => !NodePath.isAbsolute(entry.path) || NodePath.resolve(entry.path) !== entry.path) ||
                new Set(readback.registration.entries.map((entry) => entry.path)).size !== readback.registration.entries.length)))
            return yield* new EventSinkWriteError({ eventCount: 0, cause: "Git readback differs from the original canonical target" });
        }
        const before = observation.before.registration;
        const after = observation.after.registration;
        const absence = before.status === "complete" && after.status === "complete" &&
          before.gitCommonDirectory === after.gitCommonDirectory && observation.after.filesystem.status === "absent" &&
          !after.entries.some((entry) => entry.path === target.path);
        const entry = before.status === "complete" ? before.entries.find((candidate) => candidate.path === target.path) : undefined;
        const registeredTarget = entry !== undefined && !entry.bare && target.path !== target.projectRoot &&
          (entry.branch === target.branch || entry.branch === (target.branch === null ? null : `refs/heads/${target.branch}`));
        if (absence && observation.operation.kind === "executed" && observation.operation.completion === "exited" &&
            observation.operation.exitCode === 0 && registeredTarget && observation.before.filesystem.status === "present")
          result = { taskId: binding.effectId, result: "succeeded", effect: "confirmed" };
        else if (absence && (observation.operation.kind === "reconciled" || (observation.operation.kind === "already_absent" &&
            entry === undefined && observation.before.filesystem.status === "absent")))
          result = { taskId: binding.effectId, result: "succeeded", effect: "absent" };
        return { outcome: result, workerId: start.workerId, expectedAttempt: start.expectedAttempt, producer: "worktree" as const };
      }
      if (observation.kind === "managed_provider") {
        const operation = observation.nativeOperation;
        if (binding.task.kind !== "provider" || observation.effectId !== binding.effectId ||
            observation.bindingSha256 !== binding.bindingSha256 || observation.evidenceRevision !== binding.task.evidenceRevision ||
            nativeCreationCanonicalJson(observation.binding) !== nativeCreationCanonicalJson(binding.task.expectedBinding) ||
            operation.operationId !== binding.effectId || operation.operation !== "close_session" ||
            operation.threadId !== binding.threadId || operation.providerThreadId !== observation.binding.providerThreadId ||
            operation.providerSessionId !== observation.binding.providerSessionId || operation.instanceId !== observation.binding.instanceId ||
            operation.runtimeGeneration !== observation.binding.runtimeGeneration ||
            !Number.isFinite(Date.parse(observation.observedAt)) || Date.parse(observation.observedAt) < Date.parse(binding.recordedAt))
          return yield* new EventSinkWriteError({ eventCount: 0, cause: "Managed provider observation differs from its original operation and pinned task" });
        if (observation.result.status === "stopped") {
          if (observation.result.operationId !== binding.effectId || observation.result.readback.threadAttached !== false ||
              nativeCreationCanonicalJson(observation.result.binding) !== nativeCreationCanonicalJson(binding.task.expectedBinding) ||
              new Set(observation.result.interruptedProviderTurnIds).size !== observation.result.interruptedProviderTurnIds.length)
            return yield* new EventSinkWriteError({ eventCount: 0, cause: "Managed provider stop readback belongs to a different captured runtime" });
          result = { taskId: binding.effectId, result: "succeeded", effect: "confirmed" };
        }
        return { outcome: result, workerId: observation.workerId, expectedAttempt: observation.expectedAttempt, producer: "managed_provider" as const };
      }
      if (binding.task.kind !== "terminal" || observation.effectId !== binding.effectId ||
          observation.bindingSha256 !== binding.bindingSha256 ||
          nativeCreationCanonicalJson(observation.capture) !== nativeCreationCanonicalJson(binding.task.capture) ||
          !Number.isFinite(Date.parse(observation.observedAt)) || Date.parse(observation.observedAt) < Date.parse(binding.recordedAt))
        return yield* new EventSinkWriteError({ eventCount: 0, cause: "Managed terminal observation differs from its issued owner capture" });
      if (observation.result.status === "closed" && observation.result.processExitObserved)
        result = { taskId: binding.effectId, result: "succeeded", effect: "confirmed" };
      else if (observation.result.status === "observed_absent") result = { taskId: binding.effectId, result: "succeeded", effect: "absent" };
      return { outcome: result, workerId: observation.workerId, expectedAttempt: observation.expectedAttempt, producer: "managed_terminal" as const };
    });
    const cleanupHoldMatchesObservation = (
      binding: DeletionWorktreeTaskBindingV1 | DeletionCleanupTaskBindingV1,
      observation: DeletionCleanupObservationV1, hold: EffectOutbox.UnknownEffectHoldV2,
    ) => {
      if (!("start" in observation) && observation.kind === "managed_provider") {
        if (binding.task.kind !== "provider" || !("operation" in hold.evidence) || hold.evidence.outcome !== "unknown") return false;
        const { outcome: _outcome, ...operation } = hold.evidence;
        return nativeCreationCanonicalJson(operation) === nativeCreationCanonicalJson(Schema.encodeSync(ProviderNativeOperationContext)(observation.nativeOperation));
      }
      return "kind" in hold.evidence && hold.evidence.kind === "resource_cleanup" &&
        hold.evidence.bindingSha256 === binding.bindingSha256 && hold.evidence.taskKind === binding.task.kind;
    };
    const readQualifiedDeletionCleanupOutcomeEffect = Effect.fnUntraced(function* (effectId: string) {
      const rows = yield* sql<{ readonly ordinal: number; readonly outcome_json: string; readonly correlation_json: string }>`
        SELECT ordinal, outcome_json, correlation_json FROM orchestration_v2_lease_cleanup_task_outcomes
        WHERE effect_id = ${effectId} ORDER BY ordinal DESC LIMIT 1`;
      if (rows.length === 0) return null;
      const row = rows[0]!;
      const raw = JSON.parse(row.correlation_json);
      if (raw.evidence?.schema !== "t3.deletion-cleanup-observation/v1") return null;
      const correlation = yield* Schema.decodeUnknownEffect(Schema.Struct({ workerId: Schema.NonEmptyString,
        expectedAttempt: Schema.Int.check(Schema.isGreaterThan(0)), bindingSha256: LowerSha256,
        evidence: EffectOutbox.QualifiedDeletionCleanupEvidenceV1 }))(raw, { onExcessProperty: "error" });
      const observation = yield* Schema.decodeUnknownEffect(DeletionCleanupObservationV1)(correlation.evidence.observation, { onExcessProperty: "error" });
      const outcome = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(LeaseCleanupTaskOutcomeV2))(row.outcome_json, { onExcessProperty: "error" });
      const binding = (yield* readDeletionCleanupTaskEffect(effectId)) ?? (yield* readDeletionWorktreeTaskEffect(effectId));
      if (binding === null || correlation.bindingSha256 !== binding.bindingSha256)
        return yield* new EventSinkWriteError({ eventCount: 0, cause: "Qualified cleanup outcome lost its original task" });
      const qualified = yield* qualifyDeletionCleanupObservation(binding, observation);
      if (nativeCreationCanonicalJson(qualified.outcome) !== nativeCreationCanonicalJson(outcome) ||
          qualified.workerId !== correlation.workerId || qualified.expectedAttempt !== correlation.expectedAttempt ||
          qualified.producer !== correlation.evidence.producer)
        return yield* new EventSinkWriteError({ eventCount: 0, cause: "Stored cleanup outcome differs from its finite producer evidence" });
      const holds = (yield* effectOutbox.listHeldByThreadId(binding.threadId)).filter((hold) => hold.effectId === effectId);
      if (correlation.evidence.coveredHolds.some((hold, index) => hold.effectId !== effectId || hold.threadId !== binding.threadId ||
          hold.workerId !== qualified.workerId || hold.expectedAttempt !== qualified.expectedAttempt || hold.operationId !== effectId ||
          !cleanupHoldMatchesObservation(binding, observation, hold) || !holds.some((stored) => nativeCreationCanonicalJson(stored) === nativeCreationCanonicalJson(hold)) ||
          correlation.evidence.coveredHolds.slice(0, index).some((earlier) => earlier.effectId === hold.effectId)))
        return yield* new EventSinkWriteError({ eventCount: 0, cause: "Stored cleanup coverage differs from its immutable operation holds" });
      const evidence = { ...correlation.evidence, observation };
      return { ordinal: row.ordinal, outcome, bindingSha256: correlation.bindingSha256, evidence, correlation };
    });
    const readUnresolvedDeletionCleanupHoldsEffect = Effect.fnUntraced(function* (threadId: ThreadId) {
      const holds = yield* effectOutbox.listHeldByThreadId(threadId);
      const unresolved: EffectOutbox.UnknownEffectHoldV2[] = [];
      for (const hold of holds) {
        const latest = yield* readQualifiedDeletionCleanupOutcomeEffect(hold.effectId);
        if (latest?.outcome.result !== "succeeded" || !["confirmed", "absent"].includes(latest.outcome.effect) ||
            !latest.evidence.coveredHolds.some((covered) => nativeCreationCanonicalJson(covered) === nativeCreationCanonicalJson(hold)))
          unresolved.push(hold);
      }
      return unresolved;
    });
    const readDeletionWorktreeOutcomes = Effect.fnUntraced(function* (binding: DeletionWorktreeTaskBindingV1) {
      const rows = yield* sql<{ readonly outcome_json: string; readonly correlation_json: string; readonly ordinal: number }>`
        SELECT outcome_json, correlation_json, ordinal FROM orchestration_v2_lease_cleanup_task_outcomes
        WHERE effect_id = ${binding.effectId} ORDER BY ordinal`;
      return yield* Effect.forEach(rows, (row) => Effect.gen(function* () {
        const outcome = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(LeaseCleanupTaskOutcomeV2))(row.outcome_json, { onExcessProperty: "error" });
        const correlation = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Struct({ workerId: Schema.NonEmptyString,
          expectedAttempt: Schema.Int.check(Schema.isGreaterThan(0)), bindingSha256: LowerSha256,
          evidence: Schema.Record(Schema.String, Schema.Unknown) })))(row.correlation_json, { onExcessProperty: "error" });
        if (outcome.taskId !== binding.effectId || correlation.bindingSha256 !== binding.bindingSha256)
          return yield* new EventSinkWriteError({ eventCount: 0, cause: "Worktree outcome differs from its immutable task" });
        let start: DeletionWorktreeRemovalStartV1 | null = null;
        if (correlation.evidence.schema === "t3.deletion-worktree-removal-start/v1") {
          start = yield* Schema.decodeUnknownEffect(DeletionWorktreeRemovalStartV1)(correlation.evidence, { onExcessProperty: "error" });
          if (row.ordinal !== 0 || start.effectId !== binding.effectId || start.bindingSha256 !== binding.bindingSha256 ||
              start.workerId !== correlation.workerId || start.expectedAttempt !== correlation.expectedAttempt ||
              nativeCreationCanonicalJson(start.target) !== nativeCreationCanonicalJson(deletionWorktreeRemovalTargetV1(binding)) ||
              outcome.result !== null || outcome.effect !== "unknown")
            return yield* new EventSinkWriteError({ eventCount: 0, cause: "Worktree start differs from its exact claimed target" });
        }
        return { ordinal: row.ordinal, outcome, correlation, start };
      }), { concurrency: 1 });
    });
    const readDeletionWorktreePathAdmissionEffect = Effect.fnUntraced(function* (input: { readonly path: string }) {
      const path = NodePath.resolve(input.path);
      if (!NodePath.isAbsolute(input.path) || input.path.includes("\0") || input.path.trim().length === 0)
        return { path, status: "unavailable" as const, reservations: [], admissions: [] };
      const admissionRows = yield* sql<{ readonly operation_id: string; readonly canonical_path: string; readonly kind: string;
        readonly subject_json: string; readonly state: string; readonly started_at: string | null; readonly outcome_json: string | null;
        readonly recorded_at: string; readonly updated_at: string }>`SELECT * FROM orchestration_v2_worktree_path_admissions
        WHERE state NOT IN ('no_effect', 'released') ORDER BY operation_id`;
      const admissions: WorktreePathAdmissionV1[] = [];
      for (const row of admissionRows) {
        const admission = yield* Schema.decodeUnknownEffect(WorktreePathAdmissionV1)({ operationId: row.operation_id, path: row.canonical_path,
          kind: row.kind, subject: JSON.parse(row.subject_json), state: row.state, startedAt: row.started_at,
          outcome: row.outcome_json === null ? null : JSON.parse(row.outcome_json), recordedAt: row.recorded_at, updatedAt: row.updated_at }, { onExcessProperty: "error" });
        if (NodePath.resolve(admission.path) !== admission.path)
          return { path, status: "unavailable" as const, reservations: [], admissions };
        if (path === admission.path || path.startsWith(`${admission.path}${NodePath.sep}`) || admission.path.startsWith(`${path}${NodePath.sep}`))
          admissions.push(admission);
      }
      const rows = yield* sql<{ readonly effect_id: string }>`SELECT effect_id FROM orchestration_v2_lease_cleanup_task_bindings
        WHERE json_extract(task_json, '$.kind') = 'worktree' ORDER BY effect_id`;
      const reservations: DeletionWorktreeTaskBindingV1[] = [];
      for (const row of rows) {
        const binding = yield* readDeletionWorktreeTaskEffect(row.effect_id);
        if (binding === null) return { path, status: "unavailable" as const, reservations, admissions };
        if (binding.task.worktree.path === null) {
          const request = deletionWorktreeCleanupRequestV1(binding);
          if (request === null) return { path, status: "unavailable" as const, reservations, admissions };
          const assertedPath = request.origin === "explicit" ? request.consent.path : request.path;
          if (NodePath.isAbsolute(assertedPath) && (path === NodePath.resolve(assertedPath) ||
              path.startsWith(`${NodePath.resolve(assertedPath)}${NodePath.sep}`) || NodePath.resolve(assertedPath).startsWith(`${path}${NodePath.sep}`)))
            return { path, status: "unavailable" as const, reservations, admissions };
          continue;
        }
        if (yield* readDeletionWorktreeRetirementEffect(binding)) continue;
        const reservedPath = binding.task.worktree.path;
        if (path === reservedPath || path.startsWith(`${reservedPath}${NodePath.sep}`) || reservedPath.startsWith(`${path}${NodePath.sep}`))
          reservations.push(binding);
      }
      const unavailablePolicyRows = yield* sql<{ readonly command_id: string }>`SELECT command_id FROM orchestration_v2_thread_deletion_commands
        WHERE json_extract(worktree_inventory_json, '$.reason') = 'worktree_policy_capture_unavailable'
          AND json_extract(worktree_inventory_json, '$.request') IS NULL`;
      for (const row of unavailablePolicyRows) {
        const original = yield* readThreadDeletionCommandEffect(CommandId.make(row.command_id));
        const unavailablePath = original?.inventory.worktree.path;
        if (unavailablePath !== undefined && unavailablePath !== null && (path === unavailablePath ||
            path.startsWith(`${unavailablePath}${NodePath.sep}`) || unavailablePath.startsWith(`${path}${NodePath.sep}`)))
          return { path, status: "unavailable" as const, reservations, admissions };
      }
      return { path, status: reservations.length === 0 && admissions.length === 0 ? "available" as const : "reserved" as const, reservations, admissions };
    });
    const readDeletionWorktreeRetirementEffect = Effect.fnUntraced(function* (binding: DeletionWorktreeTaskBindingV1) {
      const rows = yield* sql<{ readonly state: string; readonly subject_json: string; readonly outcome_json: string | null }>`
        SELECT state, subject_json, outcome_json FROM orchestration_v2_worktree_path_admissions WHERE operation_id = ${binding.effectId}`;
      if (rows.length !== 1 || rows[0]!.state !== "released" || rows[0]!.outcome_json === null) return false;
      const retirement = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Struct({ schema: Schema.Literal("t3.deletion-worktree-retirement/v1"),
        effectId: Schema.NonEmptyString, bindingSha256: LowerSha256, qualifiedOrdinal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
        originalLeaseInventory: DeletionWorktreeLeaseInventoryV1, finalizedAt: CapturedRestartIsoTimestampV1 })))(rows[0]!.outcome_json, { onExcessProperty: "error" });
      const latest = yield* readQualifiedDeletionCleanupOutcomeEffect(binding.effectId);
      const effect = Option.getOrNull(yield* effectOutbox.get(binding.effectId));
      const holds = yield* readUnresolvedDeletionCleanupHoldsEffect(binding.threadId);
      return retirement.effectId === binding.effectId && retirement.bindingSha256 === binding.bindingSha256 &&
        JSON.parse(rows[0]!.subject_json).bindingSha256 === binding.bindingSha256 &&
        nativeCreationCanonicalJson(retirement.originalLeaseInventory) === nativeCreationCanonicalJson(binding.leaseInventory) &&
        latest?.ordinal === retirement.qualifiedOrdinal && latest.outcome.result === "succeeded" &&
        ["confirmed", "absent"].includes(latest.outcome.effect) && effect?.status === "succeeded" &&
        !holds.some((hold) => hold.effectId === binding.effectId || binding.task.prerequisiteEffectIds.includes(hold.effectId));
    });
    const assertDeletionWorktreePathWritable = Effect.fnUntraced(function* (path: string) {
      const admission = yield* readDeletionWorktreePathAdmissionEffect({ path });
      if (admission.status !== "available") return yield* new EventSinkWriteError({ eventCount: 0,
        cause: "A pending or uncertain worktree removal reserves this application path" });
    });
    const observeThreadDeletionCleanup: EventSinkV2Shape["observeThreadDeletionCleanup"] = (input) => sql.withTransaction(Effect.gen(function* () {
      const receipt = Option.getOrNull(yield* commandReceipts.getByCommandId(input.commandId));
      let result: OrchestrationV2ThreadDeletionCleanupObservation = { ...input, receipt: null, deletion: null, worktree: null,
        state: "not_found", removalOutcome: null, currentLease: "unavailable", reason: null };
      if (receipt === null || receipt.threadId !== input.threadId || receipt.commandType !== "thread.delete") return result;
      result = { ...result, receipt };
      const original = yield* readThreadDeletionCommandEffect(input.commandId);
      if (original === null) return { ...result, state: "unknown" as const, reason: "original_worktree_consent_unavailable" };
      result = { ...result, deletion: { eventId: original.deletion.eventId, sequence: original.deletion.sequence, resultSequence: receipt.resultSequence } };
      if (original.command.worktreeRemoval === undefined && original.inventory.request === undefined)
        return original.inventory.reason === "worktree_policy_capture_unavailable" ?
          { ...result, state: "unknown" as const, reason: original.inventory.reason } : { ...result, state: "not_requested" as const };
      const binding = yield* readDeletionWorktreeTaskEffect(deletionWorktreeEffectIdV1(input.commandId, input.threadId));
      if (binding === null) return { ...result, state: "unknown" as const, reason: "original_worktree_consent_unavailable" };
      const worktree = binding.task.worktree;
      if (worktree.path !== null) result = { ...result, worktree: { ...worktree, path: worktree.path } };
      if (worktree.path !== null) {
        const rows = yield* sql`SELECT resource_path AS "resourcePath", lease_id AS "leaseId", owner_thread_id AS "ownerThreadId",
          owner_incarnation AS "ownerIncarnation", branch, acquired_at_ms AS "acquiredAtMs", renewed_at_ms AS "renewedAtMs", expires_at_ms AS "expiresAtMs"
          FROM worktree_ownership_leases WHERE resource_path = ${worktree.path}`;
        result = { ...result, currentLease: rows.length === 0 ? "absent" : binding.leaseInventory.status === "original" && rows.length === 1 &&
          nativeCreationCanonicalJson(rows[0]) === nativeCreationCanonicalJson(binding.leaseInventory.lease) ? "original" : "replacement" };
      }
      const outcomes = yield* readDeletionWorktreeOutcomes(binding);
      const last = outcomes.at(-1);
      if (yield* readDeletionWorktreeRetirementEffect(binding)) return { ...result, state: "completed" as const,
        removalOutcome: last === undefined ? null : { result: last.outcome.result, effect: last.outcome.effect }, reason: null };
      const effect = Option.getOrNull(yield* effectOutbox.get(binding.effectId));
      const held = (yield* readUnresolvedDeletionCleanupHoldsEffect(input.threadId)).some((hold) => hold.effectId === binding.effectId);
      const admission = yield* sql<{ readonly state: string; readonly started_at: string | null }>`SELECT state, started_at FROM orchestration_v2_worktree_path_admissions
        WHERE operation_id = ${binding.effectId} AND kind = 'worktree_removal' AND json_extract(subject_json, '$.bindingSha256') = ${binding.bindingSha256}`;
      const start = last?.start;
      const now = DateTime.formatIso(yield* DateTime.now);
      const liveStart = start !== undefined && start !== null && admission.length === 1 && admission[0]!.state === "started" &&
        admission[0]!.started_at === start.startedAt && effect?.status === "running" && effect.leaseOwner === start.workerId &&
        effect.attemptCount === start.expectedAttempt && effect.leaseExpiresAt !== null && Number.isFinite(Date.parse(effect.leaseExpiresAt)) &&
        Date.parse(effect.leaseExpiresAt) > Date.parse(now);
      const state = held || (last?.outcome.effect === "unknown" && !liveStart) ? "unknown" : binding.task.captureStatus === "retained" ? "retained" :
        liveStart ? "removing" :
        effect?.status === "pending" || effect?.status === "running" ? "pending" : "unknown";
      return { ...result, state, removalOutcome: last === undefined ? null : { result: last.outcome.result, effect: last.outcome.effect },
        reason: binding.task.reason ?? (state === "unknown" ? "complete_worktree_removal_proof_unavailable" : null) };
    })).pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, commandId: input.commandId, cause })));
    const readLeaseCleanupTaskEffect = Effect.fnUntraced(function* (effectId: string) {
      const rows = yield* sql<{ readonly thread_id: string; readonly lease_json: string; readonly owner_birth_json: string;
        readonly deletion_json: string; readonly task_json: string; readonly binding_sha256: string; readonly recorded_at: string }>`
        SELECT * FROM orchestration_v2_lease_cleanup_task_bindings WHERE effect_id = ${effectId}`;
      if (rows.length === 0) return null;
      if (rows.length !== 1) return yield* new EventSinkWriteError({ eventCount: 0, cause: "Cleanup task binding is ambiguous" });
      const row = rows[0]!;
      if (JSON.parse(row.task_json).kind === "worktree") return null;
      if (JSON.parse(row.lease_json).status === "absent") return null;
      const binding = yield* Schema.decodeUnknownEffect(LeaseCleanupTaskBindingV2)({ version: 2, effectId, threadId: row.thread_id,
        lease: JSON.parse(row.lease_json), ownerBirth: JSON.parse(row.owner_birth_json), deletion: JSON.parse(row.deletion_json),
        task: JSON.parse(row.task_json), bindingSha256: row.binding_sha256, recordedAt: row.recorded_at }, { onExcessProperty: "error" });
      const { bindingSha256, recordedAt: _recordedAt, ...subject } = binding;
      const effect = Option.getOrNull(yield* effectOutbox.get(effectId));
      const birth = yield* sql`SELECT event_id FROM orchestration_events WHERE event_id = ${binding.ownerBirth.eventId}
        AND sequence = ${binding.ownerBirth.sequence} AND application_event_version = 2 AND aggregate_kind = 'thread'
        AND stream_id = ${binding.threadId} AND event_type = 'thread.created' AND json_extract(payload_json, '$.id') = ${binding.threadId}`;
      const deletion = yield* sql`SELECT event_id FROM orchestration_events WHERE event_id = ${binding.deletion.eventId}
        AND sequence = ${binding.deletion.sequence} AND command_id = ${binding.deletion.commandId} AND application_event_version = 2
        AND aggregate_kind = 'thread' AND stream_id = ${binding.threadId} AND event_type = 'thread.deleted'
        AND sequence > ${binding.ownerBirth.sequence} AND json_extract(payload_json, '$.id') = ${binding.threadId}`;
      const receipt = Option.getOrNull(yield* commandReceipts.getByCommandId(binding.deletion.commandId));
      if (bindingSha256 !== leaseCleanupTaskBindingDigestV2(subject) || binding.lease.ownerThreadId !== binding.threadId ||
          binding.ownerBirth.threadId !== binding.threadId || binding.lease.ownerIncarnation !== JSON.stringify([
            "t3.orchestration-v2.thread-birth/v1", binding.ownerBirth.eventId, binding.ownerBirth.sequence]) ||
          birth.length !== 1 || deletion.length !== 1 || receipt?.status !== "accepted" || effect === null ||
          effect.threadId !== binding.threadId || effect.commandId !== binding.deletion.commandId)
        return yield* new EventSinkWriteError({ eventCount: 0, cause: "Cleanup task lost its exact historical lease/effect/deletion association" });
      const task = binding.task;
      if ((task.kind === "provider" && (effect.request.type !== "provider-session.detach" ||
            effect.request.providerSessionId !== task.expectedBinding.providerSessionId || task.expectedBinding.threadId !== binding.threadId)) ||
          (task.kind === "terminal" && (effect.request.type !== "terminal.cleanup" || task.capture.threadId !== binding.threadId ||
            nativeCreationCanonicalJson(task.capture.ownerBirth) !== nativeCreationCanonicalJson(binding.ownerBirth) || task.capture.targets.some((target) =>
              target.threadId !== binding.threadId || nativeCreationCanonicalJson(target.ownerBirth) !== nativeCreationCanonicalJson(binding.ownerBirth)))) ||
          (task.kind === "attachment" && (effect.request.type !== "attachment.cleanup" ||
            nativeCreationCanonicalJson(effect.request.attachmentIds) !== nativeCreationCanonicalJson(task.attachmentIds))))
        return yield* new EventSinkWriteError({ eventCount: 0, cause: "Cleanup task differs from its pinned target" });
      return binding;
    });
    const readDeletionCleanupTaskEffect = Effect.fnUntraced(function* (effectId: string) {
      const rows = yield* sql<{ readonly thread_id: string; readonly lease_json: string; readonly owner_birth_json: string;
        readonly deletion_json: string; readonly task_json: string; readonly binding_sha256: string; readonly recorded_at: string }>`
        SELECT * FROM orchestration_v2_lease_cleanup_task_bindings WHERE effect_id = ${effectId}`;
      if (rows.length === 0) return null;
      if (rows.length !== 1) return yield* new EventSinkWriteError({ eventCount: 0, cause: "Deletion cleanup task is ambiguous" });
      const row = rows[0]!;
      if (JSON.parse(row.task_json).kind === "worktree") return null;
      const inventory = JSON.parse(row.lease_json);
      if (inventory.status !== "absent") return yield* readLeaseCleanupTaskEffect(effectId);
      const binding = yield* Schema.decodeUnknownEffect(UnleasedDeletionCleanupTaskBindingV1)({ version: 1, effectId,
        threadId: row.thread_id, leaseInventory: inventory, ownerBirth: JSON.parse(row.owner_birth_json),
        deletion: JSON.parse(row.deletion_json), task: JSON.parse(row.task_json), bindingSha256: row.binding_sha256,
        recordedAt: row.recorded_at }, { onExcessProperty: "error" });
      const { bindingSha256, recordedAt: _recordedAt, ...subject } = binding;
      const original = yield* readThreadDeletionCommandEffect(binding.deletion.commandId);
      const effect = Option.getOrNull(yield* effectOutbox.get(effectId));
      if (bindingSha256 !== deletionCleanupTaskBindingDigestV1(subject) || original === null ||
          original.inventory.captureStatus !== "captured" || original.inventory.leaseInventory.status !== "absent" ||
          original.inventory.worktree.path !== inventory.resourcePath || original.command.threadId !== binding.threadId ||
          nativeCreationCanonicalJson(original.ownerBirth) !== nativeCreationCanonicalJson(binding.ownerBirth) ||
          nativeCreationCanonicalJson(original.deletion) !== nativeCreationCanonicalJson(binding.deletion) || effect === null ||
          effect.threadId !== binding.threadId || effect.commandId !== binding.deletion.commandId ||
          !original.inventory.prerequisiteEffectIds.includes(effectId))
        return yield* new EventSinkWriteError({ eventCount: 0, cause: "Cleanup absence was not captured with the original deletion" });
      const task = binding.task;
      if ((task.kind === "provider" && (effect.request.type !== "provider-session.detach" ||
          effect.request.providerSessionId !== task.expectedBinding.providerSessionId || task.expectedBinding.threadId !== binding.threadId)) ||
          (task.kind === "terminal" && (effect.request.type !== "terminal.cleanup" || task.capture.threadId !== binding.threadId ||
            nativeCreationCanonicalJson(task.capture.ownerBirth) !== nativeCreationCanonicalJson(binding.ownerBirth) || task.capture.targets.some((target) =>
              target.threadId !== binding.threadId || nativeCreationCanonicalJson(target.ownerBirth) !== nativeCreationCanonicalJson(binding.ownerBirth)))) ||
          (task.kind === "attachment" && (effect.request.type !== "attachment.cleanup" ||
            nativeCreationCanonicalJson(effect.request.attachmentIds) !== nativeCreationCanonicalJson(task.attachmentIds))))
        return yield* new EventSinkWriteError({ eventCount: 0, cause: "Unleased cleanup task differs from its original target" });
      return binding;
    });
    const readDeletionCleanupTaskOwnerBirthEffect = Effect.fnUntraced(function* (effectId: string) {
      const binding = (yield* readDeletionCleanupTaskEffect(effectId)) ?? (yield* readDeletionWorktreeTaskEffect(effectId));
      if (binding?.ownerBirth == null) return null;
      const births = yield* sql<{ readonly event_id: string; readonly sequence: number; readonly payload_json: string }>`
        SELECT event_id, sequence, payload_json FROM orchestration_events WHERE application_event_version = 2
          AND aggregate_kind = 'thread' AND stream_id = ${binding.threadId} AND event_type = 'thread.created'
        ORDER BY sequence DESC LIMIT 1`;
      const current = yield* sql<{ readonly payload_json: string }>`SELECT payload_json FROM orchestration_v2_projection_threads
        WHERE thread_id = ${binding.threadId}`;
      if (births.length !== 1 || current.length !== 1) return null;
      const identity = Schema.fromJsonString(Schema.Struct({ id: ThreadId, projectId: ProjectId, createdAt: Schema.String }));
      const birth = yield* Schema.decodeUnknownEffect(identity)(births[0]!.payload_json).pipe(Effect.option);
      const projection = yield* Schema.decodeUnknownEffect(identity)(current[0]!.payload_json).pipe(Effect.option);
      if (Option.isNone(birth) || Option.isNone(projection) || birth.value.id !== binding.threadId ||
          projection.value.id !== binding.threadId || birth.value.projectId !== projection.value.projectId ||
          birth.value.createdAt !== projection.value.createdAt) return null;
      const latestApplicationBirth = yield* Schema.decodeUnknownEffect(ApplicationBirthSchemaV2)({ kind: 'application_v2_thread_birth',
        threadId: binding.threadId, eventId: births[0]!.event_id, sequence: births[0]!.sequence }, { onExcessProperty: 'error' });
      return { latestApplicationBirth,
        matchesOriginal: nativeCreationCanonicalJson(latestApplicationBirth) === nativeCreationCanonicalJson(binding.ownerBirth) };
    });
    const readDeletionCleanupTaskOutcomeEffect = Effect.fnUntraced(function* (effectId: string) {
      const binding = (yield* readDeletionCleanupTaskEffect(effectId)) ?? (yield* readDeletionWorktreeTaskEffect(effectId));
      if (binding === null) return null;
      const rows = yield* sql<{ readonly ordinal: number; readonly outcome_json: string; readonly correlation_json: string; readonly recorded_at: string }>`
        SELECT ordinal, outcome_json, correlation_json, recorded_at FROM orchestration_v2_lease_cleanup_task_outcomes
        WHERE effect_id = ${effectId} ORDER BY ordinal DESC LIMIT 1`;
      if (rows.length === 0) return null;
      const row = rows[0]!;
      const position = yield* Schema.decodeUnknownEffect(Schema.Struct({ ordinal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
        recordedAt: CapturedRestartIsoTimestampV1 }))({ ordinal: row.ordinal, recordedAt: row.recorded_at }, { onExcessProperty: "error" });
      const outcome = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(LeaseCleanupTaskOutcomeV2))(row.outcome_json, { onExcessProperty: "error" });
      const correlation = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Struct({ workerId: Schema.NonEmptyString,
        expectedAttempt: Schema.Int.check(Schema.isGreaterThan(0)), bindingSha256: LowerSha256,
        evidence: Schema.Record(Schema.String, Schema.Unknown) })))(row.correlation_json, { onExcessProperty: "error" });
      if (outcome.taskId !== effectId || correlation.bindingSha256 !== binding.bindingSha256)
        return yield* new EventSinkWriteError({ eventCount: 0, cause: "Latest cleanup outcome lost its original task correlation" });
      if (correlation.evidence.schema === "t3.deletion-cleanup-observation/v1") yield* readQualifiedDeletionCleanupOutcomeEffect(effectId);
      else if (outcome.result === "succeeded" || outcome.effect === "confirmed" || outcome.effect === "absent")
        return yield* new EventSinkWriteError({ eventCount: 0, cause: "Positive cleanup outcome has no qualified finite observation" });
      return { ...position, outcome, correlation } satisfies DeletionCleanupTaskOutcomeRowV1;
    });
    const readLeaseCleanupStoreBasisEffect = Effect.fnUntraced(function* (input: WorktreeOwnershipLease) {
      const lease = yield* Schema.decodeUnknownEffect(CleanupLeaseSchemaV2)(input, { onExcessProperty: "error" });
      const actual = yield* sql`SELECT resource_path AS "resourcePath", lease_id AS "leaseId", owner_thread_id AS "ownerThreadId",
        owner_incarnation AS "ownerIncarnation", branch, acquired_at_ms AS "acquiredAtMs", renewed_at_ms AS "renewedAtMs", expires_at_ms AS "expiresAtMs"
        FROM worktree_ownership_leases WHERE resource_path = ${lease.resourcePath}`;
      const leaseCurrent = actual.length === 1 && nativeCreationCanonicalJson(actual[0]) === nativeCreationCanonicalJson(lease);
      const currentApplicationBirth = yield* readApplicationBirthRecordEffect(lease.ownerThreadId);
      let historicalOwnerBirth: ApplicationThreadBirthV2 | null = null;
      let deletion: typeof CleanupDeletionSchemaV2.Type | null = null;
      const incarnation = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Tuple([
        Schema.Literal("t3.orchestration-v2.thread-birth/v1"), EventId, Schema.Int.check(Schema.isGreaterThan(0)),
      ])))(lease.ownerIncarnation).pipe(Effect.option);
      if (Option.isSome(incarnation) && JSON.stringify(incarnation.value) === lease.ownerIncarnation) {
        const [_, eventId, sequence] = incarnation.value;
        const births = yield* sql<{ readonly payload_json: string }>`SELECT payload_json FROM orchestration_events
          WHERE event_id = ${eventId} AND sequence = ${sequence} AND application_event_version = 2 AND aggregate_kind = 'thread'
            AND stream_id = ${lease.ownerThreadId} AND event_type = 'thread.created' AND json_extract(payload_json, '$.id') = ${lease.ownerThreadId}`;
        if (births.length === 1) {
          historicalOwnerBirth = { kind: "application_v2_thread_birth", threadId: lease.ownerThreadId, eventId, sequence };
          const deleted = yield* sql<{ readonly command_id: string; readonly event_id: string; readonly sequence: number }>`
            SELECT event.command_id, event.event_id, event.sequence FROM orchestration_events event
            JOIN orchestration_command_receipts receipt ON receipt.command_id = event.command_id AND receipt.status = 'accepted'
            WHERE event.application_event_version = 2 AND event.aggregate_kind = 'thread' AND event.stream_id = ${lease.ownerThreadId}
              AND event.event_type = 'thread.deleted' AND event.sequence > ${sequence}
              AND json_extract(event.payload_json, '$.id') = ${lease.ownerThreadId}
              AND json_extract(event.payload_json, '$.projectId') IS json_extract(${births[0]!.payload_json}, '$.projectId')
              AND json_extract(event.payload_json, '$.createdAt') IS json_extract(${births[0]!.payload_json}, '$.createdAt')
              AND json_extract(event.payload_json, '$.worktreePath') = ${lease.resourcePath}
              AND (${lease.branch} IS NULL OR json_extract(event.payload_json, '$.branch') IS ${lease.branch})
              AND receipt.command_type IN ('thread.delete', 'project.delete')
              AND NOT EXISTS (SELECT 1 FROM orchestration_events next WHERE next.application_event_version = 2
                AND next.aggregate_kind = 'thread' AND next.stream_id = event.stream_id AND next.event_type = 'thread.created'
                AND next.sequence > ${sequence} AND next.sequence <= event.sequence)`;
          if (deleted.length === 1) deletion = { commandId: CommandId.make(deleted[0]!.command_id), eventId: EventId.make(deleted[0]!.event_id), sequence: deleted[0]!.sequence };
        }
      }
      const ownerPresence = historicalOwnerBirth !== null && nativeCreationCanonicalJson(currentApplicationBirth) === nativeCreationCanonicalJson(historicalOwnerBirth)
        ? "current" as const : deletion !== null ? "absent" as const : "unavailable" as const;
      const effects = deletion === null ? [] : yield* effectOutbox.listByCommandId(deletion.commandId);
      const ownerEffects = effects.filter((effect) => effect.threadId === lease.ownerThreadId);
      const tasks: LeaseCleanupTaskBindingV2[] = [];
      for (const effect of ownerEffects) {
        const task = yield* readLeaseCleanupTaskEffect(effect.id);
        if (task !== null && nativeCreationCanonicalJson(task.lease) === nativeCreationCanonicalJson(lease)) tasks.push(task);
      }
      const outcomeRows = tasks.length === 0 ? [] : yield* sql<{ readonly effect_id: string; readonly ordinal: number;
        readonly outcome_json: string; readonly correlation_json: string; readonly recorded_at: string }>`
        SELECT * FROM orchestration_v2_lease_cleanup_task_outcomes WHERE effect_id IN ${sql.in(tasks.map((task) => task.effectId))}
        ORDER BY effect_id, ordinal`;
      const outcomes = yield* Effect.forEach(outcomeRows, (row) => Effect.gen(function* () {
        const outcome = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(LeaseCleanupTaskOutcomeV2))(row.outcome_json, { onExcessProperty: "error" });
        const correlation = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Struct({ workerId: Schema.NonEmptyString,
          expectedAttempt: Schema.Int.check(Schema.isGreaterThan(0)), bindingSha256: LowerSha256,
          evidence: Schema.Record(Schema.String, Schema.Unknown) })))(row.correlation_json, { onExcessProperty: "error" });
        if (outcome.taskId !== row.effect_id || tasks.find((task) => task.effectId === row.effect_id)?.bindingSha256 !== correlation.bindingSha256)
          return yield* new EventSinkWriteError({ eventCount: 0, cause: "Cleanup outcome lost its immutable task association" });
        return { ordinal: row.ordinal, outcome, correlation, recordedAt: row.recorded_at };
      }), { concurrency: 1 });
      const held = yield* readUnresolvedDeletionCleanupHoldsEffect(lease.ownerThreadId);
      const pending = yield* sql<{ readonly effect_id: string; readonly result_sequence: number | null }>`
        SELECT effect.effect_id, receipt.result_sequence FROM orchestration_v2_effect_outbox effect
        LEFT JOIN orchestration_command_receipts receipt ON receipt.command_id = effect.command_id
        WHERE effect.thread_id = ${lease.ownerThreadId} AND effect.status IN ('pending', 'running')`;
      const pendingOwnerEffectIds = pending.filter((row) => currentApplicationBirth === null || historicalOwnerBirth === null ||
        currentApplicationBirth.sequence === historicalOwnerBirth.sequence || row.result_sequence === null ||
        row.result_sequence < currentApplicationBirth.sequence).map((row) => row.effect_id);
      const inventoryComplete = ownerPresence === "absent" && leaseCurrent && held.length === 0 &&
        ownerEffects.every((effect) => effect.request.type === "worktree.cleanup" ||
          (["provider-session.detach", "terminal.cleanup", "attachment.cleanup"].includes(effect.request.type) ?
            tasks.some((task) => task.effectId === effect.id) : effect.status === "succeeded" && effect.completedAt !== null)) &&
        ownerEffects.some((effect) => effect.request.type === "terminal.cleanup");
      return { lease, leaseCurrent, ownerPresence, historicalOwnerBirth, currentApplicationBirth, deletion, pendingOwnerEffectIds,
        inventoryComplete, tasks, outcomes } satisfies LeaseCleanupStoreBasisV2;
    });
    const readDeletionWorktreeExecutionBasisEffect = Effect.fnUntraced(function* (effectId: string) {
      const binding = yield* readDeletionWorktreeTaskEffect(effectId);
      if (binding === null) return null;
      const effect = Option.getOrNull(yield* effectOutbox.get(effectId));
      if (effect === null) return yield* new EventSinkWriteError({ eventCount: 0, cause: "Worktree execution lost its original effect" });
      const heldEffects = yield* readUnresolvedDeletionCleanupHoldsEffect(binding.threadId);
      const held = heldEffects.some((hold) => hold.effectId === effectId);
      const outcomeRows = yield* readDeletionWorktreeOutcomes(binding);
      const latest = outcomeRows.at(-1);
      const starts = outcomeRows.filter((row) => row.start !== null);
      if (starts.length > 1) return yield* new EventSinkWriteError({ eventCount: 0, cause: "Worktree native start is ambiguous" });
      const start = starts.length === 1 ? { ordinal: starts[0]!.ordinal, evidence: starts[0]!.start! } : null;
      const admissions = yield* sql`SELECT * FROM orchestration_v2_worktree_path_admissions WHERE operation_id = ${effectId}`;
      let admission: WorktreePathAdmissionV1 | null = null;
      if (admissions.length === 1) {
        const row = admissions[0]!;
        admission = yield* Schema.decodeUnknownEffect(WorktreePathAdmissionV1)({ operationId: row.operation_id, path: row.canonical_path,
          kind: row.kind, subject: JSON.parse(String(row.subject_json)), state: row.state, startedAt: row.started_at,
          outcome: row.outcome_json === null ? null : JSON.parse(String(row.outcome_json)), recordedAt: row.recorded_at, updatedAt: row.updated_at },
        { onExcessProperty: "error" });
        if (admission.kind !== "worktree_removal" || admission.path !== binding.task.worktree.path ||
            admission.subject.bindingSha256 !== binding.bindingSha256 || admission.subject.effectId !== effectId ||
            admission.subject.threadId !== binding.threadId || admission.subject.commandId !== binding.deletion.commandId)
          return yield* new EventSinkWriteError({ eventCount: 0, cause: "Worktree admission lost its immutable task association" });
        if ((start === null && admission.startedAt !== null) || (start !== null && admission.startedAt !== start.evidence.startedAt))
          return yield* new EventSinkWriteError({ eventCount: 0, cause: "Worktree admission differs from its persisted start" });
      } else if (admissions.length > 1)
        return yield* new EventSinkWriteError({ eventCount: 0, cause: "Worktree admission is ambiguous" });
      const path = binding.task.worktree.path;
      const leases = path === null ? null : yield* sql`SELECT resource_path AS "resourcePath", lease_id AS "leaseId", owner_thread_id AS "ownerThreadId",
        owner_incarnation AS "ownerIncarnation", branch, acquired_at_ms AS "acquiredAtMs", renewed_at_ms AS "renewedAtMs", expires_at_ms AS "expiresAtMs"
        FROM worktree_ownership_leases WHERE resource_path = ${path}`;
      const currentLease = leases === null ? "unavailable" as const : leases.length === 0 ? "absent" as const :
        leases.length === 1 && binding.leaseInventory.status === "original" &&
        nativeCreationCanonicalJson(leases[0]) === nativeCreationCanonicalJson(binding.leaseInventory.lease) ? "original" as const : "replacement" as const;
      const prerequisites: Array<DeletionWorktreeExecutionBasisV1["prerequisites"][number]> = [];
      for (const prerequisiteId of binding.task.prerequisiteEffectIds.filter((id) => id !== effectId)) {
        const prerequisite = yield* readDeletionCleanupTaskEffect(prerequisiteId);
        const rows = yield* sql<{ readonly ordinal: number; readonly outcome_json: string; readonly correlation_json: string }>`
          SELECT ordinal, outcome_json, correlation_json FROM orchestration_v2_lease_cleanup_task_outcomes
          WHERE effect_id = ${prerequisiteId} ORDER BY ordinal DESC LIMIT 1`;
        let latestOutcome: DeletionWorktreeExecutionBasisV1["prerequisites"][number]["latestOutcome"] = null;
        if (rows.length === 1) {
          const row = rows[0]!;
          const outcome = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(LeaseCleanupTaskOutcomeV2))(row.outcome_json, { onExcessProperty: "error" });
          const correlation = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Struct({ workerId: Schema.NonEmptyString,
            expectedAttempt: Schema.Int.check(Schema.isGreaterThan(0)), bindingSha256: LowerSha256,
            evidence: Schema.Record(Schema.String, Schema.Unknown) })))(row.correlation_json, { onExcessProperty: "error" });
          if (outcome.taskId !== prerequisiteId || prerequisite === null || correlation.bindingSha256 !== prerequisite.bindingSha256)
            return yield* new EventSinkWriteError({ eventCount: 0, cause: "Worktree prerequisite lost its immutable task correlation" });
          latestOutcome = { ordinal: row.ordinal, outcome };
        }
        const prerequisiteHeld = heldEffects.some((hold) => hold.effectId === prerequisiteId);
        const sameOwner = prerequisite !== null && nativeCreationCanonicalJson(prerequisite.ownerBirth) === nativeCreationCanonicalJson(binding.ownerBirth) &&
          nativeCreationCanonicalJson(prerequisite.deletion) === nativeCreationCanonicalJson(binding.deletion) &&
          (prerequisite.version === 2 ? binding.leaseInventory.status === "original" &&
            nativeCreationCanonicalJson(prerequisite.lease) === nativeCreationCanonicalJson(binding.leaseInventory.lease) :
            binding.leaseInventory.status === "absent" && prerequisite.leaseInventory.resourcePath === path);
        const qualified = prerequisite === null ? null : yield* readQualifiedDeletionCleanupOutcomeEffect(prerequisiteId);
        const prerequisiteEffect = Option.getOrNull(yield* effectOutbox.get(prerequisiteId));
        const importedChoices = prerequisiteEffect?.request.type === "provider-turn.start" ?
          yield* sql`SELECT command_id FROM orchestration_v2_imported_history_start_choices
            WHERE command_id = ${prerequisiteEffect.commandId} AND thread_id = ${prerequisiteEffect.threadId}` : [];
        const needsStartConfirmation = prerequisiteEffect?.request.type === "provider-turn.start" &&
          (prerequisiteEffect.nativeCreationExecutionReference !== undefined || importedChoices.length !== 0);
        const ownStartConfirmation = needsStartConfirmation ?
          yield* nativeCreationRepository.readNativeEffectConfirmation(prerequisiteId) : null;
        const ownCompletion = prerequisite === null && prerequisiteEffect !== null &&
          !["provider-session.detach", "terminal.cleanup", "attachment.cleanup", "worktree.cleanup"].includes(prerequisiteEffect.request.type) &&
          prerequisiteEffect.status === "succeeded" && prerequisiteEffect.completedAt !== null && !prerequisiteHeld &&
          (!needsStartConfirmation || ownStartConfirmation !== null);
        prerequisites.push({ effectId: prerequisiteId, binding: prerequisite, latestOutcome, held: prerequisiteHeld,
          ready: ownCompletion || (sameOwner && !prerequisiteHeld && prerequisiteEffect?.status === "succeeded" &&
            qualified?.ordinal === latestOutcome?.ordinal && qualified?.outcome.result === "succeeded" &&
            (qualified.outcome.effect === "confirmed" || qualified.outcome.effect === "absent")) });
      }
      let reason = binding.task.reason;
      const project = Option.getOrNull(yield* projectStore.get(binding.task.worktree.projectId));
      const birthsAfterDeletion = yield* sql`SELECT event_id FROM orchestration_events WHERE application_event_version = 2
        AND aggregate_kind = 'thread' AND stream_id = ${binding.threadId} AND event_type = 'thread.created'
        AND sequence > ${binding.deletion.sequence}`;
      if (binding.ownerBirth === null || path === null || binding.task.projectRoot === null || binding.task.captureStatus !== "captured")
        reason ??= "original_worktree_inventory_unavailable";
      else if (project === null || NodePath.resolve(project.workspaceRoot) !== binding.task.projectRoot || path === binding.task.projectRoot)
        reason = "project_worktree_identity_changed";
      else if (birthsAfterDeletion.length > 0) reason = "replacement_application_birth";
      else if (binding.leaseInventory.status === "original" ? currentLease !== "original" :
          binding.leaseInventory.status !== "absent" || currentLease !== "absent") reason = "original_lease_identity_changed";
      if (path !== null) {
        for (const candidate of yield* projectStore.list()) {
          const root = NodePath.resolve(candidate.workspaceRoot);
          if (root === path || root.startsWith(`${path}${NodePath.sep}`)) reason = "project_root_retained";
        }
        const threads = yield* sql<{ readonly thread_id: string; readonly payload_json: string }>`
          SELECT thread_id, payload_json FROM orchestration_v2_projection_threads WHERE thread_id <> ${binding.threadId}`;
        for (const row of threads) {
          const candidate = JSON.parse(row.payload_json);
          if (candidate.deletedAt !== null || typeof candidate.worktreePath !== "string") continue;
          const candidateProject = Option.getOrNull(yield* projectStore.get(ProjectId.make(candidate.projectId)));
          if (candidateProject === null) { reason = "shared_worktree_inventory_unavailable"; continue; }
          const candidatePath = NodePath.resolve(candidateProject.workspaceRoot, candidate.worktreePath);
          if (candidatePath === path || candidatePath.startsWith(`${path}${NodePath.sep}`) || path.startsWith(`${candidatePath}${NodePath.sep}`))
            reason = "shared_worktree_retained";
        }
      }
      const deletionRows = yield* sql<{ readonly payload_json: string }>`SELECT payload_json FROM orchestration_events
        WHERE event_id = ${binding.deletion.eventId} AND sequence = ${binding.deletion.sequence}`;
      const originalProviderThreadId = deletionRows.length === 1 ? JSON.parse(deletionRows[0]!.payload_json).activeProviderThreadId : undefined;
      const inventoryComplete = reason === null && admission !== null && originalProviderThreadId !== undefined &&
        prerequisites.every((prerequisite) => prerequisite.binding !== null || prerequisite.ready) &&
        prerequisites.some((prerequisite) => prerequisite.binding?.task.kind === "terminal") &&
        (originalProviderThreadId === null || prerequisites.some((prerequisite) => prerequisite.binding?.task.kind === "provider" &&
          prerequisite.binding.task.expectedBinding.providerThreadId === originalProviderThreadId));
      return { binding, effect, admission, currentLease, inventoryComplete,
        prerequisitesReady: prerequisites.every((prerequisite) => prerequisite.ready) && heldEffects.every((hold) => hold.effectId === effectId),
        prerequisites, latestOutcome: latest === undefined ? null : { ordinal: latest.ordinal, outcome: latest.outcome }, start, held, reason } satisfies DeletionWorktreeExecutionBasisV1;
    });
    const startDeletionWorktreeRemoval: EventSinkV2Shape["startDeletionWorktreeRemoval"] = (input) => withTransaction(Effect.gen(function* () {
      const decoded = yield* Schema.decodeUnknownEffect(Schema.Struct({ effectId: Schema.NonEmptyString, bindingSha256: LowerSha256,
        workerId: Schema.NonEmptyString, expectedAttempt: Schema.Int.check(Schema.isGreaterThan(0)), target: DeletionWorktreeRemovalTargetV1 }))
        ({ effectId: input.effectId, bindingSha256: input.bindingSha256, workerId: input.workerId,
          expectedAttempt: input.expectedAttempt, target: input.target }, { onExcessProperty: "error" });
      const basis = yield* readDeletionWorktreeExecutionBasisEffect(decoded.effectId);
      if (basis === null) return { status: "retained" as const, reason: "original_worktree_task_unavailable", basis };
      if (basis.binding.bindingSha256 !== decoded.bindingSha256 ||
          nativeCreationCanonicalJson(decoded.target) !== nativeCreationCanonicalJson(deletionWorktreeRemovalTargetV1(basis.binding)))
        return { status: "retained" as const, reason: "worktree_target_changed", basis };
      if (basis.start !== null || basis.admission?.state !== "reserved" || basis.latestOutcome !== null)
        return { status: "observe_only" as const, reason: "worktree_start_or_outcome_already_recorded", basis };
      const request = deletionWorktreeCleanupRequestV1(basis.binding);
      if (request?.origin === "policy") {
        const currentRules = input.revalidatePolicy === undefined ? null : yield* input.revalidatePolicy.pipe(Effect.result);
        if (currentRules === null || currentRules._tag === "Failure" || !request.rules.worktreeOnDelete ||
            nativeCreationCanonicalJson(currentRules.success) !== nativeCreationCanonicalJson(request.rules))
          return { status: "retained" as const, reason: "worktree_policy_changed_or_unavailable", basis };
      }
      const now = DateTime.formatIso(yield* DateTime.now);
      const effect = basis.effect;
      if (!basis.inventoryComplete || !basis.prerequisitesReady || basis.held || effect.status !== "running" ||
          effect.leaseOwner !== decoded.workerId || effect.attemptCount !== decoded.expectedAttempt || effect.leaseExpiresAt === null ||
          !Number.isFinite(Date.parse(effect.leaseExpiresAt)) || Date.parse(effect.leaseExpiresAt) <= Date.parse(now))
        return { status: "retained" as const, reason: basis.reason ?? "worktree_claim_or_prerequisites_unavailable", basis };
      const start: DeletionWorktreeRemovalStartV1 = { schema: "t3.deletion-worktree-removal-start/v1", ...decoded, startedAt: now };
      const outcome: LeaseCleanupTaskOutcomeV2 = { taskId: decoded.effectId, result: null, effect: "unknown" };
      const correlation = { workerId: decoded.workerId, expectedAttempt: decoded.expectedAttempt,
        bindingSha256: decoded.bindingSha256, evidence: start };
      yield* sql`INSERT INTO orchestration_v2_lease_cleanup_task_outcomes (effect_id, ordinal, outcome_json, correlation_json, recorded_at)
        VALUES (${decoded.effectId}, 0, ${nativeCreationCanonicalJson(outcome)}, ${nativeCreationCanonicalJson(correlation)}, ${now})`;
      const transitioned = yield* sql`UPDATE orchestration_v2_worktree_path_admissions SET state = 'started', started_at = ${now}, updated_at = ${now}
        WHERE operation_id = ${decoded.effectId} AND kind = 'worktree_removal' AND state = 'reserved'
          AND json_extract(subject_json, '$.bindingSha256') = ${decoded.bindingSha256} RETURNING operation_id`;
      if (transitioned.length !== 1) return yield* new EventSinkWriteError({ eventCount: 0, cause: "Worktree start lost its original reservation" });
      return { status: "start_now" as const, start, basis: (yield* readDeletionWorktreeExecutionBasisEffect(decoded.effectId))! };
    })).pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause })));
    const revalidateDeletionWorktreeRemovalStart: EventSinkV2Shape["revalidateDeletionWorktreeRemovalStart"] = (start, startOrdinal, revalidatePolicy) =>
      sql.withTransaction(Effect.gen(function* () {
        const decoded = yield* Schema.decodeUnknownEffect(DeletionWorktreeRemovalStartV1)(start, { onExcessProperty: "error" });
        const basis = yield* readDeletionWorktreeExecutionBasisEffect(decoded.effectId);
        const now = DateTime.formatIso(yield* DateTime.now);
        if (basis === null || basis.start?.ordinal !== startOrdinal ||
            nativeCreationCanonicalJson(basis.start.evidence) !== nativeCreationCanonicalJson(decoded) ||
            basis.admission?.state !== "started" || basis.latestOutcome?.ordinal !== startOrdinal ||
            !basis.inventoryComplete || !basis.prerequisitesReady || basis.held || basis.effect.status !== "running" ||
            basis.effect.leaseOwner !== decoded.workerId || basis.effect.attemptCount !== decoded.expectedAttempt ||
            basis.effect.leaseExpiresAt === null || !Number.isFinite(Date.parse(basis.effect.leaseExpiresAt)) ||
            Date.parse(basis.effect.leaseExpiresAt) <= Date.parse(now))
          return yield* new EventSinkWriteError({ eventCount: 0, cause: "Removal start no longer owns its original target and claim" });
        const request = deletionWorktreeCleanupRequestV1(basis.binding);
        if (request?.origin === "policy") {
          const rules = revalidatePolicy === undefined ? null : yield* revalidatePolicy.pipe(Effect.result);
          if (rules === null || rules._tag === "Failure" || !request.rules.worktreeOnDelete ||
              nativeCreationCanonicalJson(rules.success) !== nativeCreationCanonicalJson(request.rules))
            return yield* new EventSinkWriteError({ eventCount: 0, cause: "Removal policy changed before its recorded native invocation" });
        }
      })).pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause })));
    const recordObservedDeletionCleanupOutcome: EventSinkV2Shape["recordObservedDeletionCleanupOutcome"] = (input) =>
      withTransaction(Effect.gen(function* () {
        const observation = yield* Schema.decodeUnknownEffect(DeletionCleanupObservationV1)(input.observation, { onExcessProperty: "error" });
        const coveredHolds = yield* Schema.decodeUnknownEffect(Schema.Array(EffectOutbox.UnknownEffectHoldSchemaV2))(input.coveredHolds,
          { onExcessProperty: "error" });
        const binding = (yield* readDeletionCleanupTaskEffect(input.effectId)) ?? (yield* readDeletionWorktreeTaskEffect(input.effectId));
        if (binding === null || binding.bindingSha256 !== input.bindingSha256 || !Number.isSafeInteger(input.expectedLatestOrdinal) || input.expectedLatestOrdinal < -1)
          return yield* new EventSinkWriteError({ eventCount: 0, cause: "Observed cleanup has no immutable original task" });
        const qualified = yield* qualifyDeletionCleanupObservation(binding, observation);
        if (!("start" in observation) && observation.kind === "managed_provider") {
          const owner = yield* readDeletionCleanupTaskOwnerBirthEffect(input.effectId);
          if (owner === null || !owner.matchesOriginal)
            return yield* new EventSinkWriteError({ eventCount: 0, cause: "Managed provider observation lost its exact original application birth" });
        }
        const latestRows = yield* sql<{ readonly ordinal: number }>`SELECT ordinal FROM orchestration_v2_lease_cleanup_task_outcomes
          WHERE effect_id = ${input.effectId} ORDER BY ordinal DESC LIMIT 1`;
        if ((latestRows[0]?.ordinal ?? -1) !== input.expectedLatestOrdinal)
          return yield* new EventSinkWriteError({ eventCount: 0, cause: "Observed cleanup lost its latest outcome comparison" });
        const effect = Option.getOrNull(yield* effectOutbox.get(input.effectId));
        const now = DateTime.formatIso(yield* DateTime.now);
        if (effect === null || effect.status !== "running" || effect.leaseOwner !== qualified.workerId ||
            effect.attemptCount !== qualified.expectedAttempt || (!("start" in observation) &&
              (effect.leaseExpiresAt === null || !Number.isFinite(Date.parse(effect.leaseExpiresAt)) || Date.parse(effect.leaseExpiresAt) <= Date.parse(now))))
          return yield* new EventSinkWriteError({ eventCount: 0, cause: "Observed cleanup differs from its original operation claim" });
        const holds = (yield* effectOutbox.listHeldByThreadId(binding.threadId)).filter((hold) => hold.effectId === input.effectId);
        if (coveredHolds.some((hold, index) => hold.effectId !== input.effectId || hold.threadId !== binding.threadId ||
            hold.operationId !== input.effectId || hold.workerId !== qualified.workerId || hold.expectedAttempt !== qualified.expectedAttempt ||
            !cleanupHoldMatchesObservation(binding, observation, hold) || !holds.some((stored) => nativeCreationCanonicalJson(stored) === nativeCreationCanonicalJson(hold)) ||
            coveredHolds.slice(0, index).some((earlier) => earlier.effectId === hold.effectId)))
          return yield* new EventSinkWriteError({ eventCount: 0, cause: "Cleanup outcome cannot cover another or unbound unknown operation" });
        if (qualified.outcome.result === "succeeded" && (coveredHolds.length !== holds.length || holds.some((hold) =>
            !coveredHolds.some((covered) => nativeCreationCanonicalJson(covered) === nativeCreationCanonicalJson(hold)))))
          return yield* new EventSinkWriteError({ eventCount: 0, cause: "Positive cleanup must account for every immutable hold on its operation" });
        const evidence: ObservedDeletionCleanupOutcomeV1["evidence"] = { version: 1, schema: "t3.deletion-cleanup-observation/v1",
          producer: qualified.producer, observation, coveredHolds };
        const ordinal = input.expectedLatestOrdinal + 1;
        const correlation = { workerId: qualified.workerId, expectedAttempt: qualified.expectedAttempt,
          bindingSha256: binding.bindingSha256, evidence };
        yield* sql`INSERT INTO orchestration_v2_lease_cleanup_task_outcomes (effect_id, ordinal, outcome_json, correlation_json, recorded_at)
          VALUES (${input.effectId}, ${ordinal}, ${nativeCreationCanonicalJson(qualified.outcome)}, ${nativeCreationCanonicalJson(correlation)}, ${now})`;
        if (binding.task.kind === "worktree") {
          const states = qualified.outcome.result === "succeeded" ? ["started", "unknown"] : ["reserved", "started"];
          yield* sql`UPDATE orchestration_v2_worktree_path_admissions SET state = ${qualified.outcome.result === "succeeded" ? "completed" : "unknown"},
            outcome_json = ${nativeCreationCanonicalJson({ ordinal, outcome: qualified.outcome, correlation })}, updated_at = ${now}
            WHERE operation_id = ${input.effectId} AND kind = 'worktree_removal' AND state IN ${sql.in(states)}
              AND json_extract(subject_json, '$.bindingSha256') = ${binding.bindingSha256}`;
        }
        if (qualified.outcome.effect === "unknown") {
          if (!("start" in observation) && observation.kind === "managed_provider") {
            const nativeEvidence = { ...observation.nativeOperation, outcome: "unknown" as const };
            const held = yield* effectOutbox.holdUnknown({ effectId: input.effectId, workerId: qualified.workerId,
              expectedAttempt: qualified.expectedAttempt, operationId: input.effectId, evidence: nativeEvidence });
            if (!held && !holds.some((hold) => hold.workerId === qualified.workerId && hold.expectedAttempt === qualified.expectedAttempt &&
                nativeCreationCanonicalJson(hold.evidence) === nativeCreationCanonicalJson(nativeEvidence)))
              return yield* new EventSinkWriteError({ eventCount: 0, cause: "Unknown managed provider cleanup could not preserve its original native hold" });
            return { ordinal, outcome: qualified.outcome, bindingSha256: binding.bindingSha256, evidence };
          }
          const resourceEvidence: EffectOutbox.ResourceCleanupUnknownEvidenceV1 = { version: 1, kind: "resource_cleanup",
            operationId: input.effectId, threadId: binding.threadId, taskKind: binding.task.kind === "worktree" ? "worktree" : "terminal",
            bindingSha256: binding.bindingSha256, outcome: "unknown" };
          const held = yield* effectOutbox.holdResourceCleanupUnknown({ effectId: input.effectId, workerId: qualified.workerId,
            expectedAttempt: qualified.expectedAttempt, evidence: resourceEvidence });
          if (!held && !holds.some((hold) => hold.workerId === qualified.workerId && hold.expectedAttempt === qualified.expectedAttempt &&
              nativeCreationCanonicalJson(hold.evidence) === nativeCreationCanonicalJson(resourceEvidence)))
            return yield* new EventSinkWriteError({ eventCount: 0, cause: "Unknown observed cleanup could not preserve its exact hold" });
        }
        return { ordinal, outcome: qualified.outcome, bindingSha256: binding.bindingSha256, evidence };
      })).pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause })));
    const completeObservedDeletionCleanup: EventSinkV2Shape["completeObservedDeletionCleanup"] = (input) =>
      withTransaction(Effect.gen(function* () {
        const latest = yield* readQualifiedDeletionCleanupOutcomeEffect(input.effectId);
        if (latest === null || latest.ordinal !== input.expectedLatestOrdinal || latest.bindingSha256 !== input.bindingSha256 ||
            latest.outcome.result !== "succeeded" || !["confirmed", "absent"].includes(latest.outcome.effect)) return false;
        const completed = yield* effectOutbox.completeObservedDeletionCleanup({ ...input, workerId: latest.correlation.workerId,
          expectedAttempt: latest.correlation.expectedAttempt });
        if (completed) yield* afterCommit(effectOutbox.notifyAvailable());
        return completed;
      })).pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause })));
    const finalizeDeletionWorktreeCleanup: EventSinkV2Shape["finalizeDeletionWorktreeCleanup"] = (input) =>
      withTransaction(Effect.gen(function* () {
        const basis = yield* readDeletionWorktreeExecutionBasisEffect(input.effectId);
        if (basis === null || basis.binding.bindingSha256 !== input.bindingSha256)
          return { status: "retained" as const, reason: "original_worktree_task_unavailable" };
        if (yield* readDeletionWorktreeRetirementEffect(basis.binding)) return { status: "completed" as const, reason: null };
        const latest = yield* readQualifiedDeletionCleanupOutcomeEffect(input.effectId);
        if (latest === null || latest.ordinal !== input.expectedLatestOrdinal || latest.outcome.result !== "succeeded" ||
            !["confirmed", "absent"].includes(latest.outcome.effect) || basis.admission?.state !== "completed" ||
            !basis.inventoryComplete || !basis.prerequisitesReady || basis.held || basis.reason !== null)
          return { status: "retained" as const, reason: basis.reason ?? "qualified_cleanup_or_prerequisites_unavailable" };
        const inventory = basis.binding.leaseInventory;
        if (inventory.status === "original" && (basis.currentLease !== "original" || input.releaseOriginalLease === undefined))
          return { status: "retained" as const, reason: "original_lease_release_unavailable" };
        if (inventory.status !== "original" && (inventory.status !== "absent" || basis.currentLease !== "absent"))
          return { status: "retained" as const, reason: "original_lease_inventory_changed" };
        if (!(yield* completeObservedDeletionCleanup(input)))
          return { status: "retained" as const, reason: "qualified_effect_completion_unavailable" };
        if (inventory.status === "original") yield* input.releaseOriginalLease!(inventory.lease);
        const path = basis.binding.task.worktree.path!;
        const remaining = yield* sql`SELECT lease_id FROM worktree_ownership_leases WHERE resource_path = ${path}`;
        const currentHolds = yield* readUnresolvedDeletionCleanupHoldsEffect(basis.binding.threadId);
        const currentLatest = yield* readQualifiedDeletionCleanupOutcomeEffect(input.effectId);
        if (remaining.length !== 0 || currentLatest?.ordinal !== latest.ordinal || currentHolds.some((hold) =>
            hold.effectId === input.effectId || basis.binding.task.prerequisiteEffectIds.includes(hold.effectId)))
          return yield* new EventSinkWriteError({ eventCount: 0, cause: "Qualified cleanup release did not preserve its exact final postcondition" });
        const now = DateTime.formatIso(yield* DateTime.now);
        const retirement = { schema: "t3.deletion-worktree-retirement/v1", effectId: input.effectId, bindingSha256: input.bindingSha256,
          qualifiedOrdinal: latest.ordinal, originalLeaseInventory: inventory, finalizedAt: now };
        const retired = yield* sql`UPDATE orchestration_v2_worktree_path_admissions SET state = 'released',
          outcome_json = ${nativeCreationCanonicalJson(retirement)}, updated_at = ${now}
          WHERE operation_id = ${input.effectId} AND kind = 'worktree_removal' AND state = 'completed'
            AND json_extract(subject_json, '$.bindingSha256') = ${input.bindingSha256} RETURNING operation_id`;
        if (retired.length !== 1 || !(yield* readDeletionWorktreeRetirementEffect(basis.binding)))
          return yield* new EventSinkWriteError({ eventCount: 0, cause: "Worktree path retirement lost its qualified original association" });
        return { status: "completed" as const, reason: null };
      })).pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause })));
    const writeLeaseCleanupTaskBinding = (binding: LeaseCleanupTaskBindingV2) => sql`
      INSERT INTO orchestration_v2_lease_cleanup_task_bindings
        (effect_id, thread_id, lease_json, owner_birth_json, deletion_json, task_json, binding_sha256, recorded_at)
      VALUES (${binding.effectId}, ${binding.threadId}, ${nativeCreationCanonicalJson(binding.lease)},
        ${nativeCreationCanonicalJson(binding.ownerBirth)}, ${nativeCreationCanonicalJson(binding.deletion)},
        ${nativeCreationCanonicalJson(binding.task)}, ${binding.bindingSha256}, ${binding.recordedAt})`;
    const writeUnleasedDeletionCleanupTaskBinding = (binding: UnleasedDeletionCleanupTaskBindingV1) => sql`
      INSERT INTO orchestration_v2_lease_cleanup_task_bindings
        (effect_id, thread_id, lease_json, owner_birth_json, deletion_json, task_json, binding_sha256, recorded_at)
      VALUES (${binding.effectId}, ${binding.threadId}, ${nativeCreationCanonicalJson(binding.leaseInventory)},
        ${nativeCreationCanonicalJson(binding.ownerBirth)}, ${nativeCreationCanonicalJson(binding.deletion)},
        ${nativeCreationCanonicalJson(binding.task)}, ${binding.bindingSha256}, ${binding.recordedAt})`;
    const prepareLeaseCleanupTaskBindings: EventSinkV2Shape["prepareLeaseCleanupTaskBindings"] = (input) =>
      withTransaction(Effect.gen(function* () {
        const basis = yield* readLeaseCleanupStoreBasisEffect(input.lease);
        if (!basis.leaseCurrent || basis.ownerPresence !== "absent" || basis.historicalOwnerBirth === null || basis.deletion === null)
          return basis;
        for (const effect of yield* effectOutbox.listByCommandId(basis.deletion.commandId)) {
          if (effect.threadId !== basis.lease.ownerThreadId || (yield* readLeaseCleanupTaskEffect(effect.id)) !== null) continue;
          let task: LeaseCleanupTaskV2 | null = null;
          if (effect.request.type === "terminal.cleanup" && input.terminalCapture !== null) {
            task = yield* Schema.decodeUnknownEffect(LeaseCleanupTaskV2)({ kind: "terminal", capture: input.terminalCapture }, { onExcessProperty: "error" });
            if (task.kind !== "terminal" || task.capture.threadId !== basis.lease.ownerThreadId ||
                nativeCreationCanonicalJson(task.capture.ownerBirth) !== nativeCreationCanonicalJson(basis.historicalOwnerBirth) ||
                task.capture.targets.some((target) => target.threadId !== basis.lease.ownerThreadId ||
                  nativeCreationCanonicalJson(target.ownerBirth) !== nativeCreationCanonicalJson(basis.historicalOwnerBirth)))
              return yield* new EventSinkWriteError({ eventCount: 0, cause: "Terminal capture belongs to a different lease owner" });
          } else if (effect.request.type === "attachment.cleanup") task = { kind: "attachment", attachmentIds: effect.request.attachmentIds };
          if (task === null) continue;
          const subject = { version: 2 as const, effectId: effect.id, threadId: basis.lease.ownerThreadId, lease: basis.lease,
            ownerBirth: basis.historicalOwnerBirth, deletion: basis.deletion, task };
          yield* writeLeaseCleanupTaskBinding({ ...subject, bindingSha256: leaseCleanupTaskBindingDigestV2(subject),
            recordedAt: DateTime.formatIso(yield* DateTime.now) });
        }
        return yield* readLeaseCleanupStoreBasisEffect(input.lease);
      })).pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause })));
    const prepareDeletionCleanupTaskBindings: EventSinkV2Shape["prepareDeletionCleanupTaskBindings"] = (input) =>
      withTransaction(Effect.gen(function* () {
        const original = yield* readThreadDeletionCommandEffect(input.commandId);
        if (original === null || original.ownerBirth === null || original.inventory.captureStatus !== "captured") return [];
        if (original.inventory.leaseInventory.status === "original") {
          yield* prepareLeaseCleanupTaskBindings({ lease: original.inventory.leaseInventory.lease, terminalCapture: input.terminalCapture });
        } else if (original.inventory.leaseInventory.status === "absent" && original.inventory.worktree.path !== null) {
          for (const effect of yield* effectOutbox.listByCommandId(input.commandId)) {
            if (effect.threadId !== original.command.threadId || !original.inventory.prerequisiteEffectIds.includes(effect.id) ||
                (yield* readDeletionCleanupTaskEffect(effect.id)) !== null) continue;
            let task: LeaseCleanupTaskV2 | null = null;
            if (effect.request.type === "terminal.cleanup" && input.terminalCapture !== null) {
              task = yield* Schema.decodeUnknownEffect(LeaseCleanupTaskV2)({ kind: "terminal", capture: input.terminalCapture }, { onExcessProperty: "error" });
              if (task.kind !== "terminal" || task.capture.threadId !== original.command.threadId ||
                  nativeCreationCanonicalJson(task.capture.ownerBirth) !== nativeCreationCanonicalJson(original.ownerBirth) ||
                  task.capture.targets.some((target) => target.threadId !== original.command.threadId ||
                    nativeCreationCanonicalJson(target.ownerBirth) !== nativeCreationCanonicalJson(original.ownerBirth)))
                return yield* new EventSinkWriteError({ eventCount: 0, cause: "Managed terminal capture differs from the original absent-lease owner" });
            } else if (effect.request.type === "attachment.cleanup") task = { kind: "attachment", attachmentIds: effect.request.attachmentIds };
            if (task === null) continue;
            const subject = { version: 1 as const, effectId: effect.id, threadId: original.command.threadId,
              leaseInventory: { status: "absent" as const, resourcePath: original.inventory.worktree.path },
              ownerBirth: original.ownerBirth, deletion: original.deletion, task };
            yield* writeUnleasedDeletionCleanupTaskBinding({ ...subject, bindingSha256: deletionCleanupTaskBindingDigestV1(subject),
              recordedAt: DateTime.formatIso(yield* DateTime.now) });
          }
        }
        const tasks: DeletionCleanupTaskBindingV1[] = [];
        for (const effectId of original.inventory.prerequisiteEffectIds) {
          const task = yield* readDeletionCleanupTaskEffect(effectId);
          if (task !== null) tasks.push(task);
        }
        return tasks;
      })).pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause })));
    const recordLeaseCleanupTaskOutcome: EventSinkV2Shape["recordLeaseCleanupTaskOutcome"] = (input) =>
      withTransaction(Effect.gen(function* () {
        const task = (yield* readDeletionCleanupTaskEffect(input.effectId)) ?? (yield* readDeletionWorktreeTaskEffect(input.effectId));
        const outcome = yield* Schema.decodeUnknownEffect(LeaseCleanupTaskOutcomeV2)(input.outcome, { onExcessProperty: "error" });
        if (task === null || outcome.taskId !== input.effectId || !Number.isSafeInteger(input.expectedAttempt) || input.expectedAttempt < 1 ||
            input.workerId.length === 0 || outcome.result === "succeeded" || outcome.effect === "confirmed" || outcome.effect === "absent")
          return yield* new EventSinkWriteError({ eventCount: 0, cause: "Cleanup outcome has no available complete owner-effect proof" });
        if (task.task.kind === "worktree" && input.evidence.schema === "t3.deletion-worktree-removal-start/v1")
          return yield* new EventSinkWriteError({ eventCount: 0, cause: "Worktree native start must use its atomic reservation port" });
        let providerEvidence: ProviderNativeEffectEvidence | null = null;
        let resourceEvidence: EffectOutbox.ResourceCleanupUnknownEvidenceV1 | null = null;
        if (task.task.kind === "provider") {
          const evidence = yield* Schema.decodeUnknownEffect(ProviderNativeEffectEvidence)(input.evidence, { onExcessProperty: "error" });
          const expected = task.task.expectedBinding;
          if (evidence.operationId !== input.effectId || evidence.operation !== "close_session" || evidence.threadId !== expected.threadId ||
              evidence.providerThreadId !== expected.providerThreadId || evidence.providerSessionId !== expected.providerSessionId ||
              evidence.instanceId !== expected.instanceId || evidence.runtimeGeneration !== expected.runtimeGeneration ||
              evidence.outcome !== (outcome.effect === "unknown" ? "unknown" : "known_no_effect"))
            return yield* new EventSinkWriteError({ eventCount: 0, cause: "Cleanup outcome differs from its complete pinned provider operation" });
          providerEvidence = evidence;
        } else {
          if (outcome.effect !== "unknown")
            return yield* new EventSinkWriteError({ eventCount: 0, cause: "Resource cleanup has no complete no-effect producer" });
          resourceEvidence = { version: 1, kind: "resource_cleanup", operationId: input.effectId, threadId: task.threadId,
            taskKind: task.task.kind, bindingSha256: task.bindingSha256, outcome: "unknown" };
        }
        const correlation = { workerId: input.workerId, expectedAttempt: input.expectedAttempt,
          bindingSha256: task.bindingSha256, evidence: input.evidence };
        const previous = yield* sql<{ readonly outcome_json: string }>`SELECT outcome_json FROM orchestration_v2_lease_cleanup_task_outcomes
          WHERE effect_id = ${input.effectId} AND correlation_json = ${nativeCreationCanonicalJson(correlation)}`;
        if (previous.length > 1) return yield* new EventSinkWriteError({ eventCount: 0, cause: "Cleanup outcome correlation is ambiguous" });
        if (previous.length === 1) {
          const stored = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(LeaseCleanupTaskOutcomeV2))(previous[0]!.outcome_json);
          if (nativeCreationCanonicalJson(stored) !== nativeCreationCanonicalJson(outcome))
            return yield* new EventSinkWriteError({ eventCount: 0, cause: "Cleanup outcome changed on replay" });
          return stored;
        }
        const now = DateTime.formatIso(yield* DateTime.now);
        const effect = Option.getOrNull(yield* effectOutbox.get(input.effectId));
        if (effect === null || effect.status !== "running" || effect.leaseOwner !== input.workerId || effect.attemptCount !== input.expectedAttempt ||
            effect.leaseExpiresAt === null || !Number.isFinite(Date.parse(effect.leaseExpiresAt)) || Date.parse(effect.leaseExpiresAt) <= Date.parse(now))
          return yield* new EventSinkWriteError({ eventCount: 0, cause: "Cleanup outcome lost its exact owned unexpired claim" });
        const ordinal = yield* sql<{ readonly next: number }>`SELECT COALESCE(MAX(ordinal), -1) + 1 AS next
          FROM orchestration_v2_lease_cleanup_task_outcomes WHERE effect_id = ${input.effectId}`;
        yield* sql`INSERT INTO orchestration_v2_lease_cleanup_task_outcomes (effect_id, ordinal, outcome_json, correlation_json, recorded_at)
          VALUES (${input.effectId}, ${ordinal[0]!.next}, ${nativeCreationCanonicalJson(outcome)}, ${nativeCreationCanonicalJson(correlation)}, ${now})`;
        if (task.task.kind === "worktree" && outcome.effect === "unknown") {
          yield* sql`UPDATE orchestration_v2_worktree_path_admissions SET state = 'unknown',
            outcome_json = ${nativeCreationCanonicalJson({ outcome, correlation })}, updated_at = ${now}
            WHERE operation_id = ${input.effectId} AND kind = 'worktree_removal' AND state IN ('reserved', 'started')
              AND json_extract(subject_json, '$.bindingSha256') = ${task.bindingSha256}`;
        }
        if (outcome.effect === "unknown") {
          const held = providerEvidence !== null ? yield* effectOutbox.holdUnknown({ effectId: input.effectId, workerId: input.workerId,
            expectedAttempt: input.expectedAttempt, operationId: input.effectId, evidence: providerEvidence }) :
            resourceEvidence !== null ? yield* effectOutbox.holdResourceCleanupUnknown({ effectId: input.effectId,
              workerId: input.workerId, expectedAttempt: input.expectedAttempt, evidence: resourceEvidence }) : false;
          if (!held) {
            const matches = (yield* effectOutbox.listHeldByThreadId(task.threadId)).filter((hold) =>
              hold.effectId === input.effectId && hold.workerId === input.workerId && hold.operationId === input.effectId &&
              hold.expectedAttempt === input.expectedAttempt &&
              nativeCreationCanonicalJson(hold.evidence) === nativeCreationCanonicalJson(providerEvidence ?? resourceEvidence));
            if (matches.length !== 1)
              return yield* new EventSinkWriteError({ eventCount: 0, cause: "Cleanup unknown outcome could not atomically retain its exact operation hold" });
          }
        }
        return outcome;
      })).pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause })));
    const targetTables = [
      "threads", "runs", "run_attempts", "nodes", "provider_threads", "provider_turns",
      "runtime_requests", "messages", "plans", "turn_items", "checkpoint_scopes", "checkpoints",
      "context_handoffs", "subagents",
    ] as const;
    const readCommitSnapshot = Effect.fnUntraced(function* (
      threadId: ThreadId, commandId: CommandId, authority: NativeCommandAuthorityReadV2,
    ): Effect.fn.Return<NativeCommandTargetSnapshotV2, unknown> {
      const records: Record<string, ReadonlyArray<Readonly<Record<string, unknown>>>> = {};
      for (const table of targetTables) {
        records[table] = yield* sql`SELECT * FROM ${sql(`orchestration_v2_projection_${table}`)}
          WHERE thread_id = ${threadId} ORDER BY rowid`;
      }
      records.context_transfers = yield* sql`SELECT * FROM orchestration_v2_projection_context_transfers
        WHERE source_thread_id = ${threadId} OR target_thread_id = ${threadId} ORDER BY context_transfer_id`;
      records.project = yield* sql`SELECT * FROM projection_projects
        WHERE project_id = ${authority.projectId ?? null} OR project_id IN (
          SELECT project_id FROM orchestration_v2_projection_threads WHERE thread_id = ${threadId}
        ) ORDER BY project_id`;
      records.projection_schema = yield* sql`SELECT projection_name, schema_version
        FROM orchestration_v2_projection_metadata WHERE projection_name = 'thread-projections'`;
      records.source_runtime = yield* sql`SELECT * FROM provider_session_runtime WHERE thread_id = ${threadId}`;
      records.turn_item_positions = yield* sql`SELECT * FROM orchestration_v2_turn_item_positions WHERE thread_id = ${threadId} ORDER BY turn_item_id`;
      records.provider_sessions = yield* sql`
        SELECT session.* FROM orchestration_v2_projection_provider_sessions session
        WHERE session.provider_session_id IN (
          SELECT provider_session_id FROM orchestration_v2_projection_provider_session_bindings WHERE thread_id = ${threadId}
          UNION SELECT provider_session_id FROM orchestration_v2_projection_provider_threads WHERE thread_id = ${threadId}
        ) ORDER BY session.provider_session_id`;
      records.session_bindings = yield* sql`SELECT * FROM orchestration_v2_projection_provider_session_bindings
        WHERE thread_id = ${threadId} ORDER BY provider_session_id`;
      records.effects = yield* sql`SELECT * FROM orchestration_v2_effect_outbox WHERE thread_id = ${threadId} ORDER BY effect_id`;
      records.thread_deletion_commands = yield* sql`SELECT * FROM orchestration_v2_thread_deletion_commands WHERE thread_id = ${threadId} ORDER BY command_id`;
      records.cleanup_task_bindings = yield* sql`SELECT * FROM orchestration_v2_lease_cleanup_task_bindings WHERE thread_id = ${threadId} ORDER BY effect_id`;
      records.cleanup_task_outcomes = yield* sql`SELECT outcome.* FROM orchestration_v2_lease_cleanup_task_outcomes outcome
        JOIN orchestration_v2_lease_cleanup_task_bindings binding ON binding.effect_id = outcome.effect_id
        WHERE binding.thread_id = ${threadId} ORDER BY outcome.effect_id, outcome.ordinal`;
      records.worktree_path_admissions = yield* sql`SELECT * FROM orchestration_v2_worktree_path_admissions
        WHERE state NOT IN ('no_effect', 'released') OR json_extract(subject_json, '$.threadId') = ${threadId} ORDER BY operation_id`;
      records.unknown_effect_holds = yield* sql`SELECT hold.* FROM orchestration_v2_unknown_effect_holds hold
        JOIN orchestration_v2_effect_outbox effect ON effect.effect_id = hold.effect_id
        WHERE effect.thread_id = ${threadId} ORDER BY hold.effect_id`;
      records.launch_workflows = yield* sql`SELECT * FROM orchestration_v2_thread_launch_workflows WHERE thread_id = ${threadId} ORDER BY command_id`;
      records.runtime_evidence = yield* sql`SELECT * FROM orchestration_v2_provider_runtime_evidence WHERE thread_id = ${threadId}`;
      records.continuation_sources = yield* sql`SELECT * FROM orchestration_v2_provider_continuation_sources WHERE thread_id = ${threadId} ORDER BY source_id`;
      records.restart_continuations = yield* sql`SELECT * FROM orchestration_v2_restart_continuation_markers WHERE thread_id = ${threadId} ORDER BY marker_id`;
      records.legacy_continuation = yield* sql`SELECT * FROM orchestration_v2_legacy_continuation_dispositions WHERE thread_id = ${threadId}`;
      records.native_import_seals = yield* sql`SELECT * FROM orchestration_v2_native_import_transcript_seals WHERE thread_id = ${threadId}`;
      records.legacy_import_markers = yield* sql`SELECT * FROM orchestration_v2_legacy_imports WHERE thread_id = ${threadId}`;
      records.legacy_source_threads = yield* sql`SELECT * FROM projection_threads WHERE thread_id = ${threadId}`;
      records.legacy_source_messages = yield* sql`SELECT * FROM projection_thread_messages WHERE thread_id = ${threadId}
        AND role IN ('user', 'assistant') ORDER BY created_at, message_id`;
      records.imported_source_events = yield* sql`SELECT * FROM orchestration_events WHERE stream_id = ${threadId}
        AND application_event_version = 2 AND aggregate_kind = 'thread'
        AND (event_id LIKE 'migration:v1:%' OR event_id LIKE 'agent-session-import:v2:%') ORDER BY sequence`;
      records.native_confirmations = yield* sql`SELECT * FROM orchestration_v2_native_effect_confirmations WHERE thread_id = ${threadId} ORDER BY effect_id`;
      records.imported_choices = yield* sql`SELECT * FROM orchestration_v2_imported_history_start_choices WHERE thread_id = ${threadId} ORDER BY command_id`;
      records.imported_outcomes = yield* sql`SELECT outcome.* FROM orchestration_v2_imported_history_start_outcomes outcome
        JOIN orchestration_v2_imported_history_start_choices choice ON choice.command_id = outcome.command_id
        WHERE choice.thread_id = ${threadId} ORDER BY outcome.command_id`;
      records.stop_intents = yield* sql`SELECT * FROM orchestration_v2_current_runtime_stop_intents WHERE thread_id = ${threadId} ORDER BY command_id`;
      records.stop_fences = yield* sql`SELECT * FROM orchestration_v2_queued_runtime_stop_fences WHERE thread_id = ${threadId} ORDER BY stop_command_id, run_id`;
      records.start_reservations = yield* sql`SELECT * FROM orchestration_v2_queued_start_reservations WHERE thread_id = ${threadId} ORDER BY effect_id`;
      records.workstream_witnesses = yield* sql`SELECT * FROM orchestration_v2_workstream_settlement_witnesses WHERE thread_id = ${threadId} ORDER BY command_id`;
      const authorityRecords: Record<string, ReadonlyArray<Readonly<Record<string, unknown>>>> = {};
      authorityRecords.sessions = yield* sql`SELECT session_id, subject, scopes, method, issued_at, expires_at, revoked_at
        FROM auth_sessions WHERE session_id = ${authority.actorSessionId ?? null}`;
      authorityRecords.automation_enrollment = yield* sql`SELECT * FROM native_creation_automation_enrollments WHERE session_id = ${authority.actorSessionId ?? null}`;
      authorityRecords.provider_enrollment = yield* sql`SELECT * FROM workstreams_native_enrollments WHERE session_id = ${authority.actorSessionId ?? null}`;
      authorityRecords.attempts = yield* sql`SELECT * FROM workstreams_native_attempts
        WHERE native_command_id = ${commandId} OR json_extract(request_json, '$.identity.native_id') = ${threadId} ORDER BY owner_id, principal_id, command_id`;
      authorityRecords.claims = yield* sql`SELECT * FROM native_creation_intents
        WHERE claim_id = ${authority.claimId ?? null} OR thread_id = ${threadId}
          OR worktree_path IN ${sql.in(authority.resourcePaths ?? [])} ORDER BY claim_id`;
      for (const table of ["normalized_commands", "reserved_commands", "reserved_command_identities", "effect_facts"] as const) {
        authorityRecords[table] = yield* sql`SELECT * FROM ${sql(`native_creation_${table}`)}
          WHERE claim_id IN ${sql.in(authorityRecords.claims.map((claim) => claim.claim_id))}
          ORDER BY rowid`;
      }
      authorityRecords.leases = yield* sql`SELECT * FROM worktree_ownership_leases
        WHERE owner_thread_id = ${threadId} OR resource_path IN ${sql.in(authority.resourcePaths ?? [])} ORDER BY resource_path`;
      const decodeStoredJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
      for (const group of [records, authorityRecords]) {
        for (const rows of Object.values(group)) {
          for (const row of rows) for (const [key, value] of Object.entries(row)) {
            if (key.endsWith("_json") && value !== null) yield* decodeStoredJson(value);
          }
        }
      }
      const latest = yield* eventStore.latestSequence({ threadId });
      const birth = yield* readIncarnation(threadId);
      return { commandId, threadId, targetEventSequence: latest, ...birth, records, authority, authorityRecords };
    });
    const readNativeCommandFactsEffect = Effect.fnUntraced(function* (
      input: Parameters<EventSinkV2Shape["readNativeCommandFacts"]>[0],
    ) {
      return yield* sql.withTransaction(Effect.gen(function* () {
        const commitSnapshot = yield* readCommitSnapshot(input.threadId, input.commandId, input.authority ?? {});
        const receipt = yield* commandReceipts.getByCommandId(input.commandId);
        const eventMetadata = yield* sql<OrchestrationCommandEventMetadata>`
          SELECT event_id AS "eventId", command_id AS "commandId", aggregate_kind AS "aggregateKind",
            stream_id AS "aggregateId", sequence, event_type AS type, occurred_at AS "occurredAt",
            application_event_version AS "applicationEventVersion"
          FROM orchestration_events WHERE command_id = ${input.commandId} ORDER BY sequence ASC LIMIT 257`;
        const events = yield* eventStore.read({ commandId: input.commandId, limit: 257 }).pipe(Stream.runCollect);
        const localProjection = (commitSnapshot.records.threads?.length ?? 0) === 0
          ? null : yield* projectionStore.getThreadRecords(input.threadId, [
            "runs", "attempts", "nodes", "subagents", "providerSessions", "providerThreads",
            "providerTurns", "runtimeRequests", "messages", "plans", "turnItems", "checkpointScopes",
            "checkpoints", "contextHandoffs", "contextTransfers",
          ]);
        return {
          commandId: input.commandId, threadId: input.threadId,
          receipt: Option.getOrNull(receipt), identity: yield* readIdentity(input.commandId),
          events, eventMetadata, eventMetadataOverflow: eventMetadata.length > 256,
          snapshotSequence: yield* eventStore.latestApplicationSequence,
          targetEventSequence: commitSnapshot.targetEventSequence, incarnation: commitSnapshot.incarnation,
          creationProvenance: commitSnapshot.creationProvenance,
          projection: localProjection === null ? null : { ...localProjection, visibleTurnItems: [], updatedAt: localProjection.thread.updatedAt }, commitSnapshot,
          creationHistory: commitSnapshot.authorityRecords.claims ?? [],
          nativeCreationHistory: yield* nativeCreationRepository.readBoundedHistoryByThread(input.threadId),
          workstreamWitness: yield* readWorkstreamWitness(input.commandId),
        } satisfies NativeCommandFactsV2;
      }));
    });
    const recordLegacyContinuationDisposition: EventSinkV2Shape["recordLegacyContinuationDisposition"] = (input) =>
      withTransaction(Effect.gen(function* () {
        const evidence = input.evidence === null ? null : yield* Schema.decodeUnknownEffect(LegacyProviderContinuationEvidenceV1)(input.evidence);
        if (evidence !== null && (evidence.threadId !== input.threadId || evidence.provenance !== input.provenance))
          return yield* new EventSinkWriteError({ eventCount: 0, cause: "Continuation evidence disagrees with import identity" });
        const qualified = yield* Schema.decodeUnknownEffect(ContinuationQualification)(input.qualification);
        if (qualified.type === "qualified" && (evidence === null || evidence.stoppedProof === null ||
            evidence.historicalSourceIdentity === null || evidence.accessibility === null ||
            qualified.nativeThreadId !== evidence.nativeThreadId || qualified.continuationKey !== evidence.accessibility.continuationKey))
          return yield* new EventSinkWriteError({ eventCount: 0, cause: "Qualified legacy continuation lacks its historical proof" });
        const qualification = nativeCreationCanonicalJson(qualified);
        const encoded = evidence === null ? null : Schema.encodeSync(Schema.fromJsonString(LegacyProviderContinuationEvidenceV1))(evidence);
        const existing = yield* sql<{ readonly qualification_json: string; readonly evidence_json: string | null; readonly provenance: string }>`
          SELECT qualification_json, evidence_json, provenance FROM orchestration_v2_legacy_continuation_dispositions WHERE thread_id = ${input.threadId}`;
        if (existing.length > 0) {
          const row = existing[0]!;
          if (row.qualification_json !== qualification || row.evidence_json !== encoded || row.provenance !== input.provenance)
            return yield* new EventSinkWriteError({ eventCount: 0, cause: "Legacy continuation disposition is immutable" });
          return;
        }
        yield* sql`INSERT INTO orchestration_v2_legacy_continuation_dispositions
          (thread_id, provenance, qualification_json, evidence_json, imported_at)
          VALUES (${input.threadId}, ${input.provenance}, ${qualification}, ${encoded}, ${input.importedAt})`;
      })).pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause })));

    const readLegacyContinuationDisposition: EventSinkV2Shape["readLegacyContinuationDisposition"] = (threadId) =>
      sql.withTransaction(Effect.gen(function* () {
        const rows = yield* sql<{ readonly provenance: "legacy_row" | "native_import"; readonly qualification_json: string;
          readonly evidence_json: string | null; readonly imported_at: string }>`
          SELECT provenance, qualification_json, evidence_json, imported_at
          FROM orchestration_v2_legacy_continuation_dispositions WHERE thread_id = ${threadId}`;
        if (rows.length === 0) return null;
        const row = rows[0]!;
        return { threadId, provenance: row.provenance,
          qualification: yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ContinuationQualification))(row.qualification_json),
          evidence: row.evidence_json === null ? null : yield* Schema.decodeUnknownEffect(Schema.fromJsonString(LegacyProviderContinuationEvidenceV1))(row.evidence_json),
          importedAt: row.imported_at } satisfies LegacyContinuationDispositionV1;
      })).pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause })));

    const importSealFailure = (cause: string) => Effect.fail(new EventSinkWriteError({ eventCount: 0, cause }));
    const encodeImportEvents = Effect.fnUntraced(function* (threadId: ThreadId, events: ReadonlyArray<OrchestrationV2DomainEvent>) {
      if (events.length < 2 || events.length > 400 || events.length % 2 !== 0)
        return yield* importSealFailure("Native import seal requires 1..200 complete visible message pairs");
      const encoded = yield* Effect.forEach(events, (event) => Schema.encodeEffect(OrchestrationV2DomainEventJson)(event), { concurrency: 1 });
      for (let index = 0; index < encoded.length / 2; index++) {
        const message = encoded[index * 2]!;
        const item = encoded[index * 2 + 1]!;
        const suffix = String(index).padStart(6, "0");
        if (message.type !== "message.updated" || item.type !== "turn-item.updated" ||
            message.threadId !== threadId || item.threadId !== threadId ||
            message.id !== `agent-session-import:v2:message:${threadId}:${suffix}` ||
            item.id !== `agent-session-import:v2:turn-item:${threadId}:${suffix}`)
          return yield* importSealFailure("Native import event pair identity differs from the completed parser subset");
        const payload = message.payload;
        if ((payload.role !== "user" && payload.role !== "assistant") || payload.id !== `${threadId}:${suffix}` ||
            payload.threadId !== threadId || payload.runId !== null || payload.nodeId !== null ||
            payload.createdBy !== (payload.role === "user" ? "user" : "agent") || payload.creationSource !== "server" ||
            payload.attachments.length !== 0 || payload.streaming || payload.createdAt !== message.occurredAt ||
            payload.updatedAt !== message.occurredAt || item.occurredAt !== message.occurredAt)
          return yield* importSealFailure("Native import message is not the exact normalized visible snapshot");
        const common = { id: item.id, threadId, runId: null, nodeId: null, providerThreadId: null, providerTurnId: null,
          nativeItemRef: null, parentItemId: null, ordinal: index + 1, status: "completed", title: null,
          startedAt: message.occurredAt, completedAt: message.occurredAt, updatedAt: message.occurredAt };
        const expected = payload.role === "user"
          ? { ...common, createdBy: "user", creationSource: "server", type: "user_message", messageId: payload.id,
            inputIntent: "turn_start", text: payload.text, attachments: [] }
          : { ...common, type: "assistant_message", messageId: payload.id, text: payload.text, streaming: false };
        if (nativeCreationCanonicalJson(item.payload) !== nativeCreationCanonicalJson(expected))
          return yield* importSealFailure("Native import turn item differs from its message payload or ordinal");
      }
      return encoded.map((event) => ({ id: event.id, type: event.type, threadId: event.threadId,
        occurredAt: event.occurredAt, payload: event.payload }));
    });
    const validateImportSource = Effect.fnUntraced(function* (threadId: ThreadId, source: AgentSessionImportSource) {
      const decoded = yield* Schema.decodeUnknownEffect(AgentSessionImportSource)(source, { onExcessProperty: "error" });
      if (threadId !== `import:${decoded.providerInstanceId}:${decoded.providerSessionId}`)
        return yield* importSealFailure("Native import source does not identify this imported thread");
      const rows = yield* sql<{ readonly provider_name: string; readonly provider_instance_id: string | null;
        readonly adapter_key: string; readonly resume_cursor_json: string; readonly runtime_payload_json: string }>`
        SELECT provider_name, provider_instance_id, adapter_key, resume_cursor_json, runtime_payload_json
        FROM provider_session_runtime WHERE thread_id = ${threadId}`;
      const row = rows[0];
      if (row === undefined || row.provider_name !== decoded.provider || row.adapter_key !== decoded.provider ||
          row.provider_instance_id !== decoded.providerInstanceId)
        return yield* importSealFailure("Native import runtime source identity differs from the parser source");
      const cursorValue = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(row.resume_cursor_json);
      const runtimeValue = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(row.runtime_payload_json);
      if (cursorValue === null || typeof cursorValue !== "object" || runtimeValue === null || typeof runtimeValue !== "object")
        return yield* importSealFailure("Native import runtime marker is malformed");
      const cursor = cursorValue as Record<string, unknown>;
      const runtime = runtimeValue as Record<string, unknown>;
      const sources = runtime.importedTranscripts;
      if (runtime.importOrigin !== "native_import" || !Array.isArray(sources) ||
          (decoded.provider === "codex" ? cursor.threadId !== decoded.providerSessionId : cursor.resume !== decoded.providerSessionId))
        return yield* importSealFailure("Native import source marker or native session correlation is missing");
      const matching = sources.filter((value: unknown) => value !== null && typeof value === "object" &&
        (value as Record<string, unknown>).providerInstanceId === decoded.providerInstanceId &&
        (value as Record<string, unknown>).filePath === decoded.filePath);
      if (matching.length !== 1 || nativeCreationCanonicalJson(matching[0]) !== nativeCreationCanonicalJson(decoded))
        return yield* importSealFailure("Native import source marker changed after this snapshot");
      return decoded;
    });
    const recordNativeImportTranscriptSeal: EventSinkV2Shape["recordNativeImportTranscriptSeal"] = (input) =>
      Effect.gen(function* () {
        yield* assertPublicationScope;
        const written = yield* TransactionWrittenEvents;
        if (written === undefined || input.parserPolicy !== "agent_session_visible_messages_v1" ||
            !Number.isFinite(Date.parse(input.importedAt)))
          return yield* importSealFailure("Native import sealing requires a completed parser inside EventSink.withTransaction");
        const source = yield* validateImportSource(input.threadId, input.source);
        const encoded = yield* encodeImportEvents(input.threadId, input.messageEvents);
        const birthId = EventId.make(`agent-session-import:v2:thread:${input.threadId}:created`);
        const birth = written.get(birthId);
        if (birth === undefined || birth.event.type !== "thread.created" || birth.event.threadId !== input.threadId ||
            birth.event.payload.id !== input.threadId || birth.event.payload.projectId !== input.projectId ||
            birth.event.payload.historyOrigin !== "v1_import" || birth.event.payload.providerInstanceId !== source.providerInstanceId ||
            birth.event.payload.modelSelection.instanceId !== source.providerInstanceId || birth.commandId !== null)
          return yield* importSealFailure("Native import seal has no matching newly written imported thread birth");
        const births = yield* sql`SELECT event_id FROM orchestration_events WHERE stream_id = ${input.threadId}
          AND aggregate_kind = 'thread' AND application_event_version = 2 AND event_type = 'thread.created'`;
        if (births.length !== 1)
          return yield* importSealFailure("Native import seal has ambiguous thread birth");
        const disposition = yield* readLegacyContinuationDisposition(input.threadId);
        if (disposition?.provenance !== "native_import")
          return yield* importSealFailure("Native import seal has no native import disposition");
        const actual = yield* Effect.forEach(input.messageEvents, (event) => {
          const stored = written.get(event.id);
          return stored === undefined || stored.commandId !== null
            ? importSealFailure("Native import seal cannot adopt preexisting or command-attributed events")
            : Effect.succeed(stored);
        }, { concurrency: 1 });
        const actualEncoded = yield* encodeImportEvents(input.threadId, actual.map((stored) => stored.event));
        if (nativeCreationCanonicalJson(encoded) !== nativeCreationCanonicalJson(actualEncoded) ||
            actual.some((stored, index) => stored.sequence <= (index === 0 ? birth.sequence : actual[index - 1]!.sequence)))
          return yield* importSealFailure("Native import seal differs from the actual committed event payload or order");
        const seal: NativeImportTranscriptSealV1 = { version: 1, threadId: input.threadId, projectId: input.projectId,
          provenance: "native_import", source, parserPolicy: input.parserPolicy, messageCount: encoded.length / 2,
          eventsSha256: nativeCreationSha256(nativeCreationCanonicalJson(encoded)),
          eventBasis: actual.map((stored) => ({ eventId: stored.event.id, sequence: stored.sequence })),
          birth: { eventId: birthId, sequence: birth.sequence }, importedAt: input.importedAt };
        yield* sql`INSERT INTO orchestration_v2_native_import_transcript_seals
          (schema_version, thread_id, project_id, provenance, source_identity_json, parser_policy, message_count,
            events_sha256, event_basis_json, birth_event_id, birth_sequence, imported_at)
          VALUES (1, ${seal.threadId}, ${seal.projectId}, 'native_import', ${nativeCreationCanonicalJson(source)},
            ${seal.parserPolicy}, ${seal.messageCount}, ${seal.eventsSha256}, ${nativeCreationCanonicalJson(seal.eventBasis)},
            ${seal.birth.eventId}, ${seal.birth.sequence}, ${seal.importedAt})`;
        return seal;
      }).pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: input.messageEvents.length, cause })));
    const readNativeImportTranscriptSeal: EventSinkV2Shape["readNativeImportTranscriptSeal"] = (threadId) =>
      sql.withTransaction(Effect.gen(function* () {
        const rows = yield* sql<{ readonly schema_version: number; readonly thread_id: string; readonly project_id: string;
          readonly provenance: string; readonly source_identity_json: string; readonly parser_policy: string;
          readonly message_count: number; readonly events_sha256: string; readonly event_basis_json: string;
          readonly birth_event_id: string; readonly birth_sequence: number; readonly imported_at: string }>`
          SELECT * FROM orchestration_v2_native_import_transcript_seals WHERE thread_id = ${threadId}`;
        if (rows.length === 0) return null;
        const row = rows[0]!;
        const validated = yield* Effect.gen(function* () {
          if (row.schema_version !== 1 || row.provenance !== "native_import" || row.parser_policy !== "agent_session_visible_messages_v1")
            return yield* importSealFailure("Unsupported native import snapshot seal");
          const source = yield* validateImportSource(threadId,
            yield* Schema.decodeUnknownEffect(Schema.fromJsonString(AgentSessionImportSource))(row.source_identity_json));
          const basis = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Array(Schema.Struct({
            eventId: EventId, sequence: Schema.Int,
          }))))(row.event_basis_json);
          if (basis.length !== row.message_count * 2 || basis.length < 2 || basis.length > 400 ||
              basis.some((entry, index) => entry.sequence <= (index === 0 ? row.birth_sequence : basis[index - 1]!.sequence)))
            return yield* importSealFailure("Native import snapshot basis is incomplete or reordered");
          const births = yield* sql<{ readonly payload_json: string }>`SELECT payload_json FROM orchestration_events
            WHERE event_id = ${row.birth_event_id} AND sequence = ${row.birth_sequence} AND application_event_version = 2
              AND aggregate_kind = 'thread' AND stream_id = ${threadId} AND event_type = 'thread.created' AND command_id IS NULL`;
          const projected = yield* sql<{ readonly project_id: string }>`SELECT project_id FROM orchestration_v2_projection_threads WHERE thread_id = ${threadId}`;
          const birthValue = births[0] === undefined ? null : yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(births[0].payload_json);
          const birth = birthValue !== null && typeof birthValue === "object" ? birthValue as Record<string, unknown> : null;
          if (birth === null || birth.id !== threadId || birth.projectId !== row.project_id || birth.historyOrigin !== "v1_import" ||
              birth.providerInstanceId !== source.providerInstanceId || projected[0]?.project_id !== row.project_id ||
              (yield* readLegacyContinuationDisposition(threadId))?.provenance !== "native_import")
            return yield* importSealFailure("Native import snapshot birth, project or provenance changed");
          const stored = yield* sql<{ readonly sequence: number; readonly event_id: string; readonly event_type: string;
            readonly occurred_at: string; readonly payload_json: string }>`
            SELECT event.sequence, event.event_id, event.event_type, event.occurred_at, event.payload_json
            FROM json_each(${row.event_basis_json}) basis JOIN orchestration_events event
              ON event.event_id = json_extract(basis.value, '$.eventId') AND event.sequence = json_extract(basis.value, '$.sequence')
            WHERE event.application_event_version = 2 AND event.aggregate_kind = 'thread' AND event.stream_id = ${threadId}
              AND event.command_id IS NULL ORDER BY CAST(basis.key AS INTEGER)`;
          if (stored.length !== basis.length) return yield* importSealFailure("Native import snapshot event is missing or misattributed");
          const events = yield* Effect.forEach(stored, (event) => Effect.gen(function* () {
            const payload = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(event.payload_json);
            return yield* Schema.decodeUnknownEffect(OrchestrationV2DomainEventJson)({
              id: event.event_id, type: event.event_type, threadId, occurredAt: event.occurred_at, payload,
            });
          }), { concurrency: 1 });
          const encoded = yield* encodeImportEvents(threadId, events);
          if (nativeCreationSha256(nativeCreationCanonicalJson(encoded)) !== row.events_sha256)
            return yield* importSealFailure("Native import snapshot payload digest changed");
          return { version: 1, threadId, projectId: ProjectId.make(row.project_id), provenance: "native_import", source,
            parserPolicy: "agent_session_visible_messages_v1", messageCount: row.message_count, eventsSha256: row.events_sha256,
            eventBasis: basis, birth: { eventId: EventId.make(row.birth_event_id), sequence: row.birth_sequence },
            importedAt: row.imported_at } satisfies NativeImportTranscriptSealV1;
        }).pipe(Effect.option);
        return Option.getOrNull(validated);
      })).pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause })));

    const readProviderRuntimeEvidenceEffect = Effect.fnUntraced(function* (threadId: ThreadId) {
      const rows = yield* sql<{ readonly thread_id: string; readonly provider_thread_id: string; readonly provider_session_id: string;
        readonly provider_instance_id: string; readonly driver: ProviderDriverKind; readonly native_thread_id: string | null;
        readonly runtime_generation: string; readonly evidence_revision: number; readonly observation_json: string | null; readonly registered_at: string }>`
        SELECT * FROM orchestration_v2_provider_runtime_evidence WHERE thread_id = ${threadId}`;
      if (rows.length === 0) return null;
      const row = rows[0]!;
      const binding = yield* Schema.decodeUnknownEffect(Schema.Struct({
        threadId: ThreadId, providerThreadId: ProviderThreadId, providerSessionId: ProviderSessionId,
        instanceId: ProviderInstanceId, driver: ProviderDriverKind, nativeThreadId: Schema.NullOr(Schema.String), runtimeGeneration: Schema.NonEmptyString,
      }))({ threadId: row.thread_id, providerThreadId: row.provider_thread_id, providerSessionId: row.provider_session_id,
        instanceId: row.provider_instance_id, driver: row.driver, nativeThreadId: row.native_thread_id, runtimeGeneration: row.runtime_generation });
      if (!Number.isSafeInteger(row.evidence_revision) || row.evidence_revision < 1)
        return yield* new EventSinkWriteError({ eventCount: 0, cause: "Invalid runtime evidence revision" });
      return { binding, evidenceRevision: row.evidence_revision,
        observation: row.observation_json === null ? null : yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ProviderRuntimeObservation))(row.observation_json),
        registeredAt: row.registered_at } satisfies ProviderRuntimeEvidenceV2;
    });
    const readCurrentProviderRuntimeOwnerEffect = Effect.fnUntraced(function* (threadId: ThreadId) {
      const current = yield* readProviderRuntimeEvidenceEffect(threadId);
      if (current === null) return null;
      const binding = current.binding;
      const rows = yield* sql`SELECT provider.provider_thread_id FROM orchestration_v2_projection_threads thread
        JOIN orchestration_v2_projection_provider_threads provider ON provider.provider_thread_id = ${binding.providerThreadId}
        JOIN orchestration_v2_projection_provider_sessions session ON session.provider_session_id = provider.provider_session_id
        JOIN orchestration_v2_projection_provider_session_bindings attached
          ON attached.provider_session_id = session.provider_session_id AND attached.thread_id = thread.thread_id
        WHERE thread.thread_id = ${threadId} AND json_extract(thread.payload_json, '$.deletedAt') IS NULL
          AND json_extract(thread.payload_json, '$.activeProviderThreadId') = provider.provider_thread_id
          AND provider.thread_id = thread.thread_id AND provider.provider_session_id = ${binding.providerSessionId}
          AND provider.provider_instance_id = ${binding.instanceId} AND provider.driver = ${binding.driver}
          AND json_extract(provider.payload_json, '$.nativeThreadRef.nativeId') IS ${binding.nativeThreadId}
          AND session.provider_instance_id = ${binding.instanceId} AND session.driver = ${binding.driver}`;
      return rows.length === 1 ? current : null;
    });
    const readConfirmedImportedHistoryBindingEffect = (input: Parameters<EventSinkV2Shape["readConfirmedImportedHistoryBinding"]>[0], historical = false) =>
      Effect.gen(function* () {
        const current = yield* readCurrentProviderRuntimeOwnerEffect(input.expectedBinding.threadId);
        if (current === null || current.evidenceRevision !== input.expectedEvidenceRevision ||
            nativeCreationCanonicalJson(current.binding) !== nativeCreationCanonicalJson(input.expectedBinding) ||
            input.expectedBinding.nativeThreadId === null || input.expectedBinding.runtimeGeneration === null) return null;
        const rows = yield* sql<{ readonly effect_id: string; readonly evidence_revision: number; readonly native_execution_reference_json: string | null }>`
          SELECT effect_id, evidence_revision, native_execution_reference_json FROM orchestration_v2_native_effect_confirmations
          WHERE thread_id = ${input.expectedBinding.threadId}
            AND json_extract(binding_json, '$.threadId') = ${input.expectedBinding.threadId}
            AND json_extract(binding_json, '$.providerThreadId') = ${input.expectedBinding.providerThreadId}
            ${historical ? sql`AND 1 = 1` : sql`AND json_extract(binding_json, '$.providerSessionId') = ${input.expectedBinding.providerSessionId}`}
            AND json_extract(binding_json, '$.instanceId') = ${input.expectedBinding.instanceId}
            AND json_extract(binding_json, '$.nativeThreadId') = ${input.expectedBinding.nativeThreadId}
            ${historical ? sql`AND 1 = 1` : sql`AND json_extract(binding_json, '$.runtimeGeneration') = ${input.expectedBinding.runtimeGeneration}`}`;
        if (rows.length !== 1 || rows[0]!.native_execution_reference_json !== null ||
            rows[0]!.evidence_revision > current.evidenceRevision) return null;
        const proof = Option.getOrNull(yield* nativeCreationRepository.readNativeEffectConfirmation(rows[0]!.effect_id).pipe(Effect.option));
        if (proof === null || proof.nativeExecutionReference !== null) return null;
        const choice = yield* readImportedHistoryStartChoiceEffect({ threadId: proof.threadId, commandId: proof.commandId });
        const birth = yield* readApplicationBirthRecordEffect(proof.threadId);
        const reservations = yield* sql<{ readonly application_birth_json: string; readonly execution_intent_json: string;
          readonly basis_json: string; readonly basis_digest: string }>`SELECT application_birth_json, execution_intent_json, basis_json, basis_digest
          FROM orchestration_v2_queued_start_reservations WHERE effect_id = ${proof.effectId} AND command_id = ${proof.commandId}
            AND thread_id = ${proof.threadId} AND run_id = ${proof.runId} AND run_attempt_id = ${proof.attemptId}`;
        if (choice === null || choice.receipt.status !== "accepted" || choice.runId !== proof.runId || birth === null || reservations.length !== 1 ||
            (choice.basis.target as { readonly descriptor?: { readonly driver?: unknown } } | undefined)?.descriptor?.driver !== input.expectedBinding.driver)
          return null;
        const reservation = reservations[0]!;
        const lineage = yield* Effect.gen(function* () {
          const intent = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ExecutionIntentSchemaV2))(reservation.execution_intent_json);
          const basis = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ContinuationBasisSchemaV2))(reservation.basis_json);
          let originalProviderId = proof.binding.providerThreadId;
          if (choice.command.delivery.type === "queued_run") {
            const original = yield* Schema.decodeUnknownEffect(Schema.Struct({ records: Schema.Struct({
              runs: Schema.Array(Schema.Struct({ run_id: Schema.String, payload_json: Schema.String })) }) }))(choice.basis.snapshot);
            const originalRows = original.records.runs.filter((row) => row.run_id === proof.runId);
            if (originalRows.length !== 1) return null;
            const originalRun = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestrationV2RunJson))(originalRows[0]!.payload_json);
            if (originalRun.providerThreadId === null || originalRun.activeAttemptId !== proof.attemptId ||
                originalRun.providerThreadId === proof.binding.providerThreadId) return null;
            originalProviderId = originalRun.providerThreadId;
          }
          if (intent.kind !== "imported_history_choice" || intent.commandId !== proof.commandId || intent.runId !== proof.runId ||
              intent.runAttemptId !== proof.attemptId || intent.effectId !== proof.effectId || intent.reviewedBasis !== choice.command.reviewedBasis ||
              reservation.application_birth_json !== nativeCreationCanonicalJson(birth) || basis.sourceMode !== "new_context" ||
              basis.runId !== proof.runId || basis.runAttemptId !== proof.attemptId || basis.messageId !== choice.messageId ||
              basis.queuedProviderThreadId !== originalProviderId || basis.basisDigest !== reservation.basis_digest ||
              queuedRunContinuationBasisDigestV2(basis) !== reservation.basis_digest ||
              nativeCreationCanonicalJson(basis.executionIntent) !== nativeCreationCanonicalJson(intent)) return null;
          // This retained ACK proves lineage for the same incarnation, not fresh activity or authority.
          return proof;
        }).pipe(Effect.option);
        return Option.getOrNull(lineage);
      });
    const readConfirmedImportedHistoryBinding: EventSinkV2Shape["readConfirmedImportedHistoryBinding"] = (input) =>
      sql.withTransaction(readConfirmedImportedHistoryBindingEffect(input)).pipe(
        Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause })));
    const readConfirmedImportedHistoryContinuation: EventSinkV2Shape["readConfirmedImportedHistoryContinuation"] = (input) =>
      sql.withTransaction(Effect.gen(function* () {
        const confirmation = yield* readConfirmedImportedHistoryBindingEffect(input, true);
        if (confirmation === null || confirmation.binding.nativeThreadId === undefined) return null;
        const historicalSource = yield* readProviderContinuationSourceIdentity({ ...confirmation.binding,
          nativeThreadId: confirmation.binding.nativeThreadId, driver: input.expectedBinding.driver });
        const currentSource = yield* readProviderContinuationSourceIdentity(input.expectedBinding);
        if (historicalSource === null || currentSource === null || historicalSource.driverKind !== input.expectedBinding.driver ||
            currentSource.driverKind !== historicalSource.driverKind || currentSource.continuationKey !== historicalSource.continuationKey ||
            historicalSource.runtimeGeneration !== confirmation.binding.runtimeGeneration || currentSource.runtimeGeneration !== input.expectedBinding.runtimeGeneration)
          return null;
        // Each source retains its own actual initialized generation; neither source grants current authority.
        return { confirmation, historicalSource, currentSource } satisfies ConfirmedImportedHistoryContinuationV1;
      })).pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause })));
    const readCurrentThreadRuntimeStopIntentEffect = Effect.fnUntraced(function* (input: { readonly threadId: ThreadId; readonly commandId: CommandId }) {
      const rows = yield* sql<{ readonly command_id: string; readonly thread_id: string; readonly application_birth_json: string;
        readonly canonical_request_digest: string; readonly actor_binding_digest: string; readonly target_binding_json: string;
        readonly target_evidence_revision: number; readonly stop_event_id: string; readonly stop_event_sequence: number;
        readonly affected_run_ids_json: string; readonly queued_bases_json: string }>`
        SELECT * FROM orchestration_v2_current_runtime_stop_intents WHERE command_id = ${input.commandId} AND thread_id = ${input.threadId}`;
      if (rows.length === 0) return null;
      const row = rows[0]!;
      const intent = yield* Schema.decodeUnknownEffect(StopIntentSchemaV2)({ commandId: row.command_id, threadId: row.thread_id,
        incarnation: yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ApplicationBirthSchemaV2))(row.application_birth_json),
        canonicalRequestDigest: row.canonical_request_digest, actorBindingDigest: row.actor_binding_digest,
        targetBinding: yield* Schema.decodeUnknownEffect(Schema.fromJsonString(RegisteredSourceSchemaV2))(row.target_binding_json),
        targetEvidenceRevision: row.target_evidence_revision, stopEventId: row.stop_event_id, stopEventSequence: row.stop_event_sequence,
        affectedRunIds: yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Array(RunId)))(row.affected_run_ids_json),
        queuedBases: yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Array(ContinuationBasisSchemaV2)))(row.queued_bases_json) });
      const events = yield* sql`SELECT event_id FROM orchestration_events event JOIN orchestration_command_receipts receipt
        ON receipt.command_id = event.command_id WHERE receipt.command_id = ${intent.commandId} AND receipt.status = 'accepted'
          AND receipt.command_type = 'provider-session.detach' AND event.event_type = 'provider-session.detach-requested'
          AND json_extract(event.payload_json, '$.providerSessionId') = ${intent.targetBinding.providerSessionId}
          AND receipt.aggregate_id = ${intent.threadId} AND event.event_id = ${intent.stopEventId} AND event.sequence = ${intent.stopEventSequence}
          AND event.stream_id = ${intent.threadId} AND event.aggregate_kind = 'thread' AND event.application_event_version = 2`;
      if (intent.incarnation.threadId !== input.threadId || intent.targetBinding.threadId !== input.threadId || events.length !== 1 ||
          new Set(intent.affectedRunIds).size !== intent.affectedRunIds.length || new Set(intent.queuedBases.map((basis) => basis.runId)).size !== intent.queuedBases.length ||
          intent.queuedBases.some((basis) => queuedRunContinuationBasisDigestV2(basis) !== basis.basisDigest))
        return yield* new NativeCommandPreconditionError({ commandId: input.commandId, reason: "unknown_evidence" });
      return intent satisfies CurrentThreadRuntimeStopIntentV2;
    });
    const readQueuedRunRuntimeStopFencesEffect = Effect.fnUntraced(function* (input: { readonly threadId: ThreadId; readonly runId: RunId; readonly incarnation: ApplicationThreadBirthV2 }) {
      const rows = yield* sql<{ readonly stop_command_id: string; readonly thread_id: string; readonly application_birth_json: string;
        readonly run_id: string; readonly queued_provider_thread_id: string; readonly source_binding_json: string;
        readonly source_evidence_revision: number; readonly switch_plan_json: string | null; readonly source_mode: string;
        readonly execution_intent_json: string; readonly basis_digest: string }>`
        SELECT * FROM orchestration_v2_queued_runtime_stop_fences WHERE thread_id = ${input.threadId} AND run_id = ${input.runId} ORDER BY stop_command_id`;
      const fences: QueuedRunRuntimeStopFenceV2[] = [];
      for (const row of rows) {
        const fence = yield* Schema.decodeUnknownEffect(StopFenceSchemaV2)({ stopCommandId: row.stop_command_id, threadId: row.thread_id,
          incarnation: yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ApplicationBirthSchemaV2))(row.application_birth_json),
          runId: row.run_id, queuedProviderThreadId: row.queued_provider_thread_id,
          sourceBinding: yield* Schema.decodeUnknownEffect(Schema.fromJsonString(RegisteredSourceSchemaV2))(row.source_binding_json),
          sourceEvidenceRevision: row.source_evidence_revision, switchPlan: row.switch_plan_json === null ? null :
            yield* Schema.decodeUnknownEffect(Schema.fromJsonString(SwitchPlanSchemaV2))(row.switch_plan_json), sourceMode: row.source_mode,
          executionIntent: yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ExecutionIntentSchemaV2))(row.execution_intent_json), basisDigest: row.basis_digest });
        if (nativeCreationCanonicalJson(fence.incarnation) !== nativeCreationCanonicalJson(input.incarnation)) continue;
        const intent = yield* readCurrentThreadRuntimeStopIntentEffect({ threadId: input.threadId, commandId: fence.stopCommandId });
        const basis = intent?.queuedBases.find((basis) => basis.runId === input.runId);
        if (intent === null || !intent.affectedRunIds.includes(input.runId) || basis === undefined || basis.basisDigest !== fence.basisDigest ||
            nativeCreationCanonicalJson(basis.executionIntent) !== nativeCreationCanonicalJson(fence.executionIntent) ||
            nativeCreationCanonicalJson(basis.sourceBinding) !== nativeCreationCanonicalJson(fence.sourceBinding) ||
            basis.sourceEvidenceRevision !== fence.sourceEvidenceRevision || basis.sourceMode !== fence.sourceMode ||
            basis.queuedProviderThreadId !== fence.queuedProviderThreadId ||
            nativeCreationCanonicalJson(basis.switchPlan) !== nativeCreationCanonicalJson(fence.switchPlan))
          return yield* new NativeCommandPreconditionError({ commandId: fence.stopCommandId, reason: "unknown_evidence" });
        fences.push(fence);
      }
      return fences;
    });
    const readInFlightQueuedRunStartBases: EventSinkV2Shape["readInFlightQueuedRunStartBases"] = (input) =>
      sql.withTransaction(Effect.gen(function* () {
        if (input.incarnation.threadId !== input.threadId || nativeCreationCanonicalJson(yield* readApplicationBirthRecordEffect(input.threadId)) !==
            nativeCreationCanonicalJson(input.incarnation)) return yield* importSealFailure("Starting cohort has no exact live application birth");
        const rows = yield* sql<{ readonly effect_id: string; readonly command_id: string; readonly run_id: string; readonly run_attempt_id: string;
          readonly application_birth_json: string; readonly basis_json: string; readonly basis_digest: string; readonly execution_intent_json: string; readonly payload_json: string }>`
          SELECT reservation.*, effect.payload_json FROM orchestration_v2_queued_start_reservations reservation
          JOIN orchestration_v2_projection_runs run ON run.thread_id = reservation.thread_id AND run.run_id = reservation.run_id AND run.status = 'starting'
          JOIN orchestration_v2_projection_run_attempts attempt ON attempt.thread_id = reservation.thread_id AND attempt.run_id = run.run_id
            AND attempt.attempt_id = reservation.run_attempt_id AND attempt.status IN ('pending', 'running')
          JOIN orchestration_v2_effect_outbox effect ON effect.effect_id = reservation.effect_id AND effect.command_id = reservation.command_id
            AND effect.thread_id = reservation.thread_id AND effect.effect_type = 'provider-turn.start' AND effect.status IN ('pending', 'running')
          WHERE reservation.thread_id = ${input.threadId} AND json_extract(run.payload_json, '$.activeAttemptId') = attempt.attempt_id
          ORDER BY reservation.effect_id`;
        const result: InFlightQueuedRunStartBasisV2[] = [];
        for (const row of rows) {
          const birth = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ApplicationBirthSchemaV2))(row.application_birth_json);
          const basis = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ContinuationBasisSchemaV2))(row.basis_json);
          const intent = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ExecutionIntentSchemaV2))(row.execution_intent_json);
          const payload = yield* EffectOutbox.decodeOrchestrationEffectPayloadV2(row.payload_json);
          if (nativeCreationCanonicalJson(birth) !== nativeCreationCanonicalJson(input.incarnation) || intent.effectId !== row.effect_id ||
              intent.commandId !== row.command_id || intent.runId !== row.run_id || intent.runAttemptId !== row.run_attempt_id ||
              intent.effectId !== `effect:${intent.commandId}:provider-turn.start:${intent.runId}` || payload.request.type !== "provider-turn.start" ||
              payload.request.runId !== intent.runId || basis.runId !== intent.runId || basis.runAttemptId !== intent.runAttemptId ||
              basis.basisDigest !== row.basis_digest || queuedRunContinuationBasisDigestV2(basis) !== row.basis_digest ||
              nativeCreationCanonicalJson(basis.executionIntent) !== nativeCreationCanonicalJson(intent))
            return yield* importSealFailure("Starting cohort has a mismatched immutable original reservation");
          result.push({ incarnation: birth, basis, executionIntent: intent });
        }
        return result;
      })).pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause })));
    const isFirstOrdinaryQueuedContext = Effect.fnUntraced(function* (
      snapshot: NativeCommandTargetSnapshotV2, basis: QueuedRunContinuationBasisV2, intent: QueuedRunExecutionIntentV2,
      prepared = false,
    ) {
      if (basis.sourceMode !== "new_context" || intent.kind !== "queued" || basis.queuedProviderThreadId === null ||
          basis.runAttemptId === null || snapshot.creationProvenance !== "native_created" || snapshot.incarnation === null ||
          intent.commandId !== `command:system:start-queued:${intent.runId}`) return false;
      // Absence of a resident owner cannot turn detached or imported history into a first context.
      for (const key of ["provider_sessions", "session_bindings", "runtime_evidence", "continuation_sources", "source_runtime",
        "legacy_continuation", "native_import_seals", "legacy_import_markers", "legacy_source_threads", "imported_source_events",
        "imported_choices", "imported_outcomes", "stop_intents", "stop_fences", "unknown_effect_holds", "native_confirmations"])
        if ((snapshot.records[key]?.length ?? 0) !== 0) return false;
      if ((snapshot.records.start_reservations ?? []).some((reservation) => !prepared || reservation.effect_id !== intent.effectId)) return false;
      if ((snapshot.records.effects ?? []).some((effect) => effect.effect_id !== intent.effectId &&
          ["provider-turn.start", "provider-turn.restart", "provider-turn.interrupt", "provider-turn.steer", "provider-subagent.start", "provider-runtime.continue"].includes(String(effect.effect_type)))) return false;
      if ((snapshot.authorityRecords.claims?.length ?? 0) !== 0) return false;
      const local = yield* projectionStore.getThreadRecords(snapshot.threadId, ["runs", "attempts", "nodes", "providerThreads", "messages"]);
      const run = local.runs.find((candidate) => candidate.id === intent.runId);
      const attempt = local.attempts.find((candidate) => candidate.id === intent.runAttemptId);
      const provider = local.providerThreads.find((candidate) => candidate.id === basis.queuedProviderThreadId);
      const message = local.messages.find((candidate) => candidate.id === basis.messageId);
      const node = local.nodes.find((candidate) => candidate.id === run?.rootNodeId);
      if (run === undefined || attempt === undefined || provider === undefined || message === undefined || node === undefined ||
          run.status !== (prepared ? "starting" : "queued") || run.startedAt !== null ||
          (!prepared && (run.queueHeld === true || queuedRunsInDeliveryOrder(local)[0]?.id !== run.id)) || run.rootNodeId === null ||
          run.activeAttemptId !== attempt.id || run.providerThreadId !== provider.id || run.userMessageId !== message.id ||
          attempt.status !== "pending" || attempt.runId !== run.id || attempt.rootNodeId !== run.rootNodeId || attempt.providerThreadId !== provider.id ||
          node.runId !== run.id || node.rootNodeId !== run.rootNodeId || node.providerThreadId !== provider.id || node.providerTurnId !== null ||
          message.role !== "user" || message.runId !== run.id || message.nodeId !== node.id || provider.appThreadId !== snapshot.threadId ||
          provider.nativeThreadRef !== null || provider.nativeConversationHeadRef !== null ||
          run.providerInstanceId !== provider.providerInstanceId || run.modelSelection.instanceId !== provider.providerInstanceId ||
          attempt.providerInstanceId !== provider.providerInstanceId || local.thread.modelSelection.instanceId !== provider.providerInstanceId ||
          (local.thread.activeProviderThreadId !== null && local.thread.activeProviderThreadId !== provider.id) ||
          local.providerThreads.some((candidate) => candidate.nativeThreadRef !== null || candidate.nativeConversationHeadRef !== null)) return false;
      if (provider.providerSessionId !== null) {
        const used = yield* sql`SELECT provider_session_id FROM orchestration_v2_projection_provider_sessions WHERE provider_session_id = ${provider.providerSessionId}
          UNION ALL SELECT provider_session_id FROM orchestration_v2_projection_provider_session_bindings WHERE provider_session_id = ${provider.providerSessionId}
          UNION ALL SELECT thread_id FROM orchestration_v2_provider_runtime_evidence WHERE provider_session_id = ${provider.providerSessionId}
          UNION ALL SELECT source_id FROM orchestration_v2_provider_continuation_sources WHERE provider_session_id = ${provider.providerSessionId}
          UNION ALL SELECT event_id FROM orchestration_events WHERE application_event_version = 2 AND
            event_type IN ('provider-session.attached', 'provider-session.updated') AND json_extract(payload_json, '$.id') = ${provider.providerSessionId}`;
        if (used.length !== 0) return false;
        const pooled = yield* sql<{ readonly provider_thread_id: string; readonly thread_id: string; readonly payload_json: string }>`
          SELECT provider_thread_id, thread_id, payload_json FROM orchestration_v2_projection_provider_threads
          WHERE provider_session_id = ${provider.providerSessionId} AND provider_thread_id != ${provider.id}`;
        for (const association of pooled) {
          const associatedThreadId = ThreadId.make(association.thread_id);
          const birth = yield* readIncarnation(associatedThreadId);
          if (birth.incarnation === null || birth.creationProvenance !== "native_created" || (yield* readApplicationBirthRecordEffect(associatedThreadId)) === null) return false;
          const associated = yield* projectionStore.getThreadRecords(associatedThreadId, ["runs", "attempts", "providerThreads", "nodes", "messages"]);
          const unused = associated.providerThreads.find((candidate) => candidate.id === association.provider_thread_id);
          if (unused === undefined || unused.providerSessionId !== provider.providerSessionId || unused.nativeThreadRef !== null || unused.nativeConversationHeadRef !== null) return false;
          const history = yield* readCommitSnapshot(associatedThreadId, intent.commandId, {});
          for (const key of ["provider_sessions", "session_bindings", "runtime_evidence", "continuation_sources", "source_runtime", "legacy_continuation",
            "native_import_seals", "legacy_import_markers", "legacy_source_threads", "imported_source_events", "imported_choices", "imported_outcomes",
            "stop_intents", "stop_fences", "start_reservations", "unknown_effect_holds", "native_confirmations", "provider_turns"])
            if ((history.records[key]?.length ?? 0) !== 0) return false;
          if ((history.authorityRecords.claims?.length ?? 0) !== 0 || (history.records.effects ?? []).some((effect) =>
              ["provider-turn.start", "provider-turn.restart", "provider-turn.interrupt", "provider-turn.steer", "provider-subagent.start", "provider-runtime.continue"].includes(String(effect.effect_type)))) return false;
          const starts = yield* sql`SELECT event_id FROM orchestration_events WHERE application_event_version = 2 AND aggregate_kind = 'thread'
            AND stream_id = ${associatedThreadId} AND (event_type IN ('provider-session.attached', 'provider-session.updated', 'provider-session.detached',
              'provider-session.detach-requested', 'provider-turn.updated', 'checkpoint-scope.created') OR
              (event_type = 'run.updated' AND json_extract(payload_json, '$.status') IN ('starting', 'running'))) LIMIT 1`;
          if (starts.length !== 0) return false;
          const origin = yield* sql`SELECT introduced.event_id FROM orchestration_events introduced
            JOIN orchestration_command_receipts receipt ON receipt.command_id = introduced.command_id AND receipt.status = 'accepted'
              AND receipt.command_type = 'message.dispatch' AND receipt.aggregate_kind = 'thread' AND receipt.aggregate_id = introduced.stream_id
            WHERE introduced.application_event_version = 2 AND introduced.aggregate_kind = 'thread' AND introduced.stream_id = ${associatedThreadId}
              AND introduced.event_type = 'provider-thread.updated' AND json_extract(introduced.payload_json, '$.id') = ${unused.id}
              AND json_extract(introduced.payload_json, '$.providerSessionId') = ${provider.providerSessionId}
              AND json_extract(introduced.payload_json, '$.nativeThreadRef') IS NULL AND json_extract(introduced.payload_json, '$.nativeConversationHeadRef') IS NULL
              AND EXISTS (SELECT 1 FROM orchestration_events created WHERE created.command_id = introduced.command_id AND created.application_event_version = 2
                AND created.aggregate_kind = 'thread' AND created.stream_id = introduced.stream_id AND created.event_type = 'run.created'
                AND json_extract(created.payload_json, '$.status') = 'preparing' AND json_extract(created.payload_json, '$.providerThreadId') = ${unused.id})`;
          if (origin.length !== 1) return false;
          const associatedRuns = associated.runs.filter((candidate) => candidate.providerThreadId === unused.id);
          if (associatedRuns.length === 0) return false;
          for (const candidate of associatedRuns) {
            const activeAttempt = associated.attempts.find((attempt) => attempt.id === candidate.activeAttemptId);
            const root = associated.nodes.find((node) => node.id === candidate.rootNodeId);
            const user = associated.messages.find((message) => message.id === candidate.userMessageId);
            if (candidate.startedAt !== null || !["preparing", "queued", "completed", "cancelled", "failed", "interrupted"].includes(candidate.status) ||
                activeAttempt === undefined || root === undefined || user === undefined || activeAttempt.runId !== candidate.id ||
                activeAttempt.rootNodeId !== root.id || activeAttempt.providerThreadId !== unused.id || activeAttempt.providerTurnId !== null ||
                activeAttempt.startedAt !== null || root.runId !== candidate.id || root.providerThreadId !== unused.id || root.checkpointScopeId !== null ||
                root.providerTurnId !== null || user.runId !== candidate.id || user.nodeId !== root.id || user.role !== "user") return false;
            const lineage = yield* sql`SELECT created.event_id FROM orchestration_events created JOIN orchestration_command_receipts receipt
              ON receipt.command_id = created.command_id AND receipt.status = 'accepted' AND receipt.command_type = 'message.dispatch'
                AND receipt.aggregate_kind = 'thread' AND receipt.aggregate_id = created.stream_id
              WHERE created.application_event_version = 2 AND created.aggregate_kind = 'thread' AND created.stream_id = ${associatedThreadId}
                AND created.event_type = 'run.created' AND json_extract(created.payload_json, '$.id') = ${candidate.id}
                AND json_extract(created.payload_json, '$.status') IN ('preparing', 'queued')
                AND json_extract(created.payload_json, '$.providerThreadId') = ${unused.id} AND json_extract(created.payload_json, '$.startedAt') IS NULL
                AND json_extract(created.payload_json, '$.activeAttemptId') = ${activeAttempt.id} AND json_extract(created.payload_json, '$.rootNodeId') = ${root.id}
                AND json_extract(created.payload_json, '$.userMessageId') = ${user.id}
                AND EXISTS (SELECT 1 FROM orchestration_events pending WHERE pending.command_id = created.command_id AND pending.application_event_version = 2
                  AND pending.aggregate_kind = 'thread' AND pending.stream_id = created.stream_id AND pending.event_type = 'run-attempt.created'
                  AND json_extract(pending.payload_json, '$.id') = ${activeAttempt.id} AND json_extract(pending.payload_json, '$.status') = 'pending'
                  AND json_extract(pending.payload_json, '$.providerThreadId') = ${unused.id} AND json_extract(pending.payload_json, '$.rootNodeId') = ${root.id})
                AND EXISTS (SELECT 1 FROM orchestration_events node WHERE node.command_id = created.command_id AND node.application_event_version = 2
                  AND node.aggregate_kind = 'thread' AND node.stream_id = created.stream_id AND node.event_type = 'node.updated'
                  AND json_extract(node.payload_json, '$.id') = ${root.id} AND json_extract(node.payload_json, '$.checkpointScopeId') IS NULL)
                AND EXISTS (SELECT 1 FROM orchestration_events message WHERE message.command_id = created.command_id AND message.application_event_version = 2
                  AND message.aggregate_kind = 'thread' AND message.stream_id = created.stream_id AND message.event_type = 'message.updated'
                  AND json_extract(message.payload_json, '$.id') = ${user.id})
                AND NOT EXISTS (SELECT 1 FROM orchestration_v2_native_command_identities identity WHERE identity.command_id = receipt.command_id)`;
            if (lineage.length !== 1) return false;
            if (!["preparing", "queued"].includes(candidate.status)) {
              const ended = yield* sql`SELECT event.event_id FROM orchestration_events event JOIN orchestration_command_receipts receipt
                ON receipt.command_id = event.command_id AND receipt.status = 'accepted' AND receipt.aggregate_kind = 'thread' AND receipt.aggregate_id = event.stream_id
                WHERE event.application_event_version = 2 AND event.aggregate_kind = 'thread' AND event.stream_id = ${associatedThreadId}
                  AND event.event_type = 'run.updated' AND json_extract(event.payload_json, '$.id') = ${candidate.id}
                  AND json_extract(event.payload_json, '$.status') = ${candidate.status} AND json_extract(event.payload_json, '$.startedAt') IS NULL
                  AND (${candidate.status === "interrupted" ? sql`receipt.command_type = 'run.interrupt' AND EXISTS (
                    SELECT 1 FROM orchestration_events requested JOIN orchestration_events result ON result.command_id = requested.command_id
                    WHERE requested.command_id = event.command_id AND requested.stream_id = event.stream_id AND result.stream_id = event.stream_id
                      AND requested.application_event_version = 2 AND result.application_event_version = 2
                      AND requested.event_type = 'turn-item.updated' AND result.event_type = 'turn-item.updated'
                      AND json_extract(requested.payload_json, '$.type') = 'run_interrupt_request' AND json_extract(result.payload_json, '$.type') = 'run_interrupt_result'
                      AND json_extract(requested.payload_json, '$.runId') = ${candidate.id} AND json_extract(result.payload_json, '$.runId') = ${candidate.id}
                      AND json_extract(result.payload_json, '$.parentItemId') = json_extract(requested.payload_json, '$.id')
                      AND json_extract(requested.payload_json, '$.providerTurnId') IS NULL AND json_extract(result.payload_json, '$.providerTurnId') IS NULL)` : sql`1 = 1`})
                  AND EXISTS (SELECT 1 FROM orchestration_events ended WHERE ended.command_id = event.command_id AND ended.application_event_version = 2
                    AND ended.aggregate_kind = 'thread' AND ended.stream_id = event.stream_id AND ended.event_type = 'run-attempt.updated'
                    AND json_extract(ended.payload_json, '$.id') = ${activeAttempt.id} AND json_extract(ended.payload_json, '$.status') = ${activeAttempt.status}
                    AND json_extract(ended.payload_json, '$.startedAt') IS NULL)`;
              if (ended.length !== 1) return false;
            }
          }
        }
      }
      const historicalBinding = yield* sql`SELECT event_id FROM orchestration_events WHERE application_event_version = 2
        AND ((aggregate_kind = 'thread' AND stream_id = ${snapshot.threadId} AND event_type IN ('provider-session.attached', 'provider-session.updated', 'provider-session.detached', 'provider-session.detach-requested'))
          OR (event_type = 'provider-thread.updated' AND json_extract(payload_json, '$.id') = ${provider.id}
            AND (json_extract(payload_json, '$.nativeThreadRef') IS NOT NULL
              OR json_extract(payload_json, '$.nativeConversationHeadRef') IS NOT NULL))) LIMIT 1`;
      if (historicalBinding.length !== 0) return false;
      const oldStarts = yield* sql`SELECT event_id FROM orchestration_events WHERE application_event_version = 2 AND aggregate_kind = 'thread'
        AND stream_id = ${snapshot.threadId} AND ((event_type = 'provider-turn.updated') OR
          (event_type = 'run.updated' AND json_extract(payload_json, '$.status') IN ('starting', 'running')
            AND (command_id IS NOT ${intent.commandId} OR json_extract(payload_json, '$.id') IS NOT ${run.id}))) LIMIT 1`;
      if (oldStarts.length !== 0) return false;
      const predecessorCommands = new Set<CommandId>();
      for (const predecessor of local.runs.filter((candidate) => candidate.id !== run.id && candidate.status !== "queued")) {
        if (!["completed", "cancelled", "failed", "interrupted"].includes(predecessor.status) || predecessor.ordinal >= run.ordinal || predecessor.startedAt !== null ||
            predecessor.rootNodeId === null || predecessor.activeAttemptId === null || predecessor.providerThreadId !== provider.id) return false;
        const source = yield* sql<{ readonly command_id: string | null }>`SELECT command_id FROM orchestration_events
          WHERE application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ${snapshot.threadId}
            AND event_type = 'run.created' AND json_extract(payload_json, '$.id') = ${predecessor.id}
            AND json_extract(payload_json, '$.status') = 'preparing' AND json_extract(payload_json, '$.startedAt') IS NULL`;
        if (source.length !== 1 || source[0]!.command_id === null) return false;
        const sourceCommandId = CommandId.make(source[0]!.command_id);
        const sourceReceipt = Option.getOrNull(yield* commandReceipts.getByCommandId(sourceCommandId));
        if (sourceReceipt?.status !== "accepted" || sourceReceipt.commandType !== "message.dispatch" || sourceReceipt.threadId !== snapshot.threadId ||
            (yield* readIdentity(sourceCommandId)) !== null) return false;
        const original = yield* eventStore.read({ threadId: snapshot.threadId, commandId: sourceCommandId, limit: 257 }).pipe(Stream.runCollect);
        const originalRun = original.filter((item) => item.event.type === "run.created" && item.event.payload.id === predecessor.id);
        const originalAttempt = original.filter((item) => item.event.type === "run-attempt.created" && item.event.payload.id === predecessor.activeAttemptId);
        const originalNode = original.filter((item) => item.event.type === "node.updated" && item.event.payload.id === predecessor.rootNodeId);
        const originalMessage = original.filter((item) => item.event.type === "message.updated" && item.event.payload.id === predecessor.userMessageId);
        const predecessorAttempt = local.attempts.find((candidate) => candidate.id === predecessor.activeAttemptId);
        if (original.length > 256 || original.some((item) => item.sequence > sourceReceipt.resultSequence) || originalRun.length !== 1 ||
            originalRun[0]!.event.type !== "run.created" || originalRun[0]!.event.payload.providerThreadId !== provider.id ||
            originalRun[0]!.event.payload.rootNodeId !== predecessor.rootNodeId || originalRun[0]!.event.payload.activeAttemptId !== predecessor.activeAttemptId ||
            originalRun[0]!.event.payload.userMessageId !== predecessor.userMessageId || originalAttempt.length !== 1 ||
            originalAttempt[0]!.event.type !== "run-attempt.created" || originalAttempt[0]!.event.payload.status !== "pending" ||
            originalAttempt[0]!.event.payload.providerThreadId !== provider.id || originalAttempt[0]!.event.payload.rootNodeId !== predecessor.rootNodeId ||
            originalNode.length !== 1 || originalNode[0]!.event.type !== "node.updated" || originalNode[0]!.event.payload.checkpointScopeId !== null ||
            originalMessage.length !== 1 || predecessorAttempt?.runId !== predecessor.id || predecessorAttempt.providerThreadId !== provider.id ||
            predecessorAttempt.startedAt !== null || !["completed", "cancelled", "failed", "interrupted"].includes(predecessorAttempt.status)) return false;
        const terminal = yield* sql`SELECT terminal.event_id FROM orchestration_events terminal JOIN orchestration_command_receipts receipt
          ON receipt.command_id = terminal.command_id AND receipt.status = 'accepted' AND receipt.aggregate_kind = 'thread' AND receipt.aggregate_id = terminal.stream_id
          WHERE terminal.application_event_version = 2 AND terminal.aggregate_kind = 'thread' AND terminal.stream_id = ${snapshot.threadId}
            AND terminal.event_type = 'run.updated' AND json_extract(terminal.payload_json, '$.id') = ${predecessor.id}
            AND (${predecessor.status === "interrupted" ? sql`receipt.command_type = 'run.interrupt' AND EXISTS (
              SELECT 1 FROM orchestration_events requested JOIN orchestration_events result ON result.command_id = requested.command_id
              WHERE requested.command_id = terminal.command_id AND requested.stream_id = terminal.stream_id AND result.stream_id = terminal.stream_id
                AND requested.application_event_version = 2 AND result.application_event_version = 2
                AND requested.event_type = 'turn-item.updated' AND result.event_type = 'turn-item.updated'
                AND json_extract(requested.payload_json, '$.type') = 'run_interrupt_request' AND json_extract(result.payload_json, '$.type') = 'run_interrupt_result'
                AND json_extract(requested.payload_json, '$.runId') = ${predecessor.id} AND json_extract(result.payload_json, '$.runId') = ${predecessor.id}
                AND json_extract(result.payload_json, '$.parentItemId') = json_extract(requested.payload_json, '$.id')
                AND json_extract(requested.payload_json, '$.providerTurnId') IS NULL AND json_extract(result.payload_json, '$.providerTurnId') IS NULL)` : sql`1 = 1`})
            AND json_extract(terminal.payload_json, '$.status') = ${predecessor.status} AND json_extract(terminal.payload_json, '$.startedAt') IS NULL
            AND json_extract(terminal.payload_json, '$.activeAttemptId') = ${predecessor.activeAttemptId}
            AND EXISTS (SELECT 1 FROM orchestration_events ended WHERE ended.command_id = terminal.command_id AND ended.application_event_version = 2
              AND ended.aggregate_kind = 'thread' AND ended.stream_id = terminal.stream_id AND ended.event_type = 'run-attempt.updated'
              AND json_extract(ended.payload_json, '$.id') = ${predecessor.activeAttemptId}
              AND json_extract(ended.payload_json, '$.status') = ${predecessorAttempt.status} AND json_extract(ended.payload_json, '$.startedAt') IS NULL)
            AND NOT EXISTS (SELECT 1 FROM orchestration_events scope WHERE scope.application_event_version = 2 AND scope.aggregate_kind = 'thread'
              AND scope.stream_id = terminal.stream_id AND scope.event_type = 'checkpoint-scope.created' AND json_extract(scope.payload_json, '$.runId') = ${predecessor.id})`;
        if (terminal.length !== 1) return false;
        predecessorCommands.add(sourceCommandId);
      }
      const creations = yield* sql<{ readonly command_id: string | null }>`SELECT command_id FROM orchestration_events
        WHERE application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ${snapshot.threadId}
          AND event_type = 'run.created' AND json_extract(payload_json, '$.id') = ${run.id}`;
      if (creations.length !== 1 || creations[0]!.command_id === null) return false;
      const originalCommandId = CommandId.make(creations[0]!.command_id);
      const receipt = Option.getOrNull(yield* commandReceipts.getByCommandId(originalCommandId));
      if (receipt === null || receipt.status !== "accepted" || receipt.commandType !== "message.dispatch" || receipt.threadId !== snapshot.threadId ||
          (yield* readIdentity(originalCommandId)) !== null) return false;
      const providerOrigins = yield* sql<{ readonly command_id: string | null }>`SELECT command_id FROM orchestration_events WHERE application_event_version = 2
        AND event_type = 'provider-thread.updated' AND json_extract(payload_json, '$.id') = ${provider.id}
        AND aggregate_kind = 'thread' AND stream_id = ${snapshot.threadId} ORDER BY sequence LIMIT 1`;
      if (providerOrigins.length !== 1 || providerOrigins[0]!.command_id === null) return false;
      const providerOriginCommand = CommandId.make(providerOrigins[0]!.command_id);
      if (providerOriginCommand !== originalCommandId && !predecessorCommands.has(providerOriginCommand)) return false;
      const providerOrigin = yield* eventStore.read({ threadId: snapshot.threadId, commandId: providerOriginCommand, limit: 257 }).pipe(Stream.runCollect);
      const introducedProvider = providerOrigin.filter((item) => item.event.type === "provider-thread.updated" && item.event.payload.id === provider.id);
      if (introducedProvider.length !== 1 || introducedProvider[0]!.event.type !== "provider-thread.updated" ||
          introducedProvider[0]!.event.payload.nativeThreadRef !== null || introducedProvider[0]!.event.payload.nativeConversationHeadRef !== null ||
          introducedProvider[0]!.event.payload.appThreadId !== snapshot.threadId || introducedProvider[0]!.event.payload.providerInstanceId !== provider.providerInstanceId ||
          introducedProvider[0]!.event.payload.driver !== provider.driver) return false;
      const stored = yield* eventStore.read({ threadId: snapshot.threadId, commandId: originalCommandId, limit: 257 }).pipe(Stream.runCollect);
      if (stored.length === 0 || stored.length > 256 || stored.some((item) => item.commandId !== originalCommandId ||
          item.event.threadId !== snapshot.threadId || item.sequence > receipt.resultSequence)) return false;
      const events = stored.map((item) => item.event);
      const createdRuns = events.filter((event) => event.type === "run.created" && event.payload.id === run.id);
      const createdAttempts = events.filter((event) => event.type === "run-attempt.created" && event.payload.id === attempt.id);
      const createdProviders = events.filter((event) => event.type === "provider-thread.updated" && event.payload.id === provider.id);
      const createdNodes = events.filter((event) => event.type === "node.updated" && event.payload.id === node.id);
      const createdMessages = events.filter((event) => event.type === "message.updated" && event.payload.id === message.id);
      const createdRun = createdRuns[0]; const createdAttempt = createdAttempts[0]; const createdProvider = createdProviders[0];
      const createdNode = createdNodes[0]; const createdMessage = createdMessages[0];
      return createdRuns.length === 1 && createdRun?.type === "run.created" && createdRun.payload.status === "queued" &&
        createdRun.payload.activeAttemptId === attempt.id && createdRun.payload.rootNodeId === node.id &&
        createdRun.payload.providerThreadId === provider.id && createdRun.payload.userMessageId === message.id &&
        nativeCreationCanonicalJson(createdRun.payload.modelSelection) === nativeCreationCanonicalJson(run.modelSelection) &&
        createdAttempts.length === 1 && createdAttempt?.type === "run-attempt.created" && createdAttempt.payload.status === "pending" &&
        createdAttempt.payload.runId === run.id && createdAttempt.payload.rootNodeId === node.id && createdAttempt.payload.providerThreadId === provider.id &&
        (providerOriginCommand !== originalCommandId ? createdProviders.length === 0 : createdProviders.length === 1 && createdProvider?.type === "provider-thread.updated" &&
          createdProvider.payload.nativeThreadRef === null && createdProvider.payload.nativeConversationHeadRef === null) &&
        createdNodes.length === 1 && createdNode?.type === "node.updated" && createdNode.payload.runId === run.id &&
        createdNode.payload.rootNodeId === node.id && createdNode.payload.providerThreadId === provider.id &&
        createdMessages.length === 1 && createdMessage?.type === "message.updated" &&
        nativeCreationCanonicalJson(createdMessage.payload) === nativeCreationCanonicalJson(message);
    });
    const readClaimedQueuedRunStart: EventSinkV2Shape["readClaimedQueuedRunStart"] = (input) =>
      sql.withTransaction(Effect.gen(function* () {
        if (!Number.isSafeInteger(input.expectedAttempt) || input.expectedAttempt < 1) return null;
        const now = DateTime.formatIso(yield* DateTime.now);
        const rows = yield* sql<{ readonly command_id: string; readonly application_birth_json: string; readonly execution_intent_json: string;
          readonly basis_json: string; readonly basis_digest: string; readonly payload_json: string }>`
          SELECT reservation.*, effect.payload_json FROM orchestration_v2_queued_start_reservations reservation
          JOIN orchestration_v2_effect_outbox effect ON effect.effect_id = reservation.effect_id AND effect.command_id = reservation.command_id
            AND effect.thread_id = reservation.thread_id AND effect.effect_type = 'provider-turn.start'
          WHERE reservation.effect_id = ${input.effectId} AND reservation.thread_id = ${input.threadId} AND reservation.run_id = ${input.runId}
            AND effect.status = 'running' AND effect.lease_owner = ${input.workerId} AND effect.attempt_count = ${input.expectedAttempt}
            AND effect.lease_expires_at > ${now}`;
        if (rows.length !== 1) return null;
        const row = rows[0]!;
        const incarnation = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ApplicationBirthSchemaV2))(row.application_birth_json);
        const basis = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ContinuationBasisSchemaV2))(row.basis_json);
        const executionIntent = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ExecutionIntentSchemaV2))(row.execution_intent_json);
        const payload = yield* EffectOutbox.decodeOrchestrationEffectPayloadV2(row.payload_json);
        if (executionIntent.kind !== "queued" || executionIntent.commandId !== row.command_id || executionIntent.runId !== input.runId ||
            executionIntent.effectId !== input.effectId || executionIntent.commandId !== `command:system:start-queued:${input.runId}` ||
            executionIntent.effectId !== `effect:${executionIntent.commandId}:provider-turn.start:${input.runId}` ||
            "nativeCreationExecutionReference" in payload || payload.request.type !== "provider-turn.start" || payload.request.runId !== input.runId ||
            incarnation.threadId !== input.threadId || basis.runId !== input.runId || basis.runAttemptId !== executionIntent.runAttemptId ||
            basis.basisDigest !== row.basis_digest || queuedRunContinuationBasisDigestV2(basis) !== row.basis_digest ||
            nativeCreationCanonicalJson(basis.executionIntent) !== nativeCreationCanonicalJson(executionIntent) || basis.sourceMode === "unknown" ||
            nativeCreationCanonicalJson(yield* readApplicationBirthRecordEffect(input.threadId)) !== nativeCreationCanonicalJson(incarnation)) return null;
        const current = yield* readCommitSnapshot(input.threadId, executionIntent.commandId, {});
        if (current.records.unknown_effect_holds!.length !== 0) return null;
        const local = yield* projectionStore.getThreadRecords(input.threadId, ["runs", "attempts", "nodes", "providerThreads", "providerSessions", "checkpointScopes"]);
        const run = local.runs.find((candidate) => candidate.id === input.runId);
        const attempt = local.attempts.find((candidate) => candidate.id === executionIntent.runAttemptId);
        const provider = local.providerThreads.find((candidate) => candidate.id === basis.queuedProviderThreadId);
        const node = local.nodes.find((candidate) => candidate.id === run?.rootNodeId);
        if (run === undefined || attempt === undefined || provider === undefined || node === undefined || run.status !== "starting" ||
            attempt.status !== "pending" || run.activeAttemptId !== attempt.id || attempt.runId !== run.id ||
            attempt.providerThreadId !== provider.id || run.providerThreadId !== provider.id || run.userMessageId !== basis.messageId ||
            run.rootNodeId !== attempt.rootNodeId || node.providerThreadId !== provider.id || node.runId !== run.id ||
            local.thread.activeProviderThreadId !== provider.id) return null;
        const fences = yield* readQueuedRunRuntimeStopFencesEffect({ threadId: input.threadId, runId: run.id, incarnation });
        if (fences.some((fence) => nativeCreationCanonicalJson(fence.executionIntent) === nativeCreationCanonicalJson(executionIntent))) return null;
        for (const row of current.records.stop_intents ?? []) {
          const stopped = yield* readCurrentThreadRuntimeStopIntentEffect({ threadId: input.threadId, commandId: CommandId.make(String(row.command_id)) });
          if (stopped?.queuedBases.some((reviewed) => reviewed.runId === run.id && reviewed.sourceMode === "unknown" &&
              (reviewed.executionIntent === null || nativeCreationCanonicalJson(reviewed.executionIntent) === nativeCreationCanonicalJson(executionIntent)))) return null;
        }
        if (basis.sourceMode === "new_context") {
          if (!(yield* isFirstOrdinaryQueuedContext(current, basis, executionIntent, true))) return null;
          const prepared = yield* eventStore.read({ threadId: input.threadId, commandId: executionIntent.commandId, limit: 257 }).pipe(Stream.runCollect);
          const providers = prepared.filter((item) => item.event.type === "provider-thread.updated" && item.event.payload.id === provider.id);
          const runs = prepared.filter((item) => item.event.type === "run.updated" && item.event.payload.id === run.id);
          const scopes = prepared.filter((item) => item.event.type === "checkpoint-scope.created" && item.event.payload.runId === run.id &&
            item.event.payload.nodeId === node.id && item.event.payload.providerThreadId === provider.id && item.event.payload.id === node.checkpointScopeId);
          if (prepared.length > 256 || providers.length !== 1 || providers[0]!.event.type !== "provider-thread.updated" ||
              nativeCreationCanonicalJson(providers[0]!.event.payload) !== nativeCreationCanonicalJson(provider) ||
              runs.length !== 1 || runs[0]!.event.type !== "run.updated" || nativeCreationCanonicalJson(runs[0]!.event.payload) !== nativeCreationCanonicalJson(run) ||
              scopes.length !== 1 || provider.providerSessionId === null) return null;
        } else if (basis.sourceMode === "active_native_copy") {
          const sourceBinding = basis.sourceBinding;
          const plan = basis.switchPlan;
          const registered = yield* readProviderRuntimeEvidenceEffect(input.threadId);
          if (sourceBinding === null || plan === null || !plan.instanceChanged || plan.transition.type !== "restart_and_resume" ||
              sourceBinding.providerThreadId === provider.id || registered === null || registered.evidenceRevision !== basis.sourceEvidenceRevision ||
              nativeCreationCanonicalJson(registered.binding) !== nativeCreationCanonicalJson(sourceBinding)) return null;
          const source = local.providerThreads.find((candidate) => candidate.id === sourceBinding.providerThreadId);
          const session = local.providerSessions.find((candidate) => candidate.id === sourceBinding.providerSessionId);
          const attached = yield* sql`SELECT thread_id FROM orchestration_v2_projection_provider_session_bindings
            WHERE thread_id = ${input.threadId} AND provider_session_id = ${sourceBinding.providerSessionId}`;
          if (source === undefined || session === undefined || attached.length !== 1 || source.appThreadId !== input.threadId ||
              source.providerSessionId !== sourceBinding.providerSessionId || source.providerInstanceId !== sourceBinding.instanceId ||
              source.driver !== sourceBinding.driver || source.nativeThreadRef?.nativeId !== sourceBinding.nativeThreadId ||
              source.nativeThreadRef === null || session.providerInstanceId !== sourceBinding.instanceId || session.driver !== sourceBinding.driver ||
              provider.providerSessionId === null || provider.providerInstanceId !== run.providerInstanceId || attempt.providerInstanceId !== run.providerInstanceId ||
              provider.providerInstanceId === source.providerInstanceId || provider.driver !== source.driver ||
              nativeCreationCanonicalJson(provider.nativeThreadRef) !== nativeCreationCanonicalJson(source.nativeThreadRef) ||
              nativeCreationCanonicalJson(provider.nativeConversationHeadRef) !== nativeCreationCanonicalJson(source.nativeConversationHeadRef) ||
              nativeCreationCanonicalJson(provider.nativeMetadata ?? null) !== nativeCreationCanonicalJson(source.nativeMetadata ?? null)) return null;
          const prepared = yield* eventStore.read({ threadId: input.threadId, commandId: executionIntent.commandId, limit: 257 }).pipe(Stream.runCollect);
          const providers = prepared.filter((item) => item.event.type === "provider-thread.updated" && item.event.payload.id === provider.id);
          const runs = prepared.filter((item) => item.event.type === "run.updated" && item.event.payload.id === run.id);
          const nodes = prepared.filter((item) => item.event.type === "node.updated" && item.event.payload.id === node.id);
          const scopes = prepared.filter((item) => item.event.type === "checkpoint-scope.created" && item.event.payload.runId === run.id &&
            item.event.payload.nodeId === node.id && item.event.payload.providerThreadId === provider.id && item.event.payload.id === node.checkpointScopeId);
          if (prepared.length > 256 || providers.length !== 1 || providers[0]!.event.type !== "provider-thread.updated" ||
              nativeCreationCanonicalJson(providers[0]!.event.payload) !== nativeCreationCanonicalJson(provider) ||
              runs.length !== 1 || runs[0]!.event.type !== "run.updated" || nativeCreationCanonicalJson(runs[0]!.event.payload) !== nativeCreationCanonicalJson(run) ||
              nodes.length !== 1 || nodes[0]!.event.type !== "node.updated" || nativeCreationCanonicalJson(nodes[0]!.event.payload) !== nativeCreationCanonicalJson(node) ||
              scopes.length !== 1 || prepared.some((item) => item.event.type === "provider-turn.updated")) return null;
          const targetHistory = yield* sql`SELECT event_id FROM orchestration_events WHERE application_event_version = 2 AND aggregate_kind = 'thread'
            AND stream_id = ${input.threadId} AND ((event_type = 'provider-thread.updated' AND json_extract(payload_json, '$.id') = ${provider.id}
              AND command_id IS NOT ${executionIntent.commandId} AND (json_extract(payload_json, '$.nativeThreadRef') IS NOT NULL OR
                json_extract(payload_json, '$.nativeConversationHeadRef') IS NOT NULL)) OR
              (event_type = 'provider-turn.updated' AND json_extract(payload_json, '$.providerThreadId') = ${provider.id})) LIMIT 1`;
          const targetEffects = yield* sql`SELECT effect.effect_id FROM orchestration_v2_effect_outbox effect
            JOIN orchestration_v2_projection_runs old_run ON old_run.run_id = COALESCE(json_extract(effect.payload_json, '$.runId'), json_extract(effect.payload_json, '$.request.runId'))
              AND old_run.thread_id = effect.thread_id
            WHERE effect.thread_id = ${input.threadId} AND effect.effect_id != ${input.effectId}
              AND effect.effect_type IN ('provider-turn.start', 'provider-turn.restart') AND effect.attempt_count > 0
              AND json_extract(old_run.payload_json, '$.providerThreadId') = ${provider.id} LIMIT 1`;
          if (targetHistory.length !== 0 || targetEffects.length !== 0 || current.records.native_confirmations!.some((row) =>
              typeof row.binding_json === "string" && (JSON.parse(row.binding_json) as { providerThreadId?: unknown }).providerThreadId === provider.id)) return null;
        } else {
          const owner = yield* readCurrentProviderRuntimeOwnerEffect(input.threadId);
          if (owner === null || owner.evidenceRevision !== basis.sourceEvidenceRevision ||
              nativeCreationCanonicalJson(owner.binding) !== nativeCreationCanonicalJson(basis.sourceBinding)) return null;
        }
        return { incarnation, basis, executionIntent } satisfies InFlightQueuedRunStartBasisV2;
      })).pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause })));
    const reserveQueuedRunStart: EventSinkV2Shape["reserveQueuedRunStart"] = (input) => Effect.gen(function* () {
      if (input.pendingEffect !== undefined && (yield* CommitPublications) === undefined)
        return { status: "rejected" as const, reason: "intent_conflict" as const };
      return yield* withTransaction(Effect.gen(function* () {
      const basis = yield* Schema.decodeUnknownEffect(ContinuationBasisSchemaV2)(input.basis, { onExcessProperty: "error" });
      const executionIntent = yield* Schema.decodeUnknownEffect(ExecutionIntentSchemaV2)(input.executionIntent, { onExcessProperty: "error" });
      const reject = (reason: "fenced" | "basis_changed" | "source_unknown" | "intent_conflict") => ({ status: "rejected" as const, reason });
      if (basis.basisDigest !== queuedRunContinuationBasisDigestV2(basis) || basis.runId !== executionIntent.runId ||
          basis.runAttemptId !== executionIntent.runAttemptId || input.incarnation.threadId !== input.snapshot.threadId) return reject("basis_changed");
      if (input.pendingEffect !== undefined) {
        const request = yield* Schema.decodeUnknownEffect(Schema.Struct({ type: Schema.Literal("provider-turn.start"), runId: RunId }))(
          input.pendingEffect.request, { onExcessProperty: "error" });
        if (input.pendingEffect.id !== executionIntent.effectId || input.pendingEffect.commandId !== executionIntent.commandId ||
            input.pendingEffect.threadId !== input.snapshot.threadId || request.runId !== executionIntent.runId ||
            input.pendingEffect.nativeCreationExecutionReference !== undefined) return reject("intent_conflict");
      }
      const existing = yield* sql<{ readonly execution_intent_json: string; readonly basis_digest: string; readonly application_birth_json: string }>`
        SELECT execution_intent_json, basis_digest, application_birth_json FROM orchestration_v2_queued_start_reservations WHERE effect_id = ${executionIntent.effectId}`;
      if (existing.length > 0) {
        const original = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ExecutionIntentSchemaV2))(existing[0]!.execution_intent_json);
        const effects = yield* sql<{ readonly payload_json: string }>`SELECT payload_json FROM orchestration_v2_effect_outbox
          WHERE effect_id = ${executionIntent.effectId} AND command_id = ${executionIntent.commandId} AND thread_id = ${input.snapshot.threadId}`;
        if (effects.length !== 1) return reject("intent_conflict");
        const payload = yield* EffectOutbox.decodeOrchestrationEffectPayloadV2(effects[0]!.payload_json);
        if (payload.request.type !== "provider-turn.start" || payload.request.runId !== executionIntent.runId ||
            (input.pendingEffect !== undefined && nativeCreationCanonicalJson(payload.request) !== nativeCreationCanonicalJson(input.pendingEffect.request))) return reject("intent_conflict");
        return nativeCreationCanonicalJson(original) === nativeCreationCanonicalJson(executionIntent) && existing[0]!.basis_digest === basis.basisDigest &&
          existing[0]!.application_birth_json === nativeCreationCanonicalJson(input.incarnation)
          ? { status: "already_started" as const, executionIntent: original } : reject("intent_conflict");
      }
      if (basis.sourceMode === "unknown") return reject("source_unknown");
      const current = yield* readCommitSnapshot(input.snapshot.threadId, input.snapshot.commandId, input.snapshot.authority);
      const firstOrdinaryContext = yield* isFirstOrdinaryQueuedContext(current, basis, executionIntent);
      if (nativeCreationCanonicalJson(current) !== nativeCreationCanonicalJson(input.snapshot) ||
          nativeCreationCanonicalJson(yield* (firstOrdinaryContext || (basis.sourceMode === "new_context" && executionIntent.kind === "imported_history_choice")
            ? readApplicationBirthRecordEffect(input.snapshot.threadId) : readApplicationThreadBirth(input.snapshot.threadId))) !== nativeCreationCanonicalJson(input.incarnation)) return reject("basis_changed");
      yield* input.revalidateCurrentSource;
      const effects = yield* sql<{ readonly payload_json: string }>`SELECT effect.payload_json FROM orchestration_v2_effect_outbox effect
        JOIN orchestration_v2_projection_runs run ON run.run_id = ${executionIntent.runId} AND run.thread_id = effect.thread_id
        JOIN orchestration_v2_projection_run_attempts attempt ON attempt.attempt_id = ${executionIntent.runAttemptId}
          AND attempt.run_id = run.run_id AND attempt.thread_id = run.thread_id
        WHERE effect.effect_id = ${executionIntent.effectId} AND effect.command_id = ${executionIntent.commandId}
          AND effect.thread_id = ${input.snapshot.threadId} AND effect.effect_type = 'provider-turn.start'
          AND effect.status IN ('pending', 'running') AND run.status IN ('queued', 'starting', 'running')
          AND attempt.status IN ('pending', 'running') AND json_extract(run.payload_json, '$.activeAttemptId') = attempt.attempt_id
          AND json_extract(run.payload_json, '$.userMessageId') = ${basis.messageId}
          AND json_extract(attempt.payload_json, '$.providerThreadId') IS ${basis.queuedProviderThreadId}
          AND NOT EXISTS (SELECT 1 FROM orchestration_v2_unknown_effect_holds hold JOIN orchestration_v2_effect_outbox held
            ON held.effect_id = hold.effect_id WHERE held.thread_id = effect.thread_id)`;
      if (executionIntent.effectId !== `effect:${executionIntent.commandId}:provider-turn.start:${executionIntent.runId}`) return reject("intent_conflict");
      if (input.pendingEffect !== undefined) {
        if ((yield* CommitPublications) === undefined || input.pendingEffect.id !== executionIntent.effectId ||
            input.pendingEffect.commandId !== executionIntent.commandId || input.pendingEffect.threadId !== input.snapshot.threadId ||
            input.pendingEffect.request.type !== "provider-turn.start" || input.pendingEffect.request.runId !== executionIntent.runId ||
            input.pendingEffect.nativeCreationExecutionReference !== undefined) return reject("intent_conflict");
      }
      if (effects.length === 0 && input.pendingEffect !== undefined) {
        const candidates = yield* sql`SELECT run.run_id FROM orchestration_v2_projection_runs run
          JOIN orchestration_v2_projection_run_attempts attempt ON attempt.attempt_id = ${executionIntent.runAttemptId}
            AND attempt.run_id = run.run_id AND attempt.thread_id = run.thread_id
          WHERE run.run_id = ${executionIntent.runId} AND run.thread_id = ${input.snapshot.threadId}
            AND run.status IN ('queued', 'starting') AND attempt.status = 'pending'
            AND json_extract(run.payload_json, '$.activeAttemptId') = attempt.attempt_id
            AND json_extract(run.payload_json, '$.userMessageId') = ${basis.messageId}
            AND json_extract(attempt.payload_json, '$.providerThreadId') IS ${basis.queuedProviderThreadId}
            AND NOT EXISTS (SELECT 1 FROM orchestration_v2_effect_outbox old WHERE old.effect_id = ${executionIntent.effectId})
            AND NOT EXISTS (SELECT 1 FROM orchestration_v2_unknown_effect_holds hold JOIN orchestration_v2_effect_outbox held
              ON held.effect_id = hold.effect_id WHERE held.thread_id = run.thread_id)`;
        if (candidates.length !== 1) return reject("intent_conflict");
      } else if (effects.length !== 1) return reject("intent_conflict");
      if (effects.length === 1) {
        const payload = yield* EffectOutbox.decodeOrchestrationEffectPayloadV2(effects[0]!.payload_json);
        if (payload.request.type !== "provider-turn.start" || payload.request.runId !== executionIntent.runId) return reject("intent_conflict");
      }
      if (executionIntent.kind === "imported_history_choice") {
        const choice = yield* readImportedHistoryStartChoiceEffect({ commandId: executionIntent.commandId, threadId: input.snapshot.threadId });
        if (choice === null || choice.receipt.status !== "accepted" || choice.runId !== executionIntent.runId ||
            (choice.effectId !== null && choice.effectId !== executionIntent.effectId) || choice.command.reviewedBasis !== executionIntent.reviewedBasis ||
            choice.messageId !== basis.messageId || basis.sourceMode !== "new_context") return reject("intent_conflict");
      } else if (basis.executionIntent === null || nativeCreationCanonicalJson(basis.executionIntent) !== nativeCreationCanonicalJson(executionIntent)) return reject("intent_conflict");
      const fences = yield* readQueuedRunRuntimeStopFencesEffect({ threadId: input.snapshot.threadId, runId: basis.runId, incarnation: input.incarnation });
      if (fences.some((fence) => nativeCreationCanonicalJson(fence.executionIntent) === nativeCreationCanonicalJson(executionIntent))) return reject("fenced");
      const priorStops = yield* sql<{ readonly command_id: string }>`SELECT command_id FROM orchestration_v2_current_runtime_stop_intents
        WHERE thread_id = ${input.snapshot.threadId} ORDER BY command_id`;
      for (const row of priorStops) {
        const stopped = yield* readCurrentThreadRuntimeStopIntentEffect({ threadId: input.snapshot.threadId, commandId: CommandId.make(row.command_id) });
        if (stopped === null || nativeCreationCanonicalJson(stopped.incarnation) !== nativeCreationCanonicalJson(input.incarnation)) continue;
        const reviewed = stopped.queuedBases.find((candidate) => candidate.runId === basis.runId);
        if (reviewed?.sourceMode === "unknown" && (executionIntent.kind !== "imported_history_choice" || basis.sourceMode !== "new_context" ||
            (reviewed.executionIntent !== null && nativeCreationCanonicalJson(reviewed.executionIntent) === nativeCreationCanonicalJson(executionIntent))))
          return reject("source_unknown");
      }
      if (basis.sourceMode === "queued_thread" || basis.sourceMode === "active_native_copy") {
        const owner = yield* readCurrentProviderRuntimeOwnerEffect(input.snapshot.threadId);
        if (owner === null || owner.evidenceRevision !== basis.sourceEvidenceRevision ||
            nativeCreationCanonicalJson(owner.binding) !== nativeCreationCanonicalJson(basis.sourceBinding)) return reject("basis_changed");
      }
      if (input.pendingEffect !== undefined) yield* effectOutbox.enqueue([input.pendingEffect]);
      yield* sql`INSERT INTO orchestration_v2_queued_start_reservations
        (effect_id, command_id, thread_id, run_id, run_attempt_id, application_birth_json, execution_intent_json, basis_json, basis_digest, reserved_at)
        VALUES (${executionIntent.effectId}, ${executionIntent.commandId}, ${input.snapshot.threadId}, ${executionIntent.runId},
          ${executionIntent.runAttemptId}, ${nativeCreationCanonicalJson(input.incarnation)}, ${nativeCreationCanonicalJson(executionIntent)},
          ${nativeCreationCanonicalJson(basis)}, ${basis.basisDigest}, ${DateTime.formatIso(yield* DateTime.now)})`;
      return { status: "reserved" as const, executionIntent };
      }));
    }).pipe(Effect.mapError((cause) => cause instanceof NativeCommandPreconditionError ? cause : new EventSinkWriteError({ eventCount: 0, cause })));
    const rejectProviderBinding = (rejection: Extract<ProviderBindingWriteResultV2, { committed: false }>["rejection"]) =>
      ({ committed: false as const, rejection, storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent> });
    const continuationBindingSchema = Schema.Struct({
      threadId: ThreadId, providerThreadId: ProviderThreadId, providerSessionId: ProviderSessionId,
      instanceId: ProviderInstanceId, driver: ProviderDriverKind, nativeThreadId: Schema.NonEmptyString, runtimeGeneration: Schema.NonEmptyString,
    });
    const readProviderContinuationSourceIdentity: EventSinkV2Shape["readProviderContinuationSourceIdentity"] = (expected) =>
      sql.withTransaction(Effect.gen(function* () {
        if (expected.nativeThreadId === null || expected.runtimeGeneration === null) return null;
        const binding = yield* Schema.decodeUnknownEffect(continuationBindingSchema)(expected);
        const sourceId = nativeCreationSha256(nativeCreationCanonicalJson(binding));
        const rows = yield* sql<{ readonly continuation_key: string; readonly driver: ProviderDriverKind; readonly runtime_generation: string }>`
          SELECT continuation_key, driver, runtime_generation FROM orchestration_v2_provider_continuation_sources
          WHERE source_id = ${sourceId} AND thread_id = ${binding.threadId} AND provider_thread_id = ${binding.providerThreadId}
            AND provider_session_id = ${binding.providerSessionId} AND provider_instance_id = ${binding.instanceId}
            AND driver = ${binding.driver} AND native_thread_id = ${binding.nativeThreadId} AND runtime_generation = ${binding.runtimeGeneration}`;
        if (rows.length === 0) return null;
        if (rows.length !== 1) return yield* new EventSinkWriteError({ eventCount: 0, cause: "Ambiguous historical continuation source" });
        return yield* Schema.decodeUnknownEffect(ProviderContinuationSourceIdentity)({ driverKind: rows[0]!.driver,
          continuationKey: rows[0]!.continuation_key, runtimeGeneration: rows[0]!.runtime_generation });
      })).pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause })));
    const checkProviderBinding = Effect.fnUntraced(function* (
      input: { readonly expectedBinding: ProviderBindingExpectationV2; readonly expectedEvidenceRevision: number;
        readonly expectedRegisteredBinding?: ProviderBindingExpectationV2 | null;
        readonly expectedRunId?: RunId; readonly expectedRunAttemptId?: RunAttemptId },
    ) {
      const expected = input.expectedBinding;
      const rows = yield* sql<{ readonly active_provider_thread_id: string | null; readonly model_instance: string | null;
        readonly thread_id: string | null; readonly provider_session_id: string | null; readonly provider_instance_id: string | null;
        readonly driver: string | null; readonly native_thread_id: string | null; readonly session_status: string;
        readonly session_instance: string | null; readonly session_driver: string | null }>`
        SELECT json_extract(t.payload_json, '$.activeProviderThreadId') AS active_provider_thread_id,
          json_extract(t.payload_json, '$.modelSelection.instanceId') AS model_instance,
          p.thread_id, p.provider_session_id, p.provider_instance_id, p.driver,
          json_extract(p.payload_json, '$.nativeThreadRef.nativeId') AS native_thread_id,
          s.status AS session_status, s.provider_instance_id AS session_instance, s.driver AS session_driver
        FROM orchestration_v2_projection_threads t
        JOIN orchestration_v2_projection_provider_threads p ON p.provider_thread_id = ${expected.providerThreadId}
        JOIN orchestration_v2_projection_provider_sessions s ON s.provider_session_id = p.provider_session_id
        JOIN orchestration_v2_projection_provider_session_bindings b ON b.provider_session_id = s.provider_session_id AND b.thread_id = t.thread_id
        WHERE t.thread_id = ${expected.threadId}`;
      const row = rows[0];
      if (rows.length !== 1 || row === undefined || row.active_provider_thread_id !== expected.providerThreadId ||
          row.model_instance !== expected.instanceId || row.thread_id !== expected.threadId || row.provider_session_id !== expected.providerSessionId ||
          row.provider_instance_id !== expected.instanceId || row.driver !== expected.driver || row.native_thread_id !== expected.nativeThreadId ||
          row.session_instance !== expected.instanceId || row.session_driver !== expected.driver || row.session_status === "stopped" || row.session_status === "error")
        return "binding_mismatch" as const;
      const current = yield* readProviderRuntimeEvidenceEffect(expected.threadId);
      if ((current?.evidenceRevision ?? 0) !== input.expectedEvidenceRevision) return "evidence_revision_mismatch" as const;
      const registeredExpected = input.expectedRegisteredBinding === undefined ? expected : input.expectedRegisteredBinding;
      if (current === null ? (registeredExpected !== null && registeredExpected.runtimeGeneration !== null) :
          registeredExpected === null || nativeCreationCanonicalJson(current.binding) !== nativeCreationCanonicalJson(registeredExpected))
        return "unregistered_generation" as const;
      if (input.expectedRunId !== undefined || input.expectedRunAttemptId !== undefined) {
        if (input.expectedRunId === undefined || input.expectedRunAttemptId === undefined) return "attempt_mismatch" as const;
        const attempts = yield* sql`
          SELECT r.run_id FROM orchestration_v2_projection_runs r
          JOIN orchestration_v2_projection_run_attempts a ON a.attempt_id = ${input.expectedRunAttemptId} AND a.run_id = r.run_id AND a.thread_id = r.thread_id
          WHERE r.run_id = ${input.expectedRunId} AND r.thread_id = ${expected.threadId}
            AND json_extract(r.payload_json, '$.activeAttemptId') = ${input.expectedRunAttemptId}
            AND json_extract(a.payload_json, '$.providerThreadId') = ${expected.providerThreadId}`;
        if (attempts.length !== 1) return "attempt_mismatch" as const;
      }
      return null;
    });
    const registerProviderRuntimeEffect = Effect.fnUntraced(function* (
      input: Parameters<EventSinkV2Shape["registerProviderRuntime"]>[0],
    ) {
      yield* assertPublicationScope;
      return yield* sql.withTransaction(Effect.gen(function* () {
        const rejection = yield* checkProviderBinding(input);
        if (rejection !== null) return rejectProviderBinding(rejection);
        const actual = yield* Schema.decodeUnknownEffect(ProviderRuntimeBinding)(input.actualBinding);
        const expected = input.expectedBinding;
        if (actual.threadId !== expected.threadId || actual.providerThreadId !== expected.providerThreadId ||
            actual.providerSessionId !== expected.providerSessionId || actual.instanceId !== expected.instanceId ||
            (actual.nativeThreadId ?? null) !== expected.nativeThreadId)
          return rejectProviderBinding("binding_mismatch");
        const sourceIdentity = input.actualContinuationSourceIdentity === undefined ? null
          : yield* Schema.decodeUnknownEffect(ProviderContinuationSourceIdentity)(input.actualContinuationSourceIdentity);
        if (sourceIdentity !== null && (sourceIdentity.driverKind !== expected.driver || sourceIdentity.runtimeGeneration !== actual.runtimeGeneration))
          return rejectProviderBinding("binding_mismatch");
        const sourceBinding = sourceIdentity === null || actual.nativeThreadId === undefined ? null
          : yield* Schema.decodeUnknownEffect(continuationBindingSchema)({ ...actual, driver: expected.driver });
        const sourceId = sourceBinding === null ? null : nativeCreationSha256(nativeCreationCanonicalJson(sourceBinding));
        const previousSource = sourceId === null ? [] : yield* sql<{ readonly continuation_key: string }>`
          SELECT continuation_key FROM orchestration_v2_provider_continuation_sources WHERE source_id = ${sourceId}`;
        if (previousSource.length > 1 || (previousSource.length === 1 && previousSource[0]!.continuation_key !== sourceIdentity?.continuationKey))
          return rejectProviderBinding("binding_mismatch");
        const revision = input.expectedEvidenceRevision + 1;
        const now = DateTime.formatIso(yield* DateTime.now);
        if (input.expectedEvidenceRevision === 0) {
          yield* sql`INSERT INTO orchestration_v2_provider_runtime_evidence
            (thread_id, provider_thread_id, provider_session_id, provider_instance_id, driver, native_thread_id,
              runtime_generation, evidence_revision, observation_json, registered_at)
            VALUES (${actual.threadId}, ${actual.providerThreadId}, ${actual.providerSessionId}, ${actual.instanceId}, ${expected.driver},
              ${actual.nativeThreadId ?? null}, ${actual.runtimeGeneration}, ${revision}, NULL, ${now})`;
        } else {
          const rows = yield* sql`UPDATE orchestration_v2_provider_runtime_evidence SET
            provider_thread_id = ${actual.providerThreadId}, provider_session_id = ${actual.providerSessionId},
            provider_instance_id = ${actual.instanceId}, driver = ${expected.driver}, native_thread_id = ${actual.nativeThreadId ?? null},
            runtime_generation = ${actual.runtimeGeneration}, evidence_revision = ${revision}, observation_json = NULL, registered_at = ${now}
            WHERE thread_id = ${actual.threadId} AND evidence_revision = ${input.expectedEvidenceRevision}
              AND runtime_generation = ${(input.expectedRegisteredBinding ?? expected).runtimeGeneration} RETURNING thread_id`;
          if (rows.length !== 1) return rejectProviderBinding("evidence_revision_mismatch");
        }
        if (sourceBinding !== null && sourceIdentity !== null && previousSource.length === 0)
          yield* sql`INSERT INTO orchestration_v2_provider_continuation_sources
            (source_id, thread_id, provider_thread_id, provider_session_id, provider_instance_id, driver, native_thread_id, runtime_generation, continuation_key, registered_at)
            VALUES (${sourceId}, ${sourceBinding.threadId}, ${sourceBinding.providerThreadId}, ${sourceBinding.providerSessionId}, ${sourceBinding.instanceId},
              ${sourceBinding.driver}, ${sourceBinding.nativeThreadId}, ${sourceBinding.runtimeGeneration}, ${sourceIdentity.continuationKey}, ${now})`;
        return { committed: true as const, evidenceRevision: revision, storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent> };
      }));
    });
    const checkCurrentProviderRuntimeOwner = Effect.fnUntraced(function* (
      input: Pick<Parameters<EventSinkV2Shape["writeIfProviderBindingCurrent"]>[0],
        "expectedBinding" | "expectedEvidenceRevision" | "expectedRunId" | "expectedRunAttemptId">,
    ) {
      const owner = yield* readCurrentProviderRuntimeOwnerEffect(input.expectedBinding.threadId);
      if (owner === null || nativeCreationCanonicalJson(owner.binding) !== nativeCreationCanonicalJson(input.expectedBinding)) return "binding_mismatch" as const;
      if (owner.evidenceRevision !== input.expectedEvidenceRevision) return "evidence_revision_mismatch" as const;
      if ((yield* readApplicationBirthRecordEffect(input.expectedBinding.threadId)) === null) return "binding_mismatch" as const;
      if (input.expectedRunId !== undefined || input.expectedRunAttemptId !== undefined) {
        if (input.expectedRunId === undefined || input.expectedRunAttemptId === undefined) return "attempt_mismatch" as const;
        const attempts = yield* sql`SELECT run.run_id FROM orchestration_v2_projection_runs run
          JOIN orchestration_v2_projection_run_attempts attempt ON attempt.run_id = run.run_id AND attempt.thread_id = run.thread_id
          WHERE run.thread_id = ${input.expectedBinding.threadId} AND run.run_id = ${input.expectedRunId}
            AND attempt.attempt_id = ${input.expectedRunAttemptId} AND json_extract(run.payload_json, '$.activeAttemptId') = attempt.attempt_id
            AND json_extract(run.payload_json, '$.providerThreadId') = ${input.expectedBinding.providerThreadId}
            AND json_extract(run.payload_json, '$.modelSelection.instanceId') = ${input.expectedBinding.instanceId}
            AND json_extract(attempt.payload_json, '$.providerThreadId') = ${input.expectedBinding.providerThreadId}`;
        if (attempts.length !== 1) return "attempt_mismatch" as const;
      }
      return null;
    });
    const writeIfProviderBindingCurrentEffect = Effect.fnUntraced(function* (
      input: Parameters<EventSinkV2Shape["writeIfProviderBindingCurrent"]>[0] & { readonly revalidateCurrentOwner?: Effect.Effect<void, unknown> },
      ownerOnly = false,
    ) {
      yield* assertPublicationScope;
      const result = yield* sql.withTransaction(Effect.gen(function* () {
        const rejection = yield* (ownerOnly ? checkCurrentProviderRuntimeOwner(input) : checkProviderBinding(input));
        if (rejection !== null) return rejectProviderBinding(rejection);
        if (input.expectedBinding.runtimeGeneration === null || input.expectedEvidenceRevision < 1)
          return rejectProviderBinding("unregistered_generation");
        if (input.events.some((event) => event.threadId !== input.expectedBinding.threadId ||
            (event.type === "provider-session.updated" && (event.payload.id !== input.expectedBinding.providerSessionId ||
              event.payload.driver !== input.expectedBinding.driver || event.payload.providerInstanceId !== input.expectedBinding.instanceId ||
              event.payload.runtimeIdentity?.requested.providerInstanceId !== input.expectedBinding.instanceId ||
              event.payload.runtimeIdentity?.requested.providerDriver !== input.expectedBinding.driver ||
              event.payload.runtimeIdentity?.runtimeGeneration !== input.expectedBinding.runtimeGeneration))))
          return rejectProviderBinding("binding_mismatch");
        if (ownerOnly) {
          if (input.revalidateCurrentOwner === undefined) return rejectProviderBinding("unregistered_generation");
          if (input.events.length === 0 && input.observation === undefined) return rejectProviderBinding("binding_mismatch");
          for (const event of input.events) {
            if (event.type !== "provider-session.updated" || event.payload.runtimeIdentity === undefined ||
                (event.providerInstanceId !== undefined && event.providerInstanceId !== input.expectedBinding.instanceId) ||
                (event.driver !== undefined && event.driver !== input.expectedBinding.driver) ||
                ("providerThreadId" in event && event.providerThreadId !== input.expectedBinding.providerThreadId)) return rejectProviderBinding("binding_mismatch");
            const sessions = yield* sql<{ readonly payload_json: string }>`SELECT payload_json FROM orchestration_v2_projection_provider_sessions
              WHERE provider_session_id = ${input.expectedBinding.providerSessionId}`;
            if (sessions.length !== 1) return rejectProviderBinding("binding_mismatch");
            const session = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestrationV2ProviderSessionJson))(sessions[0]!.payload_json);
            const { runtimeIdentity: _oldIdentity, updatedAt: _oldUpdated, ...oldFields } = Schema.encodeSync(OrchestrationV2ProviderSessionJson)(session);
            const { runtimeIdentity: _newIdentity, updatedAt: _newUpdated, ...newFields } = Schema.encodeSync(OrchestrationV2ProviderSessionJson)(event.payload);
            if (nativeCreationCanonicalJson(oldFields) !== nativeCreationCanonicalJson(newFields)) return rejectProviderBinding("binding_mismatch");
          }
          yield* input.revalidateCurrentOwner;
          const after = yield* checkCurrentProviderRuntimeOwner(input);
          if (after !== null) return rejectProviderBinding(after);
        }
        const observation = input.observation === undefined ? null : yield* Schema.decodeUnknownEffect(ProviderRuntimeObservation)(input.observation);
        if (ownerOnly && observation !== null && observation.binding === undefined) return rejectProviderBinding("binding_mismatch");
        if (observation !== null && observation.binding !== undefined &&
            (observation.binding.threadId !== input.expectedBinding.threadId || observation.binding.providerThreadId !== input.expectedBinding.providerThreadId ||
             observation.binding.providerSessionId !== input.expectedBinding.providerSessionId || observation.binding.instanceId !== input.expectedBinding.instanceId ||
             observation.binding.runtimeGeneration !== input.expectedBinding.runtimeGeneration || (observation.binding.nativeThreadId ?? null) !== input.expectedBinding.nativeThreadId))
          return rejectProviderBinding("binding_mismatch");
        const revision = input.expectedEvidenceRevision + 1;
        const rows = yield* sql`UPDATE orchestration_v2_provider_runtime_evidence SET
          evidence_revision = ${revision}, observation_json = ${observation === null ? null : Schema.encodeSync(Schema.fromJsonString(ProviderRuntimeObservation))(observation)}
          WHERE thread_id = ${input.expectedBinding.threadId} AND evidence_revision = ${input.expectedEvidenceRevision}
            AND runtime_generation = ${input.expectedBinding.runtimeGeneration} RETURNING thread_id`;
        if (rows.length !== 1) return rejectProviderBinding("evidence_revision_mismatch");
        const storedEvents = yield* eventStore.append({ events: yield* normalizeEvents(input.events) });
        yield* applyStoredEvents(storedEvents);
        return { committed: true as const, evidenceRevision: revision, storedEvents };
      }));
      if (result.committed) {
        yield* afterCommit(eventStore.publishCommitted(result.storedEvents));
        yield* afterCommit(publishLiveEvents(result.storedEvents));
      }
      return result;
    });

    const writeIfCurrentProviderRuntimeOutputOwner = Effect.fnUntraced(function* (
      input: Parameters<EventSinkV2Shape["writeIfCurrentProviderRuntimeOutputOwner"]>[0],
    ) {
      yield* assertPublicationScope;
      const result = yield* sql.withTransaction(Effect.gen(function* () {
        const rejection = yield* checkCurrentProviderRuntimeOwner(input);
        if (rejection !== null) return rejectProviderBinding(rejection);
        if (input.expectedBinding.runtimeGeneration === null || input.expectedEvidenceRevision < 1)
          return rejectProviderBinding("unregistered_generation");
        if (input.events.length === 0) return rejectProviderBinding("binding_mismatch");
        const events = yield* Effect.forEach(input.events, (event) =>
          Schema.decodeUnknownEffect(OrchestrationV2DomainEvent)(event, { onExcessProperty: "error" }));
        const companions = yield* Effect.forEach(input.companionNodes ?? [], (event) =>
          Schema.decodeUnknownEffect(OrchestrationV2DomainEvent)(event, { onExcessProperty: "error" }));
        const nodes = new Map<NodeId, Extract<OrchestrationV2DomainEvent, { readonly type: "node.updated" }>>();
        const sameEnvelope = (event: OrchestrationV2DomainEvent) => event.threadId === input.expectedBinding.threadId &&
          event.runId === input.expectedRunId && event.driver === input.expectedBinding.driver &&
          event.providerInstanceId === input.expectedBinding.instanceId;
        for (const event of events) {
          if (!sameEnvelope(event) || event.nodeId === undefined ||
              (event.type !== "message.updated" && event.type !== "turn-item.updated")) return rejectProviderBinding("binding_mismatch");
          if (event.payload.threadId !== input.expectedBinding.threadId || event.payload.runId !== input.expectedRunId ||
              event.payload.nodeId !== event.nodeId) return rejectProviderBinding("attempt_mismatch");
          if (event.type === "message.updated") {
            if (event.payload.role !== "assistant") return rejectProviderBinding("binding_mismatch");
          } else if ((event.payload.type !== "assistant_message" && event.payload.type !== "reasoning") ||
              event.payload.providerThreadId !== input.expectedBinding.providerThreadId ||
              event.payload.providerTurnId !== input.expectedProviderTurnId) return rejectProviderBinding("binding_mismatch");
        }
        for (const event of companions) {
          if (event.type !== "node.updated" || !sameEnvelope(event) || event.nodeId !== event.payload.id ||
              (event.payload.kind !== "assistant_message" && event.payload.kind !== "reasoning") ||
              event.payload.countsForRun ||
              event.payload.threadId !== input.expectedBinding.threadId || event.payload.runId !== input.expectedRunId ||
              event.payload.providerThreadId !== input.expectedBinding.providerThreadId ||
              event.payload.providerTurnId !== input.expectedProviderTurnId || nodes.has(event.payload.id) ||
              !events.some((output) => output.nodeId === event.payload.id &&
                (output.type === "message.updated" ? event.payload.kind === "assistant_message" :
                  output.type === "turn-item.updated" && output.payload.type === event.payload.kind)))
            return rejectProviderBinding("binding_mismatch");
          nodes.set(event.payload.id, event);
        }
        const readNode = Effect.fnUntraced(function* (nodeId: NodeId) {
          const rows = yield* sql<{ readonly payload_json: string }>`SELECT payload_json FROM orchestration_v2_projection_nodes WHERE node_id = ${nodeId}`;
          if (rows.length !== 1) return null;
          return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestrationV2ExecutionNodeJson))(rows[0]!.payload_json);
        });
        const withoutFields = (value: object, fields: ReadonlyArray<string>) =>
          Object.fromEntries(Object.entries(value).filter(([key]) => !fields.includes(key)));
        const validateOutput = Effect.gen(function* () {
          const turns = yield* sql<{ readonly payload_json: string }>`SELECT turn.payload_json
            FROM orchestration_v2_projection_provider_turns turn
            JOIN orchestration_v2_projection_runs run ON run.run_id = ${input.expectedRunId} AND run.thread_id = turn.thread_id
            JOIN orchestration_v2_projection_provider_threads provider ON provider.provider_thread_id = turn.provider_thread_id AND provider.thread_id = turn.thread_id
            WHERE turn.provider_turn_id = ${input.expectedProviderTurnId} AND turn.thread_id = ${input.expectedBinding.threadId}
              AND turn.provider_thread_id = ${input.expectedBinding.providerThreadId} AND turn.run_attempt_id = ${input.expectedRunAttemptId}
              AND json_extract(provider.payload_json, '$.lastRunOrdinal') = run.ordinal`;
          if (turns.length !== 1) return "attempt_mismatch" as const;
          const turn = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestrationV2ProviderTurnJson))(turns[0]!.payload_json);
          if (turn.id !== input.expectedProviderTurnId || turn.providerThreadId !== input.expectedBinding.providerThreadId ||
              turn.runAttemptId !== input.expectedRunAttemptId) return "attempt_mismatch" as const;
          const turnNode = yield* readNode(turn.nodeId);
          if (turnNode === null || turnNode.threadId !== input.expectedBinding.threadId || turnNode.runId !== input.expectedRunId ||
              turnNode.providerThreadId !== input.expectedBinding.providerThreadId ||
              (turnNode.providerTurnId !== null && turnNode.providerTurnId !== turn.id)) return "attempt_mismatch" as const;
          const root = yield* readNode(turnNode.rootNodeId);
          const roots = yield* sql`SELECT run_id FROM orchestration_v2_projection_runs WHERE run_id = ${input.expectedRunId}
            AND thread_id = ${input.expectedBinding.threadId} AND json_extract(payload_json, '$.rootNodeId') = ${turnNode.rootNodeId}`;
          if (root === null || roots.length !== 1 || root.id !== root.rootNodeId || root.threadId !== input.expectedBinding.threadId ||
              root.runId !== input.expectedRunId || root.providerThreadId !== input.expectedBinding.providerThreadId ||
              (root.providerTurnId !== null && root.providerTurnId !== turn.id)) return "attempt_mismatch" as const;
          for (const companion of nodes.values()) {
            const previous = yield* readNode(companion.payload.id);
            if (previous !== null && nativeCreationCanonicalJson(withoutFields(Schema.encodeSync(OrchestrationV2ExecutionNodeJson)(previous),
                ["status", "startedAt", "completedAt"])) !== nativeCreationCanonicalJson(withoutFields(Schema.encodeSync(OrchestrationV2ExecutionNodeJson)(companion.payload),
                ["status", "startedAt", "completedAt"]))) return "binding_mismatch" as const;
          }
          for (const event of events) {
            if (event.type !== "message.updated" && event.type !== "turn-item.updated") return "binding_mismatch" as const;
            const output = event.payload;
            if (output.nodeId === null) return "attempt_mismatch" as const;
            const node = nodes.get(output.nodeId)?.payload ?? (yield* readNode(output.nodeId));
            if (node === null || node.threadId !== input.expectedBinding.threadId || node.runId !== input.expectedRunId ||
                node.providerThreadId !== input.expectedBinding.providerThreadId || node.rootNodeId !== turnNode.rootNodeId ||
                (node.id !== turn.nodeId && node.providerTurnId !== turn.id) ||
                (node.id !== turn.nodeId && node.kind !== (event.type === "message.updated" ? "assistant_message" : event.payload.type)))
              return "attempt_mismatch" as const;
            const visited = new Set<NodeId>();
            let ancestor = node;
            while (ancestor.id !== turn.nodeId) {
              if (visited.has(ancestor.id) || visited.size >= 256 || ancestor.parentNodeId === null) return "attempt_mismatch" as const;
              visited.add(ancestor.id);
              const parent = yield* readNode(ancestor.parentNodeId);
              if (parent === null || parent.threadId !== input.expectedBinding.threadId || parent.runId !== input.expectedRunId ||
                  parent.providerThreadId !== input.expectedBinding.providerThreadId || parent.rootNodeId !== turnNode.rootNodeId ||
                  (parent.providerTurnId !== null && parent.providerTurnId !== turn.id)) return "attempt_mismatch" as const;
              ancestor = parent;
            }
            if (event.type === "message.updated") {
              const rows = yield* sql<{ readonly payload_json: string }>`SELECT payload_json FROM orchestration_v2_projection_messages WHERE message_id = ${event.payload.id}`;
              if (rows.length > 1) return "binding_mismatch" as const;
              if (rows.length === 1) {
                const previous = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestrationV2ConversationMessageJson))(rows[0]!.payload_json);
                if (nativeCreationCanonicalJson(withoutFields(Schema.encodeSync(OrchestrationV2ConversationMessageJson)(previous),
                    ["text", "attachments", "streaming", "updatedAt"])) !== nativeCreationCanonicalJson(withoutFields(Schema.encodeSync(OrchestrationV2ConversationMessageJson)(event.payload),
                    ["text", "attachments", "streaming", "updatedAt"]))) return "binding_mismatch" as const;
              }
              const associations = yield* sql`SELECT turn_item_id FROM orchestration_v2_projection_turn_items
                WHERE type = 'assistant_message' AND json_extract(payload_json, '$.messageId') = ${event.payload.id}
                  AND (thread_id <> ${input.expectedBinding.threadId} OR run_id IS NOT ${input.expectedRunId} OR node_id IS NOT ${output.nodeId}
                    OR provider_thread_id IS NOT ${input.expectedBinding.providerThreadId} OR provider_turn_id IS NOT ${input.expectedProviderTurnId})`;
              if (associations.length > 0) return "binding_mismatch" as const;
            } else {
              const rows = yield* sql<{ readonly payload_json: string }>`SELECT payload_json FROM orchestration_v2_projection_turn_items WHERE turn_item_id = ${event.payload.id}`;
              if (rows.length > 1) return "binding_mismatch" as const;
              if (rows.length === 1) {
                const previous = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestrationV2TurnItemJson))(rows[0]!.payload_json);
                if (nativeCreationCanonicalJson(withoutFields(Schema.encodeSync(OrchestrationV2TurnItemJson)(previous),
                    ["text", "attachments", "streaming", "status", "title", "completedAt", "updatedAt"])) !== nativeCreationCanonicalJson(withoutFields(Schema.encodeSync(OrchestrationV2TurnItemJson)(event.payload),
                    ["text", "attachments", "streaming", "status", "title", "completedAt", "updatedAt"]))) return "binding_mismatch" as const;
              }
              if (event.payload.type === "assistant_message") {
                const messageId = event.payload.messageId;
                const messages = yield* sql`SELECT message_id FROM orchestration_v2_projection_messages WHERE message_id = ${messageId}
                  AND (thread_id <> ${input.expectedBinding.threadId} OR run_id IS NOT ${input.expectedRunId} OR node_id IS NOT ${output.nodeId} OR role <> 'assistant')`;
                if (messages.length > 0 || events.some((candidate) => candidate.type === "message.updated" && candidate.payload.id === messageId &&
                    candidate.payload.nodeId !== output.nodeId)) return "binding_mismatch" as const;
              }
            }
          }
          return null;
        });
        const before = yield* validateOutput;
        if (before !== null) return rejectProviderBinding(before);
        yield* input.revalidateCurrentOwner;
        const current = yield* checkCurrentProviderRuntimeOwner(input);
        if (current !== null) return rejectProviderBinding(current);
        const after = yield* validateOutput;
        if (after !== null) return rejectProviderBinding(after);
        const storedEvents = yield* eventStore.append({ events: yield* normalizeEvents([...nodes.values(), ...events]) });
        yield* applyStoredEvents(storedEvents);
        return { committed: true as const, evidenceRevision: input.expectedEvidenceRevision, storedEvents };
      }));
      if (result.committed) {
        yield* afterCommit(eventStore.publishCommitted(result.storedEvents));
        yield* afterCommit(publishLiveEvents(result.storedEvents));
      }
      return result;
    });

    const decodeProviderBinding = Schema.decodeUnknownEffect(Schema.Struct({
      threadId: ThreadId, providerThreadId: ProviderThreadId, providerSessionId: ProviderSessionId,
      instanceId: ProviderInstanceId, driver: ProviderDriverKind,
      nativeThreadId: Schema.NullOr(Schema.String), runtimeGeneration: Schema.NullOr(Schema.String),
    }));
    type RestartMarkerRow = { readonly marker_id: string; readonly thread_id: string; readonly project_id: string;
      readonly source_run_id: string; readonly source_run_attempt_id: string; readonly binding_json: string;
      readonly evidence_revision: number; readonly created_at: string; readonly status: string };
    const markerFromRow = Effect.fnUntraced(function* (row: RestartMarkerRow) {
      return { markerId: row.marker_id, threadId: ThreadId.make(row.thread_id), projectId: ProjectId.make(row.project_id),
        sourceRunId: RunId.make(row.source_run_id), sourceRunAttemptId: RunAttemptId.make(row.source_run_attempt_id),
        binding: yield* decodeProviderBinding(yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(row.binding_json)),
        evidenceRevision: row.evidence_revision, createdAt: row.created_at } satisfies RestartContinuationMarkerV2;
    });
    const markerMatches = (row: RestartMarkerRow, marker: RestartContinuationMarkerV2) =>
      row.marker_id === marker.markerId && row.thread_id === marker.threadId && row.project_id === marker.projectId &&
      row.source_run_id === marker.sourceRunId && row.source_run_attempt_id === marker.sourceRunAttemptId &&
      row.binding_json === nativeCreationCanonicalJson(marker.binding) && row.evidence_revision === marker.evidenceRevision && row.created_at === marker.createdAt;
    const prepareRestartContinuation: EventSinkV2Shape["prepareRestartContinuation"] = (input) =>
      withTransaction(Effect.gen(function* () {
        const reject = yield* checkProviderBinding({ expectedBinding: input.expectedBinding, expectedEvidenceRevision: input.expectedEvidenceRevision,
          expectedRunId: input.sourceRunId, expectedRunAttemptId: input.sourceRunAttemptId });
        if (reject !== null || input.expectedBinding.threadId !== input.threadId || input.expectedBinding.runtimeGeneration === null)
          return yield* new RestartContinuationMarkerError({ reason: "binding_changed" });
        const project = yield* sql`SELECT thread_id FROM orchestration_v2_projection_threads WHERE thread_id = ${input.threadId}
          AND project_id = ${input.projectId}`;
        if (project.length !== 1) return yield* new RestartContinuationMarkerError({ reason: "source_changed" });
        if ((yield* effectOutbox.listHeldByThreadId(input.threadId)).length > 0)
          return yield* new RestartContinuationMarkerError({ reason: "unknown_effect" });
        const existing = yield* sql<RestartMarkerRow>`SELECT * FROM orchestration_v2_restart_continuation_markers
          WHERE thread_id = ${input.threadId} AND status = 'dormant'`;
        if (existing.length > 0) {
          const row = existing[0]!;
          const same = existing.length === 1 && row.status === "dormant" &&
            row.thread_id === input.threadId && row.project_id === input.projectId && row.source_run_id === input.sourceRunId &&
            row.source_run_attempt_id === input.sourceRunAttemptId && row.evidence_revision === input.expectedEvidenceRevision &&
            row.binding_json === nativeCreationCanonicalJson(input.expectedBinding);
          if (!same) return yield* new RestartContinuationMarkerError({ reason: "marker_conflict" });
          return yield* markerFromRow(row);
        }
        const markerId = typeof input.markerId === "string" ? input.markerId : yield* input.markerId;
        const now = DateTime.formatIso(yield* DateTime.now);
        yield* sql`INSERT INTO orchestration_v2_restart_continuation_markers
          (marker_id, thread_id, project_id, source_run_id, source_run_attempt_id, binding_json, evidence_revision, status, created_at, updated_at, effect_id)
          VALUES (${markerId}, ${input.threadId}, ${input.projectId}, ${input.sourceRunId}, ${input.sourceRunAttemptId},
            ${nativeCreationCanonicalJson(input.expectedBinding)}, ${input.expectedEvidenceRevision}, 'dormant', ${now}, ${now}, NULL)`;
        return { markerId, threadId: input.threadId, projectId: input.projectId,
          sourceRunId: input.sourceRunId, sourceRunAttemptId: input.sourceRunAttemptId,
          binding: input.expectedBinding, evidenceRevision: input.expectedEvidenceRevision, createdAt: now };
      })).pipe(Effect.mapError((cause) => Schema.is(RestartContinuationMarkerError)(cause) ? cause : new EventSinkWriteError({ eventCount: 0, cause })));
    const findDormantRestartContinuation: EventSinkV2Shape["findDormantRestartContinuation"] = (input) => sql.withTransaction(Effect.gen(function* () {
      const rows = yield* sql<RestartMarkerRow>`SELECT * FROM orchestration_v2_restart_continuation_markers
        WHERE thread_id = ${input.threadId} AND status = 'dormant'`;
      if (rows.length === 0) return null;
      const row = rows[0]!;
      if (rows.length !== 1 || row.project_id !== input.projectId || row.source_run_id !== input.sourceRunId ||
          row.source_run_attempt_id !== input.sourceRunAttemptId || row.evidence_revision !== input.expectedEvidenceRevision ||
          row.binding_json !== nativeCreationCanonicalJson(input.expectedBinding))
        return yield* new RestartContinuationMarkerError({ reason: "marker_conflict" });
      const project = yield* sql`SELECT thread_id FROM orchestration_v2_projection_threads
        WHERE thread_id = ${input.threadId} AND project_id = ${input.projectId}`;
      if (project.length !== 1) return yield* new RestartContinuationMarkerError({ reason: "source_changed" });
      if ((yield* checkProviderBinding({ expectedBinding: input.expectedBinding, expectedEvidenceRevision: input.expectedEvidenceRevision,
          expectedRunId: input.sourceRunId, expectedRunAttemptId: input.sourceRunAttemptId })) !== null)
        return yield* new RestartContinuationMarkerError({ reason: "binding_changed" });
      if ((yield* effectOutbox.listHeldByThreadId(input.threadId)).length > 0)
        return yield* new RestartContinuationMarkerError({ reason: "unknown_effect" });
      return yield* markerFromRow(row);
    })).pipe(Effect.mapError((cause) => Schema.is(RestartContinuationMarkerError)(cause) ? cause : new EventSinkWriteError({ eventCount: 0, cause })));
    const readDormantRestartContinuations = sql<RestartMarkerRow>`SELECT * FROM orchestration_v2_restart_continuation_markers
      WHERE status = 'dormant' ORDER BY created_at, marker_id`.pipe(
      Effect.flatMap((rows) => Effect.forEach(rows, markerFromRow)), Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause })));
    const readReleasedRestartContinuation: EventSinkV2Shape["readReleasedRestartContinuation"] = (input) => sql.withTransaction(Effect.gen(function* () {
      const rows = yield* sql<RestartMarkerRow>`SELECT * FROM orchestration_v2_restart_continuation_markers WHERE status = 'released'
        AND effect_id = ${input.effectId} AND thread_id = ${input.threadId} AND source_run_id = ${input.sourceRunId}`;
      if (rows.length === 0) return null;
      if (rows.length !== 1) return yield* importSealFailure("Released continuation marker attribution is ambiguous");
      const effect = Option.getOrNull(yield* effectOutbox.get(input.effectId));
      if (effect === null || effect.threadId !== input.threadId || effect.request.type !== "provider-runtime.continue" ||
          effect.request.sourceRunId !== input.sourceRunId) return null;
      return yield* markerFromRow(rows[0]!);
    })).pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause })));
    const readCapturedRestartOriginEffect = Effect.fnUntraced(function* (commandId: CommandId) {
      const rows = yield* sql<{ readonly command_id: string; readonly thread_id: string; readonly effect_id: string;
        readonly marker_json: string; readonly canonical_command_json: string; readonly command_digest: string;
        readonly original_claim_json: string; readonly recorded_at: string }>`
        SELECT * FROM orchestration_v2_captured_restart_command_origins WHERE command_id = ${commandId}`;
      if (rows.length === 0) return null;
      const row = rows[0]!;
      const marker = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(RestartContinuationMarkerSchemaV2))(row.marker_json, { onExcessProperty: "error" });
      const command = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestrationV2Command))(row.canonical_command_json, { onExcessProperty: "error" });
      const originalClaim = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(CapturedRestartOriginalClaimSchemaV1))(row.original_claim_json, { onExcessProperty: "error" });
      yield* Schema.decodeUnknownEffect(CapturedRestartIsoTimestampV1)(row.recorded_at);
      const ids = capturedRestartContinuationIdsV1({ effectId: row.effect_id, marker });
      const receipt = Option.getOrNull(yield* commandReceipts.getByCommandId(commandId));
      const released = yield* readReleasedRestartContinuation({ effectId: row.effect_id, threadId: marker.threadId, sourceRunId: marker.sourceRunId });
      if (rows.length !== 1 || command.type !== "message.dispatch" || command.commandId !== commandId || commandId !== ids.commandId ||
          command.messageId !== ids.messageId || command.threadId !== marker.threadId || row.thread_id !== marker.threadId ||
          marker.binding.threadId !== marker.threadId || command.restartContinuationOfRunId !== marker.sourceRunId ||
          command.createdBy !== "agent" || command.creationSource !== "server" || command.dispatchMode.type !== "start_immediately" ||
          row.command_digest !== nativeCreationSha256(nativeCreationCanonicalJson(Schema.encodeSync(OrchestrationV2Command)(command))) ||
          receipt === null || receipt.threadId !== marker.threadId || receipt.commandType !== "message.dispatch" ||
          released === null || nativeCreationCanonicalJson(released) !== nativeCreationCanonicalJson(marker) ||
          !Number.isFinite(Date.parse(originalClaim.leaseExpiresAt)) || !Number.isFinite(Date.parse(row.recorded_at)))
        return yield* new NativeCommandPreconditionError({ commandId, reason: "identity_conflict" });
      return { commandId, threadId: marker.threadId, effectId: row.effect_id, marker, command, commandDigest: row.command_digest,
        originalClaim, recordedAt: row.recorded_at } satisfies CapturedRestartCommandOriginV1;
    });
    const validateCapturedRestartContextEffect = Effect.fnUntraced(function* (
      commandInput: Extract<OrchestrationV2Command, { readonly type: "message.dispatch" }>, context: CapturedRestartDispatchContextV2,
    ) {
      const command = yield* Schema.decodeUnknownEffect(OrchestrationV2Command)(commandInput, { onExcessProperty: "error" });
      const marker = yield* Schema.decodeUnknownEffect(RestartContinuationMarkerSchemaV2)(context.marker, { onExcessProperty: "error" });
      const fail = (reason: NativeCommandPreconditionError["reason"]) => new NativeCommandPreconditionError({ commandId: commandInput.commandId, reason });
      const ids = capturedRestartContinuationIdsV1({ effectId: context.effectId, marker });
      if (command.type !== "message.dispatch" || command.commandId !== ids.commandId || command.messageId !== ids.messageId ||
          command.threadId !== marker.threadId || marker.binding.threadId !== marker.threadId ||
          command.restartContinuationOfRunId !== marker.sourceRunId || command.createdBy !== "agent" || command.creationSource !== "server" ||
          command.dispatchMode.type !== "start_immediately" || context.workerId.length === 0 ||
          !Number.isSafeInteger(context.expectedAttempt) || context.expectedAttempt < 1) return yield* fail("identity_conflict");
      const now = DateTime.formatIso(yield* DateTime.now);
      const effect = Option.getOrNull(yield* effectOutbox.get(context.effectId));
      const released = yield* readReleasedRestartContinuation({ effectId: context.effectId, threadId: marker.threadId, sourceRunId: marker.sourceRunId });
      if (released === null || nativeCreationCanonicalJson(released) !== nativeCreationCanonicalJson(marker) || effect === null ||
          effect.status !== "running" || effect.leaseOwner !== context.workerId || effect.attemptCount !== context.expectedAttempt ||
          effect.leaseExpiresAt === null || !Number.isFinite(Date.parse(effect.leaseExpiresAt)) || Date.parse(effect.leaseExpiresAt) <= Date.parse(now) || effect.threadId !== marker.threadId ||
          effect.request.type !== "provider-runtime.continue" || effect.request.sourceRunId !== marker.sourceRunId ||
          effect.nativeCreationExecutionReference !== undefined || (yield* effectOutbox.listHeldByThreadId(marker.threadId)).length > 0)
        return yield* fail("unknown_evidence");
      const stopRows = yield* sql<{ readonly command_id: string }>`SELECT command_id FROM orchestration_v2_current_runtime_stop_intents
        WHERE thread_id = ${marker.threadId}`;
      for (const stopRow of stopRows) {
        const stopped = yield* readCurrentThreadRuntimeStopIntentEffect({ threadId: marker.threadId, commandId: CommandId.make(stopRow.command_id) });
        if (stopped === null) return yield* fail("unknown_evidence");
        if (stopped.affectedRunIds.includes(marker.sourceRunId)) return yield* fail("unknown_evidence");
        if (nativeCreationCanonicalJson(stopped.targetBinding) === nativeCreationCanonicalJson(marker.binding)) {
          const creation = yield* sql<{ readonly sequence: number }>`SELECT event.sequence FROM orchestration_events event
            JOIN orchestration_v2_projection_run_attempts attempt ON attempt.attempt_id = ${marker.sourceRunAttemptId}
            WHERE event.application_event_version = 2 AND event.aggregate_kind = 'thread' AND event.stream_id = ${marker.threadId}
              AND event.event_type = 'run-attempt.created' AND json_extract(event.payload_json, '$.id') = ${marker.sourceRunAttemptId}
              AND json_extract(event.payload_json, '$.runId') = ${marker.sourceRunId}
              AND json_extract(event.payload_json, '$.providerThreadId') = ${marker.binding.providerThreadId}
              AND attempt.thread_id = ${marker.threadId} AND attempt.run_id = ${marker.sourceRunId}
              AND json_extract(attempt.payload_json, '$.providerThreadId') = ${marker.binding.providerThreadId}`;
          if (creation.length !== 1 || stopped.stopEventSequence > creation[0]!.sequence) return yield* fail("unknown_evidence");
        }
        const exactBasis = stopped.queuedBases.find((basis) => basis.runId === marker.sourceRunId &&
          basis.runAttemptId === marker.sourceRunAttemptId && basis.sourceEvidenceRevision === marker.evidenceRevision &&
          nativeCreationCanonicalJson(basis.sourceBinding) === nativeCreationCanonicalJson(marker.binding));
        if (exactBasis !== undefined) {
          const fences = yield* readQueuedRunRuntimeStopFencesEffect({ threadId: marker.threadId, runId: marker.sourceRunId, incarnation: stopped.incarnation });
          if (fences.some((fence) => fence.stopCommandId === stopped.commandId && fence.basisDigest === exactBasis.basisDigest &&
              fence.executionIntent.runAttemptId === marker.sourceRunAttemptId && fence.sourceEvidenceRevision === marker.evidenceRevision &&
              nativeCreationCanonicalJson(fence.sourceBinding) === nativeCreationCanonicalJson(marker.binding))) return yield* fail("unknown_evidence");
        }
      }
      const commandDigest = nativeCreationSha256(nativeCreationCanonicalJson(Schema.encodeSync(OrchestrationV2Command)(command)));
      const previous = yield* readCapturedRestartOriginEffect(command.commandId);
      if (previous !== null && (previous.effectId !== context.effectId || previous.commandDigest !== commandDigest ||
          nativeCreationCanonicalJson(previous.marker) !== nativeCreationCanonicalJson(marker) ||
          nativeCreationCanonicalJson(previous.command) !== nativeCreationCanonicalJson(command))) return yield* fail("identity_conflict");
      if (previous === null && Option.isSome(yield* commandReceipts.getByCommandId(command.commandId))) return yield* fail("unbound_receipt");
      return { previous, origin: previous ?? { commandId: command.commandId, threadId: marker.threadId, effectId: context.effectId,
        marker, command, commandDigest, originalClaim: { workerId: context.workerId, expectedAttempt: context.expectedAttempt,
          leaseExpiresAt: effect.leaseExpiresAt }, recordedAt: now } satisfies CapturedRestartCommandOriginV1 };
    });
    const writeCapturedRestartOrigin = (origin: CapturedRestartCommandOriginV1) => sql`
      INSERT INTO orchestration_v2_captured_restart_command_origins
        (command_id, thread_id, effect_id, marker_json, canonical_command_json, command_digest, original_claim_json, recorded_at)
      VALUES (${origin.commandId}, ${origin.threadId}, ${origin.effectId}, ${nativeCreationCanonicalJson(origin.marker)},
        ${nativeCreationCanonicalJson(Schema.encodeSync(OrchestrationV2Command)(origin.command))}, ${origin.commandDigest},
        ${nativeCreationCanonicalJson(origin.originalClaim)}, ${origin.recordedAt})`;
    const clearRestartContinuation: EventSinkV2Shape["clearRestartContinuation"] = (marker) => withTransaction(Effect.gen(function* () {
      const rows = yield* sql<RestartMarkerRow>`SELECT * FROM orchestration_v2_restart_continuation_markers WHERE marker_id = ${marker.markerId}`;
      if (rows.length !== 1 || !markerMatches(rows[0]!, marker) || rows[0]!.status !== "dormant") return false;
      const updated = yield* sql`UPDATE orchestration_v2_restart_continuation_markers
        SET status = 'cleared', updated_at = ${DateTime.formatIso(yield* DateTime.now)}
        WHERE marker_id = ${marker.markerId} AND status = 'dormant' RETURNING marker_id`;
      return updated.length === 1;
    })).pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause })));
    const releaseRestartContinuation: EventSinkV2Shape["releaseRestartContinuation"] = (input) => withTransaction(Effect.gen(function* () {
      const marker = input.marker;
      const rows = yield* sql<RestartMarkerRow>`SELECT * FROM orchestration_v2_restart_continuation_markers WHERE marker_id = ${marker.markerId}`;
      if (rows.length !== 1 || !markerMatches(rows[0]!, marker) || rows[0]!.status !== "dormant") return false;
      if (input.currentSnapshot.threadId !== marker.threadId) return yield* new RestartContinuationMarkerError({ reason: "source_changed" });
      const snapshot = yield* readCommitSnapshot(marker.threadId, input.currentSnapshot.commandId, input.currentSnapshot.authority);
      if (nativeCreationCanonicalJson(snapshot) !== nativeCreationCanonicalJson(input.currentSnapshot))
        return yield* new RestartContinuationMarkerError({ reason: "source_changed" });
      const source = yield* sql`SELECT r.run_id FROM orchestration_v2_projection_runs r
        JOIN orchestration_v2_projection_run_attempts a ON a.attempt_id = ${marker.sourceRunAttemptId} AND a.run_id = r.run_id AND a.thread_id = r.thread_id
        JOIN orchestration_v2_projection_threads t ON t.thread_id = r.thread_id
        WHERE r.run_id = ${marker.sourceRunId} AND r.thread_id = ${marker.threadId} AND t.project_id = ${marker.projectId}
          AND a.provider_thread_id = ${marker.binding.providerThreadId}`;
      if (source.length !== 1) return yield* new RestartContinuationMarkerError({ reason: "source_changed" });
      if ((yield* effectOutbox.listHeldByThreadId(marker.threadId)).length > 0)
        return yield* new RestartContinuationMarkerError({ reason: "unknown_effect" });
      yield* input.revalidateAfterTrial.pipe(Effect.mapError(() => new RestartContinuationMarkerError({ reason: "qualification_unavailable" })));
      const effectId = `restart-continuation:${marker.markerId}`;
      yield* effectOutbox.enqueue([{ id: effectId, commandId: input.currentSnapshot.commandId, threadId: marker.threadId,
        request: { type: "provider-runtime.continue", sourceRunId: marker.sourceRunId } }]);
      yield* sql`UPDATE orchestration_v2_restart_continuation_markers SET status = 'released', effect_id = ${effectId},
        updated_at = ${DateTime.formatIso(yield* DateTime.now)} WHERE marker_id = ${marker.markerId} AND status = 'dormant'`;
      yield* afterCommit(effectOutbox.notifyAvailable());
      return true;
    })).pipe(Effect.mapError((cause) => Schema.is(RestartContinuationMarkerError)(cause) ? cause : new EventSinkWriteError({ eventCount: 0, cause })));

    // A user can answer after terminal normalization reads the pending request.
    // Recheck inside the write transaction so stale cleanup cannot erase answers.
    const guardUserInputCancellations = (events: ReadonlyArray<OrchestrationV2DomainEvent>) =>
      Effect.gen(function* () {
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

    const applyStoredEvents = (storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>) =>
      Effect.gen(function* () {
        yield* Effect.forEach(storedEvents, (stored) => Effect.gen(function* () {
          const event = stored.event;
          if (event.type === "thread.created" || event.type === "thread.metadata-updated") {
            const current = yield* sql<{ readonly payload_json: string }>`SELECT payload_json FROM orchestration_v2_projection_threads
              WHERE thread_id = ${event.threadId}`;
            const previous = current.length === 1 ? JSON.parse(current[0]!.payload_json) : null;
            if (event.type === "thread.created" || previous === null || previous.projectId !== event.payload.projectId || previous.worktreePath !== event.payload.worktreePath) {
              const project = Option.getOrNull(yield* projectStore.get(event.payload.projectId));
              if (project !== null) yield* assertDeletionWorktreePathWritable(NodePath.resolve(project.workspaceRoot, event.payload.worktreePath ?? "."));
            }
          }
          yield* projectionStore.apply(event);
        }), {
          concurrency: 1,
        });
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
      });

    const writeEffect = Effect.fn("orchestrationV2.EventSink.write")(function* (
      input: Parameters<EventSinkV2Shape["writeWithEffects"]>[0],
    ) {
      yield* assertPublicationScope;
      yield* Effect.annotateCurrentSpan({
        "orchestration_v2.command_id": input.commandId ?? null,
        "orchestration_v2.event_count": input.events.length,
        "orchestration_v2.thread_id": input.events[0]?.threadId ?? null,
      });

      const storedEvents = yield* sql.withTransaction(
        Effect.gen(function* () {
          const normalized = yield* normalizeEvents(
            input.guardPendingUserInputCancellations === true
              ? yield* guardUserInputCancellations(input.events)
              : input.events,
          );
          const committed = yield* eventStore.append({
            ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
            events: normalized,
          });
          yield* applyStoredEvents(committed);
          yield* effectOutbox.enqueue(input.effects);
          return committed;
        }),
      );
      const writtenEvents = yield* TransactionWrittenEvents;
      if (writtenEvents !== undefined) for (const stored of storedEvents) writtenEvents.set(stored.event.id, stored);
      if (input.effects.length > 0) {
        yield* afterCommit(effectOutbox.notifyAvailable(input.effects.length));
      } else if (storedEvents.some(({ event }) => event.type === "run.updated" || event.type === "run.created" || event.type === "message.updated")) {
        yield* afterCommit(effectOutbox.notifyAvailable());
      }
      yield* afterCommit(eventStore.publishCommitted(storedEvents));
      yield* afterCommit(publishLiveEvents(storedEvents));
      return storedEvents;
    });

    const writeIfRunCurrentEffect = Effect.fn("orchestrationV2.EventSink.writeIfRunCurrent")(
      function* (input: Parameters<EventSinkV2Shape["writeIfRunCurrent"]>[0]) {
      yield* assertPublicationScope;
        yield* Effect.annotateCurrentSpan({
          "orchestration_v2.command_id": input.commandId ?? null,
          "orchestration_v2.event_count": input.events.length,
          "orchestration_v2.run_id": input.runId,
          "orchestration_v2.thread_id": input.threadId,
        });

        const result = yield* sql.withTransaction(
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

            const normalized = yield* normalizeEvents(
              input.guardPendingUserInputCancellations === true
                ? yield* guardUserInputCancellations(input.events)
                : input.events,
            );
            const storedEvents = yield* eventStore.append({
              ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
              events: normalized,
            });
            yield* applyStoredEvents(storedEvents);
            return { committed: true as const, storedEvents };
          }),
        );
        if (result.committed) {
          yield* afterCommit(eventStore.publishCommitted(result.storedEvents));
          yield* afterCommit(publishLiveEvents(result.storedEvents));
        }
        return result;
      },
    );

    const writeIfProviderThreadOwnerEffect = Effect.fn(
      "orchestrationV2.EventSink.writeIfProviderThreadOwner",
    )(function* (input: Parameters<EventSinkV2Shape["writeIfProviderThreadOwner"]>[0]) {
      yield* assertPublicationScope;
      yield* Effect.annotateCurrentSpan({
        "orchestration_v2.command_id": input.commandId ?? null,
        "orchestration_v2.event_count": input.events.length,
        "orchestration_v2.provider_thread_id": input.providerThreadId,
        "orchestration_v2.run_id": input.runId,
        "orchestration_v2.active_attempt_id": input.activeAttemptId,
        "orchestration_v2.expected_last_run_ordinal": input.expectedLastRunOrdinal,
      });

      const result = yield* sql.withTransaction(
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

          const normalized = yield* normalizeEvents(
            input.guardPendingUserInputCancellations === true
              ? yield* guardUserInputCancellations(input.events)
              : input.events,
          );
          const storedEvents = yield* eventStore.append({
            ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
            events: normalized,
          });
          yield* applyStoredEvents(storedEvents);
          return { committed: true as const, storedEvents };
        }),
      );
      if (result.committed) {
        yield* afterCommit(eventStore.publishCommitted(result.storedEvents));
        yield* afterCommit(publishLiveEvents(result.storedEvents));
      }
      return result;
    });

    const existingCommandResult = (commandId: CommandId) =>
      Effect.gen(function* () {
        const existing = yield* commandReceipts.getByCommandId(commandId);
        if (Option.isNone(existing)) {
          return yield* Effect.die(
            new Error(`Command receipt ${commandId} disappeared during its transaction.`),
          );
        }
        const storedEvents = yield* eventStore.readByCommandId({ commandId }).pipe(
          Stream.runCollect,
          Effect.map((events): ReadonlyArray<OrchestrationV2StoredEvent> => Array.from(events)),
        );
        return { receipt: existing.value, storedEvents };
      });

    const validateNativeThreadRecovery = Effect.fnUntraced(function* (
      command: Extract<OrchestrationV2Command, { readonly type: "thread.delete" }>, context: NativeCreationThreadRecoveryContextV2,
    ) {
      const fail = (reason: NativeCommandPreconditionError["reason"]) => new NativeCommandPreconditionError({ commandId: command.commandId, reason });
      const issued = getNativeCreationThreadRecoveryReference(context);
      const reference = yield* nativeCreationRepository.readThreadRecoveryCommand(command.commandId);
      if (issued === null || reference === null || command.type !== "thread.delete" || issued.commandId !== command.commandId ||
          issued.threadId !== command.threadId || issued.claimId !== reference.claimId || issued.commandDigest !== reference.commandDigest ||
          issued.commandStartEffectId !== reference.commandStartEffectId || issued.cleanupStartEffectId !== reference.cleanupStartEffectId ||
          issued.recoveryScopeId !== reference.recoveryScopeId ||
          nativeCreationCanonicalJson(issued.incarnation) !== nativeCreationCanonicalJson(reference.resource.incarnation) ||
          nativeCreationCanonicalJson(command) !== nativeCreationCanonicalJson(reference.canonicalCommand)) return yield* fail("identity_conflict");
      const history = yield* nativeCreationRepository.readHistoryByClaim(reference.claimId);
      const start = history.effects.find((fact) => fact.effectId === reference.commandStartEffectId && fact.phase === "started");
      if (start === undefined || start.kind !== "native_command" || start.phase !== "started" || start.commandId !== command.commandId ||
          start.threadId !== command.threadId || start.commandType !== "thread.delete" || start.commandDigest !== reference.commandDigest)
        return yield* fail("unknown_evidence");
      const receipt = Option.getOrNull(yield* commandReceipts.getByCommandId(command.commandId));
      const completions = history.effects.filter((fact) => fact.effectId === reference.commandStartEffectId && fact.phase === "completed");
      if (receipt !== null) {
        const completion = completions[0];
        const attributed = yield* sql`SELECT event_id FROM orchestration_events WHERE command_id = ${command.commandId}
          AND application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ${command.threadId}
          AND event_type = 'thread.deleted' AND json_extract(payload_json, '$.id') = ${command.threadId}`;
        const final = completion?.kind === "native_command" && completion.phase === "completed" ? yield* sql`
          SELECT event_id FROM orchestration_events WHERE event_id = ${completion.eventId} AND sequence = ${completion.sequence}
            AND sequence = ${receipt.resultSequence} AND command_id = ${command.commandId} AND application_event_version = 2
            AND aggregate_kind = 'thread' AND stream_id = ${command.threadId}` : [];
        if (receipt.status !== "accepted" || receipt.commandType !== "thread.delete" || receipt.threadId !== command.threadId ||
            completions.length !== 1 || completion?.kind !== "native_command" || completion.phase !== "completed" ||
            completion.commandId !== command.commandId || completion.commandType !== "thread.delete" || completion.threadId !== command.threadId ||
            completion.commandDigest !== reference.commandDigest || completion.ordinal <= start.ordinal || attributed.length !== 1 || final.length !== 1)
          return yield* fail("unbound_receipt");
        return reference;
      }
      if (completions.length !== 0) return yield* fail("unknown_evidence");
      // The producer's earlier classification cannot fence a final release or foreign start before this deletion transaction.
      const finalCommandId = CommandId.make(history.intent.commandId);
      const effects = [...history.effects, ...history.effectsV2];
      const finalEvents = yield* sql`SELECT event_id FROM orchestration_events WHERE command_id = ${finalCommandId}`;
      if (history.intent.threadId !== command.threadId || history.effectOverflow ||
          Option.isSome(yield* commandReceipts.getByCommandId(finalCommandId)) || (yield* readIdentity(finalCommandId)) !== null ||
          finalEvents.length !== 0 || effects.some((fact) => fact.kind === "native_command" && fact.commandId === finalCommandId))
        return yield* fail("unknown_evidence");
      const cleanup = history.effects.find((fact) => fact.effectId === reference.cleanupStartEffectId && fact.phase === "started");
      if (cleanup?.kind !== "cleanup" || cleanup.phase !== "started" || cleanup.ordinal !== reference.cleanupStartOrdinal ||
          cleanup.ordinal >= start.ordinal || cleanup.recoveryScopeId !== reference.recoveryScopeId ||
          nativeCreationCanonicalJson(cleanup.resource) !== nativeCreationCanonicalJson(reference.resource) ||
          effects.some((fact) => fact.effectId === cleanup.effectId && fact.phase === "completed"))
        return yield* fail("unknown_evidence");
      const completionFields = new Set(["timestamp", "ordinal", "phase", "result", "eventId", "sequence", "exitCode", "terminalId", "ownership"]);
      for (const fact of effects) {
        if (fact.phase === "started" && (fact === start || fact === cleanup)) continue;
        const partners = effects.filter((candidate) => candidate.effectId === fact.effectId && candidate.phase !== fact.phase);
        const partner = partners[0];
        if (partners.length !== 1 || partner === undefined || partner.kind !== fact.kind)
          return yield* fail("unknown_evidence");
        const begin = fact.phase === "started" ? fact : partner;
        const end = fact.phase === "completed" ? fact : partner;
        if (end.ordinal <= begin.ordinal || ("result" in end && end.result === "unknown") ||
            Object.entries(begin).some(([key, value]) => !completionFields.has(key) &&
              nativeCreationCanonicalJson(value) !== nativeCreationCanonicalJson(Reflect.get(end, key))) ||
            (begin.kind === "setup" && end.kind === "setup" && begin.terminalId !== null && begin.terminalId !== end.terminalId))
          return yield* fail("unknown_evidence");
        if (fact.kind === "native_command" && fact.phase === "completed") {
          const attribution = yield* sql`SELECT event_id FROM orchestration_events WHERE event_id = ${fact.eventId}
            AND sequence = ${fact.sequence} AND command_id = ${fact.commandId} AND aggregate_kind = 'thread' AND stream_id = ${command.threadId}`;
          if (attribution.length !== 1) return yield* fail("unknown_evidence");
        }
      }
      const competing = yield* sql`SELECT effect_id AS id FROM orchestration_v2_effect_outbox WHERE thread_id = ${command.threadId}
        UNION ALL SELECT hold.effect_id FROM orchestration_v2_unknown_effect_holds hold
          JOIN orchestration_v2_effect_outbox effect ON effect.effect_id = hold.effect_id WHERE effect.thread_id = ${command.threadId}
        UNION ALL SELECT thread_id FROM orchestration_v2_provider_runtime_evidence WHERE thread_id = ${command.threadId}
        UNION ALL SELECT marker_id FROM orchestration_v2_restart_continuation_markers WHERE thread_id = ${command.threadId}
        UNION ALL SELECT command_id FROM orchestration_v2_thread_launch_workflows WHERE thread_id = ${command.threadId}
        UNION ALL SELECT thread_id FROM provider_session_runtime WHERE thread_id = ${command.threadId}`;
      if (competing.length !== 0) return yield* fail("unknown_evidence");
      const birth = yield* readIncarnation(command.threadId);
      const current = yield* sql`SELECT thread_id FROM orchestration_v2_projection_threads WHERE thread_id = ${command.threadId}
        AND json_extract(payload_json, '$.deletedAt') IS NULL`;
      if (birth.creationProvenance !== "native_created" || birth.incarnation === null || current.length !== 1 ||
          nativeCreationCanonicalJson(birth.incarnation) !== nativeCreationCanonicalJson(reference.resource.incarnation))
        return yield* fail("stale_target");
      const createCommandId = CommandId.make(`${history.intent.commandId}:native:v2:create`);
      const createReservation = yield* nativeCreationRepository.getReservedCommand(createCommandId);
      if (Option.isNone(createReservation)) return yield* fail("unknown_evidence");
      yield* nativeCreationRepository.validateCommandAcceptanceV2({ commandId: createCommandId, threadId: command.threadId,
        commandType: "thread.create", commandDigest: createReservation.value.commandDigest,
        bindingDigest: history.intent.bindingDigest, eventId: birth.incarnation.eventId, sequence: birth.incarnation.sequence });
      yield* authorizeNativeCreationThreadRecovery(context);
      return reference;
    });

    const ordinaryEventBasis = (events: ReadonlyArray<OrchestrationV2StoredEvent>, commandId: CommandId) => events.map((stored) => ({
      eventId: stored.event.id, sequence: stored.sequence, threadId: stored.event.threadId, commandId, eventType: stored.event.type,
    }));
    const ordinaryFailure = (capture: OrdinaryCheckout.OrdinaryCheckoutCaptureV1,
      reason: OrdinaryCheckout.OrdinaryCheckoutOwnershipError["reason"], message: string) =>
      new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({ reason, threadId: capture.threadId, path: capture.canonicalCheckoutPath, message });
    const ordinaryError = (cause: unknown): OrdinaryCheckoutCommitErrorV1 =>
      Schema.is(EventSinkV2Error)(cause) || Schema.is(WorktreeOwnershipConflictError)(cause) ||
        Schema.is(OrdinaryCheckout.OrdinaryCheckoutOwnershipError)(cause) ? cause : new EventSinkWriteError({ eventCount: 0, cause });
    const readOrdinaryCheckoutAdmissionsEffect = Effect.fnUntraced(function* (commandId: CommandId) {
      const rows = yield* sql<{ readonly admission_id: string; readonly command_id: string; readonly thread_id: string;
        readonly admission_sha256: string; readonly admission_json: string; readonly recorded_at: string }>`
        SELECT * FROM orchestration_v2_ordinary_checkout_admissions WHERE command_id = ${commandId} ORDER BY thread_id`;
      const admissions: OrdinaryCheckout.OrdinaryCheckoutAdmissionV1[] = [];
      for (const row of rows) {
        const admission = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(OrdinaryCheckout.OrdinaryCheckoutAdmissionV1))(
          row.admission_json, { onExcessProperty: "error" });
        const capture = admission.capture;
        const command = yield* Schema.decodeUnknownEffect(Schema.toType(OrchestrationV2Command))({ ...capture.canonicalCommand,
          ...(typeof capture.canonicalCommand.createdAt === "string" ? {
            createdAt: yield* Schema.decodeUnknownEffect(Schema.DateTimeUtcFromString)(capture.canonicalCommand.createdAt),
          } : {}),
        }, { onExcessProperty: "error" });
        const receipt = Option.getOrNull(yield* commandReceipts.getByCommandId(commandId));
        const events = yield* eventStore.readByCommandId({ commandId }).pipe(Stream.runCollect);
        const birth = yield* sql`SELECT event_id FROM orchestration_events WHERE application_event_version = 2
          AND aggregate_kind = 'thread' AND stream_id = ${capture.threadId} AND event_type = 'thread.created'
          AND event_id = ${capture.applicationBirth.eventId} AND sequence = ${capture.applicationBirth.sequence}
          AND json_extract(payload_json, '$.id') = ${capture.threadId} AND json_extract(payload_json, '$.projectId') = ${capture.projectId}`;
        const ref = OrdinaryCheckout.ordinaryCheckoutAdmissionRefV1(admission);
        if (row.command_id !== commandId || row.thread_id !== capture.threadId || row.admission_id !== admission.admissionId ||
            row.admission_sha256 !== ref.admissionSha256 || row.recorded_at !== DateTime.formatIso(admission.recordedAt) ||
            !OrdinaryCheckout.ordinaryCheckoutAdmissionMatchesV1(admission) || birth.length !== 1 ||
            nativeCreationCanonicalJson(Schema.encodeSync(OrchestrationV2Command)(command)) !== nativeCreationCanonicalJson(capture.canonicalCommand) ||
            receipt?.status !== "accepted" || nativeCreationCanonicalJson(Schema.encodeSync(OrdinaryCheckout.OrdinaryAcceptedReceiptV1)(admission.receipt)) !==
              nativeCreationCanonicalJson({ ...receipt, acceptedAt: DateTime.formatIso(receipt.acceptedAt) }) ||
            nativeCreationCanonicalJson(admission.eventBasis) !== nativeCreationCanonicalJson(ordinaryEventBasis(events, commandId)))
          return yield* ordinaryFailure(capture, "stale_admission", "Original checkout admission lost its command, receipt or event association");
        if (admission.run !== null) {
          const run = admission.run;
          const messages = events.filter((stored) => stored.event.type === "message.updated" && stored.event.threadId === capture.threadId &&
            stored.event.payload.id === run.messageId && stored.event.payload.role === "user" && stored.event.payload.runId === run.runId);
          const attempts = yield* sql`SELECT event_id FROM orchestration_events WHERE application_event_version = 2 AND aggregate_kind = 'thread'
            AND stream_id = ${capture.threadId} AND event_type = 'run-attempt.created' AND json_extract(payload_json, '$.id') = ${run.runAttemptId}
            AND json_extract(payload_json, '$.runId') = ${run.runId} AND json_extract(payload_json, '$.rootNodeId') = ${run.nodeId}`;
          if (messages.length !== 1 || attempts.length !== 1)
            return yield* ordinaryFailure(capture, "stale_admission", "Original checkout admission lost its accepted message or attempt");
        }
        admissions.push(admission);
      }
      return admissions;
    });
    const readOrdinaryCheckoutEffectLinkEffect = Effect.fnUntraced(function* (effectId: string) {
      const rows = yield* sql<{ readonly admission_id: string; readonly link_json: string; readonly recorded_at: string }>`
        SELECT * FROM orchestration_v2_ordinary_checkout_effect_links WHERE effect_id = ${effectId}`;
      if (rows.length === 0) return null;
      const row = rows[0]!;
      const link = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(OrdinaryCheckout.OrdinaryCheckoutEffectLinkV1))(
        row.link_json, { onExcessProperty: "error" });
      const owners = yield* sql<{ readonly command_id: string }>`SELECT command_id FROM orchestration_v2_ordinary_checkout_admissions
        WHERE admission_id = ${row.admission_id}`;
      const admission = owners.length === 1 ? (yield* readOrdinaryCheckoutAdmissionsEffect(CommandId.make(owners[0]!.command_id)))
        .find((item) => item.admissionId === row.admission_id) : undefined;
      const effect = Option.getOrNull(yield* effectOutbox.get(effectId));
      if (admission === undefined || rows.length !== 1 || link.effectId !== effectId || link.threadId !== admission.capture.threadId ||
          row.recorded_at !== DateTime.formatIso(link.recordedAt) ||
          nativeCreationCanonicalJson(link.admission) !== nativeCreationCanonicalJson(OrdinaryCheckout.ordinaryCheckoutAdmissionRefV1(admission)) ||
          effect === null || effect.commandId !== link.commandId || effect.threadId !== link.threadId ||
          link.requestSha256 !== nativeCreationSha256(nativeCreationCanonicalJson(Schema.encodeSync(EffectOutbox.OrchestrationEffectRequestV2)(effect.request))))
        return yield* new EventSinkWriteError({ eventCount: 0, cause: "Original checkout effect link lost its actual effect association" });
      return link;
    });
    const readOrdinaryCheckoutCommandLinksEffect = Effect.fnUntraced(function* (commandId: CommandId) {
      const rows = yield* sql<{ readonly effect_id: string }>`SELECT link.effect_id FROM orchestration_v2_ordinary_checkout_effect_links link
        JOIN orchestration_v2_effect_outbox effect ON effect.effect_id = link.effect_id WHERE effect.command_id = ${commandId} ORDER BY link.effect_id`;
      const links: OrdinaryCheckout.OrdinaryCheckoutEffectLinkV1[] = [];
      for (const row of rows) {
        const link = yield* readOrdinaryCheckoutEffectLinkEffect(row.effect_id);
        if (link === null) return yield* new EventSinkWriteError({ eventCount: 0, commandId, cause: "Ordinary command lost its effect link" });
        links.push(link);
      }
      return links;
    });
    const readOrdinaryCheckoutAdmissionForRunEffect = Effect.fnUntraced(function* (input: { readonly threadId: ThreadId; readonly runId: RunId }) {
      const rows = yield* sql<{ readonly command_id: string }>`SELECT admission.command_id FROM orchestration_v2_ordinary_checkout_admissions admission
        JOIN orchestration_events event ON event.command_id = admission.command_id AND event.stream_id = admission.thread_id
        WHERE admission.thread_id = ${input.threadId} AND json_extract(admission.admission_json, '$.run.runId') = ${input.runId}
          AND event.application_event_version = 2 AND event.aggregate_kind = 'thread' AND event.event_type = 'run.created'
          AND json_extract(event.payload_json, '$.id') = ${input.runId}`;
      if (rows.length === 0) return null;
      if (rows.length !== 1) return yield* new EventSinkWriteError({ eventCount: 0, cause: "Original checkout run admission is ambiguous" });
      const admission = (yield* readOrdinaryCheckoutAdmissionsEffect(CommandId.make(rows[0]!.command_id)))
        .find((item) => item.capture.threadId === input.threadId && item.run?.runId === input.runId);
      if (admission === undefined) return yield* new EventSinkWriteError({ eventCount: 0, cause: "Original checkout run admission is unavailable" });
      return admission;
    });
    const validateOrdinaryCheckoutCapture = Effect.fnUntraced(function* (capture: OrdinaryCheckout.OrdinaryCheckoutCaptureV1,
      source: OrdinaryCheckoutSqlCaptureV1["source"], use?: { readonly operationId: string; readonly requireLiveLease: boolean }) {
      const birth = yield* readApplicationBirthRecordEffect(capture.threadId);
      const local = yield* projectionStore.getThreadRecords(capture.threadId, []);
      const project = Option.getOrNull(yield* projectStore.get(capture.projectId));
      const rows = yield* sql`SELECT resource_path AS "resourcePath", lease_id AS "leaseId", owner_thread_id AS "ownerThreadId",
        owner_incarnation AS "ownerIncarnation", branch, acquired_at_ms AS "acquiredAtMs", renewed_at_ms AS "renewedAtMs", expires_at_ms AS "expiresAtMs"
        FROM worktree_ownership_leases WHERE resource_path = ${capture.canonicalCheckoutPath}`;
      const lease = rows.length === 1 ? yield* Schema.decodeUnknownEffect(OrdinaryCheckout.OrdinaryCheckoutLeaseV1)(rows[0]) : null;
      if (lease !== null && lease.ownerThreadId !== capture.threadId)
        return yield* new WorktreeOwnershipConflictError({ resourcePath: lease.resourcePath, ownerThreadId: lease.ownerThreadId,
          requestingThreadId: capture.threadId, ownerBranch: lease.branch, expiresAtMs: lease.expiresAtMs });
      if (!OrdinaryCheckout.ordinaryCheckoutCaptureMatchesV1(capture) || birth === null ||
          nativeCreationCanonicalJson(birth) !== nativeCreationCanonicalJson(capture.applicationBirth) || local.thread.deletedAt !== null ||
          local.thread.projectId !== capture.projectId || local.thread.branch !== capture.branch || project === null ||
          project.workspaceRoot !== source.projectWorkspaceRoot || local.thread.worktreePath !== source.worktreePath ||
          NodePath.resolve(capture.canonicalCheckoutPath) !== capture.canonicalCheckoutPath ||
          NodePath.resolve(capture.canonicalProjectRoot) !== capture.canonicalProjectRoot ||
          lease === null || nativeCreationCanonicalJson(OrdinaryCheckout.ordinaryCheckoutLeaseIdentityV1(lease)) !==
            nativeCreationCanonicalJson(OrdinaryCheckout.ordinaryCheckoutLeaseIdentityV1(capture.lease)))
        return yield* ordinaryFailure(capture, "target_changed", "Checkout birth, target or captured lease is no longer current");
      if (use === undefined) yield* assertDeletionWorktreePathWritable(capture.canonicalCheckoutPath);
      else {
        const barriers = yield* readDeletionWorktreePathAdmissionEffect({ path: capture.canonicalCheckoutPath });
        if (barriers.status === "unavailable" || barriers.reservations.length > 0 ||
            barriers.admissions.some((admission) => admission.operationId !== use.operationId))
          return yield* ordinaryFailure(capture, "unknown_use", "Another removal or native operation reserves this checkout");
        const now = DateTime.toEpochMillis(yield* DateTime.now);
        if (use.requireLiveLease && lease.expiresAtMs <= now)
          return yield* ordinaryFailure(capture, "target_changed", "Captured checkout lease is no longer live");
      }
      return lease;
    });
    const writeOrdinaryCheckoutAcceptance = Effect.fnUntraced(function* (input: Parameters<EventSinkV2Shape["commitCommand"]>[0],
      context: OrdinaryCheckoutCommitContextV1, receipt: CommandReceiptStore.CommandReceiptV2,
      events: ReadonlyArray<OrchestrationV2StoredEvent>, effects: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2>) {
      const contracts = yield* context.captureAfterProjection(events);
      if (contracts.length === 0 || new Set(contracts.map((item) => item.capture.threadId)).size !== contracts.length)
        return yield* new EventSinkWriteError({ commandId: input.commandId, eventCount: events.length, cause: "Ordinary acceptance has no unique captured subject" });
      const canonicalCommand = Schema.encodeSync(OrchestrationV2Command)(context.command);
      for (const contract of contracts) {
        const capture = yield* Schema.decodeUnknownEffect(OrdinaryCheckout.OrdinaryCheckoutCaptureV1)(contract.capture, { onExcessProperty: "error" });
        yield* validateOrdinaryCheckoutCapture(capture, contract.source);
        let admission: OrdinaryCheckout.OrdinaryCheckoutAdmissionV1;
        if (contract.originalAdmission !== null) {
          const ref = yield* Schema.decodeUnknownEffect(OrdinaryCheckout.OrdinaryCheckoutAdmissionRefV1)(contract.originalAdmission, { onExcessProperty: "error" });
          const original = (yield* readOrdinaryCheckoutAdmissionsEffect(capture.commandId)).find((item) => item.capture.threadId === capture.threadId);
          if (original === undefined || nativeCreationCanonicalJson(ref) !== nativeCreationCanonicalJson(OrdinaryCheckout.ordinaryCheckoutAdmissionRefV1(original)) ||
              nativeCreationCanonicalJson({ ...capture, lease: OrdinaryCheckout.ordinaryCheckoutLeaseIdentityV1(capture.lease) }) !==
                nativeCreationCanonicalJson({ ...original.capture, lease: OrdinaryCheckout.ordinaryCheckoutLeaseIdentityV1(original.capture.lease) }))
            return yield* ordinaryFailure(capture, "stale_admission", "Later acceptance does not retain its exact original checkout admission");
          admission = original;
        } else {
          if (nativeCreationCanonicalJson(capture.canonicalCommand) !== nativeCreationCanonicalJson(canonicalCommand))
            return yield* ordinaryFailure(capture, "stale_admission", "Checkout capture does not match the actual outer command");
          const subjectMessages = events.filter((stored) => stored.event.threadId === capture.threadId && stored.event.type === "message.updated" && stored.event.payload.role === "user");
          if (subjectMessages.length > 1) return yield* ordinaryFailure(capture, "unavailable", "Accepted checkout message association is ambiguous");
          const message = subjectMessages[0]?.event;
          let run: OrdinaryCheckout.OrdinaryAcceptedRunV1 | null = null;
          if (message?.type === "message.updated" && message.payload.runId !== null) {
            const local = yield* projectionStore.getThreadRecords(capture.threadId, ["runs", "attempts", "nodes"]);
            const currentRun = local.runs.find((item) => item.id === message.payload.runId);
            const attempt = currentRun === undefined ? undefined : local.attempts.find((item) => item.id === currentRun.activeAttemptId && item.runId === currentRun.id);
            const node = currentRun === undefined ? undefined : local.nodes.find((item) => item.id === currentRun.rootNodeId && item.runId === currentRun.id);
            if (currentRun === undefined || attempt === undefined || node === undefined || attempt.rootNodeId !== node.id)
              return yield* ordinaryFailure(capture, "unavailable", "Accepted checkout run has no exact attempt and root node");
            run = { runId: currentRun.id, runAttemptId: attempt.id, nodeId: node.id, messageId: message.payload.id };
          }
          admission = yield* Schema.decodeUnknownEffect(Schema.toType(OrdinaryCheckout.OrdinaryCheckoutAdmissionV1))({ version: 1,
            admissionId: OrdinaryCheckout.ordinaryCheckoutAdmissionIdV1(capture), capture, receipt,
            eventBasis: ordinaryEventBasis(events, input.commandId), run, recordedAt: input.acceptedAt });
          if (!OrdinaryCheckout.ordinaryCheckoutAdmissionMatchesV1(admission))
            return yield* ordinaryFailure(capture, "stale_admission", "Checkout admission does not match the accepted outer origin");
          const ref = OrdinaryCheckout.ordinaryCheckoutAdmissionRefV1(admission);
          yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_admissions
            (admission_id, command_id, thread_id, admission_sha256, admission_json, recorded_at)
            VALUES (${admission.admissionId}, ${capture.commandId}, ${capture.threadId}, ${ref.admissionSha256},
              ${nativeCreationCanonicalJson(Schema.encodeSync(OrdinaryCheckout.OrdinaryCheckoutAdmissionV1)(admission))}, ${DateTime.formatIso(input.acceptedAt)})`;
        }
        for (const effect of effects) {
          if (effect.threadId !== capture.threadId || !["provider-turn.start", "provider-turn.restart", "provider-turn.steer", "provider-thread.rollback",
              "provider-runtime.continue", "runtime-request.respond", "checkpoint.capture"].includes(effect.request.type)) continue;
          if (effect.commandId !== input.commandId)
            return yield* ordinaryFailure(capture, "stale_admission", "Checkout effect is not attributed to the accepted outer command");
          const link: OrdinaryCheckout.OrdinaryCheckoutEffectLinkV1 = { version: 1, effectId: effect.id, commandId: effect.commandId,
            threadId: effect.threadId, requestSha256: nativeCreationSha256(nativeCreationCanonicalJson(Schema.encodeSync(EffectOutbox.OrchestrationEffectRequestV2)(effect.request))),
            admission: OrdinaryCheckout.ordinaryCheckoutAdmissionRefV1(admission), recordedAt: input.acceptedAt };
          yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_effect_links (effect_id, admission_id, link_json, recorded_at)
            VALUES (${link.effectId}, ${admission.admissionId}, ${nativeCreationCanonicalJson(Schema.encodeSync(OrdinaryCheckout.OrdinaryCheckoutEffectLinkV1)(link))},
              ${DateTime.formatIso(input.acceptedAt)})`;
        }
      }
      const executing = effects.filter((effect) => ["provider-turn.start", "provider-turn.restart", "provider-turn.steer", "provider-thread.rollback",
        "provider-runtime.continue", "runtime-request.respond", "checkpoint.capture"].includes(effect.request.type));
      if (executing.some((effect) => !contracts.some((contract) => contract.capture.threadId === effect.threadId)))
        return yield* new EventSinkWriteError({ commandId: input.commandId, eventCount: events.length, cause: "Executing ordinary effect has no captured checkout subject" });
    });

    const captureOrdinaryPreparedLaunch = Effect.fnUntraced(function* (
      input: Parameters<EventSinkV2Shape["captureOrdinaryPreparedLaunch"]>[0],
    ) {
      return yield* sql.withTransaction(Effect.gen(function* () {
        const command = yield* Schema.decodeUnknownEffect(Schema.toType(OrchestrationV2Command))(input.command, { onExcessProperty: "error" });
        if (command.type !== "thread.create" && command.type !== "thread.metadata.update")
          return yield* new EventSinkWriteError({ eventCount: 0, cause: "Runless preparation has no accepted creation or empty-thread update" });
        const encoded = Schema.encodeSync(OrchestrationV2Command)(command);
        const preparation = yield* Schema.decodeUnknownEffect(OrdinaryCheckout.OrdinaryAcceptedEventV1)(input.preparationEvent, { onExcessProperty: "error" });
        const receipt = Option.getOrNull(yield* commandReceipts.getByCommandId(command.commandId));
        const events = yield* eventStore.readByCommandId({ commandId: command.commandId }).pipe(Stream.runCollect);
        const prepared = events.find((stored) => stored.event.id === preparation.eventId && stored.sequence === preparation.sequence);
        const identity = yield* readIdentity(command.commandId);
        const reservation = Option.getOrNull(yield* nativeCreationRepository.getReservedCommandIdentity(command.commandId));
        const local = yield* projectionStore.getThreadRecords(command.threadId, ["messages", "runs"]);
        if (receipt?.status !== "accepted" || receipt.threadId !== command.threadId || receipt.commandType !== command.type ||
            reservation !== null || identity !== null || local.messages.length > 0 || local.runs.length > 0 ||
            (command.type === "thread.metadata.update" && command.expectedEmpty !== true) ||
            prepared === undefined || preparation.commandId !== command.commandId || preparation.threadId !== command.threadId ||
            preparation.eventType !== prepared.event.type || prepared.event.threadId !== command.threadId ||
            prepared.event.type !== (command.type === "thread.create" ? "thread.created" : "thread.metadata-updated"))
          return yield* new EventSinkWriteError({ commandId: command.commandId, eventCount: 0, cause: "Original runless preparation association is unavailable" });
        const original = (yield* readOrdinaryCheckoutAdmissionsEffect(command.commandId)).find((item) => item.capture.threadId === command.threadId);
        if (original !== undefined) {
          if (original.run !== null || nativeCreationCanonicalJson(original.capture.canonicalCommand) !== nativeCreationCanonicalJson(encoded))
            return yield* ordinaryFailure(original.capture, "stale_admission", "Runless preparation differs from its original admission");
          return original;
        }
        const contract = yield* input.capture();
        if (contract.originalAdmission !== null || contract.capture.threadId !== command.threadId ||
            contract.capture.origin.kind !== "command" ||
            !((prepared.event.type === "thread.created" || prepared.event.type === "thread.metadata-updated") &&
              prepared.event.payload.projectId === contract.capture.projectId && prepared.event.payload.branch === contract.capture.branch &&
              prepared.event.payload.worktreePath === contract.source.worktreePath))
          return yield* ordinaryFailure(contract.capture, "stale_admission", "Runless capture does not retain its actual preparation target");
        yield* writeOrdinaryCheckoutAcceptance({ commandId: command.commandId, threadId: command.threadId,
          commandType: command.type, acceptedAt: receipt.acceptedAt, events: events.map((stored) => stored.event), effects: [] },
          { command, captureAfterProjection: () => Effect.succeed([contract]) }, receipt, events, []);
        const admission = (yield* readOrdinaryCheckoutAdmissionsEffect(command.commandId)).find((item) => item.capture.threadId === command.threadId);
        if (admission === undefined) return yield* new EventSinkWriteError({ commandId: command.commandId, eventCount: 0, cause: "Runless capture was not stored" });
        return admission;
      }));
    });
    const resolveOrdinaryCheckoutAdmission = Effect.fnUntraced(function* (ref: OrdinaryCheckout.OrdinaryCheckoutAdmissionRefV1) {
      const reference = yield* Schema.decodeUnknownEffect(OrdinaryCheckout.OrdinaryCheckoutAdmissionRefV1)(ref, { onExcessProperty: "error" });
      const rows = yield* sql<{ readonly command_id: string }>`SELECT command_id FROM orchestration_v2_ordinary_checkout_admissions
        WHERE admission_id = ${reference.admissionId}`;
      const original = rows.length === 1 ? (yield* readOrdinaryCheckoutAdmissionsEffect(CommandId.make(rows[0]!.command_id)))
        .find((item) => item.admissionId === reference.admissionId) : undefined;
      if (original === undefined || nativeCreationCanonicalJson(reference) !== nativeCreationCanonicalJson(OrdinaryCheckout.ordinaryCheckoutAdmissionRefV1(original)))
        return yield* new EventSinkWriteError({ eventCount: 0, cause: "Original checkout reference is unavailable" });
      return original;
    });
    const readOrdinaryCheckoutUseEffect = Effect.fnUntraced(function* (operationId: string) {
      const rows = yield* sql<{ readonly canonical_path: string; readonly kind: string; readonly subject_json: string;
        readonly state: OrdinaryCheckoutUseRecordV1["state"]; readonly started_at: string | null }>`
        SELECT canonical_path, kind, subject_json, state, started_at FROM orchestration_v2_worktree_path_admissions WHERE operation_id = ${operationId}`;
      if (rows.length === 0) return null;
      const row = rows[0]!;
      const subject = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(OrdinaryCheckoutUseSubjectV1))(row.subject_json, { onExcessProperty: "error" });
      const original = yield* resolveOrdinaryCheckoutAdmission(subject.use.admission);
      if (rows.length !== 1 || row.kind !== "native_operation" || subject.use.operationId !== operationId ||
          row.canonical_path !== original.capture.canonicalCheckoutPath ||
          nativeCreationCanonicalJson(OrdinaryCheckout.ordinaryCheckoutLeaseIdentityV1(subject.use.lease)) !==
            nativeCreationCanonicalJson(OrdinaryCheckout.ordinaryCheckoutLeaseIdentityV1(original.capture.lease)))
        return yield* ordinaryFailure(original.capture, "stale_admission", "Captured use lost its immutable path and lease association");
      return { subject, state: row.state, startedAt: row.started_at } satisfies OrdinaryCheckoutUseRecordV1;
    });
    const validateOrdinaryCheckoutUseSource = Effect.fnUntraced(function* (use: OrdinaryCheckout.OrdinaryCheckoutUseV1,
      admission: OrdinaryCheckout.OrdinaryCheckoutAdmissionV1, completion = false) {
      const source = use.source;
      const holds = yield* effectOutbox.listHeldByThreadId(admission.capture.threadId);
      if (holds.length > 0) return yield* ordinaryFailure(admission.capture, "unknown_use", "Unresolved native evidence prevents checkout use");
      if (source.kind === "outbox") {
        const link = yield* readOrdinaryCheckoutEffectLinkEffect(source.link.effectId);
        const effect = Option.getOrNull(yield* effectOutbox.get(source.link.effectId));
        const now = DateTime.formatIso(yield* DateTime.now);
        if (link === null || effect === null || source.link.effectId !== use.operationId ||
            nativeCreationCanonicalJson(Schema.encodeSync(OrdinaryCheckout.OrdinaryCheckoutEffectLinkV1)(link)) !==
              nativeCreationCanonicalJson(Schema.encodeSync(OrdinaryCheckout.OrdinaryCheckoutEffectLinkV1)(source.link)) ||
            nativeCreationCanonicalJson(link.admission) !== nativeCreationCanonicalJson(use.admission) ||
            !["provider-turn.start", "provider-turn.restart", "provider-turn.steer", "provider-thread.rollback", "provider-runtime.continue",
              "runtime-request.respond", "checkpoint.capture"].includes(effect.request.type) ||
            effect.attemptCount !== source.expectedAttempt ||
            (completion ? effect.status !== "succeeded" || effect.completedAt === null : effect.status !== "running" || effect.leaseOwner !== source.workerId ||
              effect.leaseExpiresAt === null || effect.leaseExpiresAt !== DateTime.formatIso(source.leaseExpiresAt) ||
              effect.leaseExpiresAt <= now))
          return yield* ordinaryFailure(admission.capture, "claim_mismatch", "Captured checkout use has no exact real effect claim or completion");
        return;
      }
      if (completion) return yield* ordinaryFailure(admission.capture, "unavailable", "Direct preparation completion requires its actual producer observation");
      if (nativeCreationCanonicalJson(source.admission) !== nativeCreationCanonicalJson(use.admission))
        return yield* ordinaryFailure(admission.capture, "stale_admission", "Direct preparation lost its original checkout reference");
      if (source.kind === "prepared_run") {
        const local = yield* projectionStore.getThreadRecords(admission.capture.threadId, ["runs", "attempts", "nodes", "messages"]);
        const run = local.runs.find((item) => item.id === source.preparation.runId);
        const attempt = local.attempts.find((item) => item.id === source.preparation.runAttemptId);
        const node = local.nodes.find((item) => item.id === source.preparation.nodeId);
        const message = local.messages.find((item) => item.id === source.preparation.messageId);
        if (admission.run === null || nativeCreationCanonicalJson(admission.run) !== nativeCreationCanonicalJson(source.preparation) ||
            run?.activeAttemptId !== source.preparation.runAttemptId || run.rootNodeId !== source.preparation.nodeId ||
            run.userMessageId !== source.preparation.messageId || attempt?.runId !== run.id || attempt.rootNodeId !== node?.id ||
            node?.runId !== run.id || message?.runId !== run.id || message.role !== "user")
          return yield* ordinaryFailure(admission.capture, "stale_admission", "Direct prepared run no longer matches its accepted run, attempt, node and message");
      } else if (admission.run !== null || source.preparationCommandId !== admission.capture.commandId ||
          !admission.eventBasis.some((event) => nativeCreationCanonicalJson(event) === nativeCreationCanonicalJson(source.preparationEvent)) ||
          source.preparationEvent.threadId !== admission.capture.threadId || source.preparationEvent.commandId !== admission.capture.commandId ||
          !["thread.created", "thread.metadata-updated"].includes(source.preparationEvent.eventType) ||
          nativeCreationCanonicalJson(source.applicationBirth) !== nativeCreationCanonicalJson(admission.capture.applicationBirth) ||
          source.projectId !== admission.capture.projectId || source.canonicalProjectRoot !== admission.capture.canonicalProjectRoot ||
          source.canonicalCheckoutPath !== admission.capture.canonicalCheckoutPath || source.branch !== admission.capture.branch ||
          !["thread.create", "thread.metadata.update"].includes(admission.capture.commandType))
        return yield* ordinaryFailure(admission.capture, "stale_admission", "Direct runless launch does not match its actual accepted preparation");
    });
    const exactOrdinaryCheckoutUse = Effect.fnUntraced(function* (input: OrdinaryCheckout.OrdinaryCheckoutUseV1) {
      const use = yield* Schema.decodeUnknownEffect(Schema.toType(OrdinaryCheckout.OrdinaryCheckoutUseV1))(input, { onExcessProperty: "error" });
      const record = yield* readOrdinaryCheckoutUseEffect(use.operationId);
      if (record === null || nativeCreationCanonicalJson(Schema.encodeSync(OrdinaryCheckout.OrdinaryCheckoutUseV1)(record.subject.use)) !==
          nativeCreationCanonicalJson(Schema.encodeSync(OrdinaryCheckout.OrdinaryCheckoutUseV1)(use)))
        return yield* new EventSinkWriteError({ eventCount: 0, cause: "Checkout use differs from its exact persisted subject" });
      return record;
    });
    const beginOrdinaryCheckoutUse = Effect.fnUntraced(function* (input: OrdinaryCheckoutUseInputV1) {
      return yield* sql.withTransaction(Effect.gen(function* () {
        const admission = yield* resolveOrdinaryCheckoutAdmission(input.admission);
        const subject = yield* Schema.decodeUnknownEffect(Schema.toType(OrdinaryCheckoutUseSubjectV1))({ schema: "t3.ordinary-checkout-use/v1",
          use: { version: 1, kind: "ordinary_checkout_use", operationId: input.operationId, admission: input.admission,
            source: input.source, lease: admission.capture.lease }, source: input.targetSource }, { onExcessProperty: "error" });
        const existing = yield* readOrdinaryCheckoutUseEffect(input.operationId);
        if (existing !== null) {
          if (nativeCreationCanonicalJson(Schema.encodeSync(OrdinaryCheckoutUseSubjectV1)(existing.subject)) !==
              nativeCreationCanonicalJson(Schema.encodeSync(OrdinaryCheckoutUseSubjectV1)(subject)))
            return yield* ordinaryFailure(admission.capture, "unknown_use", "Existing checkout operation has another original subject");
          return { status: "observe_only" as const, record: existing };
        }
        yield* validateOrdinaryCheckoutCapture(admission.capture, subject.source, { operationId: input.operationId, requireLiveLease: true });
        yield* validateOrdinaryCheckoutUseSource(subject.use, admission);
        const now = DateTime.formatIso(yield* DateTime.now);
        yield* sql`INSERT INTO orchestration_v2_worktree_path_admissions
          (operation_id, canonical_path, kind, subject_json, state, started_at, outcome_json, recorded_at, updated_at)
          VALUES (${input.operationId}, ${admission.capture.canonicalCheckoutPath}, 'native_operation',
            ${nativeCreationCanonicalJson(Schema.encodeSync(OrdinaryCheckoutUseSubjectV1)(subject))}, 'reserved', NULL, NULL, ${now}, ${now})`;
        return { status: "use_now" as const, record: { subject, state: "reserved" as const, startedAt: null } };
      }));
    });
    const revalidateOrdinaryCheckoutUse = Effect.fnUntraced(function* (use: OrdinaryCheckout.OrdinaryCheckoutUseV1) {
      return yield* sql.withTransaction(Effect.gen(function* () {
        const record = yield* exactOrdinaryCheckoutUse(use);
        const admission = yield* resolveOrdinaryCheckoutAdmission(use.admission);
        if (record.state !== "reserved" && record.state !== "started")
          return yield* ordinaryFailure(admission.capture, "unknown_use", "Checkout use is no longer an entered operation");
        yield* validateOrdinaryCheckoutCapture(admission.capture, record.subject.source, { operationId: use.operationId, requireLiveLease: true });
        yield* validateOrdinaryCheckoutUseSource(use, admission);
        if (record.state === "started") return record;
        const now = DateTime.formatIso(yield* DateTime.now);
        const changed = yield* sql`UPDATE orchestration_v2_worktree_path_admissions SET state = 'started', started_at = ${now}, updated_at = ${now}
          WHERE operation_id = ${use.operationId} AND state = 'reserved' AND started_at IS NULL RETURNING operation_id`;
        if (changed.length !== 1) return yield* ordinaryFailure(admission.capture, "unknown_use", "Checkout use changed before native entry");
        return { ...record, state: "started" as const, startedAt: now };
      }));
    });
    const settleOrdinaryCheckoutUse = Effect.fnUntraced(function* (use: OrdinaryCheckout.OrdinaryCheckoutUseV1, completion: boolean) {
      return yield* sql.withTransaction(Effect.gen(function* () {
        const record = yield* exactOrdinaryCheckoutUse(use);
        const admission = yield* resolveOrdinaryCheckoutAdmission(use.admission);
        if (record.state === (completion ? "released" : "no_effect")) return true;
        if (record.state !== (completion ? "started" : "reserved")) return false;
        yield* validateOrdinaryCheckoutCapture(admission.capture, record.subject.source, { operationId: use.operationId, requireLiveLease: false });
        yield* validateOrdinaryCheckoutUseSource(use, admission, completion);
        const now = DateTime.formatIso(yield* DateTime.now);
        const outcome = { schema: "t3.ordinary-checkout-outcome/v1", kind: completion ? "known_outbox_completion" : "unstarted_no_effect",
          operationId: use.operationId, admission: use.admission, effectId: use.source.kind === "outbox" ? use.source.link.effectId : null, observedAt: now };
        const changed = yield* sql`UPDATE orchestration_v2_worktree_path_admissions SET state = ${completion ? "completed" : "no_effect"},
          outcome_json = ${nativeCreationCanonicalJson(outcome)}, updated_at = ${now}
          WHERE operation_id = ${use.operationId} AND state = ${completion ? "started" : "reserved"} RETURNING operation_id`;
        if (changed.length !== 1) return false;
        if (completion) yield* sql`UPDATE orchestration_v2_worktree_path_admissions SET state = 'released', updated_at = ${now}
          WHERE operation_id = ${use.operationId} AND state = 'completed'`;
        return true;
      }));
    });
    const holdOrdinaryCheckoutUseUnknown = Effect.fnUntraced(function* (input: Parameters<EventSinkV2Shape["holdOrdinaryCheckoutUseUnknown"]>[0]) {
      return yield* sql.withTransaction(Effect.gen(function* () {
        const record = yield* exactOrdinaryCheckoutUse(input.use);
        const admission = yield* resolveOrdinaryCheckoutAdmission(input.use.admission);
        if (record.state === "unknown") return record;
        if ((record.state !== "reserved" && record.state !== "started") || input.reason.trim().length === 0)
          return yield* ordinaryFailure(admission.capture, "unknown_use", "Checkout operation cannot record unknown from this state");
        const now = DateTime.formatIso(yield* DateTime.now);
        yield* sql`UPDATE orchestration_v2_worktree_path_admissions SET state = 'unknown', updated_at = ${now},
          outcome_json = ${nativeCreationCanonicalJson({ schema: "t3.ordinary-checkout-outcome/v1", kind: "unknown",
            operationId: input.use.operationId, admission: input.use.admission, reason: input.reason, observedAt: now })}
          WHERE operation_id = ${input.use.operationId} AND state = ${record.state}`;
        return { ...record, state: "unknown" as const };
      }));
    });

    const validateNativeCreationReservation = Effect.fnUntraced(function* (input: {
      readonly commandId: CommandId; readonly threadId: ThreadId; readonly commandType: string;
      readonly identity: NativeCommandIdentityV2 | null; readonly context: NativeCommandCommitContextV2 | undefined;
    }) {
      const reservation = Option.getOrNull(yield* nativeCreationRepository.getReservedCommandIdentity(input.commandId));
      if (reservation === null) return null;
      const fail = () => new NativeCommandPreconditionError({ commandId: input.commandId, reason: "identity_conflict" });
      const { identity, context } = input;
      if (context === undefined || identity?.kind !== "native_creation_stage" || reservation.threadId !== input.threadId ||
          identity.commandId !== input.commandId || identity.commandType !== input.commandType || identity.aggregateId !== input.threadId ||
          context.snapshot.commandId !== input.commandId || context.snapshot.threadId !== input.threadId ||
          context.snapshot.authority.claimId !== reservation.claimId)
        return yield* fail();
      const body = Option.getOrNull(yield* nativeCreationRepository.getReservedCommand(input.commandId));
      const history = yield* nativeCreationRepository.readHistoryByClaim(reservation.claimId);
      if (body === null || body.claimId !== reservation.claimId || body.threadId !== input.threadId || body.commandType !== input.commandType ||
          body.commandDigest !== identity.normalizedCommandDigest || history.intent.bindingDigest !== identity.bindingDigest ||
          context.snapshot.authority.actorSessionId !== history.intent.actorSessionId)
        return yield* fail();
      return context;
    });

    const commitCommandEffect = Effect.fn("orchestrationV2.EventSink.commitCommand")(function* (
      input: Parameters<EventSinkV2Shape["commitCommand"]>[0],
    ) {
      yield* assertPublicationScope;
      const result = yield* sql.withTransaction(
        Effect.gen(function* () {
          const failPrecondition = (reason: NativeCommandPreconditionError["reason"]) => new NativeCommandPreconditionError({ commandId: input.commandId, reason });
          const ordinaryContext = input.ordinaryCheckoutContext;
          if (ordinaryContext !== undefined)
            yield* sql`UPDATE worktree_ownership_leases SET lease_id = lease_id WHERE 0`;
          const priorOrdinary = yield* readOrdinaryCheckoutAdmissionsEffect(input.commandId);
          const priorOrdinaryLinks = yield* readOrdinaryCheckoutCommandLinksEffect(input.commandId);
          if ((priorOrdinary.length > 0 || priorOrdinaryLinks.length > 0) && ordinaryContext === undefined) return yield* failPrecondition("identity_conflict");
          if (ordinaryContext !== undefined) {
            const command = yield* Schema.decodeUnknownEffect(Schema.toType(OrchestrationV2Command))(ordinaryContext.command, { onExcessProperty: "error" });
            const encoded = Schema.encodeSync(OrchestrationV2Command)(command);
            if (command.commandId !== input.commandId || command.type !== input.commandType ||
                Reflect.get(encoded, command.type === "delegated_task.request" ? "parentThreadId" : "threadId") !== input.threadId ||
                (input.nativeContext !== undefined && input.nativeContext.identity.kind !== "guarded_message_dispatch") ||
                input.recoveryContext !== undefined || input.stopContext !== undefined ||
                priorOrdinary.some((admission) => nativeCreationCanonicalJson(admission.capture.canonicalCommand) !== nativeCreationCanonicalJson(encoded)))
              return yield* failPrecondition("identity_conflict");
          }
          const deletionCommand = input.commandType === "thread.delete" ? yield* Schema.decodeUnknownEffect(OrchestrationV2Command)(
            input.deletionCommand ?? { type: "thread.delete", commandId: input.commandId, threadId: input.threadId }, { onExcessProperty: "error" }) : null;
          if ((input.deletionCommand !== undefined || input.deletionWorktreePolicy !== undefined) && input.commandType !== "thread.delete")
            return yield* failPrecondition("identity_conflict");
          if (deletionCommand !== null && (deletionCommand.type !== "thread.delete" || deletionCommand.commandId !== input.commandId || deletionCommand.threadId !== input.threadId))
            return yield* failPrecondition("identity_conflict");
          const originalDeletion = yield* readThreadDeletionCommandEffect(input.commandId);
          if (originalDeletion !== null && (deletionCommand === null ||
              nativeCreationCanonicalJson(originalDeletion.command) !== nativeCreationCanonicalJson(deletionCommand))) return yield* failPrecondition("identity_conflict");
          const reservedRecovery = yield* nativeCreationRepository.readThreadRecoveryCommand(input.commandId);
          if (reservedRecovery !== null && input.recoveryContext === undefined) return yield* failPrecondition("identity_conflict");
          if (input.recoveryContext !== undefined && (input.commandType !== "thread.delete" || input.nativeContext !== undefined ||
              input.stopContext !== undefined || input.capturedRestartContext !== undefined ||
              input.events.some((event) => event.threadId !== input.threadId) ||
              input.effects.some((effect) => effect.threadId !== input.threadId || effect.commandId !== input.commandId)))
            return yield* failPrecondition("identity_conflict");
          const recovery = input.recoveryContext === undefined ? null : yield* validateNativeThreadRecovery(
            { type: "thread.delete", commandId: input.commandId, threadId: input.threadId }, input.recoveryContext);
          if (recovery !== null && deletionCommand?.type === "thread.delete" && deletionCommand.worktreeRemoval !== undefined)
            return yield* failPrecondition("identity_conflict");
          const previousRestart = yield* readCapturedRestartOriginEffect(input.commandId);
          const restartContext = input.capturedRestartContext;
          if (previousRestart !== null && restartContext === undefined) return yield* failPrecondition("identity_conflict");
          if (restartContext !== undefined && (input.commandType !== "message.dispatch" || restartContext.command.commandId !== input.commandId ||
              restartContext.command.threadId !== input.threadId || input.nativeContext !== undefined || input.stopContext !== undefined ||
              input.events.some((event) => event.threadId !== input.threadId) ||
              input.effects.some((effect) => effect.threadId !== input.threadId || effect.commandId !== input.commandId)))
            return yield* failPrecondition("identity_conflict");
          const restart = restartContext === undefined ? null : yield* validateCapturedRestartContextEffect(restartContext.command, restartContext);
          const previousStop = yield* readCurrentThreadRuntimeStopIntentEffect({ threadId: input.threadId, commandId: input.commandId });
          const stop = input.stopContext;
          if (previousStop !== null && (stop === undefined || previousStop.canonicalRequestDigest !== stop.canonicalRequestDigest ||
              previousStop.actorBindingDigest !== stop.actorBindingDigest ||
              nativeCreationCanonicalJson(previousStop.incarnation) !== nativeCreationCanonicalJson(stop.incarnation) ||
              nativeCreationCanonicalJson(previousStop.targetBinding) !== nativeCreationCanonicalJson(stop.targetBinding) ||
              previousStop.targetEvidenceRevision !== stop.targetEvidenceRevision ||
              nativeCreationCanonicalJson(previousStop.queuedBases) !== nativeCreationCanonicalJson(stop.queuedBases) ||
              nativeCreationCanonicalJson(previousStop.affectedRunIds) !== nativeCreationCanonicalJson(stop.affectedRunIds))) return yield* failPrecondition("identity_conflict");
          const imported = yield* sql<{ readonly command_digest: string }>`SELECT command_digest FROM orchestration_v2_imported_history_start_choices WHERE command_id = ${input.commandId}`;
          if (imported.length > 0 && (input.commandType !== "thread.imported-history.start" ||
              (yield* ImportedHistoryReservation) !== imported[0]!.command_digest)) return yield* failPrecondition("identity_conflict");
          const currentIdentity = yield* readIdentity(input.commandId);
          const context = input.nativeContext;
          const identity = context === undefined ? null : yield* Schema.decodeUnknownEffect(NativeCommandIdentityV2)(context.identity);
          yield* validateNativeCreationReservation({ commandId: input.commandId, threadId: input.threadId,
            commandType: input.commandType, identity, context });
          const witness = context?.workstreamWitness === undefined ? null :
            yield* Schema.decodeUnknownEffect(NativeWorkstreamSettlementWitnessV2)(context.workstreamWitness, { onExcessProperty: "error" });
          const previousWitness = yield* readWorkstreamWitness(input.commandId);
          if ((identity?.kind === "workstream_settlement" && witness === null) ||
              (witness !== null && (identity?.kind !== "workstream_settlement" ||
                (witness.command.type !== "thread.settle" && witness.command.type !== "thread.unsettle") ||
                witness.command.commandId !== input.commandId || witness.command.threadId !== input.threadId || witness.command.type !== input.commandType ||
                nativeCreationSha256(nativeCreationCanonicalJson(witness.command)) !== identity.normalizedCommandDigest ||
                nativeWorkstreamSettlementWitnessBindingDigestV2(witness) !== identity.bindingDigest)) ||
              (previousWitness !== null && (witness === null || nativeCreationCanonicalJson(previousWitness) !== nativeCreationCanonicalJson(witness))))
            return yield* failPrecondition("identity_conflict");
          if (identity !== null && (identity.commandId !== input.commandId || identity.commandType !== input.commandType || identity.aggregateId !== input.threadId))
            return yield* failPrecondition("identity_conflict");
          if (identity !== null && (input.events.some((event) => event.threadId !== input.threadId) ||
              input.effects.some((effect) => effect.threadId !== input.threadId || effect.commandId !== input.commandId)))
            return yield* failPrecondition("identity_conflict");
          if (currentIdentity !== null && (identity === null || nativeCreationCanonicalJson(currentIdentity) !== nativeCreationCanonicalJson(identity)))
            return yield* failPrecondition("identity_conflict");
          const priorReceipt = yield* commandReceipts.getByCommandId(input.commandId);
          if (Option.isSome(priorReceipt)) {
            if (priorReceipt.value.threadId !== input.threadId || priorReceipt.value.commandType !== input.commandType)
              return yield* failPrecondition("identity_conflict");
            if (identity !== null && currentIdentity === null) return yield* failPrecondition("unbound_receipt");
            if ((stop !== undefined && previousStop === null) || (witness !== null && previousWitness === null)) return yield* failPrecondition("unbound_receipt");
            if (deletionCommand !== null && originalDeletion === null) return yield* failPrecondition("unbound_receipt");
            if (ordinaryContext !== undefined && priorOrdinary.length === 0 && priorOrdinaryLinks.length === 0) return yield* failPrecondition("unbound_receipt");
            const existing = yield* existingCommandResult(input.commandId);
            return { ...existing, committed: false as const, cancelledEffectIds: [] };
          }
          if (recovery !== null && input.events.filter((event) => event.type === "thread.deleted" && event.payload.id === input.threadId).length !== 1)
            return yield* failPrecondition("identity_conflict");
          if (restart !== null) {
            const command = restart.origin.command;
            const messages = input.events.filter((event) => event.type === "message.updated" && event.payload.id === command.messageId);
            const message = messages[0];
            if (messages.length !== 1 || message?.type !== "message.updated" || message.payload.threadId !== command.threadId ||
                message.payload.role !== "user" || message.payload.text !== command.text ||
                nativeCreationCanonicalJson(message.payload.attachments) !== nativeCreationCanonicalJson(command.attachments) ||
                nativeCreationCanonicalJson(message.payload.context ?? null) !== nativeCreationCanonicalJson(command.context ?? null))
              return yield* failPrecondition("identity_conflict");
          }
          if (witness !== null) {
            const event = input.events.at(-1);
            if (input.effects.length !== 0 || event === undefined ||
                event.type !== (witness.command.type === "thread.settle" ? "thread.settled" : "thread.unsettled") ||
                !("id" in event.payload) || event.payload.id !== input.threadId) return yield* failPrecondition("identity_conflict");
          }
          if (context !== undefined) {
            if (context.snapshot.threadId !== input.threadId || context.snapshot.commandId !== input.commandId) return yield* failPrecondition("identity_conflict");
            const current = yield* readCommitSnapshot(input.threadId, input.commandId, context.snapshot.authority);
            const creating = identity?.kind === "native_creation_stage" && input.commandType === "thread.create";
            if (!creating && current.incarnation === null) return yield* failPrecondition("missing_target");
            if (nativeCreationCanonicalJson(current) !== nativeCreationCanonicalJson(context.snapshot))
              return yield* failPrecondition("stale_target");
            yield* context.revalidateAuthority.pipe(Effect.mapError(() => failPrecondition("authority_changed")));
            if (witness !== null) {
              if (nativeCreationCanonicalJson(witness.incarnation) !== nativeCreationCanonicalJson(current.incarnation) ||
                  witness.targetEventSequence !== current.targetEventSequence || context.snapshot.authority.actorSessionId !== witness.actorSessionId)
                return yield* failPrecondition("stale_target");
              const owner = yield* readCurrentProviderRuntimeOwnerEffect(input.threadId);
              const absent = yield* sql`SELECT thread_id FROM orchestration_v2_projection_threads WHERE thread_id = ${input.threadId}
                AND json_extract(payload_json, '$.activeProviderThreadId') IS NULL`;
              const attempts = yield* sql`SELECT native_command_id FROM workstreams_native_attempts
                WHERE owner_id = ${witness.attemptKey.owner_id} AND principal_id = ${witness.attemptKey.principal_id}
                  AND command_id = ${witness.attemptKey.command_id} AND native_command_id = ${input.commandId}
                  AND dispatch_started_at = ${witness.dispatchStartedAt} AND enrollment_sha256 = ${witness.enrollmentSha256}
                  AND request_bytes_sha256 = ${witness.requestBytesSha256}`;
              if (attempts.length !== 1 || (witness.provider === null ? owner !== null || current.records.runtime_evidence!.length > 0 || absent.length !== 1 :
                  owner === null || owner.evidenceRevision !== witness.provider.evidenceRevision ||
                  nativeCreationCanonicalJson(owner.binding) !== nativeCreationCanonicalJson(witness.provider.binding)))
                return yield* failPrecondition("unknown_evidence");
            }
          }
          if (stop !== undefined) {
            const requestEvent = input.events[0];
            if (input.commandType !== "provider-session.detach" || input.events.length !== 1 || input.effects.some((effect) =>
                effect.commandId !== input.commandId || effect.threadId !== input.threadId || effect.request.type !== "provider-session.detach" ||
                effect.request.providerSessionId !== stop.targetBinding.providerSessionId) ||
                requestEvent?.type !== "provider-session.detach-requested" || requestEvent.threadId !== input.threadId ||
                requestEvent.payload.providerSessionId !== stop.targetBinding.providerSessionId)
              return yield* failPrecondition("identity_conflict");
            const current = yield* readCommitSnapshot(input.threadId, input.commandId, stop.snapshot.authority);
            const owner = yield* readCurrentProviderRuntimeOwnerEffect(input.threadId);
            if (stop.snapshot.commandId !== input.commandId || stop.snapshot.threadId !== input.threadId ||
                nativeCreationCanonicalJson(current) !== nativeCreationCanonicalJson(stop.snapshot) ||
                nativeCreationCanonicalJson(yield* readApplicationThreadBirth(input.threadId)) !== nativeCreationCanonicalJson(stop.incarnation) ||
                owner === null || owner.evidenceRevision !== stop.targetEvidenceRevision ||
                nativeCreationCanonicalJson(owner.binding) !== nativeCreationCanonicalJson(stop.targetBinding)) return yield* failPrecondition("stale_target");
            yield* stop.revalidateCurrentTarget.pipe(Effect.mapError(() => failPrecondition("stale_target")));
            yield* Schema.decodeUnknownEffect(LowerSha256)(stop.canonicalRequestDigest);
            yield* Schema.decodeUnknownEffect(LowerSha256)(stop.actorBindingDigest);
            const bases = yield* Schema.decodeUnknownEffect(Schema.Array(ContinuationBasisSchemaV2))(stop.queuedBases);
            if (new Set(bases.map((basis) => basis.runId)).size !== bases.length || new Set(stop.affectedRunIds).size !== stop.affectedRunIds.length ||
                bases.some((basis) => basis.basisDigest !== queuedRunContinuationBasisDigestV2(basis)) || stop.affectedRunIds.some((runId) => {
                  const basis = bases.find((basis) => basis.runId === runId);
                  return basis === undefined || (basis.sourceMode !== "queued_thread" && basis.sourceMode !== "active_native_copy") ||
                    basis.executionIntent === null || basis.queuedProviderThreadId === null ||
                    basis.sourceEvidenceRevision !== stop.targetEvidenceRevision ||
                    nativeCreationCanonicalJson(basis.sourceBinding) !== nativeCreationCanonicalJson(stop.targetBinding);
                })) return yield* failPrecondition("unknown_evidence");
          }
          const cleanupProviderTasks: Array<{ readonly effectId: string; readonly lease: WorktreeOwnershipLease | null;
            readonly absentPath: string | null;
            readonly ownerBirth: ApplicationThreadBirthV2; readonly task: Extract<LeaseCleanupTaskV2, { readonly kind: "provider" }> }> = [];
          const deletionEvent = input.events.find((event) => event.type === "thread.deleted" && event.payload.id === input.threadId);
          if (deletionCommand !== null && (deletionCommand.type !== "thread.delete" || deletionEvent?.type !== "thread.deleted" ||
              input.events.filter((event) => event.type === "thread.deleted" && event.payload.id === input.threadId).length !== 1))
            return yield* failPrecondition("identity_conflict");
          const deletionCapture = deletionCommand?.type === "thread.delete" && deletionEvent?.type === "thread.deleted" ?
            yield* captureDeletionWorktreeInventory(deletionCommand, deletionEvent, input.effects, input.deletionWorktreePolicy) : null;
          if (deletionCapture !== null && deletionCapture.inventory.worktree.path !== null) {
            const admission = yield* readDeletionWorktreePathAdmissionEffect({ path: deletionCapture.inventory.worktree.path });
            if (admission.status !== "available") deletionCapture.inventory = { ...deletionCapture.inventory,
              captureStatus: "retained", reason: "existing_worktree_path_admission" };
          }
          const worktreeEffect = deletionCommand?.type === "thread.delete" && deletionCapture?.inventory.request !== undefined ? {
            id: deletionWorktreeEffectIdV1(input.commandId, input.threadId), commandId: input.commandId, threadId: input.threadId,
            request: { type: "worktree.cleanup" as const },
          } : null;
          if (input.effects.some((effect) => effect.request.type === "worktree.cleanup" &&
              (worktreeEffect === null || effect.id !== worktreeEffect.id || effect.commandId !== input.commandId || effect.threadId !== input.threadId)))
            return yield* failPrecondition("identity_conflict");
          const committedEffects = worktreeEffect === null || input.effects.some((effect) => effect.id === worktreeEffect.id) ? input.effects : [...input.effects, worktreeEffect];
          if (deletionEvent?.type === "thread.deleted") {
            const ownerBirth = yield* readApplicationBirthRecordEffect(input.threadId);
            const owner = yield* readCurrentProviderRuntimeOwnerEffect(input.threadId);
            if (ownerBirth !== null && owner !== null) {
              const leases = yield* sql`SELECT resource_path AS "resourcePath", lease_id AS "leaseId", owner_thread_id AS "ownerThreadId",
                owner_incarnation AS "ownerIncarnation", branch, acquired_at_ms AS "acquiredAtMs", renewed_at_ms AS "renewedAtMs", expires_at_ms AS "expiresAtMs"
                FROM worktree_ownership_leases WHERE owner_thread_id = ${input.threadId}`;
              for (const row of leases) {
                const lease = yield* Schema.decodeUnknownEffect(CleanupLeaseSchemaV2)(row);
                if (lease.ownerIncarnation !== JSON.stringify(["t3.orchestration-v2.thread-birth/v1", ownerBirth.eventId, ownerBirth.sequence]) ||
                    lease.resourcePath !== deletionEvent.payload.worktreePath || (lease.branch !== null && lease.branch !== deletionEvent.payload.branch)) continue;
                const task = yield* Schema.decodeUnknownEffect(LeaseCleanupTaskV2)({ kind: "provider", expectedBinding: owner.binding,
                  evidenceRevision: owner.evidenceRevision });
                if (task.kind !== "provider") return yield* failPrecondition("unknown_evidence");
                for (const effect of input.effects) if (effect.request.type === "provider-session.detach" &&
                    effect.request.providerSessionId === owner.binding.providerSessionId && effect.threadId === input.threadId)
                  cleanupProviderTasks.push({ effectId: effect.id, lease, absentPath: null, ownerBirth, task });
              }
              if (deletionCapture?.inventory.captureStatus === "captured" && deletionCapture.inventory.leaseInventory.status === "absent" &&
                  deletionCapture.inventory.worktree.path !== null &&
                  nativeCreationCanonicalJson(deletionCapture.ownerBirth) === nativeCreationCanonicalJson(ownerBirth)) {
                const task = yield* Schema.decodeUnknownEffect(LeaseCleanupTaskV2)({ kind: "provider", expectedBinding: owner.binding,
                  evidenceRevision: owner.evidenceRevision });
                if (task.kind !== "provider") return yield* failPrecondition("unknown_evidence");
                for (const effect of input.effects) if (effect.request.type === "provider-session.detach" &&
                    effect.request.providerSessionId === owner.binding.providerSessionId && effect.threadId === input.threadId)
                  cleanupProviderTasks.push({ effectId: effect.id, lease: null, absentPath: deletionCapture.inventory.worktree.path, ownerBirth, task });
              }
            }
          }
          const reserved = yield* commandReceipts.insertIfAbsent({
            commandId: input.commandId,
            threadId: input.threadId,
            commandType: input.commandType,
            acceptedAt: input.acceptedAt,
            resultSequence: 0,
            status: "accepted",
            error: null,
          });
          if (!reserved) {
            const existing = yield* existingCommandResult(input.commandId);
            return { ...existing, committed: false as const, cancelledEffectIds: [] };
          }

          if (identity !== null) yield* sql`INSERT INTO orchestration_v2_native_command_identities
            (command_id, kind, version, command_type, aggregate_kind, aggregate_id, normalized_command_digest, binding_digest)
            VALUES (${identity.commandId}, ${identity.kind}, 2, ${identity.commandType}, 'thread', ${identity.aggregateId},
              ${identity.normalizedCommandDigest}, ${identity.bindingDigest})`;
          const normalized = yield* normalizeEvents(input.events);
          const storedEvents = yield* eventStore.append({
            commandId: input.commandId,
            events: normalized,
          });
          const sequence = storedEvents.at(-1)?.sequence;
          if (sequence === undefined) {
            return yield* Effect.die(
              new Error(`Command ${input.commandId} produced no orchestration events.`),
            );
          }
          yield* effectOutbox.enqueue(committedEffects);
          const receipt: CommandReceiptStore.CommandReceiptV2 = {
            commandId: input.commandId,
            threadId: input.threadId,
            commandType: input.commandType,
            acceptedAt: input.acceptedAt,
            resultSequence: sequence,
            status: "accepted",
            error: null,
          };
          yield* commandReceipts.upsert(receipt);
          if (deletionCapture !== null && deletionCommand?.type === "thread.delete") {
            const deletion = storedEvents.find((item) => item.event.type === "thread.deleted" && item.event.threadId === input.threadId)!;
            const association = { commandId: input.commandId, eventId: deletion.event.id, sequence: deletion.sequence };
            const recordedAt = DateTime.formatIso(input.acceptedAt);
            const encodedCommand = nativeCreationCanonicalJson(Schema.encodeSync(OrchestrationV2Command)(deletionCommand));
            const commandDigest = nativeCreationSha256(encodedCommand);
            yield* sql`INSERT INTO orchestration_v2_thread_deletion_commands
              (command_id, thread_id, canonical_command_json, command_digest, owner_birth_json, worktree_inventory_json,
                deletion_event_id, deletion_event_sequence, recorded_at)
              VALUES (${input.commandId}, ${input.threadId}, ${encodedCommand}, ${commandDigest},
                ${nativeCreationCanonicalJson({ birth: deletionCapture.ownerBirth })}, ${nativeCreationCanonicalJson(deletionCapture.inventory)},
                ${association.eventId}, ${association.sequence}, ${recordedAt})`;
            if (worktreeEffect !== null && deletionCapture.inventory.request !== undefined) {
              const { leaseInventory, ...inventory } = deletionCapture.inventory;
              const subject = { version: 1 as const, effectId: worktreeEffect.id, threadId: input.threadId, leaseInventory,
                ownerBirth: deletionCapture.ownerBirth, deletion: association, task: { kind: "worktree" as const,
                  canonicalCommand: deletionCommand, commandDigest,
                  ...(deletionCommand.worktreeRemoval === undefined ? {} : { consent: deletionCommand.worktreeRemoval }), ...inventory } };
              const binding = yield* Schema.decodeUnknownEffect(DeletionWorktreeTaskBindingV1)({ ...subject,
                bindingSha256: deletionWorktreeTaskBindingDigestV1(subject), recordedAt }, { onExcessProperty: "error" });
              yield* sql`INSERT INTO orchestration_v2_lease_cleanup_task_bindings
                (effect_id, thread_id, lease_json, owner_birth_json, deletion_json, task_json, binding_sha256, recorded_at)
                VALUES (${binding.effectId}, ${binding.threadId}, ${nativeCreationCanonicalJson(binding.leaseInventory)},
                  ${nativeCreationCanonicalJson({ birth: binding.ownerBirth })}, ${nativeCreationCanonicalJson(binding.deletion)},
                  ${nativeCreationCanonicalJson(binding.task)}, ${binding.bindingSha256}, ${binding.recordedAt})`;
              if (binding.task.worktree.path !== null) {
                const existing = yield* sql`SELECT operation_id FROM orchestration_v2_worktree_path_admissions
                  WHERE canonical_path = ${binding.task.worktree.path} AND state NOT IN ('no_effect', 'released')`;
                if (existing.length === 0) yield* sql`INSERT INTO orchestration_v2_worktree_path_admissions
                  (operation_id, canonical_path, kind, subject_json, state, started_at, outcome_json, recorded_at, updated_at)
                  VALUES (${binding.effectId}, ${binding.task.worktree.path}, 'worktree_removal',
                    ${nativeCreationCanonicalJson({ version: 1, effectId: binding.effectId, threadId: binding.threadId,
                      commandId: input.commandId, bindingSha256: binding.bindingSha256 })}, 'reserved', NULL, NULL, ${recordedAt}, ${recordedAt})`;
              }
            }
          }
          yield* applyStoredEvents(storedEvents);
          if (ordinaryContext !== undefined)
            yield* writeOrdinaryCheckoutAcceptance(input, ordinaryContext, receipt, storedEvents, committedEffects);
          if (cleanupProviderTasks.length > 0) {
            const deletion = storedEvents.find((item) => item.event.type === "thread.deleted" && item.event.threadId === input.threadId)!;
            for (const captured of cleanupProviderTasks) {
              const common = { effectId: captured.effectId, threadId: input.threadId, ownerBirth: captured.ownerBirth,
                deletion: { commandId: input.commandId, eventId: deletion.event.id, sequence: deletion.sequence }, task: captured.task };
              if (captured.lease !== null) {
                const subject = { version: 2 as const, ...common, lease: captured.lease };
                yield* writeLeaseCleanupTaskBinding({ ...subject, bindingSha256: leaseCleanupTaskBindingDigestV2(subject), recordedAt: DateTime.formatIso(input.acceptedAt) });
              } else if (captured.absentPath !== null) {
                const subject = { version: 1 as const, ...common, leaseInventory: { status: "absent" as const, resourcePath: captured.absentPath } };
                yield* writeUnleasedDeletionCleanupTaskBinding({ ...subject, bindingSha256: deletionCleanupTaskBindingDigestV1(subject), recordedAt: DateTime.formatIso(input.acceptedAt) });
              }
            }
          }
          if (recovery !== null) {
            const final = storedEvents.at(-1)!;
            yield* nativeCreationRepository.completeEffect(recovery.claimId, { kind: "native_command", phase: "completed",
              effectId: recovery.commandStartEffectId, timestamp: DateTime.formatIso(input.acceptedAt), commandId: input.commandId,
              threadId: input.threadId, commandType: "thread.delete", commandDigest: recovery.commandDigest,
              eventId: final.event.id, sequence: final.sequence });
          }
          if (restart !== null) yield* writeCapturedRestartOrigin(restart.origin);
          if (witness !== null) yield* sql`INSERT INTO orchestration_v2_workstream_settlement_witnesses
            (command_id, thread_id, witness_json, recorded_at) VALUES (${input.commandId}, ${input.threadId},
              ${yield* Schema.encodeEffect(Schema.fromJsonString(NativeWorkstreamSettlementWitnessV2))(witness)}, ${DateTime.formatIso(input.acceptedAt)})`;
          if (stop !== undefined) {
            const stopEvent = storedEvents.at(-1)!;
            const intent = yield* Schema.decodeUnknownEffect(StopIntentSchemaV2)({ commandId: input.commandId, threadId: input.threadId,
              incarnation: stop.incarnation, canonicalRequestDigest: stop.canonicalRequestDigest, actorBindingDigest: stop.actorBindingDigest,
              targetBinding: stop.targetBinding, targetEvidenceRevision: stop.targetEvidenceRevision,
              stopEventId: stopEvent.event.id, stopEventSequence: stopEvent.sequence, affectedRunIds: stop.affectedRunIds, queuedBases: stop.queuedBases });
            yield* sql`INSERT INTO orchestration_v2_current_runtime_stop_intents
              (command_id, thread_id, application_birth_json, canonical_request_digest, actor_binding_digest, target_binding_json,
                target_evidence_revision, stop_event_id, stop_event_sequence, affected_run_ids_json, queued_bases_json, accepted_at)
              VALUES (${intent.commandId}, ${intent.threadId}, ${nativeCreationCanonicalJson(intent.incarnation)},
                ${intent.canonicalRequestDigest}, ${intent.actorBindingDigest}, ${nativeCreationCanonicalJson(intent.targetBinding)},
                ${intent.targetEvidenceRevision}, ${intent.stopEventId}, ${intent.stopEventSequence},
                ${nativeCreationCanonicalJson(intent.affectedRunIds)}, ${nativeCreationCanonicalJson(intent.queuedBases)}, ${DateTime.formatIso(input.acceptedAt)})`;
            for (const basis of intent.queuedBases.filter((basis) => intent.affectedRunIds.includes(basis.runId))) {
              yield* sql`INSERT INTO orchestration_v2_queued_runtime_stop_fences
                (stop_command_id, run_id, thread_id, application_birth_json, queued_provider_thread_id, source_binding_json,
                  source_evidence_revision, switch_plan_json, source_mode, execution_intent_json, basis_digest)
                VALUES (${intent.commandId}, ${basis.runId}, ${intent.threadId}, ${nativeCreationCanonicalJson(intent.incarnation)},
                  ${basis.queuedProviderThreadId}, ${nativeCreationCanonicalJson(basis.sourceBinding)}, ${basis.sourceEvidenceRevision},
                  ${basis.switchPlan === null ? null : nativeCreationCanonicalJson(basis.switchPlan)}, ${basis.sourceMode},
                  ${nativeCreationCanonicalJson(basis.executionIntent)}, ${basis.basisDigest})`;
            }
          }
          if (identity?.kind === "native_creation_stage" &&
              (input.commandType === "thread.create" || input.commandType === "message.dispatch")) {
            const acceptedEvent = storedEvents.at(-1)!;
            yield* nativeCreationRepository.validateCommandAcceptanceV2({ commandId: input.commandId,
              threadId: input.threadId, commandType: input.commandType,
              commandDigest: identity.normalizedCommandDigest, bindingDigest: identity.bindingDigest,
              eventId: acceptedEvent.event.id, sequence: acceptedEvent.sequence });
          }
          const cancelledEffectIds =
            input.cancelUnsettledEffects === undefined
              ? []
              : stop === undefined ? yield* effectOutbox.cancelUnsettled({
                  threadId: input.threadId,
                  ...input.cancelUnsettledEffects,
                }) : (yield* sql<{ readonly effect_id: string }>`UPDATE orchestration_v2_effect_outbox
                  SET status = 'cancelled', completed_at = ${DateTime.formatIso(input.acceptedAt)}, updated_at = ${DateTime.formatIso(input.acceptedAt)},
                    last_error = ${input.cancelUnsettledEffects.reason}
                  WHERE thread_id = ${input.threadId} AND status = 'pending'
                    AND effect_id IN ${sql.in(stop.queuedBases.filter((basis) => stop.affectedRunIds.includes(basis.runId))
                      .flatMap((basis) => basis.executionIntent === null ? [] : [basis.executionIntent.effectId]))}
                    AND effect_type IN ${sql.in(input.cancelUnsettledEffects.effectTypes)}
                    AND NOT EXISTS (SELECT 1 FROM orchestration_v2_unknown_effect_holds hold
                      WHERE hold.effect_id = orchestration_v2_effect_outbox.effect_id) RETURNING effect_id`).map((row) => row.effect_id);
          return { receipt, storedEvents, committed: true as const, cancelledEffectIds };
        }),
      );
      yield* afterCommit(effectOutbox.signalCancellations(result.cancelledEffectIds));
      if (result.committed && (input.effects.length > 0 || input.deletionCommand?.worktreeRemoval !== undefined)) {
        yield* afterCommit(effectOutbox.notifyAvailable(input.effects.length + (input.deletionCommand?.worktreeRemoval === undefined ? 0 : 1)));
      } else if (result.committed && result.storedEvents.some(({ event }) => event.type === "run.updated" || event.type === "run.created" || event.type === "message.updated")) {
        yield* afterCommit(effectOutbox.notifyAvailable());
      }
      if (result.committed) {
        yield* afterCommit(eventStore.publishCommitted(result.storedEvents));
        yield* afterCommit(publishLiveEvents(result.storedEvents));
      }
      return {
        receipt: result.receipt,
        storedEvents: result.storedEvents,
        committed: result.committed,
        cancelledEffectCount: result.cancelledEffectIds.length,
      };
    });

    const commitRejectedCommandEffect = Effect.fn(
      "orchestrationV2.EventSink.commitRejectedCommand",
    )(function* (input: Parameters<EventSinkV2Shape["commitRejectedCommand"]>[0]) {
      yield* assertPublicationScope;
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          if ((yield* readOrdinaryCheckoutAdmissionsEffect(input.commandId)).length > 0 ||
              (yield* readOrdinaryCheckoutCommandLinksEffect(input.commandId)).length > 0)
            return yield* new NativeCommandPreconditionError({ commandId: input.commandId, reason: "identity_conflict" });
          if ((yield* nativeCreationRepository.readThreadRecoveryCommand(input.commandId)) !== null)
            return yield* new NativeCommandPreconditionError({ commandId: input.commandId, reason: "identity_conflict" });
          const previousRestart = yield* readCapturedRestartOriginEffect(input.commandId);
          const restartContext = input.capturedRestartContext;
          if (previousRestart !== null && restartContext === undefined) return yield* new NativeCommandPreconditionError({ commandId: input.commandId, reason: "identity_conflict" });
          if (restartContext !== undefined && (input.commandType !== "message.dispatch" || restartContext.command.commandId !== input.commandId ||
              restartContext.command.threadId !== input.threadId || input.nativeIdentity !== undefined || input.workstreamWitness !== undefined))
            return yield* new NativeCommandPreconditionError({ commandId: input.commandId, reason: "identity_conflict" });
          const restart = restartContext === undefined ? null : yield* validateCapturedRestartContextEffect(restartContext.command, restartContext);
          const stops = yield* sql`SELECT command_id FROM orchestration_v2_current_runtime_stop_intents WHERE command_id = ${input.commandId}`;
          if (stops.length > 0) return yield* new NativeCommandPreconditionError({ commandId: input.commandId, reason: "identity_conflict" });
          const imported = yield* sql<{ readonly command_digest: string }>`SELECT command_digest FROM orchestration_v2_imported_history_start_choices WHERE command_id = ${input.commandId}`;
          if (imported.length > 0 && (input.commandType !== "thread.imported-history.start" ||
              (yield* ImportedHistoryReservation) !== imported[0]!.command_digest))
            return yield* new NativeCommandPreconditionError({ commandId: input.commandId, reason: "identity_conflict" });
          const currentIdentity = yield* readIdentity(input.commandId);
          const context = input.nativeContext;
          const identityInput = input.nativeIdentity ?? context?.identity;
          const identity = identityInput === undefined ? null : yield* Schema.decodeUnknownEffect(NativeCommandIdentityV2)(identityInput);
          if (context !== undefined && (identity?.kind !== "native_creation_stage" ||
              nativeCreationCanonicalJson(context.identity) !== nativeCreationCanonicalJson(identity)))
            return yield* new NativeCommandPreconditionError({ commandId: input.commandId, reason: "identity_conflict" });
          const reservedContext = yield* validateNativeCreationReservation({ commandId: input.commandId, threadId: input.threadId,
            commandType: input.commandType, identity, context });
          const witness = input.workstreamWitness === undefined ? null :
            yield* Schema.decodeUnknownEffect(NativeWorkstreamSettlementWitnessV2)(input.workstreamWitness, { onExcessProperty: "error" });
          const previousWitness = yield* readWorkstreamWitness(input.commandId);
          if ((identity?.kind === "workstream_settlement" && witness === null) || (witness !== null &&
              (identity?.kind !== "workstream_settlement" || (witness.command.type !== "thread.settle" && witness.command.type !== "thread.unsettle") ||
                witness.command.commandId !== input.commandId || witness.command.threadId !== input.threadId || witness.command.type !== input.commandType ||
                nativeCreationSha256(nativeCreationCanonicalJson(witness.command)) !== identity.normalizedCommandDigest ||
                nativeWorkstreamSettlementWitnessBindingDigestV2(witness) !== identity.bindingDigest)) ||
              (previousWitness !== null && (witness === null || nativeCreationCanonicalJson(previousWitness) !== nativeCreationCanonicalJson(witness))))
            return yield* new NativeCommandPreconditionError({ commandId: input.commandId, reason: "identity_conflict" });
          if ((identity !== null && (identity.commandId !== input.commandId || identity.commandType !== input.commandType || identity.aggregateId !== input.threadId)) ||
              (currentIdentity !== null && (identity === null || nativeCreationCanonicalJson(currentIdentity) !== nativeCreationCanonicalJson(identity))))
            return yield* new NativeCommandPreconditionError({ commandId: input.commandId, reason: "identity_conflict" });
          const previous = yield* commandReceipts.getByCommandId(input.commandId);
          if (Option.isSome(previous)) {
            if (previous.value.threadId !== input.threadId || previous.value.commandType !== input.commandType)
              return yield* new NativeCommandPreconditionError({ commandId: input.commandId, reason: "identity_conflict" });
            if (identity !== null && currentIdentity === null)
              return yield* new NativeCommandPreconditionError({ commandId: input.commandId, reason: "unbound_receipt" });
            if (witness !== null && previousWitness === null)
              return yield* new NativeCommandPreconditionError({ commandId: input.commandId, reason: "unbound_receipt" });
            return previous.value;
          }
          if (reservedContext !== null) yield* reservedContext.revalidateAuthority.pipe(Effect.mapError(() =>
            new NativeCommandPreconditionError({ commandId: input.commandId, reason: "authority_changed" })));
          const sequence = yield* eventStore.latestSequence({ threadId: input.threadId });
          if (witness !== null) {
            const attempts = yield* sql`SELECT native_command_id FROM workstreams_native_attempts
              WHERE owner_id = ${witness.attemptKey.owner_id} AND principal_id = ${witness.attemptKey.principal_id}
                AND command_id = ${witness.attemptKey.command_id} AND native_command_id = ${input.commandId}
                AND dispatch_started_at = ${witness.dispatchStartedAt} AND enrollment_sha256 = ${witness.enrollmentSha256}
                AND request_bytes_sha256 = ${witness.requestBytesSha256}`;
            if (attempts.length !== 1) return yield* new NativeCommandPreconditionError({ commandId: input.commandId, reason: "unknown_evidence" });
          }
          const receipt: CommandReceiptStore.CommandReceiptV2 = {
            commandId: input.commandId,
            threadId: input.threadId,
            commandType: input.commandType,
            acceptedAt: input.rejectedAt,
            resultSequence: sequence,
            status: "rejected",
            error: input.error,
          };
          const inserted = yield* commandReceipts.insertIfAbsent(receipt);
          if (inserted) {
            if (restart !== null) yield* writeCapturedRestartOrigin(restart.origin);
            if (identity !== null) yield* sql`INSERT INTO orchestration_v2_native_command_identities
              (command_id, kind, version, command_type, aggregate_kind, aggregate_id, normalized_command_digest, binding_digest)
              VALUES (${identity.commandId}, ${identity.kind}, 2, ${identity.commandType}, 'thread', ${identity.aggregateId},
                ${identity.normalizedCommandDigest}, ${identity.bindingDigest})`;
            if (witness !== null) yield* sql`INSERT INTO orchestration_v2_workstream_settlement_witnesses
              (command_id, thread_id, witness_json, recorded_at) VALUES (${input.commandId}, ${input.threadId},
                ${yield* Schema.encodeEffect(Schema.fromJsonString(NativeWorkstreamSettlementWitnessV2))(witness)}, ${DateTime.formatIso(input.rejectedAt)})`;
            return receipt;
          }
          return yield* new NativeCommandPreconditionError({ commandId: input.commandId, reason: "identity_conflict" });
        }),
      );
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
      yield* assertPublicationScope;
        const result = yield* sql.withTransaction(
          Effect.gen(function* () {
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
            if ("workspaceRoot" in input.event.payload && typeof input.event.payload.workspaceRoot === "string") {
              const current = Option.getOrNull(yield* projectStore.get(input.projectId, { includeDeleted: true }));
              if (current === null || current.workspaceRoot !== input.event.payload.workspaceRoot)
                yield* assertDeletionWorktreePathWritable(NodePath.resolve(input.event.payload.workspaceRoot));
            }
            const event = yield* eventStore.appendProjectEvent(input.event);
            yield* projectStore.apply(event);
            const receipt = { ...reserved, resultSequence: event.sequence };
            yield* commandReceipts.upsert(receipt);
            return { receipt, event };
          }),
        );
        if (result.event !== undefined) {
          yield* afterCommit(eventStore.publishCommitted([result.event]));
        }
        return { receipt: result.receipt, committed: result.event !== undefined };
      },
    );

    const commitRejectedProjectCommandEffect = Effect.fn(
      "orchestrationV2.EventSink.commitRejectedProjectCommand",
    )(function* (input: Parameters<EventSinkV2Shape["commitRejectedProjectCommand"]>[0]) {
      yield* assertPublicationScope;
      return yield* sql.withTransaction(
        Effect.gen(function* () {
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
                const current = Stream.fromIterable(events);
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
              project: (stored) => ({ ...stored, event: projectDomainEventForWire(stored.event) }),
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

    const readImportedHistoryStartReviewEffect = Effect.fnUntraced(function* (input: ImportedHistoryStartReviewContextV2) {
      const delivery = yield* Schema.decodeUnknownEffect(OrchestrationV2ImportedHistoryDelivery)(input.delivery, { onExcessProperty: "error" });
      const snapshot = yield* readCommitSnapshot(input.threadId, CommandId.make(`imported-history-review:${input.threadId}`), { actorSessionId: input.actorSessionId });
      const target = delivery.type === "message" ? { type: "message" as const, messageId: delivery.messageId }
        : { type: "queued_run" as const, runId: delivery.runId, messageId: delivery.messageId };
      const local = snapshot.records.threads?.length === 1 ? yield* projectionStore.getThreadRecords(input.threadId,
        ["runs", "messages", "providerThreads"]).pipe(Effect.option) : Option.none();
      const disposition = yield* readLegacyContinuationDisposition(input.threadId);
      const nativeSeal = yield* readNativeImportTranscriptSeal(input.threadId);
      const legacy = input.readLegacyTranscript === undefined ? null :
        Option.getOrNull(yield* input.readLegacyTranscript.pipe(Effect.option));
      const thread = Option.isSome(local) ? local.value.thread : null;
      const queued = delivery.type === "queued_run" && Option.isSome(local)
        ? local.value.runs.find((run) => run.id === delivery.runId && run.userMessageId === delivery.messageId) : undefined;
      const selected = delivery.type === "message" ? delivery.modelSelection ?? thread?.modelSelection : queued?.modelSelection;
      const descriptor = selected === undefined ? null : Option.getOrNull(yield* input.readTargetCapability(selected.instanceId).pipe(Effect.option));
      const capability = descriptor !== null && descriptor.instanceId === selected?.instanceId && descriptor.enabled === true &&
        descriptor.declared?.canConsumeHandoffSummaries === true && descriptor.declared.supportsFullThreadHandoff === true &&
        descriptor.declared.supportsProviderSwitchingViaHandoff === true;
      const imported = thread?.historyOrigin === "v1_import" && disposition !== null;
      const legacyMarker = snapshot.records.legacy_import_markers?.[0];
      const sourceThread = snapshot.records.legacy_source_threads?.[0];
      const legacyPositive = disposition?.provenance === "legacy_row" && legacy !== null && legacy.version === 1 &&
        legacy.policy === "legacy_user_assistant_rows_v1" && legacy.threadId === input.threadId && legacy.messageCount > 0 &&
        legacy.eventBasis.length === legacy.messageCount * 2 && legacy.sourceUpdatedAt === sourceThread?.updated_at &&
        legacyMarker?.transcript_imported_at !== null && legacyMarker?.last_error === null &&
        legacyMarker?.imported_message_count === legacy.messageCount;
      const nativePositive = disposition?.provenance === "native_import" && nativeSeal !== null;
      const exactDelivery = delivery.type === "message" ? thread !== null && thread.deletedAt === null :
        queued !== undefined && queued.status === "queued" && queued.queueHeld === true && Option.isSome(local) &&
        local.value.messages.some((message) => message.id === delivery.messageId && message.runId === delivery.runId && message.role === "user");
      const unresolvedCreation = yield* sql`SELECT started.effect_id FROM native_creation_effect_facts started
        JOIN native_creation_intents claim ON claim.claim_id = started.claim_id
        WHERE claim.thread_id = ${input.threadId} AND started.phase = 'started' AND NOT EXISTS (
          SELECT 1 FROM native_creation_effect_facts completed WHERE completed.claim_id = started.claim_id
            AND completed.effect_id = started.effect_id AND completed.phase = 'completed')`;
      const uncertain = (snapshot.records.unknown_effect_holds?.length ?? 0) > 0 || unresolvedCreation.length > 0 ||
        (snapshot.records.effects ?? []).some((effect) => effect.status === "running" &&
          ["provider-turn.start", "provider-runtime.continue", "provider-subagent.start"].includes(String(effect.effect_type)));
      const qualified = disposition?.qualification.type === "qualified";
      let restoredBinding: OrchestrationV2ImportedHistoryReviewResult["restoredBinding"] = { type: "missing", reason: "no_exact_restored_binding" };
      if (qualified && thread !== null && Option.isSome(local)) {
        const provider = local.value.providerThreads.find((provider) => provider.id === thread.activeProviderThreadId);
        const evidence = yield* readProviderRuntimeEvidenceEffect(input.threadId);
        if (provider?.nativeThreadRef?.nativeId === disposition.qualification.nativeThreadId &&
            provider.appThreadId === input.threadId && provider.providerInstanceId === selected?.instanceId) {
          restoredBinding = { type: "ready", providerThreadId: provider.id, providerInstanceId: provider.providerInstanceId,
            driver: provider.driver, nativeThreadId: provider.nativeThreadRef.nativeId, providerSessionId: provider.providerSessionId,
            ...(evidence?.binding.providerThreadId === provider.id && evidence.binding.nativeThreadId === provider.nativeThreadRef.nativeId &&
              evidence.binding.runtimeGeneration !== null ? { runtimeGeneration: evidence.binding.runtimeGeneration } : {}) };
        }
      }
      const basis = { version: 1, actorSessionId: input.actorSessionId, threadId: input.threadId,
        delivery: yield* Schema.encodeEffect(OrchestrationV2ImportedHistoryDelivery)(delivery), snapshot,
        target: selected === undefined || thread === null ? null : { modelSelection: selected,
          runtimeMode: delivery.type === "message" ? delivery.runtimeMode : thread.runtimeMode,
          interactionMode: delivery.type === "message" ? delivery.interactionMode : thread.interactionMode,
          project: snapshot.records.project, descriptor }, nativeSeal, legacyTranscript: legacy,
        unresolvedCreation, exactDelivery };
      const eligible = imported && exactDelivery && capability && !qualified && !uncertain && (legacyPositive || nativePositive);
      const review = yield* Schema.decodeUnknownEffect(OrchestrationV2ImportedHistoryReviewResult)({ version: 2,
        threadId: input.threadId, target, capability: { startWithImportedHistory: capability },
        applicability: imported ? "imported" : thread === null ? "unknown" : "not_imported",
        qualification: qualified ? { type: "qualified" } : disposition?.qualification ?? { type: "unknown", reason: "no_historical_disposition" },
        restoredBinding, nativeEffects: uncertain ? { type: "unknown", reason: "unresolved_native_operation" } : { type: "clear" },
        transcriptEligibility: (legacyPositive || nativePositive) && exactDelivery ? { type: "eligible" } :
          { type: "unknown", reason: "no_complete_current_imported_snapshot_or_exact_delivery" },
        reviewedBasis: eligible ? OrchestrationV2ImportedHistoryReviewBasis.make(
          `imported-history-review:v1:${nativeCreationSha256(nativeCreationCanonicalJson(basis))}`) : null });
      return { review, snapshot, basis } satisfies ImportedHistoryStartReviewFactsV2;
    });
    const readImportedHistoryStartReview: EventSinkV2Shape["readImportedHistoryStartReview"] = (input) =>
      sql.withTransaction(readImportedHistoryStartReviewEffect(input)).pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause })));
    const readImportedHistoryStartChoiceEffect = Effect.fnUntraced(function* (input: { readonly threadId: ThreadId; readonly commandId: CommandId }) {
      const rows = yield* sql<{ readonly actor_session_id: string; readonly command_digest: string; readonly canonical_command_json: string;
        readonly basis_json: string; readonly intent_status: string; readonly run_id: string | null; readonly message_id: string | null;
        readonly effect_id: string | null; readonly outcome_json: string }>`
        SELECT choice.actor_session_id, choice.command_digest, choice.canonical_command_json, choice.basis_json,
          outcome.intent_status, outcome.run_id, outcome.message_id, outcome.effect_id, outcome.outcome_json
        FROM orchestration_v2_imported_history_start_choices choice
        JOIN orchestration_v2_imported_history_start_outcomes outcome ON outcome.command_id = choice.command_id
        WHERE choice.command_id = ${input.commandId} AND choice.thread_id = ${input.threadId}`;
      if (rows.length === 0) return null;
      const row = rows[0]!;
      const command = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestrationV2StartWithImportedHistoryCommand))(row.canonical_command_json);
      const receipt = Option.getOrNull(yield* commandReceipts.getByCommandId(input.commandId));
      const details = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Struct({ rejectionReason: Schema.NullOr(Schema.String) })))(row.outcome_json);
      if (command.commandId !== input.commandId || command.threadId !== input.threadId ||
          nativeCreationSha256(nativeCreationCanonicalJson(yield* Schema.encodeEffect(OrchestrationV2StartWithImportedHistoryCommand)(command))) !== row.command_digest ||
          receipt === null || receipt.threadId !== input.threadId || receipt.commandType !== command.type || receipt.status !== row.intent_status)
        return yield* importSealFailure("Imported choice identity, canonical digest, outcome and receipt disagree");
      return { commandId: input.commandId, threadId: input.threadId, actorSessionId: AuthSessionId.make(row.actor_session_id),
        commandDigest: row.command_digest, command,
        basis: yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)))(row.basis_json), receipt,
        runId: row.run_id === null ? null : RunId.make(row.run_id), messageId: row.message_id === null ? null : MessageId.make(row.message_id),
        effectId: row.effect_id, rejectionReason: details.rejectionReason } satisfies ImportedHistoryStartOutcomeV2;
    });
    const readImportedHistoryStartChoice: EventSinkV2Shape["readImportedHistoryStartChoice"] = (input) =>
      sql.withTransaction(readImportedHistoryStartChoiceEffect(input)).pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause })));
    const commitImportedHistoryStart: EventSinkV2Shape["commitImportedHistoryStart"] = (input) => withTransaction(Effect.gen(function* () {
      const command = yield* Schema.decodeUnknownEffect(OrchestrationV2StartWithImportedHistoryCommand)(input.command, { onExcessProperty: "error" });
      const canonical = nativeCreationCanonicalJson(yield* Schema.encodeEffect(OrchestrationV2StartWithImportedHistoryCommand)(command));
      const digest = nativeCreationSha256(canonical);
      const conflict = () => Effect.fail(new NativeCommandPreconditionError({ commandId: command.commandId, reason: "identity_conflict" }));
      const priorOrdinary = yield* readOrdinaryCheckoutAdmissionsEffect(command.commandId);
      const priorOrdinaryLinks = yield* readOrdinaryCheckoutCommandLinksEffect(command.commandId);
      if (((priorOrdinary.length > 0 || priorOrdinaryLinks.length > 0) && input.ordinaryCheckoutContext === undefined) ||
          (input.ordinaryCheckoutContext !== undefined && canonical !==
            nativeCreationCanonicalJson(Schema.encodeSync(OrchestrationV2Command)(input.ordinaryCheckoutContext.command)))) return yield* conflict();
      if (input.reviewContext.threadId !== command.threadId ||
          nativeCreationCanonicalJson(yield* Schema.encodeEffect(OrchestrationV2ImportedHistoryDelivery)(input.reviewContext.delivery)) !==
          nativeCreationCanonicalJson(yield* Schema.encodeEffect(OrchestrationV2ImportedHistoryDelivery)(command.delivery))) return yield* conflict();
      const identities = yield* sql<{ readonly command_digest: string; readonly canonical_command_json: string; readonly actor_session_id: string }>`
        SELECT command_digest, canonical_command_json, actor_session_id FROM orchestration_v2_imported_history_start_choices WHERE command_id = ${command.commandId}`;
      if (identities.length > 0) {
        const row = identities[0]!;
        if (row.command_digest !== digest || row.canonical_command_json !== canonical || row.actor_session_id !== input.reviewContext.actorSessionId)
          return yield* conflict();
        yield* input.revalidateAuthority;
        const outcome = yield* readImportedHistoryStartChoiceEffect({ threadId: command.threadId, commandId: command.commandId });
        if (outcome === null) return yield* new NativeCommandPreconditionError({ commandId: command.commandId, reason: "unknown_evidence" });
        return { committed: false, outcome };
      }
      if (Option.isSome(yield* commandReceipts.getByCommandId(command.commandId)) || (yield* readIdentity(command.commandId)) !== null)
        return yield* new NativeCommandPreconditionError({ commandId: command.commandId, reason: "unbound_receipt" });
      yield* input.revalidateAuthority;
      const facts = yield* readImportedHistoryStartReviewEffect(input.reviewContext);
      const now = yield* DateTime.now;
      yield* sql`INSERT INTO orchestration_v2_imported_history_start_choices
        (command_id, command_type, thread_id, actor_session_id, command_digest, canonical_command_json, reviewed_basis, basis_json, reserved_at)
        VALUES (${command.commandId}, ${command.type}, ${command.threadId}, ${input.reviewContext.actorSessionId}, ${digest}, ${canonical},
          ${command.reviewedBasis}, ${nativeCreationCanonicalJson(facts.basis)}, ${DateTime.formatIso(now)})`;
      if (facts.review.reviewedBasis === null || facts.review.reviewedBasis !== command.reviewedBasis) {
        yield* commitRejectedCommandEffect({ commandId: command.commandId, threadId: command.threadId, commandType: command.type,
          rejectedAt: now, error: "imported_history_review_changed" }).pipe(Effect.provideService(ImportedHistoryReservation, digest));
        yield* sql`INSERT INTO orchestration_v2_imported_history_start_outcomes
          (command_id, intent_status, run_id, message_id, effect_id, outcome_json, recorded_at)
          VALUES (${command.commandId}, 'rejected', NULL, NULL, NULL, ${nativeCreationCanonicalJson({ rejectionReason: "imported_history_review_changed" })}, ${DateTime.formatIso(now)})`;
      } else {
        const plan = yield* input.plan(facts);
        if (plan.messageId !== command.delivery.messageId || (command.delivery.type === "queued_run" && plan.runId !== command.delivery.runId) ||
            plan.events.some((event) => event.threadId !== command.threadId || event.type.startsWith("provider-session.") ||
              (event.type === "provider-thread.updated" && (command.delivery.type !== "message" ||
                event.payload.providerSessionId !== null || event.payload.nativeThreadRef !== null ||
                event.payload.nativeConversationHeadRef !== null || event.payload.appThreadId !== command.threadId ||
                facts.snapshot.records.provider_threads?.some((row) => row.provider_thread_id === event.payload.id)))) ||
            plan.effects.some((effect) => effect.commandId !== command.commandId || effect.threadId !== command.threadId || effect.nativeCreationExecutionReference !== undefined))
          return yield* conflict();
        const starts = plan.effects.filter((effect) => effect.request.type === "provider-turn.start");
        if (starts.length > 1 || starts.some((effect) => effect.request.type !== "provider-turn.start" || effect.request.runId !== plan.runId ||
            effect.id !== `effect:${command.commandId}:provider-turn.start:${plan.runId}`) ||
            (command.delivery.type === "queued_run" && (starts.length !== 1 || plan.effects.length !== 1)))
          return yield* conflict();
        const delivery = command.delivery;
        const beforeRun = delivery.type === "queued_run" ? facts.snapshot.records.runs?.find((run) => run.run_id === delivery.runId) : undefined;
        const beforeMessage = delivery.type === "queued_run" ? facts.snapshot.records.messages?.find((message) => message.message_id === delivery.messageId) : undefined;
        yield* input.revalidateAuthority;
        const target = facts.basis.target as { readonly modelSelection: { readonly instanceId: ProviderInstanceId }; readonly descriptor: unknown } | null;
        if (target === null || nativeCreationCanonicalJson(yield* input.reviewContext.readTargetCapability(target.modelSelection.instanceId)) !== nativeCreationCanonicalJson(target.descriptor))
          return yield* new NativeCommandPreconditionError({ commandId: command.commandId, reason: "stale_target" });
        yield* commitCommandEffect({ commandId: command.commandId, threadId: command.threadId, commandType: command.type,
          acceptedAt: now, events: plan.events, effects: plan.effects,
          ...(input.ordinaryCheckoutContext === undefined ? {} : { ordinaryCheckoutContext: input.ordinaryCheckoutContext }),
        }).pipe(Effect.provideService(ImportedHistoryReservation, digest));
        const actual = yield* projectionStore.getThreadRecords(command.threadId, ["runs", "messages"]);
        const run = actual.runs.find((run) => run.id === plan.runId && run.userMessageId === plan.messageId);
        const message = actual.messages.find((message) => message.id === plan.messageId && message.runId === plan.runId && message.role === "user");
        if (run === undefined || message === undefined) return yield* conflict();
        if (command.delivery.type === "queued_run") {
          const afterRun = yield* sql`SELECT * FROM orchestration_v2_projection_runs WHERE run_id = ${plan.runId} AND thread_id = ${command.threadId}`;
          const afterMessage = yield* sql`SELECT * FROM orchestration_v2_projection_messages WHERE message_id = ${plan.messageId} AND thread_id = ${command.threadId}`;
          if (nativeCreationCanonicalJson(beforeRun) !== nativeCreationCanonicalJson(afterRun[0]) ||
              nativeCreationCanonicalJson(beforeMessage) !== nativeCreationCanonicalJson(afterMessage[0])) return yield* conflict();
        } else {
          const actualThread = actual.thread;
          if (message.text !== command.delivery.text || nativeCreationCanonicalJson(message.attachments) !== nativeCreationCanonicalJson(command.delivery.attachments) ||
              nativeCreationCanonicalJson(message.context ?? null) !== nativeCreationCanonicalJson(command.delivery.context ?? null) ||
              nativeCreationCanonicalJson(run.modelSelection) !== nativeCreationCanonicalJson(target.modelSelection) ||
              run.providerInstanceId !== target.modelSelection.instanceId || actualThread.runtimeMode !== command.delivery.runtimeMode ||
              actualThread.interactionMode !== command.delivery.interactionMode ||
              nativeCreationCanonicalJson(run.sourcePlanRef ?? null) !== nativeCreationCanonicalJson(command.delivery.sourcePlanRef ?? null)) return yield* conflict();
        }
        yield* sql`INSERT INTO orchestration_v2_imported_history_start_outcomes
          (command_id, intent_status, run_id, message_id, effect_id, outcome_json, recorded_at)
          VALUES (${command.commandId}, 'accepted', ${plan.runId}, ${plan.messageId}, ${starts[0]?.id ?? null},
            ${nativeCreationCanonicalJson({ rejectionReason: null })}, ${DateTime.formatIso(now)})`;
      }
      const outcome = yield* readImportedHistoryStartChoiceEffect({ threadId: command.threadId, commandId: command.commandId });
      if (outcome === null) return yield* importSealFailure("Imported choice outcome readback is missing");
      return { committed: true, outcome };
    })).pipe(Effect.mapError(ordinaryError));
    const prepareImportedHistoryStartExecution: EventSinkV2Shape["prepareImportedHistoryStartExecution"] = (input) =>
      withTransaction(Effect.gen(function* () {
        const reference = yield* Schema.decodeUnknownEffect(Schema.Struct({ commandId: CommandId, threadId: ThreadId, runId: RunId,
          effectId: Schema.NonEmptyString }))(input.reference, { onExcessProperty: "error" });
        const rejected = (reason: string) => ({ status: "rejected" as const, reason });
        const choice = yield* readImportedHistoryStartChoiceEffect(reference);
        if (choice === null || choice.receipt.status !== "accepted" || choice.runId !== reference.runId ||
            (choice.effectId !== null && choice.effectId !== reference.effectId) ||
            reference.effectId !== `effect:${reference.commandId}:provider-turn.start:${reference.runId}` ||
            input.currentSnapshot.commandId !== reference.commandId || input.currentSnapshot.threadId !== reference.threadId ||
            input.currentSnapshot.authority.actorSessionId !== choice.actorSessionId) return rejected("choice_identity_conflict");
        const current = yield* readCommitSnapshot(reference.threadId, reference.commandId, input.currentSnapshot.authority);
        if (nativeCreationCanonicalJson(current) !== nativeCreationCanonicalJson(input.currentSnapshot)) return rejected("source_changed");
        const now = DateTime.formatIso(yield* DateTime.now);
        const claims = yield* sql<{ readonly payload_json: string }>`SELECT payload_json FROM orchestration_v2_effect_outbox
          WHERE effect_id = ${reference.effectId} AND command_id = ${reference.commandId} AND thread_id = ${reference.threadId}
            AND effect_type = 'provider-turn.start' AND status = 'running' AND lease_owner = ${input.workerId}
            AND attempt_count = ${input.expectedAttempt} AND lease_expires_at > ${now}`;
        if (claims.length !== 1 || !Number.isSafeInteger(input.expectedAttempt) || input.expectedAttempt < 1) return rejected("claim_changed");
        const effect = yield* EffectOutbox.decodeOrchestrationEffectPayloadV2(claims[0]!.payload_json);
        if (effect.request.type !== "provider-turn.start" || effect.request.runId !== reference.runId ||
            "nativeCreationExecutionReference" in effect || current.records.unknown_effect_holds!.length > 0) return rejected("unknown_prior_effect");
        const unresolved = yield* sql`SELECT started.effect_id FROM native_creation_effect_facts started
          JOIN native_creation_intents claim ON claim.claim_id = started.claim_id
          WHERE claim.thread_id = ${reference.threadId} AND started.phase = 'started' AND NOT EXISTS (
            SELECT 1 FROM native_creation_effect_facts completed WHERE completed.claim_id = started.claim_id
              AND completed.effect_id = started.effect_id AND completed.phase = 'completed')`;
        if (unresolved.length > 0 || current.records.effects!.some((row) => row.effect_id !== reference.effectId && row.status === "running" &&
            ["provider-turn.start", "provider-runtime.continue", "provider-subagent.start"].includes(String(row.effect_type)))) return rejected("unknown_prior_effect");
        const retained = yield* sql<{ readonly execution_intent_json: string }>`SELECT execution_intent_json
          FROM orchestration_v2_queued_start_reservations WHERE effect_id = ${reference.effectId} AND command_id = ${reference.commandId}
            AND thread_id = ${reference.threadId} AND run_id = ${reference.runId}`;
        if (retained.length !== 0) {
          const originalIntent = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ExecutionIntentSchemaV2))(retained[0]!.execution_intent_json);
          if (retained.length !== 1 || originalIntent.kind !== "imported_history_choice" || originalIntent.commandId !== reference.commandId ||
              originalIntent.runId !== reference.runId || originalIntent.effectId !== reference.effectId || originalIntent.reviewedBasis !== choice.command.reviewedBasis)
            return rejected("choice_identity_conflict");
          return { status: "already_prepared" as const, executionIntent: originalIntent };
        }
        const local = yield* projectionStore.getThreadRecords(reference.threadId, ["runs", "attempts", "messages", "providerThreads", "providerSessions", "nodes", "checkpointScopes"]);
        const run = local.runs.find((run) => run.id === reference.runId && run.userMessageId === choice.messageId);
        const attempt = local.attempts.find((attempt) => attempt.id === run?.activeAttemptId && attempt.runId === reference.runId);
        const provider = local.providerThreads.find((provider) => provider.id === run?.providerThreadId);
        const message = local.messages.find((message) => message.id === choice.messageId && message.runId === reference.runId && message.role === "user");
        if (run === undefined || attempt === undefined || provider === undefined || message === undefined || run.rootNodeId === null ||
            attempt.rootNodeId !== run.rootNodeId || attempt.providerThreadId !== provider.id || attempt.status !== "pending" ||
            (run.status !== "starting" && run.status !== "queued") || provider.appThreadId !== reference.threadId ||
            (choice.command.delivery.type === "message" && (provider.nativeThreadRef !== null || provider.nativeConversationHeadRef !== null)))
          return rejected("run_binding_changed");
        if (local.runs.some((candidate) => candidate.id !== run.id && (candidate.status === "running" || candidate.status === "starting" ||
            (choice.command.delivery.type !== "queued_run" && candidate.status === "queued" && candidate.queueHeld === true)))) return rejected("unrelated_work_pending");
        if (choice.command.delivery.type === "queued_run" && queuedRunsInDeliveryOrder(local)[0]?.id !== run.id)
          return rejected("queued_delivery_not_first");
        const original = choice.basis.snapshot as NativeCommandTargetSnapshotV2 | undefined;
        if (original === undefined || original.threadId !== reference.threadId || original.authority.actorSessionId !== choice.actorSessionId)
          return rejected("choice_basis_unavailable");
        const target = yield* Schema.decodeUnknownEffect(Schema.Struct({ modelSelection: ModelSelection, runtimeMode: RuntimeMode,
          interactionMode: ProviderInteractionMode, descriptor: Schema.Unknown, project: Schema.Array(Schema.Unknown) }))(choice.basis.target);
        const descriptor = yield* input.reviewSource.readTargetCapability(target.modelSelection.instanceId);
        if (descriptor === null || descriptor.enabled !== true || descriptor.declared?.canConsumeHandoffSummaries !== true ||
            descriptor.declared.supportsFullThreadHandoff !== true || descriptor.declared.supportsProviderSwitchingViaHandoff !== true ||
            nativeCreationCanonicalJson(descriptor) !== nativeCreationCanonicalJson(target.descriptor) ||
            nativeCreationCanonicalJson(run.modelSelection) !== nativeCreationCanonicalJson(target.modelSelection) ||
            run.providerInstanceId !== target.modelSelection.instanceId || provider.providerInstanceId !== run.providerInstanceId || provider.driver !== descriptor.driver ||
            local.thread.runtimeMode !== target.runtimeMode || local.thread.interactionMode !== target.interactionMode ||
            nativeCreationCanonicalJson(current.records.project) !== nativeCreationCanonicalJson(target.project)) return rejected("target_changed");
        for (const key of ["legacy_continuation", "native_import_seals", "legacy_import_markers", "legacy_source_threads", "legacy_source_messages", "imported_source_events"] as const) {
          if (nativeCreationCanonicalJson(current.records[key]) !== nativeCreationCanonicalJson(original.records[key])) return rejected("transcript_source_changed");
        }
        const disposition = yield* readLegacyContinuationDisposition(reference.threadId);
        if (disposition === null || disposition.qualification.type === "qualified") return rejected("explicit_new_context_unavailable");
        if (disposition.provenance === "native_import") {
          const seal = yield* readNativeImportTranscriptSeal(reference.threadId);
          if (seal === null || nativeCreationCanonicalJson(seal) !== nativeCreationCanonicalJson(choice.basis.nativeSeal)) return rejected("transcript_source_changed");
        } else {
          const legacy = input.reviewSource.readLegacyTranscript === undefined ? null : yield* input.reviewSource.readLegacyTranscript;
          if (legacy === null || nativeCreationCanonicalJson(legacy) !== nativeCreationCanonicalJson(choice.basis.legacyTranscript)) return rejected("transcript_source_changed");
        }
        if (choice.command.delivery.type === "message") {
          if (message.text !== choice.command.delivery.text || nativeCreationCanonicalJson(message.attachments) !== nativeCreationCanonicalJson(choice.command.delivery.attachments) ||
              nativeCreationCanonicalJson(message.context ?? null) !== nativeCreationCanonicalJson(choice.command.delivery.context ?? null)) return rejected("delivery_changed");
        } else {
          const source = original.records.messages?.find((row) => row.message_id === choice.messageId);
          const actual = current.records.messages?.find((row) => row.message_id === choice.messageId);
          const originalRun = original.records.runs?.find((row) => row.run_id === run.id);
          const currentRun = current.records.runs?.find((row) => row.run_id === run.id);
          const originalProvider = original.records.provider_threads?.find((row) => row.provider_thread_id === provider.id);
          const currentProvider = current.records.provider_threads?.find((row) => row.provider_thread_id === provider.id);
          if (source === undefined || originalRun === undefined || originalProvider === undefined ||
              nativeCreationCanonicalJson(source) !== nativeCreationCanonicalJson(actual) ||
              nativeCreationCanonicalJson(originalRun) !== nativeCreationCanonicalJson(currentRun) ||
              nativeCreationCanonicalJson(originalProvider) !== nativeCreationCanonicalJson(currentProvider) ||
              nativeCreationCanonicalJson(original.records.runtime_evidence) !== nativeCreationCanonicalJson(current.records.runtime_evidence))
            return rejected("delivery_changed");
        }
        yield* input.revalidateAuthority(choice);
        const incarnation = yield* readApplicationBirthRecordEffect(reference.threadId);
        if (incarnation === null) return rejected("birth_unavailable");
        const executionIntent = { kind: "imported_history_choice" as const, commandId: reference.commandId, runId: reference.runId,
          runAttemptId: attempt.id, effectId: reference.effectId, reviewedBasis: choice.command.reviewedBasis };
        const fields = { runId: run.id, messageId: message.id, queuedProviderThreadId: provider.id, runAttemptId: attempt.id,
          executionIntent, switchPlan: null, sourceMode: "new_context" as const, sourceBinding: null, sourceEvidenceRevision: null };
        const reservation = yield* reserveQueuedRunStart({ snapshot: current, incarnation,
          basis: { ...fields, basisDigest: queuedRunContinuationBasisDigestV2(fields) }, executionIntent,
          revalidateCurrentSource: input.revalidateAuthority(choice) });
        if (reservation.status === "already_started") return { status: "already_prepared" as const, executionIntent: reservation.executionIntent };
        if (reservation.status !== "reserved") return rejected(reservation.status === "rejected" ? reservation.reason : reservation.reason);
        if (choice.command.delivery.type === "message" && provider.providerSessionId !== null)
          return yield* new NativeCommandPreconditionError({ commandId: reference.commandId, reason: "unknown_evidence" });
        const events = yield* input.prepare(choice);
        const providerEvents = events.filter((event) => event.type === "provider-thread.updated");
        const preparedProviderId = providerEvents.at(-1)?.payload.id;
        if (preparedProviderId === undefined || providerEvents.some((event) => event.payload.id !== preparedProviderId) ||
            (choice.command.delivery.type === "message" ? preparedProviderId !== provider.id : preparedProviderId === provider.id))
          return yield* new NativeCommandPreconditionError({ commandId: reference.commandId, reason: "identity_conflict" });
        if (choice.command.delivery.type === "queued_run") {
          const priorProviders = yield* sql`SELECT provider_thread_id FROM orchestration_v2_projection_provider_threads WHERE provider_thread_id = ${preparedProviderId}
            UNION ALL SELECT event_id FROM orchestration_events WHERE application_event_version = 2 AND event_type = 'provider-thread.updated'
              AND json_extract(payload_json, '$.id') = ${preparedProviderId}`;
          if (priorProviders.length !== 0)
            return yield* new NativeCommandPreconditionError({ commandId: reference.commandId, reason: "identity_conflict" });
        }
        const allowed = new Set(["provider-thread.updated", "thread.metadata-updated",
          "thread.provider-switched", "node.updated", "checkpoint-scope.created", "run.updated", "run-attempt.updated", "context-handoff.updated"]);
        if (events.length === 0 || events.some((event) => event.threadId !== reference.threadId || !allowed.has(event.type) ||
            (event.type === "provider-thread.updated" && (event.payload.id !== preparedProviderId || event.payload.appThreadId !== reference.threadId ||
              event.payload.providerInstanceId !== run.providerInstanceId || event.payload.driver !== provider.driver ||
              event.payload.nativeThreadRef !== null || event.payload.nativeConversationHeadRef !== null)) ||
            ((event.type === "run.created" || event.type === "run.updated") && event.payload.id !== run.id) ||
            ((event.type === "run-attempt.created" || event.type === "run-attempt.updated") && event.payload.id !== attempt.id) ||
            (event.type === "node.updated" && event.payload.id !== run.rootNodeId) ||
            (event.type === "checkpoint-scope.created" && (event.payload.runId !== run.id || event.payload.nodeId !== run.rootNodeId || event.payload.providerThreadId !== preparedProviderId)) ||
            (event.type === "context-handoff.updated" && (event.payload.targetRunId !== run.id || event.payload.toProviderThreadId !== preparedProviderId))))
          return yield* new NativeCommandPreconditionError({ commandId: reference.commandId, reason: "identity_conflict" });
        const plannedSessionId = providerEvents.at(-1)?.payload.providerSessionId;
        if (plannedSessionId === undefined || plannedSessionId === null ||
            providerEvents.some((event) => event.payload.providerSessionId !== plannedSessionId))
          return yield* new NativeCommandPreconditionError({ commandId: reference.commandId, reason: "identity_conflict" });
        const priorSessionIds = yield* sql`SELECT provider_session_id FROM orchestration_v2_projection_provider_sessions
          WHERE provider_session_id = ${plannedSessionId}
          UNION ALL SELECT provider_session_id FROM orchestration_v2_projection_provider_threads WHERE provider_session_id = ${plannedSessionId}
          UNION ALL SELECT provider_session_id FROM orchestration_v2_projection_provider_session_bindings WHERE provider_session_id = ${plannedSessionId}
          UNION ALL SELECT event_id FROM orchestration_events WHERE application_event_version = 2 AND
            ((event_type IN ('provider-session.attached', 'provider-session.updated') AND json_extract(payload_json, '$.id') = ${plannedSessionId}) OR
             (event_type IN ('provider-thread.created', 'provider-thread.updated') AND json_extract(payload_json, '$.providerSessionId') = ${plannedSessionId}))`;
        if (priorSessionIds.length !== 0)
          return yield* new NativeCommandPreconditionError({ commandId: reference.commandId, reason: "identity_conflict" });
        yield* input.revalidateAuthority(choice);
        if (nativeCreationCanonicalJson(yield* input.reviewSource.readTargetCapability(target.modelSelection.instanceId)) !== nativeCreationCanonicalJson(descriptor))
          return yield* new NativeCommandPreconditionError({ commandId: reference.commandId, reason: "stale_target" });
        const storedEvents = yield* writeEffect({ commandId: reference.commandId, events, effects: [] });
        const after = yield* projectionStore.getThreadRecords(reference.threadId, ["runs", "attempts", "messages", "providerThreads", "providerSessions", "nodes", "checkpointScopes"]);
        const afterRun = after.runs.find((candidate) => candidate.id === run.id);
        const afterAttempt = after.attempts.find((candidate) => candidate.id === attempt.id);
        const afterProvider = after.providerThreads.find((candidate) => candidate.id === preparedProviderId);
        const node = after.nodes.find((candidate) => candidate.id === run.rootNodeId);
        const scope = after.checkpointScopes.find((candidate) => candidate.id === node?.checkpointScopeId);
        const project = Option.getOrNull(yield* projectStore.get(local.thread.projectId));
        const cwd = local.thread.worktreePath ?? project?.workspaceRoot;
        if (afterRun === undefined || afterAttempt === undefined || afterProvider === undefined || node === undefined || scope === undefined ||
            cwd === undefined || scope.cwd !== cwd || scope.runId !== run.id || scope.nodeId !== run.rootNodeId || scope.providerThreadId !== preparedProviderId ||
            after.thread.activeProviderThreadId !== preparedProviderId || afterProvider.nativeThreadRef !== null || afterProvider.nativeConversationHeadRef !== null ||
            afterProvider.providerSessionId !== plannedSessionId || afterProvider.providerInstanceId !== run.providerInstanceId || afterProvider.driver !== provider.driver ||
            after.providerSessions.some((session) => session.id === plannedSessionId) ||
            afterRun.status !== (choice.command.delivery.type === "queued_run" ? "starting" : run.status) || afterAttempt.status !== "pending" || afterRun.rootNodeId !== run.rootNodeId ||
            afterRun.activeAttemptId !== attempt.id || afterRun.userMessageId !== message.id || afterRun.providerThreadId !== preparedProviderId ||
            nativeCreationCanonicalJson(afterRun.modelSelection) !== nativeCreationCanonicalJson(run.modelSelection) || afterRun.ordinal !== run.ordinal ||
            afterRun.queueHeld !== run.queueHeld || afterRun.queuePosition !== run.queuePosition || afterAttempt.providerThreadId !== preparedProviderId || afterAttempt.rootNodeId !== run.rootNodeId ||
            nativeCreationCanonicalJson(after.messages.find((candidate) => candidate.id === message.id)) !== nativeCreationCanonicalJson(message))
          return yield* new NativeCommandPreconditionError({ commandId: reference.commandId, reason: "identity_conflict" });
        if (choice.command.delivery.type === "queued_run" &&
            nativeCreationCanonicalJson(after.providerThreads.find((candidate) => candidate.id === provider.id)) !== nativeCreationCanonicalJson(provider))
          return yield* new NativeCommandPreconditionError({ commandId: reference.commandId, reason: "identity_conflict" });
        const { providerThreadId: _originalProvider, status: _originalStatus, contextHandoffId: _originalHandoff, ...originalRunFields } = run;
        const { providerThreadId: _preparedProvider, status: _preparedStatus, contextHandoffId: preparedHandoff, ...preparedRunFields } = afterRun;
        if (nativeCreationCanonicalJson(originalRunFields) !== nativeCreationCanonicalJson(preparedRunFields) ||
            (preparedHandoff !== run.contextHandoffId && !events.some((event) => event.type === "context-handoff.updated" &&
              event.payload.id === preparedHandoff && event.payload.targetRunId === run.id && event.payload.toProviderThreadId === preparedProviderId)))
          return yield* new NativeCommandPreconditionError({ commandId: reference.commandId, reason: "identity_conflict" });
        return { status: "prepared" as const, choice, executionIntent, storedEvents };
      })).pipe(Effect.mapError((cause) => cause instanceof NativeCommandPreconditionError ? cause : new EventSinkWriteError({ eventCount: 0, cause })));

    const observeImportedHistoryStart: EventSinkV2Shape["observeImportedHistoryStart"] = (input) => sql.withTransaction(Effect.gen(function* () {
      const outcome = yield* readImportedHistoryStartChoiceEffect(input);
      const empty = { status: "not_started" as const, runId: null, providerThreadId: null, providerSessionId: null,
        nativeThreadId: null, effectOutcome: null, error: null };
      if (outcome === null) return yield* Schema.decodeUnknownEffect(OrchestrationV2ImportedHistoryStartReceipt)({ version: 2,
        ...input, target: null, reviewedBasis: null, intentStatus: "not_found", receipt: null, rejectionReason: null, execution: empty });
      const target = outcome.command.delivery.type === "message" ? { type: "message", messageId: outcome.command.delivery.messageId }
        : { type: "queued_run", runId: outcome.command.delivery.runId, messageId: outcome.command.delivery.messageId };
      let execution: OrchestrationV2ImportedHistoryStartReceipt["execution"] = outcome.receipt.status === "rejected" ? empty :
        { ...empty, status: "pending", runId: outcome.runId };
      let effectId = outcome.effectId;
      if (effectId === null && outcome.receipt.status === "accepted" && outcome.runId !== null) {
        const reservations = yield* sql<{ readonly effect_id: string; readonly execution_intent_json: string }>`
          SELECT effect_id, execution_intent_json FROM orchestration_v2_queued_start_reservations
          WHERE command_id = ${input.commandId} AND thread_id = ${input.threadId} AND run_id = ${outcome.runId}`;
        if (reservations.length === 1) {
          const intent = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ExecutionIntentSchemaV2))(reservations[0]!.execution_intent_json);
          if (intent.kind !== "imported_history_choice" || intent.commandId !== input.commandId || intent.runId !== outcome.runId ||
              intent.reviewedBasis !== outcome.command.reviewedBasis || intent.effectId !== reservations[0]!.effect_id ||
              intent.effectId !== `effect:${input.commandId}:provider-turn.start:${outcome.runId}`)
            return yield* importSealFailure("Imported observer has a mismatched execution reservation");
          effectId = intent.effectId;
        } else if (reservations.length > 1) return yield* importSealFailure("Imported observer has ambiguous execution reservations");
      }
      if (effectId !== null) {
        const held = yield* effectOutbox.listHeldByThreadId(input.threadId);
        const effects = yield* effectOutbox.listByCommandId(input.commandId);
        const effect = effects.find((effect) => effect.id === effectId);
        const confirmed = Option.getOrNull(yield* nativeCreationRepository.readNativeEffectConfirmation(effectId).pipe(Effect.option));
        const current = yield* readCurrentProviderRuntimeOwnerEffect(input.threadId);
        if (confirmed !== null && confirmed.commandId === input.commandId && confirmed.runId === outcome.runId &&
            current?.evidenceRevision === confirmed.evidenceRevision &&
            current.binding.providerThreadId === confirmed.binding.providerThreadId && current.binding.providerSessionId === confirmed.binding.providerSessionId &&
            current.binding.instanceId === confirmed.binding.instanceId && current.binding.nativeThreadId === confirmed.binding.nativeThreadId &&
            current.binding.runtimeGeneration === confirmed.binding.runtimeGeneration && effect?.status === "succeeded")
          execution = { status: "started", runId: confirmed.runId, providerThreadId: confirmed.binding.providerThreadId,
            providerSessionId: confirmed.binding.providerSessionId, nativeThreadId: confirmed.binding.nativeThreadId ?? null,
            runtimeGeneration: confirmed.binding.runtimeGeneration, effectOutcome: "confirmed_success", error: null };
        else if (held.some((hold) => hold.effectId === effectId) || effect?.status === "running" || effect?.status === "succeeded" || effect?.status === "failed")
          execution = { ...empty, status: "unknown", runId: outcome.runId, effectOutcome: "unknown", error: "no_complete_current_native_confirmation" };
      }
      return yield* Schema.decodeUnknownEffect(OrchestrationV2ImportedHistoryStartReceipt)({ version: 2, ...input, target,
        reviewedBasis: outcome.command.reviewedBasis, intentStatus: outcome.receipt.status, receipt: outcome.receipt,
        rejectionReason: outcome.rejectionReason, execution });
    })).pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause })));

    return EventSinkV2.of({
      readOrdinaryCheckoutAdmission: (input) => sql.withTransaction(readOrdinaryCheckoutAdmissionsEffect(input.commandId)).pipe(
        Effect.map((admissions) => admissions.find((admission) => admission.capture.threadId === input.threadId) ?? null),
        Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, commandId: input.commandId, cause }))),
      readOrdinaryCheckoutEffectLink: (effectId) => sql.withTransaction(readOrdinaryCheckoutEffectLinkEffect(effectId)).pipe(
        Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      readOrdinaryCheckoutAdmissionForRun: (input) => sql.withTransaction(readOrdinaryCheckoutAdmissionForRunEffect(input)).pipe(
        Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      captureOrdinaryPreparedLaunch: (input) => captureOrdinaryPreparedLaunch(input).pipe(Effect.mapError(ordinaryError)),
      readOrdinaryCheckoutUse: (operationId) => sql.withTransaction(readOrdinaryCheckoutUseEffect(operationId)).pipe(Effect.mapError(ordinaryError)),
      beginOrdinaryCheckoutUse: (input) => beginOrdinaryCheckoutUse(input).pipe(Effect.mapError(ordinaryError)),
      revalidateOrdinaryCheckoutUse: (use) => revalidateOrdinaryCheckoutUse(use).pipe(Effect.mapError(ordinaryError)),
      endOrdinaryCheckoutOutboxUse: (use) => settleOrdinaryCheckoutUse(use, true).pipe(Effect.mapError(ordinaryError)),
      abortUnstartedOrdinaryCheckoutUse: (use) => settleOrdinaryCheckoutUse(use, false).pipe(Effect.mapError(ordinaryError)),
      holdOrdinaryCheckoutUseUnknown: (input) => holdOrdinaryCheckoutUseUnknown(input).pipe(Effect.mapError(ordinaryError)),
      prepareImportedHistoryStartExecution,
      reserveQueuedRunStart,
      readInFlightQueuedRunStartBases,
      readClaimedQueuedRunStart,
      readCurrentThreadRuntimeStopIntent: (input) => sql.withTransaction(readCurrentThreadRuntimeStopIntentEffect(input)).pipe(
        Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, commandId: input.commandId, cause }))),
      readQueuedRunRuntimeStopFences: (input) => sql.withTransaction(readQueuedRunRuntimeStopFencesEffect(input)).pipe(
        Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      readImportedHistoryStartReview,
      readApplicationThreadBirth,
      readApplicationBirthRecord: (threadId) => sql.withTransaction(readApplicationBirthRecordEffect(threadId)).pipe(
        Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      readImportedHistoryStartChoice,
      commitImportedHistoryStart,
      observeImportedHistoryStart,
      recordNativeImportTranscriptSeal,
      readNativeImportTranscriptSeal,
      readCommandReceiptIdentity: (commandId) => sql.withTransaction(Effect.gen(function* () {
        const choices = yield* sql<{ readonly command_digest: string; readonly thread_id: string; readonly actor_session_id: string }>`
          SELECT command_digest, thread_id, actor_session_id FROM orchestration_v2_imported_history_start_choices WHERE command_id = ${commandId}`;
        const stops = yield* sql<{ readonly thread_id: string; readonly canonical_request_digest: string; readonly actor_binding_digest: string }>`
          SELECT thread_id, canonical_request_digest, actor_binding_digest FROM orchestration_v2_current_runtime_stop_intents WHERE command_id = ${commandId}`;
        return { receipt: Option.getOrNull(yield* commandReceipts.getByCommandId(commandId)),
          nativeCreationReservation: Option.getOrNull(yield* nativeCreationRepository.getReservedCommandIdentity(commandId)),
          capturedRestartOrigin: yield* readCapturedRestartOriginEffect(commandId),
          threadRecovery: yield* nativeCreationRepository.readThreadRecoveryCommand(commandId),
          threadDeletion: yield* readThreadDeletionCommandEffect(commandId),
          ordinaryCheckoutAdmissions: yield* readOrdinaryCheckoutAdmissionsEffect(commandId),
          ordinaryCheckoutEffectLinks: yield* readOrdinaryCheckoutCommandLinksEffect(commandId),
          projectReceipt: Option.getOrNull(yield* commandReceipts.getProjectByCommandId(commandId)),
          currentRuntimeStopIdentity: stops.length === 0 ? null : { threadId: ThreadId.make(stops[0]!.thread_id),
            canonicalRequestDigest: stops[0]!.canonical_request_digest, actorBindingDigest: stops[0]!.actor_binding_digest },
          identity: yield* readIdentity(commandId), importedHistoryChoiceIdentity: choices.length === 0 ? null :
            { commandDigest: choices[0]!.command_digest, threadId: ThreadId.make(choices[0]!.thread_id), actorSessionId: AuthSessionId.make(choices[0]!.actor_session_id) } };
      })).pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, commandId, cause }))),
      readCapturedRestartCommandOrigin: (input) => sql.withTransaction(validateCapturedRestartContextEffect(input.command, input.context)).pipe(
        Effect.map((result) => result.previous), Effect.mapError((cause) => Schema.is(NativeCommandPreconditionError)(cause) ? cause :
          new EventSinkWriteError({ eventCount: 0, commandId: input.command.commandId, cause }))),
      readNativeThreadRecovery: (input) => sql.withTransaction(validateNativeThreadRecovery(input.command, input.context)).pipe(
        Effect.mapError((cause) => Schema.is(NativeCommandPreconditionError)(cause) ? cause :
          new EventSinkWriteError({ eventCount: 0, commandId: input.command.commandId, cause }))),
      prepareRestartContinuation,
      findDormantRestartContinuation,
      readDormantRestartContinuations,
      readReleasedRestartContinuation,
      clearRestartContinuation,
      releaseRestartContinuation,
      onCommit: (effect) => assertPublicationScope.pipe(Effect.andThen(afterCommit(effect))),
      withTransaction,
      getThreadIncarnation: (threadId) => sql.withTransaction(readIncarnation(threadId)).pipe(
        Effect.map((birth) => birth.incarnation), Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      readNativeCommandFacts: (input) => readNativeCommandFactsEffect(input).pipe(
        Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, commandId: input.commandId, cause }))),
      recordLegacyContinuationDisposition,
      readLegacyContinuationDisposition,
      readProviderContinuationSourceIdentity,
      readProviderRuntimeEvidence: (threadId) => sql.withTransaction(readProviderRuntimeEvidenceEffect(threadId)).pipe(
        Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      readCurrentProviderRuntimeOwner: (threadId) => sql.withTransaction(readCurrentProviderRuntimeOwnerEffect(threadId)).pipe(
        Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      readLeaseCleanupStoreBasis: (lease) => sql.withTransaction(readLeaseCleanupStoreBasisEffect(lease)).pipe(
        Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      readLeaseCleanupTask: (effectId) => sql.withTransaction(readLeaseCleanupTaskEffect(effectId)).pipe(
        Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      readDeletionCleanupTask: (effectId) => sql.withTransaction(readDeletionCleanupTaskEffect(effectId)).pipe(
        Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      readDeletionCleanupTaskOwnerBirth: (effectId) => sql.withTransaction(readDeletionCleanupTaskOwnerBirthEffect(effectId)).pipe(
        Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      readDeletionCleanupTaskOutcome: (effectId) => sql.withTransaction(readDeletionCleanupTaskOutcomeEffect(effectId)).pipe(
        Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      prepareLeaseCleanupTaskBindings,
      prepareDeletionCleanupTaskBindings,
      recordLeaseCleanupTaskOutcome,
      recordObservedDeletionCleanupOutcome,
      readUnresolvedDeletionCleanupHolds: (threadId) => sql.withTransaction(readUnresolvedDeletionCleanupHoldsEffect(threadId)).pipe(
        Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      completeObservedDeletionCleanup,
      finalizeDeletionWorktreeCleanup,
      readThreadDeletionCommand: (commandId) => sql.withTransaction(readThreadDeletionCommandEffect(commandId)).pipe(
        Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, commandId, cause }))),
      readDeletionWorktreeTask: (effectId) => sql.withTransaction(readDeletionWorktreeTaskEffect(effectId)).pipe(
        Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      readDeletionWorktreeExecutionBasis: (effectId) => sql.withTransaction(readDeletionWorktreeExecutionBasisEffect(effectId)).pipe(
        Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      startDeletionWorktreeRemoval,
      revalidateDeletionWorktreeRemovalStart,
      readDeletionWorktreePathAdmission: (input) => sql.withTransaction(readDeletionWorktreePathAdmissionEffect(input)).pipe(
        Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      observeThreadDeletionCleanup,
      withWorktreeOwnershipTransaction,
      withDeletionWorktreeSqlMutation: (input, mutation) => withWorktreeOwnershipTransaction(Effect.gen(function* () {
        if (input.ordinaryMutation === undefined) {
          yield* assertDeletionWorktreePathWritable(input.path);
          return yield* mutation;
        }
        const own = yield* Schema.decodeUnknownEffect(Schema.toType(OrdinaryCheckout.OrdinaryCheckoutOwnMutationV1))(
          input.ordinaryMutation, { onExcessProperty: "error" });
        const use = own.ordinaryUse;
        const record = yield* exactOrdinaryCheckoutUse(use);
        const admission = yield* resolveOrdinaryCheckoutAdmission(use.admission);
        if (input.path !== admission.capture.canonicalCheckoutPath ||
            (record.state !== "reserved" && record.state !== "started"))
          return yield* ordinaryFailure(admission.capture, "unknown_use", "Own lease mutation requires its exact entered checkout operation");
        const before = yield* validateOrdinaryCheckoutCapture(admission.capture, record.subject.source,
          { operationId: use.operationId, requireLiveLease: false });
        yield* validateOrdinaryCheckoutUseSource(use, admission);
        const result = yield* mutation;
        const after = yield* validateOrdinaryCheckoutCapture(admission.capture, record.subject.source,
          { operationId: use.operationId, requireLiveLease: false });
        if (after.renewedAtMs < before.renewedAtMs || after.expiresAtMs < before.expiresAtMs)
          return yield* ordinaryFailure(admission.capture, "target_changed", "Own lease mutation cannot decrease captured lease liveness");
        return result;
      })).pipe(Effect.mapError((cause) => cause instanceof EventSinkWriteError ? cause :
        new EventSinkWriteError({ eventCount: 0, cause }))),
      readConfirmedImportedHistoryBinding,
      readConfirmedImportedHistoryContinuation,
      registerProviderRuntime: (input) => registerProviderRuntimeEffect(input).pipe(
        Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      writeIfProviderBindingCurrent: (input) => writeIfProviderBindingCurrentEffect(input).pipe(
        Effect.mapError((cause) => new EventSinkWriteError({ eventCount: input.events.length, cause }))),
      writeIfCurrentProviderRuntimeOwner: (input) => writeIfProviderBindingCurrentEffect(input, true).pipe(
        Effect.mapError((cause) => new EventSinkWriteError({ eventCount: input.events.length, cause }))),
      writeIfCurrentProviderRuntimeOutputOwner: (input) => writeIfCurrentProviderRuntimeOutputOwner(input).pipe(
        Effect.mapError((cause) => new EventSinkWriteError({ eventCount: input.events.length + (input.companionNodes?.length ?? 0), cause }))),
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
      commitCommand: (input) =>
        commitCommandEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              Schema.is(NativeCommandPreconditionError)(cause) || Schema.is(WorktreeOwnershipConflictError)(cause) ||
                Schema.is(OrdinaryCheckout.OrdinaryCheckoutOwnershipError)(cause) ? cause : new EventSinkWriteError({
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
              Schema.is(NativeCommandPreconditionError)(cause) ? cause : new EventSinkWriteError({
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
      readByCommandId: (input) =>
        eventStore.readByCommandId(input).pipe(
          Stream.mapError(
            (cause) =>
              new EventSinkStreamError({
                cause,
              }),
          ),
        ),
    } satisfies EventSinkV2Shape);
  }),
);

/**
 * Event sink layer for application compositions that already own the
 * persistence services. Keeping the outbox instance shared with the worker is
 * important because enqueue notifications are in-memory wakeups backed by the
 * durable SQL queue.
 */
export const layerFromStores = baseLayer.pipe(Layer.provide(NativeCreationRepositoryLayer));

export const layer: Layer.Layer<
  EventSinkV2,
  never,
  EventStore.EventStoreV2 | ProjectionStore.ProjectionStoreV2 | SqlClient.SqlClient
> = baseLayer.pipe(
  Layer.provide(
    Layer.mergeAll(
      CommandReceiptStore.layer,
      EffectOutbox.layer,
      ProjectStore.layer,
      TurnItemPositionStore.layer,
      NativeCreationRepositoryLayer,
    ),
  ),
);
