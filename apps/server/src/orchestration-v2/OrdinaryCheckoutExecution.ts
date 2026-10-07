import {
  CommandId,
  CheckpointId,
  CheckpointScopeId,
  EventId,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderSessionId,
  RunId,
  ThreadId,
  OrchestrationV2CheckpointJson,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { RecordedStoredEventJson } from "./RecordedTypes.ts";
import { NativeProviderRuntimeBindingV1 } from "../nativeCreation/NativeCreationExecutionTypes.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as OrdinaryCheckout from "./OrdinaryCheckoutOwnership.ts";
import type { OrdinaryCheckoutCommitCapture as OrdinaryCheckoutSqlCaptureV1 } from "./OrdinaryCheckoutStore.ts";
import { ProviderNativeOperationContext, ProviderNativeEffectEvidence } from "./ProviderAdapter.ts";
import { ProviderManagedActorClosureV1 } from "./ProviderManagedActorCompletion.ts";

export const OrdinaryCheckoutUseSubjectV1 = Schema.Struct({
  schema: Schema.Literal("t3.ordinary-checkout-use/v1"),
  use: OrdinaryCheckout.OrdinaryCheckoutUseV1,
  source: Schema.Struct({
    projectWorkspaceRoot: Schema.NonEmptyString,
    worktreePath: Schema.NullOr(Schema.NonEmptyString),
  }),
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
export const OrdinaryFinalCheckpointCompletionBasisV1 = Schema.Struct({
  version: Schema.Literal(1),
  schema: Schema.Literal("t3.ordinary-final-checkpoint-basis/v1"),
  checkpointExecution: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1,
  effectId: Schema.NonEmptyString,
  runId: RunId,
  scopeId: CheckpointScopeId,
  joinOrdinal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  managedRetirements: Schema.Array(
    Schema.Struct({
      managedExecution: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1,
      retirementOrdinal: Schema.Int.check(Schema.isGreaterThan(0)),
      closureSha256: Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/)),
    }),
  ),
});
export type OrdinaryFinalCheckpointCompletionBasisV1 =
  typeof OrdinaryFinalCheckpointCompletionBasisV1.Type;
export interface OrdinaryFinalCheckpointCandidateV1 {
  readonly effectId: string;
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly scopeId: CheckpointScopeId;
  readonly originalUse: OrdinaryCheckout.OrdinaryCheckoutUseV1;
  readonly managedExecutions: ReadonlyArray<OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1>;
  readonly expectedAssociationOrdinal: number;
  readonly expectedAttemptCount: number;
}
export type OrdinaryFinalCheckpointClaimResultV1 =
  | { readonly status: "not_ready"; readonly reason: string }
  | {
      readonly status: "claimed" | "already_committed";
      readonly effect: EffectOutbox.OrchestrationEffectV2;
      readonly execution: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1;
      readonly completionBasis: OrdinaryFinalCheckpointCompletionBasisV1;
    };
const OrdinaryPreparedPhysicalFieldsV1 = {
  version: Schema.Literal(1),
  producerId: Schema.NonEmptyString,
  execution: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1,
  targetSource: OrdinaryCheckoutUseSubjectV1.fields.source,
  checkoutPath: Schema.NonEmptyString,
  observedAt: Schema.NonEmptyString,
};
export const OrdinaryPreparedBranchObservationV1 = Schema.Struct({
  ...OrdinaryPreparedPhysicalFieldsV1,
  kind: Schema.Literal("prepared_branch_renamed"),
  oldBranch: Schema.NonEmptyString,
  requestedBranch: Schema.NonEmptyString,
  renamedBranch: Schema.NonEmptyString,
  readback: Schema.Struct({ cwd: Schema.NonEmptyString, refName: Schema.NonEmptyString }),
});
export const OrdinaryPreparedBranchTransitionV1 = Schema.Struct({
  observation: OrdinaryPreparedBranchObservationV1,
  commandId: CommandId,
  eventId: EventId,
  sequence: Schema.Int.check(Schema.isGreaterThan(0)),
  associationOrdinal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
});
export const OrdinaryCheckoutExecutionLivenessV1 = Schema.Struct({
  version: Schema.Literal(1),
  schema: Schema.Literal("t3.ordinary-checkout-execution-liveness/v1"),
  kind: Schema.Literals(["bind", "join", "renew"]),
  expiresAt: Schema.NonEmptyString,
  previousExpiry: Schema.optionalKey(Schema.NonEmptyString),
  completionBasis: Schema.optionalKey(OrdinaryFinalCheckpointCompletionBasisV1),
});
export const OrdinaryManagedStartObservationV1 = Schema.Struct({
  kind: Schema.Literal("dispatch_returned"),
  settlementMode: Schema.optionalKey(
    Schema.Literals(["primary_terminal_checkpoint", "managed_actor_completion"]),
  ),
  startExecution: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1,
  managedExecutor: OrdinaryCheckout.OrdinaryCheckoutExecutionExecutorV1,
  observedAt: Schema.NonEmptyString,
});
const transferSha256 = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
export const NativeStartTransferReceiptV1 = Schema.Struct({
  schema: Schema.Literal("t3.native-start-managed-transfer/v1"),
  version: Schema.Literal(1),
  captureId: Schema.NonEmptyString,
  checkpointScopeId: CheckpointScopeId,
  originalAdmission: OrdinaryCheckout.OrdinaryCheckoutAdmissionV1,
  originalUseSha256: transferSha256,
  startExecutionSha256: transferSha256,
  claimPayloadSha256: transferSha256,
  participantOrdinal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  historyTailOrdinal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  historyTailSha256: transferSha256,
  claimLeaseExpiresAt: Schema.NonEmptyString,
  enteredAt: Schema.NonEmptyString,
  entryValidatedAt: Schema.NonEmptyString,
  effectId: Schema.NonEmptyString,
  commandId: CommandId,
  workerId: Schema.NonEmptyString,
  expectedAttempt: Schema.Int.check(Schema.isGreaterThan(0)),
  driver: Schema.Literal("codex"),
  binding: Schema.Struct({
    ...NativeProviderRuntimeBindingV1.fields,
    nativeThreadId: Schema.NonEmptyString,
  }),
  evidenceRevision: Schema.Int.check(Schema.isGreaterThan(0)),
  nativeAcknowledgment: Schema.Struct({
    method: Schema.Literal("turn/start"),
    nativeTurnId: Schema.NonEmptyString,
    observedAt: Schema.NonEmptyString,
  }),
  confirmationSha256: transferSha256,
  successorSha256: transferSha256,
  returnedAt: Schema.NonEmptyString,
});
export type NativeStartTransferReceiptV1 = typeof NativeStartTransferReceiptV1.Type;
export const OrdinaryCheckoutExecutionActivationV1 = Schema.Struct({
  version: Schema.Literal(1),
  schema: Schema.Literal("t3.ordinary-checkout-execution-activation/v1"),
  kind: Schema.Literal("activate"),
  expiresAt: Schema.NonEmptyString,
  actualStartObservation: OrdinaryManagedStartObservationV1,
  nativeStartTransfer: Schema.optionalKey(NativeStartTransferReceiptV1),
});
export const OrdinaryCheckpointProducerObservationV1 = Schema.Struct({
  version: Schema.Literal(1),
  kind: Schema.Literal("captured"),
  checkpoint: OrchestrationV2CheckpointJson,
  commit: Schema.Struct({
    receipt: CommandReceiptStore.CommandReceiptV2.mapFields((fields) => ({
      ...fields,
      acceptedAt: Schema.DateTimeUtcFromString,
    })),
    storedEvents: Schema.Array(RecordedStoredEventJson),
    committed: Schema.Boolean,
    cancelledEffectCount: Schema.Int.check(Schema.makeFilter((value) => value >= 0)),
  }),
  ordinaryCheckoutExecution: Schema.optionalKey(OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1),
  ordinaryFinalCheckpointBasis: Schema.optionalKey(OrdinaryFinalCheckpointCompletionBasisV1),
});
export type OrdinaryCheckpointProducerObservationV1 =
  typeof OrdinaryCheckpointProducerObservationV1.Type;
export const OrdinaryCheckpointExecutorOutcomeV1 = Schema.Struct({
  kind: Schema.Literal("checkpoint_captured"),
  observation: OrdinaryCheckpointProducerObservationV1,
});
export const OrdinaryPreparedSetupObservationV1 = Schema.Struct({
  ...OrdinaryPreparedPhysicalFieldsV1,
  kind: Schema.Literal("prepared_setup_completed"),
  branch: Schema.NullOr(Schema.String),
  readback: Schema.optionalKey(
    Schema.Struct({
      cwd: Schema.NonEmptyString,
      refName: Schema.NullOr(Schema.String),
      isRepo: Schema.Boolean,
    }),
  ),
  worktree: Schema.NullOr(
    Schema.Struct({
      path: Schema.NonEmptyString,
      refName: Schema.NonEmptyString,
      headSha: Schema.optionalKey(Schema.String),
    }),
  ),
  setup: Schema.Union([
    Schema.Struct({ status: Schema.Literal("no-script") }),
    Schema.Struct({
      status: Schema.Literal("completed"),
      scriptId: Schema.NonEmptyString,
      terminalId: Schema.NonEmptyString,
      cwd: Schema.NonEmptyString,
      exitCode: Schema.Int,
      durationMs: Schema.Number.check(
        Schema.makeFilter((value) => Number.isFinite(value) && value >= 0),
      ),
    }),
  ]),
});
export type OrdinaryPreparedSetupObservationV1 = typeof OrdinaryPreparedSetupObservationV1.Type;
export const OrdinaryPreparedExecutorOutcomeV1 = Schema.Struct({
  kind: Schema.Literal("prepared_completed"),
  observation: OrdinaryPreparedSetupObservationV1,
});
// A failed preparation ends only through its retained producer's own readback: the
// verified checkout and ref, and no T3-managed terminal left for the original birth.
export const OrdinaryPreparedFailureObservationV1 = Schema.Struct({
  ...OrdinaryPreparedPhysicalFieldsV1,
  kind: Schema.Literal("prepared_failure_observed"),
  branch: Schema.NullOr(Schema.String),
  readback: Schema.Struct({
    cwd: Schema.NonEmptyString,
    refName: Schema.NullOr(Schema.String),
    isRepo: Schema.Boolean,
  }),
  worktree: OrdinaryPreparedSetupObservationV1.fields.worktree,
  failure: Schema.NonEmptyString,
  setup: Schema.Struct({
    status: Schema.Literal("no_managed_process"),
    managerId: Schema.NonEmptyString,
    ownerBirth: OrdinaryCheckout.OrdinaryApplicationBirthV1,
    targetCount: Schema.Literal(0),
  }),
});
export const OrdinaryPreparedPrecreationFailureObservationV1 = Schema.Struct({
  ...OrdinaryPreparedPhysicalFieldsV1,
  kind: Schema.Literal("prepared_precreation_failure_observed"),
  branch: Schema.NullOr(Schema.String),
  readback: Schema.Struct({
    cwd: Schema.NonEmptyString,
    refName: Schema.NullOr(Schema.String),
    isRepo: Schema.Literal(true),
  }),
  parentHeadBefore: Schema.NonEmptyString,
  parentHeadAfter: Schema.NonEmptyString,
  containerPath: Schema.NonEmptyString,
  structuralParentPath: Schema.NonEmptyString,
  plannedChildPath: Schema.NonEmptyString,
  targetState: Schema.Literal("absent"),
  worktree: Schema.Null,
  failure: Schema.NonEmptyString,
  setup: Schema.Struct({
    status: Schema.Literal("no_managed_process"),
    managerId: Schema.NonEmptyString,
    ownerBirth: OrdinaryCheckout.OrdinaryApplicationBirthV1,
    targetCount: Schema.Literal(0),
  }),
});
export const OrdinaryPreparedFailedExecutorOutcomeV1 = Schema.Struct({
  kind: Schema.Literal("prepared_failed"),
  observation: Schema.Union([
    OrdinaryPreparedFailureObservationV1,
    OrdinaryPreparedPrecreationFailureObservationV1,
  ]),
});
export const OrdinaryRollbackProducerObservationV1 = Schema.Struct({
  version: Schema.Literal(1),
  kind: Schema.Literal("rolled_back"),
  sourceEffect: Schema.Struct({ effectId: Schema.NonEmptyString, commandId: CommandId }),
  threadId: ThreadId,
  providerThreadId: ProviderThreadId,
  checkpointId: CheckpointId,
  scopeId: CheckpointScopeId,
  execution: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1,
  completedAt: Schema.NonEmptyString,
  pruneEffectId: Schema.NonEmptyString,
  storedEvents: Schema.Array(RecordedStoredEventJson),
});
export type OrdinaryRollbackProducerObservationV1 =
  typeof OrdinaryRollbackProducerObservationV1.Type;
export const OrdinaryRollbackExecutorOutcomeV1 = Schema.Struct({
  kind: Schema.Literal("rollback_completed"),
  observation: OrdinaryRollbackProducerObservationV1,
});
export const StartFailedBeforeOpenObservationV1 = Schema.Struct({
  version: Schema.Literal(1),
  schema: Schema.Literal("t3.start-failed-before-open/v1"),
  execution: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1,
  run: OrdinaryCheckout.OrdinaryAcceptedRunV1,
  providerInstanceId: ProviderInstanceId,
  providerThreadId: ProviderThreadId,
  providerSessionId: ProviderSessionId,
  checkpointScopeId: Schema.NullOr(CheckpointScopeId),
  attemptedOperation: ProviderNativeOperationContext,
  nativeEffect: ProviderNativeEffectEvidence,
  completedAt: Schema.NonEmptyString,
  terminalEvents: Schema.Array(
    Schema.Struct({ eventId: EventId, sequence: Schema.Int.check(Schema.isGreaterThan(0)) }),
  ),
  terminalPayloadSha256: Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/)),
});
export type StartFailedBeforeOpenObservationV1 = typeof StartFailedBeforeOpenObservationV1.Type;
export const StartRetryBeforeOpenObservationV1 = Schema.Struct({
  version: Schema.Literal(1),
  schema: Schema.Literal("t3.start-retry-before-open/v1"),
  execution: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1,
  run: OrdinaryCheckout.OrdinaryAcceptedRunV1,
  providerInstanceId: ProviderInstanceId,
  providerThreadId: ProviderThreadId,
  providerSessionId: ProviderSessionId,
  checkpointScopeId: Schema.NullOr(CheckpointScopeId),
  attemptedOperation: ProviderNativeOperationContext,
  nativeEffect: ProviderNativeEffectEvidence,
  completedAt: Schema.NonEmptyString,
});
export type StartRetryBeforeOpenObservationV1 = typeof StartRetryBeforeOpenObservationV1.Type;
export const OrdinaryRetryStartExecutorOutcomeV1 = Schema.Struct({
  kind: Schema.Literal("start_retry_before_open"),
  observation: StartRetryBeforeOpenObservationV1,
  availableAt: Schema.NonEmptyString,
  error: Schema.String,
});
export const OrdinaryFailedStartExecutorOutcomeV1 = Schema.Struct({
  kind: Schema.Literal("start_failed_before_open"),
  observation: StartFailedBeforeOpenObservationV1,
});
export const OrdinaryCheckoutExecutorOutcomeV1 = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("outbox_completed"),
    effectId: Schema.NonEmptyString,
    workerId: Schema.NonEmptyString,
    expectedAttempt: Schema.Int.check(Schema.makeFilter((value) => value > 0)),
    completedAt: Schema.DateTimeUtcFromString,
  }),
  OrdinaryCheckpointExecutorOutcomeV1,
  OrdinaryFailedStartExecutorOutcomeV1,
  OrdinaryRetryStartExecutorOutcomeV1,
  OrdinaryPreparedExecutorOutcomeV1,
  OrdinaryPreparedFailedExecutorOutcomeV1,
  OrdinaryRollbackExecutorOutcomeV1,
  Schema.Struct({
    kind: Schema.Literal("managed_mutations_finished"),
    observation: ProviderManagedActorClosureV1,
  }),
  Schema.Struct({
    kind: Schema.Literal("start_activated"),
    managedExecution: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1,
    actualStartObservation: OrdinaryManagedStartObservationV1,
  }),
  Schema.Struct({
    kind: Schema.Literal("unknown"),
    reason: Schema.NonEmptyString,
    observedAt: Schema.DateTimeUtcFromString,
  }),
]);
export type OrdinaryCheckoutExecutorOutcomeV1 = typeof OrdinaryCheckoutExecutorOutcomeV1.Type;
export const OrdinaryCheckoutCompletionEvidenceV1 = Schema.Struct({
  ref: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1,
  actualProducerOutcome: Schema.Union([
    OrdinaryCheckpointExecutorOutcomeV1,
    OrdinaryPreparedExecutorOutcomeV1,
    OrdinaryPreparedFailedExecutorOutcomeV1,
    OrdinaryFailedStartExecutorOutcomeV1,
  ]),
});
export type OrdinaryCheckoutCompletionEvidenceV1 = typeof OrdinaryCheckoutCompletionEvidenceV1.Type;
export const OrdinaryCheckoutExecutionOutcomeFactV1 = Schema.Struct({
  version: Schema.Literal(1),
  schema: Schema.Literal("t3.ordinary-checkout-execution-outcome/v1"),
  kind: Schema.Literals(["retire", "unknown"]),
  expiresAt: Schema.NonEmptyString,
  actualProducerOutcome: OrdinaryCheckoutExecutorOutcomeV1,
});
export const OrdinaryCheckoutExecutionEvidenceV1 = Schema.Union([
  OrdinaryCheckoutExecutionLivenessV1,
  OrdinaryCheckoutExecutionActivationV1,
  OrdinaryCheckoutExecutionOutcomeFactV1,
]);
export interface OrdinaryCheckoutExecutionAssociationFactV1 {
  readonly ordinal: number;
  readonly predecessorOrdinal: number | null;
  readonly ref: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1;
  readonly eventKind: "bind" | "activate" | "join" | "renew" | "retire" | "unknown";
  readonly evidence: typeof OrdinaryCheckoutExecutionEvidenceV1.Type;
  readonly recordedAt: string;
}
export interface OrdinaryCheckoutExecutionAssociationsV1 {
  readonly originalUse: OrdinaryCheckout.OrdinaryCheckoutUseV1;
  readonly latestOrdinal: number;
  readonly facts: ReadonlyArray<OrdinaryCheckoutExecutionAssociationFactV1>;
  readonly participants: ReadonlyArray<{
    readonly ref: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1;
    readonly state: "active" | "retired" | "unknown";
    readonly latestOrdinal: number;
    readonly expiresAt: string;
  }>;
}
