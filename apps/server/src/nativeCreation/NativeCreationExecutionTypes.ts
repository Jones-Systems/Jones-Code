import * as Schema from "effect/Schema";
import {
  CommandId,
  EventId,
  IsoDateTime,
  MessageId,
  NonNegativeInt,
  NodeId,
  PositiveInt,
  PlanId,
  RunId,
  ThreadId,
  ScheduledTaskId,
  TrimmedNonEmptyString,
  NativeCreationEffect,
  NativeCreationHistoricalBinding,
  OrchestrationV2Actor,
  OrchestrationV2CreationSource,
  OrchestrationV2Notification,
  OrchestrationMessageContext,
  ChatAttachment,
  ModelSelection,
  RuntimeMode,
  ProviderInteractionMode,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderDriverKind,
  AuthSessionId,
  OrchestrationV2Command,
} from "@t3tools/contracts";
import { nativeCreationCanonicalJson, nativeCreationSha256 } from "./NativeCreationPreparation.ts";
const closedNativeStruct = <Fields extends Schema.Struct.Fields>(fields: Fields) => {
  const schema = Schema.Struct(fields);
  // Validate original wire keys before struct decoding can discard them.
  return Schema.flip(
    Schema.flip(schema).check(
      Schema.makeFilter((value) =>
        Reflect.ownKeys(value).every((key) => Object.hasOwn(fields, key)),
      ),
    ),
  );
};

export const NativeCommandIdentityV2 = closedNativeStruct({
  kind: Schema.Literals([
    "guarded_message_dispatch",
    "native_creation_stage",
    "workstream_settlement",
  ]),
  version: Schema.Literal(2),
  commandId: CommandId,
  commandType: TrimmedNonEmptyString,
  aggregateKind: Schema.Literal("thread"),
  aggregateId: ThreadId,
  normalizedCommandDigest: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  bindingDigest: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
});
export type NativeCommandIdentityV2 = typeof NativeCommandIdentityV2.Type;

export const NativeThreadIncarnationV2 = closedNativeStruct({
  eventId: EventId,
  sequence: NonNegativeInt,
});
export type NativeThreadIncarnationV2 = typeof NativeThreadIncarnationV2.Type;

const PlacementId = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/));
const PlacementNamespace = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(512),
  Schema.isPattern(/^[^\u0000-\u001f\u007f]+$/),
);
const PlacementGeneration = Schema.Number.check(
  Schema.isInt(),
  Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
);
const SettlementProviderBinding = closedNativeStruct({
  threadId: ThreadId,
  providerThreadId: ProviderThreadId,
  providerSessionId: ProviderSessionId,
  instanceId: ProviderInstanceId,
  driver: ProviderDriverKind,
  nativeThreadId: Schema.NullOr(Schema.String),
  runtimeGeneration: Schema.NullOr(Schema.NonEmptyString),
});
export const NativeWorkstreamSettlementWitnessV2 = closedNativeStruct({
  version: Schema.Literal(2),
  command: OrchestrationV2Command,
  attemptKey: closedNativeStruct({
    owner_id: Schema.NonEmptyString,
    principal_id: Schema.NonEmptyString,
    command_id: Schema.NonEmptyString,
  }),
  dispatchStartedAt: Schema.NonEmptyString,
  actorSessionId: AuthSessionId,
  enrollmentSha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  requestBytesSha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  authority: closedNativeStruct({
    environmentId: PlacementId,
    authorityNamespace: PlacementNamespace,
    storeGeneration: PlacementGeneration,
  }),
  incarnation: NativeThreadIncarnationV2,
  targetEventSequence: NonNegativeInt,
  provider: Schema.NullOr(
    closedNativeStruct({
      binding: SettlementProviderBinding,
      evidenceRevision: Schema.Int.check(Schema.isGreaterThan(0)),
    }),
  ),
});
export type NativeWorkstreamSettlementWitnessV2 = typeof NativeWorkstreamSettlementWitnessV2.Type;
export const nativeWorkstreamSettlementWitnessBindingDigestV2 = (
  witness: NativeWorkstreamSettlementWitnessV2,
): string =>
  nativeCreationSha256(
    nativeCreationCanonicalJson({
      schema: "t3.workstream-settlement-binding/v2",
      witness: Schema.encodeSync(NativeWorkstreamSettlementWitnessV2)(witness),
    }),
  );

const NativeCommandReceiptObservationV2Fields = {
  commandId: CommandId,
  threadId: ThreadId,
  commandType: Schema.String,
  acceptedAt: Schema.DateTimeUtc,
  resultSequence: NonNegativeInt,
  status: Schema.Literals(["accepted", "rejected"]),
  error: Schema.NullOr(Schema.String),
};
export const NativeCommandReceiptObservationV2 = closedNativeStruct(
  NativeCommandReceiptObservationV2Fields,
);
export type NativeCommandReceiptObservationV2 = typeof NativeCommandReceiptObservationV2.Type;
export const NativeCommandReceiptObservationV2Json = closedNativeStruct({
  ...NativeCommandReceiptObservationV2Fields,
  acceptedAt: Schema.DateTimeUtcFromString,
});
export type NativeCommandReceiptObservationV2Json =
  typeof NativeCommandReceiptObservationV2Json.Type;

export const NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS = 256;
export const NATIVE_CREATION_OBSERVATION_V2_MAX_STAGES = 3;

const NativeCreationV2String = Schema.String.check(Schema.isNonEmpty());
const NativeCreationV2Sha256 = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
const NativeCreationV2CommandType = Schema.Literals([
  "thread.create",
  "message.dispatch",
  "prepared-run.release",
]);
const NativeCreationEffectV2Fields = {
  version: Schema.Literal(2),
  kind: Schema.Literal("native_command"),
  effectId: NativeCreationV2String,
  ordinal: NonNegativeInt,
  timestamp: IsoDateTime,
  commandId: CommandId,
  threadId: ThreadId,
  commandType: NativeCreationV2CommandType,
  commandDigest: NativeCreationV2Sha256,
};

export const NativeCreationEffectV2 = Schema.Union([
  closedNativeStruct({
    ...NativeCreationEffectV2Fields,
    phase: Schema.Literal("started"),
  }),
  closedNativeStruct({
    ...NativeCreationEffectV2Fields,
    phase: Schema.Literal("completed"),
    eventId: EventId,
    sequence: NonNegativeInt,
  }),
]);
export type NativeCreationEffectV2 = typeof NativeCreationEffectV2.Type;

const NativeCreationStageObservationV2Fields = {
  claimId: NativeCreationV2String,
  commandId: CommandId,
  threadId: ThreadId,
  commandType: NativeCreationV2CommandType,
  commandDigest: NativeCreationV2Sha256,
  event: Schema.NullOr(NativeThreadIncarnationV2),
};
const NativeCreationStageObservationV2 = closedNativeStruct({
  ...NativeCreationStageObservationV2Fields,
  receipt: Schema.NullOr(NativeCommandReceiptObservationV2),
});
const NativeCreationStageObservationV2Json = closedNativeStruct({
  ...NativeCreationStageObservationV2Fields,
  receipt: Schema.NullOr(NativeCommandReceiptObservationV2Json),
});

const NativeCreationObservationV2Fields = {
  version: Schema.Literal(2),
  schema: Schema.Literal("t3.native-creation-observation/v2"),
  preparationId: NativeCreationV2String,
  operationId: NativeCreationV2String,
  preparationSha256: NativeCreationV2Sha256,
  bindingDigest: NativeCreationV2Sha256,
  promptDigest: NativeCreationV2Sha256,
  commandDigest: NativeCreationV2Sha256,
  normalizedCommandDigest: Schema.NullOr(NativeCreationV2Sha256),
  claimId: NativeCreationV2String,
  claimedBootId: NativeCreationV2String,
  claimedAt: IsoDateTime,
  actorSessionId: NativeCreationV2String,
  grantId: NativeCreationV2String,
  grantRevision: Schema.Int.check(
    Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
  ),
  binding: NativeCreationHistoricalBinding,
  incarnation: Schema.NullOr(NativeThreadIncarnationV2),
  effectsV1: Schema.Array(NativeCreationEffect).check(
    Schema.isMaxLength(NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS),
  ),
  effectsV2: Schema.Array(NativeCreationEffectV2).check(
    Schema.isMaxLength(NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS),
  ),
  unresolvedEffects: Schema.Array(NativeCreationV2String).check(
    Schema.isMaxLength(NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS),
  ),
  outcome: Schema.Literals(["complete", "in_progress", "incomplete", "unknown"]),
  overflow: Schema.Boolean,
};

// Recorded history contains bounded attestation and attribution; canonical bodies remain server-owned.
export const NativeCreationObservationV2 = closedNativeStruct({
  ...NativeCreationObservationV2Fields,
  stageCommands: Schema.Array(NativeCreationStageObservationV2).check(
    Schema.isMaxLength(NATIVE_CREATION_OBSERVATION_V2_MAX_STAGES),
  ),
  finalReceipt: Schema.NullOr(NativeCommandReceiptObservationV2),
}).check(
  Schema.makeFilter((observation) => !observation.overflow || observation.outcome === "unknown"),
);
export type NativeCreationObservationV2 = typeof NativeCreationObservationV2.Type;

export const NativeCreationObservationV2Json = closedNativeStruct({
  ...NativeCreationObservationV2Fields,
  stageCommands: Schema.Array(NativeCreationStageObservationV2Json).check(
    Schema.isMaxLength(NATIVE_CREATION_OBSERVATION_V2_MAX_STAGES),
  ),
  finalReceipt: Schema.NullOr(NativeCommandReceiptObservationV2Json),
}).check(
  Schema.makeFilter((observation) => !observation.overflow || observation.outcome === "unknown"),
);
export type NativeCreationObservationV2Json = typeof NativeCreationObservationV2Json.Type;

const executionReferenceFields = {
  version: Schema.Literal(2),
  claimId: Schema.String.check(Schema.isNonEmpty()),
  stageCommandId: CommandId,
  effectId: Schema.String.check(Schema.isNonEmpty()),
  stage: Schema.Literals([
    "claim",
    "normalization",
    "tracker_registration",
    "bootstrap_detachment",
    "fetch",
    "worktree",
    "worktree_ownership",
    "native_command",
    "setup",
    "setup_detachment",
    "setup_completion_detachment",
    "cleanup",
    "deletion_drain",
    "git_status_refresh",
  ]),
};
const executionReference = Schema.Struct(executionReferenceFields);

// Persisted references identify ledger work; they never establish current authority.
export const NativeCreationExecutionReferenceV2 = Schema.flip(
  Schema.flip(executionReference).check(
    Schema.makeFilter((value) =>
      Reflect.ownKeys(value).every((key) => Object.hasOwn(executionReferenceFields, key)),
    ),
  ),
);
export type NativeCreationExecutionReferenceV2 = typeof NativeCreationExecutionReferenceV2.Type;

const OrchestrationV2MessageDispatchCommand = Schema.Struct({
  type: Schema.Literal("message.dispatch"),
  notification: Schema.optional(OrchestrationV2Notification),
  createdBy: OrchestrationV2Actor,
  creationSource: OrchestrationV2CreationSource,
  scheduledTaskId: Schema.optional(ScheduledTaskId),
  senderThreadId: Schema.optional(ThreadId),
  commandId: CommandId,
  threadId: ThreadId,
  messageId: MessageId,
  text: Schema.String,
  context: Schema.optional(OrchestrationMessageContext),
  attachments: Schema.Array(ChatAttachment),
  /** Seed the temporary title and generate a durable replacement for the first message. */
  titleSeed: Schema.optional(TrimmedNonEmptyString),
  modelSelection: Schema.optional(ModelSelection),
  sourcePlanRef: Schema.optional(Schema.Struct({ threadId: ThreadId, planId: PlanId })),
  restartContinuationOfRunId: Schema.optional(RunId),
  usageLimitContinuationOfRunId: Schema.optional(RunId),
  manualContinuationOfRunId: Schema.optional(RunId),
  usageLimitRecoveryRequestId: Schema.optional(CommandId),
  /** Resolve untargeted delivery against the server's serialized thread state. */
  deliveryIntent: Schema.optional(Schema.Literals(["auto", "steer", "restart"])),
  delegatedCompletion: Schema.optional(
    Schema.Struct({
      parentRunId: RunId,
      generation: PositiveInt,
      taskIds: Schema.Array(NodeId),
    }),
  ),
  dispatchMode: Schema.Union([
    Schema.Struct({ type: Schema.Literal("defer_start") }),
    Schema.Struct({ type: Schema.Literal("steer_active"), targetRunId: RunId }),
    Schema.Struct({ type: Schema.Literal("restart_active"), targetRunId: RunId }),
    Schema.Struct({ type: Schema.Literal("queue_after_active") }),
    Schema.Struct({ type: Schema.Literal("start_immediately") }),
  ]),
});

// The opaque basis describes a reviewed read; it grants no provider or mutation authority.
export const OrchestrationV2ImportedHistoryReviewBasis = TrimmedNonEmptyString.pipe(
  Schema.brand("OrchestrationV2ImportedHistoryReviewBasis"),
);
export type OrchestrationV2ImportedHistoryReviewBasis =
  typeof OrchestrationV2ImportedHistoryReviewBasis.Type;

export const OrchestrationV2ImportedHistoryDelivery = Schema.Union([
  closedNativeStruct({
    type: Schema.Literal("message"),
    messageId: OrchestrationV2MessageDispatchCommand.fields.messageId,
    text: OrchestrationV2MessageDispatchCommand.fields.text,
    attachments: OrchestrationV2MessageDispatchCommand.fields.attachments,
    context: OrchestrationV2MessageDispatchCommand.fields.context,
    modelSelection: OrchestrationV2MessageDispatchCommand.fields.modelSelection,
    runtimeMode: RuntimeMode,
    interactionMode: ProviderInteractionMode,
    titleSeed: OrchestrationV2MessageDispatchCommand.fields.titleSeed,
    sourcePlanRef: OrchestrationV2MessageDispatchCommand.fields.sourcePlanRef,
    deliveryIntent: OrchestrationV2MessageDispatchCommand.fields.deliveryIntent,
    dispatchMode: OrchestrationV2MessageDispatchCommand.fields.dispatchMode,
  }),
  closedNativeStruct({
    type: Schema.Literal("queued_run"),
    runId: RunId,
    messageId: MessageId,
  }),
]);
export type OrchestrationV2ImportedHistoryDelivery =
  typeof OrchestrationV2ImportedHistoryDelivery.Type;

export const OrchestrationV2StartWithImportedHistoryCommand = closedNativeStruct({
  type: Schema.Literal("thread.imported-history.start"),
  commandId: CommandId,
  threadId: ThreadId,
  reviewedBasis: OrchestrationV2ImportedHistoryReviewBasis,
  delivery: OrchestrationV2ImportedHistoryDelivery,
});
export type OrchestrationV2StartWithImportedHistoryCommand =
  typeof OrchestrationV2StartWithImportedHistoryCommand.Type;

export const NativeProviderRuntimeBindingV1 = Schema.Struct({
  threadId: ThreadId,
  providerThreadId: ProviderThreadId,
  providerSessionId: ProviderSessionId,
  instanceId: ProviderInstanceId,
  runtimeGeneration: TrimmedNonEmptyString,
  nativeThreadId: Schema.optional(TrimmedNonEmptyString),
});
export type NativeProviderRuntimeBindingV1 = typeof NativeProviderRuntimeBindingV1.Type;

export const NativeProviderContinuationSourceIdentityV1 = Schema.Struct({
  driverKind: ProviderDriverKind,
  continuationKey: TrimmedNonEmptyString,
  runtimeGeneration: TrimmedNonEmptyString,
});
export type NativeProviderContinuationSourceIdentityV1 =
  typeof NativeProviderContinuationSourceIdentityV1.Type;

export const NativeProviderRuntimeObservationV1 = Schema.Union([
  Schema.Struct({
    status: Schema.Literals(["working", "monitoring", "busy", "idle"]),
    binding: NativeProviderRuntimeBindingV1,
    observedAt: Schema.String,
  }),
  Schema.Struct({
    status: Schema.Literal("unknown"),
    binding: Schema.optional(NativeProviderRuntimeBindingV1),
    reason: Schema.String,
  }),
]);
export type NativeProviderRuntimeObservationV1 = typeof NativeProviderRuntimeObservationV1.Type;
