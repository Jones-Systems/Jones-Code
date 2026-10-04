import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import { CommandId, EventId, IsoDateTime, MessageId, NonNegativeInt, ProjectId, ProviderSessionId, ProviderThreadId, ProviderTurnId, RunAttemptId, RunId, ThreadId, TrimmedNonEmptyString, TurnId } from "./baseSchemas.ts";
import { OrchestrationMessageContext } from "./composerContext.ts";
import { ChatAttachment } from "./chatAttachment.ts";
import { ModelSelection } from "./modelSelection.ts";
import { NativeCreationEffect, NativeCreationHistoricalBinding, NativeCreationObservation } from "./nativeCreation.ts";
import { OrchestrationV2ImportedHistoryReviewBasis, OrchestrationV2ProviderSession, OrchestrationV2RunStatus } from "./orchestrationV2.ts";
import { ProviderDriverKind, ProviderInstanceId } from "./providerInstance.ts";
import { DEFAULT_PROVIDER_INTERACTION_MODE, DEFAULT_RUNTIME_MODE, ProviderInteractionMode, RuntimeMode } from "./providerPolicy.ts";

const closedNativeStruct = <Fields extends Schema.Struct.Fields>(fields: Fields) => {
  const schema = Schema.Struct(fields);
  // Validate original wire keys before struct decoding can discard them.
  return Schema.flip(Schema.flip(schema).check(Schema.makeFilter(
    (value) => Reflect.ownKeys(value).every((key) => Object.hasOwn(fields, key)),
  )));
};

const OrchestrationV2CurrentRuntimeBinding = closedNativeStruct({
  threadId: ThreadId,
  providerThreadId: ProviderThreadId,
  providerSessionId: ProviderSessionId,
  instanceId: ProviderInstanceId,
  runtimeGeneration: TrimmedNonEmptyString,
  nativeThreadId: Schema.optional(TrimmedNonEmptyString),
});

export const OrchestrationV2ThreadRuntimeAttachment = Schema.Union([
  closedNativeStruct({
    status: Schema.Literal("attached"),
    binding: OrchestrationV2CurrentRuntimeBinding,
    driver: ProviderDriverKind,
    runtimeStatus: OrchestrationV2ProviderSession.fields.status,
    evidenceRevision: NonNegativeInt,
    observedAt: Schema.String,
  }),
  closedNativeStruct({
    status: Schema.Literal("stopped"),
    reason: Schema.Literal("runtime_not_resident"),
    observedAt: Schema.String,
  }),
  closedNativeStruct({
    status: Schema.Literal("unknown"),
    reason: Schema.String,
    observedAt: Schema.String,
  }),
]);
export type OrchestrationV2ThreadRuntimeAttachment =
  typeof OrchestrationV2ThreadRuntimeAttachment.Type;

export const OrchestrationV2ThreadRuntimeAttachmentResult = closedNativeStruct({
  threadId: ThreadId,
  attachment: OrchestrationV2ThreadRuntimeAttachment,
  // Feature availability does not attest authorization, currentness, or a completed stop.
  stopCapability: Schema.optionalKey(Schema.NullOr(closedNativeStruct({ version: Schema.Literal(2) }))),
});
export type OrchestrationV2ThreadRuntimeAttachmentResult =
  typeof OrchestrationV2ThreadRuntimeAttachmentResult.Type;


export const OrchestrationV2ThreadRuntimeObservation = Schema.Union([
  closedNativeStruct({
    status: Schema.Literals(["working", "monitoring", "busy", "idle"]),
    binding: OrchestrationV2CurrentRuntimeBinding,
    observedAt: Schema.String,
  }),
  closedNativeStruct({
    status: Schema.Literal("unknown"),
    binding: Schema.optional(OrchestrationV2CurrentRuntimeBinding),
    reason: Schema.String,
  }),
]);
export type OrchestrationV2ThreadRuntimeObservation =
  typeof OrchestrationV2ThreadRuntimeObservation.Type;

export const OrchestrationV2ThreadRuntimeObservationResult = closedNativeStruct({
  threadId: ThreadId,
  observation: OrchestrationV2ThreadRuntimeObservation,
}).check(Schema.makeFilter((result) =>
  result.observation.binding === undefined || result.observation.binding.threadId === result.threadId,
));
export type OrchestrationV2ThreadRuntimeObservationResult =
  typeof OrchestrationV2ThreadRuntimeObservationResult.Type;

export const OrchestrationV2OperatingCountsResult = closedNativeStruct({
  total: NonNegativeInt,
  operating: NonNegativeInt,
  foregroundWaitingApproval: NonNegativeInt,
  foregroundWaitingInput: NonNegativeInt,
  foregroundWaitingPlan: NonNegativeInt,
  backgroundOperating: NonNegativeInt,
  backgroundUnknown: NonNegativeInt,
  snapshotSequence: NonNegativeInt,
  observedAt: Schema.String,
  backgroundSampledAt: Schema.String,
});
export type OrchestrationV2OperatingCountsResult =
  typeof OrchestrationV2OperatingCountsResult.Type;

export const OrchestrationV2CurrentThreadRuntimeTarget = closedNativeStruct({
  binding: OrchestrationV2CurrentRuntimeBinding,
  driver: ProviderDriverKind,
  evidenceRevision: NonNegativeInt,
});
export type OrchestrationV2CurrentThreadRuntimeTarget =
  typeof OrchestrationV2CurrentThreadRuntimeTarget.Type;


// V1 codecs describe historical native requests and observations; live dispatch uses V2.
export const OrchestrationSessionStatus = Schema.Literals([
  "idle",
  "starting",
  "running",
  "ready",
  "interrupted",
  "stopped",
  "error",
]);
export type OrchestrationSessionStatus = typeof OrchestrationSessionStatus.Type;

const ThreadTurnStartBootstrapCreateThread = Schema.Struct({
  projectId: ProjectId,
  title: TrimmedNonEmptyString,
  modelSelection: ModelSelection,
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode,
  branch: Schema.NullOr(TrimmedNonEmptyString),
  worktreePath: Schema.NullOr(TrimmedNonEmptyString),
  createdAt: IsoDateTime,
});

const ThreadTurnStartBootstrapPrepareWorktree = Schema.Struct({
  projectCwd: TrimmedNonEmptyString,
  baseBranch: TrimmedNonEmptyString,
  branch: Schema.optional(TrimmedNonEmptyString),
  startFromOrigin: Schema.optional(Schema.Boolean),
  requireWorktree: Schema.optional(Schema.Boolean),
});

const ThreadTurnStartBootstrap = Schema.Struct({
  createThread: Schema.optional(ThreadTurnStartBootstrapCreateThread),
  prepareWorktree: Schema.optional(ThreadTurnStartBootstrapPrepareWorktree),
  runSetupScript: Schema.optional(Schema.Boolean),
});

export type ThreadTurnStartBootstrap = typeof ThreadTurnStartBootstrap.Type;

export const ThreadTurnDispatchGuard = Schema.Struct({
  observedSnapshotSequence: NonNegativeInt,
  expectedModelSelection: ModelSelection,
  expectedSessionStatus: Schema.NullOr(OrchestrationSessionStatus),
  expectedActiveTurnId: Schema.NullOr(TurnId),
  expectedLatestTurnId: Schema.NullOr(TurnId),
  requireIdle: Schema.Literal(true),
});
export type ThreadTurnDispatchGuard = typeof ThreadTurnDispatchGuard.Type;

export const OrchestrationDispatchTarget = Schema.Struct({
  modelSelection: ModelSelection,
  sessionStatus: Schema.NullOr(OrchestrationSessionStatus),
  activeTurnId: Schema.NullOr(TurnId),
  latestTurnId: Schema.NullOr(TurnId),
  requireIdle: Schema.Literal(true),
  idle: Schema.Boolean,
  blockers: Schema.Array(
    Schema.Literals([
      "archived",
      "settled",
      "pending_turn",
      "running_turn",
      "session_starting",
      "session_running",
      "active_turn",
      "pending_approval",
      "pending_user_input",
      "actionable_plan",
      "background_work",
    ]),
  ),
});
export type OrchestrationDispatchTarget = typeof OrchestrationDispatchTarget.Type;

export const OrchestrationObservedTurn = Schema.Struct({
  turnId: Schema.NullOr(TurnId),
  state: Schema.Literals(["pending", "running", "interrupted", "completed", "error"]),
  requestedAt: IsoDateTime,
  startedAt: Schema.NullOr(IsoDateTime),
  completedAt: Schema.NullOr(IsoDateTime),
  assistantMessageId: Schema.NullOr(MessageId),
});
export type OrchestrationObservedTurn = typeof OrchestrationObservedTurn.Type;

// Correlation is historical projection evidence, not proof of task delivery.
export const OrchestrationCommandObservation = Schema.Struct({
  threadId: ThreadId,
  commandId: CommandId,
  messageId: MessageId,
  snapshotSequence: NonNegativeInt,
  commandStatus: Schema.Literals(["accepted", "rejected", "not_found"]),
  acceptedSequence: Schema.NullOr(NonNegativeInt),
  correlation: Schema.Literals(["exact", "pending", "missing", "ambiguous", "mismatched"]),
  turn: Schema.NullOr(OrchestrationObservedTurn),
  target: Schema.NullOr(OrchestrationDispatchTarget),
  creation: Schema.optionalKey(NativeCreationObservation),
});
export type OrchestrationCommandObservation = typeof OrchestrationCommandObservation.Type;

const SourceProposedPlanReference = Schema.Struct({
  threadId: ThreadId,
  planId: TrimmedNonEmptyString,
});

export const ThreadTurnStartCommand = Schema.Struct({
  type: Schema.Literal("thread.turn.start"),
  commandId: CommandId,
  threadId: ThreadId,
  message: Schema.Struct({
    messageId: MessageId,
    role: Schema.Literal("user"),
    text: Schema.String,
    attachments: Schema.Array(ChatAttachment),
    context: Schema.optional(OrchestrationMessageContext),
  }),
  modelSelection: Schema.optional(ModelSelection),
  titleSeed: Schema.optional(TrimmedNonEmptyString),
  runtimeMode: RuntimeMode.pipe(Schema.withDecodingDefault(Effect.succeed(DEFAULT_RUNTIME_MODE))),
  interactionMode: ProviderInteractionMode.pipe(
    Schema.withDecodingDefault(Effect.succeed(DEFAULT_PROVIDER_INTERACTION_MODE)),
  ),
  bootstrap: Schema.optional(ThreadTurnStartBootstrap),
  dispatchGuard: Schema.optional(ThreadTurnDispatchGuard),
  sourceProposedPlan: Schema.optional(SourceProposedPlanReference),
  createdAt: IsoDateTime,
});


// Producer-canonical V1 strings retain Python stripping semantics; ordinary historical decoding still trims.
export const LegacyNativeBootstrapCommandV1 = closedNativeStruct({
  type: ThreadTurnStartCommand.fields.type,
  commandId: Schema.toType(ThreadTurnStartCommand.fields.commandId),
  threadId: Schema.toType(ThreadTurnStartCommand.fields.threadId),
  message: Schema.toType(ThreadTurnStartCommand.fields.message),
  runtimeMode: ThreadTurnStartCommand.fields.runtimeMode,
  interactionMode: ThreadTurnStartCommand.fields.interactionMode,
  createdAt: ThreadTurnStartCommand.fields.createdAt,
  bootstrap: closedNativeStruct({
    createThread: Schema.toType(ThreadTurnStartBootstrapCreateThread),
    prepareWorktree: closedNativeStruct({
      ...ThreadTurnStartBootstrapPrepareWorktree.fields,
      projectCwd: Schema.toType(ThreadTurnStartBootstrapPrepareWorktree.fields.projectCwd),
      baseBranch: Schema.toType(ThreadTurnStartBootstrapPrepareWorktree.fields.baseBranch),
      branch: Schema.toType(TrimmedNonEmptyString),
      startFromOrigin: Schema.Boolean,
      requireWorktree: Schema.Literal(true),
    }),
    runSetupScript: Schema.Boolean,
  }),
});
export type LegacyNativeBootstrapCommandV1 = typeof LegacyNativeBootstrapCommandV1.Type;


export const NativeCommandIdentityV2 = closedNativeStruct({
  kind: Schema.Literals(["guarded_message_dispatch", "native_creation_stage", "workstream_settlement"]),
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

export const ThreadTurnDispatchGuardV2 = closedNativeStruct({
  version: Schema.Literal(2),
  observedSnapshotSequence: NonNegativeInt,
  expectedIncarnation: NativeThreadIncarnationV2,
  expectedModelSelection: ModelSelection,
  expectedActiveRunId: Schema.NullOr(RunId),
  expectedLatestRunId: Schema.NullOr(RunId),
  expectedActiveRunAttemptId: Schema.NullOr(RunAttemptId),
  expectedActiveProviderThreadId: Schema.NullOr(ProviderThreadId),
  expectedProviderSessionId: Schema.NullOr(ProviderSessionId),
  expectedProviderSessionStatus: Schema.NullOr(OrchestrationV2ProviderSession.fields.status),
  expectedRuntimeGeneration: Schema.optionalKey(TrimmedNonEmptyString),
  requireIdle: Schema.Literal(true),
});
export type ThreadTurnDispatchGuardV2 = typeof ThreadTurnDispatchGuardV2.Type;

export const OrchestrationDispatchBlockerV2 = Schema.Literals([
  "archived", "deleted", "settled", "queued_run", "held_run", "active_run", "active_attempt",
  "provider_turn", "execution_node", "provider_activity", "pending_approval", "pending_user_input",
  "pending_tool", "pending_auth_refresh", "actionable_plan", "subagent_work", "background_work",
  "completion_delivery", "wake_delivery", "pending_native_effect", "unknown_resume", "unresolved_start",
  "unknown_evidence",
]);
export type OrchestrationDispatchBlockerV2 = typeof OrchestrationDispatchBlockerV2.Type;

export const OrchestrationDispatchTargetV2 = closedNativeStruct({
  incarnation: Schema.NullOr(NativeThreadIncarnationV2),
  modelSelection: ModelSelection,
  activeRunId: Schema.NullOr(RunId),
  latestRunId: Schema.NullOr(RunId),
  activeRunAttemptId: Schema.NullOr(RunAttemptId),
  activeProviderThreadId: Schema.NullOr(ProviderThreadId),
  providerSessionId: Schema.NullOr(ProviderSessionId),
  providerSessionStatus: Schema.NullOr(OrchestrationV2ProviderSession.fields.status),
  runtimeGeneration: Schema.optionalKey(TrimmedNonEmptyString),
  snapshotSequence: NonNegativeInt,
  targetEventSequence: NonNegativeInt,
  complete: Schema.Boolean,
  requireIdle: Schema.Literal(true),
  idle: Schema.Boolean,
  blockers: Schema.Array(OrchestrationDispatchBlockerV2),
}).check(Schema.makeFilter((target) =>
  (target.complete || (!target.idle && target.blockers.includes("unknown_evidence"))) &&
  (!target.idle || target.blockers.length === 0),
));
export type OrchestrationDispatchTargetV2 = typeof OrchestrationDispatchTargetV2.Type;

const NativeCommandReceiptObservationV2Fields = {
  commandId: CommandId,
  threadId: ThreadId,
  commandType: Schema.String,
  acceptedAt: Schema.DateTimeUtc,
  resultSequence: NonNegativeInt,
  status: Schema.Literals(["accepted", "rejected"]),
  error: Schema.NullOr(Schema.String),
};
export const NativeCommandReceiptObservationV2 = closedNativeStruct(NativeCommandReceiptObservationV2Fields);
export type NativeCommandReceiptObservationV2 = typeof NativeCommandReceiptObservationV2.Type;
export const NativeCommandReceiptObservationV2Json = closedNativeStruct({
  ...NativeCommandReceiptObservationV2Fields,
  acceptedAt: Schema.DateTimeUtcFromString,
});
export type NativeCommandReceiptObservationV2Json = typeof NativeCommandReceiptObservationV2Json.Type;

const OrchestrationV2ThreadDeletionCleanupObservationFields = {
  threadId: ThreadId,
  commandId: CommandId,
  receipt: Schema.NullOr(NativeCommandReceiptObservationV2),
  deletion: Schema.NullOr(closedNativeStruct({
    eventId: EventId,
    sequence: NonNegativeInt,
    resultSequence: NonNegativeInt,
  })),
  worktree: Schema.NullOr(closedNativeStruct({
    projectId: ProjectId,
    path: Schema.String,
    branch: Schema.NullOr(Schema.String),
  })),
  state: Schema.Literals(["not_found", "not_requested", "pending", "removing", "completed", "retained", "unknown"]),
  removalOutcome: Schema.NullOr(closedNativeStruct({
    result: Schema.NullOr(Schema.Literals(["succeeded", "failed"])),
    effect: Schema.Literals(["confirmed", "absent", "no_effect", "unknown"]),
  })),
  currentLease: Schema.Literals(["original", "absent", "replacement", "unavailable"]),
  reason: Schema.NullOr(Schema.String),
};
export const OrchestrationV2ThreadDeletionCleanupObservation =
  closedNativeStruct(OrchestrationV2ThreadDeletionCleanupObservationFields);
export type OrchestrationV2ThreadDeletionCleanupObservation =
  typeof OrchestrationV2ThreadDeletionCleanupObservation.Type;
export const OrchestrationV2ThreadDeletionCleanupObservationJson = closedNativeStruct({
  ...OrchestrationV2ThreadDeletionCleanupObservationFields,
  receipt: Schema.NullOr(NativeCommandReceiptObservationV2Json),
});
export type OrchestrationV2ThreadDeletionCleanupObservationJson =
  typeof OrchestrationV2ThreadDeletionCleanupObservationJson.Type;

const OrchestrationV2StopCurrentThreadRuntimeResultFields = {
  version: Schema.Literal(2),
  commandId: CommandId,
  threadId: ThreadId,
  target: Schema.NullOr(OrchestrationV2CurrentThreadRuntimeTarget),
  commandStatus: Schema.Literals(["accepted", "rejected", "not_found", "unknown"]),
  receipt: Schema.NullOr(NativeCommandReceiptObservationV2),
  queueFence: closedNativeStruct({
    status: Schema.Literals(["installed", "not_installed", "unknown"]),
    affectedRunIds: Schema.Array(RunId),
  }),
  runtimeStop: closedNativeStruct({ status: Schema.Literals(["not_started", "pending", "stopped", "unknown"]) }),
  reason: Schema.NullOr(Schema.String),
};
const OrchestrationV2StopCurrentThreadRuntimeResultSchema =
  closedNativeStruct(OrchestrationV2StopCurrentThreadRuntimeResultFields);
const currentRuntimeStopTargetIsPresent = (result: typeof OrchestrationV2StopCurrentThreadRuntimeResultSchema.Type) =>
  (result.target === null || result.target.binding.threadId === result.threadId) &&
  (result.commandStatus !== "not_found" || result.target === null) &&
  (result.runtimeStop.status !== "stopped" || result.queueFence.status === "installed") &&
  ((result.commandStatus !== "accepted" && result.queueFence.status !== "installed" && result.runtimeStop.status !== "stopped") || result.target !== null);

export const OrchestrationV2StopCurrentThreadRuntimeResult =
  OrchestrationV2StopCurrentThreadRuntimeResultSchema.check(Schema.makeFilter(currentRuntimeStopTargetIsPresent));
export type OrchestrationV2StopCurrentThreadRuntimeResult =
  typeof OrchestrationV2StopCurrentThreadRuntimeResult.Type;
export const OrchestrationV2StopCurrentThreadRuntimeResultJson = closedNativeStruct({
  ...OrchestrationV2StopCurrentThreadRuntimeResultFields,
  receipt: Schema.NullOr(NativeCommandReceiptObservationV2Json),
}).check(Schema.makeFilter(currentRuntimeStopTargetIsPresent));
export type OrchestrationV2StopCurrentThreadRuntimeResultJson =
  typeof OrchestrationV2StopCurrentThreadRuntimeResultJson.Type;

const OrchestrationV2ImportedHistoryTarget = Schema.Union([
  closedNativeStruct({ type: Schema.Literal("message"), messageId: MessageId }),
  closedNativeStruct({ type: Schema.Literal("queued_run"), runId: RunId, messageId: MessageId }),
]);

export const OrchestrationV2ImportedHistoryReviewResult = closedNativeStruct({
  version: Schema.Literal(2),
  threadId: ThreadId,
  target: OrchestrationV2ImportedHistoryTarget,
  capability: closedNativeStruct({ startWithImportedHistory: Schema.Boolean }),
  applicability: Schema.Literals(["imported", "not_imported", "unknown"]),
  qualification: Schema.Union([
    closedNativeStruct({ type: Schema.Literal("qualified") }),
    closedNativeStruct({ type: Schema.Literals(["unknown", "unsupported"]), reason: Schema.String }),
  ]),
  restoredBinding: Schema.Union([
    closedNativeStruct({
      type: Schema.Literal("ready"),
      providerThreadId: ProviderThreadId,
      providerInstanceId: ProviderInstanceId,
      driver: ProviderDriverKind,
      nativeThreadId: TrimmedNonEmptyString,
      providerSessionId: Schema.NullOr(ProviderSessionId),
      runtimeGeneration: Schema.optional(TrimmedNonEmptyString),
    }),
    closedNativeStruct({ type: Schema.Literals(["missing", "mismatched", "unknown"]), reason: Schema.String }),
  ]),
  nativeEffects: Schema.Union([
    closedNativeStruct({ type: Schema.Literal("clear") }),
    closedNativeStruct({ type: Schema.Literal("unknown"), reason: Schema.String }),
  ]),
  transcriptEligibility: Schema.Union([
    closedNativeStruct({ type: Schema.Literal("eligible") }),
    closedNativeStruct({ type: Schema.Literals(["ineligible", "unknown"]), reason: Schema.String }),
  ]),
  reviewedBasis: Schema.NullOr(OrchestrationV2ImportedHistoryReviewBasis),
}).check(Schema.makeFilter((review) => review.reviewedBasis === null || (
  review.applicability === "imported" &&
  review.qualification.type !== "qualified" &&
  review.transcriptEligibility.type === "eligible" &&
  review.nativeEffects.type === "clear" &&
  review.capability.startWithImportedHistory
)));
export type OrchestrationV2ImportedHistoryReviewResult =
  typeof OrchestrationV2ImportedHistoryReviewResult.Type;

const OrchestrationV2ImportedHistoryExecution = closedNativeStruct({
  status: Schema.Literals(["not_started", "pending", "started", "unknown"]),
  runId: Schema.NullOr(RunId),
  providerThreadId: Schema.NullOr(ProviderThreadId),
  providerSessionId: Schema.NullOr(ProviderSessionId),
  nativeThreadId: Schema.NullOr(TrimmedNonEmptyString),
  runtimeGeneration: Schema.optional(TrimmedNonEmptyString),
  effectOutcome: Schema.NullOr(Schema.Literals(["confirmed_success", "known_no_effect", "unknown"])),
  error: Schema.NullOr(Schema.String),
}).check(Schema.makeFilter((execution) => execution.status !== "started" || (
  execution.effectOutcome === "confirmed_success" &&
  execution.runId !== null && execution.providerThreadId !== null &&
  execution.providerSessionId !== null && execution.nativeThreadId !== null
)));

const OrchestrationV2ImportedHistoryStartReceiptFields = {
  version: Schema.Literal(2),
  commandId: CommandId,
  threadId: ThreadId,
  target: Schema.NullOr(OrchestrationV2ImportedHistoryTarget),
  reviewedBasis: Schema.NullOr(OrchestrationV2ImportedHistoryReviewBasis),
  intentStatus: Schema.Literals(["accepted", "rejected", "not_found", "unknown"]),
  receipt: Schema.NullOr(NativeCommandReceiptObservationV2),
  rejectionReason: Schema.NullOr(TrimmedNonEmptyString),
  execution: OrchestrationV2ImportedHistoryExecution,
};
export const OrchestrationV2ImportedHistoryStartReceipt =
  closedNativeStruct(OrchestrationV2ImportedHistoryStartReceiptFields).check(Schema.makeFilter(
    (receipt) => receipt.target !== null || receipt.intentStatus === "not_found" || receipt.intentStatus === "unknown",
  ));
export type OrchestrationV2ImportedHistoryStartReceipt =
  typeof OrchestrationV2ImportedHistoryStartReceipt.Type;
export const OrchestrationV2ImportedHistoryStartReceiptJson = closedNativeStruct({
  ...OrchestrationV2ImportedHistoryStartReceiptFields,
  receipt: Schema.NullOr(NativeCommandReceiptObservationV2Json),
}).check(Schema.makeFilter(
  (receipt) => receipt.target !== null || receipt.intentStatus === "not_found" || receipt.intentStatus === "unknown",
));
export type OrchestrationV2ImportedHistoryStartReceiptJson =
  typeof OrchestrationV2ImportedHistoryStartReceiptJson.Type;

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

// Public history contains bounded attestation and attribution; canonical bodies remain server-owned.
export const NativeCreationObservationV2 = closedNativeStruct({
  ...NativeCreationObservationV2Fields,
  stageCommands: Schema.Array(NativeCreationStageObservationV2).check(
    Schema.isMaxLength(NATIVE_CREATION_OBSERVATION_V2_MAX_STAGES),
  ),
  finalReceipt: Schema.NullOr(NativeCommandReceiptObservationV2),
}).check(Schema.makeFilter((observation) => !observation.overflow || observation.outcome === "unknown"));
export type NativeCreationObservationV2 = typeof NativeCreationObservationV2.Type;

export const NativeCreationObservationV2Json = closedNativeStruct({
  ...NativeCreationObservationV2Fields,
  stageCommands: Schema.Array(NativeCreationStageObservationV2Json).check(
    Schema.isMaxLength(NATIVE_CREATION_OBSERVATION_V2_MAX_STAGES),
  ),
  finalReceipt: Schema.NullOr(NativeCommandReceiptObservationV2Json),
}).check(Schema.makeFilter((observation) => !observation.overflow || observation.outcome === "unknown"));
export type NativeCreationObservationV2Json = typeof NativeCreationObservationV2Json.Type;

const NativeBootstrapDispatchResultV2Fields = {
  version: Schema.Literal(2),
  commandId: CommandId,
  threadId: ThreadId,
  messageId: MessageId,
  commandAcceptance: Schema.Literals(["accepted", "pending", "rejected", "unknown"]),
  creation: Schema.NullOr(NativeCreationObservationV2),
};
const NativeBootstrapDispatchResultV2Schema = closedNativeStruct(NativeBootstrapDispatchResultV2Fields);

const nativeBootstrapAcceptanceIsAttributed = (result: typeof NativeBootstrapDispatchResultV2Schema.Type) => {
  if (result.commandAcceptance !== "accepted") return true;
  const creation = result.creation;
  if (creation === null || creation.overflow || creation.stageCommands.length !== 3 || creation.incarnation === null)
    return false;
  const expectedIds = [
    `${result.commandId}:native:v2:create`,
    `${result.commandId}:native:v2:message`,
    result.commandId,
  ];
  const expectedTypes = ["thread.create", "message.dispatch", "prepared-run.release"];
  let previousEventSequence = -1;
  let previousReceiptSequence = -1;
  const eventIds = new Set<string>();
  for (const [index, stage] of creation.stageCommands.entries()) {
    const receipt = stage.receipt;
    const event = stage.event;
    if (stage.commandId !== expectedIds[index] || stage.commandType !== expectedTypes[index] ||
        stage.threadId !== result.threadId || stage.claimId !== creation.claimId ||
        receipt === null || event === null || receipt.status !== "accepted" || receipt.error !== null ||
        receipt.commandId !== stage.commandId || receipt.threadId !== stage.threadId ||
        receipt.commandType !== stage.commandType ||
        eventIds.has(event.eventId) ||
        event.sequence !== receipt.resultSequence ||
        event.sequence <= previousEventSequence || receipt.resultSequence <= previousReceiptSequence)
      return false;
    eventIds.add(event.eventId);
    previousEventSequence = event.sequence;
    previousReceiptSequence = receipt.resultSequence;
  }
  const birth = creation.stageCommands[0]!.event!;
  const release = creation.stageCommands[2]!.receipt!;
  const finalReceipt = creation.finalReceipt;
  return birth.eventId === creation.incarnation.eventId && birth.sequence === creation.incarnation.sequence &&
    finalReceipt !== null && finalReceipt.commandId === result.commandId &&
    finalReceipt.threadId === result.threadId && finalReceipt.commandType === "prepared-run.release" &&
    finalReceipt.status === "accepted" && finalReceipt.error === null &&
    DateTime.toEpochMillis(finalReceipt.acceptedAt) === DateTime.toEpochMillis(release.acceptedAt) &&
    finalReceipt.resultSequence === release.resultSequence;
};

// Ordered command acceptance is independent of the external creation outcome.
export const NativeBootstrapDispatchResultV2 = NativeBootstrapDispatchResultV2Schema.check(
  Schema.makeFilter(nativeBootstrapAcceptanceIsAttributed),
);
export type NativeBootstrapDispatchResultV2 = typeof NativeBootstrapDispatchResultV2.Type;
export const NativeBootstrapDispatchResultV2Json = closedNativeStruct({
  ...NativeBootstrapDispatchResultV2Fields,
  creation: Schema.NullOr(NativeCreationObservationV2Json),
}).check(Schema.makeFilter(nativeBootstrapAcceptanceIsAttributed));
export type NativeBootstrapDispatchResultV2Json = typeof NativeBootstrapDispatchResultV2Json.Type;

const NativeCommandObservationV2Fields = {
  version: Schema.Literal(2),
  threadId: ThreadId,
  commandId: CommandId,
  messageId: MessageId,
  commandStatus: Schema.Literals(["accepted", "rejected", "not_found"]),
  identity: Schema.NullOr(NativeCommandIdentityV2),
  identityVerification: Schema.Literals(["verified", "unbound", "missing", "mismatched", "unknown"]),
  correlation: Schema.Literals(["exact", "pending", "missing", "ambiguous", "mismatched"]),
  snapshot: closedNativeStruct({
    snapshotSequence: NonNegativeInt,
    targetEventSequence: NonNegativeInt,
    complete: Schema.Boolean,
  }),
  correlatedMessageId: Schema.NullOr(MessageId),
  run: Schema.NullOr(closedNativeStruct({
    runId: RunId,
    runAttemptId: Schema.NullOr(RunAttemptId),
    providerThreadId: Schema.NullOr(ProviderThreadId),
    providerTurnId: Schema.NullOr(ProviderTurnId),
    status: OrchestrationV2RunStatus,
  })),
  target: Schema.NullOr(OrchestrationDispatchTargetV2),
};

// Historical receipt acceptance and exact correlation do not attest provider execution or settlement.
export const NativeCommandObservationV2 = closedNativeStruct({
  ...NativeCommandObservationV2Fields,
  receipt: Schema.NullOr(NativeCommandReceiptObservationV2),
  creation: Schema.optionalKey(NativeCreationObservationV2),
});
export type NativeCommandObservationV2 = typeof NativeCommandObservationV2.Type;
export const NativeCommandObservationV2Json = closedNativeStruct({
  ...NativeCommandObservationV2Fields,
  receipt: Schema.NullOr(NativeCommandReceiptObservationV2Json),
  creation: Schema.optionalKey(NativeCreationObservationV2Json),
});
export type NativeCommandObservationV2Json = typeof NativeCommandObservationV2Json.Type;
