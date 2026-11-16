import * as Schema from "effect/Schema";
import { CommandId, ThreadId, EventId, IsoDateTime, NonNegativeInt } from "@t3tools/contracts";
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

// A final RPC acknowledgement cannot prove the earlier activation or history injection stages.
export const NativeCreationWholeOperationEvidence = closedNativeStruct({
  version: Schema.Literal(1),
  outcome: Schema.Literal("confirmed_success"),
  effectId: Schema.NonEmptyString,
  threadId: ThreadId,
  commandId: CommandId,
  providerSessionId: Schema.NonEmptyString,
  providerThreadId: Schema.NonEmptyString,
  runtimeGeneration: Schema.NonEmptyString,
  runId: Schema.NonEmptyString,
  attemptId: Schema.NonEmptyString,
  coverage: Schema.Literal("whole_operation"),
});
export type NativeCreationWholeOperationEvidence = typeof NativeCreationWholeOperationEvidence.Type;
