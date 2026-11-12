import * as Schema from "effect/Schema";

import {
  CommandId,
  EventId,
  IsoDateTime,
  MessageId,
  NonNegativeInt,
  ProjectId,
  ThreadId,
} from "./baseSchemas.ts";
import { ProviderInstanceId } from "./providerInstance.ts";
import { RuntimeMode, ProviderInteractionMode } from "./providerPolicy.ts";

const nativeCreationStruct = <Fields extends Schema.Struct.Fields>(fields: Fields) => {
  const schema = Schema.Struct(fields);
  // Flipped checks validate original wire keys that ordinary struct decoding would strip.
  return Schema.flip(
    Schema.flip(schema).check(
      Schema.makeFilter((value) =>
        Reflect.ownKeys(value).every((key) => Object.hasOwn(fields, key)),
      ),
    ),
  );
};

export const NATIVE_BOOTSTRAP_MAX_PREPARATION_BYTES = 1_048_576;
export const NATIVE_BOOTSTRAP_MAX_SUBMISSION_BYTES = 2_097_152;

const NativeCreationRevision = Schema.Int.check(
  Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
);
const NativeCreationString = Schema.String.check(Schema.isNonEmpty());
const NativeCreationSha256 = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));

export const NativeCreationGuard = nativeCreationStruct({
  schema: Schema.Literal("t3.native-creation-guard/v1"),
  grantId: NativeCreationString,
  grantRevision: NativeCreationRevision,
});
export type NativeCreationGuard = typeof NativeCreationGuard.Type;

export const NativeBootstrapSubmission = nativeCreationStruct({
  schema: Schema.Literal("t3.native-bootstrap-submission/v1"),
  preparationBase64: Schema.String.check(
    Schema.isNonEmpty(),
    Schema.isMaxLength(4 * Math.ceil(NATIVE_BOOTSTRAP_MAX_PREPARATION_BYTES / 3)),
    Schema.isPattern(/^[A-Za-z0-9+/]*={0,2}$/),
    Schema.makeFilter((value) => {
      const padding = value.endsWith("==") ? 2 : value.endsWith("=") ? 1 : 0;
      return (
        value.length % 4 === 0 &&
        (value.length / 4) * 3 - padding <= NATIVE_BOOTSTRAP_MAX_PREPARATION_BYTES
      );
    }),
  ),
  creationGuard: NativeCreationGuard,
}).check(
  Schema.makeFilter(
    (value) =>
      new TextEncoder().encode(JSON.stringify(value)).byteLength <=
      NATIVE_BOOTSTRAP_MAX_SUBMISSION_BYTES,
  ),
);
export type NativeBootstrapSubmission = typeof NativeBootstrapSubmission.Type;

export const NativeCreationRejectionCode = Schema.Literals([
  "unsupported_authority",
  "invalid_preparation",
  "stale_grant",
  "binding_mismatch",
  "conflict",
  "unresolved_claim",
]);
export type NativeCreationRejectionCode = typeof NativeCreationRejectionCode.Type;

const NativeCreationModelSelection = nativeCreationStruct({
  instanceId: ProviderInstanceId,
  model: NativeCreationString,
  options: Schema.optionalKey(
    Schema.Array(
      nativeCreationStruct({
        id: NativeCreationString,
        value: Schema.Union([NativeCreationString, Schema.Boolean]),
      }),
    ),
  ),
});

export const NativeCreationHistoricalBinding = nativeCreationStruct({
  backendInstance: NativeCreationString,
  environmentId: NativeCreationString,
  projectId: ProjectId,
  projectCwd: NativeCreationString,
  accountRef: NativeCreationString,
  accountBindingId: NativeCreationString,
  accountBindingRevision: NativeCreationRevision,
  providerModelSelection: NativeCreationModelSelection,
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode,
  baseBranch: NativeCreationString,
  startFromOrigin: Schema.Boolean,
  runSetupScript: Schema.Boolean,
  requestedBranch: NativeCreationString,
});
export type NativeCreationHistoricalBinding = typeof NativeCreationHistoricalBinding.Type;

const NativeCreationIncarnation = nativeCreationStruct({
  eventId: EventId,
  sequence: NonNegativeInt,
});
const NativeCreationEffectBase = {
  effectId: NativeCreationString,
  ordinal: NonNegativeInt,
  timestamp: IsoDateTime,
};
const NativeCreationCommandDetails = {
  commandId: CommandId,
  threadId: ThreadId,
  commandType: Schema.Literals([
    "thread.create",
    "thread.meta.update",
    "thread.message.user.append",
    "thread.session.set",
    "thread.turn.start",
    "thread.delete",
  ]),
  commandDigest: NativeCreationSha256,
};
const NativeCreationWorktreeDetails = {
  projectCwd: NativeCreationString,
  worktreePath: NativeCreationString,
  branch: NativeCreationString,
  baseRef: NativeCreationString,
  ownership: Schema.Literals(["claimed", "created", "unknown"]),
};
const NativeCreationCleanupDetails = {
  resource: Schema.Union([
    nativeCreationStruct({
      kind: Schema.Literal("worktree"),
      ...NativeCreationWorktreeDetails,
    }),
    nativeCreationStruct({
      kind: Schema.Literal("setup_terminal"),
      terminalId: NativeCreationString,
      worktreePath: NativeCreationString,
    }),
    nativeCreationStruct({
      kind: Schema.Literal("thread"),
      threadId: ThreadId,
      incarnation: NativeCreationIncarnation,
    }),
  ]),
  recoveryScopeId: NativeCreationString,
};
const NativeCreationExternalResult = Schema.Literals(["succeeded", "failed", "unknown"]);

const NativeCreationLifecycleDetails = {
  threadId: ThreadId,
  action: Schema.Literals([
    "normalization",
    "tracker_registration",
    "bootstrap_detachment",
    "setup_detachment",
    "setup_completion_detachment",
    "worktree_ownership",
    "deletion_drain",
    "git_status_refresh",
  ]),
};

export const NativeCreationEffect = Schema.Union([
  nativeCreationStruct({
    ...NativeCreationEffectBase,
    kind: Schema.Literal("lifecycle"),
    phase: Schema.Literal("started"),
    ...NativeCreationLifecycleDetails,
  }),
  nativeCreationStruct({
    ...NativeCreationEffectBase,
    kind: Schema.Literal("lifecycle"),
    phase: Schema.Literal("completed"),
    ...NativeCreationLifecycleDetails,
    result: NativeCreationExternalResult,
  }),
  nativeCreationStruct({
    ...NativeCreationEffectBase,
    kind: Schema.Literal("native_command"),
    phase: Schema.Literal("started"),
    ...NativeCreationCommandDetails,
  }),
  nativeCreationStruct({
    ...NativeCreationEffectBase,
    kind: Schema.Literal("native_command"),
    phase: Schema.Literal("completed"),
    ...NativeCreationCommandDetails,
    eventId: EventId,
    sequence: NonNegativeInt,
  }),
  nativeCreationStruct({
    ...NativeCreationEffectBase,
    kind: Schema.Literal("fetch"),
    phase: Schema.Literal("started"),
    projectCwd: NativeCreationString,
    baseRef: NativeCreationString,
  }),
  nativeCreationStruct({
    ...NativeCreationEffectBase,
    kind: Schema.Literal("fetch"),
    phase: Schema.Literal("completed"),
    projectCwd: NativeCreationString,
    baseRef: NativeCreationString,
    result: NativeCreationExternalResult,
  }),
  nativeCreationStruct({
    ...NativeCreationEffectBase,
    kind: Schema.Literal("worktree"),
    phase: Schema.Literal("started"),
    ...NativeCreationWorktreeDetails,
  }),
  nativeCreationStruct({
    ...NativeCreationEffectBase,
    kind: Schema.Literal("worktree"),
    phase: Schema.Literal("completed"),
    ...NativeCreationWorktreeDetails,
    result: NativeCreationExternalResult,
  }),
  nativeCreationStruct({
    ...NativeCreationEffectBase,
    kind: Schema.Literal("setup"),
    phase: Schema.Literal("started"),
    worktreePath: NativeCreationString,
    terminalId: Schema.NullOr(NativeCreationString),
  }),
  nativeCreationStruct({
    ...NativeCreationEffectBase,
    kind: Schema.Literal("setup"),
    phase: Schema.Literal("completed"),
    worktreePath: NativeCreationString,
    terminalId: Schema.NullOr(NativeCreationString),
    exitCode: Schema.NullOr(Schema.Int),
    result: NativeCreationExternalResult,
  }),
  nativeCreationStruct({
    ...NativeCreationEffectBase,
    kind: Schema.Literal("cleanup"),
    phase: Schema.Literal("started"),
    ...NativeCreationCleanupDetails,
  }),
  nativeCreationStruct({
    ...NativeCreationEffectBase,
    kind: Schema.Literal("cleanup"),
    phase: Schema.Literal("completed"),
    ...NativeCreationCleanupDetails,
    result: NativeCreationExternalResult,
  }),
]);
export type NativeCreationEffect = typeof NativeCreationEffect.Type;

/** Historical creation attestation does not attest a terminal turn or release capacity. */
export const NativeCreationObservation = nativeCreationStruct({
  schema: Schema.Literal("t3.native-creation-observation/v1"),
  preparationId: NativeCreationString,
  operationId: NativeCreationString,
  preparationSha256: NativeCreationSha256,
  bindingDigest: NativeCreationSha256,
  promptDigest: NativeCreationSha256,
  commandDigest: NativeCreationSha256,
  normalizedCommandDigest: NativeCreationSha256,
  claimId: NativeCreationString,
  claimedBootId: NativeCreationString,
  claimedAt: IsoDateTime,
  actorSessionId: NativeCreationString,
  grantId: NativeCreationString,
  grantRevision: NativeCreationRevision,
  binding: NativeCreationHistoricalBinding,
  incarnation: Schema.NullOr(NativeCreationIncarnation),
  effects: Schema.Array(NativeCreationEffect),
  unresolvedEffects: Schema.Array(NativeCreationString),
  outcome: Schema.Literals(["complete", "in_progress", "incomplete", "unknown"]),
});
export type NativeCreationObservation = typeof NativeCreationObservation.Type;

export const NativeBootstrapDispatchResultV2 = nativeCreationStruct({
  version: Schema.Literal(2),
  commandId: CommandId,
  threadId: ThreadId,
  messageId: MessageId,
  commandAcceptance: Schema.Literal("accepted"),
});
export type NativeBootstrapDispatchResultV2 = typeof NativeBootstrapDispatchResultV2.Type;
export class NativeBootstrapDispatchError extends Schema.TaggedError<NativeBootstrapDispatchError>()(
  "NativeBootstrapDispatchError",
  { code: NativeCreationRejectionCode, message: Schema.String },
) {}
