import * as Schema from "effect/Schema";
import { ProviderRuntimeBinding } from "@t3tools/contracts";
import { canonical, digest, DirectoryIdentity, Utc } from "../ordinaryCheckout/CheckoutProtocol.ts";
import { NativeCreationExecutionReferenceV2 } from "../nativeCreation/NativeCreationExecutionTypes.ts";

const id = Schema.NonEmptyString;
const hash = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
const ordinal = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const provider = Schema.Struct({ sessionId: id, instanceId: id, driver: id });
export const RuntimeUseOutboxClaim = Schema.Struct({
  schema: Schema.Literal("jones.workspace-runtime-outbox-claim/v1"),
  effectId: id,
  commandId: id,
  threadId: id,
  workerId: id,
  attemptCount: Schema.Int.check(Schema.isGreaterThan(0)),
  claimedAt: Utc,
  runId: Schema.NullOr(id),
  attemptId: Schema.NullOr(id),
  requestSha256: hash,
  nativeReference: Schema.NullOr(NativeCreationExecutionReferenceV2),
});
export type RuntimeUseOutboxClaim = typeof RuntimeUseOutboxClaim.Type;
export const RuntimeUseBinding = Schema.Struct({
  schema: Schema.Literal("jones.workspace-runtime-use/v1"),
  useId: id,
  canonicalPath: id,
  directoryIdentity: DirectoryIdentity,
  threadId: id,
  actor: Schema.Union([
    Schema.Struct({ kind: Schema.Literals(["provider_open", "provider_invoke"]), provider }),
    Schema.Struct({ kind: Schema.Literal("terminal_spawn"), terminalSessionId: id }),
  ]),
  original: Schema.Struct({
    commandId: Schema.NullOr(id),
    runId: Schema.NullOr(id),
    attemptId: Schema.NullOr(id),
    outboxReference: Schema.NullOr(id),
    nativeReference: Schema.NullOr(id),
    claim: Schema.NullOr(RuntimeUseOutboxClaim),
  }),
}).check(
  Schema.makeFilter((binding) => {
    const { original } = binding;
    const claim = original.claim;
    return (
      (claim === null && original.outboxReference === null) ||
      (claim !== null &&
        binding.threadId === claim.threadId &&
        original.commandId === claim.commandId &&
        original.runId === claim.runId &&
        original.attemptId === claim.attemptId &&
        original.outboxReference === claim.effectId &&
        original.nativeReference === (claim.nativeReference?.claimId ?? null))
    );
  }),
);
export type RuntimeUseBinding = typeof RuntimeUseBinding.Type;

// An observation records coverage, rather than assuming that a process exit
// covers other threads, descendants, replacements or a physical open.
export const RuntimeUseProducer = Schema.Struct({
  schema: Schema.Literal("jones.workspace-runtime-producer/v1"),
  useId: id,
  bindingSha256: hash,
  producerId: id,
  generation: id,
  threadId: id,
  writerSetSha256: hash,
  coverage: Schema.Literals(["captured_process_only", "logical_thread", "complete"]),
});
export type RuntimeUseProducer = typeof RuntimeUseProducer.Type;
export const RuntimeUseNativeBinding = Schema.Struct({
  schema: Schema.Literal("jones.workspace-runtime-native-binding/v1"),
  useId: id,
  bindingSha256: hash,
  producerId: id,
  generation: id,
  binding: ProviderRuntimeBinding,
});
export type RuntimeUseNativeBinding = typeof RuntimeUseNativeBinding.Type;
const retiredProducer = Schema.Struct({
  producerId: id,
  generation: id,
  observationSha256: hash,
  driver: Schema.Literal("codex"),
  instanceId: id,
  providerSessionId: id,
  threadId: id,
  pid: Schema.Int.check(Schema.isGreaterThan(0)),
  scopeUnit: id,
  writerBindingsSha256: hash,
});
export const RuntimeUseRetirement = Schema.Union([
  Schema.Struct({
    schema: Schema.Literal("jones.workspace-runtime-retirement/v1"),
    useId: id,
    bindingSha256: hash,
    outcome: Schema.Literal("not_invoked"),
    observationCount: Schema.Literal(0),
  }),
  Schema.Struct({
    schema: Schema.Literal("jones.workspace-runtime-retirement/v1"),
    useId: id,
    bindingSha256: hash,
    outcome: Schema.Literal("producers_exited"),
    observationCount: ordinal,
    producers: Schema.Array(retiredProducer),
    nativeBindings: Schema.Array(RuntimeUseNativeBinding),
  }),
]);
export type RuntimeUseRetirement = typeof RuntimeUseRetirement.Type;

export class RuntimeUseError extends Schema.TaggedError<RuntimeUseError>()("RuntimeUseError", {
  reason: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {}
const isRuntimeUseError = Schema.is(RuntimeUseError);
export const runtimeUseError = (cause: unknown): RuntimeUseError =>
  isRuntimeUseError(cause)
    ? cause
    : new RuntimeUseError({ reason: "runtime_use_unavailable", cause });

export const decodeRuntimeUseBinding = Schema.decodeUnknownEffect(RuntimeUseBinding, {
  onExcessProperty: "error",
});
export const decodeRuntimeUseProducer = Schema.decodeUnknownEffect(RuntimeUseProducer, {
  onExcessProperty: "error",
});
export const decodeRuntimeUseNativeBinding = Schema.decodeUnknownEffect(RuntimeUseNativeBinding, {
  onExcessProperty: "error",
});
export const decodeRuntimeUseRetirement = Schema.decodeUnknownEffect(RuntimeUseRetirement, {
  onExcessProperty: "error",
});
export { canonical, digest };
