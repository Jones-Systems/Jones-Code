import { CheckpointScopeId, CommandId, EventId, MessageId, NonNegativeInt, ProjectId, ProviderDriverKind, ProviderInstanceId, ProviderSessionId, ProviderThreadId, ProviderTurnId, RunId, ThreadId, TrimmedNonEmptyString } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

import { nativeCreationCanonicalJson, nativeCreationSha256 } from "./NativeCreationPreparation.ts";

const positiveInt = NonNegativeInt.check(Schema.makeFilter((value) => value > 0));
const sha256 = Schema.String.check(Schema.makeFilter((value) => /^[0-9a-f]{64}$/.test(value)));
const absolutePath = TrimmedNonEmptyString.check(Schema.makeFilter((value) => value.startsWith("/")));

function isJson(value: unknown): boolean {
  if (value === null || typeof value === "string" || typeof value === "boolean") return true;
  if (typeof value === "number") return Number.isFinite(value);
  if (Array.isArray(value)) return value.every(isJson);
  if (typeof value !== "object" || Object.getPrototypeOf(value) !== Object.prototype) return false;
  return Object.values(value).every(isJson);
}
const jsonObject = Schema.Record(Schema.String, Schema.Unknown).check(Schema.makeFilter(isJson));

export const OrdinaryApplicationBirthV1 = Schema.Struct({
  kind: Schema.Literal("application_v2_thread_birth"), threadId: ThreadId,
  eventId: EventId, sequence: positiveInt,
});
export type OrdinaryApplicationBirthV1 = typeof OrdinaryApplicationBirthV1.Type;

export const OrdinaryCheckoutLeaseV1 = Schema.Struct({
  resourcePath: absolutePath, leaseId: TrimmedNonEmptyString, ownerThreadId: ThreadId,
  ownerIncarnation: TrimmedNonEmptyString, branch: Schema.NullOr(Schema.String),
  acquiredAtMs: NonNegativeInt, renewedAtMs: NonNegativeInt, expiresAtMs: NonNegativeInt,
});
export type OrdinaryCheckoutLeaseV1 = typeof OrdinaryCheckoutLeaseV1.Type;

export const OrdinaryCheckoutOriginV1 = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("command") }),
  Schema.Struct({ kind: Schema.Literal("runtime_request_answer"), requestId: TrimmedNonEmptyString }),
  Schema.Struct({ kind: Schema.Literal("delegated_child"), parentThreadId: ThreadId }),
]);
export type OrdinaryCheckoutOriginV1 = typeof OrdinaryCheckoutOriginV1.Type;

export const OrdinaryCheckoutCaptureV1 = Schema.Struct({
  version: Schema.Literal(1), commandId: CommandId, commandType: TrimmedNonEmptyString,
  canonicalCommand: jsonObject, commandDigest: sha256, origin: OrdinaryCheckoutOriginV1,
  threadId: ThreadId, applicationBirth: OrdinaryApplicationBirthV1, projectId: ProjectId,
  canonicalProjectRoot: absolutePath, canonicalCheckoutPath: absolutePath,
  branch: Schema.NullOr(Schema.String), lease: OrdinaryCheckoutLeaseV1,
});
export type OrdinaryCheckoutCaptureV1 = typeof OrdinaryCheckoutCaptureV1.Type;

export const OrdinaryAcceptedReceiptV1 = Schema.Struct({
  commandId: CommandId, threadId: ThreadId, commandType: TrimmedNonEmptyString,
  acceptedAt: Schema.DateTimeUtcFromString, resultSequence: NonNegativeInt,
  status: Schema.Literal("accepted"), error: Schema.Null,
});
export type OrdinaryAcceptedReceiptV1 = typeof OrdinaryAcceptedReceiptV1.Type;
export const OrdinaryAcceptedEventV1 = Schema.Struct({
  eventId: EventId, sequence: positiveInt, threadId: ThreadId,
  commandId: Schema.NullOr(CommandId), eventType: TrimmedNonEmptyString,
});
export type OrdinaryAcceptedEventV1 = typeof OrdinaryAcceptedEventV1.Type;
export const OrdinaryAcceptedRunV1 = Schema.Struct({
  runId: RunId, runAttemptId: TrimmedNonEmptyString, nodeId: TrimmedNonEmptyString, messageId: MessageId,
});
export type OrdinaryAcceptedRunV1 = typeof OrdinaryAcceptedRunV1.Type;

export const OrdinaryCheckoutAdmissionV1 = Schema.Struct({
  version: Schema.Literal(1), admissionId: sha256, capture: OrdinaryCheckoutCaptureV1,
  receipt: OrdinaryAcceptedReceiptV1,
  eventBasis: Schema.Array(OrdinaryAcceptedEventV1).check(Schema.makeFilter((events) => events.length > 0)),
  run: Schema.NullOr(OrdinaryAcceptedRunV1), recordedAt: Schema.DateTimeUtcFromString,
});
export type OrdinaryCheckoutAdmissionV1 = typeof OrdinaryCheckoutAdmissionV1.Type;

export const OrdinaryCheckoutAdmissionRefV1 = Schema.Struct({
  version: Schema.Literal(1), admissionId: sha256, admissionSha256: sha256,
});
export type OrdinaryCheckoutAdmissionRefV1 = typeof OrdinaryCheckoutAdmissionRefV1.Type;

export const OrdinaryCheckoutEffectLinkV1 = Schema.Struct({
  version: Schema.Literal(1), effectId: TrimmedNonEmptyString, commandId: CommandId,
  threadId: ThreadId, requestSha256: sha256, admission: OrdinaryCheckoutAdmissionRefV1,
  recordedAt: Schema.DateTimeUtcFromString,
});
export type OrdinaryCheckoutEffectLinkV1 = typeof OrdinaryCheckoutEffectLinkV1.Type;

export const OrdinaryCheckoutUseSourceV1 = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("outbox"), link: OrdinaryCheckoutEffectLinkV1,
    workerId: TrimmedNonEmptyString, expectedAttempt: positiveInt, leaseExpiresAt: Schema.DateTimeUtcFromString }),
  Schema.Struct({ kind: Schema.Literal("prepared_run"), admission: OrdinaryCheckoutAdmissionRefV1,
    preparation: OrdinaryAcceptedRunV1 }),
  Schema.Struct({ kind: Schema.Literal("prepared_launch"), admission: OrdinaryCheckoutAdmissionRefV1,
    preparationCommandId: CommandId, preparationEvent: OrdinaryAcceptedEventV1,
    applicationBirth: OrdinaryApplicationBirthV1, projectId: ProjectId,
    canonicalProjectRoot: absolutePath, canonicalCheckoutPath: absolutePath, branch: Schema.NullOr(Schema.String) }),
]);
export type OrdinaryCheckoutUseSourceV1 = typeof OrdinaryCheckoutUseSourceV1.Type;

// These references carry captured facts; only the owning SQL service can authorize their use.
export const OrdinaryCheckoutUseV1 = Schema.Struct({
  version: Schema.Literal(1), kind: Schema.Literal("ordinary_checkout_use"),
  operationId: TrimmedNonEmptyString, admission: OrdinaryCheckoutAdmissionRefV1,
  source: OrdinaryCheckoutUseSourceV1, lease: OrdinaryCheckoutLeaseV1,
});
export type OrdinaryCheckoutUseV1 = typeof OrdinaryCheckoutUseV1.Type;

export const OrdinaryCheckoutTargetTransitionV1 = Schema.Struct({
  version: Schema.Literal(1), operationId: TrimmedNonEmptyString,
  admission: OrdinaryCheckoutAdmissionRefV1, source: OrdinaryCheckoutUseSourceV1,
  canonicalCheckoutPath: absolutePath, leaseId: TrimmedNonEmptyString,
  applicationBirth: OrdinaryApplicationBirthV1, beforeLease: OrdinaryCheckoutLeaseV1, afterLease: OrdinaryCheckoutLeaseV1,
  fromBranch: Schema.NullOr(Schema.String), toBranch: Schema.NullOr(Schema.String),
  evidence: jsonObject, recordedAt: Schema.DateTimeUtcFromString,
});
export type OrdinaryCheckoutTargetTransitionV1 = typeof OrdinaryCheckoutTargetTransitionV1.Type;

export const OrdinaryCheckoutOwnMutationV1 = Schema.Struct({
  mutation: Schema.Literals(["ensure_ordinary", "renew"]), ordinaryUse: OrdinaryCheckoutUseV1,
});
export type OrdinaryCheckoutOwnMutationV1 = typeof OrdinaryCheckoutOwnMutationV1.Type;

// Ownership conflicts retain the existing WorktreeOwnershipConflictError with actual owner fields.
export class OrdinaryCheckoutOwnershipError extends Schema.TaggedError<OrdinaryCheckoutOwnershipError>()(
  "OrdinaryCheckoutOwnershipError", {
    reason: Schema.Literals(["unavailable", "stale_admission", "claim_mismatch", "target_changed", "unknown_use"]),
    threadId: ThreadId, path: Schema.String, message: Schema.String,
  },
) {}

export function ordinaryApplicationIncarnationV1(birth: OrdinaryApplicationBirthV1): string {
  return JSON.stringify(["t3.orchestration-v2.thread-birth/v1", birth.eventId, birth.sequence]);
}

// Renewal changes liveness, never the immutable captured lease identity.
export function ordinaryCheckoutLeaseIdentityV1(lease: OrdinaryCheckoutLeaseV1) {
  return { resourcePath: lease.resourcePath, leaseId: lease.leaseId,
    ownerThreadId: lease.ownerThreadId, ownerIncarnation: lease.ownerIncarnation,
    branch: lease.branch, acquiredAtMs: lease.acquiredAtMs };
}

export const ordinaryCheckoutCommandDigestV1 = (encodedCommand: Record<string, unknown>): string =>
  nativeCreationSha256(nativeCreationCanonicalJson(encodedCommand));

export function ordinaryCheckoutAdmissionIdV1(capture: OrdinaryCheckoutCaptureV1): string {
  return nativeCreationSha256(nativeCreationCanonicalJson({ version: 1,
    commandId: capture.commandId, threadId: capture.threadId, applicationBirth: capture.applicationBirth,
  }));
}

export function ordinaryCheckoutAdmissionRefV1(admission: OrdinaryCheckoutAdmissionV1): OrdinaryCheckoutAdmissionRefV1 {
  const encoded = Schema.encodeSync(OrdinaryCheckoutAdmissionV1)(admission);
  return { version: 1, admissionId: admission.admissionId,
    admissionSha256: nativeCreationSha256(nativeCreationCanonicalJson({ ...encoded,
      capture: { ...encoded.capture, lease: ordinaryCheckoutLeaseIdentityV1(admission.capture.lease) },
    })) };
}

export function ordinaryCheckoutCaptureMatchesV1(capture: OrdinaryCheckoutCaptureV1): boolean {
  return capture.commandId === capture.canonicalCommand.commandId
    && capture.commandType === capture.canonicalCommand.type
    && capture.commandDigest === ordinaryCheckoutCommandDigestV1(capture.canonicalCommand)
    && capture.applicationBirth.threadId === capture.threadId
    && capture.lease.ownerThreadId === capture.threadId
    && capture.lease.ownerIncarnation === ordinaryApplicationIncarnationV1(capture.applicationBirth)
    && capture.lease.resourcePath === capture.canonicalCheckoutPath
    && capture.lease.branch === capture.branch;
}

export function ordinaryCheckoutAdmissionMatchesV1(admission: OrdinaryCheckoutAdmissionV1): boolean {
  const { capture, receipt, eventBasis } = admission;
  return ordinaryCheckoutCaptureMatchesV1(capture)
    && admission.admissionId === ordinaryCheckoutAdmissionIdV1(capture)
    && receipt.commandId === capture.commandId && receipt.commandType === capture.commandType
    && (capture.origin.kind === "delegated_child"
      ? capture.canonicalCommand.parentThreadId === receipt.threadId
      : capture.canonicalCommand.threadId === receipt.threadId)
    && (capture.origin.kind !== "runtime_request_answer" || capture.commandType === "runtime-request.respond")
    && (capture.origin.kind !== "delegated_child" || capture.commandType === "delegated_task.request")
    && receipt.threadId === (capture.origin.kind === "delegated_child" ? capture.origin.parentThreadId : capture.threadId)
    && eventBasis.every((event, index) => event.sequence <= receipt.resultSequence
      && (index === 0 || event.sequence > eventBasis[index - 1]!.sequence))
    && eventBasis.some((event) => event.threadId === capture.threadId && event.commandId === capture.commandId);
}

/**
 * Owning SQL transaction: recheck birth, canonical target, deletion admission and exact stable lease;
 * commit lease, receipt, events, admission and initial links atomically. Publish cache only on outer
 * commit. Exact replay resolves the original admission without acquiring or rotating a lease.
 * Begin/revalidate use resolves persisted facts and the current real claim (or original prepared
 * association), then reserves the existing native_operation path admission before external work.
 * Own ensure/renew is eligible only against its exact reserved/started ordinary use; unknown,
 * release, rotation, removal and foreign operations remain fenced. Completion/no-effect/unknown
 * updates that existing admission. A verified branch transition retains original path/lease and
 * immutable history. Carrier shape or digest alone supplies none of these currentness proofs.
 */
export interface OrdinaryCheckoutOwnershipTransactionContractV1 {
  readonly capture: OrdinaryCheckoutCaptureV1;
  readonly originalAdmission: OrdinaryCheckoutAdmissionRefV1 | null;
}

export const OrdinaryCheckoutOutboxExecutionSourceV1 = Schema.Struct({
  kind: Schema.Literal("outbox"), link: OrdinaryCheckoutEffectLinkV1,
  workerId: TrimmedNonEmptyString, expectedAttempt: positiveInt, leaseExpiresAt: Schema.DateTimeUtcFromString,
});
export const OrdinaryCheckoutPreparedExecutionSourceV1 = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("prepared_run"), admission: OrdinaryCheckoutAdmissionRefV1,
    preparation: OrdinaryAcceptedRunV1 }),
  Schema.Struct({ kind: Schema.Literal("prepared_launch"), admission: OrdinaryCheckoutAdmissionRefV1,
    preparationCommandId: CommandId, preparationEvent: OrdinaryAcceptedEventV1,
    applicationBirth: OrdinaryApplicationBirthV1, projectId: ProjectId,
    canonicalProjectRoot: absolutePath, canonicalCheckoutPath: absolutePath, branch: Schema.NullOr(Schema.String) }),
]);

export const OrdinaryCheckoutExecutionExecutorV1 = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("actual_outbox_claim"), source: OrdinaryCheckoutOutboxExecutionSourceV1 }),
  Schema.Struct({ kind: Schema.Literal("captured_managed_run"), captureId: TrimmedNonEmptyString,
    run: OrdinaryAcceptedRunV1, checkpointScopeId: CheckpointScopeId, driver: ProviderDriverKind,
    binding: Schema.Struct({ threadId: ThreadId, providerThreadId: ProviderThreadId,
      providerSessionId: ProviderSessionId, instanceId: ProviderInstanceId }),
    runtimeGeneration: Schema.optionalKey(TrimmedNonEmptyString),
    nativeThreadId: Schema.optionalKey(TrimmedNonEmptyString),
    evidenceRevision: Schema.optionalKey(positiveInt), providerTurnId: Schema.optionalKey(ProviderTurnId) }),
  Schema.Struct({ kind: Schema.Literal("actual_prepared_producer"), producerId: TrimmedNonEmptyString,
    source: OrdinaryCheckoutPreparedExecutionSourceV1 }),
]);
export type OrdinaryCheckoutExecutionExecutorV1 = typeof OrdinaryCheckoutExecutionExecutorV1.Type;

const OrdinaryCheckoutExecutionSeedV1 = Schema.Struct({
  version: Schema.Literal(1), originalUse: OrdinaryCheckoutUseV1, executor: OrdinaryCheckoutExecutionExecutorV1,
});
export type OrdinaryCheckoutExecutionSeedV1 = typeof OrdinaryCheckoutExecutionSeedV1.Type;

export const ordinaryCheckoutExecutionAssociationIdV1 = (seed: OrdinaryCheckoutExecutionSeedV1): string =>
  nativeCreationSha256(nativeCreationCanonicalJson(Schema.encodeSync(OrdinaryCheckoutExecutionSeedV1)(seed)));

// A ref identifies immutable executor facts. Its digest cannot issue a captured handle or current permission.
export const OrdinaryCheckoutExecutionRefV1 = Schema.Struct({
  version: Schema.Literal(1), originalUse: OrdinaryCheckoutUseV1,
  associationId: sha256, executor: OrdinaryCheckoutExecutionExecutorV1,
}).check(Schema.makeFilter((ref) => ref.associationId === ordinaryCheckoutExecutionAssociationIdV1({
  version: ref.version, originalUse: ref.originalUse, executor: ref.executor,
})));
export type OrdinaryCheckoutExecutionRefV1 = typeof OrdinaryCheckoutExecutionRefV1.Type;

export const decodeOrdinaryCheckoutExecutionRefV1 = Schema.decodeUnknownSync(
  OrdinaryCheckoutExecutionRefV1, { onExcessProperty: "error" },
);

export function makeOrdinaryCheckoutExecutionRefV1(
  input: { readonly originalUse: OrdinaryCheckoutUseV1; readonly executor: OrdinaryCheckoutExecutionExecutorV1 },
): OrdinaryCheckoutExecutionRefV1 {
  const seed = { version: 1 as const, originalUse: input.originalUse, executor: input.executor };
  return decodeOrdinaryCheckoutExecutionRefV1(Schema.encodeSync(OrdinaryCheckoutExecutionRefV1)({
    ...seed, associationId: ordinaryCheckoutExecutionAssociationIdV1(seed),
  }));
}

export function ordinaryCheckoutOutboxOperationIdV1(effectId: string, expectedAttempt: number): string {
  const input = Schema.decodeUnknownSync(Schema.Struct({
    effectId: Schema.String.check(Schema.makeFilter((value) => value.length > 0 && value.trim() === value)),
    expectedAttempt: positiveInt,
  }))({
    effectId, expectedAttempt,
  });
  return `${input.effectId}:ordinary-checkout:attempt:${input.expectedAttempt}`;
}
