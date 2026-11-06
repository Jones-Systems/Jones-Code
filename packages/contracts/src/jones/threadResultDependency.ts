import * as Schema from "effect/Schema";
import { ThreadId, MessageId, RunId } from "../baseSchemas.ts";

const Identity = Schema.NonEmptyString.check(Schema.isMaxLength(256));
export const ThreadResultRequestDescriptor = Schema.Struct({
  schemaVersion: Schema.Literal("t3.thread-result-request/v1"),
  operationId: Identity,
});
export type ThreadResultRequestDescriptor = typeof ThreadResultRequestDescriptor.Type;
export const ThreadResultOutcome = Schema.Literals([
  "fulfilled",
  "declined",
  "refused",
  "withdrawn",
  "dropped",
  "unknown",
  "not_sent",
  "released",
]);
export type ThreadResultOutcome = typeof ThreadResultOutcome.Type;
export const ThreadResultObservation = Schema.Literals([
  "admission_held",
  "admission_dispatching",
  "queued",
  "queued_held",
  "receiver_working",
  "receiver_needs_owner",
  "receiver_idle_unanswered",
  "receiver_stopped",
  "unreachable",
  "delivery_unknown",
]);
export type ThreadResultObservation = typeof ThreadResultObservation.Type;
export const ThreadResultDependencyBinding = Schema.Struct({
  dependencyId: Identity,
  requestId: Schema.NullOr(Identity),
  state: Schema.Literals(["sending", "awaiting", "resolved", "delivered", "released"]),
});
export const ThreadResultDependency = Schema.Struct({
  ...ThreadResultDependencyBinding.fields,
  operationId: Identity,
  targetThreadId: ThreadId,
  targetEnvironmentId: Schema.NullOr(Schema.String),
  targetLabel: Schema.String,
  messageId: Schema.NullOr(MessageId),
  observation: ThreadResultObservation,
  outcome: Schema.NullOr(ThreadResultOutcome),
  result: Schema.NullOr(Schema.String.check(Schema.isMaxLength(16_000))),
});
export type ThreadResultDependency = typeof ThreadResultDependency.Type;
export const ThreadResultRequest = Schema.Struct({
  requestId: Identity,
  operationId: Identity,
  targetThreadId: ThreadId,
  messageId: MessageId,
  originThreadId: ThreadId,
  originEnvironmentId: Schema.String,
  originLabel: Schema.String,
  state: Schema.Literals(["pending", "fulfilled", "declined", "dropped", "withdrawn"]),
  result: Schema.NullOr(Schema.String.check(Schema.isMaxLength(16_000))),
  resolvedByRunId: Schema.NullOr(RunId),
  requesterReleased: Schema.Boolean,
});
export type ThreadResultRequest = typeof ThreadResultRequest.Type;
export const ThreadRequestFulfillInput = Schema.Struct({
  requestId: Identity,
  outcome: Schema.Literals(["fulfilled", "declined"]),
  result: Schema.NonEmptyString.check(Schema.isMaxLength(16_000)),
  clientRequestId: Schema.optional(Identity),
});
export const ThreadRequestFulfillResult = Schema.Struct({
  request: ThreadResultRequest,
  requester: Schema.Literals(["waiting", "released", "linked"]),
});
export const ThreadDependencyReleaseInput = Schema.Struct({ dependencyId: Identity });
export const ThreadDependencyReleaseResult = Schema.Struct({ released: Schema.Boolean });
export const ThreadRequestObserveFields = {
  operationId: Identity,
  afterToken: Schema.optional(Schema.String.check(Schema.isMaxLength(4096))),
  fence: Schema.optional(Schema.Boolean),
  timeoutMs: Schema.optional(
    Schema.Number.check(Schema.isBetween({ minimum: 0, maximum: 50_000 })),
  ),
};
export const ThreadRequestObservation = Schema.Struct({
  token: Schema.String,
  state: Schema.Literals(["pending", "answered", "not_sent"]),
  request: Schema.NullOr(ThreadResultRequest),
  admissionId: Schema.NullOr(Schema.String),
  observation: ThreadResultObservation,
  outcome: Schema.NullOr(ThreadResultOutcome),
  result: Schema.NullOr(Schema.String.check(Schema.isMaxLength(16_000))),
});
export type ThreadRequestObservation = typeof ThreadRequestObservation.Type;
