import * as Schema from "effect/Schema";
import { ModelSelection } from "../modelSelection.ts";

const Identity = Schema.NonEmptyString.check(Schema.isMaxLength(256));
export const RemoteAdmissionDigest = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
export const RemoteAdmissionStatus = Schema.Literals([
  "evaluating",
  "passed",
  "held",
  "approved",
  "denied",
  "cancelled",
  "dispatching",
  "dispatched",
]);
export type RemoteAdmissionStatus = typeof RemoteAdmissionStatus.Type;

export const RemoteAdmissionReceiverPolicy = Schema.Struct({
  enabled: Schema.Boolean,
  revision: Identity,
  modelSelection: Schema.NullOr(ModelSelection),
});
export type RemoteAdmissionReceiverPolicy = typeof RemoteAdmissionReceiverPolicy.Type;

export const RemoteAdmissionEnvelope = Schema.Struct({
  identity: Schema.Struct({ clientId: Identity, linkId: Schema.NullOr(Identity) }),
  operation: Identity,
  payload: Schema.Json,
  target: Schema.Struct({
    projectId: Schema.NullOr(Identity),
    threadId: Schema.NullOr(Identity),
    runtimeMode: Identity,
    interactionMode: Identity,
  }),
  receiverPolicy: RemoteAdmissionReceiverPolicy,
});
export type RemoteAdmissionEnvelope = typeof RemoteAdmissionEnvelope.Type;
export const RemoteAdmissionRequest = Schema.Struct({
  idempotencyKey: Identity,
  envelope: RemoteAdmissionEnvelope,
});
export type RemoteAdmissionRequest = typeof RemoteAdmissionRequest.Type;

const Reason = Schema.String.check(
  Schema.isMaxLength(600),
  Schema.makeFilter((value) => value.trim().length > 0),
);
export const RemoteAdmissionEvaluation = Schema.Union([
  Schema.Struct({
    verdict: Schema.Literal("pass"),
    category: Schema.Literal("none"),
    reason: Reason,
  }),
  Schema.Struct({
    verdict: Schema.Literal("hold"),
    category: Schema.Literals(["malicious", "ridiculous", "risky", "out_of_scope"]),
    reason: Reason,
  }),
]);
export type RemoteAdmissionEvaluation = typeof RemoteAdmissionEvaluation.Type;

export const RemoteAdmissionPublicStatus = Schema.Struct({
  id: Identity,
  bindingDigest: RemoteAdmissionDigest,
  status: RemoteAdmissionStatus,
  commandId: Schema.NullOr(Identity),
  createdAt: Schema.String,
  updatedAt: Schema.String,
});
export type RemoteAdmissionPublicStatus = typeof RemoteAdmissionPublicStatus.Type;

export const RemoteAdmissionRecord = Schema.Struct({
  ...RemoteAdmissionPublicStatus.fields,
  idempotencyKey: Identity,
  requestDigest: RemoteAdmissionDigest,
  envelope: RemoteAdmissionEnvelope,
  reason: Schema.NullOr(Reason),
  category: Schema.NullOr(
    Schema.Literals([
      "none",
      "malicious",
      "ridiculous",
      "risky",
      "out_of_scope",
      "evaluation_unavailable",
      "evaluation_interrupted",
    ]),
  ),
  resolvedBy: Schema.NullOr(Identity),
  resolvedAt: Schema.NullOr(Schema.String),
});
export type RemoteAdmissionRecord = typeof RemoteAdmissionRecord.Type;

// Only owner-local views may receive the full record, including evaluator reasons and payload.
export const publicRemoteAdmission = (
  record: RemoteAdmissionRecord,
): RemoteAdmissionPublicStatus => ({
  id: record.id,
  bindingDigest: record.bindingDigest,
  status: record.status,
  commandId: record.commandId,
  createdAt: record.createdAt,
  updatedAt: record.updatedAt,
});
