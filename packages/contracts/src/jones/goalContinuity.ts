import * as Schema from "effect/Schema";
import {
  EnvironmentId,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ThreadId,
  TrimmedNonEmptyString,
} from "../baseSchemas.ts";
import { ProviderDriverKind, ProviderInstanceId } from "../providerInstance.ts";

// Do not trim content before fingerprinting: a request key names the exact request.
const key = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(256),
  Schema.makeFilter((value) => value.trim() === value && value.length > 0),
);
const text = Schema.String.check(
  Schema.isMinLength(1),
  Schema.makeFilter((value) => value.trim().length > 0),
);
export const ContinuityCounter = Schema.String.check(
  Schema.makeFilter((value) =>
    /^(0|[1-9][0-9]{0,18})(?![\s\S])/.test(value) && BigInt(value) <= 9223372036854775807n,
  ),
);
export type ContinuityCounter = typeof ContinuityCounter.Type;
export const ContinuityDigest = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}(?![\s\S])/));
export type ContinuityDigest = typeof ContinuityDigest.Type;

export const ContinuityOwner = Schema.Struct({
  environmentId: EnvironmentId,
  threadId: ThreadId,
  providerInstanceId: ProviderInstanceId,
  providerThreadId: ProviderThreadId,
  driver: ProviderDriverKind,
  nativeThreadId: text,
});
export type ContinuityOwner = typeof ContinuityOwner.Type;
export const ContinuityWitness = Schema.Struct({
  ...ContinuityOwner.fields,
  providerSessionId: ProviderSessionId,
  runtimeGeneration: TrimmedNonEmptyString,
  runId: RunId,
  attemptId: RunAttemptId,
});
export type ContinuityWitness = typeof ContinuityWitness.Type;
export const ContinuityTarget = Schema.Struct({
  goalId: key,
  revision: ContinuityCounter,
  generation: ContinuityCounter,
  owner: ContinuityOwner,
});
export type ContinuityTarget = typeof ContinuityTarget.Type;
export const ContinuityGoalRef = Schema.Struct({
  goalId: key,
  revision: ContinuityCounter,
  generation: ContinuityCounter,
  snapshotDigest: ContinuityDigest,
});
export type ContinuityGoalRef = typeof ContinuityGoalRef.Type;

export const ContinuityItem = Schema.Struct({ id: key, text });
export type ContinuityItem = typeof ContinuityItem.Type;
export const ContinuityEvidence = Schema.Struct({ id: key, reference: text, assertion: text });
export type ContinuityEvidence = typeof ContinuityEvidence.Type;
export const ContinuityBoundary = Schema.Struct({
  id: key,
  text,
  finishIds: Schema.Array(key),
  evidenceIds: Schema.Array(key),
});
export type ContinuityBoundary = typeof ContinuityBoundary.Type;
export const ContinuitySource = Schema.Struct({
  type: Schema.Literals(["owner_request", "document_reference", "explicit_derivation"]),
  reference: text,
  identity: text,
});
export type ContinuitySource = typeof ContinuitySource.Type;
export const ContinuityIntent = Schema.Struct({
  goalId: key,
  revision: ContinuityCounter,
  northStar: text,
  preservedQualities: Schema.Array(ContinuityItem),
  finishItems: Schema.Array(ContinuityItem).check(Schema.isMinLength(1)),
  source: ContinuitySource,
  derivation: Schema.NullOr(text),
  initializationMode: Schema.Literals(["direct", "confirm_first"]),
  startEvidence: Schema.Array(ContinuityEvidence).check(Schema.isMinLength(1)),
  predecessor: Schema.NullOr(ContinuityGoalRef),
});
export type ContinuityIntent = typeof ContinuityIntent.Type;

export const ContinuityUnresolvedEffect = Schema.Struct({ id: key, description: text });
export type ContinuityUnresolvedEffect = typeof ContinuityUnresolvedEffect.Type;
export const ContinuityDisposition = Schema.Struct({
  effectId: key,
  resolution: Schema.Literals(["known_no_effect", "effect_confirmed"]),
  evidenceIds: Schema.Array(key).check(Schema.isMinLength(1)),
  generation: ContinuityCounter,
});
export type ContinuityDisposition = typeof ContinuityDisposition.Type;
export const ContinuityAttachment = Schema.Union([
  Schema.Struct({ state: Schema.Literal("attached") }),
  Schema.Struct({ state: Schema.Literal("detached"), reason: text }),
]);
export type ContinuityAttachment = typeof ContinuityAttachment.Type;
export const ContinuitySnapshot = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  intent: ContinuityIntent,
  owner: ContinuityOwner,
  attachment: ContinuityAttachment,
  generation: ContinuityCounter,
  status: Schema.Literals(["active", "blocked", "complete", "abandoned"]),
  focus: Schema.NullOr(text),
  boundaries: Schema.Array(ContinuityBoundary),
  decisions: Schema.Array(ContinuityItem),
  blockers: Schema.Array(ContinuityItem),
  nextActions: Schema.Array(ContinuityItem),
  evidence: Schema.Array(ContinuityEvidence),
  unresolvedEffect: Schema.NullOr(ContinuityUnresolvedEffect),
  recentDispositions: Schema.Array(ContinuityDisposition),
  completionEvidenceIds: Schema.Array(key),
});
export type ContinuitySnapshot = typeof ContinuitySnapshot.Type;

const itemOperations = Schema.Struct({ add: Schema.Array(ContinuityItem), remove: Schema.Array(key) });
export const ContinuityPatch = Schema.Struct({
  focus: Schema.optionalKey(Schema.NullOr(text)),
  status: Schema.optionalKey(Schema.Literals(["active", "blocked"])),
  boundaries: Schema.optionalKey(
    Schema.Struct({ append: Schema.Array(ContinuityBoundary) }).annotate({
      parseOptions: { onExcessProperty: "error" },
    }),
  ),
  decisions: Schema.optionalKey(itemOperations),
  blockers: Schema.optionalKey(itemOperations),
  evidence: Schema.optionalKey(
    Schema.Struct({ add: Schema.Array(ContinuityEvidence), remove: Schema.Array(key) }),
  ),
  nextActions: Schema.optionalKey(Schema.Array(ContinuityItem)),
  unresolvedEffect: Schema.optionalKey(
    Schema.Union([
      Schema.Struct({ operation: Schema.Literal("set"), effect: ContinuityUnresolvedEffect }),
      Schema.Struct({
        operation: Schema.Literal("resolve"),
        effectId: key,
        resolution: Schema.Literals(["known_no_effect", "effect_confirmed"]),
        evidenceIds: Schema.Array(key).check(Schema.isMinLength(1)),
      }),
    ]),
  ),
});
export type ContinuityPatch = typeof ContinuityPatch.Type;

export const ContinuityRefusalReason = Schema.Literals([
  "invalid_request", "invalid_counter", "counter_exhausted", "corrupt_snapshot", "snapshot_too_large",
  "stale", "binding_mismatch", "detached", "terminal", "duplicate", "unknown_removal",
  "contradictory_patch", "no_change", "foreign_finish", "missing_evidence", "unresolved_effect",
  "uncovered_finish", "blockers", "successor_required", "confirmation_required", "confirmation_mismatch",
  "proposal_mismatch", "unsupported", "unavailable", "forbidden", "request_key_conflict",
  "selection_bounds", "digest_mismatch", "projection_too_large", "delivery_unknown",
  "delivery_pending", "recovery_required", "invalid_transition", "permit_consumed",
  "permit_revoked", "quiescence_required", "no_effect_proof_required",
]);
export type ContinuityRefusalReason = typeof ContinuityRefusalReason.Type;
export const ContinuityRefusal = Schema.Struct({
  outcome: Schema.Literal("refused"),
  reason: ContinuityRefusalReason,
});
export type ContinuityRefusal = typeof ContinuityRefusal.Type;
export class GoalContinuityError extends Schema.TaggedError<GoalContinuityError>()(
  "GoalContinuityError", { reason: ContinuityRefusalReason },
) {
  override get message(): string { return `Goal continuity refused: ${this.reason}.`; }
}

export const ContinuitySelectionField = Schema.Literals([
  "intent", "focus", "status", "attachment", "boundaries", "decisions", "blockers", "nextActions",
  "evidence", "unresolvedEffect", "recentDispositions", "completionEvidenceIds", "finishItems", "preservedQualities", "startEvidence",
]);
export type ContinuitySelectionField = typeof ContinuitySelectionField.Type;
export const ContinuitySelectionCursor = Schema.Struct({
  generation: ContinuityCounter,
  fieldDigest: ContinuityDigest,
  offset: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
});
export type ContinuitySelectionCursor = typeof ContinuitySelectionCursor.Type;
export const ContinuitySelection = Schema.Struct({
  field: ContinuitySelectionField,
  cursor: Schema.optionalKey(ContinuitySelectionCursor),
});
export type ContinuitySelection = typeof ContinuitySelection.Type;
export const ContinuitySelectionRequest = Schema.Struct({
  goalId: key,
  revision: ContinuityCounter,
  generation: ContinuityCounter,
  fields: Schema.Array(ContinuitySelection),
});
export type ContinuitySelectionRequest = typeof ContinuitySelectionRequest.Type;
export const ContinuityIntentHeader = Schema.Struct({
  goalId: key,
  revision: ContinuityCounter,
  northStar: text,
  source: ContinuitySource,
  derivation: Schema.NullOr(text),
  initializationMode: Schema.Literals(["direct", "confirm_first"]),
  predecessor: Schema.NullOr(ContinuityGoalRef),
});
export type ContinuityIntentHeader = typeof ContinuityIntentHeader.Type;
export const ContinuitySelectedValue = Schema.Union([
  Schema.Null,
  Schema.String,
  ContinuityIntentHeader,
  ContinuityAttachment,
  ContinuityUnresolvedEffect,
  Schema.Array(ContinuityBoundary),
  Schema.Array(ContinuityItem),
  Schema.Array(ContinuityEvidence),
  Schema.Array(ContinuityDisposition),
  Schema.Array(key),
]);
export type ContinuitySelectedValue = typeof ContinuitySelectedValue.Type;
export const ContinuitySelectionResult = Schema.Union([
  ContinuityRefusal,
  Schema.Struct({
    outcome: Schema.Literal("selected"),
    goalId: key,
    revision: ContinuityCounter,
    generation: ContinuityCounter,
    snapshotDigest: ContinuityDigest,
    fields: Schema.Array(Schema.Struct({
      field: ContinuitySelectionField,
      fieldDigest: ContinuityDigest,
      totalItems: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
      value: ContinuitySelectedValue,
      nextCursor: Schema.NullOr(ContinuitySelectionCursor),
    })),
  }),
]);
export type ContinuitySelectionResult = typeof ContinuitySelectionResult.Type;

// A persisted proposal creates no attached record. Only the owner response path
// can mint confirmation provenance; the agent request contains no confirmation boolean.
export const ContinuityProposal = Schema.Struct({
  proposalId: key,
  intentDigest: ContinuityDigest,
  intent: ContinuityIntent,
  owner: ContinuityOwner,
});
export type ContinuityProposal = typeof ContinuityProposal.Type;
export const ContinuityOwnerConfirmation = Schema.Struct({
  confirmationId: key,
  proposalId: key,
  intentDigest: ContinuityDigest,
  owner: ContinuityOwner,
  ownerResponseId: key,
  authenticatedOwnerId: key,
});
export type ContinuityOwnerConfirmation = typeof ContinuityOwnerConfirmation.Type;
export const ContinuityRecoveryPoint = Schema.Union([
  Schema.Struct({ type: Schema.Literal("turn_start") }),
  Schema.Struct({ type: Schema.Literal("compaction"), nativeTurnId: text, itemId: text }),
]);
export type ContinuityRecoveryPoint = typeof ContinuityRecoveryPoint.Type;
export const RecoveryProjection = Schema.Union([
  Schema.Struct({ outcome: Schema.Literal("absent") }),
  Schema.Struct({ outcome: Schema.Literal("unsupported"), reason: ContinuityRefusalReason }),
  Schema.Struct({ outcome: Schema.Literal("hold"), reason: ContinuityRefusalReason }),
  Schema.Struct({
    outcome: Schema.Literal("context"),
    goalId: key,
    revision: ContinuityCounter,
    generation: ContinuityCounter,
    snapshotDigest: ContinuityDigest,
    text: Schema.String,
  }),
]);
export type RecoveryProjection = typeof RecoveryProjection.Type;
export const ContinuityRecoveryCapability = Schema.Struct({
  strongIdentity: Schema.Boolean,
  turnStart: Schema.Literals(["fresh_context", "unsupported"]),
  compaction: Schema.Literals(["fresh_context_injection", "unsupported"]),
  strictPreResumptionBarrier: Schema.Literal("unsupported"),
});
export type ContinuityRecoveryCapability = typeof ContinuityRecoveryCapability.Type;

// Expectations identify state to compare at commit; they do not authorize the caller.
export const ExpectedContinuity = Schema.Struct({
  ...ContinuityTarget.fields,
  snapshotDigest: ContinuityDigest,
});
export type ExpectedContinuity = typeof ExpectedContinuity.Type;
export const ExpectedRecoveryTarget = ExpectedContinuity;
export type ExpectedRecoveryTarget = typeof ExpectedRecoveryTarget.Type;

export const DeliveryPhase = Schema.Literals([
  "pending", "prepared", "dispatching", "acknowledged", "not_sent", "unknown",
]);
export type DeliveryPhase = typeof DeliveryPhase.Type;
export const DeliveryGate = Schema.Union([
  Schema.Struct({ outcome: Schema.Literal("clear"), fenceRevision: ContinuityCounter }),
  Schema.Struct({
    outcome: Schema.Literal("hold"),
    fenceRevision: ContinuityCounter,
    reason: Schema.Literals(["delivery_pending", "delivery_unknown", "recovery_required"]),
    deliveryIds: Schema.Array(key).check(Schema.isMinLength(1), Schema.makeFilter((ids) => new Set(ids).size === ids.length)),
  }),
]);
export type DeliveryGate = typeof DeliveryGate.Type;

// producerId is a server-minted incarnation ID, never an SDK client or object token.
export const ContinuityProducerIdentity = Schema.Struct({
  producerId: key,
  driver: ProviderDriverKind,
  providerInstanceId: ProviderInstanceId,
  providerSessionId: ProviderSessionId,
  runtimeGeneration: TrimmedNonEmptyString,
});
export type ContinuityProducerIdentity = typeof ContinuityProducerIdentity.Type;
export const ContinuityRecoveryBoundary = Schema.Struct({
  witness: ContinuityWitness,
  producer: ContinuityProducerIdentity,
  point: ContinuityRecoveryPoint,
});
export type ContinuityRecoveryBoundary = typeof ContinuityRecoveryBoundary.Type;
export const ContinuityRecoveryTransition = Schema.Struct({
  eventId: key,
  type: Schema.Literals(["started", "completed"]),
});
export type ContinuityRecoveryTransition = typeof ContinuityRecoveryTransition.Type;

export const ContinuityDeliveryIdentity = Schema.Struct({
  deliveryId: key,
  originalOwner: ContinuityOwner,
  goalId: key,
  revision: ContinuityCounter,
  generation: ContinuityCounter,
  point: ContinuityRecoveryPoint,
  witness: ContinuityWitness,
  producer: ContinuityProducerIdentity,
});
export type ContinuityDeliveryIdentity = typeof ContinuityDeliveryIdentity.Type;
export const ContinuityDeliveryPreparation = Schema.Struct({
  target: ExpectedRecoveryTarget,
  capsuleDigest: ContinuityDigest,
});
export type ContinuityDeliveryPreparation = typeof ContinuityDeliveryPreparation.Type;
const deliveryVersion = {
  identity: ContinuityDeliveryIdentity,
  deliveryRevision: ContinuityCounter,
  fenceRevision: ContinuityCounter,
};
export const ContinuityPendingTicket = Schema.Struct({
  type: Schema.Literal("pending_ticket"),
  ...deliveryVersion,
});
export type ContinuityPendingTicket = typeof ContinuityPendingTicket.Type;
export const ContinuityPreparedTicket = Schema.Struct({
  type: Schema.Literal("prepared_ticket"),
  ...deliveryVersion,
  preparation: ContinuityDeliveryPreparation,
});
export type ContinuityPreparedTicket = typeof ContinuityPreparedTicket.Type;

// A serialized permit is only a handle to committed, revocable single-use state.
// Issuance is durable and once-only; the captured adapter consumes it once.
// A reconstructed or revoked permit cannot authorize another send.
export const ContinuityDispatchPermit = Schema.Struct({
  type: Schema.Literal("dispatch_permit"),
  ...deliveryVersion,
  preparation: ContinuityDeliveryPreparation,
  permitId: key,
  dispatchAttemptId: key,
  rpcRequestId: key,
});
export type ContinuityDispatchPermit = typeof ContinuityDispatchPermit.Type;
export const ContinuityDeliveryEvidence = Schema.Union([
  Schema.Struct({
    outcome: Schema.Literal("acknowledged"),
    rpcRequestId: key,
    acknowledgementRef: key,
    witness: ContinuityWitness,
    producer: ContinuityProducerIdentity,
  }),
  Schema.Struct({
    outcome: Schema.Literal("not_sent"),
    proof: Schema.Union([
      Schema.Struct({ type: Schema.Literal("dispatch_precluded"), revocationRef: key }),
      Schema.Struct({ type: Schema.Literal("protocol_no_effect"), rpcRequestId: key, responseRef: key }),
    ]),
  }),
  Schema.Struct({
    outcome: Schema.Literal("unknown"),
    cause: Schema.Literals(["timeout", "cancelled", "lost_response", "reconstruction"]),
  }),
]);
export type ContinuityDeliveryEvidence = typeof ContinuityDeliveryEvidence.Type;
export const ContinuityDeliverySettlement = Schema.Struct({
  permit: ContinuityDispatchPermit,
  evidence: ContinuityDeliveryEvidence,
});
export type ContinuityDeliverySettlement = typeof ContinuityDeliverySettlement.Type;

export const ReconcileDeliveryRequest = Schema.Struct({
  action: Schema.Literal("reconcile_delivery"),
  clientRequestId: key,
  deliveryId: key,
  expectedOriginalOwner: ContinuityOwner,
  expectedDeliveryRevision: ContinuityCounter,
  expectedFenceRevision: ContinuityCounter,
  expectedCurrentRecord: Schema.NullOr(ExpectedContinuity),
  reason: text,
  evidenceRefs: Schema.Array(key).check(Schema.isMinLength(1), Schema.makeFilter((refs) => new Set(refs).size === refs.length)),
  disposition: Schema.Union([
    Schema.Struct({ type: Schema.Literal("retry_proven_not_sent"), recoveryTarget: ExpectedRecoveryTarget }),
    Schema.Struct({ type: Schema.Literal("accept_unknown_for_one_recovery"), recoveryTarget: ExpectedRecoveryTarget }),
    Schema.Struct({ type: Schema.Literal("retire_without_redelivery") }),
  ]),
}).annotate({ parseOptions: { onExcessProperty: "error" } });
export type ReconcileDeliveryRequest = typeof ReconcileDeliveryRequest.Type;

// This handle must resolve to an unconsumed service-owned grant. Serialized
// fields or possession alone never establish authorization.
export const ContinuityRecoveryAuthorization = Schema.Struct({
  type: Schema.Literal("single_use_recovery"),
  authorizationId: key,
  dispositionId: key,
  deliveryId: key,
  originalOwner: ContinuityOwner,
  recoveryTarget: ExpectedRecoveryTarget,
  fenceRevision: ContinuityCounter,
});
export type ContinuityRecoveryAuthorization = typeof ContinuityRecoveryAuthorization.Type;

// These are server-observed proofs, not evidence strings supplied by an owner.
export const ContinuityQuiescence = Schema.Union([
  Schema.Struct({ outcome: Schema.Literal("unproven") }),
  Schema.Struct({
    outcome: Schema.Literal("confirmed"),
    witness: ContinuityWitness,
    producer: ContinuityProducerIdentity,
    dispatchRevocationRef: key,
    nativeSource: Schema.Union([
      Schema.Struct({ type: Schema.Literal("terminated"), terminationRef: key }),
      Schema.Struct({ type: Schema.Literal("paused_and_settled"), pauseRef: key, turnSettlementRef: key }),
    ]),
  }),
]);
export type ContinuityQuiescence = typeof ContinuityQuiescence.Type;
export const ContinuityDeliveryDisposition = Schema.Struct({
  dispositionId: key,
  request: ReconcileDeliveryRequest,
  actorNamespace: key,
  actorId: key,
  deliveryRevision: ContinuityCounter,
  fenceRevision: ContinuityCounter,
  quiescence: ContinuityQuiescence,
  effect: Schema.Union([
    Schema.Struct({ type: Schema.Literal("held"), reason: Schema.Literals(["quiescence_required", "no_effect_proof_required", "recovery_required", "stale", "terminal"]) }),
    Schema.Struct({ type: Schema.Literal("recovery_authorized"), authorization: ContinuityRecoveryAuthorization }),
    Schema.Struct({ type: Schema.Literal("retired") }),
  ]),
}).check(Schema.makeFilter((decision) =>
  decision.effect.type === "held" ||
  (decision.quiescence.outcome === "confirmed" &&
    (decision.effect.type === "retired"
      ? decision.request.disposition.type === "retire_without_redelivery"
      : decision.request.disposition.type !== "retire_without_redelivery")),
));
export type ContinuityDeliveryDisposition = typeof ContinuityDeliveryDisposition.Type;
export const ReconcileDeliveryResult = Schema.Struct({
  outcome: Schema.Literal("delivery_reconciled"),
  disposition: ContinuityDeliveryDisposition,
  gate: DeliveryGate,
});
export type ReconcileDeliveryResult = typeof ReconcileDeliveryResult.Type;

// Events and decisions are append-only. A late ack adds to the original attempt's
// history; it neither rewrites uncertainty nor erases a disposition or successor.
export const ContinuityDeliveryPhaseEvent = Schema.Struct({
  eventId: key,
  deliveryRevision: ContinuityCounter,
  fenceRevision: ContinuityCounter,
  phase: DeliveryPhase,
  transition: Schema.NullOr(ContinuityRecoveryTransition),
  permit: Schema.NullOr(ContinuityDispatchPermit),
  evidence: Schema.NullOr(ContinuityDeliveryEvidence),
}).check(Schema.makeFilter((event) => {
  switch (event.phase) {
    case "pending": return event.transition !== null && event.permit === null && event.evidence === null;
    case "prepared": return event.transition === null && event.permit === null && event.evidence === null;
    case "dispatching": return event.transition === null && event.permit !== null && event.evidence === null;
    case "acknowledged": return event.transition === null && event.permit !== null && event.evidence?.outcome === "acknowledged";
    case "not_sent": return event.transition === null && event.evidence?.outcome === "not_sent";
    case "unknown": return event.transition === null && event.evidence?.outcome === "unknown";
  }
}));
export type ContinuityDeliveryPhaseEvent = typeof ContinuityDeliveryPhaseEvent.Type;
export const ContinuityDeliveryReceipt = Schema.Struct({
  ...deliveryVersion,
  phase: DeliveryPhase,
  preparation: Schema.NullOr(ContinuityDeliveryPreparation),
  events: Schema.Array(ContinuityDeliveryPhaseEvent).check(Schema.isMinLength(1)),
  dispositions: Schema.Array(ContinuityDeliveryDisposition),
}).check(Schema.makeFilter((receipt) =>
  receipt.events[receipt.events.length - 1]?.phase === receipt.phase &&
  (receipt.phase !== "pending" || receipt.preparation === null) &&
  (!["prepared", "dispatching", "acknowledged"].includes(receipt.phase) || receipt.preparation !== null),
));
export type ContinuityDeliveryReceipt = typeof ContinuityDeliveryReceipt.Type;
export const ContinuityRecoveryTransitionResult = Schema.Union([
  ContinuityRefusal,
  Schema.Struct({
    outcome: Schema.Literal("recorded"),
    receipt: ContinuityDeliveryReceipt,
    pendingTicket: Schema.NullOr(ContinuityPendingTicket),
    gate: DeliveryGate,
  }),
  Schema.Struct({ outcome: Schema.Literal("absent"), gate: DeliveryGate }),
]);
export type ContinuityRecoveryTransitionResult = typeof ContinuityRecoveryTransitionResult.Type;
export const ContinuityDeliveryResult = Schema.Union([
  ContinuityRefusal,
  Schema.Struct({ outcome: Schema.Literal("prepared"), ticket: ContinuityPreparedTicket, gate: DeliveryGate }),
  Schema.Struct({ outcome: Schema.Literal("dispatching"), permit: ContinuityDispatchPermit, gate: DeliveryGate }),
  Schema.Struct({ outcome: Schema.Literal("settled"), receipt: ContinuityDeliveryReceipt, gate: DeliveryGate }),
  Schema.Struct({ outcome: Schema.Literal("hold"), reason: ContinuityRefusalReason, gate: DeliveryGate }),
]);
export type ContinuityDeliveryResult = typeof ContinuityDeliveryResult.Type;

// Retain the intent name for downstream code; this describes preparation only
// and grants no send authority.
export const ContinuityDeliveryIntent = ContinuityDeliveryPreparation;
export type ContinuityDeliveryIntent = typeof ContinuityDeliveryIntent.Type;

const mutation = {
  clientRequestId: key,
  expected: ContinuityTarget,
};
export const ContinuityRequest = Schema.Union([
  Schema.Struct({
    action: Schema.Literal("initialize"),
    clientRequestId: key,
    expectedOwner: ContinuityOwner,
    expectedCurrent: Schema.NullOr(ContinuityGoalRef),
    intent: ContinuityIntent,
    initialization: Schema.Union([
      Schema.Struct({ type: Schema.Literal("direct") }),
      Schema.Struct({ type: Schema.Literal("propose"), proposalId: key }),
      Schema.Struct({ type: Schema.Literal("confirm"), proposalId: key, confirmationId: key }),
    ]),
  }),
  Schema.Struct({ action: Schema.Literal("read") }),
  ReconcileDeliveryRequest,
  Schema.Struct({ action: Schema.Literal("checkpoint"), ...mutation, patch: ContinuityPatch }),
  Schema.Struct({ action: Schema.Literal("close"), ...mutation, completionEvidenceIds: Schema.Array(key) }),
  Schema.Struct({ action: Schema.Literal("abandon"), ...mutation, reason: text, evidenceIds: Schema.Array(key) }),
  Schema.Struct({ action: Schema.Literal("detach"), ...mutation, reason: text }),
  Schema.Struct({ action: Schema.Literal("handoff"), ...mutation, destination: ContinuityOwner, evidenceIds: Schema.Array(key) }),
  Schema.Struct({ action: Schema.Literal("receipt"), clientRequestId: key, requestDigest: ContinuityDigest }),
  Schema.Struct({ action: Schema.Literal("delivery_receipt"), deliveryId: key }),
  Schema.Struct({ action: Schema.Literal("selection"), selection: ContinuitySelectionRequest }),
]).annotate({ parseOptions: { onExcessProperty: "error" } });
export type ContinuityRequest = typeof ContinuityRequest.Type;
export type ContinuityMutationRequest = Exclude<ContinuityRequest, { action: "read" | "receipt" | "delivery_receipt" | "selection" }>;

export const ContinuityMutationOutcome = Schema.Union([
  ContinuityRefusal,
  ReconcileDeliveryResult,
  Schema.Struct({ outcome: Schema.Literal("applied"), snapshot: ContinuitySnapshot }),
  Schema.Struct({ outcome: Schema.Literal("proposed"), proposal: ContinuityProposal }),
]);
export type ContinuityMutationOutcome = typeof ContinuityMutationOutcome.Type;
export const ContinuityReceipt = Schema.Struct({
  clientRequestId: key,
  requestDigest: ContinuityDigest,
  environmentId: EnvironmentId,
  threadId: ThreadId,
  actorNamespace: key,
  actorId: key,
  targetOwner: ContinuityOwner,
  witness: Schema.NullOr(ContinuityWitness),
  previousGeneration: Schema.NullOr(ContinuityCounter),
  attemptedGeneration: Schema.NullOr(ContinuityCounter),
  result: ContinuityMutationOutcome,
});
export type ContinuityReceipt = typeof ContinuityReceipt.Type;
export const ContinuityResult = Schema.Union([
  ContinuityMutationOutcome,
  ContinuitySelectionResult,
  Schema.Struct({ outcome: Schema.Literal("absent") }),
  Schema.Struct({ outcome: Schema.Literal("snapshot"), snapshot: ContinuitySnapshot, snapshotDigest: ContinuityDigest }),
  Schema.Struct({ outcome: Schema.Literal("receipt"), receipt: ContinuityReceipt, replayed: Schema.Boolean }),
  Schema.Struct({ outcome: Schema.Literal("delivery_receipt"), receipt: ContinuityDeliveryReceipt, gate: DeliveryGate }),
  Schema.Struct({ outcome: Schema.Literal("known_no_effect"), clientRequestId: key, requestDigest: ContinuityDigest }),
  Schema.Struct({
    outcome: Schema.Literal("unknown"),
    clientRequestId: key,
    requestDigest: ContinuityDigest,
    previousGeneration: Schema.NullOr(ContinuityCounter),
    attemptedGeneration: Schema.NullOr(ContinuityCounter),
  }),
]);
export type ContinuityResult = typeof ContinuityResult.Type;

