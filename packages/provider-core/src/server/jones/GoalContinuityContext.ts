import type {
  ContinuityDispatchPermit,
  ContinuityDeliveryEvidence,
  ContinuityDeliveryResult,
  ContinuityGoalRef,
  ContinuityOwner,
  ContinuityPendingTicket,
  ContinuityPreparedTicket,
  ContinuityRecoveryAuthorization,
  ContinuityRecoveryBoundary,
  ContinuityRecoveryCapability,
  ContinuityRecoveryTransition,
  ContinuityRecoveryTransitionResult,
  DeliveryGate,
  GoalContinuityError,
  RecoveryProjection,
} from "@t3tools/contracts";
import type * as Effect from "effect/Effect";
import type { ProviderEventProducerOrigin } from "./ProviderEventOrigin.ts";

/** The serializable identity is persisted; the captured origin stays in memory.
 * The service verifies their association rather than trusting caller fields.
 */
export interface RecoveryBoundary extends ContinuityRecoveryBoundary {
  readonly producerOrigin: ProviderEventProducerOrigin;
}

// Always resolve delivery history first, independently of the current-record
// pointer and before loading a provider. A hold can therefore have no record.
export type ContinuityRequirement =
  | { readonly outcome: "absent"; readonly gate: Extract<DeliveryGate, { outcome: "clear" }> }
  | {
      readonly outcome: "required";
      readonly owner: ContinuityOwner;
      readonly target: ContinuityGoalRef;
      readonly gate: Extract<DeliveryGate, { outcome: "clear" }>;
    }
  | {
      readonly outcome: "hold";
      readonly reason: "delivery_pending" | "delivery_unknown" | "recovery_required";
      readonly gate: Extract<DeliveryGate, { outcome: "hold" }>;
      readonly current: { readonly owner: ContinuityOwner; readonly target: ContinuityGoalRef } | null;
    }
  | {
      readonly outcome: "hold";
      readonly reason: "unavailable" | "corrupt_snapshot" | "detached" | "terminal";
      // null preserves unknown fence state when the durable lookup itself fails.
      readonly gate: DeliveryGate | null;
      readonly current: { readonly owner: ContinuityOwner; readonly target: ContinuityGoalRef } | null;
    };

export interface ContinuityBeginDelivery {
  readonly pendingTicket: ContinuityPendingTicket;
  readonly projection: Extract<RecoveryProjection, { outcome: "context" }>;
  readonly recoveryAuthorization?: ContinuityRecoveryAuthorization;
}
export interface ContinuitySettleDelivery {
  readonly permit: ContinuityDispatchPermit;
  readonly evidence: ContinuityDeliveryEvidence;
}
export interface ContinuityContinuationCheck {
  readonly requirement: ContinuityRequirement;
  readonly currentOwner: ContinuityOwner | null;
}

/** Internal captured-producer callbacks, never an MCP authorization surface.
 * Agent writes require a service-derived live witness revalidated at commit;
 * authenticated owner administration uses a separate service authorization path
 * without loading a provider. No request may choose between those authorities.
 *
 * recordRecoveryTransition matches compaction started/completed by exact boundary
 * and deduplicates event IDs; turn_start is a completed-only boundary. The reader
 * may await this short durable operation, never injection or the stop RPC.
 * prepareRecovery remains a fresh read-only projection. beginDelivery binds it
 * under CAS and consumes any exact-target one-use recovery authorization.
 *
 * prepared grants no send authority. markDispatching commits dispatching before
 * issuing a permit ONCE, revalidating producer, snapshot, binding and fence. A
 * duplicate ticket must not yield another send permit. The adapter consumes the
 * permit once in its captured producer's scope; a failed/unknown commit sends
 * nothing. Reconstructed dispatching is unknown, never an automatic retry.
 *
 * settleDelivery authenticates evidence against the original permit/producer,
 * including late acknowledgements after retirement. It must not borrow current
 * producer identity, settle a successor or erase an owner disposition. Unknown
 * triggers the bound stop path; stop uncertainty is not quiescence evidence.
 *
 * checkContinuation is used immediately before ordinary dispatch and before each
 * held native successor's adoption. A hold uses the captured revalidated pause /
 * interrupt path. Acknowledgement never resumes work. These reactive gates do
 * not establish a strict post-compaction pre-token barrier.
 */
export interface GoalContinuityContext {
  readonly capability: ContinuityRecoveryCapability;
  readonly recordRecoveryTransition: (
    boundary: RecoveryBoundary,
    transition: ContinuityRecoveryTransition,
  ) => Effect.Effect<ContinuityRecoveryTransitionResult, GoalContinuityError>;
  readonly prepareRecovery: (boundary: RecoveryBoundary) => Effect.Effect<RecoveryProjection, GoalContinuityError>;
  readonly beginDelivery: (input: ContinuityBeginDelivery) => Effect.Effect<ContinuityDeliveryResult, GoalContinuityError>;
  readonly markDispatching: (ticket: ContinuityPreparedTicket) => Effect.Effect<ContinuityDeliveryResult, GoalContinuityError>;
  readonly settleDelivery: (input: ContinuitySettleDelivery) => Effect.Effect<ContinuityDeliveryResult, GoalContinuityError>;
  readonly checkContinuation: (boundary: RecoveryBoundary) => Effect.Effect<ContinuityContinuationCheck, GoalContinuityError>;
}
