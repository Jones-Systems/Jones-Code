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
      readonly current: {
        readonly owner: ContinuityOwner;
        readonly target: ContinuityGoalRef;
      } | null;
    }
  | {
      readonly outcome: "hold";
      readonly reason: "unavailable" | "corrupt_snapshot" | "detached" | "terminal";
      // null preserves unknown fence state when the durable lookup itself fails.
      readonly gate: DeliveryGate | null;
      readonly current: {
        readonly owner: ContinuityOwner;
        readonly target: ContinuityGoalRef;
      } | null;
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

/** Captured-producer callbacks use durable dispatch-before-send permits once.
 * Reconstruction stays unknown; late evidence settles only the original attempt.
 * Holds never prove quiescence or a strict pre-token restoration barrier.
 */
export interface GoalContinuityContext {
  readonly capability: ContinuityRecoveryCapability;
  readonly recordRecoveryTransition: (
    boundary: RecoveryBoundary,
    transition: ContinuityRecoveryTransition,
  ) => Effect.Effect<ContinuityRecoveryTransitionResult, GoalContinuityError>;
  readonly prepareRecovery: (
    boundary: RecoveryBoundary,
  ) => Effect.Effect<RecoveryProjection, GoalContinuityError>;
  readonly beginDelivery: (
    input: ContinuityBeginDelivery,
  ) => Effect.Effect<ContinuityDeliveryResult, GoalContinuityError>;
  readonly markDispatching: (
    ticket: ContinuityPreparedTicket,
  ) => Effect.Effect<ContinuityDeliveryResult, GoalContinuityError>;
  readonly settleDelivery: (
    input: ContinuitySettleDelivery,
  ) => Effect.Effect<ContinuityDeliveryResult, GoalContinuityError>;
  readonly checkContinuation: (
    boundary: RecoveryBoundary,
  ) => Effect.Effect<ContinuityContinuationCheck, GoalContinuityError>;
}
