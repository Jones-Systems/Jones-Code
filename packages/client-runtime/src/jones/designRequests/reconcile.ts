import type { OrchestrationCommandObservation } from "@t3tools/contracts";
import type { DesignRequestReceiptStatus } from "@t3tools/contracts/jones/designRequests";

/** Authoritative readback of one command on its thread (`GET .../commands/:commandId`). */
export type DesignRequestObserve = (input: {
  readonly threadId: string;
  readonly commandId: string;
  readonly messageId: string;
}) => Promise<Pick<OrchestrationCommandObservation, "commandStatus" | "turn">>;

export type DesignRequestObservation = Pick<
  OrchestrationCommandObservation,
  "commandStatus" | "turn"
>;

export type DesignRequestDispatchOutcome =
  | { readonly kind: "dispatched" }
  | { readonly kind: "failed"; readonly message: string };

export interface DesignRequestClassification {
  readonly status: DesignRequestReceiptStatus;
  readonly reason?: string;
  readonly delivery?: "started" | "queued";
}

// The server rejects a command ID whose receipt belongs to another thread (OrchestratorCommandIdConflictError).
const CONFLICT = /was already handled for thread .+ and cannot be replayed/;

export const designRequestDelivery = (
  observation: Pick<OrchestrationCommandObservation, "turn">,
): "started" | "queued" =>
  observation.turn === null || observation.turn.state === "pending" ? "queued" : "started";

/**
 * Map a dispatch outcome plus its readback to a receipt. Acceptance comes only from the server:
 * a successful dispatch returns after the durable command receipt, and readback confirms it.
 * Anything without an authoritative result stays `unknown` and must be reconciled first.
 */
export function classifyDesignRequestDispatch(
  outcome: DesignRequestDispatchOutcome,
  observation: DesignRequestObservation | null,
): DesignRequestClassification {
  if (observation?.commandStatus === "accepted")
    return { status: "accepted", delivery: designRequestDelivery(observation) };
  if (observation?.commandStatus === "rejected") return { status: "rejected", reason: "rejected" };
  if (outcome.kind === "failed" && CONFLICT.test(outcome.message))
    return { status: "held", reason: "already-sent-elsewhere" };
  if (observation === null)
    return outcome.kind === "dispatched"
      ? { status: "accepted", delivery: "queued" }
      : { status: "unknown", reason: "readback-unavailable" };
  // Readback found nothing for these IDs right after the dispatch: the effect is not proven either way.
  return {
    status: "unknown",
    reason: outcome.kind === "dispatched" ? "readback-mismatch" : "dispatch-failed",
  };
}

/**
 * Readback for an explicit reconcile request. An authoritative `not_found` is held without a
 * destination, so the gallery may send the same attempt (same IDs) again.
 */
export function classifyDesignRequestObservation(
  observation: DesignRequestObservation,
): DesignRequestClassification {
  if (observation.commandStatus === "accepted")
    return { status: "accepted", delivery: designRequestDelivery(observation) };
  if (observation.commandStatus === "rejected") return { status: "rejected", reason: "rejected" };
  return { status: "held", reason: "not-found" };
}
