import type { OrchestrationV2Run, RunId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

/** Message IDs of server-made restart continuations (see `RestartContinuation.ts`). */
export const RESTART_CONTINUATION_MESSAGE_PREFIX = "message:restart-continuation:";

/**
 * Why a queued run was held. `stop` and `inherited` holds wait for the user; a `restart` hold
 * ends when the restart continuation completes. Runs held before reasons were recorded have no
 * recorded hold.
 */
export type QueueHoldReason = "stop" | "restart" | "provider_failure" | "inherited";
const QUEUE_HOLD_REASONS: ReadonlySet<string> = new Set([
  "stop",
  "restart",
  "provider_failure",
  "inherited",
]);
export const isQueueHoldReason = (value: string): value is QueueHoldReason =>
  QUEUE_HOLD_REASONS.has(value);

/** A recorded hold, kept in `jones_queue_holds` beside the upstream run. */
export interface QueueHold {
  readonly reason: QueueHoldReason;
  readonly heldAt: DateTime.Utc;
}

export function isCompletedRestartContinuation(run: OrchestrationV2Run): boolean {
  return (
    run.status === "completed" && run.userMessageId.startsWith(RESTART_CONTINUATION_MESSAGE_PREFIX)
  );
}

/**
 * Restart holds a completed continuation ends: those recorded no later than its completion. A
 * hold from a later restart waits for that restart's own continuation, and a held run without a
 * recorded restart hold stays held.
 */
export function restartHoldsEndedBy(
  continuation: OrchestrationV2Run,
  runs: ReadonlyArray<OrchestrationV2Run>,
  holds: ReadonlyMap<RunId, QueueHold>,
): ReadonlyArray<OrchestrationV2Run> {
  if (!isCompletedRestartContinuation(continuation) || continuation.completedAt === null) return [];
  const completedAt = continuation.completedAt;
  return runs.filter((run) => {
    const hold = holds.get(run.id);
    return (
      run.threadId === continuation.threadId &&
      run.status === "queued" &&
      run.queueHeld === true &&
      hold?.reason === "restart" &&
      DateTime.isLessThanOrEqualTo(hold.heldAt, completedAt)
    );
  });
}
