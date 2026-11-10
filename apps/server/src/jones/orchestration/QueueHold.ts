import type { OrchestrationV2QueueHeldReason, OrchestrationV2Run } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

/** Message IDs of server-made restart continuations (see `RestartContinuation.ts`). */
export const RESTART_CONTINUATION_MESSAGE_PREFIX = "message:restart-continuation:";

/** Holds a queued run, keeping the time and reason of a hold it already has. */
export function holdQueuedRun(
  run: OrchestrationV2Run,
  reason: OrchestrationV2QueueHeldReason,
  at: DateTime.Utc,
): OrchestrationV2Run {
  if (run.queueHeld === true) return { ...run, queueHeld: true };
  return { ...run, queueHeld: true, queueHeldAt: at, queueHeldReason: reason };
}

/** Stop holds a queued run as stopped, replacing a restart hold that its continuation would end. */
export function stopQueuedRun(run: OrchestrationV2Run, at: DateTime.Utc): OrchestrationV2Run {
  const base = run.queueHeldReason === "restart" ? releaseQueuedRunHold(run) : run;
  return holdQueuedRun(base, "stop", at);
}

/** The hold fields a new queued run takes when it queues behind held work. */
export function inheritedQueueHold(at: DateTime.Utc) {
  return { queueHeld: true, queueHeldAt: at, queueHeldReason: "inherited" } as const;
}

/** Releases a hold and drops its time and reason. */
export function releaseQueuedRunHold(run: OrchestrationV2Run): OrchestrationV2Run {
  const { queueHeldAt: _at, queueHeldReason: _reason, ...released } = run;
  return { ...released, queueHeld: false };
}

export function isCompletedRestartContinuation(run: OrchestrationV2Run): boolean {
  return (
    run.status === "completed" && run.userMessageId.startsWith(RESTART_CONTINUATION_MESSAGE_PREFIX)
  );
}

/**
 * Restart holds a completed continuation ends: those recorded no later than its completion. A
 * hold from a later restart waits for that restart's own continuation.
 */
export function restartHoldsEndedBy(
  continuation: OrchestrationV2Run,
  runs: ReadonlyArray<OrchestrationV2Run>,
): ReadonlyArray<OrchestrationV2Run> {
  if (!isCompletedRestartContinuation(continuation) || continuation.completedAt === null) return [];
  const completedAt = continuation.completedAt;
  return runs.filter(
    (run) =>
      run.threadId === continuation.threadId &&
      run.status === "queued" &&
      run.queueHeld === true &&
      run.queueHeldReason === "restart" &&
      run.queueHeldAt !== undefined &&
      DateTime.isLessThanOrEqualTo(run.queueHeldAt, completedAt),
  );
}
