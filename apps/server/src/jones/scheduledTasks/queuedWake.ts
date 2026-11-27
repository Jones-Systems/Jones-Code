import type { ScheduledTask, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as ThreadManagementService from "../../orchestration-v2/ThreadManagementService.ts";

export const queuedWakeSkipReason =
  "Skipped scheduled wake: this task already has a queued run in the target thread.";

// Called inside the scheduler's per-task reservation; only queued intent suppresses
// another timer fire. Running or waiting provider work still permits one wake.
export const readQueuedWake = Effect.fn("JonesScheduledTasks.readQueuedWake")(function* (
  taskId: ScheduledTask["id"],
  threadId: ThreadId,
) {
  const management = yield* ThreadManagementService.ThreadManagementService;
  const { runs } = yield* management.getThreadRecords(threadId, ["runs"]);
  const queued = runs.filter((run) => run.status === "queued");
  if (queued.length === 0) return null;
  const { messages } = yield* management.getThreadRecords(threadId, ["messages"], {
    messageIds: [...new Set(queued.map((run) => run.userMessageId))],
  });
  const scheduled = new Set(
    messages.filter((message) => message.scheduledTaskId === taskId).map((message) => message.id),
  );
  const matching = queued.filter((run) => scheduled.has(run.userMessageId));
  return matching.some((run) => run.queueHeld === true)
    ? "held"
    : matching.length > 0
      ? "queued"
      : null;
});
