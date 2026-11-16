import { ThreadId, type ScheduledTask } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as ThreadManagementService from "../../orchestration-v2/ThreadManagementService.ts";

export const queuedWakeSkipReason =
  "Skipped scheduled wake: this task already has a queued run in the target thread.";

// Called inside the scheduler's per-task reservation; only queued intent suppresses
// another timer fire. Running or waiting provider work still permits one wake.
export const hasQueuedWake = Effect.fn("JonesScheduledTasks.hasQueuedWake")(function* (
  task: ScheduledTask,
) {
  if (task.threadId === null) return false;
  const management = yield* ThreadManagementService.ThreadManagementService;
  const threadId = ThreadId.make(task.threadId);
  const { runs } = yield* management.getThreadRecords(threadId, ["runs"]);
  const messageIds = runs.filter((run) => run.status === "queued").map((run) => run.userMessageId);
  if (messageIds.length === 0) return false;
  const { messages } = yield* management.getThreadRecords(threadId, ["messages"], { messageIds });
  return messages.some((message) => message.scheduledTaskId === task.id);
});
