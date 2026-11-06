import type {
  DelegatedTaskNativeSession,
  OrchestrationV2Subagent,
  OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import { sourceBinding, TaskContinuationError } from "./sourceBinding.ts";

export function nativeSessionProvenance(
  task: OrchestrationV2Subagent,
  child: Parameters<typeof sourceBinding>[1],
): DelegatedTaskNativeSession | undefined {
  const binding = sourceBinding(task, child);
  if (Schema.is(TaskContinuationError)(binding)) return undefined;
  return {
    nativeSessionId: binding.nativeSessionId,
    sourceRunId: binding.sourceRunId,
    sourceAttemptId: binding.sourceAttemptId,
    evidence: "attempt",
  };
}

export const readTaskNativeSession = Effect.fn("TaskContinuation.readTaskNativeSession")(function* (
  task: OrchestrationV2Subagent,
  child: Pick<OrchestrationV2ThreadProjection, "thread" | "runs" | "contextTransfers">,
) {
  if (
    task.origin !== "app_owned" ||
    task.driver !== "claudeAgent" ||
    task.childThreadId === null ||
    task.remoteChild !== undefined ||
    task.status !== "completed" ||
    task.result === null
  )
    return undefined;
  const threads = yield* Threads.ThreadManagementService;
  const evidence = yield* threads.getThreadRecords(task.childThreadId, [
    "attempts",
    "providerTurns",
  ]);
  return nativeSessionProvenance(task, { ...child, ...evidence });
});
