import { TaskContinueInput, TaskContinueResult, OrchestratorMcpFailure } from "@t3tools/contracts";
import { Tool } from "effect/ai";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Access from "../../mcp/McpToolAccess.ts";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import * as Continuation from "./TaskContinuation.ts";

export const TaskContinueTool = Tool.make("t3_task_continue", {
  description:
    "Continue a completed local app-owned Claude task in its original native conversation, or refuse. The child must be idle. Returns a new taskId on the same child; the original published result stays unchanged. Completed task_status/results expose nativeSession with the captured nativeSessionId and source run/attempt; pass that ID as expectedNativeSessionId for an explicit compare guard. Required clientRequestId is stable across retries and parent MCP restarts: reuse it with the identical body to read current continuation evidence. nativeSessionId is expected until verification becomes verified after durable provider confirmation. Account identity is unavailable. This async operation never starts a fresh conversation. Independent review rounds still use delegate_task.",
  parameters: TaskContinueInput,
  success: TaskContinueResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies: [Invocation.McpInvocationContext, Threads.ThreadManagementService],
})
  .annotate(Tool.Title, "Continue a Claude task conversation")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, true)
  .annotate(Tool.Idempotent, true);

export const taskContinueHandler = Access.actsAsCaller((input: TaskContinueInput) =>
  Effect.gen(function* () {
    const scope = yield* Invocation.McpInvocationContext;
    const service = yield* Effect.serviceOption(Continuation.TaskContinuation);
    if (Option.isNone(service))
      return yield* new OrchestratorMcpFailure({
        code: "orchestration_error",
        message: "[storage_unavailable] Task continuation is unavailable in this server embedding.",
      });
    return yield* service.value.continue(scope, input);
  }),
);
