import { ThreadGoalInput, ThreadGoalResult, OrchestratorMcpFailure } from "@t3tools/contracts";
import { Tool } from "effect/ai";
import * as Effect from "effect/Effect";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import * as McpToolAccess from "../../mcp/McpToolAccess.ts";
import * as GoalControl from "./GoalControl.ts";

export const ThreadGoalTool = Tool.make("t3_thread_goal", {
  description:
    "Read or control a thread's provider-native goal. Read first, then supply its controlToken and a stable clientRequestId for pause, resume, clear or replace (objective required). Codex pause/clear finish the current turn without interrupting it. Replace edits only the objective in place, preserving status and usage without starting a turn; idle resume accepts exactly one owned run. A self-caller in its own active run may arm a paused or blocked goal for session-local resume at idle; busy/resume_armed_session_local_until_idle means armed, never active. The intent is lost on process restart or session retirement; controls bypassing GoalControl that leave state unchanged are undetectable. Cross-thread busy behavior is unchanged, and a thread that never becomes idle never resumes. Existing paused threads are untouched. Accepted means recorded dispatch, not native activation; only a fresh same-binding read showing active proves activation. A stale token requires a fresh read; unknown requires reconciliation, never a blind retry or a new key. Claude reads are transcript projections and mutations are unsupported. Other providers report unsupported. Omit threadId for this thread.",
  parameters: ThreadGoalInput,
  success: ThreadGoalResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies: [Invocation.McpInvocationContext, GoalControl.GoalControl],
})
  .annotate(Tool.Title, "Control a native thread goal")
  .annotate(Tool.Destructive, true)
  .annotate(Tool.Idempotent, true);

// Access is action-dependent; the service rechecks both caller and target before each native effect.
export const threadGoalHandler = McpToolAccess.reads((input: ThreadGoalInput) =>
  Effect.gen(function* () {
    const service = yield* GoalControl.GoalControl;
    return yield* service.execute(yield* Invocation.McpInvocationContext, input);
  }),
);
