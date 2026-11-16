import * as JonesRemoteRuntime from "../jones/remoteDelegation/Runtime.ts";
import type { EnvironmentId, NodeId, OrchestratorMcpFailure, OrchestratorMcpDelegateTaskInput,
  OrchestratorMcpThreadSendInput, OrchestratorMcpThreadSendResult, OrchestratorMcpThreadRun,
  OrchestrationV2Subagent, OrchestratorMcpTaskCancelResult } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import type * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
export { FollowRetrySchedule } from "../jones/remoteDelegation/retrySchedule.ts";
type ThreadScope = McpInvocationContext.McpThreadInvocationScope;

export class RemoteDelegation extends Context.Service<
  RemoteDelegation,
  {
    readonly observe: (
      scope: ThreadScope,
      task: OrchestrationV2Subagent,
    ) => Effect.Effect<
      {
        readonly observation: import("@t3tools/contracts").RemoteTaskObservation;
        readonly latestTerminalSummary: string | null;
      },
      OrchestratorMcpFailure
    >;
    readonly delegate: (
      scope: ThreadScope,
      input: OrchestratorMcpDelegateTaskInput & {
        readonly target: NonNullable<OrchestratorMcpDelegateTaskInput["target"]> & {
          readonly environmentId: EnvironmentId;
        };
      },
    ) => Effect.Effect<{ readonly taskId: NodeId }, OrchestratorMcpFailure>;
    /**
     * A `t3_thread_send` there that started or queued a run on the thread of
     * one of this thread's tasks that has its result: like a follow-up to a
     * finished child here, that run is a new task, whose id the send returns.
     * Any other send is returned as it came.
     */
    readonly followUp: (
      scope: McpInvocationContext.McpInvocationScope,
      environmentId: EnvironmentId,
      request: Pick<OrchestratorMcpThreadSendInput, "message" | "clientRequestId">,
      sent: OrchestratorMcpThreadSendResult,
    ) => Effect.Effect<OrchestratorMcpThreadSendResult, OrchestratorMcpFailure>;
    /**
     * Stops the task's thread there, then completes it here as cancelled. A
     * task that already has its result keeps it; its thread there is still
     * stopped, as cancelling a local task stops follow-up work on its child.
     */
    readonly cancel: (
      scope: ThreadScope,
      task: OrchestrationV2Subagent,
      input: {
        readonly reason?: string | undefined;
        readonly clientRequestId?: string | undefined;
      },
    ) => Effect.Effect<OrchestratorMcpTaskCancelResult, OrchestratorMcpFailure>;
    /**
     * The status of the task's run there, or null when it cannot be read: an
     * unreachable machine reads as no news rather than failing the read here.
     */
    readonly runStatus: (
      scope: ThreadScope,
      task: OrchestrationV2Subagent,
    ) => Effect.Effect<OrchestratorMcpThreadRun["status"] | null>;
    /**
     * Follows every open remote task until it ends, including after a
     * restart, and a finished one again when a Stop here owes it a stop there.
     */
    readonly start: () => Effect.Effect<void, never, Scope.Scope>;
  }
>()("t3/peer/RemoteDelegation") {}

/** The other side refused because the thread there runs above this caller's modes. */
const isModeRefusal = (error: OrchestratorMcpFailure) =>
  error.code === "runtime_mode_escalation_denied" ||
  error.code === "interaction_mode_escalation_denied";

/**
 * Refusals that following again cannot change: the link was revoked, expired
 * or forgotten, or the thread or run there is gone. Anything else (the other
 * machine asleep, a dropped route) is worth retrying.
 */
const isFinal = (error: OrchestratorMcpFailure) =>
  error.code === "capability_denied" ||
  error.code === "thread_not_found" ||
  error.code === "run_not_found";

export const layer = Layer.effect(RemoteDelegation, JonesRemoteRuntime.make);
