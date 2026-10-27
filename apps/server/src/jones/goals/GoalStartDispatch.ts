import {
  CommandId,
  MessageId,
  type RunId,
  type ThreadId,
  type ThreadTurnDispatchGuard,
  type OrchestrationV2AppThread,
  type OrchestratorMcpFailure,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { makeCommandObservationQuery } from "../../orchestration-v2/CommandObservation.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";
import { unavailable } from "../../mcp/threadAccess.ts";

interface StartObservation {
  readonly outcome: "accepted" | "busy" | "unknown";
  readonly runId?: RunId;
  readonly reason: string;
}

export class GoalStartDispatch extends Context.Service<
  GoalStartDispatch,
  {
    readonly observe: (threadId: ThreadId, requestId: string) => Effect.Effect<StartObservation>;
    readonly capture: (
      threadId: ThreadId,
    ) => Effect.Effect<ThreadTurnDispatchGuard | null, OrchestratorMcpFailure>;
    readonly start: (input: {
      readonly thread: OrchestrationV2AppThread;
      readonly guard: ThreadTurnDispatchGuard;
      readonly requestId: string;
      readonly senderThreadId?: ThreadId;
      readonly text: string;
    }) => Effect.Effect<StartObservation, OrchestratorMcpFailure>;
  }
>()("t3/jones/goals/GoalStartDispatch") {}

const make = Effect.gen(function* () {
  const query = yield* makeCommandObservationQuery();
  const threads = yield* ThreadManagement.ThreadManagementService;
  const observe = Effect.fn("GoalStartDispatch.observe")(function* (
    threadId: ThreadId,
    requestId: string,
  ) {
    const readback = yield* query
      .observe({
        threadId,
        commandId: CommandId.make(`goal:${requestId}`),
        messageId: MessageId.make(`goal:${requestId}`),
      })
      .pipe(Effect.result);
    if (
      readback._tag === "Success" &&
      readback.success.commandStatus === "accepted" &&
      (readback.success.correlation === "exact" || readback.success.correlation === "pending")
    ) {
      const records = yield* threads.getThreadRecords(threadId, ["runs"]).pipe(Effect.result);
      const runs =
        records._tag === "Success"
          ? records.success.runs.filter(
              (run) => run.userMessageId === MessageId.make(`goal:${requestId}`),
            )
          : [];
      if (runs.length === 1)
        return {
          outcome: "accepted" as const,
          runId: runs[0]!.id,
          reason: "durable_start_accepted_native_state_pending",
        };
    }
    return readback._tag === "Success" && readback.success.commandStatus === "rejected"
      ? { outcome: "busy" as const, reason: "dispatch_rejected" }
      : { outcome: "unknown" as const, reason: "dispatch_readback_unresolved" };
  });
  return GoalStartDispatch.of({
    observe,
    capture: (threadId) =>
      query.getTarget(threadId).pipe(
        Effect.map(({ snapshotSequence, target }) =>
          target?.idle
            ? {
                observedSnapshotSequence: snapshotSequence,
                expectedModelSelection: target.modelSelection,
                expectedSessionStatus: target.sessionStatus,
                expectedActiveTurnId: target.activeTurnId,
                expectedLatestTurnId: target.latestTurnId,
                requireIdle: true as const,
              }
            : null,
        ),
        Effect.mapError(unavailable),
      ),
    start: Effect.fn("GoalStartDispatch.start")(function* (input) {
      const commandId = CommandId.make(`goal:${input.requestId}`);
      const messageId = MessageId.make(`goal:${input.requestId}`);
      yield* threads
        .dispatch({
          type: "message.dispatch",
          createdBy: "agent",
          creationSource: "mcp",
          commandId,
          messageId,
          threadId: input.thread.id,
          ...(input.senderThreadId === undefined ? {} : { senderThreadId: input.senderThreadId }),
          text: input.text,
          attachments: [],
          modelSelection: input.thread.modelSelection,
          dispatchMode: { type: "start_immediately" },
          dispatchGuard: input.guard,
        })
        .pipe(Effect.result);
      return yield* observe(input.thread.id, input.requestId);
    }),
  });
});
export const layer = Layer.effect(GoalStartDispatch, make).pipe(
  Layer.provide(ProjectionStore.layer),
);
