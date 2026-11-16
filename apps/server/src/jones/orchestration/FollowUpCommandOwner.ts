import { OrchestratorMcpFailure, type OrchestrationV2ServerCommand, type ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlError from "effect/sql/SqlError";
import * as McpSessions from "../../mcp/McpSessionRegistry.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import * as Executor from "../../orchestration-v2/ThreadCommandExecutor.ts";
import { followUpOwnerRun, isActiveRun } from "../../orchestration-v2/CommandPolicy.ts";
import type { FollowUpCommandOwner } from "../remoteDelegation/FollowUp.ts";

const ParentTransaction = Context.Reference<ThreadId | undefined>(
  "jones/orchestration/FollowUpParentTransaction", { defaultValue: () => undefined },
);
const denied = () => new OrchestratorMcpFailure({ code: "capability_denied",
  message: "The original authenticated parent no longer owns this follow-up transaction." });
const failed = () => new OrchestratorMcpFailure({ code: "orchestration_error",
  message: "The follow-up transaction could not commit; delivery remains held." });

/** The Orchestrator supplies its private dispatch, which already assumes the lock is held. */
export const make = Effect.fn("CoreFollowUpCommandOwner.make")(function* <E>(
  dispatchUnderParentLock: (command: OrchestrationV2ServerCommand) => Effect.Effect<{
    readonly storedEvents: ReadonlyArray<{ readonly event: { readonly type: string; readonly payload: unknown } }>;
  }, E>,
  threadFor: (command: OrchestrationV2ServerCommand) => ThreadId,
) {
  const executor = yield* Executor.ThreadCommandExecutor;
  const sink = yield* EventSink.EventSinkV2;
  const sql = yield* SqlClient.SqlClient;
  const projections = yield* Projections.ProjectionStoreV2;
  const sessions = yield* Effect.serviceOption(McpSessions.McpSessionRegistry);
  const owner: FollowUpCommandOwner["Service"] = {
    inParentTransaction: (threadId, effect) => Effect.gen(function* () {
      if ((yield* ParentTransaction) !== undefined) return yield* denied();
      // EventSink defers events and worker notifications until this outer transaction commits.
      return yield* executor.withLock(threadId, sink.withProviderBatch(
        sql.withTransaction(effect).pipe(Effect.provideService(ParentTransaction, threadId)),
      )).pipe(Effect.mapError((cause) => cause instanceof EventSink.EventSinkWriteError || SqlError.isSqlError(cause) ? failed() : cause));
    }),
    assertParentCaller: (captured, runId) => Effect.gen(function* () {
      if ((yield* ParentTransaction) !== captured.thread.threadId || Option.isNone(sessions))
        return yield* denied();
      const current = yield* sessions.value.lookupByCredentialId(captured.requestNamespace);
      if (current === undefined || !current.capabilities.has("orchestration") ||
        current.environmentId !== captured.environmentId || current.requestNamespace !== captured.requestNamespace ||
        current.thread.threadId !== captured.thread.threadId ||
        current.thread.providerSessionId !== captured.thread.providerSessionId ||
        current.thread.providerInstanceId !== captured.thread.providerInstanceId)
        return yield* denied();
      const parent = yield* projections.getThreadRecords(captured.thread.threadId, ["runs"]).pipe(Effect.mapError(failed));
      const run = parent.runs.find((run) => run.id === runId);
      if (parent.thread.deletedAt !== null || parent.thread.archivedAt !== null || run === undefined ||
        !isActiveRun(run) || followUpOwnerRun(parent.runs)?.id !== runId ||
        run.providerInstanceId !== captured.thread.providerInstanceId)
        return yield* denied();
    }),
    dispatch: (command) => Effect.gen(function* () {
      if ((yield* ParentTransaction) !== threadFor(command)) return yield* denied();
      return yield* dispatchUnderParentLock(command).pipe(Effect.mapError(failed));
    }),
  };
  return owner;
});
