import * as CheckoutIntent from "../ordinaryCheckout/CheckoutIntent.ts";
import {
  OrchestratorMcpFailure,
  type OrchestrationV2ServerCommand,
  type ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlError from "effect/sql/SqlError";
import * as McpSessions from "../../mcp/McpSessionRegistry.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import * as ProviderSessions from "../../orchestration-v2/ProviderSessionManager.ts";
import * as Executor from "../../orchestration-v2/ThreadCommandExecutor.ts";
import { followUpOwnerRun, isActiveRun } from "../../orchestration-v2/CommandPolicy.ts";
import type { FollowUpCommandOwner } from "../remoteDelegation/FollowUp.ts";

const ParentTransaction = Context.Reference<ThreadId | undefined>(
  "jones/orchestration/FollowUpParentTransaction",
  { defaultValue: () => undefined },
);
type ReservationCommand = Extract<
  OrchestrationV2ServerCommand,
  { readonly type: "delegated_task.remote.request" }
>;
const AuthorizedReservation = Context.Reference<ReservationCommand | undefined>(
  "jones/orchestration/AuthorizedFollowUpReservation",
  { defaultValue: () => undefined },
);

export const isAuthorizedReservation = (command: ReservationCommand) =>
  Effect.map(AuthorizedReservation, (reservation) => reservation === command);

const denied = () =>
  new OrchestratorMcpFailure({
    code: "capability_denied",
    message: "The original authenticated parent no longer owns this follow-up transaction.",
  });
const failed = () =>
  new OrchestratorMcpFailure({
    code: "orchestration_error",
    message: "The follow-up transaction could not commit; delivery remains held.",
  });
// The provider batch may fail with either EventSink write or stream evidence;
// neither proves the parent transaction committed, so both keep delivery held.
const isEventSinkFailure = Schema.is(EventSink.EventSinkV2Error);
const attachmentDenied = () =>
  new OrchestratorMcpFailure({
    code: "capability_denied",
    message:
      "The original MCP caller is not attached to this parent run's native session; the follow-up remains held.",
  });

/** The Orchestrator supplies its private dispatch, which already assumes the lock is held. */
export const make = Effect.fn("CoreFollowUpCommandOwner.make")(function* <E>(
  dispatchUnderParentLock: (command: OrchestrationV2ServerCommand) => Effect.Effect<
    {
      readonly storedEvents: ReadonlyArray<{
        readonly event: { readonly type: string; readonly payload: unknown };
      }>;
    },
    E
  >,
  threadFor: (command: OrchestrationV2ServerCommand) => ThreadId,
) {
  const executor = yield* Executor.ThreadCommandExecutor;
  const sink = yield* EventSink.EventSinkV2;
  const sql = yield* SqlClient.SqlClient;
  const projections = yield* Projections.ProjectionStoreV2;
  const sessions = yield* Effect.serviceOption(McpSessions.McpSessionRegistry);
  const providerSessions = yield* Effect.serviceOption(ProviderSessions.ProviderSessionManagerV2);
  const ordinary = yield* Effect.serviceOption(CheckoutIntent.CheckoutIntent);
  const assertParentCaller = Effect.fn("CoreFollowUpCommandOwner.assertParentCaller")(function* (
    captured: Parameters<FollowUpCommandOwner["Service"]["assertParentCaller"]>[0],
    runId: Parameters<FollowUpCommandOwner["Service"]["assertParentCaller"]>[1],
  ) {
    if ((yield* ParentTransaction) !== captured.thread.threadId || Option.isNone(sessions))
      return yield* denied();
    const current = yield* sessions.value.lookupByCredentialId(captured.requestNamespace);
    if (
      current === undefined ||
      !current.capabilities.has("orchestration") ||
      current.environmentId !== captured.environmentId ||
      current.requestNamespace !== captured.requestNamespace ||
      current.thread.threadId !== captured.thread.threadId ||
      current.thread.providerSessionId !== captured.thread.providerSessionId ||
      current.thread.providerInstanceId !== captured.thread.providerInstanceId
    )
      return yield* denied();
    const parent = yield* projections
      .getThreadRecords(captured.thread.threadId, ["runs", "providerThreads"])
      .pipe(Effect.mapError(failed));
    const run = parent.runs.find((run) => run.id === runId);
    if (
      parent.thread.deletedAt !== null ||
      parent.thread.archivedAt !== null ||
      run === undefined ||
      followUpOwnerRun(parent.runs)?.id !== runId ||
      run.providerInstanceId !== captured.thread.providerInstanceId
    )
      return yield* denied();
    // Registry credentials authenticate the origin, but may outlive detachment.
    // Derive the native session only from this run's exact retained active binding.
    const native = parent.providerThreads.find(
      (provider) => provider.id === parent.thread.activeProviderThreadId,
    );
    if (
      native === undefined ||
      native.providerSessionId === null ||
      native.appThreadId !== parent.thread.id ||
      run.threadId !== parent.thread.id ||
      run.providerThreadId !== native.id ||
      native.providerInstanceId !== captured.thread.providerInstanceId ||
      Option.isNone(providerSessions) ||
      typeof providerSessions.value.isMcpCallerAttached !== "function"
    )
      return yield* attachmentDenied();
    if (
      (!isActiveRun(run) &&
        !native.pendingBackgroundTasks?.some((task) => task.kind === "subagent")) ||
      (yield* projections
        .hasRunInterruptRequest(parent.thread.id, run.id)
        .pipe(Effect.mapError(failed)))
    )
      return yield* denied();
    // Revalidate before reservation and again before beginSend in its owning transaction.
    // Do not cache the proof: credentials can be detached between those transactions.
    if (
      !(yield* providerSessions.value.isMcpCallerAttached({
        threadId: parent.thread.id,
        providerSessionId: native.providerSessionId,
        providerInstanceId: captured.thread.providerInstanceId,
        mcpCredentialId: captured.thread.providerSessionId,
      }))
    )
      return yield* attachmentDenied();
  });
  const owner: FollowUpCommandOwner["Service"] = {
    inParentTransaction: Effect.fn("CoreFollowUpCommandOwner.inParentTransaction")(function* <
      A,
      E,
      R,
    >(
      threadId: ThreadId,
      effect: Effect.Effect<A, E, R>,
    ): Effect.fn.Return<A, E | OrchestratorMcpFailure, R> {
      if ((yield* ParentTransaction) !== undefined) return yield* denied();
      // The callback may write before constructing its command. Hold it until
      // full-command preparation can run outside SQL and commit atomically with
      // reservation/claim writes; retain the batch for after-commit publication.
      if (Option.isSome(ordinary)) return yield* failed();
      // EventSink defers events and worker notifications until this outer transaction commits.
      return yield* executor
        .withLock(
          threadId,
          sink.withProviderBatch(
            sql.withTransaction(effect).pipe(Effect.provideService(ParentTransaction, threadId)),
          ),
        )
        .pipe(
          Effect.mapError((cause) =>
            isEventSinkFailure(cause) || SqlError.isSqlError(cause) ? failed() : cause,
          ),
        );
    }),
    assertParentCaller,
    dispatch: Effect.fn("CoreFollowUpCommandOwner.dispatch")(function* (
      command: OrchestrationV2ServerCommand,
      reservationCaller?: Parameters<FollowUpCommandOwner["Service"]["dispatch"]>[1],
    ) {
      if ((yield* ParentTransaction) !== threadFor(command)) return yield* denied();
      const dispatch = dispatchUnderParentLock(command).pipe(Effect.mapError(failed));
      if (reservationCaller === undefined) return yield* dispatch;
      if (
        command.type !== "delegated_task.remote.request" ||
        command.parentThreadId !== reservationCaller.thread.threadId ||
        command.remoteChild !== undefined
      )
        return yield* denied();
      yield* assertParentCaller(reservationCaller, command.parentRunId);
      return yield* dispatch.pipe(Effect.provideService(AuthorizedReservation, command));
    }),
  };
  return owner;
});
