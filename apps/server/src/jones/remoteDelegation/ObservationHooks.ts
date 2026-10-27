import {
  CommandId,
  MessageId,
  type NodeId,
  OrchestratorMcpFailure,
  type OrchestrationV2ServerCommand,
  type OrchestrationV2Subagent,
  RemoteBlockingQuestions,
  RemoteTaskObservation,
  type ThreadId,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ThreadCommandExecutor from "../../orchestration-v2/ThreadCommandExecutor.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";

type Questions = RemoteBlockingQuestions["requests"];
interface Row {
  readonly observation_json: string;
  readonly notices_json: string;
  readonly signal_pending: number;
}
export interface ObservationInput {
  readonly parentThreadId: ThreadId;
  readonly taskId: NodeId;
  readonly observation: RemoteTaskObservation;
}
export interface ObservationPorts {
  readonly recoverRemoteTaskQuestions?: Effect.Effect<void, OrchestratorMcpFailure>;
  readonly observeRemoteTask?: (
    input: ObservationInput,
  ) => Effect.Effect<void, OrchestratorMcpFailure>;
  readonly readRemoteTaskObservation?: (
    parentThreadId: ThreadId,
    taskId: NodeId,
  ) => Effect.Effect<RemoteTaskObservation | undefined, OrchestratorMcpFailure>;
  readonly reconcileRemoteTaskQuestions?: (
    parentThreadId: ThreadId,
    taskId: NodeId,
  ) => Effect.Effect<void, OrchestratorMcpFailure>;
}
const decodeObservation = Schema.decodeUnknownEffect(Schema.fromJsonString(RemoteTaskObservation));
const decodeNotices = Schema.decodeUnknownEffect(
  Schema.fromJsonString(RemoteBlockingQuestions.fields.requests),
);
const encodeNotices = Schema.encodeEffect(
  Schema.fromJsonString(RemoteBlockingQuestions.fields.requests),
);
const isOpen = (task: OrchestrationV2Subagent) =>
  task.result === null &&
  task.completionDelivery?.state !== "disposed" &&
  !["completed", "failed", "cancelled", "interrupted"].includes(task.status);
const matches = (task: OrchestrationV2Subagent, observation: RemoteTaskObservation) =>
  task.remoteChild?.environmentId === observation.environmentId &&
  task.remoteChild.threadId === observation.threadId &&
  task.remoteChild.runId === observation.originalRunId;
const failed = (_cause: unknown) =>
  new OrchestratorMcpFailure({
    code: "orchestration_error",
    message: "Remote task observation could not be reconciled.",
  });

/** Uses the parent's command lock; dispatch skips reacquiring that same lock. */
export const make = Effect.fn("RemoteTaskObservation.make")(function* <E>(
  dispatch: (command: OrchestrationV2ServerCommand) => Effect.Effect<unknown, E>,
) {
  const sql = yield* SqlClient.SqlClient;
  const tables = makeJonesTablePresence(sql);
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const sink = yield* EventSink.EventSinkV2;
  const executor = yield* ThreadCommandExecutor.ThreadCommandExecutor;
  const ids = yield* IdAllocator.IdAllocatorV2;
  const readRow = Effect.fn("RemoteTaskObservation.readRow")(function* (
    parentThreadId: ThreadId,
    taskId: NodeId,
  ) {
    if (!(yield* tables("jones_remote_task_observations"))) return undefined;
    return (yield* sql<Row>`SELECT observation_json, notices_json, signal_pending FROM jones_remote_task_observations WHERE parent_thread_id = ${parentThreadId} AND task_id = ${taskId}`)[0];
  });
  const readTask = (parentThreadId: ThreadId, taskId: NodeId) =>
    projections.getThreadRecords(parentThreadId, ["subagents", "runs"]).pipe(
      Effect.map((parent) => ({
        parent,
        task: parent.subagents.find(
          (candidate) =>
            candidate.id === taskId &&
            candidate.origin === "app_owned" &&
            candidate.driver === "remote",
        ),
      })),
      Effect.catchTags({ ProjectionStoreThreadNotFoundError: () => Effect.void }),
    );
  const readRemoteTaskObservation = (parentThreadId: ThreadId, taskId: NodeId) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const task = (yield* readTask(parentThreadId, taskId))?.task;
          if (task === undefined || !isOpen(task)) return undefined;
          const row = yield* readRow(parentThreadId, taskId);
          if (row === undefined) return undefined;
          const observation = yield* decodeObservation(row.observation_json);
          return matches(task, observation) ? observation : undefined;
        }),
      )
      .pipe(Effect.mapError(failed));
  const observeRemoteTask = (input: ObservationInput) =>
    executor
      .withLock(
        input.parentThreadId,
        Effect.gen(function* () {
          const task = yield* sql.withTransaction(
            Effect.gen(function* () {
              const current = yield* readTask(input.parentThreadId, input.taskId);
              if (current === undefined) return undefined;
              const { parent, task } = current;
              if (
                task === undefined ||
                !isOpen(task) ||
                !matches(task, input.observation) ||
                parent.thread.deletedAt !== null ||
                parent.thread.archivedAt !== null
              )
                return undefined;
              if (!(yield* tables("jones_remote_task_observations")))
                return yield* failed(undefined);
              const previous = yield* readRow(input.parentThreadId, input.taskId);
              const json = yield* Schema.encodeEffect(Schema.fromJsonString(RemoteTaskObservation))(
                input.observation,
              );
              if (previous?.observation_json === json && previous.signal_pending === 0)
                return undefined;
              yield* sql`INSERT INTO jones_remote_task_observations (parent_thread_id, task_id, observation_json, signal_pending)
        VALUES (${input.parentThreadId}, ${input.taskId}, ${json}, 1)
        ON CONFLICT(parent_thread_id, task_id) DO UPDATE SET observation_json = excluded.observation_json, signal_pending = 1`;
              return task;
            }),
          );
          if (task === undefined) return;
          // Commit the sparse observation first. A crash before signaling retains signal_pending for retry.
          yield* sink.write({
            events: [
              {
                id: yield* ids.allocate.event({}),
                type: "subagent.updated",
                threadId: input.parentThreadId,
                ...(task.runId === null ? {} : { runId: task.runId }),
                nodeId: task.id,
                driver: task.driver,
                occurredAt: yield* DateTime.now,
                payload: task,
              },
            ],
          });
          yield* sql`UPDATE jones_remote_task_observations SET signal_pending = 0 WHERE parent_thread_id = ${input.parentThreadId} AND task_id = ${input.taskId}`;
        }),
      )
      .pipe(Effect.mapError(failed));

  const reconcileRemoteTaskQuestions = (parentThreadId: ThreadId, taskId: NodeId) =>
    executor
      .withLock(
        parentThreadId,
        Effect.gen(function* () {
          const current = yield* readTask(parentThreadId, taskId);
          if (current === undefined) return;
          const { parent, task } = current;
          const row = yield* readRow(parentThreadId, taskId);
          if (row === undefined || task === undefined) return;
          const observation = yield* decodeObservation(row.observation_json);
          if (!matches(task, observation)) return;
          const open =
            isOpen(task) && parent.thread.deletedAt === null && parent.thread.archivedAt === null;
          const questions = open ? observation.blockingQuestions.requests : [];
          const notices = [...(yield* decodeNotices(row.notices_json))];
          const keyFor = (request: Questions[number]) =>
            [
              parentThreadId,
              taskId,
              observation.environmentId,
              observation.threadId,
              request.requestId,
            ]
              .map(encodeURIComponent)
              .join(":");
          const saveNotices = () =>
            sql.withTransaction(
              Effect.gen(function* () {
                const current = yield* readTask(parentThreadId, taskId);
                if (current?.task === undefined) return;
                yield* sql`UPDATE jones_remote_task_observations SET notices_json = ${yield* encodeNotices(notices)} WHERE parent_thread_id = ${parentThreadId} AND task_id = ${taskId}`;
              }),
            );
          for (const notice of notices.slice()) {
            if (questions.some((request) => request.requestId === notice.requestId)) continue;
            const key = keyFor(notice);
            const queued = parent.runs.find(
              (run) =>
                run.status === "queued" && run.userMessageId === `message:remote-question:${key}`,
            );
            if (queued !== undefined)
              yield* dispatch({
                type: "queued-run.cancel",
                commandId: CommandId.make(`server:remote-question-cancel:${key}`),
                threadId: parentThreadId,
                runId: queued.id,
              });
            notices.splice(
              notices.findIndex((request) => request.requestId === notice.requestId),
              1,
            );
            yield* saveNotices();
          }
          if (
            !open ||
            (task.completionWake === "settled_only" &&
              parent.runs.some(
                (run) =>
                  run.id === task.runId &&
                  ["preparing", "starting", "running"].includes(run.status),
              ))
          )
            return;
          for (const request of questions) {
            const key = keyFor(request);
            // Record notice intent before dispatch; stable command IDs make crash recovery idempotent.
            if (!notices.some((notice) => notice.requestId === request.requestId)) {
              notices.push(request);
              yield* saveNotices();
            }
            yield* dispatch({
              type: "message.dispatch",
              commandId: CommandId.make(`server:remote-question:${key}`),
              threadId: parentThreadId,
              messageId: MessageId.make(`message:remote-question:${key}`),
              text: `Delegated task ${taskId} is blocked on a question in linked environment ${observation.environmentId}. Read it with t3_pending_request_read using environmentId ${observation.environmentId}, threadId ${observation.threadId}, requestId ${request.requestId}. Answer through t3_pending_request_respond with those same IDs when the answer follows from the task or the user's instructions; otherwise ask the user. If it is no longer pending, it was answered or dismissed.`,
              notification: {
                source: { kind: "delegated_task", taskIds: [taskId] },
                outcome: "updated",
                summary: "Remote delegated task has a question",
              },
              attachments: [],
              dispatchMode: { type: "queue_after_active" },
              createdBy: "agent",
              creationSource: "server",
            });
          }
        }),
      )
      .pipe(Effect.mapError(failed));
  const recoverRemoteTaskQuestions = Effect.gen(function* () {
    if (!(yield* tables("jones_remote_task_observations"))) return;
    const rows = yield* sql<{
      parent_thread_id: ThreadId;
      task_id: NodeId;
      observation_json: string;
      signal_pending: number;
    }>`SELECT parent_thread_id, task_id, observation_json, signal_pending FROM jones_remote_task_observations`;
    for (const row of rows) {
      if (row.signal_pending === 1)
        yield* observeRemoteTask({
          parentThreadId: row.parent_thread_id,
          taskId: row.task_id,
          observation: yield* decodeObservation(row.observation_json),
        });
      yield* reconcileRemoteTaskQuestions(row.parent_thread_id, row.task_id);
    }
  }).pipe(Effect.mapError(failed));
  return {
    observeRemoteTask,
    readRemoteTaskObservation,
    reconcileRemoteTaskQuestions,
    recoverRemoteTaskQuestions,
  } satisfies Required<ObservationPorts>;
});
