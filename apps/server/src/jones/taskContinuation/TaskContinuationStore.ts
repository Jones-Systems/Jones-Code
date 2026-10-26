import {
  CommandId,
  MessageId,
  ModelSelection,
  NodeId,
  ProviderInstanceId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ThreadId,
  OrchestrationV2RunAttemptJson,
  OrchestrationV2ProviderThreadJson,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { modelSelectionsEqual } from "@t3tools/shared/model";
import { RecordedRunJson, RecordedAppThreadJson } from "../../orchestration-v2/RecordedTypes.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
import {
  assertCurrentBinding,
  continuationRoot,
  providerMatchesBinding,
  TaskContinuationError,
  type SourceBinding,
} from "./sourceBinding.ts";

const Record = Schema.Struct({
  messageId: MessageId,
  commandId: CommandId,
  runId: RunId,
  parentThreadId: ThreadId,
  childThreadId: ThreadId,
  taskId: NodeId,
  sourceTaskId: NodeId,
  sourceRunId: RunId,
  sourceAttemptId: RunAttemptId,
  sourceEvidence: Schema.Literal("attempt"),
  providerInstanceId: ProviderInstanceId,
  providerThreadId: ProviderThreadId,
  nativeSessionId: Schema.String,
  modelSelection: Schema.fromJsonString(ModelSelection),
  requestDigest: Schema.String,
  acceptedAt: Schema.String,
});
export type TaskContinuationRecord = Omit<typeof Record.Type, "modelSelection"> & SourceBinding;
interface Witness {
  readonly attemptId: RunAttemptId;
  readonly runId: RunId;
  readonly nativeSessionId: string;
  readonly runtimeGeneration: string;
}
export class TaskContinuationStore extends Context.Service<
  TaskContinuationStore,
  {
    readonly get: (
      messageId: MessageId,
    ) => Effect.Effect<TaskContinuationRecord | null, TaskContinuationError>;
    /** Called only by the EventSink owner inside its acceptance transaction. */
    readonly insert: (row: TaskContinuationRecord) => Effect.Effect<void, TaskContinuationError>;
    readonly confirm: (
      row: TaskContinuationRecord,
      input: Witness,
    ) => Effect.Effect<void, TaskContinuationError>;
    readonly witnesses: (
      row: TaskContinuationRecord,
    ) => Effect.Effect<ReadonlyArray<Witness>, TaskContinuationError>;
  }
>()("t3/jones/taskContinuation/TaskContinuationStore") {}

const encodeSelection = Schema.encodeSync(Schema.fromJsonString(ModelSelection));
const decodeRecord = Schema.decodeUnknownEffect(Record);
const decodeThread = Schema.decodeUnknownEffect(Schema.fromJsonString(RecordedAppThreadJson));
const decodeRun = Schema.decodeUnknownEffect(Schema.fromJsonString(RecordedRunJson));
const decodeAttempt = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2RunAttemptJson),
);
const decodeProvider = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2ProviderThreadJson),
);
const storageError = (cause: unknown) =>
  new TaskContinuationError({ reason: "storage_unavailable", cause });
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const present = makeJonesTablePresence(sql);
  const get = Effect.fn("TaskContinuationStore.get")(function* (messageId: MessageId) {
    if (!(yield* present("jones_task_continuations"))) return null;
    const rows =
      yield* sql`SELECT message_id AS messageId, command_id AS commandId, run_id AS runId,
      parent_thread_id AS parentThreadId, child_thread_id AS childThreadId, task_id AS taskId,
      source_task_id AS sourceTaskId, source_run_id AS sourceRunId, source_attempt_id AS sourceAttemptId,
      source_evidence AS sourceEvidence, provider_instance_id AS providerInstanceId,
      provider_thread_id AS providerThreadId, native_session_id AS nativeSessionId,
      model_selection_json AS modelSelection, request_digest AS requestDigest, accepted_at AS acceptedAt
      FROM jones_task_continuations WHERE message_id = ${messageId}`;
    return rows[0] === undefined ? null : yield* decodeRecord(rows[0]);
  }, Effect.mapError(storageError));
  const insert = Effect.fn("TaskContinuationStore.insert")(function* (row: TaskContinuationRecord) {
    yield* sql`INSERT INTO jones_task_continuations
      (message_id, command_id, run_id, parent_thread_id, child_thread_id, task_id,
       source_task_id, source_run_id, source_attempt_id, source_evidence,
       provider_instance_id, provider_thread_id, native_session_id, model_selection_json,
       request_digest, accepted_at)
      VALUES (${row.messageId}, ${row.commandId}, ${row.runId}, ${row.parentThreadId},
       ${row.childThreadId}, ${row.taskId}, ${row.sourceTaskId}, ${row.sourceRunId},
       ${row.sourceAttemptId}, ${row.sourceEvidence}, ${row.providerInstanceId},
       ${row.providerThreadId}, ${row.nativeSessionId}, ${encodeSelection(row.modelSelection)},
       ${row.requestDigest}, ${row.acceptedAt})`;
  }, Effect.mapError(storageError));
  const witnesses = Effect.fn("TaskContinuationStore.witnesses")(function* (
    row: TaskContinuationRecord,
  ) {
    const rows = yield* sql<{
      attempt_id: string;
      run_id: string;
      native_session_id: string;
      runtime_generation: string;
    }>`SELECT attempt_id, run_id, native_session_id, runtime_generation
      FROM jones_task_continuation_witnesses WHERE root_run_id = ${row.runId}
      AND message_id = ${row.messageId} AND command_id = ${row.commandId}
      AND child_thread_id = ${row.childThreadId} AND provider_instance_id = ${row.providerInstanceId}
      AND provider_thread_id = ${row.providerThreadId} AND native_session_id = ${row.nativeSessionId}`;
    return rows.map((witness): Witness => ({
      attemptId: RunAttemptId.make(witness.attempt_id),
      runId: RunId.make(witness.run_id),
      nativeSessionId: witness.native_session_id,
      runtimeGeneration: witness.runtime_generation,
    }));
  }, Effect.mapError(storageError));
  const confirm = Effect.fn("TaskContinuationStore.confirm")(
    function* (row: TaskContinuationRecord, input: Witness) {
      yield* sql.withTransaction(
        Effect.gen(function* () {
          const durable = yield* get(row.messageId);
          if (
            durable === null ||
            durable.runId !== row.runId ||
            durable.commandId !== row.commandId ||
            durable.requestDigest !== row.requestDigest ||
            input.nativeSessionId !== row.nativeSessionId ||
            input.runtimeGeneration.length === 0
          )
            return yield* new TaskContinuationError({ reason: "witness_conflict" });
          const threads = yield* sql<{ payload_json: string }>`SELECT payload_json
        FROM orchestration_v2_projection_threads WHERE thread_id = ${row.childThreadId}`;
          const runs = yield* sql<{ payload_json: string }>`SELECT payload_json
        FROM orchestration_v2_projection_runs WHERE thread_id = ${row.childThreadId} AND run_id = ${input.runId}`;
          const attempts = yield* sql<{ payload_json: string }>`SELECT payload_json
        FROM orchestration_v2_projection_run_attempts WHERE thread_id = ${row.childThreadId} AND attempt_id = ${input.attemptId}`;
          const providers = yield* sql<{ payload_json: string }>`SELECT payload_json
        FROM orchestration_v2_projection_provider_threads WHERE thread_id = ${row.childThreadId} AND provider_thread_id = ${row.providerThreadId}`;
          if (!threads[0] || !runs[0] || !attempts[0] || !providers[0])
            return yield* new TaskContinuationError({ reason: "witness_conflict" });
          const thread = yield* decodeThread(threads[0].payload_json);
          const run = yield* decodeRun(runs[0].payload_json);
          const attempt = yield* decodeAttempt(attempts[0].payload_json);
          const provider = yield* decodeProvider(providers[0].payload_json);
          const lineage = [run];
          let ancestor = run;
          for (
            let depth = 0;
            ancestor.restartContinuationOfRunId !== undefined && depth < 64;
            depth++
          ) {
            const ancestors = yield* sql<{ payload_json: string }>`SELECT payload_json
          FROM orchestration_v2_projection_runs WHERE thread_id = ${row.childThreadId}
          AND run_id = ${ancestor.restartContinuationOfRunId}`;
            if (!ancestors[0])
              return yield* new TaskContinuationError({ reason: "ancestry_invalid" });
            ancestor = yield* decodeRun(ancestors[0].payload_json);
            if (lineage.some((seen) => seen.id === ancestor.id))
              return yield* new TaskContinuationError({ reason: "ancestry_invalid" });
            lineage.push(ancestor);
          }
          const root = continuationRoot(lineage, run);
          if (
            Schema.is(TaskContinuationError)(root) ||
            root.id !== row.runId ||
            root.userMessageId !== row.messageId ||
            assertCurrentBinding(row, { thread, providerThreads: [provider] }) !== undefined
          )
            return yield* new TaskContinuationError({ reason: "witness_conflict" });
          if (
            run.status !== "running" ||
            run.activeAttemptId !== input.attemptId ||
            run.delegatedTaskId !== row.taskId ||
            run.providerInstanceId !== row.providerInstanceId ||
            run.providerThreadId !== row.providerThreadId ||
            !modelSelectionsEqual(run.modelSelection, row.modelSelection) ||
            attempt.runId !== run.id ||
            attempt.status !== "running" ||
            attempt.providerInstanceId !== row.providerInstanceId ||
            attempt.providerThreadId !== row.providerThreadId ||
            attempt.nativeThreadId !== row.nativeSessionId ||
            !providerMatchesBinding(row, provider, row.childThreadId) ||
            (provider.runtimeIdentity?.runtimeGeneration !== undefined &&
              provider.runtimeIdentity.runtimeGeneration !== input.runtimeGeneration)
          )
            return yield* new TaskContinuationError({ reason: "witness_conflict" });
          const existing = yield* sql<{
            message_id: string;
            command_id: string;
            root_run_id: string;
            run_id: string;
            native_session_id: string;
            runtime_generation: string;
          }>`SELECT *
        FROM jones_task_continuation_witnesses WHERE attempt_id = ${input.attemptId}`;
          if (existing[0]) {
            const old = existing[0];
            if (
              old.message_id !== row.messageId ||
              old.command_id !== row.commandId ||
              old.root_run_id !== row.runId ||
              old.run_id !== input.runId ||
              old.native_session_id !== input.nativeSessionId ||
              old.runtime_generation !== input.runtimeGeneration
            )
              return yield* new TaskContinuationError({ reason: "witness_conflict" });
            return;
          }
          const now = DateTime.formatIso(yield* DateTime.now);
          yield* sql`INSERT INTO jones_task_continuation_witnesses
        (attempt_id,message_id,command_id,root_run_id,run_id,child_thread_id,
         provider_instance_id,provider_thread_id,native_session_id,runtime_generation,confirmed_at)
        VALUES (${input.attemptId},${row.messageId},${row.commandId},${row.runId},${input.runId},
         ${row.childThreadId},${row.providerInstanceId},${row.providerThreadId},${input.nativeSessionId},
         ${input.runtimeGeneration},${now})`;
        }),
      );
    },
    Effect.mapError((cause) =>
      Schema.is(TaskContinuationError)(cause) ? cause : storageError(cause),
    ),
  );
  return TaskContinuationStore.of({ get, insert, confirm, witnesses });
});
export const layer = Layer.effect(TaskContinuationStore, make);
