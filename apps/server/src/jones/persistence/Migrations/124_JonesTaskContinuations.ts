import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_task_continuations (
    message_id TEXT PRIMARY KEY NOT NULL,
    command_id TEXT NOT NULL UNIQUE,
    run_id TEXT NOT NULL UNIQUE,
    parent_thread_id TEXT NOT NULL,
    child_thread_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    source_task_id TEXT NOT NULL,
    source_run_id TEXT NOT NULL,
    source_attempt_id TEXT NOT NULL,
    source_evidence TEXT NOT NULL CHECK(source_evidence = 'attempt'),
    provider_instance_id TEXT NOT NULL,
    provider_thread_id TEXT NOT NULL,
    native_session_id TEXT NOT NULL,
    model_selection_json TEXT NOT NULL CHECK(json_valid(model_selection_json)),
    request_digest TEXT NOT NULL,
    accepted_at TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE jones_task_continuation_witnesses (
    attempt_id TEXT PRIMARY KEY NOT NULL,
    message_id TEXT NOT NULL,
    command_id TEXT NOT NULL,
    root_run_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    child_thread_id TEXT NOT NULL,
    provider_instance_id TEXT NOT NULL,
    provider_thread_id TEXT NOT NULL,
    native_session_id TEXT NOT NULL,
    runtime_generation TEXT NOT NULL,
    confirmed_at TEXT NOT NULL
  )`;
  yield* sql`CREATE INDEX jones_task_continuation_witness_root ON jones_task_continuation_witnesses(root_run_id)`;
});
