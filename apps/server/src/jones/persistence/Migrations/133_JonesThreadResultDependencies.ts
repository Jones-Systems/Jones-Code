import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

// This unreleased migration follows the freshly fetched main manifest through 132.
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_thread_result_dependencies (
    dependency_id TEXT PRIMARY KEY NOT NULL,
    waiter_thread_id TEXT NOT NULL,
    waiter_run_id TEXT,
    target_environment_id TEXT,
    target_thread_id TEXT NOT NULL,
    target_label TEXT NOT NULL,
    operation_id TEXT NOT NULL UNIQUE,
    forwarded_request_key TEXT,
    request_digest TEXT,
    origin_scope_json TEXT CHECK(origin_scope_json IS NULL OR json_valid(origin_scope_json)),
    request_id TEXT,
    message_id TEXT,
    state TEXT NOT NULL CHECK(state IN ('sending','awaiting','resolved','delivered','released')),
    generation INTEGER NOT NULL DEFAULT 0,
    outcome TEXT,
    result_text TEXT CHECK(result_text IS NULL OR length(result_text) <= 16000),
    observation_json TEXT NOT NULL CHECK(json_valid(observation_json)),
    wake_message_id TEXT,
    notified INTEGER NOT NULL DEFAULT 0,
    changed_sequence INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  )`;
  yield* sql`CREATE INDEX jones_thread_result_dependencies_waiter ON jones_thread_result_dependencies(waiter_thread_id, state)`;
  yield* sql`CREATE INDEX jones_thread_result_dependencies_request ON jones_thread_result_dependencies(request_id)`;
  yield* sql`CREATE INDEX jones_thread_result_dependencies_changes ON jones_thread_result_dependencies(changed_sequence)`;
  yield* sql`CREATE TABLE jones_thread_result_requests (
    request_id TEXT PRIMARY KEY NOT NULL,
    target_thread_id TEXT NOT NULL,
    message_id TEXT NOT NULL UNIQUE,
    command_id TEXT NOT NULL UNIQUE,
    origin_kind TEXT NOT NULL CHECK(origin_kind IN ('local','linked')),
    origin_environment_id TEXT NOT NULL,
    origin_thread_id TEXT NOT NULL,
    origin_label TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending','fulfilled','declined','dropped','withdrawn')),
    revision INTEGER NOT NULL DEFAULT 0,
    result_text TEXT CHECK(result_text IS NULL OR length(result_text) <= 16000),
    resolved_by_run_id TEXT,
    requester_released_at TEXT,
    changed_sequence INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(origin_environment_id, operation_id)
  )`;
  yield* sql`CREATE INDEX jones_thread_result_requests_target ON jones_thread_result_requests(target_thread_id, state)`;
  yield* sql`CREATE INDEX jones_thread_result_requests_changes ON jones_thread_result_requests(changed_sequence)`;
  yield* sql`CREATE TABLE jones_thread_result_fences (
    origin_environment_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    changed_sequence INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    generation INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(origin_environment_id, operation_id)
  )`;
  yield* sql`ALTER TABLE jones_remote_admissions ADD COLUMN origin_environment_id TEXT`;
  yield* sql`ALTER TABLE jones_remote_admissions ADD COLUMN operation_id TEXT`;
  yield* sql`CREATE UNIQUE INDEX jones_remote_admissions_result_operation ON jones_remote_admissions(origin_environment_id, operation_id) WHERE operation_id IS NOT NULL`;
});
