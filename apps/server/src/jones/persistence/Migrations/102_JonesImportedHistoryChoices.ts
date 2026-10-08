import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_imported_history_choices (
    command_id TEXT PRIMARY KEY NOT NULL, thread_id TEXT NOT NULL,
    actor_session_id TEXT NOT NULL, actor_digest TEXT NOT NULL CHECK(length(actor_digest)=64),
    command_digest TEXT NOT NULL CHECK(length(command_digest)=64),
    delivery_digest TEXT NOT NULL CHECK(length(delivery_digest)=64),
    reviewed_basis TEXT NOT NULL CHECK(length(reviewed_basis)=64),
    command_json TEXT NOT NULL CHECK(json_valid(command_json)),
    basis_json TEXT NOT NULL CHECK(json_valid(basis_json))
  )`;
  yield* sql`CREATE TABLE jones_imported_history_outcomes (
    command_id TEXT PRIMARY KEY NOT NULL REFERENCES jones_imported_history_choices(command_id),
    receipt_command_id TEXT UNIQUE NOT NULL REFERENCES orchestration_command_receipts(command_id),
    outcome_json TEXT NOT NULL CHECK(json_valid(outcome_json))
  )`;
  yield* sql`CREATE TABLE jones_imported_history_start_reservations (
    effect_id TEXT PRIMARY KEY NOT NULL REFERENCES orchestration_v2_effect_outbox(effect_id),
    command_id TEXT NOT NULL REFERENCES jones_imported_history_choices(command_id),
    thread_id TEXT NOT NULL, run_id TEXT UNIQUE NOT NULL, run_attempt_id TEXT NOT NULL,
    provider_thread_id TEXT NOT NULL, source_digest TEXT NOT NULL CHECK(length(source_digest)=64)
  )`;
  yield* sql`CREATE TABLE jones_imported_history_execution_starts (
    effect_id TEXT PRIMARY KEY NOT NULL REFERENCES jones_imported_history_start_reservations(effect_id),
    captured_json TEXT NOT NULL CHECK(json_valid(captured_json))
  )`;
  for (const table of [
    "jones_imported_history_choices",
    "jones_imported_history_outcomes",
    "jones_imported_history_start_reservations",
    "jones_imported_history_execution_starts",
  ]) {
    const key =
      table === "jones_imported_history_choices" || table === "jones_imported_history_outcomes"
        ? "command_id"
        : "effect_id";
    yield* sql`CREATE TRIGGER ${sql(`${table}_no_update`)} BEFORE UPDATE ON ${sql(table)} BEGIN SELECT RAISE(ABORT, 'imported choice is immutable'); END`;
    yield* sql`CREATE TRIGGER ${sql(`${table}_no_delete`)} BEFORE DELETE ON ${sql(table)} BEGIN SELECT RAISE(ABORT, 'imported choice is permanent'); END`;
    yield* sql`CREATE TRIGGER ${sql(`${table}_no_replace`)} BEFORE INSERT ON ${sql(table)} WHEN EXISTS (SELECT 1 FROM ${sql(table)} WHERE ${sql(key)} = NEW.${sql(key)}) BEGIN SELECT RAISE(ABORT, 'imported choice cannot be replaced'); END`;
  }
});
