import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // Only new Jones-owned schema is created. Existing foreign rows and schemas are never adopted.
  yield* sql`CREATE TABLE jones_native_creation_execution_acceptances (
    command_id TEXT PRIMARY KEY NOT NULL REFERENCES orchestration_command_receipts(command_id),
    claim_id TEXT NOT NULL REFERENCES native_creation_intents(claim_id),
    thread_id TEXT NOT NULL,
    command_digest TEXT NOT NULL CHECK(length(command_digest)=64),
    binding_digest TEXT NOT NULL CHECK(length(binding_digest)=64),
    event_id TEXT NOT NULL REFERENCES orchestration_events(event_id),
    event_sequence INTEGER NOT NULL CHECK(event_sequence>0)
  )`;
  yield* sql`CREATE TABLE jones_native_creation_execution_starts (
    effect_id TEXT PRIMARY KEY NOT NULL REFERENCES orchestration_v2_effect_outbox(effect_id),
    claim_id TEXT NOT NULL REFERENCES native_creation_intents(claim_id),
    command_id TEXT NOT NULL REFERENCES jones_native_creation_execution_acceptances(command_id),
    worker_id TEXT NOT NULL,
    expected_attempt INTEGER NOT NULL CHECK(expected_attempt>0),
    lease_expires_at TEXT NOT NULL,
    reference_json TEXT NOT NULL CHECK(json_valid(reference_json)),
    fact_json TEXT NOT NULL CHECK(json_valid(fact_json))
  )`;
  yield* sql`CREATE TABLE jones_native_creation_execution_confirmations (
    effect_id TEXT PRIMARY KEY NOT NULL REFERENCES jones_native_creation_execution_starts(effect_id),
    worker_id TEXT NOT NULL,
    expected_attempt INTEGER NOT NULL CHECK(expected_attempt>0),
    evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
    confirmed_at TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE jones_native_creation_execution_holds (
    effect_id TEXT PRIMARY KEY NOT NULL REFERENCES jones_native_creation_execution_starts(effect_id),
    reason TEXT NOT NULL,
    held_at TEXT NOT NULL
  )`;
  for (const table of [
    "jones_native_creation_execution_acceptances",
    "jones_native_creation_execution_starts",
    "jones_native_creation_execution_confirmations",
    "jones_native_creation_execution_holds",
  ]) {
    yield* sql.unsafe(
      `CREATE TRIGGER ${table}_no_update BEFORE UPDATE ON ${table} BEGIN SELECT RAISE(ABORT, 'native execution facts are immutable'); END`,
    );
    yield* sql.unsafe(
      `CREATE TRIGGER ${table}_no_delete BEFORE DELETE ON ${table} BEGIN SELECT RAISE(ABORT, 'native execution facts are permanent'); END`,
    );
    yield* sql.unsafe(
      `CREATE TRIGGER ${table}_no_replace BEFORE INSERT ON ${table} WHEN EXISTS (SELECT 1 FROM ${table} WHERE ${table === "jones_native_creation_execution_acceptances" ? "command_id=NEW.command_id" : "effect_id=NEW.effect_id"}) BEGIN SELECT RAISE(ABORT, 'native execution fact ownership is permanent'); END`,
    );
  }
});
