import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

// Earlier Jones tables declared foreign keys to upstream tables. With
// foreign_keys on, those keys reject upstream deletes and table rebuilds, such
// as settled-effect pruning. Each table is rebuilt with the same columns, rows,
// rowids, indexes and triggers; only the upstream references are removed.
// References between Jones tables remain. Callers already verify the upstream
// row in the transaction that writes the Jones row.
const rebuilt = [
  [
    "workstreams_native_enrollments",
    `CREATE TABLE workstreams_native_enrollments (
    enrollment_id TEXT PRIMARY KEY NOT NULL,
    session_id TEXT UNIQUE NOT NULL,
    request_sha256 TEXT NOT NULL CHECK(length(request_sha256) = 64),
    request_json TEXT NOT NULL CHECK(json_valid(request_json)),
    binding_json TEXT NOT NULL CHECK(json_valid(binding_json))
  )`,
  ],
  [
    "jones_native_creation_execution_acceptances",
    `CREATE TABLE jones_native_creation_execution_acceptances (
    command_id TEXT PRIMARY KEY NOT NULL,
    claim_id TEXT NOT NULL REFERENCES native_creation_intents(claim_id),
    thread_id TEXT NOT NULL,
    command_digest TEXT NOT NULL CHECK(length(command_digest)=64),
    binding_digest TEXT NOT NULL CHECK(length(binding_digest)=64),
    event_id TEXT NOT NULL,
    event_sequence INTEGER NOT NULL CHECK(event_sequence>0)
  )`,
  ],
  [
    "jones_native_creation_execution_starts",
    `CREATE TABLE jones_native_creation_execution_starts (
    effect_id TEXT PRIMARY KEY NOT NULL,
    claim_id TEXT NOT NULL REFERENCES native_creation_intents(claim_id),
    command_id TEXT NOT NULL REFERENCES jones_native_creation_execution_acceptances(command_id),
    worker_id TEXT NOT NULL,
    expected_attempt INTEGER NOT NULL CHECK(expected_attempt>0),
    lease_expires_at TEXT NOT NULL,
    reference_json TEXT NOT NULL CHECK(json_valid(reference_json)),
    fact_json TEXT NOT NULL CHECK(json_valid(fact_json))
  )`,
  ],
  [
    "jones_imported_history_outcomes",
    `CREATE TABLE jones_imported_history_outcomes (
    command_id TEXT PRIMARY KEY NOT NULL REFERENCES jones_imported_history_choices(command_id),
    receipt_command_id TEXT UNIQUE NOT NULL,
    outcome_json TEXT NOT NULL CHECK(json_valid(outcome_json))
  )`,
  ],
  [
    "jones_imported_history_start_reservations",
    `CREATE TABLE jones_imported_history_start_reservations (
    effect_id TEXT PRIMARY KEY NOT NULL,
    command_id TEXT NOT NULL REFERENCES jones_imported_history_choices(command_id),
    thread_id TEXT NOT NULL, run_id TEXT UNIQUE NOT NULL, run_attempt_id TEXT NOT NULL,
    provider_thread_id TEXT NOT NULL, source_digest TEXT NOT NULL CHECK(length(source_digest)=64)
  )`,
  ],
  [
    "jones_runtime_stop_intents",
    `CREATE TABLE jones_runtime_stop_intents (command_id TEXT PRIMARY KEY NOT NULL,thread_id TEXT NOT NULL,identity_json TEXT NOT NULL CHECK(json_valid(identity_json)))`,
  ],
] as const;

const holder = "temp.jones_upstream_reference_rebuild";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // Dropping a referenced Jones parent counts its children as violations until
  // the same rows return. Deferral lets the enclosing migration commit check that.
  yield* sql`PRAGMA defer_foreign_keys = ON`;
  for (const [table, create] of rebuilt) {
    const columns = (yield* sql<{ readonly name: string }>`
      SELECT name FROM pragma_table_info(${table}) ORDER BY cid
    `).map(({ name }) => `"${name}"`);
    const dependents = yield* sql<{ readonly sql: string }>`
      SELECT sql FROM sqlite_master
      WHERE tbl_name = ${table} AND type IN ('index', 'trigger') AND sql IS NOT NULL
      ORDER BY rowid
    `;
    yield* sql.unsafe(
      `CREATE TABLE ${holder} AS SELECT rowid AS jones_rowid, ${columns.join(",")} FROM "${table}"`,
    );
    yield* sql.unsafe(`DROP TABLE "${table}"`);
    yield* sql.unsafe(create);
    yield* sql.unsafe(
      `INSERT INTO "${table}" (rowid,${columns.join(",")}) SELECT jones_rowid,${columns.join(",")} FROM ${holder} ORDER BY jones_rowid`,
    );
    yield* sql.unsafe(`DROP TABLE ${holder}`);
    for (const dependent of dependents) yield* sql.unsafe(dependent.sql);
  }
});
