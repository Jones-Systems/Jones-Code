import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_physical_process_evidence (
    evidence_id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider_thread_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    runtime_generation TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    observation_json TEXT NOT NULL CHECK(json_valid(observation_json))
  )`;
  yield* sql`CREATE INDEX jones_physical_process_evidence_by_binding
    ON jones_physical_process_evidence(provider_thread_id, runtime_generation, evidence_id)`;
});
