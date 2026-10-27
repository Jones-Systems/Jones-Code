import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_goal_continuity (
    record_id TEXT PRIMARY KEY NOT NULL,
    environment_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    current_record INTEGER NOT NULL CHECK(current_record IN (0,1)),
    snapshot_digest TEXT NOT NULL,
    snapshot_json TEXT NOT NULL CHECK(json_valid(snapshot_json)
      AND length(CAST(snapshot_json AS BLOB)) <= 1048576)
  )`;
  yield* sql`CREATE UNIQUE INDEX jones_goal_continuity_current
    ON jones_goal_continuity(environment_id, thread_id) WHERE current_record = 1`;
  // Heads and fences are CAS projections of append-only phase/decision rows.
  // They live beside the receipts, never in upstream projection payloads.
  yield* sql`CREATE TABLE jones_goal_continuity_receipts (
    kind TEXT NOT NULL,
    environment_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    receipt_key TEXT NOT NULL,
    body_digest TEXT NOT NULL,
    open_gate INTEGER NOT NULL DEFAULT 0 CHECK(open_gate IN (0,1)),
    payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
    PRIMARY KEY(kind, environment_id, thread_id, namespace, receipt_key)
  )`;
  yield* sql`CREATE INDEX jones_goal_continuity_open_delivery
    ON jones_goal_continuity_receipts(environment_id, thread_id, kind, open_gate)`;
});
