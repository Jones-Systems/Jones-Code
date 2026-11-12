import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

// Why and when a queued upstream run was held, keyed by its run ID without upstream references.
// A row exists only while Jones has recorded a hold; a missing row reads as a legacy hold.
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_queue_holds (
    run_id TEXT PRIMARY KEY NOT NULL,
    thread_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    held_at TEXT NOT NULL
  )`;
  yield* sql`CREATE INDEX jones_queue_holds_by_thread ON jones_queue_holds(thread_id)`;
});
