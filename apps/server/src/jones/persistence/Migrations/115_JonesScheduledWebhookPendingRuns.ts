import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_scheduled_webhook_pending_runs (
    delivery_id TEXT PRIMARY KEY NOT NULL,
    task_id TEXT NOT NULL,
    task_created_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    prompt TEXT NOT NULL
  )`;
});
