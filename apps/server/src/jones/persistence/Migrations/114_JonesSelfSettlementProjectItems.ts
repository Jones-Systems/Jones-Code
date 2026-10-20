import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_self_settlement_project_items (
    command_id TEXT PRIMARY KEY NOT NULL,
    thread_id TEXT NOT NULL,
    items_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL
  )`;
});
