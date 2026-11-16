import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_client_settlement_commands (
    command_id TEXT PRIMARY KEY NOT NULL,
    thread_id TEXT NOT NULL,
    recorded_at TEXT NOT NULL
  )`;
});
