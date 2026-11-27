import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_task_folder_teardowns (
    root TEXT PRIMARY KEY NOT NULL,
    real_root TEXT NOT NULL UNIQUE,
    claim_id TEXT NOT NULL DEFAULT (lower(hex(randomblob(16)))),
    capture_json TEXT NOT NULL CHECK(json_valid(capture_json)),
    outcome TEXT NOT NULL CHECK(outcome IN ('unknown', 'succeeded'))
  )`;
});
