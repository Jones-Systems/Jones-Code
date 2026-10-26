import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

// Sparse owner intent keyed by the upstream thread, without upstream references.
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_owner_selections (
    thread_id TEXT PRIMARY KEY NOT NULL,
    command_id TEXT NOT NULL,
    result_sequence INTEGER NOT NULL,
    model_selection_json TEXT NOT NULL CHECK (json_valid(model_selection_json)),
    recorded_at TEXT NOT NULL
  )`;
});
