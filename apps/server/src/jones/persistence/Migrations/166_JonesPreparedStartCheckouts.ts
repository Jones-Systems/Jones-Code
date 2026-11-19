import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_prepared_start_checkouts (
    run_id TEXT PRIMARY KEY NOT NULL,
    release_command_id TEXT NOT NULL,
    release_event_id TEXT NOT NULL,
    witness_json TEXT NOT NULL CHECK(json_valid(witness_json) AND length(witness_json) <= 16384)
  )`;
});
