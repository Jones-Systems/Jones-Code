import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_thread_title_refinement (
    thread_id TEXT PRIMARY KEY NOT NULL,
    state_json TEXT NOT NULL CHECK(json_valid(state_json))
  )`;
});
