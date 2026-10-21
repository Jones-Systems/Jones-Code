import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_remote_admission_policy (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    policy_json TEXT NOT NULL CHECK(json_valid(policy_json))
  )`;
  yield* sql`CREATE TABLE jones_remote_admission_results (
    admission_id TEXT PRIMARY KEY NOT NULL,
    result_json TEXT NOT NULL CHECK(json_valid(result_json))
  )`;
});
