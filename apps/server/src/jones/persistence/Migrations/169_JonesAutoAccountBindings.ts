import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_auto_account_bindings (
    thread_id TEXT PRIMARY KEY NOT NULL,
    provider_instance_id TEXT NOT NULL,
    account_digest TEXT NOT NULL
  )`;
});
