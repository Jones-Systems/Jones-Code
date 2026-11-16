import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_workspace_duty_lifetimes (
    claim_id TEXT PRIMARY KEY NOT NULL,
    holder_sha256 TEXT NOT NULL,
    use_id TEXT NOT NULL,
    binding_sha256 TEXT NOT NULL,
    linked_at TEXT NOT NULL
  )`;
});
