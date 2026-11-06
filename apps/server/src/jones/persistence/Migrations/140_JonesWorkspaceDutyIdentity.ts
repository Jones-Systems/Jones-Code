import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql=yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_workspace_duty_identity (
    claim_id TEXT PRIMARY KEY NOT NULL,
    identity_json TEXT NOT NULL CHECK(json_valid(identity_json)),
    native_owner_json TEXT CHECK(native_owner_json IS NULL OR json_valid(native_owner_json))
  )`;
});
