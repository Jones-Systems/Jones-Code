import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

// Untrusted presentation hints only. No foreign keys or upstream row copies.
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_auth_session_client_metadata (
    session_id TEXT PRIMARY KEY NOT NULL CHECK(length(session_id)>0),
    identity_digest TEXT NOT NULL CHECK(length(identity_digest)=64),
    client_surface TEXT, client_app_version TEXT
  )`;
});
