import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // Each row names its own token in the secret store, including racing links
  // to the same environment. Tokens themselves never belong in this table.
  // Only sessions issued as fenced links may resolve; unmarked rows must pair again.
  yield* sql`
    CREATE TABLE IF NOT EXISTS jones_peer_environment_links (
      environment_id TEXT PRIMARY KEY,
      secret_name TEXT NOT NULL,
      label TEXT NOT NULL,
      urls_json TEXT NOT NULL,
      access TEXT NOT NULL,
      linked_at TEXT NOT NULL,
      expires_at TEXT NOT NULL,
      last_reached_at TEXT,
      last_error TEXT,
      fenced INTEGER NOT NULL DEFAULT 0
    )
  `;
});
