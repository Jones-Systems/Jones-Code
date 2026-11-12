import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

/**
 * Copy Jones peer links into upstream's peer_environment_links. secret_name is copied
 * verbatim so tokens stay valid and an existing upstream row wins. The Jones table stays
 * so a rollback to the build that reads it still finds its rows.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const legacy = yield* sql`SELECT 1 AS present FROM jones_peer_environment_links LIMIT 1`;
  // Fresh databases and databases that never linked have nothing to adopt, and
  // an upstream limit may stop before 070 created the destination.
  if (legacy.length === 0) return;

  yield* sql`
    INSERT OR IGNORE INTO peer_environment_links (
      environment_id, secret_name, label, urls_json, access, linked_at, expires_at,
      last_reached_at, last_error, fenced
    )
    SELECT
      environment_id, secret_name, label, urls_json, access, linked_at, expires_at,
      last_reached_at, last_error, fenced
    FROM jones_peer_environment_links
  `;
});
