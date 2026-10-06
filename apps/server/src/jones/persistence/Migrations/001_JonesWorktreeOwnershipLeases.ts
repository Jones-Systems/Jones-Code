import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE IF NOT EXISTS worktree_ownership_leases (
      resource_path TEXT PRIMARY KEY,
      lease_id TEXT NOT NULL,
      owner_thread_id TEXT NOT NULL,
      owner_incarnation TEXT NOT NULL,
      branch TEXT,
      acquired_at_ms INTEGER NOT NULL,
      renewed_at_ms INTEGER NOT NULL,
      expires_at_ms INTEGER NOT NULL
    )
  `;

  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_worktree_ownership_leases_owner
    ON worktree_ownership_leases(owner_thread_id)
  `;

  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_worktree_ownership_leases_expiry
    ON worktree_ownership_leases(expires_at_ms)
  `;
});
