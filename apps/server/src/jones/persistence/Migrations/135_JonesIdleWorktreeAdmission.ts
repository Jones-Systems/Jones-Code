import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_idle_worktree_admission (
    thread_id TEXT PRIMARY KEY NOT NULL,
    launch_command_id TEXT NOT NULL UNIQUE,
    request_key TEXT NOT NULL,
    generation TEXT NOT NULL,
    phase TEXT NOT NULL CHECK (phase IN (
      'pending', 'worktree_issued', 'workspace_committed', 'setup_issued',
      'prepared', 'ready', 'unresolved'
    )),
    worktree_path TEXT,
    cleanup_owned INTEGER NOT NULL DEFAULT 1 CHECK (cleanup_owned IN (0, 1))
  )`;
});
