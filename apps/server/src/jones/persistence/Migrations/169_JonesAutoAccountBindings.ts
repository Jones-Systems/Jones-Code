import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_auto_account_bindings (
    thread_id TEXT PRIMARY KEY NOT NULL,
    provider_instance_id TEXT NOT NULL,
    account_digest TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE jones_auto_account_launches (
    command_id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL,
    thread_id TEXT,
    request_digest TEXT NOT NULL,
    project_root TEXT NOT NULL,
    worktree_path TEXT,
    refusal_message TEXT,
    CHECK (thread_id IS NOT NULL OR refusal_message IS NOT NULL)
  )`;
});
