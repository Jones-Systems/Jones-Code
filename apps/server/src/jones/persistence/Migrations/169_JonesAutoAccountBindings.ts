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
    accepted_sequence INTEGER,
    first_message_id TEXT,
    first_run_id TEXT,
    CHECK ((refusal_message IS NULL AND thread_id IS NOT NULL AND accepted_sequence IS NOT NULL)
      OR (refusal_message IS NOT NULL AND accepted_sequence IS NULL)),
    CHECK ((first_message_id IS NULL AND first_run_id IS NULL) OR
      (accepted_sequence IS NOT NULL AND first_message_id IS NOT NULL AND first_run_id IS NOT NULL))
  )`;
});
