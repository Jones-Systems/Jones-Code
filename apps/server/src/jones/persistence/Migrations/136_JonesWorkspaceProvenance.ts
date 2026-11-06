import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_workspace_provenance (
    incarnation_id TEXT PRIMARY KEY NOT NULL,
    worktree TEXT NOT NULL,
    project_id TEXT NOT NULL,
    provenance_json TEXT NOT NULL CHECK(json_valid(provenance_json))
  )`;
  yield* sql`CREATE INDEX jones_workspace_provenance_path ON jones_workspace_provenance(worktree)`;
  yield* sql`CREATE TABLE jones_workspace_provenance_threads (
    incarnation_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    PRIMARY KEY(incarnation_id,thread_id,task_id)
  )`;
  yield* sql`CREATE INDEX jones_workspace_provenance_thread ON jones_workspace_provenance_threads(thread_id)`;
  yield* sql`CREATE TABLE jones_workspace_provenance_runs (
    run_id TEXT PRIMARY KEY NOT NULL,
    incarnation_id TEXT NOT NULL,
    thread_id TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE jones_workspace_provenance_facts (
    event_id TEXT PRIMARY KEY NOT NULL,
    incarnation_id TEXT NOT NULL,
    fact_json TEXT NOT NULL CHECK(json_valid(fact_json))
  )`;
  yield* sql`CREATE INDEX jones_workspace_provenance_fact_incarnation ON jones_workspace_provenance_facts(incarnation_id)`;
});
