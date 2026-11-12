import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_personal_workstreams (
    id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, lifecycle TEXT NOT NULL,
    color TEXT, order_key TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    deleted_at TEXT, rev INTEGER NOT NULL, origin TEXT NOT NULL
  ) STRICT`;
  yield* sql`CREATE TABLE jones_personal_workstream_assignments (
    thread_id TEXT PRIMARY KEY NOT NULL, workstream_id TEXT, revision INTEGER NOT NULL, updated_at TEXT NOT NULL
  ) STRICT`;
  yield* sql`CREATE INDEX jones_personal_assignments_by_workstream ON jones_personal_workstream_assignments(workstream_id)`;
  yield* sql`CREATE TABLE jones_personal_workstream_meta (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL) STRICT`;
  yield* sql`INSERT INTO jones_personal_workstream_meta(key,value) VALUES('sequence','0')`;
});
