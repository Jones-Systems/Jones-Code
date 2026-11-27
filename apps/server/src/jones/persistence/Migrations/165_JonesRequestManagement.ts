import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_request_management_edits (
    admission_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK(revision > 0),
    previous_version TEXT NOT NULL CHECK(length(previous_version) = 64),
    action TEXT NOT NULL CHECK(action IN ('label','dismiss','restore')),
    label TEXT,
    dismissed_at TEXT,
    changed_at TEXT NOT NULL,
    actor_session_id TEXT NOT NULL,
    PRIMARY KEY(admission_id,revision)
  )`;
  yield* sql`CREATE INDEX jones_remote_admission_client_inbox
    ON jones_remote_admissions(client_id,created_at DESC,id DESC)`;
});
