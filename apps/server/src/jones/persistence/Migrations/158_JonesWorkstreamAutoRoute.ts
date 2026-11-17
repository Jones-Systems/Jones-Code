import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

// One sparse row per queue dispatch the auto-router observed, keyed by upstream thread and
// command IDs without references to upstream tables. Rows hold codes and IDs only.
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_workstream_auto_route_decisions (
    thread_id TEXT NOT NULL,
    command_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    origin TEXT NOT NULL,
    state TEXT NOT NULL,
    code TEXT,
    workstream_id TEXT,
    assignment_revision INTEGER,
    confidence TEXT,
    basis TEXT,
    candidate_count INTEGER,
    requested_instance_id TEXT,
    requested_model TEXT,
    created_at TEXT NOT NULL,
    decided_at TEXT,
    PRIMARY KEY (thread_id, command_id)
  ) STRICT`;
  yield* sql`CREATE INDEX jones_workstream_auto_route_by_thread
    ON jones_workstream_auto_route_decisions(thread_id, created_at)`;
});
