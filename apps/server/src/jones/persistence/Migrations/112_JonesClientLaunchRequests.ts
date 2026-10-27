import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_mcp_client_launch_requests (
    command_id TEXT PRIMARY KEY NOT NULL,
    thread_id TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    request_version INTEGER NOT NULL,
    message_requested INTEGER NOT NULL CHECK (message_requested IN (0, 1)),
    recorded_at TEXT NOT NULL
  )`;
  yield* sql`CREATE TRIGGER jones_mcp_client_launch_requests_no_update
    BEFORE UPDATE ON jones_mcp_client_launch_requests BEGIN
    SELECT RAISE(ABORT, 'Client launch claims are immutable'); END`;
  yield* sql`CREATE TRIGGER jones_mcp_client_launch_requests_no_delete
    BEFORE DELETE ON jones_mcp_client_launch_requests BEGIN
    SELECT RAISE(ABORT, 'Client launch claims are immutable'); END`;
  // REPLACE can bypass delete triggers when recursive_triggers is disabled.
  yield* sql`CREATE TRIGGER jones_mcp_client_launch_requests_no_replace
    BEFORE INSERT ON jones_mcp_client_launch_requests
    WHEN EXISTS (SELECT 1 FROM jones_mcp_client_launch_requests WHERE command_id = NEW.command_id)
    BEGIN SELECT RAISE(ABORT, 'Client launch claims are immutable'); END`;
});
