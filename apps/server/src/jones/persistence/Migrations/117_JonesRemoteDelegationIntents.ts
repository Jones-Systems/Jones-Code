import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_remote_delegation_intents (
    command_id TEXT PRIMARY KEY NOT NULL,
    request_json TEXT NOT NULL CHECK(json_valid(request_json)),
    envelope_json TEXT NOT NULL CHECK(json_valid(envelope_json)),
    launched_json TEXT CHECK(launched_json IS NULL OR json_valid(launched_json)),
    admission_json TEXT CHECK(admission_json IS NULL OR json_valid(admission_json)),
    task_id TEXT,
    launch_started INTEGER NOT NULL DEFAULT 0 CHECK(launch_started IN (0,1)),
    status TEXT NOT NULL CHECK(status IN ('pending','recorded','stopped','denied')),
    created_at TEXT NOT NULL
  )`;
  yield* sql`CREATE INDEX jones_remote_delegation_pending ON jones_remote_delegation_intents(status)`;
  yield* sql`CREATE TRIGGER jones_remote_delegation_binding_immutable
    BEFORE UPDATE ON jones_remote_delegation_intents
    WHEN NEW.command_id != OLD.command_id OR NEW.request_json != OLD.request_json
      OR NEW.envelope_json != OLD.envelope_json OR NEW.created_at != OLD.created_at
    BEGIN SELECT RAISE(ABORT, 'Remote delegation binding is immutable'); END`;
});
