import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_remote_admissions (
    id TEXT PRIMARY KEY NOT NULL,
    client_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    binding_digest TEXT NOT NULL,
    envelope_json TEXT NOT NULL CHECK(json_valid(envelope_json)),
    status TEXT NOT NULL CHECK(status IN ('evaluating','passed','held','approved','denied','cancelled','dispatching','dispatched')),
    reason TEXT,
    category TEXT,
    command_id TEXT UNIQUE,
    resolved_by TEXT,
    resolved_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(client_id, idempotency_key),
    CHECK((status IN ('dispatching','dispatched')) = (command_id IS NOT NULL))
  )`;
  yield* sql`CREATE INDEX jones_remote_admissions_pending ON jones_remote_admissions(status, created_at, id)`;
  yield* sql`CREATE TRIGGER jones_remote_admissions_binding_immutable
    BEFORE UPDATE ON jones_remote_admissions
    WHEN NEW.id != OLD.id OR NEW.client_id != OLD.client_id
      OR NEW.idempotency_key != OLD.idempotency_key OR NEW.request_digest != OLD.request_digest
      OR NEW.binding_digest != OLD.binding_digest OR NEW.envelope_json != OLD.envelope_json
      OR NEW.created_at != OLD.created_at
      OR (OLD.command_id IS NOT NULL AND NEW.command_id IS NOT OLD.command_id)
    BEGIN SELECT RAISE(ABORT, 'Remote admission binding is immutable'); END`;
});
