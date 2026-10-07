import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS workstreams_native_attempts (
    owner_id TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    command_id TEXT NOT NULL,
    request_json TEXT NOT NULL CHECK(json_valid(request_json)),
    request_bytes_sha256 TEXT NOT NULL CHECK(length(request_bytes_sha256) = 64),
    enrollment_sha256 TEXT NOT NULL CHECK(length(enrollment_sha256) = 64),
    enrollment_json TEXT NOT NULL CHECK(json_valid(enrollment_json)),
    native_command_id TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    dispatch_started_at TEXT,
    PRIMARY KEY(owner_id, principal_id, command_id)
  )`;
  yield* sql`CREATE TRIGGER IF NOT EXISTS workstreams_native_attempts_immutable
    BEFORE UPDATE ON workstreams_native_attempts
    WHEN OLD.owner_id IS NOT NEW.owner_id OR OLD.principal_id IS NOT NEW.principal_id
      OR OLD.command_id IS NOT NEW.command_id OR OLD.request_json IS NOT NEW.request_json
      OR OLD.request_bytes_sha256 IS NOT NEW.request_bytes_sha256
      OR OLD.enrollment_sha256 IS NOT NEW.enrollment_sha256 OR OLD.enrollment_json IS NOT NEW.enrollment_json
      OR OLD.native_command_id IS NOT NEW.native_command_id OR OLD.created_at IS NOT NEW.created_at
      OR OLD.dispatch_started_at IS NOT NULL OR NEW.dispatch_started_at IS NULL
    BEGIN SELECT RAISE(ABORT, 'native attempt association is immutable'); END`;
  yield* sql`CREATE TRIGGER IF NOT EXISTS workstreams_native_attempts_no_delete
    BEFORE DELETE ON workstreams_native_attempts
    BEGIN SELECT RAISE(ABORT, 'native attempts are permanent'); END`;
  yield* sql`CREATE TRIGGER IF NOT EXISTS workstreams_native_attempts_no_replace
    BEFORE INSERT ON workstreams_native_attempts
    WHEN EXISTS (SELECT 1 FROM workstreams_native_attempts
      WHERE (owner_id = NEW.owner_id AND principal_id = NEW.principal_id AND command_id = NEW.command_id)
        OR native_command_id = NEW.native_command_id)
    BEGIN SELECT RAISE(ABORT, 'native command ownership is permanent'); END`;
});
