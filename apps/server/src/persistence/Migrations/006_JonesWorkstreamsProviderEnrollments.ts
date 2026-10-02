import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS workstreams_native_enrollments (
    enrollment_id TEXT PRIMARY KEY NOT NULL,
    session_id TEXT UNIQUE NOT NULL REFERENCES auth_sessions(session_id),
    request_sha256 TEXT NOT NULL CHECK(length(request_sha256) = 64),
    request_json TEXT NOT NULL CHECK(json_valid(request_json)),
    binding_json TEXT NOT NULL CHECK(json_valid(binding_json))
  )`;
  yield* sql`CREATE TRIGGER IF NOT EXISTS workstreams_native_enrollments_no_update
    BEFORE UPDATE ON workstreams_native_enrollments
    BEGIN SELECT RAISE(ABORT, 'native enrollment is immutable'); END`;
  yield* sql`CREATE TRIGGER IF NOT EXISTS workstreams_native_enrollments_no_delete
    BEFORE DELETE ON workstreams_native_enrollments
    BEGIN SELECT RAISE(ABORT, 'native enrollment is permanent'); END`;
  yield* sql`CREATE TRIGGER IF NOT EXISTS workstreams_native_enrollments_no_replace
    BEFORE INSERT ON workstreams_native_enrollments
    WHEN EXISTS (SELECT 1 FROM workstreams_native_enrollments
      WHERE enrollment_id = NEW.enrollment_id OR session_id = NEW.session_id)
    BEGIN SELECT RAISE(ABORT, 'native enrollment ownership is permanent'); END`;
});
