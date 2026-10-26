import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_workstreams_source_enrollments (
    registry_instance_id TEXT NOT NULL, owner_id TEXT NOT NULL,
    enrollment_id TEXT NOT NULL UNIQUE, session_id TEXT NOT NULL UNIQUE,
    request_sha256 TEXT NOT NULL CHECK(length(request_sha256) = 64),
    envelope TEXT NOT NULL CHECK(coalesce(json_valid(envelope) AND
      json_extract(envelope, '$.v') = 1 AND json_extract(envelope, '$.alg') = 'A256GCM' AND
      json_type(envelope, '$.key_id') = 'text' AND json_type(envelope, '$.nonce') = 'text' AND
      length(json_extract(envelope, '$.nonce')) = 16 AND json_type(envelope, '$.ct') = 'text' AND
      json_type(envelope, '$.tag') = 'text' AND length(json_extract(envelope, '$.tag')) = 24, 0)),
    PRIMARY KEY(registry_instance_id, owner_id, enrollment_id)
  ) STRICT`;
  yield* sql`CREATE TRIGGER jones_workstreams_source_enrollments_no_replace
    BEFORE INSERT ON jones_workstreams_source_enrollments
    WHEN EXISTS(SELECT 1 FROM jones_workstreams_source_enrollments
      WHERE enrollment_id = NEW.enrollment_id OR session_id = NEW.session_id)
    BEGIN SELECT RAISE(ABORT, 'Source issuance identity already reserved'); END`;
  yield* sql`CREATE TRIGGER jones_workstreams_source_enrollments_no_update
    BEFORE UPDATE ON jones_workstreams_source_enrollments
    BEGIN SELECT RAISE(ABORT, 'Source issuance is immutable'); END`;
  yield* sql`CREATE TRIGGER jones_workstreams_source_enrollments_no_delete
    BEFORE DELETE ON jones_workstreams_source_enrollments
    BEGIN SELECT RAISE(ABORT, 'Source issuance history cannot be deleted'); END`;
});
