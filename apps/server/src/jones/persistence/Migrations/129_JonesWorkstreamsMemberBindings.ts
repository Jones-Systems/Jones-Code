import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

// Envelope authentication, exact retained bytes, digest recomputation and payload/selector
// equality belong to the owning stores. SQL fences identities and legal history only.
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_workstreams_home_bind_receipts (
    registry_instance_id TEXT NOT NULL CHECK(length(registry_instance_id) = 36 AND
      registry_instance_id GLOB '????????-????-????-????-????????????' AND
      replace(registry_instance_id, '-', '') NOT GLOB '*[^a-f0-9]*' AND
      length(replace(registry_instance_id, '-', '')) = 32),
    owner_id TEXT NOT NULL CHECK(length(owner_id) BETWEEN 1 AND 128 AND owner_id GLOB '[A-Za-z0-9]*' AND owner_id NOT GLOB '*[^A-Za-z0-9._:-]*'),
    bind_id TEXT NOT NULL CHECK(length(bind_id) BETWEEN 1 AND 128 AND bind_id GLOB '[A-Za-z0-9]*' AND bind_id NOT GLOB '*[^A-Za-z0-9._:-]*'),
    request_sha256 TEXT NOT NULL CHECK(length(request_sha256) = 64 AND request_sha256 NOT GLOB '*[^a-f0-9]*'),
    home_pin_id TEXT NOT NULL CHECK(length(home_pin_id) BETWEEN 1 AND 128 AND home_pin_id GLOB '[A-Za-z0-9]*' AND home_pin_id NOT GLOB '*[^A-Za-z0-9._:-]*'),
    home_prepared_revision INTEGER NOT NULL CHECK(home_prepared_revision = 1),
    home_setup_request_id TEXT NOT NULL CHECK(length(home_setup_request_id) BETWEEN 1 AND 128 AND home_setup_request_id GLOB '[A-Za-z0-9]*' AND home_setup_request_id NOT GLOB '*[^A-Za-z0-9._:-]*'),
    home_setup_request_sha256 TEXT NOT NULL CHECK(length(home_setup_request_sha256) = 64 AND home_setup_request_sha256 NOT GLOB '*[^a-f0-9]*'),
    request_envelope TEXT NOT NULL CHECK(coalesce(json_valid(request_envelope) AND
      json_extract(request_envelope, '$.v') = 1 AND json_extract(request_envelope, '$.alg') = 'A256GCM' AND
      json_type(request_envelope, '$.key_id') = 'text' AND length(json_extract(request_envelope, '$.key_id')) > 0 AND
      json_type(request_envelope, '$.nonce') = 'text' AND length(json_extract(request_envelope, '$.nonce')) = 16 AND
      json_type(request_envelope, '$.ct') = 'text' AND
      json_type(request_envelope, '$.tag') = 'text' AND length(json_extract(request_envelope, '$.tag')) = 24, 0)),
    result_envelope TEXT NOT NULL CHECK(coalesce(json_valid(result_envelope) AND
      json_extract(result_envelope, '$.v') = 1 AND json_extract(result_envelope, '$.alg') = 'A256GCM' AND
      json_type(result_envelope, '$.key_id') = 'text' AND length(json_extract(result_envelope, '$.key_id')) > 0 AND
      json_type(result_envelope, '$.nonce') = 'text' AND length(json_extract(result_envelope, '$.nonce')) = 16 AND
      json_type(result_envelope, '$.ct') = 'text' AND
      json_type(result_envelope, '$.tag') = 'text' AND length(json_extract(result_envelope, '$.tag')) = 24, 0)),
    PRIMARY KEY(registry_instance_id, owner_id, bind_id),
    UNIQUE(home_pin_id),
    UNIQUE(home_setup_request_id)
  ) STRICT`;
  yield* sql`CREATE TRIGGER jones_workstreams_home_bind_receipts_no_replace
    BEFORE INSERT ON jones_workstreams_home_bind_receipts
    WHEN EXISTS(SELECT 1 FROM jones_workstreams_home_bind_receipts
      WHERE (registry_instance_id = NEW.registry_instance_id AND owner_id = NEW.owner_id AND bind_id = NEW.bind_id)
        OR home_pin_id = NEW.home_pin_id OR home_setup_request_id = NEW.home_setup_request_id)
    BEGIN SELECT RAISE(ABORT, 'Home bind preparation already consumed'); END`;
  yield* sql`CREATE TRIGGER jones_workstreams_home_bind_receipts_no_update
    BEFORE UPDATE ON jones_workstreams_home_bind_receipts
    BEGIN SELECT RAISE(ABORT, 'Home bind receipt is immutable'); END`;
  yield* sql`CREATE TRIGGER jones_workstreams_home_bind_receipts_no_delete
    BEFORE DELETE ON jones_workstreams_home_bind_receipts
    BEGIN SELECT RAISE(ABORT, 'Home bind receipt history cannot be deleted'); END`;
  yield* sql`CREATE TABLE jones_workstreams_member_enrollments (
    registry_instance_id TEXT NOT NULL CHECK(length(registry_instance_id) = 36 AND
      registry_instance_id GLOB '????????-????-????-????-????????????' AND
      replace(registry_instance_id, '-', '') NOT GLOB '*[^a-f0-9]*' AND
      length(replace(registry_instance_id, '-', '')) = 32),
    owner_id TEXT NOT NULL CHECK(length(owner_id) BETWEEN 1 AND 128 AND owner_id GLOB '[A-Za-z0-9]*' AND owner_id NOT GLOB '*[^A-Za-z0-9._:-]*'),
    enrollment_id TEXT NOT NULL CHECK(length(enrollment_id) BETWEEN 1 AND 128 AND enrollment_id GLOB '[A-Za-z0-9]*' AND enrollment_id NOT GLOB '*[^A-Za-z0-9._:-]*') UNIQUE,
    session_id TEXT NOT NULL UNIQUE CHECK(length(session_id) > 0 AND trim(session_id) = session_id),
    setup_request_id TEXT NOT NULL CHECK(length(setup_request_id) BETWEEN 1 AND 128 AND setup_request_id GLOB '[A-Za-z0-9]*' AND setup_request_id NOT GLOB '*[^A-Za-z0-9._:-]*') UNIQUE,
    bind_id TEXT NOT NULL CHECK(length(bind_id) BETWEEN 1 AND 128 AND bind_id GLOB '[A-Za-z0-9]*' AND bind_id NOT GLOB '*[^A-Za-z0-9._:-]*'),
    request_sha256 TEXT NOT NULL CHECK(length(request_sha256) = 64 AND request_sha256 NOT GLOB '*[^a-f0-9]*'),
    intent_sha256 TEXT NOT NULL CHECK(length(intent_sha256) = 64 AND intent_sha256 NOT GLOB '*[^a-f0-9]*'),
    home_environment_id TEXT NOT NULL CHECK(length(home_environment_id) BETWEEN 1 AND 128 AND home_environment_id GLOB '[A-Za-z0-9]*' AND home_environment_id NOT GLOB '*[^A-Za-z0-9._:-]*'),
    home_generation INTEGER NOT NULL CHECK(home_generation BETWEEN 1 AND 9007199254740991),
    revision INTEGER NOT NULL CHECK(revision BETWEEN 1 AND 9007199254740991),
    state TEXT NOT NULL CHECK(state IN ('prepared','bound')),
    envelope TEXT NOT NULL CHECK(coalesce(json_valid(envelope) AND
      json_extract(envelope, '$.v') = 1 AND json_extract(envelope, '$.alg') = 'A256GCM' AND
      json_type(envelope, '$.key_id') = 'text' AND length(json_extract(envelope, '$.key_id')) > 0 AND
      json_type(envelope, '$.nonce') = 'text' AND length(json_extract(envelope, '$.nonce')) = 16 AND
      json_type(envelope, '$.ct') = 'text' AND
      json_type(envelope, '$.tag') = 'text' AND length(json_extract(envelope, '$.tag')) = 24, 0)),
    PRIMARY KEY(registry_instance_id, owner_id, enrollment_id),
    UNIQUE(registry_instance_id, owner_id, bind_id)
  ) STRICT`;
  yield* sql`CREATE TRIGGER jones_workstreams_member_enrollments_prepared_insert
    BEFORE INSERT ON jones_workstreams_member_enrollments
    WHEN NEW.state != 'prepared' OR NEW.revision != 1
    BEGIN SELECT RAISE(ABORT, 'Member enrollment must begin prepared'); END`;
  yield* sql`CREATE TRIGGER jones_workstreams_member_enrollments_no_replace
    BEFORE INSERT ON jones_workstreams_member_enrollments
    WHEN EXISTS(SELECT 1 FROM jones_workstreams_member_enrollments
      WHERE enrollment_id = NEW.enrollment_id OR session_id = NEW.session_id
        OR setup_request_id = NEW.setup_request_id
        OR (registry_instance_id = NEW.registry_instance_id AND owner_id = NEW.owner_id AND bind_id = NEW.bind_id))
    BEGIN SELECT RAISE(ABORT, 'Member enrollment identity already reserved'); END`;
  yield* sql`CREATE TRIGGER jones_workstreams_member_enrollments_revision_guard
    BEFORE UPDATE ON jones_workstreams_member_enrollments
    WHEN NEW.registry_instance_id != OLD.registry_instance_id OR NEW.owner_id != OLD.owner_id
      OR NEW.enrollment_id != OLD.enrollment_id OR NEW.session_id != OLD.session_id
      OR NEW.setup_request_id != OLD.setup_request_id OR NEW.bind_id != OLD.bind_id
      OR NEW.request_sha256 != OLD.request_sha256 OR NEW.intent_sha256 != OLD.intent_sha256
      OR NEW.home_environment_id != OLD.home_environment_id OR NEW.home_generation != OLD.home_generation
      OR NEW.revision != OLD.revision + 1 OR NEW.envelope = OLD.envelope
      OR NOT (OLD.state = 'prepared' AND OLD.revision = 1 AND NEW.state = 'bound')
    BEGIN SELECT RAISE(ABORT, 'Member enrollment transition is not current'); END`;
  yield* sql`CREATE TRIGGER jones_workstreams_member_enrollments_no_delete
    BEFORE DELETE ON jones_workstreams_member_enrollments
    BEGIN SELECT RAISE(ABORT, 'Member enrollment history cannot be deleted'); END`;
});
