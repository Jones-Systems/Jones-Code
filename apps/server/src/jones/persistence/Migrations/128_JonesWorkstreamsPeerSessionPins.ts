import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_workstreams_peer_session_pins (
    registry_instance_id TEXT NOT NULL, owner_id TEXT NOT NULL,
    pin_id TEXT PRIMARY KEY, session_id TEXT NOT NULL UNIQUE,
    revision INTEGER NOT NULL CHECK(revision BETWEEN 1 AND 9007199254740991),
    state TEXT NOT NULL CHECK(state IN ('prepared','bound','revoked')),
    expires_at TEXT NOT NULL,
    setup_request_id TEXT NOT NULL UNIQUE,
    setup_request_sha256 TEXT NOT NULL CHECK(length(setup_request_sha256) = 64),
    envelope TEXT NOT NULL CHECK(coalesce(json_valid(envelope) AND
      json_extract(envelope, '$.v') = 1 AND json_extract(envelope, '$.alg') = 'A256GCM' AND
      json_type(envelope, '$.key_id') = 'text' AND json_type(envelope, '$.nonce') = 'text' AND
      length(json_extract(envelope, '$.nonce')) = 16 AND json_type(envelope, '$.ct') = 'text' AND
      json_type(envelope, '$.tag') = 'text' AND length(json_extract(envelope, '$.tag')) = 24, 0))
  ) STRICT`;
  yield* sql`CREATE TRIGGER jones_workstreams_peer_session_pins_no_replace
    BEFORE INSERT ON jones_workstreams_peer_session_pins
    WHEN EXISTS(SELECT 1 FROM jones_workstreams_peer_session_pins
      WHERE pin_id = NEW.pin_id OR session_id = NEW.session_id OR setup_request_id = NEW.setup_request_id)
    BEGIN SELECT RAISE(ABORT, 'Peer pin identity already reserved'); END`;
  yield* sql`CREATE TRIGGER jones_workstreams_peer_session_pins_revision_guard
    BEFORE UPDATE ON jones_workstreams_peer_session_pins
    WHEN NEW.registry_instance_id != OLD.registry_instance_id OR NEW.owner_id != OLD.owner_id
      OR NEW.pin_id != OLD.pin_id OR NEW.session_id != OLD.session_id
      OR NEW.setup_request_id != OLD.setup_request_id OR NEW.setup_request_sha256 != OLD.setup_request_sha256
      OR NEW.expires_at != OLD.expires_at OR NEW.revision != OLD.revision + 1
      OR NEW.envelope = OLD.envelope
      OR NOT ((OLD.state = 'prepared' AND NEW.state IN ('bound','revoked'))
        OR (OLD.state = 'bound' AND NEW.state = 'revoked'))
    BEGIN SELECT RAISE(ABORT, 'Peer pin transition is not current'); END`;
  yield* sql`CREATE TRIGGER jones_workstreams_peer_session_pins_no_delete
    BEFORE DELETE ON jones_workstreams_peer_session_pins
    BEGIN SELECT RAISE(ABORT, 'Peer pin history cannot be deleted'); END`;
});
