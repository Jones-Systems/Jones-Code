import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

/** Jones-only append-preserving parent ledger. No foreign keys to upstream. */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_workstreams_forward_receipts (
    registry_instance_id TEXT NOT NULL CHECK(length(registry_instance_id)=36),
    owner_id TEXT NOT NULL CHECK(length(owner_id) BETWEEN 1 AND 128),
    source_environment_id TEXT NOT NULL CHECK(length(source_environment_id) BETWEEN 1 AND 128),
    forward_id TEXT NOT NULL CHECK(length(forward_id) BETWEEN 16 AND 128 AND forward_id NOT GLOB '*[^A-Za-z0-9._:-]*'),
    request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64 AND request_sha256 NOT GLOB '*[^a-f0-9]*'),
    identity_sha256 TEXT NOT NULL CHECK(length(identity_sha256)=64 AND identity_sha256 NOT GLOB '*[^a-f0-9]*'),
    wire_sha256 TEXT NOT NULL CHECK(length(wire_sha256)=64 AND wire_sha256 NOT GLOB '*[^a-f0-9]*'),
    state TEXT NOT NULL CHECK(state IN ('accepted','completed')),
    request_envelope TEXT NOT NULL CHECK(coalesce(json_valid(request_envelope) AND json_extract(request_envelope,'$.v')=1 AND json_extract(request_envelope,'$.alg')='A256GCM' AND json_type(request_envelope,'$.key_id')='text' AND length(json_extract(request_envelope,'$.key_id')) BETWEEN 1 AND 128 AND json_type(request_envelope,'$.ct')='text' AND length(json_extract(request_envelope,'$.nonce'))=16 AND length(json_extract(request_envelope,'$.tag'))=24,0)),
    result_envelope TEXT,
    CHECK(coalesce((state='accepted' AND result_envelope IS NULL) OR
      (state='completed' AND result_envelope IS NOT NULL AND json_valid(result_envelope) AND json_extract(result_envelope,'$.v')=1 AND json_extract(result_envelope,'$.alg')='A256GCM' AND json_type(result_envelope,'$.key_id')='text' AND length(json_extract(result_envelope,'$.key_id')) BETWEEN 1 AND 128 AND json_type(result_envelope,'$.ct')='text' AND length(json_extract(result_envelope,'$.nonce'))=16 AND length(json_extract(result_envelope,'$.tag'))=24),0)),
    PRIMARY KEY(registry_instance_id,owner_id,source_environment_id,forward_id)
  ) STRICT`;
  yield* sql`CREATE TRIGGER jones_workstreams_forward_receipts_accepted_insert BEFORE INSERT ON jones_workstreams_forward_receipts
    WHEN NEW.state!='accepted' BEGIN SELECT RAISE(ABORT,'Forward must begin accepted'); END`;
  yield* sql`CREATE TRIGGER jones_workstreams_forward_receipts_no_replace BEFORE INSERT ON jones_workstreams_forward_receipts
    WHEN EXISTS(SELECT 1 FROM jones_workstreams_forward_receipts WHERE registry_instance_id=NEW.registry_instance_id AND owner_id=NEW.owner_id AND source_environment_id=NEW.source_environment_id AND forward_id=NEW.forward_id)
    BEGIN SELECT RAISE(ABORT,'Forward identity already reserved'); END`;
  yield* sql`CREATE TRIGGER jones_workstreams_forward_receipts_immutable BEFORE UPDATE ON jones_workstreams_forward_receipts
    WHEN NEW.registry_instance_id!=OLD.registry_instance_id OR NEW.owner_id!=OLD.owner_id OR NEW.source_environment_id!=OLD.source_environment_id OR NEW.forward_id!=OLD.forward_id
      OR NEW.request_sha256!=OLD.request_sha256 OR NEW.identity_sha256!=OLD.identity_sha256 OR NEW.wire_sha256!=OLD.wire_sha256
      OR NEW.request_envelope!=OLD.request_envelope OR OLD.state!='accepted' OR NEW.state!='completed' OR NEW.result_envelope IS NULL
    BEGIN SELECT RAISE(ABORT,'Forward history is immutable'); END`;
  yield* sql`CREATE TRIGGER jones_workstreams_forward_receipts_no_delete BEFORE DELETE ON jones_workstreams_forward_receipts
    BEGIN SELECT RAISE(ABORT,'Forward history cannot be deleted'); END`;
});
