import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`CREATE TABLE orchestration_v2_imported_application_attachment_inventories (
    inventory_id TEXT PRIMARY KEY NOT NULL CHECK(length(inventory_id) = 64 AND inventory_id NOT GLOB '*[^0-9a-f]*'),
    thread_id TEXT NOT NULL CHECK(length(thread_id) > 0),
    project_id TEXT NOT NULL CHECK(length(project_id) > 0),
    application_birth_event_id TEXT NOT NULL REFERENCES orchestration_events(event_id),
    application_birth_sequence INTEGER NOT NULL CHECK(application_birth_sequence > 0),
    adoption_ordinal INTEGER NOT NULL CHECK(adoption_ordinal >= 0),
    source_kind TEXT NOT NULL CHECK(source_kind IN ('legacy_projection', 'native_import_batch')),
    canonical_header_json TEXT NOT NULL CHECK(json_valid(canonical_header_json) AND json_type(canonical_header_json) = 'object'
      AND json_type(canonical_header_json, '$.version') IS 'integer' AND json_extract(canonical_header_json, '$.version') = 1),
    canonical_source_json TEXT NOT NULL CHECK(json_valid(canonical_source_json) AND json_type(canonical_source_json) = 'object'),
    message_carrier_count INTEGER NOT NULL CHECK(message_carrier_count >= 0),
    answer_carrier_count INTEGER NOT NULL CHECK(answer_carrier_count >= 0),
    attachment_reference_count INTEGER NOT NULL CHECK(attachment_reference_count >= 0),
    carrier_set_sha256 TEXT NOT NULL CHECK(length(carrier_set_sha256) = 64 AND carrier_set_sha256 NOT GLOB '*[^0-9a-f]*'),
    recorded_at TEXT NOT NULL CHECK(length(recorded_at) > 0),
    UNIQUE(thread_id, application_birth_event_id, application_birth_sequence, adoption_ordinal),
    CHECK(source_kind != 'native_import_batch' OR (answer_carrier_count = 0 AND attachment_reference_count = 0))
  )`;
  yield* sql`CREATE TRIGGER orchestration_v2_imported_application_attachment_inventories_no_update
    BEFORE UPDATE ON orchestration_v2_imported_application_attachment_inventories
    BEGIN SELECT RAISE(ABORT, 'imported application attachment inventories are immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_imported_application_attachment_inventories_no_delete
    BEFORE DELETE ON orchestration_v2_imported_application_attachment_inventories
    BEGIN SELECT RAISE(ABORT, 'imported application attachment inventories are permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_imported_application_attachment_inventories_no_replace
    BEFORE INSERT ON orchestration_v2_imported_application_attachment_inventories
    WHEN EXISTS (SELECT 1 FROM orchestration_v2_imported_application_attachment_inventories
      WHERE inventory_id = NEW.inventory_id OR (thread_id = NEW.thread_id
        AND application_birth_event_id = NEW.application_birth_event_id
        AND application_birth_sequence = NEW.application_birth_sequence AND adoption_ordinal = NEW.adoption_ordinal))
    BEGIN SELECT RAISE(ABORT, 'imported application attachment adoption is permanent'); END`;

  yield* sql`CREATE TABLE orchestration_v2_imported_application_attachment_carriers (
    inventory_id TEXT NOT NULL REFERENCES orchestration_v2_imported_application_attachment_inventories(inventory_id),
    carrier_kind TEXT NOT NULL CHECK(carrier_kind IN ('legacy_message', 'legacy_answer')),
    carrier_id TEXT NOT NULL CHECK(length(carrier_id) > 0),
    canonical_carrier_json TEXT NOT NULL CHECK(json_valid(canonical_carrier_json) AND json_type(canonical_carrier_json) = 'object'),
    source_row_sha256 TEXT NOT NULL CHECK(length(source_row_sha256) = 64 AND source_row_sha256 NOT GLOB '*[^0-9a-f]*'),
    PRIMARY KEY(inventory_id, carrier_kind, carrier_id)
  )`;
  yield* sql`CREATE TRIGGER orchestration_v2_imported_application_attachment_carriers_no_update
    BEFORE UPDATE ON orchestration_v2_imported_application_attachment_carriers
    BEGIN SELECT RAISE(ABORT, 'imported application attachment carriers are immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_imported_application_attachment_carriers_no_delete
    BEFORE DELETE ON orchestration_v2_imported_application_attachment_carriers
    BEGIN SELECT RAISE(ABORT, 'imported application attachment carriers are permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_imported_application_attachment_carriers_no_replace
    BEFORE INSERT ON orchestration_v2_imported_application_attachment_carriers
    WHEN EXISTS (SELECT 1 FROM orchestration_v2_imported_application_attachment_carriers
      WHERE inventory_id = NEW.inventory_id AND carrier_kind = NEW.carrier_kind AND carrier_id = NEW.carrier_id)
    BEGIN SELECT RAISE(ABORT, 'imported application attachment carrier identity is permanent'); END`;
});
