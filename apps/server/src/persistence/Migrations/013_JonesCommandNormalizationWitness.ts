import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`CREATE TABLE command_normalization_witnesses (
    command_id TEXT PRIMARY KEY NOT NULL REFERENCES orchestration_command_receipts(command_id)
      CHECK(length(command_id) > 0),
    command_type TEXT NOT NULL CHECK(command_type IN ('message.dispatch', 'queued-run.edit', 'runtime-request.respond')),
    witness_version INTEGER NOT NULL CHECK(witness_version = 1),
    request_digest TEXT NOT NULL CHECK(length(request_digest) = 64 AND request_digest NOT GLOB '*[^0-9a-f]*'),
    attachments_json TEXT NOT NULL CHECK(json_valid(attachments_json) AND json_type(attachments_json) = 'array'),
    context_remaps_json TEXT NOT NULL CHECK(json_valid(context_remaps_json) AND json_type(context_remaps_json) = 'array'),
    accepted_command_json TEXT NOT NULL CHECK(json_valid(accepted_command_json) AND json_type(accepted_command_json) = 'object'
      AND json_type(accepted_command_json, '$.commandId') IS 'text' AND json_extract(accepted_command_json, '$.commandId') = command_id
      AND json_type(accepted_command_json, '$.type') IS 'text' AND json_extract(accepted_command_json, '$.type') = command_type
      AND json_type(accepted_command_json, '$.threadId') IS 'text' AND json_extract(accepted_command_json, '$.threadId') = thread_id),
    accepted_command_digest TEXT NOT NULL CHECK(length(accepted_command_digest) = 64 AND accepted_command_digest NOT GLOB '*[^0-9a-f]*'),
    receipt_sequence INTEGER NOT NULL CHECK(typeof(receipt_sequence) = 'integer' AND receipt_sequence >= 0),
    thread_id TEXT NOT NULL CHECK(length(thread_id) > 0),
    project_id TEXT NOT NULL CHECK(length(project_id) > 0),
    application_birth_json TEXT NOT NULL CHECK(json_valid(application_birth_json) AND json_type(application_birth_json) = 'object'
      AND json_type(application_birth_json, '$.kind') IS 'text' AND json_extract(application_birth_json, '$.kind') = 'application_v2_thread_birth'
      AND json_type(application_birth_json, '$.threadId') IS 'text' AND json_extract(application_birth_json, '$.threadId') = thread_id
      AND json_type(application_birth_json, '$.eventId') IS 'text' AND length(json_extract(application_birth_json, '$.eventId')) > 0
      AND json_type(application_birth_json, '$.sequence') IS 'integer' AND json_extract(application_birth_json, '$.sequence') > 0),
    created_at TEXT NOT NULL CHECK(length(created_at) > 0)
  )`;
  yield* sql`CREATE TRIGGER command_normalization_witnesses_no_update
    BEFORE UPDATE ON command_normalization_witnesses
    BEGIN SELECT RAISE(ABORT, 'command normalization witnesses are immutable'); END`;
  yield* sql`CREATE TRIGGER command_normalization_witnesses_no_delete
    BEFORE DELETE ON command_normalization_witnesses
    BEGIN SELECT RAISE(ABORT, 'command normalization witnesses are permanent'); END`;
  yield* sql`CREATE TRIGGER command_normalization_witnesses_no_replace
    BEFORE INSERT ON command_normalization_witnesses
    WHEN EXISTS (SELECT 1 FROM command_normalization_witnesses WHERE command_id = NEW.command_id)
    BEGIN SELECT RAISE(ABORT, 'command normalization witness identity is permanent'); END`;
});
