import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`CREATE TABLE orchestration_v2_ordinary_checkout_admissions (
    admission_id TEXT PRIMARY KEY NOT NULL CHECK(length(admission_id) = 64 AND admission_id NOT GLOB '*[^0-9a-f]*'),
    command_id TEXT NOT NULL REFERENCES orchestration_command_receipts(command_id),
    thread_id TEXT NOT NULL CHECK(length(thread_id) > 0),
    admission_sha256 TEXT NOT NULL CHECK(length(admission_sha256) = 64 AND admission_sha256 NOT GLOB '*[^0-9a-f]*'),
    admission_json TEXT NOT NULL CHECK(json_valid(admission_json) AND json_type(admission_json) = 'object'),
    recorded_at TEXT NOT NULL CHECK(length(recorded_at) > 0),
    UNIQUE(command_id, thread_id)
  )`;
  yield* sql`CREATE INDEX orchestration_v2_ordinary_checkout_admissions_thread_command_idx
    ON orchestration_v2_ordinary_checkout_admissions(thread_id, command_id)`;
  yield* sql`CREATE TRIGGER orchestration_v2_ordinary_checkout_admissions_no_update
    BEFORE UPDATE ON orchestration_v2_ordinary_checkout_admissions
    BEGIN SELECT RAISE(ABORT, 'ordinary checkout admissions are immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_ordinary_checkout_admissions_no_delete
    BEFORE DELETE ON orchestration_v2_ordinary_checkout_admissions
    BEGIN SELECT RAISE(ABORT, 'ordinary checkout admissions are permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_ordinary_checkout_admissions_no_replace
    BEFORE INSERT ON orchestration_v2_ordinary_checkout_admissions
    WHEN EXISTS (SELECT 1 FROM orchestration_v2_ordinary_checkout_admissions
      WHERE admission_id = NEW.admission_id OR (command_id = NEW.command_id AND thread_id = NEW.thread_id))
    BEGIN SELECT RAISE(ABORT, 'ordinary checkout admission ownership is permanent'); END`;

  yield* sql`CREATE TABLE orchestration_v2_ordinary_checkout_effect_links (
    effect_id TEXT PRIMARY KEY NOT NULL REFERENCES orchestration_v2_effect_outbox(effect_id),
    admission_id TEXT NOT NULL REFERENCES orchestration_v2_ordinary_checkout_admissions(admission_id),
    link_json TEXT NOT NULL CHECK(json_valid(link_json) AND json_type(link_json) = 'object'),
    recorded_at TEXT NOT NULL CHECK(length(recorded_at) > 0)
  )`;
  yield* sql`CREATE INDEX orchestration_v2_ordinary_checkout_effect_links_admission_effect_idx
    ON orchestration_v2_ordinary_checkout_effect_links(admission_id, effect_id)`;
  yield* sql`CREATE TRIGGER orchestration_v2_ordinary_checkout_effect_links_no_update
    BEFORE UPDATE ON orchestration_v2_ordinary_checkout_effect_links
    BEGIN SELECT RAISE(ABORT, 'ordinary checkout effect links are immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_ordinary_checkout_effect_links_no_delete
    BEFORE DELETE ON orchestration_v2_ordinary_checkout_effect_links
    BEGIN SELECT RAISE(ABORT, 'ordinary checkout effect links are permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_ordinary_checkout_effect_links_no_replace
    BEFORE INSERT ON orchestration_v2_ordinary_checkout_effect_links
    WHEN EXISTS (SELECT 1 FROM orchestration_v2_ordinary_checkout_effect_links WHERE effect_id = NEW.effect_id)
    BEGIN SELECT RAISE(ABORT, 'ordinary checkout effect association is permanent'); END`;

  yield* sql`CREATE TABLE orchestration_v2_ordinary_checkout_target_transitions (
    operation_id TEXT PRIMARY KEY NOT NULL REFERENCES orchestration_v2_worktree_path_admissions(operation_id),
    admission_id TEXT NOT NULL REFERENCES orchestration_v2_ordinary_checkout_admissions(admission_id),
    transition_json TEXT NOT NULL CHECK(json_valid(transition_json) AND json_type(transition_json) = 'object'),
    recorded_at TEXT NOT NULL CHECK(length(recorded_at) > 0)
  )`;
  yield* sql`CREATE INDEX orchestration_v2_ordinary_checkout_target_transitions_admission_operation_idx
    ON orchestration_v2_ordinary_checkout_target_transitions(admission_id, operation_id)`;
  yield* sql`CREATE TRIGGER orchestration_v2_ordinary_checkout_target_transitions_no_update
    BEFORE UPDATE ON orchestration_v2_ordinary_checkout_target_transitions
    BEGIN SELECT RAISE(ABORT, 'ordinary checkout target transitions are immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_ordinary_checkout_target_transitions_no_delete
    BEFORE DELETE ON orchestration_v2_ordinary_checkout_target_transitions
    BEGIN SELECT RAISE(ABORT, 'ordinary checkout target transitions are permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_ordinary_checkout_target_transitions_no_replace
    BEFORE INSERT ON orchestration_v2_ordinary_checkout_target_transitions
    WHEN EXISTS (SELECT 1 FROM orchestration_v2_ordinary_checkout_target_transitions WHERE operation_id = NEW.operation_id)
    BEGIN SELECT RAISE(ABORT, 'ordinary checkout target transition is permanent'); END`;

  yield* sql`CREATE TABLE orchestration_v2_ordinary_checkout_commands (
    command_id TEXT NOT NULL REFERENCES orchestration_command_receipts(command_id),
    thread_id TEXT NOT NULL CHECK(length(thread_id) > 0),
    admission_id TEXT NOT NULL REFERENCES orchestration_v2_ordinary_checkout_admissions(admission_id),
    canonical_command_json TEXT NOT NULL CHECK(json_valid(canonical_command_json) AND json_type(canonical_command_json) = 'object'),
    command_digest TEXT NOT NULL CHECK(length(command_digest) = 64 AND command_digest NOT GLOB '*[^0-9a-f]*'),
    recorded_at TEXT NOT NULL CHECK(length(recorded_at) > 0),
    PRIMARY KEY(command_id, thread_id)
  )`;
  yield* sql`CREATE INDEX orchestration_v2_ordinary_checkout_commands_admission_command_idx
    ON orchestration_v2_ordinary_checkout_commands(admission_id, command_id)`;
  yield* sql`CREATE TRIGGER orchestration_v2_ordinary_checkout_commands_no_update
    BEFORE UPDATE ON orchestration_v2_ordinary_checkout_commands
    BEGIN SELECT RAISE(ABORT, 'ordinary checkout commands are immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_ordinary_checkout_commands_no_delete
    BEFORE DELETE ON orchestration_v2_ordinary_checkout_commands
    BEGIN SELECT RAISE(ABORT, 'ordinary checkout commands are permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_ordinary_checkout_commands_no_replace
    BEFORE INSERT ON orchestration_v2_ordinary_checkout_commands
    WHEN EXISTS (SELECT 1 FROM orchestration_v2_ordinary_checkout_commands
      WHERE command_id = NEW.command_id AND thread_id = NEW.thread_id)
    BEGIN SELECT RAISE(ABORT, 'ordinary checkout command association is permanent'); END`;
});
