import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`CREATE TABLE orchestration_v2_thread_deletion_commands (
    command_id TEXT PRIMARY KEY NOT NULL REFERENCES orchestration_command_receipts(command_id),
    thread_id TEXT NOT NULL CHECK(length(thread_id) > 0),
    canonical_command_json TEXT NOT NULL CHECK(json_valid(canonical_command_json) AND json_type(canonical_command_json) = 'object'),
    command_digest TEXT NOT NULL CHECK(length(command_digest) = 64 AND command_digest NOT GLOB '*[^0-9a-f]*'),
    owner_birth_json TEXT NOT NULL CHECK(json_valid(owner_birth_json) AND json_type(owner_birth_json) = 'object'),
    worktree_inventory_json TEXT NOT NULL CHECK(json_valid(worktree_inventory_json) AND json_type(worktree_inventory_json) = 'object'),
    deletion_event_id TEXT NOT NULL REFERENCES orchestration_events(event_id),
    deletion_event_sequence INTEGER NOT NULL CHECK(deletion_event_sequence > 0) REFERENCES orchestration_events(sequence),
    recorded_at TEXT NOT NULL CHECK(length(recorded_at) > 0)
  )`;
  yield* sql`CREATE INDEX orchestration_v2_thread_deletion_commands_thread_command_idx
    ON orchestration_v2_thread_deletion_commands(thread_id, command_id)`;
  yield* sql`CREATE TRIGGER orchestration_v2_thread_deletion_commands_no_update
    BEFORE UPDATE ON orchestration_v2_thread_deletion_commands
    BEGIN SELECT RAISE(ABORT, 'thread deletion commands are immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_thread_deletion_commands_no_delete
    BEFORE DELETE ON orchestration_v2_thread_deletion_commands
    BEGIN SELECT RAISE(ABORT, 'thread deletion commands are permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_thread_deletion_commands_no_replace
    BEFORE INSERT ON orchestration_v2_thread_deletion_commands
    WHEN EXISTS (SELECT 1 FROM orchestration_v2_thread_deletion_commands WHERE command_id = NEW.command_id)
    BEGIN SELECT RAISE(ABORT, 'thread deletion command ownership is permanent'); END`;

  yield* sql`CREATE TABLE orchestration_v2_worktree_path_admissions (
    operation_id TEXT PRIMARY KEY NOT NULL CHECK(length(operation_id) > 0),
    canonical_path TEXT NOT NULL CHECK(length(canonical_path) > 0),
    kind TEXT NOT NULL CHECK(kind IN ('worktree_removal', 'native_operation')),
    subject_json TEXT NOT NULL CHECK(json_valid(subject_json) AND json_type(subject_json) = 'object'),
    state TEXT NOT NULL CHECK(state IN ('reserved', 'started', 'unknown', 'completed', 'no_effect', 'released')),
    started_at TEXT,
    outcome_json TEXT CHECK(outcome_json IS NULL OR (json_valid(outcome_json) AND json_type(outcome_json) = 'object')),
    recorded_at TEXT NOT NULL CHECK(length(recorded_at) > 0),
    updated_at TEXT NOT NULL CHECK(length(updated_at) > 0),
    CHECK((state = 'reserved' AND started_at IS NULL AND outcome_json IS NULL)
      OR (state = 'started' AND started_at IS NOT NULL AND outcome_json IS NULL)
      OR (state = 'completed' AND started_at IS NOT NULL AND outcome_json IS NOT NULL)
      OR (state = 'unknown' AND outcome_json IS NOT NULL)
      OR (state IN ('no_effect', 'released') AND outcome_json IS NOT NULL))
  )`;
  yield* sql`CREATE UNIQUE INDEX orchestration_v2_worktree_path_admissions_active_path_idx
    ON orchestration_v2_worktree_path_admissions(canonical_path) WHERE state NOT IN ('no_effect', 'released')`;
  yield* sql`CREATE INDEX orchestration_v2_worktree_path_admissions_path_state_idx
    ON orchestration_v2_worktree_path_admissions(canonical_path, state)`;
  yield* sql`CREATE TRIGGER orchestration_v2_worktree_path_admissions_identity_immutable
    BEFORE UPDATE ON orchestration_v2_worktree_path_admissions
    WHEN NEW.operation_id IS NOT OLD.operation_id OR NEW.canonical_path IS NOT OLD.canonical_path
      OR NEW.kind IS NOT OLD.kind OR NEW.subject_json IS NOT OLD.subject_json OR NEW.recorded_at IS NOT OLD.recorded_at
    BEGIN SELECT RAISE(ABORT, 'worktree path admission identity is immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_worktree_path_admissions_transition_guard
    BEFORE UPDATE ON orchestration_v2_worktree_path_admissions
    WHEN NOT ((OLD.state = 'reserved' AND NEW.state IN ('started', 'unknown', 'no_effect'))
      OR (OLD.state = 'started' AND NEW.state IN ('unknown', 'completed'))
      OR (OLD.state = 'unknown' AND NEW.state IN ('completed', 'no_effect'))
      OR (OLD.state = 'completed' AND NEW.state = 'released'))
    BEGIN SELECT RAISE(ABORT, 'worktree path admission transition is not allowed'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_worktree_path_admissions_start_immutable
    BEFORE UPDATE ON orchestration_v2_worktree_path_admissions
    WHEN OLD.started_at IS NOT NULL AND NEW.started_at IS NOT OLD.started_at
    BEGIN SELECT RAISE(ABORT, 'worktree path admission native start is immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_worktree_path_admissions_no_delete
    BEFORE DELETE ON orchestration_v2_worktree_path_admissions
    BEGIN SELECT RAISE(ABORT, 'worktree path admissions are permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_worktree_path_admissions_no_replace
    BEFORE INSERT ON orchestration_v2_worktree_path_admissions
    WHEN EXISTS (SELECT 1 FROM orchestration_v2_worktree_path_admissions
      WHERE operation_id = NEW.operation_id OR (canonical_path = NEW.canonical_path
        AND state NOT IN ('no_effect', 'released') AND NEW.state NOT IN ('no_effect', 'released')))
    BEGIN SELECT RAISE(ABORT, 'worktree path admission ownership is permanent'); END`;
});
