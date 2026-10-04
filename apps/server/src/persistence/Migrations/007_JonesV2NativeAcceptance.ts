import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE orchestration_v2_native_command_identities (
    command_id TEXT PRIMARY KEY NOT NULL REFERENCES orchestration_command_receipts(command_id),
    kind TEXT NOT NULL CHECK(kind IN ('guarded_message_dispatch', 'native_creation_stage', 'workstream_settlement')),
    version INTEGER NOT NULL CHECK(version = 2),
    command_type TEXT NOT NULL,
    aggregate_kind TEXT NOT NULL CHECK(aggregate_kind = 'thread'),
    aggregate_id TEXT NOT NULL,
    normalized_command_digest TEXT NOT NULL CHECK(length(normalized_command_digest) = 64 AND normalized_command_digest NOT GLOB '*[^0-9a-f]*'),
    binding_digest TEXT NOT NULL CHECK(length(binding_digest) = 64 AND binding_digest NOT GLOB '*[^0-9a-f]*')
  )`;
  yield* sql`CREATE TRIGGER orchestration_v2_native_command_identities_no_update
    BEFORE UPDATE ON orchestration_v2_native_command_identities
    BEGIN SELECT RAISE(ABORT, 'native V2 command identities are immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_native_command_identities_no_delete
    BEFORE DELETE ON orchestration_v2_native_command_identities
    BEGIN SELECT RAISE(ABORT, 'native V2 command identities are permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_native_command_identities_no_replace
    BEFORE INSERT ON orchestration_v2_native_command_identities
    WHEN EXISTS (SELECT 1 FROM orchestration_v2_native_command_identities WHERE command_id = NEW.command_id)
    BEGIN SELECT RAISE(ABORT, 'native V2 command identity ownership is permanent'); END`;

  yield* sql`CREATE TABLE orchestration_v2_unknown_effect_holds (
    effect_id TEXT PRIMARY KEY NOT NULL REFERENCES orchestration_v2_effect_outbox(effect_id),
    worker_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
    expected_attempt INTEGER NOT NULL CHECK(expected_attempt > 0),
    held_at TEXT NOT NULL
  )`;
  yield* sql`CREATE TRIGGER orchestration_v2_unknown_effect_holds_no_update
    BEFORE UPDATE ON orchestration_v2_unknown_effect_holds
    BEGIN SELECT RAISE(ABORT, 'unknown effect holds are immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_unknown_effect_holds_no_delete
    BEFORE DELETE ON orchestration_v2_unknown_effect_holds
    BEGIN SELECT RAISE(ABORT, 'unknown effect holds are permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_unknown_effect_holds_no_replace
    BEFORE INSERT ON orchestration_v2_unknown_effect_holds
    WHEN EXISTS (SELECT 1 FROM orchestration_v2_unknown_effect_holds WHERE effect_id = NEW.effect_id)
    BEGIN SELECT RAISE(ABORT, 'unknown effect hold ownership is permanent'); END`;

  yield* sql`CREATE TABLE orchestration_v2_provider_runtime_evidence (
    thread_id TEXT PRIMARY KEY NOT NULL,
    provider_thread_id TEXT NOT NULL,
    provider_session_id TEXT NOT NULL,
    provider_instance_id TEXT NOT NULL,
    driver TEXT NOT NULL,
    native_thread_id TEXT,
    runtime_generation TEXT NOT NULL,
    evidence_revision INTEGER NOT NULL CHECK(evidence_revision > 0),
    observation_json TEXT CHECK(observation_json IS NULL OR json_valid(observation_json)),
    registered_at TEXT NOT NULL
  )`;

  yield* sql`CREATE TABLE orchestration_v2_legacy_continuation_dispositions (
    thread_id TEXT PRIMARY KEY NOT NULL,
    provenance TEXT NOT NULL CHECK(provenance IN ('legacy_row', 'native_import')),
    qualification_json TEXT NOT NULL CHECK(json_valid(qualification_json)),
    evidence_json TEXT CHECK(evidence_json IS NULL OR json_valid(evidence_json)),
    imported_at TEXT NOT NULL
  )`;
  yield* sql`CREATE TRIGGER orchestration_v2_legacy_continuation_dispositions_no_update
    BEFORE UPDATE ON orchestration_v2_legacy_continuation_dispositions
    BEGIN SELECT RAISE(ABORT, 'legacy continuation dispositions are immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_legacy_continuation_dispositions_no_delete
    BEFORE DELETE ON orchestration_v2_legacy_continuation_dispositions
    BEGIN SELECT RAISE(ABORT, 'legacy continuation dispositions are permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_legacy_continuation_dispositions_no_replace
    BEFORE INSERT ON orchestration_v2_legacy_continuation_dispositions
    WHEN EXISTS (SELECT 1 FROM orchestration_v2_legacy_continuation_dispositions WHERE thread_id = NEW.thread_id)
    BEGIN SELECT RAISE(ABORT, 'legacy continuation disposition ownership is permanent'); END`;

  yield* sql`CREATE TABLE orchestration_v2_restart_continuation_markers (
    marker_id TEXT PRIMARY KEY NOT NULL,
    thread_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    source_run_id TEXT NOT NULL,
    source_run_attempt_id TEXT NOT NULL,
    binding_json TEXT NOT NULL CHECK(json_valid(binding_json)),
    evidence_revision INTEGER NOT NULL CHECK(evidence_revision > 0),
    status TEXT NOT NULL CHECK(status IN ('dormant', 'released', 'cleared')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    effect_id TEXT REFERENCES orchestration_v2_effect_outbox(effect_id),
    CHECK((status = 'released' AND effect_id IS NOT NULL)
      OR (status IN ('dormant', 'cleared') AND effect_id IS NULL))
  )`;
  yield* sql`CREATE UNIQUE INDEX orchestration_v2_restart_continuation_markers_dormant_thread_idx
    ON orchestration_v2_restart_continuation_markers(thread_id) WHERE status = 'dormant'`;
  yield* sql`CREATE INDEX orchestration_v2_restart_continuation_markers_status_thread_created_idx
    ON orchestration_v2_restart_continuation_markers(status, thread_id, created_at)`;
  yield* sql`CREATE TRIGGER orchestration_v2_restart_continuation_markers_immutable
    BEFORE UPDATE ON orchestration_v2_restart_continuation_markers
    WHEN OLD.marker_id IS NOT NEW.marker_id OR OLD.thread_id IS NOT NEW.thread_id
      OR OLD.project_id IS NOT NEW.project_id OR OLD.source_run_id IS NOT NEW.source_run_id
      OR OLD.source_run_attempt_id IS NOT NEW.source_run_attempt_id
      OR OLD.binding_json IS NOT NEW.binding_json OR OLD.evidence_revision IS NOT NEW.evidence_revision
      OR OLD.created_at IS NOT NEW.created_at OR OLD.status != 'dormant'
      OR NEW.status NOT IN ('released', 'cleared')
    BEGIN SELECT RAISE(ABORT, 'restart continuation marker identity and terminal state are immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_restart_continuation_markers_no_delete
    BEFORE DELETE ON orchestration_v2_restart_continuation_markers
    BEGIN SELECT RAISE(ABORT, 'restart continuation markers are permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_restart_continuation_markers_no_replace
    BEFORE INSERT ON orchestration_v2_restart_continuation_markers
    WHEN EXISTS (SELECT 1 FROM orchestration_v2_restart_continuation_markers
      WHERE marker_id = NEW.marker_id OR (thread_id = NEW.thread_id AND status = 'dormant' AND NEW.status = 'dormant'))
    BEGIN SELECT RAISE(ABORT, 'restart continuation marker ownership is permanent'); END`;

  yield* sql`CREATE TABLE orchestration_v2_provider_continuation_sources (
    source_id TEXT PRIMARY KEY NOT NULL CHECK(length(source_id) = 64 AND source_id NOT GLOB '*[^0-9a-f]*'),
    thread_id TEXT NOT NULL CHECK(length(thread_id) > 0),
    provider_thread_id TEXT NOT NULL CHECK(length(provider_thread_id) > 0),
    provider_session_id TEXT NOT NULL CHECK(length(provider_session_id) > 0),
    provider_instance_id TEXT NOT NULL CHECK(length(provider_instance_id) > 0),
    driver TEXT NOT NULL CHECK(length(driver) > 0),
    native_thread_id TEXT NOT NULL CHECK(length(native_thread_id) > 0),
    runtime_generation TEXT NOT NULL CHECK(length(trim(runtime_generation)) > 0),
    continuation_key TEXT NOT NULL CHECK(length(trim(continuation_key)) > 0),
    registered_at TEXT NOT NULL,
    UNIQUE(thread_id, provider_thread_id, provider_session_id, provider_instance_id, driver, native_thread_id, runtime_generation)
  )`;
  yield* sql`CREATE INDEX orchestration_v2_provider_continuation_sources_thread_binding_idx
    ON orchestration_v2_provider_continuation_sources(thread_id, provider_thread_id, runtime_generation)`;
  yield* sql`CREATE TRIGGER orchestration_v2_provider_continuation_sources_no_update
    BEFORE UPDATE ON orchestration_v2_provider_continuation_sources
    BEGIN SELECT RAISE(ABORT, 'provider continuation source identities are immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_provider_continuation_sources_no_delete
    BEFORE DELETE ON orchestration_v2_provider_continuation_sources
    BEGIN SELECT RAISE(ABORT, 'provider continuation source identities are permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_provider_continuation_sources_no_replace
    BEFORE INSERT ON orchestration_v2_provider_continuation_sources
    WHEN EXISTS (SELECT 1 FROM orchestration_v2_provider_continuation_sources
      WHERE source_id = NEW.source_id OR (thread_id = NEW.thread_id AND provider_thread_id = NEW.provider_thread_id
        AND provider_session_id = NEW.provider_session_id AND provider_instance_id = NEW.provider_instance_id
        AND driver = NEW.driver AND native_thread_id = NEW.native_thread_id AND runtime_generation = NEW.runtime_generation))
    BEGIN SELECT RAISE(ABORT, 'provider continuation source identity ownership is permanent'); END`;

  yield* sql`CREATE TABLE orchestration_v2_native_import_transcript_seals (
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    thread_id TEXT PRIMARY KEY NOT NULL REFERENCES orchestration_v2_legacy_continuation_dispositions(thread_id),
    project_id TEXT NOT NULL CHECK(length(project_id) > 0),
    provenance TEXT NOT NULL CHECK(provenance = 'native_import'),
    source_identity_json TEXT NOT NULL CHECK(json_valid(source_identity_json)),
    parser_policy TEXT NOT NULL CHECK(parser_policy = 'agent_session_visible_messages_v1'),
    message_count INTEGER NOT NULL CHECK(message_count BETWEEN 1 AND 200),
    events_sha256 TEXT NOT NULL CHECK(length(events_sha256) = 64 AND events_sha256 NOT GLOB '*[^0-9a-f]*'),
    event_basis_json TEXT NOT NULL CHECK(json_valid(event_basis_json) AND json_type(event_basis_json) = 'array'
      AND json_array_length(event_basis_json) = message_count * 2),
    birth_event_id TEXT NOT NULL REFERENCES orchestration_events(event_id),
    birth_sequence INTEGER NOT NULL CHECK(birth_sequence > 0) REFERENCES orchestration_events(sequence),
    imported_at TEXT NOT NULL CHECK(length(imported_at) > 0)
  )`;
  yield* sql`CREATE TRIGGER orchestration_v2_native_import_transcript_seals_no_update
    BEFORE UPDATE ON orchestration_v2_native_import_transcript_seals
    BEGIN SELECT RAISE(ABORT, 'native import transcript seals are immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_native_import_transcript_seals_no_delete
    BEFORE DELETE ON orchestration_v2_native_import_transcript_seals
    BEGIN SELECT RAISE(ABORT, 'native import transcript seals are permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_native_import_transcript_seals_no_replace
    BEFORE INSERT ON orchestration_v2_native_import_transcript_seals
    WHEN EXISTS (SELECT 1 FROM orchestration_v2_native_import_transcript_seals WHERE thread_id = NEW.thread_id)
    BEGIN SELECT RAISE(ABORT, 'native import transcript seal ownership is permanent'); END`;

  yield* sql`CREATE TABLE orchestration_v2_native_effect_confirmations (
    effect_id TEXT PRIMARY KEY NOT NULL REFERENCES orchestration_v2_effect_outbox(effect_id),
    command_id TEXT NOT NULL CHECK(length(command_id) > 0),
    thread_id TEXT NOT NULL CHECK(length(thread_id) > 0),
    worker_id TEXT NOT NULL CHECK(length(worker_id) > 0),
    operation_id TEXT NOT NULL CHECK(length(operation_id) > 0),
    run_id TEXT NOT NULL CHECK(length(run_id) > 0),
    run_attempt_id TEXT NOT NULL CHECK(length(run_attempt_id) > 0),
    expected_attempt INTEGER NOT NULL CHECK(expected_attempt > 0),
    binding_json TEXT NOT NULL CHECK(json_valid(binding_json)),
    evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
    evidence_revision INTEGER NOT NULL CHECK(evidence_revision > 0),
    native_execution_reference_json TEXT CHECK(native_execution_reference_json IS NULL OR json_valid(native_execution_reference_json)),
    command_event_id TEXT NOT NULL REFERENCES orchestration_events(event_id),
    command_event_sequence INTEGER NOT NULL CHECK(command_event_sequence > 0) REFERENCES orchestration_events(sequence),
    confirmed_at TEXT NOT NULL CHECK(length(confirmed_at) > 0)
  )`;
  yield* sql`CREATE INDEX orchestration_v2_native_effect_confirmations_thread_command_idx
    ON orchestration_v2_native_effect_confirmations(thread_id, command_id)`;
  yield* sql`CREATE TRIGGER orchestration_v2_native_effect_confirmations_no_update
    BEFORE UPDATE ON orchestration_v2_native_effect_confirmations
    BEGIN SELECT RAISE(ABORT, 'native effect confirmations are immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_native_effect_confirmations_no_delete
    BEFORE DELETE ON orchestration_v2_native_effect_confirmations
    BEGIN SELECT RAISE(ABORT, 'native effect confirmations are permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_native_effect_confirmations_no_replace
    BEFORE INSERT ON orchestration_v2_native_effect_confirmations
    WHEN EXISTS (SELECT 1 FROM orchestration_v2_native_effect_confirmations WHERE effect_id = NEW.effect_id)
    BEGIN SELECT RAISE(ABORT, 'native effect confirmation ownership is permanent'); END`;

  yield* sql`CREATE TABLE orchestration_v2_imported_history_start_choices (
    command_id TEXT PRIMARY KEY NOT NULL,
    command_type TEXT NOT NULL CHECK(command_type = 'thread.imported-history.start'),
    thread_id TEXT NOT NULL CHECK(length(thread_id) > 0),
    actor_session_id TEXT NOT NULL CHECK(length(actor_session_id) > 0),
    reviewed_basis TEXT NOT NULL CHECK(length(reviewed_basis) > 0),
    reserved_at TEXT NOT NULL CHECK(length(reserved_at) > 0),
    command_digest TEXT NOT NULL CHECK(length(command_digest) = 64 AND command_digest NOT GLOB '*[^0-9a-f]*'),
    canonical_command_json TEXT NOT NULL CHECK(json_valid(canonical_command_json)),
    basis_json TEXT NOT NULL CHECK(json_valid(basis_json))
  )`;
  yield* sql`CREATE INDEX orchestration_v2_imported_history_start_choices_thread_command_idx
    ON orchestration_v2_imported_history_start_choices(thread_id, command_id)`;
  yield* sql`CREATE TRIGGER orchestration_v2_imported_history_start_choices_no_update
    BEFORE UPDATE ON orchestration_v2_imported_history_start_choices
    BEGIN SELECT RAISE(ABORT, 'imported history start choices are immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_imported_history_start_choices_no_delete
    BEFORE DELETE ON orchestration_v2_imported_history_start_choices
    BEGIN SELECT RAISE(ABORT, 'imported history start choices are permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_imported_history_start_choices_no_replace
    BEFORE INSERT ON orchestration_v2_imported_history_start_choices
    WHEN EXISTS (SELECT 1 FROM orchestration_v2_imported_history_start_choices WHERE command_id = NEW.command_id)
    BEGIN SELECT RAISE(ABORT, 'imported history start choice ownership is permanent'); END`;

  yield* sql`CREATE TABLE orchestration_v2_imported_history_start_outcomes (
    command_id TEXT PRIMARY KEY NOT NULL,
    intent_status TEXT NOT NULL CHECK(intent_status IN ('accepted', 'rejected')),
    run_id TEXT,
    message_id TEXT,
    effect_id TEXT REFERENCES orchestration_v2_effect_outbox(effect_id),
    outcome_json TEXT NOT NULL CHECK(json_valid(outcome_json)),
    recorded_at TEXT NOT NULL CHECK(length(recorded_at) > 0),
    FOREIGN KEY(command_id) REFERENCES orchestration_v2_imported_history_start_choices(command_id),
    FOREIGN KEY(command_id) REFERENCES orchestration_command_receipts(command_id),
    CHECK((intent_status = 'accepted' AND run_id IS NOT NULL AND length(run_id) > 0
      AND message_id IS NOT NULL AND length(message_id) > 0)
      OR (intent_status = 'rejected' AND run_id IS NULL AND message_id IS NULL AND effect_id IS NULL)),
    CHECK((run_id IS NULL AND message_id IS NULL) OR (run_id IS NOT NULL AND message_id IS NOT NULL))
  )`;
  yield* sql`CREATE TRIGGER orchestration_v2_imported_history_start_outcomes_no_update
    BEFORE UPDATE ON orchestration_v2_imported_history_start_outcomes
    BEGIN SELECT RAISE(ABORT, 'imported history start outcomes are immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_imported_history_start_outcomes_no_delete
    BEFORE DELETE ON orchestration_v2_imported_history_start_outcomes
    BEGIN SELECT RAISE(ABORT, 'imported history start outcomes are permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_imported_history_start_outcomes_no_replace
    BEFORE INSERT ON orchestration_v2_imported_history_start_outcomes
    WHEN EXISTS (SELECT 1 FROM orchestration_v2_imported_history_start_outcomes WHERE command_id = NEW.command_id)
    BEGIN SELECT RAISE(ABORT, 'imported history start outcome ownership is permanent'); END`;
  yield* sql`CREATE TABLE orchestration_v2_current_runtime_stop_intents (
    command_id TEXT PRIMARY KEY NOT NULL REFERENCES orchestration_command_receipts(command_id),
    thread_id TEXT NOT NULL CHECK(length(thread_id) > 0),
    application_birth_json TEXT NOT NULL CHECK(json_valid(application_birth_json) AND json_type(application_birth_json) = 'object'),
    canonical_request_digest TEXT NOT NULL CHECK(length(canonical_request_digest) = 64 AND canonical_request_digest NOT GLOB '*[^0-9a-f]*'),
    actor_binding_digest TEXT NOT NULL CHECK(length(actor_binding_digest) = 64 AND actor_binding_digest NOT GLOB '*[^0-9a-f]*'),
    target_binding_json TEXT NOT NULL CHECK(json_valid(target_binding_json) AND json_type(target_binding_json) = 'object'),
    target_evidence_revision INTEGER NOT NULL CHECK(target_evidence_revision > 0),
    stop_event_id TEXT NOT NULL REFERENCES orchestration_events(event_id),
    stop_event_sequence INTEGER NOT NULL CHECK(stop_event_sequence > 0) REFERENCES orchestration_events(sequence),
    affected_run_ids_json TEXT NOT NULL CHECK(json_valid(affected_run_ids_json) AND json_type(affected_run_ids_json) = 'array'),
    queued_bases_json TEXT NOT NULL CHECK(json_valid(queued_bases_json) AND json_type(queued_bases_json) = 'array'),
    accepted_at TEXT NOT NULL CHECK(length(accepted_at) > 0)
  )`;
  yield* sql`CREATE INDEX orchestration_v2_current_runtime_stop_intents_thread_command_idx
    ON orchestration_v2_current_runtime_stop_intents(thread_id, command_id)`;
  yield* sql`CREATE TRIGGER orchestration_v2_current_runtime_stop_intents_no_update
    BEFORE UPDATE ON orchestration_v2_current_runtime_stop_intents
    BEGIN SELECT RAISE(ABORT, 'current runtime stop intents are immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_current_runtime_stop_intents_no_delete
    BEFORE DELETE ON orchestration_v2_current_runtime_stop_intents
    BEGIN SELECT RAISE(ABORT, 'current runtime stop intents are permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_current_runtime_stop_intents_no_replace
    BEFORE INSERT ON orchestration_v2_current_runtime_stop_intents
    WHEN EXISTS (SELECT 1 FROM orchestration_v2_current_runtime_stop_intents WHERE command_id = NEW.command_id)
    BEGIN SELECT RAISE(ABORT, 'current runtime stop intent ownership is permanent'); END`;
  yield* sql`CREATE TABLE orchestration_v2_queued_runtime_stop_fences (
    stop_command_id TEXT NOT NULL REFERENCES orchestration_v2_current_runtime_stop_intents(command_id),
    run_id TEXT NOT NULL CHECK(length(run_id) > 0),
    thread_id TEXT NOT NULL CHECK(length(thread_id) > 0),
    application_birth_json TEXT NOT NULL CHECK(json_valid(application_birth_json) AND json_type(application_birth_json) = 'object'),
    queued_provider_thread_id TEXT NOT NULL CHECK(length(queued_provider_thread_id) > 0),
    source_binding_json TEXT NOT NULL CHECK(json_valid(source_binding_json) AND json_type(source_binding_json) = 'object'),
    source_evidence_revision INTEGER NOT NULL CHECK(source_evidence_revision > 0),
    switch_plan_json TEXT CHECK(switch_plan_json IS NULL OR json_valid(switch_plan_json)),
    source_mode TEXT NOT NULL CHECK(source_mode IN ('queued_thread', 'active_native_copy')),
    execution_intent_json TEXT NOT NULL CHECK(json_valid(execution_intent_json) AND json_type(execution_intent_json) = 'object'),
    basis_digest TEXT NOT NULL CHECK(length(basis_digest) = 64 AND basis_digest NOT GLOB '*[^0-9a-f]*'),
    PRIMARY KEY(stop_command_id, run_id)
  )`;
  yield* sql`CREATE INDEX orchestration_v2_queued_runtime_stop_fences_thread_run_idx
    ON orchestration_v2_queued_runtime_stop_fences(thread_id, run_id)`;
  yield* sql`CREATE TRIGGER orchestration_v2_queued_runtime_stop_fences_no_update
    BEFORE UPDATE ON orchestration_v2_queued_runtime_stop_fences
    BEGIN SELECT RAISE(ABORT, 'queued runtime stop fences are immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_queued_runtime_stop_fences_no_delete
    BEFORE DELETE ON orchestration_v2_queued_runtime_stop_fences
    BEGIN SELECT RAISE(ABORT, 'queued runtime stop fences are permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_queued_runtime_stop_fences_no_replace
    BEFORE INSERT ON orchestration_v2_queued_runtime_stop_fences
    WHEN EXISTS (SELECT 1 FROM orchestration_v2_queued_runtime_stop_fences WHERE stop_command_id = NEW.stop_command_id AND run_id = NEW.run_id)
    BEGIN SELECT RAISE(ABORT, 'queued runtime stop fence ownership is permanent'); END`;
  yield* sql`CREATE TABLE orchestration_v2_queued_start_reservations (
    effect_id TEXT PRIMARY KEY NOT NULL REFERENCES orchestration_v2_effect_outbox(effect_id),
    command_id TEXT NOT NULL CHECK(length(command_id) > 0),
    thread_id TEXT NOT NULL CHECK(length(thread_id) > 0),
    run_id TEXT NOT NULL CHECK(length(run_id) > 0),
    run_attempt_id TEXT NOT NULL CHECK(length(run_attempt_id) > 0),
    application_birth_json TEXT NOT NULL CHECK(json_valid(application_birth_json) AND json_type(application_birth_json) = 'object'),
    execution_intent_json TEXT NOT NULL CHECK(json_valid(execution_intent_json) AND json_type(execution_intent_json) = 'object'),
    basis_json TEXT NOT NULL CHECK(json_valid(basis_json) AND json_type(basis_json) = 'object'),
    basis_digest TEXT NOT NULL CHECK(length(basis_digest) = 64 AND basis_digest NOT GLOB '*[^0-9a-f]*'),
    reserved_at TEXT NOT NULL CHECK(length(reserved_at) > 0)
  )`;
  yield* sql`CREATE INDEX orchestration_v2_queued_start_reservations_thread_run_idx
    ON orchestration_v2_queued_start_reservations(thread_id, run_id)`;
  yield* sql`CREATE TRIGGER orchestration_v2_queued_start_reservations_no_update
    BEFORE UPDATE ON orchestration_v2_queued_start_reservations
    BEGIN SELECT RAISE(ABORT, 'queued start reservations are immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_queued_start_reservations_no_delete
    BEFORE DELETE ON orchestration_v2_queued_start_reservations
    BEGIN SELECT RAISE(ABORT, 'queued start reservations are permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_queued_start_reservations_no_replace
    BEFORE INSERT ON orchestration_v2_queued_start_reservations
    WHEN EXISTS (SELECT 1 FROM orchestration_v2_queued_start_reservations WHERE effect_id = NEW.effect_id)
    BEGIN SELECT RAISE(ABORT, 'queued start reservation ownership is permanent'); END`;
  yield* sql`CREATE TABLE orchestration_v2_workstream_settlement_witnesses (
    command_id TEXT PRIMARY KEY NOT NULL REFERENCES orchestration_command_receipts(command_id),
    thread_id TEXT NOT NULL CHECK(length(thread_id) > 0),
    witness_json TEXT NOT NULL CHECK(json_valid(witness_json) AND json_type(witness_json) = 'object'),
    recorded_at TEXT NOT NULL CHECK(length(recorded_at) > 0)
  )`;
  yield* sql`CREATE INDEX orchestration_v2_workstream_settlement_witnesses_thread_command_idx
    ON orchestration_v2_workstream_settlement_witnesses(thread_id, command_id)`;
  yield* sql`CREATE TRIGGER orchestration_v2_workstream_settlement_witnesses_no_update
    BEFORE UPDATE ON orchestration_v2_workstream_settlement_witnesses
    BEGIN SELECT RAISE(ABORT, 'workstream settlement witnesses are immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_workstream_settlement_witnesses_no_delete
    BEFORE DELETE ON orchestration_v2_workstream_settlement_witnesses
    BEGIN SELECT RAISE(ABORT, 'workstream settlement witnesses are permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_workstream_settlement_witnesses_no_replace
    BEFORE INSERT ON orchestration_v2_workstream_settlement_witnesses
    WHEN EXISTS (SELECT 1 FROM orchestration_v2_workstream_settlement_witnesses WHERE command_id = NEW.command_id)
    BEGIN SELECT RAISE(ABORT, 'workstream settlement witness ownership is permanent'); END`;
  yield* sql`CREATE TABLE orchestration_v2_captured_restart_command_origins (
    command_id TEXT PRIMARY KEY NOT NULL REFERENCES orchestration_command_receipts(command_id),
    thread_id TEXT NOT NULL CHECK(length(thread_id) > 0),
    effect_id TEXT NOT NULL REFERENCES orchestration_v2_effect_outbox(effect_id),
    marker_json TEXT NOT NULL CHECK(json_valid(marker_json) AND json_type(marker_json) = 'object'),
    canonical_command_json TEXT NOT NULL CHECK(json_valid(canonical_command_json) AND json_type(canonical_command_json) = 'object'),
    command_digest TEXT NOT NULL CHECK(length(command_digest) = 64 AND command_digest NOT GLOB '*[^0-9a-f]*'),
    original_claim_json TEXT NOT NULL CHECK(json_valid(original_claim_json) AND json_type(original_claim_json) = 'object'),
    recorded_at TEXT NOT NULL CHECK(length(recorded_at) > 0)
  )`;
  yield* sql`CREATE INDEX orchestration_v2_captured_restart_command_origins_thread_command_idx
    ON orchestration_v2_captured_restart_command_origins(thread_id, command_id)`;
  yield* sql`CREATE TRIGGER orchestration_v2_captured_restart_command_origins_no_update
    BEFORE UPDATE ON orchestration_v2_captured_restart_command_origins
    BEGIN SELECT RAISE(ABORT, 'captured restart command origins are immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_captured_restart_command_origins_no_delete
    BEFORE DELETE ON orchestration_v2_captured_restart_command_origins
    BEGIN SELECT RAISE(ABORT, 'captured restart command origins are permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_captured_restart_command_origins_no_replace
    BEFORE INSERT ON orchestration_v2_captured_restart_command_origins
    WHEN EXISTS (SELECT 1 FROM orchestration_v2_captured_restart_command_origins WHERE command_id = NEW.command_id)
    BEGIN SELECT RAISE(ABORT, 'captured restart command origin ownership is permanent'); END`;
  yield* sql`CREATE TABLE native_creation_thread_recovery_commands (
    command_id TEXT PRIMARY KEY NOT NULL CHECK(length(command_id) > 0),
    claim_id TEXT NOT NULL UNIQUE REFERENCES native_creation_intents(claim_id),
    thread_id TEXT NOT NULL CHECK(length(thread_id) > 0),
    command_digest TEXT NOT NULL CHECK(length(command_digest) = 64 AND command_digest NOT GLOB '*[^0-9a-f]*'),
    recovery_json TEXT NOT NULL CHECK(json_valid(recovery_json) AND json_type(recovery_json) = 'object'),
    reserved_at TEXT NOT NULL CHECK(length(reserved_at) > 0)
  )`;
  yield* sql`CREATE INDEX native_creation_thread_recovery_commands_thread_command_idx
    ON native_creation_thread_recovery_commands(thread_id, command_id)`;
  yield* sql`CREATE TRIGGER native_creation_thread_recovery_commands_no_update
    BEFORE UPDATE ON native_creation_thread_recovery_commands
    BEGIN SELECT RAISE(ABORT, 'native thread recovery commands are immutable'); END`;
  yield* sql`CREATE TRIGGER native_creation_thread_recovery_commands_no_delete
    BEFORE DELETE ON native_creation_thread_recovery_commands
    BEGIN SELECT RAISE(ABORT, 'native thread recovery commands are permanent'); END`;
  yield* sql`CREATE TRIGGER native_creation_thread_recovery_commands_no_replace
    BEFORE INSERT ON native_creation_thread_recovery_commands
    WHEN EXISTS (SELECT 1 FROM native_creation_thread_recovery_commands WHERE command_id = NEW.command_id OR claim_id = NEW.claim_id)
    BEGIN SELECT RAISE(ABORT, 'native thread recovery command ownership is permanent'); END`;
  yield* sql`CREATE TABLE orchestration_v2_lease_cleanup_task_bindings (
    effect_id TEXT PRIMARY KEY NOT NULL REFERENCES orchestration_v2_effect_outbox(effect_id),
    thread_id TEXT NOT NULL CHECK(length(thread_id) > 0),
    lease_json TEXT NOT NULL CHECK(json_valid(lease_json) AND json_type(lease_json) = 'object'),
    owner_birth_json TEXT NOT NULL CHECK(json_valid(owner_birth_json) AND json_type(owner_birth_json) = 'object'),
    deletion_json TEXT NOT NULL CHECK(json_valid(deletion_json) AND json_type(deletion_json) = 'object'),
    task_json TEXT NOT NULL CHECK(json_valid(task_json) AND json_type(task_json) = 'object'),
    binding_sha256 TEXT NOT NULL CHECK(length(binding_sha256) = 64 AND binding_sha256 NOT GLOB '*[^0-9a-f]*'),
    recorded_at TEXT NOT NULL CHECK(length(recorded_at) > 0)
  )`;
  yield* sql`CREATE INDEX orchestration_v2_lease_cleanup_task_bindings_thread_effect_idx
    ON orchestration_v2_lease_cleanup_task_bindings(thread_id, effect_id)`;
  yield* sql`CREATE TRIGGER orchestration_v2_lease_cleanup_task_bindings_no_update
    BEFORE UPDATE ON orchestration_v2_lease_cleanup_task_bindings
    BEGIN SELECT RAISE(ABORT, 'lease cleanup task bindings are immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_lease_cleanup_task_bindings_no_delete
    BEFORE DELETE ON orchestration_v2_lease_cleanup_task_bindings
    BEGIN SELECT RAISE(ABORT, 'lease cleanup task bindings are permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_lease_cleanup_task_bindings_no_replace
    BEFORE INSERT ON orchestration_v2_lease_cleanup_task_bindings
    WHEN EXISTS (SELECT 1 FROM orchestration_v2_lease_cleanup_task_bindings WHERE effect_id = NEW.effect_id)
    BEGIN SELECT RAISE(ABORT, 'lease cleanup task binding ownership is permanent'); END`;
  yield* sql`CREATE TABLE orchestration_v2_lease_cleanup_task_outcomes (
    effect_id TEXT NOT NULL REFERENCES orchestration_v2_lease_cleanup_task_bindings(effect_id),
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
    outcome_json TEXT NOT NULL CHECK(json_valid(outcome_json) AND json_type(outcome_json) = 'object'),
    correlation_json TEXT NOT NULL CHECK(json_valid(correlation_json) AND json_type(correlation_json) = 'object'),
    recorded_at TEXT NOT NULL CHECK(length(recorded_at) > 0),
    PRIMARY KEY(effect_id, ordinal)
  )`;
  yield* sql`CREATE TRIGGER orchestration_v2_lease_cleanup_task_outcomes_no_update
    BEFORE UPDATE ON orchestration_v2_lease_cleanup_task_outcomes
    BEGIN SELECT RAISE(ABORT, 'lease cleanup task outcomes are immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_lease_cleanup_task_outcomes_no_delete
    BEFORE DELETE ON orchestration_v2_lease_cleanup_task_outcomes
    BEGIN SELECT RAISE(ABORT, 'lease cleanup task outcomes are permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_lease_cleanup_task_outcomes_no_replace
    BEFORE INSERT ON orchestration_v2_lease_cleanup_task_outcomes
    WHEN EXISTS (SELECT 1 FROM orchestration_v2_lease_cleanup_task_outcomes WHERE effect_id = NEW.effect_id AND ordinal = NEW.ordinal)
    BEGIN SELECT RAISE(ABORT, 'lease cleanup task outcome ownership is permanent'); END`;
});
