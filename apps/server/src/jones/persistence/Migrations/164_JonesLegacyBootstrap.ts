import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

// Root allocates/registers this migration in the independent Jones loader.
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_legacy_bootstrap_threads (
    thread_id TEXT PRIMARY KEY NOT NULL CHECK(length(thread_id) > 0),
    identity_json TEXT NOT NULL CHECK(json_valid(identity_json)),
    revision INTEGER NOT NULL CHECK(revision >= 0),
    metadata_json TEXT NOT NULL CHECK(json_valid(metadata_json))
  )`;
  yield* sql`CREATE TABLE jones_legacy_bootstrap_runs (
    run_id TEXT PRIMARY KEY NOT NULL CHECK(length(run_id) > 0),
    thread_id TEXT NOT NULL CHECK(length(thread_id) > 0),
    metadata_event_id TEXT NOT NULL, metadata_sequence INTEGER NOT NULL CHECK(metadata_sequence >= 0),
    identity_json TEXT NOT NULL CHECK(json_valid(identity_json)),
    revision INTEGER NOT NULL CHECK(revision >= 0),
    metadata_json TEXT NOT NULL CHECK(json_valid(metadata_json))
  )`;
  yield* sql`CREATE INDEX jones_legacy_bootstrap_runs_thread ON jones_legacy_bootstrap_runs(thread_id, run_id)`;
  yield* sql`CREATE TABLE jones_legacy_bootstrap_event_metadata (
    event_id TEXT PRIMARY KEY NOT NULL CHECK(length(event_id) > 0),
    sequence INTEGER NOT NULL CHECK(sequence >= 0),
    command_id TEXT, thread_id TEXT NOT NULL, run_id TEXT, event_type TEXT NOT NULL,
    public_digest TEXT NOT NULL CHECK(length(public_digest) = 64),
    metadata_json TEXT NOT NULL CHECK(json_valid(metadata_json))
  )`;
  yield* sql`CREATE INDEX jones_legacy_bootstrap_event_metadata_command ON jones_legacy_bootstrap_event_metadata(command_id, sequence)`;
  yield* sql`CREATE TABLE jones_legacy_bootstrap_commands (
    command_id TEXT PRIMARY KEY NOT NULL CHECK(length(command_id) > 0),
    thread_id TEXT NOT NULL, project_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('lifecycle','preflight-intent','preflight-outcome','delete')),
    binding_digest TEXT NOT NULL CHECK(length(binding_digest) = 64),
    sequence_domain TEXT NOT NULL CHECK(sequence_domain IN ('lifecycle','jones-legacy-ordinal')),
    private_ordinal INTEGER UNIQUE CHECK(private_ordinal IS NULL OR private_ordinal > 0),
    record_json TEXT NOT NULL CHECK(json_valid(record_json)),
    CHECK((kind IN ('preflight-intent','preflight-outcome') AND sequence_domain = 'jones-legacy-ordinal' AND private_ordinal IS NOT NULL)
      OR (kind IN ('lifecycle','delete') AND sequence_domain = 'lifecycle' AND private_ordinal IS NULL))
  )`;
  yield* sql`CREATE INDEX jones_legacy_bootstrap_commands_target ON jones_legacy_bootstrap_commands(thread_id, kind)`;
  yield* sql`CREATE TABLE jones_legacy_bootstrap_terminal_cleanup (
    effect_id TEXT PRIMARY KEY NOT NULL CHECK(length(effect_id) > 0),
    command_id TEXT NOT NULL, thread_id TEXT NOT NULL,
    control_json TEXT NOT NULL CHECK(json_valid(control_json)),
    state TEXT NOT NULL CHECK(state IN ('pending','started','succeeded','unknown')),
    attempt_id TEXT, revision INTEGER NOT NULL CHECK(revision >= 0),
    outcome TEXT CHECK(outcome IS NULL OR outcome IN ('closed','owner-unavailable','interrupted','unconfirmed')),
    CHECK((state = 'pending' AND attempt_id IS NULL AND outcome IS NULL)
      OR (state = 'started' AND attempt_id IS NOT NULL AND outcome IS NULL)
      OR (state = 'succeeded' AND attempt_id IS NOT NULL AND outcome = 'closed')
      OR (state = 'unknown' AND attempt_id IS NOT NULL AND outcome IS NOT NULL AND outcome <> 'closed'))
  )`;
  yield* sql`CREATE INDEX jones_legacy_bootstrap_terminal_cleanup_state ON jones_legacy_bootstrap_terminal_cleanup(state, effect_id)`;
});
