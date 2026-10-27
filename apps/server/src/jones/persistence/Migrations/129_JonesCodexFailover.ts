import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_codex_failover_evidence (
    provider_turn_id TEXT PRIMARY KEY NOT NULL,
    thread_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    provider_thread_id TEXT NOT NULL,
    provider_instance_id TEXT NOT NULL,
    disposition TEXT NOT NULL CHECK(disposition IN ('usage_limit', 'unavailable')),
    occurred_at TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE jones_codex_failover_decisions (
    provider_turn_id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('deferred','decided')),
    decision TEXT NULL CHECK (decision IS NULL OR length(decision) > 0),
    updated_at TEXT NOT NULL,
    CHECK ((state = 'decided') = (decision IS NOT NULL))
  )`;
  yield* sql`CREATE INDEX jones_codex_failover_decisions_deferred_idx
    ON jones_codex_failover_decisions (thread_id) WHERE state = 'deferred'`;
  yield* sql`CREATE INDEX jones_codex_failover_evidence_thread_idx
    ON jones_codex_failover_evidence (thread_id)`;
  yield* sql`CREATE INDEX jones_codex_failover_evidence_occurred_idx
    ON jones_codex_failover_evidence (occurred_at)`;
  yield* sql`CREATE TABLE jones_codex_failover_steps (
    switch_command_id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    chain_root_run_id TEXT NOT NULL,
    origin_run_id TEXT NOT NULL UNIQUE,
    origin_provider_turn_id TEXT NOT NULL,
    source_instance_id TEXT NOT NULL,
    source_failure TEXT NOT NULL CHECK (source_failure IN ('usage_limit','unavailable')),
    source_reset_at TEXT NULL,
    target_instance_id TEXT NOT NULL,
    saved_selection_json TEXT NOT NULL CHECK (json_valid(saved_selection_json)),
    continuation_selection_json TEXT NOT NULL CHECK (json_valid(continuation_selection_json)),
    continue_command_id TEXT NOT NULL UNIQUE,
    continue_message_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('switched','continued','abandoned')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  )`;
  yield* sql`CREATE INDEX jones_codex_failover_steps_chain_idx
    ON jones_codex_failover_steps (chain_root_run_id)`;
  yield* sql`CREATE INDEX jones_codex_failover_steps_open_idx
    ON jones_codex_failover_steps (thread_id) WHERE status = 'switched'`;
  yield* sql`CREATE INDEX jones_codex_failover_steps_reset_idx
    ON jones_codex_failover_steps (thread_id, source_instance_id)
    WHERE source_failure = 'usage_limit' AND source_reset_at IS NOT NULL`;
  yield* sql`CREATE TABLE jones_codex_failover_queued_runs (
    run_id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    inherited_at TEXT NOT NULL,
    override_selection_json TEXT NULL CHECK (override_selection_json IS NULL OR json_valid(override_selection_json)),
    override_switch_command_id TEXT NULL,
    updated_at TEXT NOT NULL,
    CHECK ((override_selection_json IS NULL) = (override_switch_command_id IS NULL))
  )`;
  yield* sql`CREATE INDEX jones_codex_failover_queued_runs_thread_idx
    ON jones_codex_failover_queued_runs (thread_id)`;
});
