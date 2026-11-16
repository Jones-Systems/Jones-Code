import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_workspace_retirement_intents (
    event_id TEXT PRIMARY KEY NOT NULL, thread_id TEXT NOT NULL, worktree TEXT NOT NULL,
    settled_at TEXT NOT NULL, script_state TEXT NOT NULL DEFAULT 'pending',
    script_started_at TEXT, script_completed_at TEXT, script_exit_code INTEGER,
    outcome_json TEXT CHECK(outcome_json IS NULL OR json_valid(outcome_json))
  )`;
  yield* sql`CREATE INDEX jones_workspace_retirement_intents_thread ON jones_workspace_retirement_intents(thread_id,worktree)`;
  yield* sql`CREATE TABLE jones_workspace_retirement_receipts (
    receipt_id TEXT PRIMARY KEY NOT NULL, incarnation_id TEXT NOT NULL, worktree TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('intent','retired','not_retired','partial','unknown')),
    receipt_json TEXT NOT NULL CHECK(json_valid(receipt_json))
  )`;
  yield* sql`CREATE TABLE jones_workspace_retirement_state (
    worktree TEXT PRIMARY KEY NOT NULL, incarnation_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('retiring','retired')), receipt_id TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE jones_workspace_duty_claims (
    claim_id TEXT PRIMARY KEY NOT NULL, worktree TEXT NOT NULL, incarnation_id TEXT,
    kind TEXT NOT NULL, owner TEXT NOT NULL, holder_json TEXT NOT NULL CHECK(json_valid(holder_json)),
    acquired_at TEXT NOT NULL, released_at TEXT
  )`;
  yield* sql`CREATE INDEX jones_workspace_duty_claims_live ON jones_workspace_duty_claims(worktree,released_at)`;
  yield* sql`CREATE TABLE jones_workspace_disposal_journal (
    receipt_id TEXT NOT NULL, ordinal INTEGER NOT NULL, path TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('untracked','ignored')), identity_json TEXT NOT NULL CHECK(json_valid(identity_json)),
    state TEXT NOT NULL CHECK(state IN ('planned','invoking','completed','removed_with_checkout')),
    PRIMARY KEY(receipt_id,ordinal)
  )`;
  yield* sql`CREATE TABLE jones_workspace_duty_identity (
    claim_id TEXT PRIMARY KEY NOT NULL,
    identity_json TEXT NOT NULL CHECK(json_valid(identity_json)),
    native_owner_json TEXT CHECK(native_owner_json IS NULL OR json_valid(native_owner_json))
  )`;
});
