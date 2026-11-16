import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";

// Shared by the additive migration and runtime provenance gate. No historical definitions change.
export const nativeProcessCustodyDefinitions: Readonly<Record<string, string>> = {
  jones_native_creation_execution_process_claims:
    "CREATE TABLE jones_native_creation_execution_process_claims (\n    effect_id TEXT PRIMARY KEY NOT NULL,\n    claim_id TEXT NOT NULL,\n    command_id TEXT NOT NULL,\n    thread_id TEXT NOT NULL,\n    run_id TEXT NOT NULL,\n    request_digest TEXT NOT NULL CHECK(length(request_digest)=64),\n    worker_id TEXT NOT NULL,\n    expected_attempt INTEGER NOT NULL CHECK(expected_attempt > 0),\n    claimed_at TEXT NOT NULL,\n    reference_json TEXT NOT NULL CHECK(json_valid(reference_json)),\n    fact_json TEXT NOT NULL CHECK(json_valid(fact_json))\n  )",
  jones_native_creation_execution_process_confirmations:
    "CREATE TABLE jones_native_creation_execution_process_confirmations (\n    effect_id TEXT PRIMARY KEY NOT NULL,\n    worker_id TEXT NOT NULL,\n    expected_attempt INTEGER NOT NULL CHECK(expected_attempt > 0),\n    evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),\n    confirmed_at TEXT NOT NULL\n  )",
  jones_native_creation_execution_process_holds:
    "CREATE TABLE jones_native_creation_execution_process_holds (\n    effect_id TEXT PRIMARY KEY NOT NULL,\n    reason TEXT NOT NULL,\n    held_at TEXT NOT NULL\n  )",
  jones_native_creation_execution_process_bindings:
    "CREATE TABLE jones_native_creation_execution_process_bindings (\n    effect_id TEXT PRIMARY KEY NOT NULL,\n    binding_json TEXT NOT NULL CHECK(json_valid(binding_json)),\n    bound_at TEXT NOT NULL\n  )",
  jones_native_creation_execution_process_claims_no_update:
    "CREATE TRIGGER jones_native_creation_execution_process_claims_no_update BEFORE UPDATE ON jones_native_creation_execution_process_claims BEGIN SELECT RAISE(ABORT, 'native process custody is immutable'); END",
  jones_native_creation_execution_process_claims_no_delete:
    "CREATE TRIGGER jones_native_creation_execution_process_claims_no_delete BEFORE DELETE ON jones_native_creation_execution_process_claims BEGIN SELECT RAISE(ABORT, 'native process custody is permanent'); END",
  jones_native_creation_execution_process_claims_no_replace:
    "CREATE TRIGGER jones_native_creation_execution_process_claims_no_replace BEFORE INSERT ON jones_native_creation_execution_process_claims WHEN EXISTS (SELECT 1 FROM jones_native_creation_execution_process_claims WHERE effect_id=NEW.effect_id) BEGIN SELECT RAISE(ABORT, 'native process custody ownership is permanent'); END",
  jones_native_creation_execution_process_confirmations_no_update:
    "CREATE TRIGGER jones_native_creation_execution_process_confirmations_no_update BEFORE UPDATE ON jones_native_creation_execution_process_confirmations BEGIN SELECT RAISE(ABORT, 'native process custody is immutable'); END",
  jones_native_creation_execution_process_confirmations_no_delete:
    "CREATE TRIGGER jones_native_creation_execution_process_confirmations_no_delete BEFORE DELETE ON jones_native_creation_execution_process_confirmations BEGIN SELECT RAISE(ABORT, 'native process custody is permanent'); END",
  jones_native_creation_execution_process_confirmations_no_replace:
    "CREATE TRIGGER jones_native_creation_execution_process_confirmations_no_replace BEFORE INSERT ON jones_native_creation_execution_process_confirmations WHEN EXISTS (SELECT 1 FROM jones_native_creation_execution_process_confirmations WHERE effect_id=NEW.effect_id) BEGIN SELECT RAISE(ABORT, 'native process custody ownership is permanent'); END",
  jones_native_creation_execution_process_holds_no_update:
    "CREATE TRIGGER jones_native_creation_execution_process_holds_no_update BEFORE UPDATE ON jones_native_creation_execution_process_holds BEGIN SELECT RAISE(ABORT, 'native process custody is immutable'); END",
  jones_native_creation_execution_process_holds_no_delete:
    "CREATE TRIGGER jones_native_creation_execution_process_holds_no_delete BEFORE DELETE ON jones_native_creation_execution_process_holds BEGIN SELECT RAISE(ABORT, 'native process custody is permanent'); END",
  jones_native_creation_execution_process_holds_no_replace:
    "CREATE TRIGGER jones_native_creation_execution_process_holds_no_replace BEFORE INSERT ON jones_native_creation_execution_process_holds WHEN EXISTS (SELECT 1 FROM jones_native_creation_execution_process_holds WHERE effect_id=NEW.effect_id) BEGIN SELECT RAISE(ABORT, 'native process custody ownership is permanent'); END",
  jones_native_creation_execution_process_bindings_no_update:
    "CREATE TRIGGER jones_native_creation_execution_process_bindings_no_update BEFORE UPDATE ON jones_native_creation_execution_process_bindings BEGIN SELECT RAISE(ABORT, 'native process custody is immutable'); END",
  jones_native_creation_execution_process_bindings_no_delete:
    "CREATE TRIGGER jones_native_creation_execution_process_bindings_no_delete BEFORE DELETE ON jones_native_creation_execution_process_bindings BEGIN SELECT RAISE(ABORT, 'native process custody is permanent'); END",
  jones_native_creation_execution_process_bindings_no_replace:
    "CREATE TRIGGER jones_native_creation_execution_process_bindings_no_replace BEFORE INSERT ON jones_native_creation_execution_process_bindings WHEN EXISTS (SELECT 1 FROM jones_native_creation_execution_process_bindings WHERE effect_id=NEW.effect_id) BEGIN SELECT RAISE(ABORT, 'native process custody ownership is permanent'); END",
};

export const hasNativeProcessCustodySchema = (sql: SqlClient.SqlClient) =>
  sql<{ readonly name: string; readonly sql: string }>`SELECT name,sql FROM sqlite_master
    WHERE name LIKE 'jones_native_creation_execution_process_%' AND sql IS NOT NULL`.pipe(
    Effect.map(
      (actual) =>
        actual.length === Object.keys(nativeProcessCustodyDefinitions).length &&
        actual.every((row) => nativeProcessCustodyDefinitions[row.name] === row.sql),
    ),
  );
