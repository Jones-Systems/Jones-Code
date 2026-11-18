import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";
import { digest, failure } from "./Protocol.ts";

export const contractDigest = digest({ foundation: 1, enrollment: 1, infrastructure: 1 });

/** Caller owns the actual sidecar writer transaction. No participant or live permit is issued. */
export const create = Effect.fn("JournalSchema.create")(function* (
  sql: SqlClient.SqlClient,
  bindingJson: string,
) {
  yield* sql`CREATE TABLE IF NOT EXISTS jones_common_admission_schema (
      singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL,
      binding_json TEXT NOT NULL)`;
  const existing = yield* sql<{
    version: number;
    binding_json: string;
  }>`SELECT version,binding_json
      FROM jones_common_admission_schema WHERE singleton=1`;
  if (
    existing.length !== 0 &&
    (existing[0]?.version !== 1 || existing[0]?.binding_json !== bindingJson)
  )
    return yield* failure("journal_identity_or_schema_changed");
  yield* sql`INSERT OR IGNORE INTO jones_common_admission_schema VALUES (1,1,${bindingJson})`;
  yield* sql`CREATE TABLE IF NOT EXISTS jones_common_participants (
      store_id TEXT PRIMARY KEY, database_json TEXT NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS jones_common_incarnations (
      nonce TEXT PRIMARY KEY, identity_json TEXT NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS jones_common_roots (
      root_id TEXT PRIMARY KEY, identity_sha256 TEXT NOT NULL UNIQUE, scope_json TEXT NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS jones_common_reservations (
      operation_key TEXT PRIMARY KEY, phase TEXT NOT NULL, record_json TEXT NOT NULL,
      record_sha256 TEXT NOT NULL,
      CHECK(phase=json_extract(record_json,'$.phase')),
      CHECK(json_valid(record_json)))`;
  yield* sql`CREATE INDEX IF NOT EXISTS jones_common_reservations_phase ON jones_common_reservations(phase)`;
  yield* sql`CREATE TABLE IF NOT EXISTS jones_common_slots (
      slot_id TEXT PRIMARY KEY, scope_path TEXT NOT NULL UNIQUE, record_json TEXT NOT NULL,
      record_sha256 TEXT NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS jones_common_enrollments (
    enrollment_id TEXT PRIMARY KEY, store_id TEXT NOT NULL, nonce TEXT NOT NULL UNIQUE,
    identity_json TEXT NOT NULL, phase TEXT NOT NULL CHECK(phase IN ('staging','draining','published','superseded')),
    revision INTEGER NOT NULL, seal_json TEXT)`;
  yield* sql`CREATE TABLE IF NOT EXISTS jones_common_store_current (
    store_id TEXT PRIMARY KEY, nonce TEXT NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS jones_common_infrastructure (
    scope_key TEXT PRIMARY KEY, scope_json TEXT NOT NULL, scope_sha256 TEXT NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS jones_common_store_infrastructure (
    store_id TEXT PRIMARY KEY, database_json TEXT NOT NULL, scope_key TEXT NOT NULL, database_path TEXT NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS jones_common_enrollment_items (
    store_id TEXT NOT NULL, item_key TEXT NOT NULL, entry_json TEXT NOT NULL,
    entry_sha256 TEXT NOT NULL, reconciliation_revision INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(store_id,item_key))`;
  yield* sql`CREATE TABLE IF NOT EXISTS jones_common_enrollment_manifest (
    enrollment_id TEXT NOT NULL, item_key TEXT NOT NULL, entry_sha256 TEXT NOT NULL,
    PRIMARY KEY(enrollment_id,item_key))`;
});
