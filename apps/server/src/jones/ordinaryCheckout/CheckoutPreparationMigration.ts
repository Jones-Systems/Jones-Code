import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

// Migration 159 in the independent Jones loader is the sole production entry.
// This program creates only Jones tables and never changes upstream objects or ledgers.
export const createCheckoutPreparationSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_ordinary_checkout_preparing_origins (
    origin_id TEXT PRIMARY KEY, command_id TEXT NOT NULL, subject_thread_id TEXT NOT NULL,
    birth_event_id TEXT NOT NULL, run_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
    record_json TEXT NOT NULL,
    UNIQUE(subject_thread_id,birth_event_id,run_id,attempt_id))`;
  yield* sql`CREATE INDEX jones_ordinary_checkout_preparing_command
    ON jones_ordinary_checkout_preparing_origins(command_id)`;
  yield* sql`CREATE TABLE jones_ordinary_checkout_preparing_bindings (
    origin_id TEXT NOT NULL, binding_revision INTEGER NOT NULL CHECK(binding_revision>0),
    command_id TEXT NOT NULL, record_json TEXT NOT NULL,
    PRIMARY KEY(origin_id,binding_revision), UNIQUE(origin_id,command_id))`;
  yield* sql`CREATE TABLE jones_ordinary_checkout_preparing_releases (
    origin_id TEXT PRIMARY KEY, admission_id TEXT NOT NULL UNIQUE,
    command_id TEXT NOT NULL UNIQUE, effect_id TEXT NOT NULL UNIQUE, record_json TEXT NOT NULL)`;
});
