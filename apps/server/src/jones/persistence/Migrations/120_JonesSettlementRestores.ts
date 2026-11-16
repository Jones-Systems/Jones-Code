import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

// One row per thread, replaced by its next state-changing settle. Thread and
// command IDs are upstream keys without foreign keys, per the Jones boundary.
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_settlement_restores (
    thread_id TEXT PRIMARY KEY NOT NULL,
    settle_command_id TEXT NOT NULL UNIQUE,
    settled_sequence INTEGER NOT NULL,
    settled_at TEXT NOT NULL,
    captured_at TEXT NOT NULL,
    prior_settled_override TEXT,
    prior_unsettled_at TEXT,
    prior_pinned_at TEXT,
    prior_pin_order_key TEXT,
    prior_active_order_key TEXT,
    snoozed_until_at_settle TEXT,
    cleared_watch_count INTEGER NOT NULL,
    restore_command_id TEXT UNIQUE,
    restored_at TEXT,
    restored_sequence INTEGER
  )`;
});
