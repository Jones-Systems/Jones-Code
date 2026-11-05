import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import { ConstraintError, SqlError } from "effect/sql/SqlError";

const table = "jones_native_creation_execution_starts";
const trigger = "jones_native_creation_execution_starts_no_update";
const releasedSql = `CREATE TRIGGER ${trigger} BEFORE UPDATE ON ${table} BEGIN SELECT RAISE(ABORT, 'native execution facts are immutable'); END`;

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const tables = yield* sql`SELECT name FROM sqlite_master
    WHERE type = 'table' AND name = ${table}`;
  const prior = yield* sql<{ readonly tbl_name: string; readonly sql: string | null }>`
    SELECT tbl_name, sql FROM sqlite_master WHERE type = 'trigger' AND name = ${trigger}`;
  if (
    tables.length !== 1 ||
    prior.length !== 1 ||
    prior[0]!.tbl_name !== table ||
    prior[0]!.sql !== releasedSql
  ) {
    return yield* new SqlError({
      reason: new ConstraintError({
        cause: undefined,
        operation: "Jones migration 136 prerequisite",
        message: "Native lease renewal requires the released immutable start trigger.",
      }),
    });
  }

  // The Jones runner owns this transaction. Liveness and coordinated outbox CAS
  // remain with the claim owner; this guard allows only the stored expiry advance.
  yield* sql`DROP TRIGGER jones_native_creation_execution_starts_no_update`;
  yield* sql`CREATE TRIGGER jones_native_creation_execution_starts_no_update
    BEFORE UPDATE ON jones_native_creation_execution_starts
    WHEN (
      NEW.rowid IS OLD.rowid
      AND NEW.effect_id IS OLD.effect_id
      AND NEW.claim_id IS OLD.claim_id
      AND NEW.command_id IS OLD.command_id
      AND NEW.worker_id IS OLD.worker_id
      AND NEW.expected_attempt IS OLD.expected_attempt
      AND NEW.reference_json IS OLD.reference_json
      AND NEW.fact_json IS OLD.fact_json
      AND typeof(OLD.lease_expires_at) = 'text'
      AND typeof(NEW.lease_expires_at) = 'text'
      AND length(OLD.lease_expires_at) = 24
      AND length(NEW.lease_expires_at) = 24
      AND strftime('%Y-%m-%dT%H:%M:%fZ', OLD.lease_expires_at, '+0 days')
          IS OLD.lease_expires_at
      AND strftime('%Y-%m-%dT%H:%M:%fZ', NEW.lease_expires_at, '+0 days')
          IS NEW.lease_expires_at
      AND NEW.lease_expires_at COLLATE BINARY > OLD.lease_expires_at COLLATE BINARY
    ) IS NOT 1
    BEGIN
      SELECT RAISE(ABORT, 'native execution facts are immutable');
    END`;
});
