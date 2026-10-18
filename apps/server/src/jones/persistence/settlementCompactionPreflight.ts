import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

export class JonesSettlementProofMigrationRequiredError extends Schema.TaggedError<JonesSettlementProofMigrationRequiredError>()(
  "JonesSettlementProofMigrationRequiredError",
  {},
) {
  override get message(): string {
    return "Settlement evidence must be preserved before database compaction. Start the updated Jones Code server once to apply its migrations, stop it, then run doctor again.";
  }
}

// Offline doctor bypasses normal startup migrations; check before any event deletion.
export const requireSettlementProofsBeforeCompaction = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const tables = yield* sql<{ readonly name: string }>`SELECT name FROM sqlite_master
    WHERE type = 'table' AND name IN (
      'jones_sql_migrations', 'workstreams_native_attempts', 'jones_workstreams_settlement_proofs'
    )`;
  if (tables.length === 0) return;
  const names = new Set(tables.map(({ name }) => name));
  if (!names.has("jones_sql_migrations") || !names.has("jones_workstreams_settlement_proofs"))
    return yield* new JonesSettlementProofMigrationRequiredError();
  const completed = yield* sql<{ readonly name: string }>`SELECT name FROM jones_sql_migrations
    WHERE migration_id = 107`;
  if (completed.length !== 1 || completed[0]!.name !== "WorkstreamsSettlementProofs")
    return yield* new JonesSettlementProofMigrationRequiredError();
});
