import * as Effect from "effect/Effect";
import * as Migrator from "effect/sql/Migrator";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import { inspectMigrationHistory } from "./migrationHistory.ts";
import { withRuntimeUseInstallationAdmission } from "../cleanup/RuntimeUseInstallation.ts";

type Entry = readonly [
  id: number,
  name: string,
  migration: Effect.Effect<void, SqlError, SqlClient.SqlClient>,
];

const badHistory = (message: string) =>
  new Migrator.MigrationError({ kind: "BadState", message: `Jones migration history: ${message}` });

export const inspectJonesMigrations = Effect.fn("inspectJonesMigrations")(function* (
  entries: ReadonlyArray<Entry>,
) {
  const originals = ["WorktreeOwnershipLeases", "ProjectionThreadRuntimeIdentity", "NativeCreationIntents",
    "NativeCreationCommandIdentities", "WorkstreamsNativeAttempts", "WorkstreamsProviderEnrollments"];
  const names = new Map(entries.map(([id, name]) => [id, name]));
  if (originals.some((name, index) => names.get(index + 1) !== name)) return yield* badHistory("invalid rebuild manifest");
  const sql = yield* SqlClient.SqlClient;
  const tables = yield* sql`SELECT name FROM sqlite_master
    WHERE type = 'table' AND name = 'jones_sql_migrations'`;
  const recorded = tables.length === 0 ? [] : yield* sql<{
    readonly migration_id: number; readonly name: string;
  }>`SELECT migration_id, name FROM jones_sql_migrations ORDER BY migration_id`;
  const result = inspectMigrationHistory(entries.map(([id, name]) => [id, name]),
    recorded.map(({ migration_id: id, name }) => ({ id, name })), "Jones");
  if (result.error !== undefined) return yield* badHistory(result.error);
  const foreign = recorded.filter(({ migration_id }) => migration_id >= 7 && migration_id < 100);
  if (foreign.length > 0) yield* Effect.logWarning(
    "Preserving known foreign Jones migration history without adopting its features",
  ).pipe(Effect.annotateLogs({ migrations: foreign.map(({ migration_id, name }) => `${migration_id}_${name}`) }));
  return result.pending;
});

export const runJonesMigrations = Effect.fn("runJonesMigrations")(function* (
  entries: ReadonlyArray<Entry>,
) {
  // A current ledger is read-only, including when another process owns startup.
  if ((yield* inspectJonesMigrations(entries)).length === 0) return [];
  const sql = yield* SqlClient.SqlClient;
  return yield* sql.withTransaction(Effect.gen(function* () {
    // Ownership may have changed between inspection and acquisition.
    if ((yield* inspectJonesMigrations(entries)).length === 0) return [];
    const migrate = Migrator.make({})({
      table: "jones_sql_migrations",
      loader: Migrator.fromRecord(Object.fromEntries(
        entries.map(([id, name, migration]) => [`${id}_${name}`, migration]),
      )),
    });
    return yield* entries.some(([id]) => id === 139)
      ? withRuntimeUseInstallationAdmission(migrate) : migrate;
  }));
});
