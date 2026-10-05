import * as Effect from "effect/Effect";
import * as Migrator from "effect/unstable/sql/Migrator";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";

const originalNames = [
  "WorktreeOwnershipLeases",
  "ProjectionThreadRuntimeIdentity",
  "NativeCreationIntents",
  "NativeCreationCommandIdentities",
  "WorkstreamsNativeAttempts",
  "WorkstreamsProviderEnrollments",
] as const;

// Foreign ledger names are source-bound to #61/#87 (da5f4aee/7c86493f)
// and #77/#91 (ae25e5d0/09ead6ea). They grant no schema or feature adoption.
const foreignV2Names = [
  "V2NativeAcceptance",
  "DeletionWorktreeAdmission",
  "OrdinaryCheckoutOwnership",
  "AttachmentCleanup",
  "OrdinaryCheckoutExecutionLifetime",
  "ImportedApplicationAttachments",
  "CommandNormalizationWitness",
] as const;

type Entry = readonly [
  id: number,
  name: string,
  migration: Effect.Effect<void, SqlError, SqlClient.SqlClient>,
];

const badHistory = (message: string) =>
  new Migrator.MigrationError({ kind: "BadState", message: `Jones migration history: ${message}` });

export const runJonesMigrations = Effect.fn("runJonesMigrations")(function* (
  entries: ReadonlyArray<Entry>,
) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const manifest = [...entries].sort(([a], [b]) => a - b);
      const manifestNames = new Map(manifest.map(([id, name]) => [id, name]));
      if (
        manifestNames.size !== manifest.length ||
        originalNames.some((name, index) => manifestNames.get(index + 1) !== name) ||
        manifest.some(([id]) => !Number.isSafeInteger(id) || id < 1 || (id > 6 && id < 100))
      ) {
        return yield* Effect.fail(badHistory("invalid rebuild manifest"));
      }

      const tables = yield* sql`SELECT name FROM sqlite_master
        WHERE type = 'table' AND name = 'jones_sql_migrations'`;
      const recorded =
        tables.length === 0
          ? []
          : yield* sql<{
              readonly migration_id: number;
              readonly name: string;
            }>`SELECT migration_id, name FROM jones_sql_migrations ORDER BY migration_id`;
      const recordedIds = new Set(recorded.map(({ migration_id }) => migration_id));
      const foreign = recorded.filter(
        ({ migration_id }) => migration_id >= 7 && migration_id < 100,
      );
      for (const { migration_id: id, name } of recorded) {
        const expected =
          id >= 7 && id < 100
            ? id === 7 && foreign.length === 1 && name === "ThreadCreationLookupIndex"
              ? "ThreadCreationLookupIndex"
              : foreignV2Names[id - 7]
            : manifestNames.get(id);
        if (!Number.isSafeInteger(id) || expected === undefined || expected !== name) {
          return yield* Effect.fail(badHistory(`unrecognized ${id}_${name}`));
        }
        if (id <= 6 || id >= 100) {
          for (const [priorId] of manifest) {
            if (priorId >= id) break;
            if (!recordedIds.has(priorId)) {
              return yield* Effect.fail(
                badHistory(`missing rebuild migration ${priorId} before ${id}`),
              );
            }
          }
        }
      }
      if (foreign.length > 0) {
        if (originalNames.some((_, index) => !recordedIds.has(index + 1))) {
          return yield* Effect.fail(badHistory("foreign history is missing original migrations"));
        }
        if (foreign.some(({ migration_id }, index) => migration_id !== index + 7)) {
          return yield* Effect.fail(badHistory("foreign history has a missing prefix"));
        }
        yield* Effect.logWarning(
          "Preserving known foreign Jones migration history without adopting its features",
        ).pipe(
          Effect.annotateLogs({
            migrations: foreign.map(({ migration_id, name }) => `${migration_id}_${name}`),
          }),
        );
      }

      // Migrator uses the maximum applied ID. The prefix checks above must precede
      // even its tracking-table creation, or a gap could silently skip an effect.
      return yield* Migrator.make({})({
        table: "jones_sql_migrations",
        loader: Migrator.fromRecord(
          Object.fromEntries(manifest.map(([id, name, migration]) => [`${id}_${name}`, migration])),
        ),
      });
    }),
  );
});
