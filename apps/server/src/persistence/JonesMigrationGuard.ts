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

export interface JonesMigrationPolicy {
  readonly foreignV2: "independent" | "inert-excluded";
  readonly requiresOwn?: ReadonlyArray<readonly [id: number, name: string]>;
  readonly sourceBasis: string;
}

export type JonesMigrationEntry = readonly [
  id: number,
  name: string,
  migration: Effect.Effect<void, SqlError, SqlClient.SqlClient>,
  policy?: JonesMigrationPolicy,
];

const badHistory = (message: string) =>
  new Migrator.MigrationError({ kind: "BadState", message: `Jones migration history: ${message}` });

interface LedgerRow {
  readonly migration_id: number;
  readonly name: string;
  readonly [key: string]: unknown;
}

const validateManifest = (entries: ReadonlyArray<JonesMigrationEntry>) => {
  const manifest = [...entries].sort(([a], [b]) => a - b);
  const names = new Map(manifest.map(([id, name]) => [id, name]));
  if (
    names.size !== manifest.length ||
    originalNames.some((name, index) => names.get(index + 1) !== name) ||
    manifest.some(
      ([id, name]) =>
        !Number.isSafeInteger(id) ||
        id < 1 ||
        (id > 6 && id < 100) ||
        typeof name !== "string" ||
        name.trim() !== name ||
        name.length === 0,
    )
  )
    return { error: badHistory("invalid rebuild manifest") } as const;
  for (const [id, , , policy] of manifest) {
    if (id <= 6) continue;
    if (
      policy === undefined ||
      policy === null ||
      !["independent", "inert-excluded"].includes(policy.foreignV2) ||
      typeof policy.sourceBasis !== "string" ||
      policy.sourceBasis.trim().length === 0 ||
      Object.keys(policy).some(
        (key) => !["foreignV2", "requiresOwn", "sourceBasis"].includes(key),
      ) ||
      (policy.requiresOwn !== undefined && !Array.isArray(policy.requiresOwn))
    ) {
      return { error: badHistory(`invalid applicability policy for ${id}`) } as const;
    }
    const prerequisites = new Set<number>();
    for (const prerequisite of policy.requiresOwn ?? []) {
      if (
        !Array.isArray(prerequisite) ||
        prerequisite.length !== 2 ||
        !Number.isSafeInteger(prerequisite[0]) ||
        prerequisite[0] < 100 ||
        prerequisite[0] >= id ||
        names.get(prerequisite[0]) !== prerequisite[1] ||
        prerequisites.has(prerequisite[0])
      ) {
        return { error: badHistory(`invalid own prerequisite for ${id}`) } as const;
      }
      prerequisites.add(prerequisite[0]);
    }
  }
  return { manifest, names } as const;
};

const readLedger = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const tables = yield* sql`SELECT name FROM sqlite_master
    WHERE type = 'table' AND name = 'jones_sql_migrations'`;
  return tables.length === 0
    ? []
    : yield* sql<LedgerRow>`SELECT * FROM jones_sql_migrations ORDER BY migration_id`;
});

const classifyHistory = (
  manifest: ReadonlyArray<JonesMigrationEntry>,
  names: ReadonlyMap<number, string>,
  recorded: ReadonlyArray<LedgerRow>,
) => {
  const recordedIds = new Set(recorded.map(({ migration_id }) => migration_id));
  const foreign = recorded.filter(({ migration_id }) => migration_id >= 7 && migration_id < 100);
  const lookupOnly =
    foreign.length === 1 &&
    foreign[0]!.migration_id === 7 &&
    foreign[0]!.name === "ThreadCreationLookupIndex";
  for (const { migration_id: id, name } of recorded) {
    const expected =
      id >= 7 && id < 100
        ? lookupOnly
          ? "ThreadCreationLookupIndex"
          : foreignV2Names[id - 7]
        : names.get(id);
    if (!Number.isSafeInteger(id) || expected === undefined || expected !== name) {
      return { error: badHistory(`unrecognized ${id}_${name}`) } as const;
    }
  }
  if (foreign.length > 0) {
    if (originalNames.some((_, index) => !recordedIds.has(index + 1))) {
      return { error: badHistory("foreign history is missing original migrations") } as const;
    }
    if (foreign.some(({ migration_id }, index) => migration_id !== index + 7)) {
      return { error: badHistory("foreign history has a missing prefix") } as const;
    }
  }
  const historyMode =
    foreign.length === 0
      ? ("own" as const)
      : lookupOnly
        ? ("lookup-only007" as const)
        : ("foreign-v2-inert" as const);
  const excluded =
    historyMode === "foreign-v2-inert"
      ? manifest.filter(([id, , , policy]) => id >= 100 && policy!.foreignV2 === "inert-excluded")
      : [];
  const excludedIds = new Set(excluded.map(([id]) => id));
  const applicable = manifest.filter(([id]) => !excludedIds.has(id));
  for (const [id, , , policy] of applicable) {
    if (policy?.requiresOwn?.some(([priorId]) => excludedIds.has(priorId))) {
      return { error: badHistory(`applicable ${id} requires excluded own migration`) } as const;
    }
  }
  for (const { migration_id: id } of recorded) {
    if (excludedIds.has(id)) {
      return {
        error: badHistory(`excluded own migration ${id} recorded alongside foreign V2 history`),
      } as const;
    }
    if (id <= 6 || id >= 100) {
      for (const [priorId] of applicable) {
        if (priorId >= id) break;
        if (!recordedIds.has(priorId)) {
          return {
            error: badHistory(`missing rebuild migration ${priorId} before ${id}`),
          } as const;
        }
      }
    }
  }
  return { historyMode, foreign, applicable, excluded, recordedIds } as const;
};

export const readJonesMigrationProfile = Effect.fn("readJonesMigrationProfile")(function* (
  entries: ReadonlyArray<JonesMigrationEntry>,
) {
  const validated = validateManifest(entries);
  if ("error" in validated) return yield* validated.error;
  const recorded = yield* readLedger;
  const profile = classifyHistory(validated.manifest, validated.names, recorded);
  if ("error" in profile) return yield* profile.error;
  return {
    historyMode: profile.historyMode,
    excluded: profile.excluded.map(([id, name, , policy]) => ({
      id,
      name,
      reason: policy!.sourceBasis,
    })),
    pendingApplicable: profile.applicable
      .filter(([id]) => !profile.recordedIds.has(id))
      .map(([id, name]) => [id, name] as const),
    ownSchemaPrerequisites: profile.applicable
      .filter(([id]) => profile.recordedIds.has(id))
      .map(([id, name]) => [id, name] as const),
  };
});

export const hasOwnJonesMigration = Effect.fn("hasOwnJonesMigration")(function* (
  entries: ReadonlyArray<JonesMigrationEntry>,
  required: readonly [number, string],
) {
  const profile = yield* readJonesMigrationProfile(entries);
  return (
    profile.historyMode !== "foreign-v2-inert" &&
    profile.ownSchemaPrerequisites.some(([id, name]) => id === required[0] && name === required[1])
  );
});

export const runJonesMigrationsDetailed = Effect.fn("runJonesMigrationsDetailed")(function* (
  entries: ReadonlyArray<JonesMigrationEntry>,
) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const validated = validateManifest(entries);
      if ("error" in validated) return yield* validated.error;
      const recorded = yield* readLedger;
      const profile = classifyHistory(validated.manifest, validated.names, recorded);
      if ("error" in profile) return yield* profile.error;
      if (profile.foreign.length > 0) {
        yield* Effect.logWarning(
          "Preserving known foreign Jones migration history without adopting its features",
        ).pipe(
          Effect.annotateLogs({
            migrations: profile.foreign.map(({ migration_id, name }) => `${migration_id}_${name}`),
          }),
        );
      }

      // Migrator uses the maximum applied ID. The prefix checks above must precede
      // even its tracking-table creation, or a gap could silently skip an effect.
      const applied = yield* Migrator.make({})({
        table: "jones_sql_migrations",
        loader: Migrator.fromRecord(
          Object.fromEntries(
            profile.applicable.map(([id, name, migration]) => [`${id}_${name}`, migration]),
          ),
        ),
      });
      const after = yield* readLedger;
      if (
        recorded.some(
          (row) =>
            JSON.stringify(after.find((current) => current.migration_id === row.migration_id)) !==
            JSON.stringify(row),
        ) ||
        after.some(
          (row) =>
            !profile.recordedIds.has(row.migration_id) &&
            !applied.some(([id, name]) => id === row.migration_id && name === row.name),
        )
      ) {
        return yield* badHistory(
          "migration changed retained history or inserted an unexecuted ledger row",
        );
      }
      const result = yield* readJonesMigrationProfile(entries);
      if (result.pendingApplicable.length > 0 || result.historyMode !== profile.historyMode) {
        return yield* badHistory("migration readback differs from applicable source profile");
      }
      return { applied, ...result };
    }),
  );
});

export const runJonesMigrations = Effect.fn("runJonesMigrations")(function* (
  entries: ReadonlyArray<JonesMigrationEntry>,
) {
  return (yield* runJonesMigrationsDetailed(entries)).applied;
});
