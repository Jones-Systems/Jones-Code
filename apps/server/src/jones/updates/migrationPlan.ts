import * as NodeFS from "node:fs";
import * as NodeSqlite from "node:sqlite";

type MigrationIdentity = readonly [id: number, name: string];
export interface MigrationPlan {
  readonly pendingUpstream: readonly number[];
  readonly pendingJones: readonly number[];
}
export type MigrationPlanResult =
  | { readonly status: "ready"; readonly migrationPlan: MigrationPlan }
  | { readonly status: "blocked"; readonly reason: string };

const foreignJonesNames = [
  "V2NativeAcceptance",
  "DeletionWorktreeAdmission",
  "OrdinaryCheckoutOwnership",
  "AttachmentCleanup",
  "OrdinaryCheckoutExecutionLifetime",
  "ImportedApplicationAttachments",
  "CommandNormalizationWitness",
];

/** Manifests must come from the staged candidate, never the running server. */
export function readMigrationPlan(input: {
  readonly databasePath: string;
  readonly upstream: readonly MigrationIdentity[];
  readonly jones: readonly MigrationIdentity[];
}): MigrationPlanResult {
  const pending = (manifest: readonly MigrationIdentity[], applied: ReadonlySet<number>) =>
    manifest.filter(([id]) => !applied.has(id)).map(([id]) => id);
  try {
    NodeFS.statSync(input.databasePath);
  } catch (cause) {
    if (
      typeof cause !== "object" ||
      cause === null ||
      !("code" in cause) ||
      cause.code !== "ENOENT"
    ) {
      return {
        status: "blocked",
        reason: "The candidate could not read the database migration ledgers.",
      };
    }
    return {
      status: "ready",
      migrationPlan: {
        pendingUpstream: pending(input.upstream, new Set()),
        pendingJones: pending(input.jones, new Set()),
      },
    };
  }
  let database: NodeSqlite.DatabaseSync | undefined;
  try {
    database = new NodeSqlite.DatabaseSync(input.databasePath, { readOnly: true });
    database.exec("BEGIN");
    const readLedger = (table: "effect_sql_migrations" | "jones_sql_migrations") => {
      const present = database!
        .prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?")
        .get(table);
      return present === undefined
        ? []
        : database!.prepare(`SELECT migration_id, name FROM ${table} ORDER BY migration_id`).all();
    };
    const upstream = readLedger("effect_sql_migrations");
    const jones = readLedger("jones_sql_migrations");
    for (const [rows, manifest, owner] of [
      [upstream, input.upstream, "upstream"],
      [jones, input.jones, "Jones"],
    ] as const) {
      const names = new Map(manifest);
      const maximum = Math.max(0, ...names.keys());
      const applied = new Set(rows.map((row) => Number(row.migration_id)));
      const foreign = rows.filter(
        (row) => Number(row.migration_id) >= 7 && Number(row.migration_id) < 100,
      );
      for (const row of rows) {
        const id = Number(row.migration_id);
        if (id > maximum)
          return {
            status: "blocked",
            reason: `Candidate older than this database (${owner} migration ${id}).`,
          };
        const foreignName =
          owner === "Jones" && id >= 7 && id < 100
            ? id === 7 && foreign.length === 1 && row.name === "ThreadCreationLookupIndex"
              ? "ThreadCreationLookupIndex"
              : foreignJonesNames[id - 7]
            : undefined;
        if (!Number.isSafeInteger(id) || (names.get(id) ?? foreignName) !== row.name) {
          return { status: "blocked", reason: `Unrecognized ${owner} migration history at ${id}.` };
        }
        if (
          owner === "Jones" &&
          (id <= 6 || id >= 100) &&
          manifest.some(([prior]) => prior < id && !applied.has(prior))
        ) {
          return { status: "blocked", reason: "Jones migration history has a missing prefix." };
        }
      }
      if (
        owner === "Jones" &&
        foreign.length > 0 &&
        ([1, 2, 3, 4, 5, 6].some((id) => !applied.has(id)) ||
          foreign.some((row, index) => Number(row.migration_id) !== index + 7))
      ) {
        return { status: "blocked", reason: "Jones migration history has a missing prefix." };
      }
    }
    return {
      status: "ready",
      migrationPlan: {
        pendingUpstream: pending(
          input.upstream,
          new Set(upstream.map((row) => Number(row.migration_id))),
        ),
        pendingJones: pending(input.jones, new Set(jones.map((row) => Number(row.migration_id)))),
      },
    };
  } catch {
    return {
      status: "blocked",
      reason: "The candidate could not read the database migration ledgers.",
    };
  } finally {
    database?.close();
  }
}

export function decodeMigrationPlan(value: unknown): MigrationPlan | undefined {
  if (typeof value !== "object" || value === null) return undefined;
  const record = value as Record<string, unknown>;
  const ids = (values: unknown): values is number[] =>
    Array.isArray(values) && values.every((id) => Number.isSafeInteger(id) && id > 0);
  return ids(record.pendingUpstream) && ids(record.pendingJones)
    ? { pendingUpstream: record.pendingUpstream, pendingJones: record.pendingJones }
    : undefined;
}
