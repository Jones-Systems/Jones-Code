// @effect-diagnostics-next-line nodeBuiltinImport:off -- Candidate preflight is a synchronous native SQLite adapter; stat distinguishes a missing database before opening it read-only.
import * as NodeFS from "node:fs";
import * as NodeSqlite from "node:sqlite";

import { inspectMigrationHistory, reconciledV2PreviewHistory } from "../persistence/migrationHistory.ts";

type MigrationIdentity = readonly [id: number, name: string];
export interface MigrationPlan {
  readonly pendingUpstream: readonly number[];
  readonly pendingJones: readonly number[];
}
export type MigrationPlanResult =
  | { readonly status: "ready"; readonly migrationPlan: MigrationPlan }
  | { readonly status: "blocked"; readonly reason: string };

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
      const recorded = rows.map((row) => ({ id: Number(row.migration_id), name: String(row.name) }));
      const reconciled = owner === "upstream" ? reconciledV2PreviewHistory(recorded) : recorded;
      if (reconciled === null) return { status: "blocked", reason: "Unexpected V2 preview migration history." };
      const result = inspectMigrationHistory(manifest, reconciled, owner);
      if (result.error !== undefined) return { status: "blocked", reason: result.error };
    }
    return {
      status: "ready",
      migrationPlan: {
        pendingUpstream: [
          ...upstream.flatMap((row) => row.name === "OrchestrationV2" && Number(row.migration_id) === 53
            ? [53, 54] : row.name === "OrchestrationV2" && Number(row.migration_id) === 54 ? [54] : []),
          ...pending(input.upstream, new Set((reconciledV2PreviewHistory(
            upstream.map((row) => ({ id: Number(row.migration_id), name: String(row.name) })),
          ) ?? []).map(({ id }) => id))),
        ],
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
  if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined;
  const record = value as Record<string, unknown>;
  const ids = (values: unknown): values is number[] => {
    if (!Array.isArray(values)) return false;
    for (let index = 0; index < values.length; index++) {
      if (!Object.hasOwn(values, index) || !Number.isSafeInteger(values[index]) || values[index] <= 0) {
        return false;
      }
    }
    return true;
  };
  return ids(record.pendingUpstream) && ids(record.pendingJones)
    ? { pendingUpstream: record.pendingUpstream, pendingJones: record.pendingJones }
    : undefined;
}
