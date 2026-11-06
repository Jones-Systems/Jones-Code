import type { ServerSettingsPatch } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";
import { RetirementError } from "./Receipts.ts";

export function guardRetentionSettings<A, E, R>(
  sql: SqlClient.SqlClient["Service"],
  patch: ServerSettingsPatch,
  effect: Effect.Effect<A, E, R>,
) {
  const affects =
    patch.worktreeCleanup !== undefined ||
    patch.storageCleanup !== undefined ||
    Object.values(patch.projectSettingsOverrides ?? {}).some(
      (p) => p?.worktreeCleanup !== undefined,
    );
  if (!affects) return effect;
  return sql.withTransaction(
    Effect.gen(function* () {
      const rows = yield* sql<{
        worktree: string;
      }>`SELECT worktree FROM jones_workspace_retirement_state WHERE state='retiring' LIMIT 1`;
      if (rows[0])
        return yield* new RetirementError({ target: rows[0].worktree, reason: "retiring" });
      // The participant transaction spans the atomic settings-file write; retirement cannot enter retiring in between.
      return yield* effect;
    }),
  );
}
