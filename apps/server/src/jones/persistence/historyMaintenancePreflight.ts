import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import { requireSettlementProofsBeforeCompaction } from "./settlementCompactionPreflight.ts";

export class JonesForeignHistoryMaintenanceBlockedError extends Schema.TaggedError<JonesForeignHistoryMaintenanceBlockedError>()(
  "JonesForeignHistoryMaintenanceBlockedError",
  { table: Schema.String },
) {
  override get message(): string {
    return "Retained records from a legacy Jones Code schema block database history maintenance. Keep this database until a compatible maintenance path is available.";
  }
}

export const requireHistoryMaintenanceSafety = Effect.fn("requireHistoryMaintenanceSafety")(
  function* (affectedTables: ReadonlyArray<string>) {
    yield* requireSettlementProofsBeforeCompaction;
    const sql = yield* SqlClient.SqlClient;
    // Released foreign tables can depend on upstream events and effects. Some of those rows are
    // proof of what an upstream write did, so a payload rewrite can break proof even without an
    // FK violation. Keep a populated schema in custody until maintenance understands its history.
    const referencing = yield* sql<{ readonly name: string }>`
      SELECT DISTINCT child.name
      FROM main.sqlite_schema AS child
      JOIN pragma_foreign_key_list(child.name, 'main') AS foreign_key
      WHERE child.type = 'table'
        AND foreign_key."table" COLLATE NOCASE IN ${sql.in(affectedTables)}
      ORDER BY child.name
    `;
    for (const { name } of referencing) {
      const rows = yield* sql`SELECT 1 FROM main.${sql(name)} LIMIT 1`;
      if (rows.length > 0) {
        return yield* new JonesForeignHistoryMaintenanceBlockedError({ table: name });
      }
    }
  },
);
