import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";

/** Reports whether a Jones table exists on the SQL client the presence was made for. */
export type JonesTablePresence = (table: string) => Effect.Effect<boolean, SqlError>;

// Upstream-only harnesses build services without Jones tables, so callers
// still check before use. Jones tables are never dropped, so a table found
// once is remembered for this client and later checks read no schema. An
// absent table is read again on every call: schema created after the caller
// was built is still observed.
export const makeJonesTablePresence = (sql: SqlClient.SqlClient): JonesTablePresence => {
  const present = new Set<string>();
  // Suspended so that a check built once and run many times sees the latch.
  return (table) =>
    Effect.suspend(() =>
      present.has(table)
        ? Effect.succeed(true)
        : sql`SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ${table}`.pipe(
            Effect.map((rows) => {
              if (rows.length > 0) present.add(table);
              return rows.length > 0;
            }),
          ),
    );
};
