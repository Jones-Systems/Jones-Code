import * as NodeAssert from "node:assert/strict";

export function qualificationSqliteApi(current) {
  return current
    ? {
        modulePath: "apps/server/src/persistence/Sqlite.ts",
        factory: "layerFromPath",
        sqlClient: "effect/sql/SqlClient",
      }
    : {
        modulePath: "apps/server/src/persistence/Layers/Sqlite.ts",
        factory: "makeSqlitePersistenceLive",
        sqlClient: "effect/unstable/sql/SqlClient",
      };
}

export async function preserveForeignContent(kind, phase) {
  if (kind !== "foreign" && kind !== "successor") return undefined;
  const { before, after } = await phase();
  for (const [table, summary] of Object.entries(before.content.tables)) {
    if (table !== "jones_sql_migrations")
      NodeAssert.deepEqual(after.content.tables[table], summary, `foreign guard changed ${table}`);
  }
  for (const definition of before.content.definitions)
    NodeAssert.ok(
      after.content.definitions.some((item) => JSON.stringify(item) === JSON.stringify(definition)),
      "foreign guard changed a recorded schema definition",
    );
  NodeAssert.deepEqual(
    after.content.ledgers.jones_sql_migrations.filter(({ id }) => id < 100),
    before.content.ledgers.jones_sql_migrations.filter(({ id }) => id < 100),
  );
  return { unchangedForeignContent: true, adoptedForeignFeatures: false };
}
