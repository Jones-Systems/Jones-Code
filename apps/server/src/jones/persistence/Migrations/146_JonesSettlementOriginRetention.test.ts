import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlitePersistence from "../../../persistence/Sqlite.ts";

const indexes = [
  ["jones_client_settlement_commands", "jones_client_settlement_commands_by_recorded_at"],
  ["jones_self_settlement_project_items", "jones_self_settlement_project_items_by_recorded_at"],
] as const;

it.effect("146 indexes the age column of both origin tables, and the age sweep uses it", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const ledger = yield* sql<{
      readonly name: string;
    }>`SELECT name FROM jones_sql_migrations WHERE migration_id = 146`;
    assert.strictEqual(ledger[0]?.name, "SettlementOriginRetention");
    for (const [table, index] of indexes) {
      const found = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ${table} AND name = ${index}`;
      assert.lengthOf(found, 1);
      const plan = yield* sql.unsafe<{ readonly detail: string }>(
        `EXPLAIN QUERY PLAN SELECT command_id FROM ${table} WHERE recorded_at < ? ORDER BY recorded_at LIMIT 200`,
        ["2026-01-01T00:00:00.000Z"],
      );
      assert.isTrue(
        plan.some((row) => row.detail.includes(index)),
        `the sweep over ${table} must use ${index}`,
      );
    }
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);
