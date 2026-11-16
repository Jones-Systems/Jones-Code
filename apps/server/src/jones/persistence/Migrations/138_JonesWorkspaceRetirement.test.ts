import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import { migrationManifest, runMigrations } from "../../../persistence/Migrations.ts";
import { runJonesMigrations } from "../JonesMigrationGuard.ts";
import { jonesMigrationEntries } from "../JonesMigrations.ts";

const through138 = jonesMigrationEntries.filter(([id]) => id <= 138);
const tables = [
  "jones_workspace_disposal_journal",
  "jones_workspace_duty_claims",
  "jones_workspace_duty_identity",
  "jones_workspace_retirement_intents",
  "jones_workspace_retirement_receipts",
  "jones_workspace_retirement_state",
];

it.effect(
  "138 atomically adds all retirement/duty tables through137, preserves upstream identity and repeats unchanged",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] });
      yield* runJonesMigrations(through138.filter(([id]) => id < 138));
      const schema =
        yield* sql`SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name`;
      const ledger = yield* sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`;
      const upstream = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
      const upstreamSchema =
        yield* sql`SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'jones_%' AND name NOT LIKE 'sqlite_%' ORDER BY type,name`;
      const failed = yield* runJonesMigrations(
        through138.map(
          ([id, name, migration]) =>
            [
              id,
              name,
              id === 138
                ? migration.pipe(Effect.andThen(Effect.die("controlled failure after all138 DDL")))
                : migration,
            ] as const,
        ),
      ).pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(failed));
      assert.deepEqual(
        yield* sql`SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name`,
        schema,
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`,
        ledger,
      );
      assert.deepEqual(yield* runJonesMigrations(through138), [[138, "WorkspaceRetirement"]]);
      assert.deepEqual(
        (yield* sql<{
          name: string;
        }>`SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'jones_workspace_%' AND name IN ('jones_workspace_disposal_journal','jones_workspace_duty_claims','jones_workspace_duty_identity','jones_workspace_retirement_intents','jones_workspace_retirement_receipts','jones_workspace_retirement_state') ORDER BY name`).map(
          (row) => row.name,
        ),
        tables,
      );
      for (const table of tables)
        assert.deepEqual(
          yield* sql.unsafe(`SELECT * FROM pragma_foreign_key_list('${table}')`),
          [],
        );
      assert.deepEqual(yield* sql`SELECT * FROM jones_workspace_duty_identity`, []);
      assert.deepEqual(
        yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,
        upstream,
      );
      assert.deepEqual(
        yield* sql`SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'jones_%' AND name NOT LIKE 'sqlite_%' ORDER BY type,name`,
        upstreamSchema,
      );
      const after = yield* sql`SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name`;
      const afterLedger = yield* sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`;
      assert.deepEqual(yield* runJonesMigrations(through138), []);
      assert.deepEqual(
        yield* sql`SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name`,
        after,
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`,
        afterLedger,
      );
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect(
  "a fresh current source database contains the complete138 feature without a140 migration",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      assert.deepEqual(
        yield* sql`SELECT migration_id,name FROM jones_sql_migrations WHERE migration_id BETWEEN 138 AND 139 ORDER BY migration_id`,
        [
          { migration_id: 138, name: "WorkspaceRetirement" },
          { migration_id: 139, name: "ConversationWorkspace" },
        ],
      );
      for (const table of tables)
        assert.equal(
          (yield* sql`SELECT name FROM sqlite_master WHERE type='table' AND name=${table}`).length,
          1,
        );
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);
