import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import { migrationManifest, runMigrations } from "../../../persistence/Migrations.ts";
import { runJonesMigrations } from "../JonesMigrationGuard.ts";
import { jonesMigrationEntries } from "../JonesMigrations.ts";

const through136 = jonesMigrationEntries.filter(([id]) => id <= 136);

it.effect(
  "136 adds provenance facts without altering upstream schema, rows, ledgers or lifecycle generations",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] });
      yield* runJonesMigrations(jonesMigrationEntries.filter(([id]) => id < 136));
      const before =
        yield* sql`SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name`;
      const ledger = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
      const transitions = yield* sql`SELECT * FROM jones_workspace_lifecycle_transitions`;
      const bindings = yield* sql`SELECT * FROM jones_workspace_lifecycle_bindings`;
      assert.deepEqual(yield* runJonesMigrations(through136), [
        [136, "WorkspaceProvenance"],
      ]);
      const after = yield* sql<{
        name: string;
      }>`SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name`;
      assert.deepEqual(
        after.filter((r) => !r.name.startsWith("jones_workspace_provenance")),
        before,
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,
        ledger,
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM jones_workspace_lifecycle_transitions`,
        transitions,
      );
      assert.deepEqual(yield* sql`SELECT * FROM jones_workspace_lifecycle_bindings`, bindings);
      assert.deepEqual(
        yield* sql`SELECT * FROM pragma_foreign_key_list('jones_workspace_provenance')`,
        [],
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM pragma_foreign_key_list('jones_workspace_provenance_threads')`,
        [],
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM pragma_foreign_key_list('jones_workspace_provenance_runs')`,
        [],
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM pragma_foreign_key_list('jones_workspace_provenance_facts')`,
        [],
      );
      assert.deepEqual(yield* runJonesMigrations(through136), []);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);
