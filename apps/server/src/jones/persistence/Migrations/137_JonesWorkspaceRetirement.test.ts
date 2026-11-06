import { assert,it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import {migrationManifest,runMigrations} from "../../../persistence/Migrations.ts";
import {runJonesMigrations} from "../JonesMigrationGuard.ts";
import {jonesMigrationEntries} from "../JonesMigrations.ts";
it.effect("137 adds only Jones retirement tables and preserves upstream rows and ledger",()=>Effect.gen(function*(){
  const sql=yield* SqlClient.SqlClient;
  yield* runMigrations({toMigrationInclusive:migrationManifest.at(-1)![0]});
  yield* runJonesMigrations(jonesMigrationEntries.filter(([id])=>id<137));
  const before=yield* sql`SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'jones_%' AND name NOT LIKE 'sqlite_%' ORDER BY type,name`;
  const ledger=yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
  assert.deepEqual(yield* runJonesMigrations(jonesMigrationEntries),[[137,"WorkspaceRetirement"]]);
  assert.deepEqual(yield* sql`SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'jones_%' AND name NOT LIKE 'sqlite_%' ORDER BY type,name`,before);
  assert.deepEqual(yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,ledger);
  assert.deepEqual(yield* sql`SELECT * FROM pragma_foreign_key_list('jones_workspace_duty_claims')`,[]);
  assert.deepEqual(yield* runJonesMigrations(jonesMigrationEntries),[]);
}).pipe(Effect.provide(NodeSqliteClient.layer({filename:":memory:"}))));
