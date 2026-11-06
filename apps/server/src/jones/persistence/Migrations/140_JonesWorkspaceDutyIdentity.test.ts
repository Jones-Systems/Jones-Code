import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import { migrationManifest, runMigrations } from "../../../persistence/Migrations.ts";
import { runJonesMigrations } from "../JonesMigrationGuard.ts";
import { jonesMigrationEntries } from "../JonesMigrations.ts";

it.effect("140 preserves legacy live duties and upstream identity without inventing physical owners", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] });
    yield* runJonesMigrations(jonesMigrationEntries.filter(([id]) => id < 140));
    yield* sql`INSERT INTO jones_workspace_duty_claims VALUES('legacy','/fixture',NULL,'session','owner','{}','2026-11-06T00:00:00Z',NULL)`;
    const duties = yield* sql`SELECT * FROM jones_workspace_duty_claims`;
    const ledger = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
    const schema = yield* sql`SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'jones_%' AND name NOT LIKE 'sqlite_%' ORDER BY type,name`;
    assert.deepEqual(yield* runJonesMigrations(jonesMigrationEntries), [[140, "WorkspaceDutyIdentity"]]);
    assert.deepEqual(yield* sql`SELECT * FROM jones_workspace_duty_claims`, duties);
    assert.deepEqual(yield* sql`SELECT * FROM jones_workspace_duty_identity`, []);
    assert.deepEqual(yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`, ledger);
    assert.deepEqual(yield* sql`SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'jones_%' AND name NOT LIKE 'sqlite_%' ORDER BY type,name`, schema);
    assert.deepEqual(yield* sql`SELECT * FROM pragma_foreign_key_list('jones_workspace_duty_identity')`, []);
    assert.deepEqual(yield* runJonesMigrations(jonesMigrationEntries), []);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);
