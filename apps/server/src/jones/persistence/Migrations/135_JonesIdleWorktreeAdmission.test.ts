import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import { migrationManifest, runMigrations } from "../../../persistence/Migrations.ts";
import { runJonesMigrations } from "../JonesMigrationGuard.ts";
import { jonesMigrationEntries } from "../JonesMigrations.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });

it.effect("135 adds only sparse Jones storage and leaves upstream objects and its ledger unchanged", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] });
    yield* runJonesMigrations(jonesMigrationEntries.filter(([id]) => id < 135));
    const ledger = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
    const objects = yield* sql`SELECT type, name, tbl_name, sql FROM sqlite_master
      WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name`;
    assert.deepEqual(yield* runJonesMigrations(jonesMigrationEntries), [
      [135, "IdleWorktreeAdmission"],
    ]);
    assert.deepEqual(yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`, ledger);
    assert.deepEqual(yield* sql`SELECT * FROM jones_idle_worktree_admission`, []);
    assert.deepEqual(
      yield* sql`SELECT * FROM pragma_foreign_key_list('jones_idle_worktree_admission')`,
      [],
    );
    const after = yield* sql<{ readonly name: string }>`SELECT type, name, tbl_name, sql FROM sqlite_master
      WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name`;
    assert.deepEqual(
      after.filter(({ name }) => !name.includes("jones_idle_worktree_admission")),
      objects,
    );
    assert.deepEqual(yield* runJonesMigrations(jonesMigrationEntries), []);
  }).pipe(Effect.provide(memory)),
);
