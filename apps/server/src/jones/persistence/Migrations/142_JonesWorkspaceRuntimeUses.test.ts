import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import { SqlError, UnknownError } from "effect/sql/SqlError";
import { migrationManifest, runMigrations } from "../../../persistence/Migrations.ts";
import { runJonesMigrations } from "../JonesMigrationGuard.ts";
import { jonesMigrationEntries } from "../JonesMigrations.ts";
import upgrade from "./142_JonesWorkspaceRuntimeUses.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const prior = jonesMigrationEntries.filter(([id]) => id < 142);
const prepare = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] });
  yield* runJonesMigrations(prior);
  yield* sql`CREATE TABLE synthetic_upgrade_preimage (identity TEXT PRIMARY KEY, value TEXT NOT NULL)`;
  yield* sql`INSERT INTO synthetic_upgrade_preimage VALUES ('original','preserved')`;
});
const objects = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<{ readonly name: string }>`SELECT type,name,tbl_name,sql FROM sqlite_master
    WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name`;
});

it.effect(
  "142 installs only the independent journal, preserves populated preimages and is idempotent",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* prepare;
      const before = yield* objects;
      const ledger = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
      const released = yield* sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`;
      assert.deepStrictEqual(yield* runJonesMigrations(jonesMigrationEntries), [
        [142, "WorkspaceRuntimeUses"],
      ]);
      assert.deepStrictEqual(
        (yield* objects).filter(({ name }) => !name.startsWith("jones_workspace_runtime_")),
        before,
      );
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,
        ledger,
      );
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM jones_sql_migrations WHERE migration_id<142 ORDER BY migration_id`,
        released,
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM synthetic_upgrade_preimage`, [
        { identity: "original", value: "preserved" },
      ]);
      for (const table of [
        "jones_workspace_runtime_uses",
        "jones_workspace_runtime_use_events",
        "jones_workspace_runtime_creation_facts",
      ]) {
        assert.deepStrictEqual(yield* sql.unsafe(`SELECT * FROM ${table}`), []);
        assert.deepStrictEqual(yield* sql.unsafe(`PRAGMA foreign_key_list(${table})`), []);
      }
      const after = yield* objects;
      assert.deepStrictEqual(
        after
          .filter(({ name }) => name.startsWith("jones_workspace_runtime_"))
          .map(({ name }) => name)
          .sort(),
        [
          "jones_workspace_runtime_uses",
          "jones_workspace_runtime_use_events",
          "jones_workspace_runtime_use_pending",
          "jones_workspace_runtime_use_deletion_conflict",
          "jones_workspace_runtime_deletion_use_conflict",
          "jones_workspace_runtime_uses_no_delete",
          "jones_workspace_runtime_uses_no_replace",
          "jones_workspace_runtime_use_events_no_delete",
          "jones_workspace_runtime_use_events_no_replace",
          "jones_workspace_runtime_uses_initial",
          "jones_workspace_runtime_uses_reserved",
          "jones_workspace_runtime_events_immutable",
          "jones_workspace_runtime_event_guard",
          "jones_workspace_runtime_event_transition",
          "jones_workspace_runtime_producer_identity",
          "jones_workspace_runtime_native_binding_identity",
          "jones_workspace_runtime_retirement_coverage",
          "jones_workspace_runtime_use_transition",
          "jones_workspace_runtime_installation",
          "jones_workspace_runtime_installation_no_update",
          "jones_workspace_runtime_installation_no_delete",
          "jones_workspace_runtime_installation_no_replace",
          "jones_workspace_runtime_creation_facts",
          "jones_workspace_runtime_creation_facts_no_update",
          "jones_workspace_runtime_creation_facts_no_delete",
          "jones_workspace_runtime_creation_facts_no_replace",
          "jones_workspace_runtime_creation_facts_original_source",
        ].sort(),
      );
      assert.deepStrictEqual(yield* runJonesMigrations(jonesMigrationEntries), []);
      assert.deepStrictEqual(yield* objects, after);
    }).pipe(Effect.provide(memory)),
);
it.effect("142 schema and ledger insertion share the actual migration runner transaction", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* prepare;
    const before = yield* objects;
    const released = yield* sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`;
    const failAfterDDL = upgrade.pipe(
      Effect.andThen(
        Effect.fail(
          new SqlError({
            reason: new UnknownError({ cause: "synthetic failure after complete journal DDL" }),
          }),
        ),
      ),
    );
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          runJonesMigrations([...prior, [142, "WorkspaceRuntimeUses", failAfterDDL]]),
        ),
      ),
    );
    assert.deepStrictEqual(yield* objects, before);
    assert.deepStrictEqual(
      yield* sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`,
      released,
    );
  }).pipe(Effect.provide(memory)),
);
it.effect(
  "142 refuses unexpected object definitions without advancing its ledger or leaving partial objects",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* prepare;
      yield* sql`CREATE TABLE jones_workspace_runtime_use_events (unexpected TEXT)`;
      const before = yield* objects;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(runJonesMigrations(jonesMigrationEntries))));
      assert.deepStrictEqual(yield* objects, before);
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM jones_sql_migrations WHERE migration_id=142`,
        [],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "142 rejects an unexpected predecessor definition before adding its reciprocal fences",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* prepare;
      yield* sql`ALTER TABLE jones_deletion_worktree_admissions ADD COLUMN synthetic_unexpected TEXT`;
      const before = yield* objects;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(runJonesMigrations(jonesMigrationEntries))));
      assert.deepStrictEqual(yield* objects, before);
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM jones_sql_migrations WHERE migration_id=142`,
        [],
      );
    }).pipe(Effect.provide(memory)),
);
