import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import { migrationManifest, runMigrations } from "../../../persistence/Migrations.ts";
import { runJonesMigrations } from "../JonesMigrationGuard.ts";
import { jonesMigrationEntries } from "../JonesMigrations.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const tables = ["owners", "admissions", "effect_links", "uses", "lifecycle_events"].map(
  (name) => `jones_ordinary_checkout_${name}`,
);
const now = "2026-10-18T00:00:00.000Z";
const seedOwner = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO jones_ordinary_checkout_owners VALUES('/repo','/repo','{"version":1,"device":"1","inode":"2","birth":"3"}',NULL,
    'lease','thread','birth',1,NULL,${now},${now},${now},0,0,0,'owned')`;
});
it.effect("109 creates empty sparse storage through only the Jones ledger", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] });
    yield* runJonesMigrations(jonesMigrationEntries.filter(([id]) => id < 109));
    const upstream = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
    const objects =
      yield* sql`SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name`;
    assert.deepEqual(yield* runJonesMigrations(jonesMigrationEntries), [
      [109, "OrdinaryCheckoutLifecycle"],
    ]);
    assert.deepEqual(
      yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,
      upstream,
    );
    for (const table of tables) {
      assert.deepEqual(yield* sql`SELECT * FROM ${sql(table)}`, []);
      const references = yield* sql<{
        readonly table: string;
      }>`SELECT "table" FROM pragma_foreign_key_list(${table})`;
      assert.isTrue(references.every((reference) => tables.includes(reference.table)));
    }
    const after = yield* sql<{
      readonly name: string;
    }>`SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name`;
    assert.deepEqual(
      after.filter((object) => !object.name.startsWith("jones_ordinary_checkout_")),
      objects,
    );
    assert.deepEqual(yield* runJonesMigrations(jonesMigrationEntries), []);
  }).pipe(Effect.provide(memory)),
);
it.effect("recognized foreign 009/011 history stays byte-preserved and unadopted", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] });
    yield* runJonesMigrations(jonesMigrationEntries.filter(([id]) => id < 109));
    for (const [index, name] of [
      "V2NativeAcceptance",
      "DeletionWorktreeAdmission",
      "OrdinaryCheckoutOwnership",
      "AttachmentCleanup",
      "OrdinaryCheckoutExecutionLifetime",
    ].entries())
      yield* sql`INSERT INTO jones_sql_migrations(migration_id,name) VALUES(${index + 7},${name})`;
    yield* sql`CREATE TABLE orchestration_v2_ordinary_checkout_admissions(lease_id TEXT PRIMARY KEY, admission_json TEXT)`;
    yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_admissions VALUES('old','{"copied":"historical only"}')`;
    yield* sql`CREATE TABLE orchestration_v2_ordinary_checkout_execution_associations(operation_id TEXT PRIMARY KEY,evidence_json TEXT)`;
    yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_execution_associations VALUES('old-operation','{"outcome":"unknown"}')`;
    const associations =
      yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_execution_associations`;
    const foreign = yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_admissions`;
    const schema =
      yield* sql`SELECT sql FROM sqlite_master WHERE name='orchestration_v2_ordinary_checkout_admissions'`;
    const ledger =
      yield* sql`SELECT * FROM jones_sql_migrations WHERE migration_id<100 ORDER BY migration_id`;
    yield* runJonesMigrations(jonesMigrationEntries);
    assert.deepEqual(
      yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_admissions`,
      foreign,
    );
    assert.deepEqual(
      yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_execution_associations`,
      associations,
    );
    assert.deepEqual(
      yield* sql`SELECT sql FROM sqlite_master WHERE name='orchestration_v2_ordinary_checkout_admissions'`,
      schema,
    );
    assert.deepEqual(
      yield* sql`SELECT * FROM jones_sql_migrations WHERE migration_id<100 ORDER BY migration_id`,
      ledger,
    );
    for (const table of tables) assert.deepEqual(yield* sql`SELECT * FROM ${sql(table)}`, []);
  }).pipe(Effect.provide(memory)),
);
it.effect("owner acquisition identity is immutable and replacement cannot erase fencing", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* seedOwner;
    const before = yield* sql`SELECT * FROM jones_ordinary_checkout_owners`;
    for (const mutation of [
      sql`UPDATE jones_ordinary_checkout_owners SET lease_id='other',revision=revision+1`,
      sql`UPDATE jones_ordinary_checkout_owners SET birth_event_id='recreated',revision=revision+1`,
      sql`UPDATE jones_ordinary_checkout_owners SET owner_epoch=1,revision=revision+1`,
      sql`UPDATE jones_ordinary_checkout_owners SET expires_at=${now}`,
      sql`DELETE FROM jones_ordinary_checkout_owners`,
      sql`INSERT OR REPLACE INTO jones_ordinary_checkout_owners SELECT * FROM jones_ordinary_checkout_owners`,
    ])
      assert.isTrue(Exit.isFailure(yield* Effect.exit(mutation)));
    assert.deepEqual(yield* sql`SELECT * FROM jones_ordinary_checkout_owners`, before);
  }).pipe(Effect.provide(memory)),
);
it.effect(
  "migration failure rolls back its objects and ledger without altering retained rows",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] });
      yield* runJonesMigrations(jonesMigrationEntries.filter(([id]) => id < 109));
      yield* sql`CREATE TABLE jones_ordinary_checkout_effect_links(retained TEXT)`;
      yield* sql`INSERT INTO jones_ordinary_checkout_effect_links VALUES('do not replace')`;
      const ledger = yield* sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(runJonesMigrations(jonesMigrationEntries))));
      assert.deepEqual(yield* sql`SELECT * FROM jones_ordinary_checkout_effect_links`, [
        { retained: "do not replace" },
      ]);
      assert.deepEqual(
        yield* sql`SELECT name FROM sqlite_master WHERE name='jones_ordinary_checkout_owners'`,
        [],
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`,
        ledger,
      );
    }).pipe(Effect.provide(memory)),
);
it.effect("lookup keys and recovery scans have Jones-owned indices", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    for (const query of [
      "SELECT * FROM jones_ordinary_checkout_admissions WHERE command_id='command'",
      "SELECT * FROM jones_ordinary_checkout_admissions WHERE subject_thread_id='thread' AND run_id='run'",
      "SELECT * FROM jones_ordinary_checkout_effect_links WHERE effect_id='effect'",
      "SELECT * FROM jones_ordinary_checkout_uses WHERE checkout_key='/repo' AND state='unknown'",
      "SELECT * FROM jones_ordinary_checkout_lifecycle_events WHERE operation_id='operation' AND association_id='association' ORDER BY ordinal",
    ]) {
      const plan = yield* sql.unsafe<{ readonly detail: string }>(`EXPLAIN QUERY PLAN ${query}`);
      assert.match(
        plan.map((row) => row.detail).join("; "),
        /SEARCH .* USING (?:COVERING )?INDEX/u,
      );
    }
  }).pipe(Effect.provide(memory)),
);
