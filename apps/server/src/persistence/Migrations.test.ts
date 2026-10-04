import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Migrator from "effect/unstable/sql/Migrator";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { afterEach, vi } from "vite-plus/test";

import { migrationManifest, runMigrations } from "./Migrations.ts";

vi.mock("effect/unstable/sql/Migrator", async (importOriginal) => ({
  ...(await importOriginal<typeof Migrator>()),
}));

const fromRecord = Migrator.fromRecord;
const memory = NodeSqliteClient.layer({ filename: ":memory:" });

const forkProbe = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`SELECT auto_settle_disabled_at FROM projection_threads`;
  yield* sql`CREATE TABLE fork_probe (upstream_max INTEGER NOT NULL)`;
  yield* sql`INSERT INTO fork_probe SELECT MAX(migration_id) FROM effect_sql_migrations`;
});

const futureUpstreamProbe = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE upstream_55_probe (id INTEGER PRIMARY KEY)`;
});

const injectMigrations = (includeFutureUpstream = false) =>
  vi
    .spyOn(Migrator, "fromRecord")
    .mockImplementation((entries) =>
      fromRecord(
        Object.hasOwn(entries, "1_WorktreeOwnershipLeases")
          ? { "1_ForkProbe": forkProbe }
          : includeFutureUpstream
            ? { ...entries, "55_FutureUpstreamProbe": futureUpstreamProbe }
            : entries,
      ),
    );

afterEach(() => vi.restoreAllMocks());

it.effect("runs upstream before fork migration 1 on a fresh database", () =>
  Effect.gen(function* () {
    injectMigrations();
    const sql = yield* SqlClient.SqlClient;

    assert.deepEqual(yield* runMigrations(), migrationManifest);
    assert.deepEqual(yield* sql`SELECT upstream_max FROM fork_probe`, [{ upstream_max: 54 }]);
    assert.deepEqual(yield* sql`SELECT migration_id, name FROM jones_sql_migrations`, [
      { migration_id: 1, name: "ForkProbe" },
    ]);
    assert.deepEqual(
      yield* sql`SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id`,
      migrationManifest.map(([migration_id, name]) => ({ migration_id, name })),
    );
  }).pipe(Effect.provide(memory)),
);

it.effect("applies fork migration 1 to an existing upstream-54 database exactly once", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* sql`DROP TABLE IF EXISTS jones_sql_migrations`;
    injectMigrations();

    assert.deepEqual(yield* runMigrations(), []);
    assert.deepEqual(yield* runMigrations(), []);
    assert.deepEqual(yield* sql`SELECT upstream_max FROM fork_probe`, [{ upstream_max: 54 }]);
    assert.deepEqual(yield* sql`SELECT migration_id, name FROM jones_sql_migrations`, [
      { migration_id: 1, name: "ForkProbe" },
    ]);
    assert.deepEqual(yield* sql`SELECT MAX(migration_id) AS id FROM effect_sql_migrations`, [
      { id: 54 },
    ]);
  }).pipe(Effect.provide(memory)),
);

it.effect("applies future upstream migration 55 after fork migration 1", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const injected = injectMigrations();
    yield* runMigrations();
    injected.mockRestore();
    injectMigrations(true);

    assert.deepEqual(yield* runMigrations(), [[55, "FutureUpstreamProbe"]]);
    assert.deepEqual(yield* runMigrations(), []);
    assert.deepEqual(yield* sql`SELECT * FROM upstream_55_probe`, []);
    assert.deepEqual(yield* sql`SELECT upstream_max FROM fork_probe`, [{ upstream_max: 54 }]);
    assert.deepEqual(yield* sql`SELECT MAX(migration_id) AS id FROM effect_sql_migrations`, [
      { id: 55 },
    ]);
    assert.deepEqual(yield* sql`SELECT migration_id, name FROM jones_sql_migrations`, [
      { migration_id: 1, name: "ForkProbe" },
    ]);
  }).pipe(Effect.provide(memory)),
);

it.effect("runs fork migration 1 after completing historical upstream replay", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;

    assert.deepEqual(
      yield* runMigrations({ toMigrationInclusive: 53 }),
      migrationManifest.filter(([id]) => id <= 53),
    );
    assert.deepEqual(yield* runMigrations(), [[54, "ProjectionThreadsAutoSettleDisabledAt"]]);
    assert.deepEqual(yield* runMigrations(), []);
    assert.deepEqual(yield* sql`SELECT migration_id, name FROM jones_sql_migrations`, [
      { migration_id: 1, name: "WorktreeOwnershipLeases" },
      { migration_id: 2, name: "ProjectionThreadRuntimeIdentity" },
      { migration_id: 3, name: "NativeCreationIntents" },
      { migration_id: 4, name: "NativeCreationCommandIdentities" },
      { migration_id: 5, name: "WorkstreamsNativeAttempts" },
      { migration_id: 6, name: "WorkstreamsProviderEnrollments" },
      { migration_id: 7, name: "ThreadCreationLookupIndex" },
    ]);
    assert.deepEqual(
      yield* sql`SELECT name FROM sqlite_master WHERE name = 'worktree_ownership_leases'`,
      [{ name: "worktree_ownership_leases" }],
    );
    assert.deepEqual(
      yield* sql`SELECT name FROM sqlite_master
        WHERE name IN ('workstreams_native_attempts', 'workstreams_native_enrollments') ORDER BY name`,
      [{ name: "workstreams_native_attempts" }, { name: "workstreams_native_enrollments" }],
    );
  }).pipe(Effect.provide(memory)),
);

it.effect("keeps historical upstream replay independent of pending fork migrations", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    injectMigrations();

    assert.deepEqual(
      yield* runMigrations({ toMigrationInclusive: 53 }),
      migrationManifest.filter(([id]) => id <= 53),
    );
    assert.deepEqual(
      yield* sql`SELECT name FROM sqlite_master WHERE name IN ('jones_sql_migrations', 'fork_probe')`,
      [],
    );
    assert.deepEqual(yield* runMigrations(), [[54, "ProjectionThreadsAutoSettleDisabledAt"]]);
    assert.deepEqual(yield* sql`SELECT upstream_max FROM fork_probe`, [{ upstream_max: 54 }]);
  }).pipe(Effect.provide(memory)),
);
