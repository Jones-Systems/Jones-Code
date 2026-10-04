import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Migrator from "effect/unstable/sql/Migrator";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { afterEach, vi } from "vite-plus/test";

import { migrationManifest, runMigrations } from "./Migrations.ts";
import JonesMigration0001 from "./Migrations/001_JonesWorktreeOwnershipLeases.ts";
import JonesMigration0002 from "./Migrations/002_JonesProjectionThreadRuntimeIdentity.ts";
import JonesMigration0003 from "./Migrations/003_JonesNativeCreationIntents.ts";
import JonesMigration0004 from "./Migrations/004_JonesNativeCreationCommandIdentities.ts";
import JonesMigration0005 from "./Migrations/005_JonesWorkstreamsNativeAttempts.ts";
import JonesMigration0006 from "./Migrations/006_JonesWorkstreamsProviderEnrollments.ts";

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
  yield* sql`CREATE TABLE upstream_57_probe (id INTEGER PRIMARY KEY)`;
});

const injectMigrations = (includeFutureUpstream = false) =>
  vi
    .spyOn(Migrator, "fromRecord")
    .mockImplementation((entries) =>
      fromRecord(
        Object.hasOwn(entries, "1_WorktreeOwnershipLeases")
          ? { "1_ForkProbe": forkProbe }
          : includeFutureUpstream
            ? { ...entries, "57_FutureUpstreamProbe": futureUpstreamProbe }
            : entries,
      ),
    );

afterEach(() => vi.restoreAllMocks());

it.effect("runs upstream before fork migration 1 on a fresh database", () =>
  Effect.gen(function* () {
    injectMigrations();
    const sql = yield* SqlClient.SqlClient;

    assert.deepEqual(yield* runMigrations(), migrationManifest);
    assert.deepEqual(yield* sql`SELECT upstream_max FROM fork_probe`, [{ upstream_max: 56 }]);
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
    yield* runMigrations({ toMigrationInclusive: 54 });
    injectMigrations();

    assert.deepEqual(
      yield* runMigrations(),
      migrationManifest.filter(([id]) => id > 54),
    );
    assert.deepEqual(yield* runMigrations(), []);
    assert.deepEqual(yield* sql`SELECT upstream_max FROM fork_probe`, [{ upstream_max: 56 }]);
    assert.deepEqual(yield* sql`SELECT migration_id, name FROM jones_sql_migrations`, [
      { migration_id: 1, name: "ForkProbe" },
    ]);
    assert.deepEqual(yield* sql`SELECT MAX(migration_id) AS id FROM effect_sql_migrations`, [
      { id: 56 },
    ]);
  }).pipe(Effect.provide(memory)),
);

it.effect("applies future upstream migration 57 after fork migration 1", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const injected = injectMigrations();
    yield* runMigrations();
    injected.mockRestore();
    injectMigrations(true);

    assert.deepEqual(yield* runMigrations(), [[57, "FutureUpstreamProbe"]]);
    assert.deepEqual(yield* runMigrations(), []);
    assert.deepEqual(yield* sql`SELECT * FROM upstream_57_probe`, []);
    assert.deepEqual(yield* sql`SELECT upstream_max FROM fork_probe`, [{ upstream_max: 56 }]);
    assert.deepEqual(yield* sql`SELECT MAX(migration_id) AS id FROM effect_sql_migrations`, [
      { id: 57 },
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
    assert.deepEqual(
      yield* runMigrations(),
      migrationManifest.filter(([id]) => id > 53),
    );
    assert.deepEqual(yield* runMigrations(), []);
    assert.deepEqual(yield* sql`SELECT migration_id, name FROM jones_sql_migrations`, [
      { migration_id: 1, name: "WorktreeOwnershipLeases" },
      { migration_id: 2, name: "ProjectionThreadRuntimeIdentity" },
      { migration_id: 3, name: "NativeCreationIntents" },
      { migration_id: 4, name: "NativeCreationCommandIdentities" },
      { migration_id: 5, name: "WorkstreamsNativeAttempts" },
      { migration_id: 6, name: "WorkstreamsProviderEnrollments" },
      { migration_id: 7, name: "V2NativeAcceptance" },
      { migration_id: 8, name: "DeletionWorktreeAdmission" },
      { migration_id: 9, name: "OrdinaryCheckoutOwnership" },
      { migration_id: 10, name: "AttachmentCleanup" },
      { migration_id: 11, name: "OrdinaryCheckoutExecutionLifetime" },
      { migration_id: 12, name: "ImportedApplicationAttachments" },
      { migration_id: 13, name: "CommandNormalizationWitness" },
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
    assert.deepEqual(
      yield* runMigrations(),
      migrationManifest.filter(([id]) => id > 53),
    );
    assert.deepEqual(yield* sql`SELECT upstream_max FROM fork_probe`, [{ upstream_max: 56 }]);
  }).pipe(Effect.provide(memory)),
);

for (const limit of [54, 55, 56, 57]) {
  it.effect(`skips every Jones migration under explicit upstream limit ${limit}`, () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      assert.deepEqual(
        yield* runMigrations({ toMigrationInclusive: limit }),
        migrationManifest.filter(([id]) => id <= limit),
      );
      assert.deepEqual(yield* runMigrations({ toMigrationInclusive: limit }), []);
      assert.deepEqual(
        yield* sql`SELECT name FROM sqlite_master WHERE name IN ('jones_sql_migrations', 'worktree_ownership_leases', 'orchestration_v2_native_command_identities')`,
        [],
      );
      assert.deepEqual(
        yield* runMigrations(),
        migrationManifest.filter(([id]) => id > limit),
      );
      assert.deepEqual(yield* runMigrations(), []);
      assert.equal((yield* sql`SELECT * FROM jones_sql_migrations`).length, 13);
    }).pipe(Effect.provide(memory)),
  );
}

const releasedJonesMigrations = [
  [1, "WorktreeOwnershipLeases", JonesMigration0001],
  [2, "ProjectionThreadRuntimeIdentity", JonesMigration0002],
  [3, "NativeCreationIntents", JonesMigration0003],
  [4, "NativeCreationCommandIdentities", JonesMigration0004],
  [5, "WorkstreamsNativeAttempts", JonesMigration0005],
  [6, "WorkstreamsProviderEnrollments", JonesMigration0006],
] as const;

for (const prefixLength of [0, 1, 2, 3, 4, 5, 6]) {
  it.effect(
    `upgrades upstream54 with Jones prefix ${prefixLength} without changing released history or schema`,
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations({ toMigrationInclusive: 54 });
        if (prefixLength > 0) {
          yield* Migrator.make({})({
            loader: Migrator.fromRecord(
              Object.fromEntries(
                releasedJonesMigrations
                  .slice(0, prefixLength)
                  .map(([id, name, migration]) => [`${id}_${name}`, migration]),
              ),
            ),
            table: "jones_sql_migrations",
          });
        }
        const releasedSchema = yield* sql<{ readonly name: string; readonly sql: string }>`
        SELECT name, sql FROM sqlite_master
        WHERE name LIKE 'worktree_ownership_leases%' OR name LIKE 'native_creation_%' OR name LIKE 'workstreams_native_%'
        ORDER BY name
      `;
        const releasedHistory =
          prefixLength === 0
            ? []
            : yield* sql`
        SELECT * FROM jones_sql_migrations ORDER BY migration_id
      `;
        assert.deepEqual(
          yield* runMigrations(),
          migrationManifest.filter(([id]) => id > 54),
        );
        assert.deepEqual(yield* runMigrations(), []);
        assert.deepEqual(
          yield* sql`SELECT * FROM jones_sql_migrations WHERE migration_id <= ${prefixLength} ORDER BY migration_id`,
          releasedHistory,
        );
        for (const row of releasedSchema) {
          assert.deepEqual(
            yield* sql`SELECT name, sql FROM sqlite_master WHERE name = ${row.name}`,
            [row],
          );
        }
        assert.deepEqual(
          yield* sql`SELECT migration_id, name FROM jones_sql_migrations ORDER BY migration_id`,
          [
            ...releasedJonesMigrations.map(([migration_id, name]) => ({ migration_id, name })),
            { migration_id: 7, name: "V2NativeAcceptance" },
            { migration_id: 8, name: "DeletionWorktreeAdmission" },
            { migration_id: 9, name: "OrdinaryCheckoutOwnership" },
            { migration_id: 10, name: "AttachmentCleanup" },
            { migration_id: 11, name: "OrdinaryCheckoutExecutionLifetime" },
            { migration_id: 12, name: "ImportedApplicationAttachments" },
            { migration_id: 13, name: "CommandNormalizationWitness" },
          ],
        );
      }).pipe(Effect.provide(memory)),
  );
}

it.effect("preserves divergent names and unknown later upstream history on repeated startup", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* sql`UPDATE effect_sql_migrations SET name = 'OtherSchema' WHERE migration_id = 55`;
    yield* sql`INSERT INTO effect_sql_migrations (migration_id, name) VALUES (99, 'LaterSchema')`;
    const history = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
    assert.deepEqual(yield* runMigrations(), []);
    assert.deepEqual(yield* runMigrations(), []);
    assert.deepEqual(
      yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,
      history,
    );
    assert.equal((yield* sql`SELECT * FROM jones_sql_migrations`).length, 13);
  }).pipe(Effect.provide(memory)),
);
