import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Logger from "effect/Logger";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import { runJonesMigrations } from "./JonesMigrationGuard.ts";
import { migrationManifest, runMigrations } from "../../persistence/Migrations.ts";
import Jones001 from "./Migrations/001_JonesWorktreeOwnershipLeases.ts";
import Jones002 from "./Migrations/002_JonesProjectionThreadRuntimeIdentity.ts";
import Jones003 from "./Migrations/003_JonesNativeCreationIntents.ts";
import Jones004 from "./Migrations/004_JonesNativeCreationCommandIdentities.ts";
import Jones005 from "./Migrations/005_JonesWorkstreamsNativeAttempts.ts";
import Jones006 from "./Migrations/006_JonesWorkstreamsProviderEnrollments.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const originals = [
  [1, "WorktreeOwnershipLeases", Jones001],
  [2, "ProjectionThreadRuntimeIdentity", Jones002],
  [3, "NativeCreationIntents", Jones003],
  [4, "NativeCreationCommandIdentities", Jones004],
  [5, "WorkstreamsNativeAttempts", Jones005],
  [6, "WorkstreamsProviderEnrollments", Jones006],
] as const;
// Released builds of 002 added this upstream column; the identity now has no effect.
const released002 = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`ALTER TABLE projection_thread_sessions ADD COLUMN runtime_identity_json TEXT`;
});
const releasedEffects = originals.map(
  ([id, name, migration]) => [id, name, id === 2 ? released002 : migration] as const,
);
const names = originals.map(([migration_id, name]) => ({ migration_id, name }));
const foreignV2Names = [
  "V2NativeAcceptance",
  "DeletionWorktreeAdmission",
  "OrdinaryCheckoutOwnership",
  "AttachmentCleanup",
  "OrdinaryCheckoutExecutionLifetime",
  "ImportedApplicationAttachments",
  "CommandNormalizationWitness",
];
const readLedger = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql`SELECT migration_id, name, created_at FROM jones_sql_migrations ORDER BY migration_id`;
});
const readSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql`SELECT type, name, sql FROM sqlite_master ORDER BY type, name`;
});

const seedLegacy = (prefix = 6) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 54 });
    yield* sql`CREATE TABLE jones_sql_migrations (
    migration_id integer PRIMARY KEY NOT NULL,
    created_at datetime NOT NULL DEFAULT current_timestamp,
    name VARCHAR(255) NOT NULL
  )`;
    for (const [id, name, migration] of releasedEffects.slice(0, prefix)) {
      yield* migration;
      yield* sql`INSERT INTO jones_sql_migrations (migration_id, name) VALUES (${id}, ${name})`;
    }
  });

const futureMigration = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_future_probe (value TEXT NOT NULL)`;
  yield* sql`INSERT INTO jones_future_probe VALUES ('applied')`;
});
const futureEntries = [...originals, [100, "FutureProbe", futureMigration] as const];

it.effect(
  "runs upstream 1–59, the six released Jones effects and receiving native execution once",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      assert.deepStrictEqual(yield* runMigrations(), migrationManifest);
      assert.deepStrictEqual(
        yield* sql`SELECT migration_id, name FROM jones_sql_migrations WHERE migration_id < 100 ORDER BY migration_id`,
        names,
      );
      assert.deepStrictEqual(
        yield* sql`SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id`,
        migrationManifest.map(([migration_id, name]) => ({ migration_id, name })),
      );
      assert.isFalse(
        (yield* sql<{ readonly name: string }>`PRAGMA table_info(projection_thread_sessions)`).some(
          ({ name }) => name === "runtime_identity_json",
        ),
      );
      assert.deepStrictEqual(
        yield* sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name IN (
      'worktree_ownership_leases', 'native_creation_intents', 'native_creation_reserved_command_identities',
      'workstreams_native_attempts', 'workstreams_native_enrollments'
    ) ORDER BY name`,
        [
          { name: "native_creation_intents" },
          { name: "native_creation_reserved_command_identities" },
          { name: "workstreams_native_attempts" },
          { name: "workstreams_native_enrollments" },
          { name: "worktree_ownership_leases" },
        ],
      );
      const ledger = yield* readLedger;
      const schema = yield* readSchema;
      assert.deepStrictEqual(yield* runMigrations(), []);
      assert.deepStrictEqual(yield* readLedger, ledger);
      assert.deepStrictEqual(yield* readSchema, schema);
      yield* sql`INSERT INTO workstreams_native_attempts VALUES (
      'owner', 'principal', 'command', '{}', ${"a".repeat(64)}, ${"b".repeat(64)}, '{}', 'native', 'created', NULL
    )`;
      yield* sql`UPDATE workstreams_native_attempts SET dispatch_started_at = 'started'`;
      assert.ok(
        Exit.isFailure(
          yield* Effect.exit(
            sql`UPDATE workstreams_native_attempts SET dispatch_started_at = 'again'`,
          ),
        ),
      );
      yield* sql`INSERT INTO native_creation_automation_enrollments VALUES ('session', 'enrolled')`;
      assert.ok(
        Exit.isFailure(yield* Effect.exit(sql`DELETE FROM native_creation_automation_enrollments`)),
      );
    }).pipe(Effect.provide(memory)),
);

it.effect.each([0, 1, 2, 3, 4, 5, 6])(
  "preserves released upstream-54 / Jones-%i history while filling the exact prefix",
  (prefix) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* seedLegacy(prefix);
      const before = yield* readLedger;
      if (prefix > 0) {
        yield* sql`INSERT INTO worktree_ownership_leases VALUES (
          '/synthetic/worktree', 'lease', 'thread', 'incarnation', 'branch', 1, 2, 3
        )`;
      }
      if (prefix >= 3) {
        yield* sql`INSERT INTO native_creation_intents VALUES (
          'claim', 'operation', 'preparation', 'command', 'thread', 'message',
          '/synthetic/project', 'branch', '/synthetic/worktree', '{}', '{}'
        )`;
      }
      if (prefix >= 5) {
        yield* sql`INSERT INTO workstreams_native_attempts VALUES (
          'owner', 'principal', 'command', '{}', ${"a".repeat(64)}, ${"b".repeat(64)}, '{}', 'native', 'created', NULL
        )`;
      }
      const intents = prefix >= 3 ? yield* sql`SELECT * FROM native_creation_intents` : [];
      const attempts = prefix >= 5 ? yield* sql`SELECT * FROM workstreams_native_attempts` : [];
      const oldSchema = yield* sql`SELECT type, name, sql FROM sqlite_master
        WHERE name LIKE 'native_creation_%' OR name LIKE 'workstreams_%' OR name LIKE '%worktree_ownership%' ORDER BY name`;
      assert.deepStrictEqual(
        yield* runMigrations(),
        migrationManifest.filter(([id]) => id > 54),
      );
      assert.deepStrictEqual((yield* readLedger).slice(0, prefix), before);
      assert.deepStrictEqual(
        yield* sql`SELECT migration_id, name FROM jones_sql_migrations WHERE migration_id < 100 ORDER BY migration_id`,
        names,
      );
      // 105 rebuilds this table without its upstream reference; UpstreamIsolation.test.ts
      // covers that shape. Its triggers keep their released text.
      for (const row of oldSchema.filter(
        ({ type, name }) => type !== "table" || name !== "workstreams_native_enrollments",
      )) {
        assert.ok(
          (yield* readSchema).some(
            (current) => current.name === row.name && current.sql === row.sql,
          ),
        );
      }
      if (prefix > 0) {
        assert.deepStrictEqual(yield* sql`SELECT * FROM worktree_ownership_leases`, [
          {
            resource_path: "/synthetic/worktree",
            lease_id: "lease",
            owner_thread_id: "thread",
            owner_incarnation: "incarnation",
            branch: "branch",
            acquired_at_ms: 1,
            renewed_at_ms: 2,
            expires_at_ms: 3,
          },
        ]);
      }
      if (prefix >= 3)
        assert.deepStrictEqual(yield* sql`SELECT * FROM native_creation_intents`, intents);
      if (prefix >= 5)
        assert.deepStrictEqual(yield* sql`SELECT * FROM workstreams_native_attempts`, attempts);
      assert.deepStrictEqual(yield* runMigrations(), []);
    }).pipe(Effect.provide(memory)),
);

it.effect("preserves applied lookup 007 and still applies a registered future migration", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* seedLegacy();
    yield* sql`CREATE INDEX IF NOT EXISTS idx_orch_events_thread_creation_lookup
      ON orchestration_events (stream_id, sequence DESC, event_id)
      WHERE aggregate_kind = 'thread' AND event_type = 'thread.created'`;
    yield* sql`INSERT INTO jones_sql_migrations (migration_id, name) VALUES (7, 'ThreadCreationLookupIndex')`;
    const before = yield* readLedger;
    const index =
      yield* sql`SELECT sql FROM sqlite_master WHERE name = 'idx_orch_events_thread_creation_lookup'`;
    // Keep the original FutureProbe fixture isolated from the installed receiving migration 100.
    yield* runJonesMigrations(originals);
    assert.deepStrictEqual(yield* readLedger, before);
    assert.deepStrictEqual(yield* runJonesMigrations(futureEntries), [[100, "FutureProbe"]]);
    assert.deepStrictEqual(yield* runJonesMigrations(futureEntries), []);
    assert.deepStrictEqual((yield* readLedger).slice(0, 7), before);
    assert.deepStrictEqual(
      yield* sql`SELECT sql FROM sqlite_master WHERE name = 'idx_orch_events_thread_creation_lookup'`,
      index,
    );
    assert.deepStrictEqual(yield* sql`SELECT * FROM jones_future_probe`, [{ value: "applied" }]);
  }).pipe(Effect.provide(memory)),
);

it.effect.each([5, 7])(
  "preserves known foreign V2 history prefix %i and nonempty foreign tables",
  (foreignPrefix) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* seedLegacy();
      // Exact table DDL from #91 09ead6ea, 007_JonesV2NativeAcceptance.ts.
      // This fixture tests inert preservation, not adoption of the foreign feature.
      yield* sql`CREATE TABLE orchestration_v2_provider_runtime_evidence (
        thread_id TEXT PRIMARY KEY NOT NULL,
        provider_thread_id TEXT NOT NULL,
        provider_session_id TEXT NOT NULL,
        provider_instance_id TEXT NOT NULL,
        driver TEXT NOT NULL,
        native_thread_id TEXT,
        runtime_generation TEXT NOT NULL,
        evidence_revision INTEGER NOT NULL CHECK(evidence_revision > 0),
        observation_json TEXT CHECK(observation_json IS NULL OR json_valid(observation_json)),
        registered_at TEXT NOT NULL
      )`;
      yield* sql`INSERT INTO orchestration_v2_provider_runtime_evidence VALUES (
        'thread', 'provider-thread', 'provider-session', 'provider-instance', 'codex',
        'native-thread', 'generation', 1, '{}', 'registered'
      )`;
      for (const [offset, name] of foreignV2Names.slice(0, foreignPrefix).entries()) {
        yield* sql`INSERT INTO jones_sql_migrations (migration_id, name) VALUES (${offset + 7}, ${name})`;
      }
      const before = yield* readLedger;
      const rows = yield* sql`SELECT * FROM orchestration_v2_provider_runtime_evidence`;
      const schema =
        yield* sql`SELECT sql FROM sqlite_master WHERE name = 'orchestration_v2_provider_runtime_evidence'`;
      const logs: Array<unknown> = [];
      const logger = Logger.make<unknown, void>(({ message }) => {
        logs.push(...(Array.isArray(message) ? message : [message]));
      });
      yield* runJonesMigrations(originals).pipe(Effect.provide(Logger.layer([logger])));
      assert.include(
        logs,
        "Preserving known foreign Jones migration history without adopting its features",
      );
      assert.deepStrictEqual(yield* readLedger, before);
      assert.deepStrictEqual(yield* runJonesMigrations(futureEntries), [[100, "FutureProbe"]]);
      assert.deepStrictEqual((yield* readLedger).slice(0, before.length), before);
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM orchestration_v2_provider_runtime_evidence`,
        rows,
      );
      assert.deepStrictEqual(
        yield* sql`SELECT sql FROM sqlite_master WHERE name = 'orchestration_v2_provider_runtime_evidence'`,
        schema,
      );
    }).pipe(Effect.provide(memory)),
);

it.effect.each([
  [
    "mismatched original name",
    "UPDATE jones_sql_migrations SET name = 'Wrong' WHERE migration_id = 2",
  ],
  [
    "unknown foreign name",
    "INSERT INTO jones_sql_migrations (migration_id, name) VALUES (7, 'Unknown')",
  ],
  [
    "unknown reserved ID",
    "INSERT INTO jones_sql_migrations (migration_id, name) VALUES (99, 'Unknown')",
  ],
  ["missing original prefix", "DELETE FROM jones_sql_migrations WHERE migration_id = 3"],
  [
    "foreign gap",
    "INSERT INTO jones_sql_migrations (migration_id, name) VALUES (8, 'DeletionWorktreeAdmission')",
  ],
  [
    "mixed foreign chains",
    "INSERT INTO jones_sql_migrations (migration_id, name) VALUES (7, 'ThreadCreationLookupIndex'), (8, 'DeletionWorktreeAdmission')",
  ],
  [
    "unknown rebuild ID",
    "INSERT INTO jones_sql_migrations (migration_id, name) VALUES (101, 'Unregistered')",
  ],
] as const)("fails closed before Jones writes for %s", ([_label, corrupt]) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* seedLegacy();
    yield* runMigrations({ toMigrationInclusive: 59 });
    yield* sql.unsafe(corrupt);
    const before = yield* readLedger;
    const schema = yield* readSchema;
    const result = yield* Effect.exit(runMigrations());
    assert.ok(Exit.isFailure(result));
    if (Exit.isFailure(result))
      assert.include(Cause.pretty(result.cause), "Jones migration history");
    assert.deepStrictEqual(yield* readLedger, before);
    assert.deepStrictEqual(yield* readSchema, schema);
    const future = yield* Effect.exit(runJonesMigrations(futureEntries));
    assert.ok(Exit.isFailure(future));
    assert.deepStrictEqual(yield* readLedger, before);
    assert.deepStrictEqual(yield* readSchema, schema);
  }).pipe(Effect.provide(memory)),
);

it.effect("rejects a future ledger gap instead of skipping a pending lower registered ID", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* seedLegacy();
    yield* sql`INSERT INTO jones_sql_migrations (migration_id, name) VALUES (101, 'LaterProbe')`;
    const before = yield* readLedger;
    const result = yield* Effect.exit(
      runJonesMigrations([...futureEntries, [101, "LaterProbe", Effect.void]]),
    );
    assert.ok(Exit.isFailure(result));
    assert.deepStrictEqual(yield* readLedger, before);
    assert.deepStrictEqual(
      yield* sql`SELECT name FROM sqlite_master WHERE name = 'jones_future_probe'`,
      [],
    );
  }).pipe(Effect.provide(memory)),
);

it.effect("an explicit upstream limit leaves absent and invalid Jones history untouched", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 59 }), migrationManifest);
    assert.deepStrictEqual(
      yield* sql`SELECT name FROM sqlite_master WHERE name = 'jones_sql_migrations' OR name = 'worktree_ownership_leases'`,
      [],
    );
    yield* sql`CREATE TABLE jones_sql_migrations (migration_id INTEGER PRIMARY KEY, name TEXT)`;
    yield* sql`INSERT INTO jones_sql_migrations VALUES (99, 'Unknown')`;
    const before = yield* sql`SELECT * FROM jones_sql_migrations`;
    assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 59 }), []);
    assert.deepStrictEqual(yield* sql`SELECT * FROM jones_sql_migrations`, before);
  }).pipe(Effect.provide(memory)),
);

const encodePreexistingRuntimeIdentity = Schema.encodeSync(
  Schema.fromJsonString(Schema.Struct({ runtimeGeneration: Schema.String })),
);

it.effect("leaves bounded upstream replay untouched and thread sessions in upstream shape", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 54 });
    const before = yield* sql<{ name: string }>`PRAGMA table_info(projection_thread_sessions)`;
    assert.isFalse(before.some((column) => column.name === "runtime_identity_json"));
    assert.deepEqual(
      yield* sql`SELECT name FROM sqlite_master WHERE name = 'jones_sql_migrations'`,
      [],
    );
    yield* sql`INSERT INTO projection_thread_sessions
      (thread_id, status, provider_name, runtime_mode, updated_at)
      VALUES ('old-session', 'ready', 'codex', 'full-access', '2026-09-01T00:00:00.000Z')`;
    yield* runMigrations();
    assert.deepEqual(
      yield* sql<{ name: string }>`PRAGMA table_info(projection_thread_sessions)`,
      before,
    );
    assert.deepEqual(yield* sql`SELECT thread_id FROM projection_thread_sessions`, [
      { thread_id: "old-session" },
    ]);
  }).pipe(Effect.provide(memory)),
);

it.effect("preserves identity JSON in a column added by a released build", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 54 });
    yield* released002;
    const identity = encodePreexistingRuntimeIdentity({ runtimeGeneration: "preexisting-runtime" });
    yield* sql`INSERT INTO projection_thread_sessions
      (thread_id, status, provider_name, runtime_mode, updated_at, runtime_identity_json)
      VALUES ('existing-identity', 'ready', 'codex', 'full-access', '2026-09-01T00:00:00.000Z', ${identity})`;
    yield* runMigrations();
    yield* Jones002;
    assert.deepEqual(yield* sql`SELECT runtime_identity_json FROM projection_thread_sessions`, [
      { runtime_identity_json: identity },
    ]);
  }).pipe(Effect.provide(memory)),
);

it.effect(
  "receiving native execution schema preserves ordinary startup and foreign evidence without adopting it",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* seedLegacy();
      yield* sql`INSERT INTO jones_sql_migrations(migration_id,name) VALUES(7,'V2NativeAcceptance')`;
      const before = yield* readLedger;
      yield* runMigrations();
      assert.deepStrictEqual((yield* readLedger).slice(0, before.length), before);
      assert.deepStrictEqual(
        yield* sql`SELECT migration_id,name FROM jones_sql_migrations WHERE migration_id=100`,
        [{ migration_id: 100, name: "NativeCreationExecution" }],
      );
      assert.deepStrictEqual(
        yield* sql`SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'jones_native_creation_execution_%' ORDER BY name`,
        [
          { name: "jones_native_creation_execution_acceptances" },
          { name: "jones_native_creation_execution_confirmations" },
          { name: "jones_native_creation_execution_holds" },
          { name: "jones_native_creation_execution_starts" },
        ],
      );
      const ledger = yield* readLedger;
      assert.deepStrictEqual(yield* runMigrations(), []);
      assert.deepStrictEqual(yield* readLedger, ledger);
    }).pipe(Effect.provide(memory)),
);
