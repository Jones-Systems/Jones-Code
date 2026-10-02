import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

import { migrationManifest, runMigrations } from "../Migrations.ts";
import migrateRuntimeIdentity from "./002_JonesProjectionThreadRuntimeIdentity.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const encodePreexistingIdentity = Schema.encodeSync(
  Schema.fromJsonString(Schema.Struct({ runtimeGeneration: Schema.String })),
);

it.effect("records runtime identity as fork migration 2 after fresh upstream migrations", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    assert.deepEqual(yield* runMigrations(), migrationManifest);
    assert.deepEqual(yield* runMigrations(), []);
    assert.deepEqual(
      yield* sql`SELECT migration_id, name FROM jones_sql_migrations ORDER BY migration_id`,
      [
        { migration_id: 1, name: "WorktreeOwnershipLeases" },
        { migration_id: 2, name: "ProjectionThreadRuntimeIdentity" },
        { migration_id: 3, name: "NativeCreationIntents" },
        { migration_id: 4, name: "NativeCreationCommandIdentities" },
        { migration_id: 5, name: "WorkstreamsNativeAttempts" },
        { migration_id: 6, name: "WorkstreamsProviderEnrollments" },
        { migration_id: 7, name: "ThreadCreationLookupIndex" },
      ],
    );
    assert.deepEqual(
      yield* sql`SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id`,
      migrationManifest.map(([migration_id, name]) => ({ migration_id, name })),
    );
  }).pipe(Effect.provide(memory)),
);

it.effect("leaves bounded upstream replay untouched and migrates existing sessions as null", () =>
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
    assert.deepEqual(yield* sql`SELECT runtime_identity_json FROM projection_thread_sessions`, [
      { runtime_identity_json: null },
    ]);
  }).pipe(Effect.provide(memory)),
);

it.effect("preserves preexisting identity JSON when the column predates the fork ledger", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 54 });
    yield* migrateRuntimeIdentity;
    const identity = encodePreexistingIdentity({ runtimeGeneration: "preexisting-runtime" });
    yield* sql`INSERT INTO projection_thread_sessions
      (thread_id, status, provider_name, runtime_mode, updated_at, runtime_identity_json)
      VALUES ('existing-identity', 'ready', 'codex', 'full-access', '2026-09-01T00:00:00.000Z', ${identity})`;
    yield* runMigrations();
    yield* migrateRuntimeIdentity;
    assert.deepEqual(yield* sql`SELECT runtime_identity_json FROM projection_thread_sessions`, [
      { runtime_identity_json: identity },
    ]);
  }).pipe(Effect.provide(memory)),
);
