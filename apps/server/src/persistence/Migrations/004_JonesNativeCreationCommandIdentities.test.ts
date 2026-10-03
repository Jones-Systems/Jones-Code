import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { migrationManifest, runMigrations } from "../Migrations.ts";
import intentMigration from "./003_JonesNativeCreationIntents.ts";
import identityMigration from "./004_JonesNativeCreationCommandIdentities.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });

it.effect("records additive fork migration 4 once without changing upstream history", () =>
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

it.effect("historical upstream replay leaves identity reservations absent", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 54 });
    assert.deepEqual(
      yield* sql`SELECT name FROM sqlite_master WHERE name = 'native_creation_reserved_command_identities'`,
      [],
    );
    yield* intentMigration;
    yield* identityMigration;
    yield* identityMigration;
    assert.deepEqual(
      yield* sql`SELECT command_id FROM native_creation_reserved_command_identities`,
      [],
    );
  }).pipe(Effect.provide(memory)),
);

it.effect("command identity rows cannot update, delete, replace or lose their claim", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* intentMigration;
    yield* identityMigration;
    yield* sql`INSERT INTO native_creation_intents
      (claim_id, operation_id, preparation_id, command_id, thread_id, message_id, project_cwd, branch, worktree_path, canonical_preparation, intent_json)
      VALUES ('claim', 'operation', 'preparation', 'command', 'thread', 'message', '/fixture/project', 'fixture-branch', '/fixture/path', '{}', '{}')`;
    yield* sql`INSERT INTO native_creation_reserved_command_identities (command_id, claim_id, thread_id) VALUES ('command', 'claim', 'thread')`;
    for (const mutation of [
      sql`UPDATE native_creation_reserved_command_identities SET claim_id = 'other'`,
      sql`UPDATE native_creation_reserved_command_identities SET command_id = 'other'`,
      sql`UPDATE native_creation_reserved_command_identities SET thread_id = 'other'`,
      sql`DELETE FROM native_creation_reserved_command_identities`,
      sql`INSERT OR REPLACE INTO native_creation_reserved_command_identities (command_id, claim_id, thread_id) VALUES ('command', 'claim', 'other')`,
      sql`INSERT INTO native_creation_reserved_command_identities (command_id, claim_id, thread_id) VALUES ('orphan', 'missing', 'thread')`,
      sql`INSERT INTO native_creation_reserved_command_identities (command_id, claim_id, thread_id) VALUES (NULL, 'claim', 'thread')`,
    ])
      assert.strictEqual((yield* mutation.pipe(Effect.result))._tag, "Failure");
    assert.deepEqual(
      yield* sql`SELECT command_id, claim_id, thread_id FROM native_creation_reserved_command_identities`,
      [{ command_id: "command", claim_id: "claim", thread_id: "thread" }],
    );
  }).pipe(Effect.provide(memory)),
);
