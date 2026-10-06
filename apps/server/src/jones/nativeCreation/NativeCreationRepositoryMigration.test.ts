import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { migrationManifest, runMigrations } from "../../persistence/Migrations.ts";
import migrate from "../persistence/Migrations/003_JonesNativeCreationIntents.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
it.effect("records additive fork migration 3 without changing upstream migration identity", () =>
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
      ],
    );
    assert.deepEqual(
      yield* sql`SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id`,
      migrationManifest.map(([migration_id, name]) => ({ migration_id, name })),
    );
    const tables = yield* sql<{
      name: string;
    }>`SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'native_creation_%' ORDER BY name`;
    assert.deepEqual(
      tables.map((row) => row.name),
      [
        "native_creation_automation_enrollments",
        "native_creation_effect_facts",
        "native_creation_intents",
        "native_creation_normalized_commands",
        "native_creation_reserved_command_identities",
        "native_creation_reserved_commands",
      ],
    );
  }).pipe(Effect.provide(memory)),
);

it.effect(
  "native session enrollment markers survive attempted update, deletion and replacement",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* migrate;
      yield* sql`INSERT INTO native_creation_automation_enrollments (session_id, enrolled_at)
    VALUES ('fixture-native-session', '2026-10-02T12:00:00Z')`;
      for (const mutation of [
        sql`UPDATE native_creation_automation_enrollments SET enrolled_at = '2099-01-01T00:00:00Z' WHERE session_id = 'fixture-native-session'`,
        sql`UPDATE native_creation_automation_enrollments SET session_id = 'other-session' WHERE session_id = 'fixture-native-session'`,
        sql`DELETE FROM native_creation_automation_enrollments WHERE session_id = 'fixture-native-session'`,
        sql`INSERT OR REPLACE INTO native_creation_automation_enrollments (session_id, enrolled_at) VALUES ('fixture-native-session', '2099-01-01T00:00:00Z')`,
      ]) {
        assert.isTrue((yield* mutation.pipe(Effect.result))._tag === "Failure");
      }
      assert.deepEqual(
        yield* sql`SELECT session_id, enrolled_at FROM native_creation_automation_enrollments`,
        [{ session_id: "fixture-native-session", enrolled_at: "2026-10-02T12:00:00Z" }],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect("bounded upstream replay leaves creation tables absent and migration is idempotent", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 54 });
    assert.deepEqual(
      yield* sql`SELECT name FROM sqlite_master WHERE name LIKE 'native_creation_%'`,
      [],
    );
    yield* migrate;
    yield* migrate;
    yield* sql`INSERT INTO native_creation_intents
    (claim_id, operation_id, preparation_id, command_id, thread_id, message_id, project_cwd, branch, worktree_path, canonical_preparation, intent_json)
    VALUES ('claim', 'operation', 'preparation', 'command', 'thread', 'message', '/fixture/project', 'fixture-branch', '/fixture/path', '{}', '{}')`;
    for (const mutation of [
      sql`UPDATE native_creation_intents SET intent_json = 'changed' WHERE claim_id = 'claim'`,
      sql`DELETE FROM native_creation_intents WHERE claim_id = 'claim'`,
    ]) {
      assert.isTrue((yield* mutation.pipe(Effect.result))._tag === "Failure");
    }
    assert.deepEqual(yield* sql`SELECT claim_id, intent_json FROM native_creation_intents`, [
      { claim_id: "claim", intent_json: "{}" },
    ]);
  }).pipe(Effect.provide(memory)),
);

it.effect(
  "all resource identity constraints reject collisions and effect facts cannot replace history",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* migrate;
      const insert = (id: string, column?: string) =>
        sql`INSERT INTO native_creation_intents ${sql.insert({
          claim_id: id,
          operation_id: id,
          preparation_id: id,
          command_id: id,
          thread_id: id,
          message_id: id,
          project_cwd: "/fixture/project",
          branch: id,
          worktree_path: `/fixture/${id}`,
          canonical_preparation: "{}",
          intent_json: "{}",
          ...(column === undefined
            ? {}
            : { [column]: column === "worktree_path" ? "/fixture/original" : "original" }),
        })}`;
      yield* insert("original");
      for (const column of [
        "claim_id",
        "operation_id",
        "preparation_id",
        "command_id",
        "thread_id",
        "message_id",
        "branch",
        "worktree_path",
      ]) {
        assert.isTrue(
          (yield* insert(`other-${column}`, column).pipe(Effect.result))._tag === "Failure",
        );
      }
      yield* sql`INSERT INTO native_creation_effect_facts (claim_id, effect_id, phase, ordinal, fact_json) VALUES ('original', 'effect', 'started', 0, '{}')`;
      for (const mutation of [
        sql`UPDATE native_creation_effect_facts SET fact_json = 'changed'`,
        sql`DELETE FROM native_creation_effect_facts`,
        sql`INSERT INTO native_creation_effect_facts (claim_id, effect_id, phase, ordinal, fact_json) VALUES ('original', 'effect', 'started', 1, 'changed')`,
        sql`INSERT OR REPLACE INTO native_creation_effect_facts (claim_id, effect_id, phase, ordinal, fact_json) VALUES ('original', 'effect', 'started', 0, 'changed')`,
      ]) {
        assert.isTrue((yield* mutation.pipe(Effect.result))._tag === "Failure");
      }
      assert.deepEqual(yield* sql`SELECT ordinal, fact_json FROM native_creation_effect_facts`, [
        { ordinal: 0, fact_json: "{}" },
      ]);
    }).pipe(Effect.provide(memory)),
);
