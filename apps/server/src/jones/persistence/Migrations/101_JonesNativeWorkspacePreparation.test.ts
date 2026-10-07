import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import migrateClaims from "./003_JonesNativeCreationIntents.ts";
import migrateWorkspace from "./101_JonesNativeWorkspacePreparation.ts";
const memory = NodeSqliteClient.layer({ filename: ":memory:" });
it.effect(
  "workspace migration creates only new Jones storage and leaves foreign history and data untouched",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* migrateClaims;
      yield* sql`CREATE TABLE foreign_native_acceptance (value TEXT NOT NULL)`;
      yield* sql`INSERT INTO foreign_native_acceptance VALUES ('untouched')`;
      yield* sql`CREATE TABLE jones_sql_migrations (migration_id INTEGER PRIMARY KEY,name TEXT NOT NULL)`;
      yield* sql`INSERT INTO jones_sql_migrations VALUES (7,'V2NativeAcceptance')`;
      const before = yield* sql`SELECT * FROM jones_sql_migrations`;
      const foreignBefore = yield* sql`SELECT * FROM foreign_native_acceptance`;
      yield* migrateWorkspace;
      assert.deepEqual(yield* sql`SELECT * FROM jones_sql_migrations`, before);
      assert.deepEqual(yield* sql`SELECT * FROM foreign_native_acceptance`, foreignBefore);
      assert.deepEqual(
        (yield* sql<{
          name: string;
        }>`SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'jones_native_workspace_%' ORDER BY name`).map(
          (row) => row.name,
        ),
        ["jones_native_workspace_admissions", "jones_native_workspace_verified"],
      );
    }).pipe(Effect.provide(memory)),
);
it.effect("admitted path and verification cannot be updated, deleted or replaced", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* migrateClaims;
    yield* migrateWorkspace;
    yield* sql`INSERT INTO native_creation_intents (claim_id, operation_id, preparation_id, command_id, thread_id, message_id, project_cwd, branch, worktree_path, canonical_preparation,intent_json) VALUES ('claim','operation','prep','command','thread','message','/project','branch','/worktree','{}','{}')`;
    yield* sql`INSERT INTO jones_native_workspace_admissions VALUES ('claim','/worktree','{}')`;
    yield* sql`INSERT INTO jones_native_workspace_verified VALUES ('claim','{}')`;
    for (const operation of [
      sql`UPDATE jones_native_workspace_admissions SET basis_json='{"changed":true}'`,
      sql`DELETE FROM jones_native_workspace_admissions`,
      sql`INSERT OR REPLACE INTO jones_native_workspace_admissions VALUES ('claim','/replacement','{}')`,
      sql`UPDATE jones_native_workspace_verified SET receipt_json='{"changed":true}'`,
      sql`DELETE FROM jones_native_workspace_verified`,
      sql`INSERT OR REPLACE INTO jones_native_workspace_verified VALUES ('claim','{}')`,
    ])
      assert.strictEqual((yield* operation.pipe(Effect.result))._tag, "Failure");
    assert.deepEqual(yield* sql`SELECT * FROM jones_native_workspace_admissions`, [
      { claim_id: "claim", worktree_path: "/worktree", basis_json: "{}" },
    ]);
  }).pipe(Effect.provide(memory)),
);
