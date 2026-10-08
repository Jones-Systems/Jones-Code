import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import migration from "./104_JonesDeletionAdmission.ts";
const memory = NodeSqliteClient.layer({ filename: ":memory:" });
it.effect(
  "keeps original path/start custody and observations immutable without modifying foreign ledgers",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE jones_sql_migrations(migration_id INTEGER PRIMARY KEY,name TEXT NOT NULL)`;
      yield* sql`INSERT INTO jones_sql_migrations VALUES(7,'DeletionWorktreeAdmission')`;
      yield* sql`CREATE TABLE jones_native_workspace_admissions(claim_id TEXT PRIMARY KEY,worktree_path TEXT UNIQUE,basis_json TEXT)`;
      yield* sql`INSERT INTO jones_native_workspace_admissions VALUES('original-claim','/synthetic/original','{}')`;
      const ledger = yield* sql`SELECT * FROM jones_sql_migrations`;
      const native = yield* sql`SELECT * FROM jones_native_workspace_admissions`;
      yield* migration;
      assert.deepStrictEqual(yield* sql`SELECT * FROM jones_sql_migrations`, ledger);
      assert.deepStrictEqual(yield* sql`SELECT * FROM jones_native_workspace_admissions`, native);
      yield* sql`INSERT INTO jones_deletion_worktree_admissions VALUES('effect:original','/synthetic/owned','{}','{}','started')`;
      assert.isTrue(
        Exit.isFailure(
          yield* sql`UPDATE jones_deletion_worktree_admissions SET start_json='{"substituted":true}'`.pipe(
            Effect.exit,
          ),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* sql`DELETE FROM jones_deletion_worktree_admissions`.pipe(Effect.exit),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* sql`INSERT OR REPLACE INTO jones_deletion_worktree_admissions VALUES('effect:new','/synthetic/owned','{}','{}','started')`.pipe(
            Effect.exit,
          ),
        ),
      );
      yield* sql`INSERT INTO jones_deletion_worktree_observations VALUES('effect:original',1,'{}','unknown')`;
      assert.isTrue(
        Exit.isFailure(
          yield* sql`UPDATE jones_deletion_worktree_observations SET result='confirmed'`.pipe(
            Effect.exit,
          ),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* sql`DELETE FROM jones_deletion_worktree_observations`.pipe(Effect.exit),
        ),
      );
    }).pipe(Effect.provide(memory)),
);
