import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import migration from "./159_JonesOrdinaryPreparation.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
class SyntheticRollback extends Schema.TaggedError<SyntheticRollback>()("SyntheticRollback", {}) {}

it.effect(
  "159 creates three sparse tables without upstream schema, data, foreign keys or ledger writes",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql.unsafe("PRAGMA foreign_keys = ON");
      yield* sql.unsafe(
        "CREATE TABLE effect_sql_migrations(migration_id INTEGER PRIMARY KEY,name TEXT)",
      );
      yield* sql.unsafe(
        "CREATE TABLE jones_sql_migrations(migration_id INTEGER PRIMARY KEY,name TEXT)",
      );
      yield* sql.unsafe(
        "CREATE TABLE orchestration_v2_projection_threads(thread_id TEXT PRIMARY KEY,payload_json TEXT)",
      );
      yield* sql.unsafe("INSERT INTO effect_sql_migrations VALUES (74,'Upstream')");
      yield* sql.unsafe("INSERT INTO jones_sql_migrations VALUES (158,'LegacyBootstrap')");
      yield* sql.unsafe("INSERT INTO orchestration_v2_projection_threads VALUES ('T','public')");
      const before = yield* sql.unsafe(
        "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name",
      );
      const upstreamLedger = yield* sql.unsafe("SELECT * FROM effect_sql_migrations");
      const jonesLedger = yield* sql.unsafe("SELECT * FROM jones_sql_migrations");
      yield* sql.withTransaction(migration);
      const after = yield* sql.unsafe<{ readonly name: string; readonly type: string }>(
        "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name",
      );
      assert.deepStrictEqual(
        after.filter(({ name }) => !name.startsWith("jones_ordinary_checkout_preparing_")),
        before,
      );
      const tables = after
        .filter(
          ({ type, name }) =>
            type === "table" && name.startsWith("jones_ordinary_checkout_preparing_"),
        )
        .map(({ name }) => name);
      assert.equal(tables.length, 3);
      for (const table of tables) {
        assert.deepStrictEqual(yield* sql.unsafe("PRAGMA foreign_key_list(" + table + ")"), []);
        assert.deepStrictEqual(yield* sql.unsafe("SELECT * FROM " + table), []);
      }
      assert.deepStrictEqual(
        yield* sql.unsafe("SELECT * FROM effect_sql_migrations"),
        upstreamLedger,
      );
      assert.deepStrictEqual(yield* sql.unsafe("SELECT * FROM jones_sql_migrations"), jonesLedger);
      assert.deepStrictEqual(
        yield* sql.unsafe("SELECT * FROM orchestration_v2_projection_threads"),
        [{ thread_id: "T", payload_json: "public" }],
      );
      yield* sql.unsafe(
        "INSERT INTO jones_ordinary_checkout_preparing_origins VALUES ('origin','C','T','birth','run','attempt','{}')",
      );
      yield* sql.unsafe("DELETE FROM orchestration_v2_projection_threads");
      yield* sql.unsafe("DROP TABLE orchestration_v2_projection_threads");
      assert.equal(
        (yield* sql.unsafe("SELECT * FROM jones_ordinary_checkout_preparing_origins")).length,
        1,
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "159 DDL rolls back with its transaction and enforces origin and binding uniqueness",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const aborted = yield* sql
        .withTransaction(migration.pipe(Effect.andThen(Effect.fail(new SyntheticRollback()))))
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(aborted));
      assert.deepStrictEqual(
        yield* sql.unsafe(
          "SELECT name FROM sqlite_master WHERE name LIKE 'jones_ordinary_checkout_preparing_%'",
        ),
        [],
      );
      yield* sql.withTransaction(migration);
      yield* sql.unsafe(
        "INSERT INTO jones_ordinary_checkout_preparing_origins VALUES ('origin','C','T','birth','run','attempt','{}')",
      );
      assert.isTrue(
        Exit.isFailure(
          yield* sql
            .unsafe(
              "INSERT INTO jones_ordinary_checkout_preparing_origins VALUES ('other','C2','T','birth','run','attempt','{}')",
            )
            .pipe(Effect.exit),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* sql
            .unsafe(
              "INSERT INTO jones_ordinary_checkout_preparing_bindings VALUES ('origin',0,'bind','{}')",
            )
            .pipe(Effect.exit),
        ),
      );
      yield* sql.unsafe(
        "INSERT INTO jones_ordinary_checkout_preparing_bindings VALUES ('origin',1,'bind','{}')",
      );
      assert.isTrue(
        Exit.isFailure(
          yield* sql
            .unsafe(
              "INSERT INTO jones_ordinary_checkout_preparing_bindings VALUES ('origin',2,'bind','{}')",
            )
            .pipe(Effect.exit),
        ),
      );
      assert.equal(
        (yield* sql.unsafe("SELECT * FROM jones_ordinary_checkout_preparing_origins")).length,
        1,
      );
      assert.equal(
        (yield* sql.unsafe("SELECT * FROM jones_ordinary_checkout_preparing_bindings")).length,
        1,
      );
    }).pipe(Effect.provide(memory)),
);
