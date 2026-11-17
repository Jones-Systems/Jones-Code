import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import migration from "./158_JonesLegacyBootstrap.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
it.effect(
  "158 creates five sparse Jones tables without upstream FKs, schema, rows or ledger changes",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`CREATE TABLE effect_sql_migrations(migration_id INTEGER PRIMARY KEY,name TEXT)`;
      yield* sql`CREATE TABLE jones_sql_migrations(migration_id INTEGER PRIMARY KEY,name TEXT)`;
      yield* sql`CREATE TABLE orchestration_v2_projection_threads(thread_id TEXT PRIMARY KEY,payload_json TEXT)`;
      yield* sql`CREATE TABLE orchestration_v2_projection_runs(run_id TEXT PRIMARY KEY,payload_json TEXT)`;
      yield* sql`INSERT INTO effect_sql_migrations VALUES (74,'Upstream')`;
      yield* sql`INSERT INTO jones_sql_migrations VALUES (157,'RemoteSendReceipts')`;
      yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES ('T','{"public":true}')`;
      const before =
        yield* sql`SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name`;
      const upstreamLedger = yield* sql`SELECT * FROM effect_sql_migrations`;
      const jonesLedger = yield* sql`SELECT * FROM jones_sql_migrations`;
      yield* sql.withTransaction(migration);
      const after = yield* sql<{
        name: string;
        type: string;
      }>`SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name`;
      assert.deepStrictEqual(
        after.filter(({ name }) => !name.startsWith("jones_legacy_bootstrap_")),
        before,
      );
      const tables = after
        .filter(({ type, name }) => type === "table" && name.startsWith("jones_legacy_bootstrap_"))
        .map(({ name }) => name);
      assert.equal(tables.length, 5);
      for (const table of tables) {
        assert.deepStrictEqual(yield* sql.unsafe(`PRAGMA foreign_key_list(${table})`), []);
        assert.deepStrictEqual(yield* sql.unsafe(`SELECT * FROM ${table}`), []);
      }
      assert.deepStrictEqual(yield* sql`SELECT * FROM effect_sql_migrations`, upstreamLedger);
      assert.deepStrictEqual(yield* sql`SELECT * FROM jones_sql_migrations`, jonesLedger);
      assert.deepStrictEqual(yield* sql`SELECT * FROM orchestration_v2_projection_threads`, [
        { thread_id: "T", payload_json: '{"public":true}' },
      ]);
      yield* sql`INSERT INTO jones_legacy_bootstrap_threads VALUES ('T','{}',0,'{}')`;
      yield* sql`DELETE FROM orchestration_v2_projection_threads`;
      yield* sql`DROP TABLE orchestration_v2_projection_threads`;
      yield* sql`DROP TABLE orchestration_v2_projection_runs`;
      assert.equal((yield* sql`SELECT * FROM jones_legacy_bootstrap_threads`).length, 1);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "158 rejects mixed sequence domains and impossible cleanup states at the SQL boundary",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* migration;
      for (const [kind, domain, ordinal] of [
        ["lifecycle", "jones-legacy-ordinal", 1],
        ["preflight-intent", "lifecycle", null],
        ["delete", "lifecycle", 4],
      ] as const) {
        assert.isTrue(
          Exit.isFailure(
            yield* sql`INSERT INTO jones_legacy_bootstrap_commands VALUES ('bad','T','P',${kind},${"a".repeat(64)},${domain},${ordinal},'{}')`.pipe(
              Effect.exit,
            ),
          ),
        );
      }
      for (const [state, attempt, outcome] of [
        ["pending", "owner", null],
        ["started", null, null],
        ["succeeded", "owner", "unconfirmed"],
        ["unknown", "owner", "closed"],
      ] as const) {
        assert.isTrue(
          Exit.isFailure(
            yield* sql`INSERT INTO jones_legacy_bootstrap_terminal_cleanup VALUES ('bad','D','T','{}',${state},${attempt},0,${outcome})`.pipe(
              Effect.exit,
            ),
          ),
        );
      }
      assert.isTrue(
        Exit.isFailure(
          yield* sql`INSERT INTO jones_legacy_bootstrap_threads VALUES ('T','not JSON',0,'{}')`.pipe(
            Effect.exit,
          ),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* sql`INSERT INTO jones_legacy_bootstrap_runs VALUES ('R','T','birth',2,'{}',-1,'{}')`.pipe(
            Effect.exit,
          ),
        ),
      );
    }).pipe(Effect.provide(memory)),
);
