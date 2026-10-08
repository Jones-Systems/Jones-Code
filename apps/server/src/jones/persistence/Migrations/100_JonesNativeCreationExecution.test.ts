import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import migration from "./100_JonesNativeCreationExecution.ts";
const memory = NodeSqliteClient.layer({ filename: ":memory:" });
it.effect("creates only owned execution schema and preserves foreign history and facts", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE jones_sql_migrations(migration_id INTEGER PRIMARY KEY,name TEXT NOT NULL)`;
    yield* sql`INSERT INTO jones_sql_migrations VALUES(7,'V2NativeAcceptance')`;
    yield* sql`CREATE TABLE orchestration_v2_provider_runtime_evidence(thread_id TEXT PRIMARY KEY, generation TEXT NOT NULL)`;
    yield* sql`INSERT INTO orchestration_v2_provider_runtime_evidence VALUES('foreign-thread','foreign-generation')`;
    const ledger = yield* sql`SELECT * FROM jones_sql_migrations`;
    const facts = yield* sql`SELECT * FROM orchestration_v2_provider_runtime_evidence`;
    const foreignSchema =
      yield* sql`SELECT sql FROM sqlite_master WHERE name='orchestration_v2_provider_runtime_evidence'`;
    yield* migration;
    assert.deepEqual(yield* sql`SELECT * FROM jones_sql_migrations`, ledger);
    assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_provider_runtime_evidence`, facts);
    assert.deepEqual(
      yield* sql`SELECT sql FROM sqlite_master WHERE name='orchestration_v2_provider_runtime_evidence'`,
      foreignSchema,
    );
    assert.deepEqual(
      yield* sql`SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'jones_native_creation_execution_%' ORDER BY name`,
      [
        { name: "jones_native_creation_execution_acceptances" },
        { name: "jones_native_creation_execution_confirmations" },
        { name: "jones_native_creation_execution_holds" },
        { name: "jones_native_creation_execution_starts" },
      ],
    );
  }).pipe(Effect.provide(memory)),
);
