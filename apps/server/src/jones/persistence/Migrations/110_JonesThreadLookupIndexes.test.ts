import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

import { migrationManifest, runMigrations } from "../../../persistence/Migrations.ts";
import { runJonesMigrations } from "../JonesMigrationGuard.ts";
import { jonesMigrationEntries } from "../JonesMigrations.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const digest = "0".repeat(64);

// The provider-start lookups whose plans this migration changes. Keep them in
// step with claimImportedHistoryStart and RuntimeStopSqlite.assertStartAllowed.
const plans = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const plan = (rows: ReadonlyArray<{ readonly detail: string }>) =>
    rows.map((row) => row.detail).join("\n");
  return {
    imported: plan(
      yield* sql<{ readonly detail: string }>`EXPLAIN QUERY PLAN
      SELECT outcome.command_id FROM jones_imported_history_outcomes outcome
      JOIN jones_imported_history_choices choice ON choice.command_id = outcome.command_id
      WHERE choice.thread_id = 'thread:a' AND json_extract(outcome.outcome_json, '$.runId') = 'run:a' LIMIT 1`,
    ),
    lineage: plan(
      yield* sql<{ readonly detail: string }>`EXPLAIN QUERY PLAN
      SELECT command_id FROM jones_runtime_stop_intents WHERE thread_id='thread:a'
        AND json_extract(identity_json,'$.request.target.binding.providerThreadId')='provider:a'
        AND (NULL IS NULL OR json_extract(identity_json,'$.request.target.binding.runtimeGeneration')=NULL) LIMIT 1`,
    ),
  };
});

it.effect("110 indexes thread lookups without changing rows or the upstream ledger", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] });
    yield* runJonesMigrations(jonesMigrationEntries.filter(([id]) => id < 110));
    for (const thread of ["thread:a", "thread:b"]) {
      yield* sql`INSERT INTO jones_imported_history_choices(command_id,thread_id,actor_session_id,actor_digest,command_digest,delivery_digest,reviewed_basis,command_json,basis_json)
        VALUES(${`command:${thread}`},${thread},'actor',${digest},${digest},${digest},${digest},'{}','{}')`;
      yield* sql`INSERT INTO jones_runtime_stop_intents(command_id,thread_id,identity_json)
        VALUES(${`stop:${thread}`},${thread},'{}')`;
    }
    const before = yield* plans;
    assert.match(before.imported, /SCAN choice/);
    assert.match(before.lineage, /SCAN jones_runtime_stop_intents/);
    const read = Effect.all([
      sql`SELECT * FROM jones_imported_history_choices ORDER BY command_id`,
      sql`SELECT * FROM jones_runtime_stop_intents ORDER BY command_id`,
      sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,
      sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`,
    ]);
    const rows = yield* read;
    const schema = Effect.map(
      sql<{
        readonly type: string;
        readonly name: string;
        readonly tbl_name: string;
        readonly sql: string | null;
      }>`SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name`,
      (objects) => objects.map((object) => JSON.stringify(object)),
    );
    const schemaBefore = yield* schema;

    assert.deepEqual(yield* runJonesMigrations(jonesMigrationEntries), [
      [110, "ThreadLookupIndexes"],
    ]);
    const after = yield* plans;
    assert.match(
      after.imported,
      /SEARCH choice USING INDEX jones_imported_history_choices_by_thread/,
    );
    assert.match(
      after.lineage,
      /SEARCH jones_runtime_stop_intents USING INDEX jones_runtime_stop_intents_by_thread/,
    );
    const [choices, intents, upstreamLedger, jonesLedger] = yield* read;
    const schemaAfter = yield* schema;
    assert.isTrue(schemaBefore.every((object) => schemaAfter.includes(object)));
    assert.deepEqual([choices, intents, upstreamLedger], rows.slice(0, 3));
    assert.deepEqual(jonesLedger.slice(0, -1), rows[3]);
    // Existing objects keep their exact SQL; the migration adds only these indexes.
    const added = schemaAfter
      .filter((object) => !schemaBefore.includes(object))
      .map((object) => JSON.parse(object) as { type: string; name: string; tbl_name: string })
      .map((object) => `${object.type}:${object.name}:${object.tbl_name}`);
    assert.deepEqual(added, [
      "index:jones_imported_history_choices_by_thread:jones_imported_history_choices",
      "index:jones_runtime_stop_intents_by_thread:jones_runtime_stop_intents",
    ]);
    assert.deepEqual(yield* runJonesMigrations(jonesMigrationEntries), []);
  }).pipe(Effect.provide(memory)),
);
