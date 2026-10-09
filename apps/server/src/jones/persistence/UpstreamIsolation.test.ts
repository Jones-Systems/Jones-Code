import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";

import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import { migrationManifest, runMigrations } from "../../persistence/Migrations.ts";
import { runJonesMigrations } from "./JonesMigrationGuard.ts";
import { jonesMigrationEntries } from "./JonesMigrations.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const upstreamThrough = migrationManifest.at(-1)![0];

// Released Jones tables named before the jones_ prefix rule. Keep these names.
const releasedUnprefixedTables = new Set([
  "worktree_ownership_leases",
  "native_creation_automation_enrollments",
  "native_creation_intents",
  "native_creation_normalized_commands",
  "native_creation_reserved_commands",
  "native_creation_effect_facts",
  "native_creation_reserved_command_identities",
  "workstreams_native_attempts",
  "workstreams_native_enrollments",
]);
const isJonesTable = (name: string) =>
  name.startsWith("jones_") || releasedUnprefixedTables.has(name);

const rebuiltTables = [
  "workstreams_native_enrollments",
  "jones_native_creation_execution_acceptances",
  "jones_native_creation_execution_starts",
  "jones_imported_history_outcomes",
  "jones_imported_history_start_reservations",
  "jones_runtime_stop_intents",
] as const;
// The upstream ID each rebuilt table is read by, including per-command event sink reads.
const lookupKeys = {
  workstreams_native_enrollments: "session_id",
  jones_native_creation_execution_acceptances: "command_id",
  jones_native_creation_execution_starts: "effect_id",
  jones_imported_history_outcomes: "receipt_command_id",
  jones_imported_history_start_reservations: "effect_id",
  jones_runtime_stop_intents: "command_id",
} as const;
// Jones children of rebuilt tables, whose references must survive the rebuild.
const rebuiltChildren = [
  "jones_native_creation_execution_confirmations",
  "jones_native_creation_execution_holds",
  "jones_imported_history_execution_starts",
  "jones_runtime_stop_fences",
  "jones_runtime_stop_observations",
] as const;

interface SchemaObject {
  readonly type: string;
  readonly name: string;
  readonly tbl_name: string;
  readonly sql: string | null;
}

const readObjects = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<SchemaObject>`SELECT type, name, tbl_name, sql FROM sqlite_master
    WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name`;
});
const readShape = (table: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    return {
      columns: yield* sql`SELECT * FROM pragma_table_info(${table}) ORDER BY cid`,
      foreignKeys: yield* sql<{
        readonly table: string;
        readonly from: string;
        readonly to: string;
      }>`SELECT "table", "from", "to" FROM pragma_foreign_key_list(${table}) ORDER BY id, seq`,
      triggers:
        yield* sql`SELECT name, sql FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ${table} ORDER BY name`,
    };
  });
const readTable = (table: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    return {
      ...(yield* readShape(table)),
      rows: yield* sql.unsafe(`SELECT rowid AS jones_rowid, * FROM "${table}" ORDER BY rowid`),
    };
  });

// Fills required columns from the live schema, so later upstream columns
// do not break the fixture.
const insertSynthetic = (table: string, values: Record<string, string | number>) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const required = yield* sql<{
      readonly name: string;
      readonly type: string;
    }>`SELECT name, type FROM pragma_table_info(${table})
      WHERE "notnull" = 1 AND dflt_value IS NULL AND pk = 0`;
    const row: Record<string, string | number> = {};
    for (const { name, type } of required)
      row[name] = type.toUpperCase().includes("INT") ? 1 : `synthetic-${name}`;
    Object.assign(row, values);
    yield* sql`INSERT INTO ${sql(table)} ${sql.insert(row)}`;
  });

const sha = (char: string) => char.repeat(64);

const seedReferencedRows = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* insertSynthetic("auth_sessions", { session_id: "session" });
  yield* insertSynthetic("orchestration_command_receipts", { command_id: "command" });
  yield* insertSynthetic("orchestration_events", { event_id: "event", command_id: "command" });
  yield* insertSynthetic("orchestration_v2_effect_outbox", {
    effect_id: "effect",
    command_id: "command",
    thread_id: "thread",
    payload_json: "{}",
    status: "succeeded",
    // Older than the retention cutoff at the test clock's epoch start.
    completed_at: "1900-01-01T00:00:00.000Z",
  });
  yield* sql`INSERT INTO native_creation_intents VALUES (
    'claim', 'operation', 'preparation', 'command', 'thread', 'message',
    '/synthetic/project', 'branch', '/synthetic/worktree', '{}', '{}'
  )`;
  // A sparse rowid makes a renumbering rebuild visible.
  yield* sql`INSERT INTO workstreams_native_enrollments
    (rowid, enrollment_id, session_id, request_sha256, request_json, binding_json)
    VALUES (41, 'enrollment', 'session', ${sha("a")}, '{}', '{}')`;
  yield* sql`INSERT INTO jones_native_creation_execution_acceptances VALUES (
    'command', 'claim', 'thread', ${sha("b")}, ${sha("c")}, 'event', 1
  )`;
  yield* sql`INSERT INTO jones_native_creation_execution_starts VALUES (
    'effect', 'claim', 'command', 'worker', 1, 'lease', '{}', '{}'
  )`;
  yield* sql`INSERT INTO jones_native_creation_execution_confirmations VALUES ('effect', 'worker', 1, '{}', 'confirmed')`;
  yield* sql`INSERT INTO jones_imported_history_choices VALUES (
    'command', 'thread', 'session', ${sha("d")}, ${sha("e")}, ${sha("f")}, ${sha("0")}, '{}', '{}'
  )`;
  yield* sql`INSERT INTO jones_imported_history_outcomes VALUES ('command', 'command', '{}')`;
  yield* sql`INSERT INTO jones_imported_history_start_reservations VALUES (
    'effect', 'command', 'thread', 'run', 'attempt', 'provider-thread', ${sha("1")}
  )`;
  yield* sql`INSERT INTO jones_imported_history_execution_starts VALUES ('effect', '{}')`;
  yield* sql`INSERT INTO jones_runtime_stop_intents VALUES ('command', 'thread', '{}')`;
  yield* sql`INSERT INTO jones_runtime_stop_fences VALUES ('command', 'thread', 'run', 'provider-thread', 'generation')`;
  yield* sql`INSERT INTO jones_runtime_stop_observations VALUES ('command', 'started', 'unknown')`;
});

const pruneSettled = Effect.gen(function* () {
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  return yield* outbox.pruneSettled;
}).pipe(Effect.provide(EffectOutbox.layer));

it.effect("Jones migrations leave upstream schema and ledger unchanged", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: upstreamThrough });
    const upstreamObjects = yield* readObjects;
    const upstreamTables = upstreamObjects.filter(({ type }) => type === "table");
    const upstreamShapes = yield* Effect.forEach(upstreamTables, ({ name }) => readShape(name));
    const upstreamLedger = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
    assert.isFalse(upstreamObjects.some(({ tbl_name }) => isJonesTable(tbl_name)));

    yield* runMigrations();
    const objects = yield* readObjects;
    for (const object of upstreamObjects) assert.deepInclude(objects, object);
    assert.deepStrictEqual(
      yield* Effect.forEach(upstreamTables, ({ name }) => readShape(name)),
      upstreamShapes,
    );
    assert.deepStrictEqual(
      yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,
      upstreamLedger,
    );

    // Every added table, index and trigger belongs to a Jones table, so upstream
    // writes fire no Jones triggers and maintain no Jones indexes.
    const added = objects.filter(
      (object) =>
        !upstreamObjects.some(({ type, name }) => type === object.type && name === object.name),
    );
    assert.isNotEmpty(added);
    for (const { type, name, tbl_name } of added) {
      assert.include(["table", "index", "trigger"], type, name);
      assert.isTrue(isJonesTable(tbl_name), `${type} ${name} on ${tbl_name}`);
    }
    for (const { name } of added.filter(({ type }) => type === "table")) {
      for (const reference of (yield* readShape(name)).foreignKeys)
        assert.isTrue(isJonesTable(reference.table), `${name} references ${reference.table}`);
    }

    // Reopening with an upstream-only migrator, as T3 Code does, is a no-op.
    assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: upstreamThrough }), []);
    assert.deepStrictEqual(yield* readObjects, objects);
  }).pipe(Effect.provide(memory)),
);

it.effect("upstream pruning and deletes are not blocked by Jones evidence", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* seedReferencedRows;
    const jones = yield* Effect.forEach([...rebuiltTables, ...rebuiltChildren], readTable);

    assert.strictEqual(yield* pruneSettled, 1);
    yield* sql`DELETE FROM orchestration_events WHERE event_id = 'event'`;
    yield* sql`DELETE FROM orchestration_command_receipts WHERE command_id = 'command'`;
    yield* sql`DELETE FROM auth_sessions WHERE session_id = 'session'`;
    // Upstream rebuilds parent tables by dropping them, as 055 did for the outbox.
    yield* sql`DROP TABLE orchestration_command_receipts`;

    // Jones rows keep the upstream IDs as plain values; readers treat a missing
    // upstream row as absent.
    assert.deepStrictEqual(
      yield* Effect.forEach([...rebuiltTables, ...rebuiltChildren], readTable),
      jones,
    );
  }).pipe(Effect.provide(memory)),
);

it.effect("migration 105 preserves released rows, rowids, triggers and Jones references", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: upstreamThrough });
    yield* runJonesMigrations(jonesMigrationEntries.filter(([id]) => id < 105));
    yield* seedReferencedRows;
    const before = yield* Effect.forEach([...rebuiltTables, ...rebuiltChildren], readTable);
    // Released schema rejects upstream settled-effect pruning.
    assert.isTrue(Exit.isFailure(yield* Effect.exit(pruneSettled)));

    assert.deepStrictEqual(
      (yield* runJonesMigrations(jonesMigrationEntries)).map(([id]) => id),
      [105],
    );
    const after = yield* Effect.forEach([...rebuiltTables, ...rebuiltChildren], readTable);
    for (const [index, table] of [...rebuiltTables, ...rebuiltChildren].entries()) {
      const prior = before[index]!;
      const current = after[index]!;
      assert.deepStrictEqual(current.columns, prior.columns, table);
      assert.deepStrictEqual(current.rows, prior.rows, table);
      assert.deepStrictEqual(current.triggers, prior.triggers, table);
      assert.deepStrictEqual(
        current.foreignKeys,
        prior.foreignKeys.filter((reference) => isJonesTable(reference.table)),
        table,
      );
    }
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    // Lookups by upstream ID stay index searches, not table scans.
    for (const table of rebuiltTables) {
      const plan = yield* sql.unsafe<{ readonly detail: string }>(
        `EXPLAIN QUERY PLAN SELECT * FROM "${table}" WHERE ${lookupKeys[table]} = 'probe'`,
      );
      assert.match(plan.map(({ detail }) => detail).join("; "), /^SEARCH .* USING /, table);
    }
    for (const table of rebuiltTables) {
      assert.isTrue(Exit.isFailure(yield* Effect.exit(sql.unsafe(`DELETE FROM "${table}"`))));
    }
    assert.strictEqual(yield* pruneSettled, 1);
    assert.deepStrictEqual(yield* runJonesMigrations(jonesMigrationEntries), []);
  }).pipe(Effect.provide(memory)),
);
