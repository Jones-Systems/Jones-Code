import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import { migrationManifest, runMigrations } from "../../../persistence/Migrations.ts";
import { runJonesMigrations } from "../JonesMigrationGuard.ts";
import { jonesMigrationEntries } from "../JonesMigrations.ts";
import renewalMigration from "./137_JonesNativeCreationLeaseRenewal.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const starts = "jones_native_creation_execution_starts";
const changedTrigger = `${starts}_no_update`;
const releasedTriggerSql = `CREATE TRIGGER ${changedTrigger} BEFORE UPDATE ON ${starts} BEGIN SELECT RAISE(ABORT, 'native execution facts are immutable'); END`;
const activeExpiry = "2026-11-05T12:00:30.000Z";
const nextExpiry = "2026-11-05T12:00:40.000Z";
const nativeTables = [
  "jones_native_creation_execution_acceptances",
  starts,
  "jones_native_creation_execution_confirmations",
  "jones_native_creation_execution_holds",
] as const;
const upstreamTables = [
  "orchestration_command_receipts",
  "orchestration_events",
  "orchestration_v2_effect_outbox",
] as const;
const through137 = jonesMigrationEntries.filter(([id]) => id <= 137);
const through136 = jonesMigrationEntries.filter(([id]) => id <= 136);

const installReleased136 = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`PRAGMA foreign_keys = ON`;
  yield* runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] });
  yield* runJonesMigrations(through136);
  assert.deepStrictEqual(yield* sql`PRAGMA foreign_keys`, [{ foreign_keys: 1 }]);
  assert.deepStrictEqual(
    yield* sql`SELECT tbl_name, sql FROM sqlite_master WHERE name = ${changedTrigger}`,
    [{ tbl_name: starts, sql: releasedTriggerSql }],
  );
});

const seedStart = (id: string, expiry: string, rowid: number) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO native_creation_intents VALUES (
      ${id}, ${`operation-${id}`}, ${`preparation-${id}`}, ${id}, ${id}, ${id},
      '/synthetic/project', ${`branch-${id}`}, ${`/synthetic/${id}`}, '{}', '{}'
    )`;
    yield* sql`INSERT INTO orchestration_events
      (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
       command_id, actor_kind, payload_json, metadata_json, application_event_version)
      VALUES (${id}, 'thread', ${id}, 1, 'run.updated', ${activeExpiry},
        ${id}, 'system', '{}', '{}', 2)`;
    yield* sql`INSERT INTO orchestration_command_receipts
      (command_id, aggregate_kind, aggregate_id, command_type, accepted_at, result_sequence, status)
      VALUES (${id}, 'thread', ${id}, 'prepared-run.release', ${activeExpiry}, ${rowid}, 'accepted')`;
    yield* sql`INSERT INTO orchestration_v2_effect_outbox
      (effect_id, command_id, thread_id, effect_type, payload_json, status, attempt_count,
       available_at, lease_owner, created_at, updated_at)
      VALUES (${id}, ${id}, ${id}, 'provider-turn.start', '{}', 'running', 1,
        ${activeExpiry}, 'worker', ${activeExpiry}, ${activeExpiry})`;
    yield* sql`INSERT INTO jones_native_creation_execution_acceptances
      (rowid, command_id, claim_id, thread_id, command_digest, binding_digest, event_id, event_sequence)
      VALUES (${rowid}, ${id}, ${id}, ${id}, ${"a".repeat(64)}, ${"b".repeat(64)}, ${id}, ${rowid})`;
    yield* sql`INSERT INTO jones_native_creation_execution_starts
      (rowid, effect_id, claim_id, command_id, worker_id, expected_attempt, lease_expires_at, reference_json, fact_json)
      VALUES (${rowid}, ${id}, ${id}, ${id}, 'worker', 1, ${expiry}, '{"tag":1}', '{"tag":1}')`;
  });

const retainedFacts = Effect.gen(function* () {
  yield* installReleased136;
  yield* seedStart("active", activeExpiry, 41);
  yield* seedStart("expired", "2000-01-01T00:00:00.000Z", 71);
  yield* seedStart("confirmed", activeExpiry, 101);
  yield* seedStart("held", activeExpiry, 131);
  yield* seedStart("historical", "lease", 161);
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO jones_native_creation_execution_confirmations
    (rowid, effect_id, worker_id, expected_attempt, evidence_json, confirmed_at)
    VALUES (211, 'confirmed', 'worker', 1, '{}', ${activeExpiry})`;
  yield* sql`INSERT INTO jones_native_creation_execution_holds
    (rowid, effect_id, reason, held_at) VALUES (241, 'held', 'unknown', ${activeExpiry})`;
});

const readRows = (table: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const columns = yield* sql<{ readonly name: string }>`
    SELECT name FROM pragma_table_info(${table}) ORDER BY cid`;
    return {
      rows: yield* sql.unsafe(`SELECT rowid AS retained_rowid, * FROM "${table}" ORDER BY rowid`),
      storage: yield* sql.unsafe(
        `SELECT rowid AS retained_rowid, ${columns
          .map(({ name }) => `typeof("${name}") AS "${name}"`)
          .join(", ")} FROM "${table}" ORDER BY rowid`,
      ),
    };
  });

const snapshot = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return {
    schema: yield* sql<{ readonly name: string; readonly sql: string | null }>`
      SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name`,
    native: yield* Effect.forEach(nativeTables, (table) =>
      Effect.gen(function* () {
        return {
          table,
          columns: yield* sql`SELECT * FROM pragma_table_info(${table}) ORDER BY cid`,
          indexes: yield* sql`SELECT * FROM pragma_index_list(${table}) ORDER BY seq`,
          foreignKeys: yield* sql`SELECT * FROM pragma_foreign_key_list(${table}) ORDER BY id, seq`,
          ...(yield* readRows(table)),
        };
      }),
    ),
    upstream: yield* Effect.forEach(upstreamTables, readRows),
    upstreamLedger: yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,
    jonesLedger: yield* sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`,
  };
});

it.effect("137 changes only the released update trigger and its normal ledger entry", () =>
  Effect.gen(function* () {
    yield* retainedFacts;
    const sql = yield* SqlClient.SqlClient;
    const before = yield* snapshot;
    assert.strictEqual(
      before.schema.filter(({ name }) =>
        nativeTables.some((table) =>
          ["update", "delete", "replace"].some((kind) => name === `${table}_no_${kind}`),
        ),
      ).length,
      12,
    );
    assert.deepStrictEqual(yield* runJonesMigrations(through137), [
      [137, "NativeCreationLeaseRenewal"],
    ]);
    const after = yield* snapshot;
    assert.deepStrictEqual(after.native, before.native);
    assert.deepStrictEqual(after.upstream, before.upstream);
    assert.deepStrictEqual(after.upstreamLedger, before.upstreamLedger);
    assert.deepStrictEqual(after.jonesLedger.slice(0, -1), before.jonesLedger);
    assert.strictEqual(after.jonesLedger.length, before.jonesLedger.length + 1);
    assert.deepInclude(after.jonesLedger.at(-1), {
      migration_id: 137,
      name: "NativeCreationLeaseRenewal",
    });
    assert.deepStrictEqual(
      after.schema.filter(({ name }) => name !== changedTrigger),
      before.schema.filter(({ name }) => name !== changedTrigger),
    );
    assert.strictEqual(after.schema.filter(({ name }) => name === changedTrigger).length, 1);
    assert.notStrictEqual(
      after.schema.find(({ name }) => name === changedTrigger)!.sql,
      releasedTriggerSql,
    );
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    assert.deepStrictEqual(yield* runJonesMigrations(through137), []);
    assert.deepStrictEqual(yield* snapshot, after);
  }).pipe(Effect.provide(memory)),
);

it.effect(
  "only expiry advances, including one millisecond and calendar boundaries, preserve facts",
  () =>
    Effect.gen(function* () {
      yield* installReleased136;
      yield* seedStart("active", "2026-12-31T23:59:59.998Z", 41);
      yield* runJonesMigrations(through137);
      const sql = yield* SqlClient.SqlClient;
      let before = yield* readRows(starts);
      for (const expiry of [
        "2026-12-31T23:59:59.999Z",
        "2027-01-01T00:00:00.000Z",
        "2027-01-31T23:59:59.999Z",
        "2027-02-01T00:00:00.000Z",
        "2028-02-28T23:59:59.999Z",
        "2028-02-29T00:00:00.000Z",
        "2028-02-29T23:59:59.999Z",
        "2028-03-01T00:00:00.000Z",
      ]) {
        const rows = yield* sql`UPDATE jones_native_creation_execution_starts
        SET lease_expires_at = ${expiry} WHERE effect_id = 'active' RETURNING effect_id`;
        assert.deepStrictEqual(rows, [{ effect_id: "active" }]);
        const after = yield* readRows(starts);
        assert.deepStrictEqual(
          after.rows,
          before.rows.map((row) => ({ ...row, lease_expires_at: expiry })),
        );
        assert.deepStrictEqual(after.storage, before.storage);
        before = after;
      }
    }).pipe(Effect.provide(memory)),
);

it.effect("every rejected timestamp leaves the exact start row and storage types unchanged", () =>
  Effect.gen(function* () {
    yield* retainedFacts;
    yield* runJonesMigrations(through137);
    const sql = yield* SqlClient.SqlClient;
    const before = yield* readRows(starts);
    for (const [label, value] of [
      ["equal", `'${activeExpiry}'`],
      ["backward", "'2026-11-05T12:00:29.999Z'"],
      ["NULL", "NULL"],
      ["integer", "123456789"],
      ["real", "12345.678"],
      ["numeric text", "'20300101000000.000'"],
      ["BLOB", "CAST('2030-01-01T00:00:00.000Z' AS BLOB)"],
      ["empty", "''"],
      ["malformed", "'not-a-timestamp'"],
      ["leading whitespace", "' 2030-01-01T00:00:00.000Z'"],
      ["trailing whitespace", "'2030-01-01T00:00:00.000Z '"],
      ["trailing text", "'2030-01-01T00:00:00.000Zx'"],
      ["missing milliseconds", "'2030-01-01T00:00:00Z'"],
      ["missing zone", "'2030-01-01T00:00:00.000'"],
      ["offset", "'2030-01-01T00:00:00.000+00:00'"],
      ["offset without milliseconds", "'2030-01-01T00:00:00+00:00'"],
      ["lowercase zone", "'2030-01-01T00:00:00.000z'"],
      ["short precision", "'2030-01-01T00:00:00.00Z'"],
      ["excess precision", "'2030-01-01T00:00:00.0000Z'"],
      ["separator", "'2030-01-01 00:00:00.000Z'"],
      ["impossible month", "'2028-13-01T00:00:00.000Z'"],
      ["impossible day", "'2028-04-31T00:00:00.000Z'"],
      ["nonleap February", "'2027-02-29T00:00:00.000Z'"],
      ["nonleap century", "'2100-02-29T00:00:00.000Z'"],
      ["leap February overflow", "'2028-02-30T00:00:00.000Z'"],
      ["zero day", "'2028-03-00T00:00:00.000Z'"],
      ["hour 24", "'2028-03-01T24:00:00.000Z'"],
      ["minute 60", "'2028-03-01T00:60:00.000Z'"],
      ["leap second", "'2028-03-01T00:00:60.000Z'"],
    ] as const) {
      const result = yield* Effect.exit(
        sql.unsafe(`UPDATE ${starts} SET lease_expires_at = ${value} WHERE effect_id = 'active'`),
      );
      assert.isTrue(Exit.isFailure(result), label);
      if (Exit.isFailure(result))
        assert.include(Cause.pretty(result.cause), "native execution facts are immutable", label);
      assert.deepStrictEqual(yield* readRows(starts), before, label);
    }
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(sql`UPDATE jones_native_creation_execution_starts
      SET lease_expires_at = lease_expires_at WHERE effect_id = 'active'`),
      ),
    );
    assert.deepStrictEqual(yield* readRows(starts), before);
  }).pipe(Effect.provide(memory)),
);

it.effect(
  "historical noncanonical expiries survive upgrade and cannot be renewed into valid values",
  () =>
    Effect.gen(function* () {
      yield* installReleased136;
      for (const [index, expiry] of [
        "lease",
        "2028-02-30T00:00:00.000Z",
        "2028-03-01T00:00:00Z",
        "2028-03-01T00:00:00.000+00:00",
        "2028-03-01T00:00:00.0000Z",
        "123456789",
      ].entries()) {
        yield* seedStart(`old-${index}`, expiry, 41 + index * 10);
      }
      const sql = yield* SqlClient.SqlClient;
      // A historical BLOB is legal under the released NOT NULL column; no constraint is disabled.
      yield* sql`INSERT INTO jones_native_creation_execution_starts
      SELECT 'old-blob', claim_id, command_id, worker_id, expected_attempt,
        CAST('2028-03-01T00:00:00.000Z' AS BLOB), reference_json, fact_json
      FROM jones_native_creation_execution_starts WHERE effect_id = 'old-0'`;
      const before = yield* readRows(starts);
      yield* runJonesMigrations(through137);
      assert.deepStrictEqual(yield* readRows(starts), before);
      const ids = yield* sql<{
        readonly effect_id: string;
      }>`SELECT effect_id FROM jones_native_creation_execution_starts`;
      for (const { effect_id: id } of ids) {
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(sql`UPDATE jones_native_creation_execution_starts
        SET lease_expires_at = '2099-01-01T00:00:00.000Z' WHERE effect_id = ${id}`),
          ),
          id,
        );
        assert.deepStrictEqual(yield* readRows(starts), before, id);
      }
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "all seven other fields and rowid reject mutation alone or with a valid expiry advance",
  () =>
    Effect.gen(function* () {
      yield* retainedFacts;
      yield* runJonesMigrations(through137);
      const sql = yield* SqlClient.SqlClient;
      const before = yield* snapshot;
      for (const mutation of [
        "rowid = rowid + 1000",
        "effect_id = 'different-effect'",
        "claim_id = 'expired'",
        "command_id = 'expired'",
        "worker_id = 'different-worker'",
        "expected_attempt = 2",
        "reference_json = '{ \"tag\": 1 }'",
        "fact_json = '{ \"tag\": 1 }'",
      ]) {
        for (const expiry of ["", `, lease_expires_at = '${nextExpiry}'`]) {
          const result = yield* Effect.exit(
            sql.unsafe(`UPDATE ${starts} SET ${mutation}${expiry} WHERE effect_id = 'active'`),
          );
          assert.isTrue(Exit.isFailure(result), mutation + expiry);
          if (Exit.isFailure(result))
            assert.include(Cause.pretty(result.cause), "native execution facts are immutable");
          assert.deepStrictEqual(yield* snapshot, before, mutation + expiry);
        }
      }
    }).pipe(Effect.provide(memory)),
);

it.effect("the other eleven permanent-fact guards retain their SQL and rejection behavior", () =>
  Effect.gen(function* () {
    yield* retainedFacts;
    const sql = yield* SqlClient.SqlClient;
    const before = yield* snapshot;
    yield* runJonesMigrations(through137);
    const after = yield* snapshot;
    assert.deepStrictEqual(
      after.schema.filter(({ name }) => name !== changedTrigger),
      before.schema.filter(({ name }) => name !== changedTrigger),
    );
    for (const [table, mutation] of [
      [
        nativeTables[0],
        "command_digest = 'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'",
      ],
      [nativeTables[2], "evidence_json = '{\"changed\":true}'"],
      [nativeTables[3], "reason = 'changed'"],
    ]) {
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(sql.unsafe(`UPDATE ${table} SET ${mutation}`))),
      );
      assert.deepStrictEqual(yield* snapshot, after);
    }
    for (const table of nativeTables) {
      for (const mutation of [
        `DELETE FROM ${table}`,
        `INSERT OR REPLACE INTO ${table} SELECT * FROM ${table}`,
      ]) {
        assert.isTrue(Exit.isFailure(yield* Effect.exit(sql.unsafe(mutation))), mutation);
        assert.deepStrictEqual(yield* snapshot, after, mutation);
      }
    }
  }).pipe(Effect.provide(memory)),
);

it.effect(
  "a failure after real replacement rolls back schema, facts and ledger, then retries once",
  () =>
    Effect.gen(function* () {
      yield* retainedFacts;
      const sql = yield* SqlClient.SqlClient;
      const before = yield* snapshot;
      let replacementObserved = false;
      const result = yield* Effect.exit(
        runJonesMigrations([
          ...through136,
          [
            137,
            "NativeCreationLeaseRenewal",
            Effect.gen(function* () {
              yield* renewalMigration;
              const current = yield* sql<{
                readonly sql: string;
              }>`SELECT sql FROM sqlite_master WHERE name = ${changedTrigger}`;
              assert.notStrictEqual(current[0]!.sql, releasedTriggerSql);
              replacementObserved = true;
              yield* sql`SELECT * FROM missing_lease_renewal_rollback_fixture`;
            }),
          ],
        ]),
      );
      assert.isTrue(Exit.isFailure(result));
      assert.isTrue(replacementObserved);
      assert.deepStrictEqual(yield* snapshot, before);
      assert.deepStrictEqual(yield* runJonesMigrations(through137), [
        [137, "NativeCreationLeaseRenewal"],
      ]);
      const after = yield* snapshot;
      assert.deepStrictEqual(yield* runJonesMigrations(through137), []);
      assert.deepStrictEqual(yield* snapshot, after);
    }).pipe(Effect.provide(memory)),
);

it.effect.each(["missing", "unexpected body", "unexpected table"] as const)(
  "137 refuses a %s prerequisite without guessing a repair",
  (drift) =>
    Effect.gen(function* () {
      yield* retainedFacts;
      const sql = yield* SqlClient.SqlClient;
      yield* sql`DROP TRIGGER jones_native_creation_execution_starts_no_update`;
      if (drift === "unexpected body") {
        yield* sql`CREATE TRIGGER jones_native_creation_execution_starts_no_update
        BEFORE UPDATE ON jones_native_creation_execution_starts
        BEGIN SELECT RAISE(ABORT, 'unexpected immutable trigger'); END`;
      } else if (drift === "unexpected table") {
        yield* sql`CREATE TRIGGER jones_native_creation_execution_starts_no_update
        BEFORE UPDATE ON jones_native_creation_execution_acceptances
        BEGIN SELECT RAISE(ABORT, 'native execution facts are immutable'); END`;
      }
      const before = yield* snapshot;
      const result = yield* Effect.exit(runJonesMigrations(through137));
      assert.isTrue(Exit.isFailure(result));
      if (Exit.isFailure(result))
        assert.include(
          Cause.pretty(result.cause),
          "Native lease renewal requires the released immutable start trigger.",
        );
      assert.deepStrictEqual(yield* snapshot, before);
    }).pipe(Effect.provide(memory)),
);
