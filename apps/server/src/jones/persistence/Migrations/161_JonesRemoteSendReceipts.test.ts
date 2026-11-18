import { assert, it } from "@effect/vitest";
import { OrchestratorMcpThreadSendResult } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { SqlError, UnknownError } from "effect/sql/SqlError";

import { migrationManifest, runMigrations } from "../../../persistence/Migrations.ts";
import { runJonesMigrations } from "../JonesMigrationGuard.ts";
import { jonesMigrationEntries } from "../JonesMigrations.ts";
import migration from "./161_JonesRemoteSendReceipts.ts";

const decodeSendResult = Schema.decodeUnknownEffect(OrchestratorMcpThreadSendResult, {
  onExcessProperty: "error",
});
const decodeSendResultJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestratorMcpThreadSendResult),
  {
    onExcessProperty: "error",
  },
);

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const prior = jonesMigrationEntries.filter(([id]) => id < 161);
const through161 = jonesMigrationEntries.filter(([id]) => id <= 161);
const result = {
  threadId: "receipt-thread",
  messageId: "receipt-message",
  runId: "receipt-run",
  status: "running",
  delivery: "started",
};
const receipt = {
  command_id: "send-original",
  origin_namespace: "authenticated-peer:fixture",
  client_request_id: "request-original",
  request_digest: "a".repeat(64),
  thread_id: "receipt-thread",
  expected_created_at: "2026-11-16T20:00:00.000Z",
  result_json: JSON.stringify(result),
  recorded_at: "2026-11-16T20:14:00.000Z",
};
const prepare = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`PRAGMA foreign_keys = ON`;
  yield* runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] });
  yield* runJonesMigrations(prior);
});
const objects = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<{ readonly name: string }>`SELECT type,name,tbl_name,sql FROM sqlite_master
    WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name`;
});

it.effect("161 adds only an empty sparse receipt table, its indexes and immutable guards", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* prepare;
    const before = yield* objects;
    const upstream = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
    const released = yield* sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`;
    assert.deepStrictEqual(yield* runJonesMigrations(through161), [[161, "RemoteSendReceipts"]]);
    const after = yield* objects;
    assert.deepStrictEqual(
      after.filter(({ name }) => !name.startsWith("jones_remote_send_receipts")),
      before,
    );
    assert.deepStrictEqual(
      after
        .filter(({ name }) => name.startsWith("jones_remote_send_receipts"))
        .map(({ name }) => name)
        .sort(),
      [
        "jones_remote_send_receipts",
        "jones_remote_send_receipts_lookup",
        "jones_remote_send_receipts_no_replace",
        "jones_remote_send_receipts_no_update",
        "jones_remote_send_receipts_recorded_at",
      ].sort(),
    );
    assert.deepStrictEqual(yield* sql`SELECT * FROM jones_remote_send_receipts`, []);
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_list(jones_remote_send_receipts)`, []);
    assert.deepStrictEqual(
      yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,
      upstream,
    );
    assert.deepStrictEqual(
      yield* sql`SELECT * FROM jones_sql_migrations WHERE migration_id < 161 ORDER BY migration_id`,
      released,
    );
    assert.deepStrictEqual(yield* sql`PRAGMA index_info(jones_remote_send_receipts_lookup)`, [
      { seqno: 0, cid: 1, name: "origin_namespace" },
      { seqno: 1, cid: 4, name: "thread_id" },
      { seqno: 2, cid: 2, name: "client_request_id" },
    ]);
    assert.deepStrictEqual(yield* sql`PRAGMA index_info(jones_remote_send_receipts_recorded_at)`, [
      { seqno: 0, cid: 7, name: "recorded_at" },
    ]);
    assert.deepStrictEqual(yield* runJonesMigrations(through161), []);
    assert.deepStrictEqual(yield* objects, after);
  }).pipe(Effect.provide(memory)),
);

it.effect(
  "161 retains the original bytes against UPDATE, REPLACE, duplicates and conflict clauses",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* prepare;
      yield* runJonesMigrations(through161);
      // This is a real accepted-result fixture. The production strict codec belongs to core.
      const original = yield* decodeSendResult(result);
      yield* sql`INSERT INTO jones_remote_send_receipts ${sql.insert(receipt)}`;
      for (const changed of [
        receipt,
        { ...receipt, origin_namespace: "different-peer", result_json: "{}" },
      ]) {
        for (const statement of [
          sql`INSERT INTO jones_remote_send_receipts ${sql.insert(changed)}`,
          sql`INSERT OR REPLACE INTO jones_remote_send_receipts ${sql.insert(changed)}`,
          sql`REPLACE INTO jones_remote_send_receipts ${sql.insert(changed)}`,
          sql`INSERT OR IGNORE INTO jones_remote_send_receipts ${sql.insert(changed)}`,
          sql`INSERT INTO jones_remote_send_receipts ${sql.insert(changed)} ON CONFLICT(command_id) DO NOTHING`,
          sql`INSERT INTO jones_remote_send_receipts ${sql.insert(changed)} ON CONFLICT(command_id) DO UPDATE SET result_json = excluded.result_json`,
        ]) {
          assert.isTrue(Exit.isFailure(yield* Effect.exit(statement)));
          assert.deepStrictEqual(yield* sql`SELECT * FROM jones_remote_send_receipts`, [receipt]);
        }
      }
      for (const field of Object.keys(receipt)) {
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              sql.unsafe(
                `UPDATE jones_remote_send_receipts SET ${field} = ${field} WHERE command_id = ?`,
                [receipt.command_id],
              ),
            ),
          ),
        );
      }
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(sql`UPDATE jones_remote_send_receipts SET result_json = '{}'`),
        ),
      );
      const [stored] = yield* sql<{
        result_json: string;
      }>`SELECT result_json FROM jones_remote_send_receipts WHERE command_id = ${receipt.command_id}`;
      assert.deepStrictEqual(yield* decodeSendResultJson(stored!.result_json), original);
      assert.deepStrictEqual(yield* sql`SELECT * FROM jones_remote_send_receipts`, [receipt]);
      // Only the command ID is unique: these scopes may legitimately reuse a request ID.
      for (const [command_id, origin_namespace, thread_id] of [
        ["same-scope-other-command", receipt.origin_namespace, receipt.thread_id],
        ["other-namespace", "authenticated-peer:other", receipt.thread_id],
        ["other-thread", receipt.origin_namespace, "other-thread"],
      ])
        yield* sql`INSERT INTO jones_remote_send_receipts ${sql.insert({ ...receipt, command_id, origin_namespace, thread_id, expected_created_at: null })}`;
      assert.lengthOf(yield* sql`SELECT * FROM jones_remote_send_receipts`, 4);
      // Retention is an explicit bounded caller action. The schema creates no synthetic replacement.
      yield* sql`DELETE FROM jones_remote_send_receipts WHERE command_id = ${receipt.command_id}`;
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM jones_remote_send_receipts WHERE command_id = ${receipt.command_id}`,
        [],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "161 rejects empty bindings, malformed JSON and every non-lowercase SHA256 representation",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* prepare;
      yield* runJonesMigrations(through161);
      const invalid: Array<Record<string, unknown>> = [
        ...[
          "command_id",
          "origin_namespace",
          "client_request_id",
          "thread_id",
          "recorded_at",
        ].flatMap((field) => [
          { ...receipt, [field]: "" },
          { ...receipt, [field]: null },
        ]),
        { ...receipt, expected_created_at: "" },
        { ...receipt, result_json: "{" },
        { ...receipt, result_json: null },
        ...[
          "",
          "a".repeat(63),
          "a".repeat(65),
          "A".repeat(64),
          "g".repeat(64),
          "0".repeat(63) + "é",
          "a".repeat(64) + "\0tail",
        ].map((request_digest) => ({ ...receipt, request_digest })),
        { ...receipt, request_digest: null },
      ];
      for (const row of invalid) {
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(sql`INSERT INTO jones_remote_send_receipts ${sql.insert(row)}`),
          ),
        );
        assert.deepStrictEqual(yield* sql`SELECT * FROM jones_remote_send_receipts`, []);
      }
      yield* sql`INSERT INTO jones_remote_send_receipts ${sql.insert({ ...receipt, request_digest: "0123456789abcdef".repeat(4), expected_created_at: null })}`;
      assert.lengthOf(yield* sql`SELECT * FROM jones_remote_send_receipts`, 1);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "161 DDL, indexes, triggers and its ledger roll back together on a late pending failure",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* prepare;
      const before = yield* objects;
      const ledger = yield* sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`;
      const upstream = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
      const failed = migration.pipe(
        Effect.andThen(
          Effect.fail(
            new SqlError({
              reason: new UnknownError({ cause: "synthetic failure after all 161 DDL" }),
            }),
          ),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(runJonesMigrations([...prior, [161, "RemoteSendReceipts", failed]])),
        ),
      );
      assert.deepStrictEqual(yield* objects, before);
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`,
        ledger,
      );
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,
        upstream,
      );
      yield* sql`CREATE TRIGGER jones_remote_send_receipts_no_replace AFTER INSERT ON jones_sql_migrations BEGIN SELECT 1; END`;
      const collision = yield* objects;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(runJonesMigrations(through161))));
      assert.deepStrictEqual(yield* objects, collision);
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`,
        ledger,
      );
      yield* sql`DROP TRIGGER jones_remote_send_receipts_no_replace`;
      assert.deepStrictEqual(yield* runJonesMigrations(through161), [[161, "RemoteSendReceipts"]]);
    }).pipe(Effect.provide(memory)),
);
