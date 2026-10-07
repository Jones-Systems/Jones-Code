import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { canonicalJson } from "../../orchestration-v2/CanonicalJson.ts";
import {
  OrdinaryCheckoutExecutionExecutorV1,
  OrdinaryCheckoutExecutionRefV1,
  OrdinaryCheckoutUseV1,
  makeOrdinaryCheckoutExecutionRefV1,
  ordinaryCheckoutOutboxOperationIdV1,
} from "../../orchestration-v2/OrdinaryCheckoutOwnership.ts";
import { migrationManifest, runMigrations } from "../Migrations.ts";

class SyntheticAssociationFailure extends Schema.TaggedError<SyntheticAssociationFailure>()(
  "SyntheticAssociationFailure",
  { cause: Schema.Defect() },
) {}

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const timestamp = "2026-10-03T00:00:00.000Z";
const effectId = "effect:lifetime-fixture";
const admissionId = "a".repeat(64);
const reference = { version: 1, admissionId, admissionSha256: "b".repeat(64) };
const source = {
  kind: "outbox",
  link: {
    version: 1,
    effectId,
    commandId: "command:lifetime-fixture",
    threadId: "thread:lifetime-fixture",
    requestSha256: "c".repeat(64),
    admission: reference,
    recordedAt: timestamp,
  },
  workerId: "worker:lifetime-fixture",
  expectedAttempt: 1,
  leaseExpiresAt: "2026-10-03T00:05:00.000Z",
};
const originalUse = Schema.decodeUnknownSync(OrdinaryCheckoutUseV1, { onExcessProperty: "error" })({
  version: 1,
  kind: "ordinary_checkout_use",
  operationId: ordinaryCheckoutOutboxOperationIdV1(effectId, 1),
  admission: reference,
  source,
  lease: {
    resourcePath: "/fixture/lifetime-checkout",
    leaseId: "lease:lifetime-fixture",
    ownerThreadId: "thread:lifetime-fixture",
    ownerIncarnation: "fixture-original-birth",
    branch: "fixture-branch",
    acquiredAtMs: 1,
    renewedAtMs: 1,
    expiresAtMs: 300001,
  },
});
const executor = Schema.decodeUnknownSync(OrdinaryCheckoutExecutionExecutorV1, {
  onExcessProperty: "error",
})({ kind: "actual_outbox_claim", source });
const ref = makeOrdinaryCheckoutExecutionRefV1({ originalUse, executor });
const operationRow = {
  operation_id: originalUse.operationId,
  canonical_path: originalUse.lease.resourcePath,
  kind: "native_operation",
  subject_json: canonicalJson({
    schema: "t3.ordinary-checkout-use/v1",
    use: Schema.encodeSync(OrdinaryCheckoutUseV1)(originalUse),
    source: { projectWorkspaceRoot: "/fixture/repo", worktreePath: originalUse.lease.resourcePath },
  }),
  state: "reserved",
  started_at: null,
  outcome_json: null,
  recorded_at: timestamp,
  updated_at: timestamp,
};
// Storage fixtures do not assert a qualified live claim, producer handle or completion outcome.
const evidenceJson = '{"version":1,"fixture":"append-only participant evidence"}';
const row = {
  operation_id: originalUse.operationId,
  ordinal: 0,
  predecessor_ordinal: null,
  association_id: ref.associationId,
  admission_id: admissionId,
  executor_kind: executor.kind,
  effect_id: effectId,
  event_kind: "bind",
  association_json: canonicalJson(Schema.encodeSync(OrdinaryCheckoutExecutionRefV1)(ref)),
  evidence_json: evidenceJson,
  recorded_at: timestamp,
};
const insertReceiptAdmission = () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO orchestration_command_receipts
    (command_id, command_type, aggregate_kind, aggregate_id, accepted_at, result_sequence, status)
    VALUES ('command:lifetime-fixture', 'message.dispatch', 'thread', 'thread:lifetime-fixture', ${timestamp}, 1, 'accepted')`;
    yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_admissions
    (admission_id, command_id, thread_id, admission_sha256, admission_json, recorded_at)
    VALUES (${admissionId}, 'command:lifetime-fixture', 'thread:lifetime-fixture', ${reference.admissionSha256},
      '{"version":1,"fixture":"original admission storage carrier"}', ${timestamp})`;
  });
const insertEffect = () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO orchestration_v2_effect_outbox
    (effect_id, command_id, thread_id, effect_type, payload_json, status, attempt_count, available_at, created_at, updated_at)
    VALUES (${effectId}, 'command:lifetime-fixture', 'thread:lifetime-fixture', 'provider-turn.start', '{}', 'pending', 0, ${timestamp}, ${timestamp}, ${timestamp})`;
  });
const insertParents = () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* insertReceiptAdmission();
    yield* insertEffect();
    yield* sql`INSERT INTO orchestration_v2_worktree_path_admissions ${sql.insert(operationRow)}`;
  });

it.effect(
  "Jones141 adds one empty association relation after upstream56 without changing released upstream history",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 56 });
      const upstream = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
      assert.deepEqual(
        upstream.map((entry): readonly [unknown, unknown] => [entry.migration_id, entry.name]),
        migrationManifest,
      );
      assert.deepEqual(
        yield* sql`SELECT name FROM sqlite_master WHERE name = 'orchestration_v2_ordinary_checkout_execution_associations'`,
        [],
      );
      yield* runMigrations();
      assert.deepEqual(yield* runMigrations(), []);
      assert.deepEqual(
        yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,
        upstream,
      );
      assert.deepEqual(
        yield* sql`SELECT migration_id, name FROM jones_sql_migrations WHERE migration_id = 141`,
        [{ migration_id: 141, name: "OrdinaryCheckoutExecutionLifetime" }],
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_execution_associations`,
        [],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "execution associations require existing operation, admission and optional real effect plus strict constrained carriers",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      assert.equal(
        (yield* Effect.result(
          sql`INSERT INTO orchestration_v2_ordinary_checkout_execution_associations ${sql.insert(row)}`,
        ))._tag,
        "Failure",
      );
      yield* insertReceiptAdmission();
      assert.equal(
        (yield* Effect.result(
          sql`INSERT INTO orchestration_v2_ordinary_checkout_execution_associations ${sql.insert(row)}`,
        ))._tag,
        "Failure",
      );
      yield* sql`INSERT INTO orchestration_v2_worktree_path_admissions ${sql.insert(operationRow)}`;
      assert.equal(
        (yield* Effect.result(
          sql`INSERT INTO orchestration_v2_ordinary_checkout_execution_associations ${sql.insert(row)}`,
        ))._tag,
        "Failure",
      );
      yield* insertEffect();
      for (const invalid of [
        { operation_id: null },
        { operation_id: "missing" },
        { ordinal: null },
        { ordinal: -1 },
        { predecessor_ordinal: 0 },
        { admission_id: null },
        { admission_id: "d".repeat(64) },
        { effect_id: "missing" },
        { association_id: null },
        { association_id: "A".repeat(64) },
        { association_id: "g".repeat(64) },
        { association_id: "a".repeat(63) },
        { executor_kind: "current_thread_lookup" },
        { executor_kind: null },
        { event_kind: "completed" },
        { event_kind: null },
        { association_json: null },
        { association_json: "invalid" },
        { association_json: "[]" },
        { association_json: "{}" },
        { association_json: '{"version":2}' },
        { association_json: '{"version":"1"}' },
        { association_json: '{"version":true}' },
        { evidence_json: null },
        { evidence_json: "invalid" },
        { evidence_json: "[]" },
        { recorded_at: null },
        { recorded_at: "" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_ordinary_checkout_execution_associations ${sql.insert({ ...row, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_execution_associations ${sql.insert(row)}`;
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_execution_associations`,
        [row],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "append succession uses the exact previous ordinal within the same original operation and retains repeated participant facts",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* insertParents();
      yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_execution_associations ${sql.insert(row)}`;
      for (const invalid of [
        { ordinal: 1, predecessor_ordinal: null },
        { ordinal: 1, predecessor_ordinal: 1 },
        { ordinal: 2, predecessor_ordinal: 0 },
        { ordinal: 2, predecessor_ordinal: 1 },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_ordinary_checkout_execution_associations ${sql.insert({ ...row, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      const renewed = { ...row, ordinal: 1, predecessor_ordinal: 0, event_kind: "renew" };
      yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_execution_associations ${sql.insert(renewed)}`;
      assert.equal(
        (yield* Effect.result(
          sql`INSERT INTO orchestration_v2_ordinary_checkout_execution_associations ${sql.insert({ ...renewed, association_id: "d".repeat(64) })}`,
        ))._tag,
        "Failure",
      );
      const retired = { ...row, ordinal: 2, predecessor_ordinal: 1, event_kind: "retire" };
      yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_execution_associations ${sql.insert(retired)}`;
      const otherOperation = {
        ...operationRow,
        operation_id: "operation:lifetime-independent",
        canonical_path: "/fixture/independent",
      };
      yield* sql`INSERT INTO orchestration_v2_worktree_path_admissions ${sql.insert(otherOperation)}`;
      assert.equal(
        (yield* Effect.result(
          sql`INSERT INTO orchestration_v2_ordinary_checkout_execution_associations ${sql.insert({ ...renewed, operation_id: otherOperation.operation_id })}`,
        ))._tag,
        "Failure",
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_execution_associations ORDER BY ordinal`,
        [row, renewed, retired],
      );
      assert.deepEqual(
        yield* sql`SELECT state FROM orchestration_v2_worktree_path_admissions WHERE operation_id = ${originalUse.operationId}`,
        [{ state: "reserved" }],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "all closed executor and event facts share the original reservation and may retain real start-effect lineage",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* insertParents();
      const events = ["bind", "activate", "join", "renew", "retire", "unknown"];
      const acceptedRun = {
        runId: "run:lifetime-fixture",
        runAttemptId: "attempt:lifetime-fixture",
        nodeId: "node:lifetime-fixture",
        messageId: "message:lifetime-fixture",
      };
      const managed = yield* Schema.decodeUnknownEffect(OrdinaryCheckoutExecutionExecutorV1, {
        onExcessProperty: "error",
      })({
        kind: "captured_managed_run",
        captureId: "capture:lifetime-fixture",
        run: acceptedRun,
        checkpointScopeId: "scope:lifetime-fixture",
        driver: "codex",
        binding: {
          threadId: "thread:lifetime-fixture",
          providerThreadId: "provider-thread:lifetime-fixture",
          providerSessionId: "provider-session:lifetime-fixture",
          instanceId: "fixture-codex",
        },
      }).pipe(Effect.orDie);
      const prepared = yield* Schema.decodeUnknownEffect(OrdinaryCheckoutExecutionExecutorV1, {
        onExcessProperty: "error",
      })({
        kind: "actual_prepared_producer",
        producerId: "producer:lifetime-fixture",
        source: { kind: "prepared_run", admission: reference, preparation: acceptedRun },
      }).pipe(Effect.orDie);
      const references = [
        ref,
        makeOrdinaryCheckoutExecutionRefV1({ originalUse, executor: managed }),
        makeOrdinaryCheckoutExecutionRefV1({ originalUse, executor: prepared }),
      ];
      const expected = [];
      for (const [ordinal, eventKind] of events.entries()) {
        const actualRef = references[ordinal % references.length]!;
        const encodedRef = yield* Schema.encodeEffect(OrdinaryCheckoutExecutionRefV1)(
          actualRef,
        ).pipe(Effect.orDie);
        const next = {
          ...row,
          ordinal,
          predecessor_ordinal: ordinal === 0 ? null : ordinal - 1,
          association_id: actualRef.associationId,
          executor_kind: actualRef.executor.kind,
          association_json: canonicalJson(encodedRef),
          effect_id: ordinal === 2 ? null : effectId,
          event_kind: eventKind,
        };
        yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_execution_associations ${sql.insert(next)}`;
        expected.push(next);
      }
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_execution_associations ORDER BY ordinal`,
        expected,
      );
      assert.deepEqual(
        yield* sql`SELECT operation_id, state FROM orchestration_v2_worktree_path_admissions`,
        [{ operation_id: originalUse.operationId, state: "reserved" }],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "association facts are permanent across every-field mutation, deletion and composite replacement with recursive triggers off",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`PRAGMA recursive_triggers = OFF`;
      yield* insertParents();
      yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_execution_associations ${sql.insert(row)}`;
      for (const changed of [
        { operation_id: "changed" },
        { ordinal: 1 },
        { predecessor_ordinal: 0 },
        { association_id: "d".repeat(64) },
        { admission_id: "d".repeat(64) },
        { executor_kind: "captured_managed_run" },
        { effect_id: null },
        { event_kind: "unknown" },
        { association_json: '{"version":1,"fixture":"changed"}' },
        { evidence_json: '{"version":1,"fixture":"changed"}' },
        { recorded_at: "changed" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE orchestration_v2_ordinary_checkout_execution_associations ${sql.update(changed)}`,
          ))._tag,
          "Failure",
        );
      for (const mutation of [
        sql`DELETE FROM orchestration_v2_ordinary_checkout_execution_associations`,
        sql`DELETE FROM orchestration_v2_effect_outbox WHERE effect_id = ${effectId}`,
        sql`INSERT OR REPLACE INTO orchestration_v2_ordinary_checkout_execution_associations ${sql.insert({ ...row, event_kind: "unknown" })}`,
      ])
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_execution_associations`,
        [row],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "failed outer bind rolls back receipt, admission, original path reservation, effect and execution association together",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      const injectedCause = new Error("synthetic execution association transaction failure");
      const result = yield* Effect.result(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* insertParents();
            yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_execution_associations ${sql.insert(row)}`;
            return yield* Effect.fail(new SyntheticAssociationFailure({ cause: injectedCause }));
          }),
        ),
      );
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.equal(result.failure._tag, "SyntheticAssociationFailure");
        if (result.failure._tag === "SyntheticAssociationFailure") {
          assert.strictEqual(result.failure.cause, injectedCause);
        }
      }
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_admissions`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_worktree_path_admissions`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_execution_associations`,
        [],
      );
    }).pipe(Effect.provide(memory)),
);
