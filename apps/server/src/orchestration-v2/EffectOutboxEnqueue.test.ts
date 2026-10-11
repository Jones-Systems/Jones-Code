import { assert, it } from "@effect/vitest";
import { CommandId, ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlError from "effect/sql/SqlError";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as Enqueue from "./EffectOutboxEnqueue.ts";

const layer = EffectOutbox.layer.pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);

const createTable = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE orchestration_v2_effect_outbox (
    effect_id TEXT PRIMARY KEY CHECK(effect_id != 'refused'),
    command_id TEXT NOT NULL, thread_id TEXT NOT NULL, effect_type TEXT NOT NULL,
    payload_json TEXT NOT NULL, status TEXT NOT NULL, attempt_count INTEGER NOT NULL DEFAULT 0,
    available_at TEXT NOT NULL, lease_owner TEXT, lease_expires_at TEXT,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL, completed_at TEXT, last_error TEXT
  )`;
});

const pending = (id: string): Enqueue.PendingOrchestrationEffectV2 => ({
  id,
  commandId: CommandId.make("command:enqueue"),
  threadId: ThreadId.make("thread:enqueue"),
  request: { type: "terminal.cleanup" },
});

const enqueueWith = (
  path: "leaf" | "service",
  effects: ReadonlyArray<Enqueue.PendingOrchestrationEffectV2>,
) =>
  path === "leaf"
    ? Enqueue.enqueue(effects)
    : Effect.flatMap(EffectOutbox.EffectOutboxV2, (outbox) => outbox.enqueue(effects));

it("preserves the public schema and error identities", () => {
  assert.strictEqual(
    EffectOutbox.OrchestrationEffectRequestV2,
    Enqueue.OrchestrationEffectRequestV2,
  );
  assert.strictEqual(
    EffectOutbox.NativeOrchestrationEffectPayloadV2,
    Enqueue.NativeOrchestrationEffectPayloadV2,
  );
  assert.strictEqual(EffectOutbox.EffectOutboxError, Enqueue.EffectOutboxError);
});

it.effect.each(["leaf", "service"] as const)(
  "%s persists exact ordinary/native rows in order without notifying workers",
  (path) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      yield* createTable;
      yield* TestClock.adjust("1 second");
      const now = DateTime.formatIso(yield* DateTime.now);
      const availableAt = "2040-01-02T03:04:05.006Z";
      const nativeReference = {
        version: 2 as const,
        claimId: "claim:enqueue",
        stageCommandId: CommandId.make("command:stage"),
        effectId: "effect:native",
        stage: "cleanup" as const,
      };
      const wake = yield* outbox.awaitAvailable.pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* enqueueWith(path, [
        pending("first"),
        {
          ...pending("first"),
          request: { type: "attachment.cleanup", attachmentIds: ["must-not-replace"] },
        },
        {
          ...pending("second"),
          request: { type: "attachment.cleanup", attachmentIds: ["owned-attachment"] },
          availableAt: DateTime.makeUnsafe(availableAt),
          nativeCreationExecutionReference: nativeReference,
        },
      ]);
      const rows = yield* sql`SELECT * FROM orchestration_v2_effect_outbox ORDER BY rowid`;
      assert.deepEqual(rows, [
        {
          effect_id: "first",
          command_id: "command:enqueue",
          thread_id: "thread:enqueue",
          effect_type: "terminal.cleanup",
          payload_json: '{"type":"terminal.cleanup"}',
          status: "pending",
          attempt_count: 0,
          available_at: now,
          lease_owner: null,
          lease_expires_at: null,
          created_at: now,
          updated_at: now,
          completed_at: null,
          last_error: null,
        },
        {
          effect_id: "second",
          command_id: "command:enqueue",
          thread_id: "thread:enqueue",
          effect_type: "attachment.cleanup",
          payload_json:
            '{"request":{"type":"attachment.cleanup","attachmentIds":["owned-attachment"]},"nativeCreationExecutionReference":{"version":2,"claimId":"claim:enqueue","stageCommandId":"command:stage","effectId":"effect:native","stage":"cleanup"}}',
          status: "pending",
          attempt_count: 0,
          available_at: availableAt,
          lease_owner: null,
          lease_expires_at: null,
          created_at: now,
          updated_at: now,
          completed_at: null,
          last_error: null,
        },
      ]);
      yield* Effect.yieldNow;
      assert.isUndefined(wake.pollUnsafe());
      yield* outbox.notifyAvailable();
      yield* Fiber.join(wake);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["leaf", "service"] as const)(
  "%s participates in the caller transaction and preserves an intentional rollback",
  (path) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* createTable;
      const rollback = yield* sql
        .withTransaction(
          enqueueWith(path, [pending("rolled-back")]).pipe(
            Effect.andThen(Effect.fail("owner-rollback")),
          ),
        )
        .pipe(Effect.flip);
      assert.equal(rollback, "owner-rollback");
      assert.deepEqual(yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox`, []);
      yield* enqueueWith(path, [pending("committed")]);
      assert.deepEqual(yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox`, [
        { effect_id: "committed" },
      ]);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["leaf", "service"] as const)(
  "%s keeps the SQL failure cause and rolls back preceding inserts",
  (path) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* createTable;
      const error = yield* sql
        .withTransaction(enqueueWith(path, [pending("before-failure"), pending("refused")]))
        .pipe(Effect.flip);
      assert.instanceOf(error, Enqueue.EffectOutboxError);
      if (!(error instanceof Enqueue.EffectOutboxError)) return;
      assert.equal(error.operation, "enqueue");
      assert.equal(error.message, "Orchestration effect outbox enqueue failed.");
      assert.isTrue(Schema.is(SqlError.SqlError)(error.cause));
      assert.deepEqual(yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox`, []);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["leaf", "service"] as const)(
  "%s accepts an empty batch without querying a missing table",
  (path) => enqueueWith(path, []).pipe(Effect.provide(layer)),
);

it.effect("the server delegate retains its captured SQL client under another SQL context", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    yield* createTable;
    // The inner database has no table. Enqueue must still use the server service's captured client.
    yield* outbox
      .enqueue([pending("captured")])
      .pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" })));
    assert.deepEqual(yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox`, [
      { effect_id: "captured" },
    ]);
  }).pipe(Effect.provide(layer)),
);
