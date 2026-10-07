import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { CommandId, EventId, MessageId, RunId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { jonesMigrationEntries, runMigrations } from "../persistence/Migrations.ts";
import { runJonesMigrations } from "../persistence/JonesMigrationGuard.ts";
import { nativeCreationCanonicalJson } from "../nativeCreation/NativeCreationPreparation.ts";
import * as EffectOutbox from "./EffectOutbox.ts";

const timestamp = "2026-10-05T00:00:00.000Z";
const effectId = "effect:recorded-worktree";
const row = {
  effect_id: effectId,
  command_id: "command:recorded-worktree",
  thread_id: "thread:recorded-worktree",
  effect_type: "worktree.cleanup",
  payload_json: '{"type":"worktree.cleanup"}',
  status: "pending",
  attempt_count: 0,
  available_at: timestamp,
  lease_owner: null,
  lease_expires_at: null,
  created_at: timestamp,
  updated_at: timestamp,
  completed_at: null,
  last_error: null,
};

// Same registered effects and foreign-history fixture profile as the native evidence regression.
const sourceProfileDatabase = (profile: "own" | "foreign" | "lookup" = "own") =>
  Layer.effectDiscard(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* runMigrations({ toMigrationInclusive: 56 });
      if (profile !== "own") {
        yield* runJonesMigrations(jonesMigrationEntries.slice(0, 6));
        if (profile === "foreign") {
          for (const id of [142, 139, 140, 143, 141])
            yield* jonesMigrationEntries.find((entry) => entry[0] === id)![2];
          for (const [index, name] of [
            "V2NativeAcceptance",
            "DeletionWorktreeAdmission",
            "OrdinaryCheckoutOwnership",
            "AttachmentCleanup",
            "OrdinaryCheckoutExecutionLifetime",
          ].entries())
            yield* sql`INSERT INTO jones_sql_migrations (migration_id,name) VALUES (${index + 7},${name})`;
        } else {
          yield* jonesMigrationEntries.find((entry) => entry[0] === 138)![2];
          yield* sql`INSERT INTO jones_sql_migrations (migration_id,name) VALUES (7,'ThreadCreationLookupIndex')`;
        }
      }
      yield* runMigrations();
    }),
  ).pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })));
const layer = (profile: "own" | "foreign" | "lookup" = "own") =>
  EffectOutbox.layer.pipe(Layer.provideMerge(sourceProfileDatabase(profile)));
const fixture = Effect.fnUntraced(function* () {
  yield* TestClock.setTime(Date.parse(timestamp));
  const sql = yield* SqlClient.SqlClient;
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const read = outbox.readQualifiedCleanupSnapshot;
  if (read === undefined)
    return yield* Effect.die("Actual recorded cleanup snapshot reader missing");
  return { sql, outbox, read };
});

it.effect.each(["own", "lookup"] as const)(
  "%s provenance recognizes closed historical worktree carrier without live admission",
  (profile) =>
    Effect.gen(function* () {
      const value = yield* fixture();
      yield* value.sql`INSERT INTO orchestration_v2_effect_outbox ${value.sql.insert(row)}`;
      const before = yield* value.sql`SELECT * FROM orchestration_v2_effect_outbox`;
      const snapshot = Option.getOrThrow(yield* value.read(effectId));
      assert.equal(snapshot.id, effectId);
      assert.equal(snapshot.commandId, row.command_id);
      assert.equal(snapshot.threadId, row.thread_id);
      assert.deepEqual(snapshot.request, { type: "worktree.cleanup" });
      assert.equal(snapshot.status, "pending");
      assert.equal(snapshot.attemptCount, 0);
      assert.equal(snapshot.completedAt, null);
      assert.equal((yield* Effect.result(value.outbox.get(effectId)))._tag, "Failure");
      assert.isTrue(
        Option.isNone(
          yield* value.outbox.claimNext({ workerId: "worker:live", leaseDurationMs: 60_000 }),
        ),
      );
      assert.isTrue(Option.isNone(yield* value.outbox.nextClaimableAt));
      assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_effect_outbox`, before);
      assert.equal(
        (yield* Effect.result(EffectOutbox.decodeOrchestrationEffectPayloadV2(row.payload_json)))
          ._tag,
        "Failure",
      );
      assert.isFalse(
        Schema.is(EffectOutbox.OrchestrationEffectRequestV2)({ type: "worktree.cleanup" }),
      );
    }).pipe(Effect.provide(Layer.fresh(layer(profile)))),
);

it.effect.each([
  "extra request",
  "wrong type",
  "native envelope",
  "invalid status",
  "invalid attempt",
  "invalid lease owner",
  "invalid lease time",
  "invalid completion time",
])("recorded cleanup snapshot refuses %s without changing persisted bytes", (variant) =>
  Effect.gen(function* () {
    const value = yield* fixture();
    const invalid = {
      ...row,
      ...(variant === "extra request"
        ? { payload_json: '{"type":"worktree.cleanup","target":"/unbound"}' }
        : {}),
      ...(variant === "wrong type" ? { effect_type: "terminal.cleanup" } : {}),
      ...(variant === "native envelope"
        ? {
            payload_json:
              '{"request":{"type":"worktree.cleanup"},"nativeCreationExecutionReference":{}}',
          }
        : {}),
      ...(variant === "invalid status" ? { status: "completed" } : {}),
      ...(variant === "invalid attempt" ? { attempt_count: -1 } : {}),
      ...(variant === "invalid lease owner" ? { lease_owner: "" } : {}),
      ...(variant === "invalid lease time" ? { lease_expires_at: "unknown" } : {}),
      ...(variant === "invalid completion time" ? { completed_at: "unknown" } : {}),
    };
    if (variant === "invalid status") {
      assert.equal(
        (yield* Effect.result(
          value.sql`INSERT INTO orchestration_v2_effect_outbox ${value.sql.insert(invalid)}`,
        ))._tag,
        "Failure",
      );
      assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
      assert.isTrue(Option.isNone(yield* value.read(effectId)));
      return;
    }
    yield* value.sql`INSERT INTO orchestration_v2_effect_outbox ${value.sql.insert(invalid)}`;
    assert.equal((yield* Effect.result(value.read(effectId)))._tag, "Failure");
    assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_effect_outbox`, [invalid]);
  }).pipe(Effect.provide(Layer.fresh(layer()))),
);

it.effect(
  "foreign-shaped historical row cannot issue qualified cleanup snapshot or mutate foreign schema/ledger",
  () =>
    Effect.gen(function* () {
      const value = yield* fixture();
      yield* value.sql`INSERT INTO orchestration_v2_effect_outbox ${value.sql.insert(row)}`;
      const before = yield* value.sql`SELECT * FROM orchestration_v2_effect_outbox`;
      const ledger = yield* value.sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`;
      const schema = yield* value.sql`SELECT * FROM sqlite_master ORDER BY name`;
      assert.isTrue(Option.isNone(yield* value.read(effectId)));
      assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_effect_outbox`, before);
      assert.deepEqual(
        yield* value.sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`,
        ledger,
      );
      assert.deepEqual(yield* value.sql`SELECT * FROM sqlite_master ORDER BY name`, schema);
    }).pipe(Effect.provide(Layer.fresh(layer("foreign")))),
);

it.effect(
  "recorded namespace envelope remains historical while ordinary attachment requests retain live behavior",
  () =>
    Effect.gen(function* () {
      const value = yield* fixture();
      const reference = {
        version: 1,
        mode: "delete_thread",
        ownerBirth: {
          kind: "application_v2_thread_birth",
          threadId: ThreadId.make(row.thread_id),
          eventId: EventId.make("event:birth"),
          sequence: 1,
        },
        triggerEventId: EventId.make("event:delete"),
      } as const;
      const envelope = {
        ...row,
        effect_id: `effect:${row.command_id}:attachment.cleanup`,
        effect_type: "attachment.cleanup",
        payload_json: nativeCreationCanonicalJson({
          request: { type: "attachment.cleanup", attachmentIds: [] },
          attachmentNamespaceCleanup: reference,
        }),
      };
      yield* value.sql`INSERT INTO orchestration_v2_effect_outbox ${value.sql.insert(envelope)}`;
      const snapshot = Option.getOrThrow(yield* value.read(envelope.effect_id));
      assert.deepEqual(snapshot.attachmentNamespaceCleanup, reference);
      assert.equal((yield* Effect.result(value.outbox.get(envelope.effect_id)))._tag, "Failure");
      assert.isTrue(
        Option.isNone(
          yield* value.outbox.claimNext({ workerId: "worker:live", leaseDurationMs: 60_000 }),
        ),
      );
      const ordinary = {
        id: "effect:ordinary-attachment",
        commandId: CommandId.make("command:ordinary-attachment"),
        threadId: ThreadId.make("thread:ordinary-attachment"),
        request: { type: "attachment.cleanup" as const, attachmentIds: [] },
      };
      yield* value.outbox.enqueue([ordinary]);
      assert.deepEqual(
        Option.getOrThrow(yield* value.outbox.get(ordinary.id)).request,
        ordinary.request,
      );
      assert.equal(
        Option.getOrThrow(
          yield* value.outbox.claimNext({ workerId: "worker:live", leaseDurationMs: 60_000 }),
        ).id,
        ordinary.id,
      );
      assert.deepEqual(
        yield* value.sql`SELECT * FROM orchestration_v2_effect_outbox WHERE effect_id = ${envelope.effect_id}`,
        [envelope],
      );
    }).pipe(Effect.provide(Layer.fresh(layer()))),
);

it("roundtrips closed standalone preparation bindings and keeps old delegated payloads decodable", () => {
  const plan = {
    version: 1 as const,
    parentThreadId: ThreadId.make("source"),
    parentCheckoutPath: "/source",
    parentCommit: "a".repeat(40),
    canonicalProjectRoot: "/project",
    projectWorkspaceRoot: "/project",
    childThreadId: ThreadId.make("target"),
    branch: "fork/target",
    worktreePath: "/target",
    workspaceStrategy: {
      type: "worktree" as const,
      baseRef: "a".repeat(40),
      branch: "fork/target",
      startFromOrigin: false,
    },
  };
  const request = { type: "delegated-workspace.prepare" as const, runId: RunId.make("run"), plan };
  const decode = Schema.decodeUnknownSync(EffectOutbox.OrchestrationEffectRequestV2);
  assert.deepEqual(decode(request), request);
  const standalone = {
    version: 1 as const,
    kind: "fork" as const,
    birthCommandId: CommandId.make("birth"),
    messageCommandId: CommandId.make("send"),
    messageId: MessageId.make("message"),
  };
  assert.deepEqual(decode({ ...request, standalone }), { ...request, standalone });
  assert.throws(() => decode({ ...request, standalone: { ...standalone, authority: true } }));
  assert.throws(() => decode({ ...request, standalone: { ...standalone, version: 2 } }));
});
