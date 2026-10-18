import { assert, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import * as Evidence from "./evidence.ts";
import { captureNativeProviderSettlementProof } from "./settlementProof.ts";
import type { NativeProviderCommandReceipt } from "./service.ts";

const memory = Evidence.makeNativeProviderEvidenceLayer().pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);
const threadId = ThreadId.make("thread-synthetic");
const now = "2026-01-01T00:00:00.000Z";
const initialize = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // Private columns are absent, so reading payload, actor, or error content fails.
  yield* sql`CREATE TABLE orchestration_events (
    sequence INTEGER, event_id TEXT, event_type TEXT, aggregate_kind TEXT,
    stream_id TEXT, occurred_at TEXT, command_id TEXT, application_event_version INTEGER
  )`;
  yield* sql`CREATE TABLE orchestration_command_receipts (
    command_id TEXT, aggregate_kind TEXT, aggregate_id TEXT, command_type TEXT,
    accepted_at TEXT, result_sequence INTEGER, status TEXT
  )`;
  yield* sql`CREATE TABLE jones_workstreams_settlement_proofs (
    native_command_id TEXT PRIMARY KEY, thread_id TEXT, native_action TEXT, proof_json TEXT
  )`;
  return sql;
});

it.effect("reads only durable receipt and event metadata without private columns", () =>
  Effect.gen(function* () {
    const sql = yield* initialize;
    yield* sql`INSERT INTO orchestration_events VALUES
      (1, 'event-synthetic', 'thread.settled', 'thread', ${threadId}, ${now}, 'command-synthetic', 2)`;
    yield* sql`INSERT INTO orchestration_command_receipts VALUES
      ('command-synthetic', 'thread', ${threadId}, 'thread.settle', ${now}, 1, 'accepted')`;
    const reader = yield* Evidence.NativeProviderEvidence;
    assert.deepEqual(yield* reader.readSnapshotByCommandId("command-synthetic", threadId), {
      receipt: Option.some<NativeProviderCommandReceipt>({
        commandId: "command-synthetic",
        aggregateKind: "thread",
        aggregateId: threadId,
        commandType: "thread.settle",
        acceptedAt: now,
        resultSequence: 1,
        status: "accepted",
      }),
      events: [
        {
          eventId: "event-synthetic",
          commandId: "command-synthetic",
          aggregateKind: "thread",
          aggregateId: threadId,
          sequence: 1,
          type: "thread.settled",
          occurredAt: now,
          applicationEventVersion: 2,
        },
      ],
    });
    assert.deepEqual(yield* reader.readSnapshotByCommandId("absent", threadId), {
      receipt: Option.none(),
      events: [],
    });
  }).pipe(Effect.provide(memory)),
);

it.effect("preserves sorted overflow evidence and parameterizes the command lookup", () =>
  Effect.gen(function* () {
    const sql = yield* initialize;
    yield* sql`WITH RECURSIVE numbers(n) AS (
      SELECT 258 UNION ALL SELECT n - 1 FROM numbers WHERE n > 1
    ) INSERT INTO orchestration_events
      SELECT n, 'event-' || n, 'thread.settled', 'thread', ${threadId}, ${now}, 'command-synthetic', 2
      FROM numbers ORDER BY n DESC`;
    yield* sql`INSERT INTO orchestration_events VALUES
      (259, 'foreign-event', 'thread.settled', 'thread', ${threadId}, ${now}, 'foreign-command', 2)`;
    const reader = yield* Evidence.NativeProviderEvidence;
    const snapshot = yield* reader.readSnapshotByCommandId("command-synthetic", threadId);
    assert.strictEqual(snapshot.events.length, 257);
    assert.deepEqual(
      snapshot.events.map((event) => event.sequence),
      Array.from({ length: 257 }, (_, index) => index + 1),
    );
    assert.strictEqual(Option.isNone(snapshot.receipt), true);
    assert.deepEqual(yield* reader.readSnapshotByCommandId("command' OR 1=1 --", threadId), {
      receipt: Option.none(),
      events: [],
    });
    assert.strictEqual(
      (yield* reader.readSnapshotByCommandId("foreign-command", threadId)).events.length,
      1,
    );
  }).pipe(Effect.provide(memory)),
);

it.effect(
  "retains conflicting domains, old versions, and zero-event receipts for consumer evaluation",
  () =>
    Effect.gen(function* () {
      const sql = yield* initialize;
      yield* sql`INSERT INTO orchestration_events VALUES
      (1, 'foreign-project-event', 'project.created', 'project', 'foreign-project', ${now}, 'command-synthetic', 1),
      (2, 'foreign-thread-event', 'thread.settled', 'thread', 'foreign-thread', ${now}, 'command-synthetic', 2)`;
      yield* sql`INSERT INTO orchestration_command_receipts VALUES
      ('command-synthetic', 'thread', ${threadId}, 'thread.settle', ${now}, 2, 'accepted'),
      ('zero-events', 'thread', ${threadId}, 'thread.settle', ${now}, 2, 'accepted'),
      ('rejected', 'thread', ${threadId}, 'thread.settle', ${now}, 2, 'rejected')`;
      const reader = yield* Evidence.NativeProviderEvidence;
      const snapshot = yield* reader.readSnapshotByCommandId("command-synthetic", threadId);
      assert.deepEqual(
        snapshot.events.map((event) => [
          event.aggregateKind,
          event.aggregateId,
          event.applicationEventVersion,
        ]),
        [
          ["project", "foreign-project", 1],
          ["thread", "foreign-thread", 2],
        ],
      );
      for (const [commandId, status] of [
        ["zero-events", "accepted"],
        ["rejected", "rejected"],
      ] as const) {
        const result = yield* reader.readSnapshotByCommandId(commandId, threadId);
        assert.deepEqual(result.events, []);
        assert.strictEqual(Option.getOrThrow(result.receipt).status, status);
        assert.strictEqual(Object.hasOwn(Option.getOrThrow(result.receipt), "error"), false);
      }
    }).pipe(Effect.provide(memory)),
);

it.effect("missing schema and invalid scalar records produce content-free typed errors", () =>
  Effect.gen(function* () {
    const reader = yield* Evidence.NativeProviderEvidence;
    const absent = yield* Effect.flip(
      reader.readSnapshotByCommandId("command-synthetic", threadId),
    );
    assert.strictEqual(Schema.is(Evidence.NativeProviderEvidenceReadError)(absent), true);
    if (!Schema.is(Evidence.NativeProviderEvidenceReadError)(absent))
      throw new Error("Expected typed failure.");
    assert.strictEqual(absent.reason, "reader_unavailable");
    assert.strictEqual(Object.hasOwn(absent, "cause"), false);
    const sql = yield* initialize;
    yield* sql`INSERT INTO orchestration_events VALUES
      (1, 'event', 'thread.settled', 'unsupported-aggregate', ${threadId}, ${now}, 'command-synthetic', 2)`;
    const invalid = yield* Effect.flip(
      reader.readSnapshotByCommandId("command-synthetic", threadId),
    );
    if (!Schema.is(Evidence.NativeProviderEvidenceReadError)(invalid))
      throw new Error("Expected typed failure.");
    assert.strictEqual(invalid.reason, "invalid_snapshot");
    assert.strictEqual(Object.hasOwn(invalid, "cause"), false);
    assert.strictEqual(invalid.message, "Native command evidence could not be read.");
  }).pipe(Effect.provide(memory)),
);

it.effect("ambiguous receipt rows fail closed instead of selecting one", () =>
  Effect.gen(function* () {
    const sql = yield* initialize;
    yield* sql`INSERT INTO orchestration_command_receipts VALUES
      ('command-synthetic', 'thread', ${threadId}, 'thread.settle', ${now}, 1, 'accepted'),
      ('command-synthetic', 'thread', 'other-thread', 'thread.settle', ${now}, 1, 'accepted')`;
    const reader = yield* Evidence.NativeProviderEvidence;
    const invalid = yield* Effect.flip(
      reader.readSnapshotByCommandId("command-synthetic", threadId),
    );
    if (!Schema.is(Evidence.NativeProviderEvidenceReadError)(invalid))
      throw new Error("Expected typed failure.");
    assert.strictEqual(invalid.reason, "invalid_snapshot");
  }).pipe(Effect.provide(memory)),
);

const proofReceipt = {
  commandId: "command-synthetic",
  aggregateKind: "thread" as const,
  aggregateId: threadId,
  commandType: "thread.settle",
  acceptedAt: now,
  resultSequence: 2,
  status: "accepted" as const,
};
const proofEvents = ["thread.settled", "thread.unpinned"].map((type, index) => ({
  eventId: `proof-event-${index}`,
  commandId: proofReceipt.commandId,
  aggregateKind: "thread" as const,
  aggregateId: threadId,
  sequence: index + 1,
  type,
  occurredAt: now,
  applicationEventVersion: 2,
}));
const reserveProof = Effect.gen(function* () {
  const sql = yield* initialize;
  yield* sql`INSERT INTO jones_workstreams_settlement_proofs VALUES ('command-synthetic', ${threadId}, 'settle', NULL)`;
  return sql;
});
const persistProofCommand = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO orchestration_command_receipts VALUES ('command-synthetic', 'thread', ${threadId}, 'thread.settle', ${now}, 2, 'accepted')`;
  for (const event of proofEvents)
    yield* sql`INSERT INTO orchestration_events VALUES (${event.sequence}, ${event.eventId}, ${event.type}, 'thread', ${threadId}, ${now}, 'command-synthetic', 2)`;
  yield* captureNativeProviderSettlementProof({ receipt: proofReceipt, events: proofEvents });
});

it.effect("proof and receipt/event writes roll back together after a post-capture failure", () =>
  Effect.gen(function* () {
    const sql = yield* reserveProof;
    const failed = yield* Effect.exit(
      sql.withTransaction(
        persistProofCommand.pipe(Effect.andThen(Effect.fail("synthetic-rollback"))),
      ),
    );
    assert.isTrue(Exit.isFailure(failed));
    assert.deepEqual(yield* sql`SELECT proof_json FROM jones_workstreams_settlement_proofs`, [
      { proof_json: null },
    ]);
    assert.deepEqual(yield* sql`SELECT * FROM orchestration_events`, []);
    assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, []);
    yield* sql.withTransaction(persistProofCommand);
    yield* sql`DELETE FROM orchestration_events`;
    const reader = yield* Evidence.NativeProviderEvidence;
    assert.deepEqual(yield* reader.readSnapshotByCommandId(proofReceipt.commandId, threadId), {
      receipt: Option.some(proofReceipt),
      events: proofEvents,
    });
    yield* sql`DELETE FROM orchestration_command_receipts`;
    assert.deepEqual(yield* reader.readSnapshotByCommandId(proofReceipt.commandId, threadId), {
      receipt: Option.none(),
      events: [],
    });
  }).pipe(Effect.provide(memory)),
);

it.effect.each(["receipt", "surviving-event", "proof", "missing-proof"] as const)(
  "compacted evidence fails closed for %s conflict",
  (conflict) =>
    Effect.gen(function* () {
      const sql = yield* reserveProof;
      yield* sql.withTransaction(persistProofCommand);
      yield* sql`DELETE FROM orchestration_events`;
      if (conflict === "receipt")
        yield* sql`UPDATE orchestration_command_receipts SET result_sequence = 99`;
      if (conflict === "surviving-event")
        yield* sql`INSERT INTO orchestration_events VALUES (1, 'conflicting', 'thread.settled', 'thread', ${threadId}, ${now}, 'command-synthetic', 2)`;
      if (conflict === "proof")
        yield* sql`UPDATE jones_workstreams_settlement_proofs SET proof_json = '{}'`;
      if (conflict === "missing-proof")
        yield* sql`UPDATE jones_workstreams_settlement_proofs SET proof_json = NULL`;
      const reader = yield* Evidence.NativeProviderEvidence;
      if (conflict === "missing-proof") {
        assert.deepEqual(
          (yield* reader.readSnapshotByCommandId(proofReceipt.commandId, threadId)).events,
          [],
        );
      } else {
        const result = yield* Effect.flip(
          reader.readSnapshotByCommandId(proofReceipt.commandId, threadId),
        );
        assert.isTrue(Schema.is(Evidence.NativeProviderEvidenceReadError)(result));
        if (!Schema.is(Evidence.NativeProviderEvidenceReadError)(result))
          throw new Error("Expected typed evidence failure.");
        assert.strictEqual(result.reason, "invalid_snapshot");
      }
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "rejected proof is sparse, has no events and still requires the retained rejection receipt",
  () =>
    Effect.gen(function* () {
      const sql = yield* reserveProof;
      const receipt = { ...proofReceipt, status: "rejected" as const };
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`INSERT INTO orchestration_command_receipts VALUES ('command-synthetic', 'thread', ${threadId}, 'thread.settle', ${now}, 2, 'rejected')`;
          yield* captureNativeProviderSettlementProof({ receipt, events: [] });
        }),
      );
      yield* captureNativeProviderSettlementProof({
        receipt: { ...receipt, commandId: "ordinary-command" },
        events: [],
      });
      assert.strictEqual(
        (yield* sql`SELECT native_command_id FROM jones_workstreams_settlement_proofs`).length,
        1,
      );
      const reader = yield* Evidence.NativeProviderEvidence;
      assert.deepEqual(yield* reader.readSnapshotByCommandId(receipt.commandId, threadId), {
        receipt: Option.some(receipt),
        events: [],
      });
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            captureNativeProviderSettlementProof({ receipt, events: proofEvents }),
          ),
        ),
      );
      yield* sql`UPDATE orchestration_command_receipts SET status = 'accepted'`;
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(reader.readSnapshotByCommandId(receipt.commandId, threadId)),
        ),
      );
    }).pipe(Effect.provide(memory)),
);

it.effect.each([
  "gap",
  "tail-time",
  "wrong-action",
  "wrong-thread",
  "duplicate-settlement",
  "overflow",
  "zero-events",
] as const)("refuses invalid proof capture: %s", (conflict) =>
  Effect.gen(function* () {
    const sql = yield* reserveProof;
    const events = proofEvents.map((event) => ({ ...event }));
    if (conflict === "gap") events[0]!.sequence = 2;
    if (conflict === "tail-time") events[1]!.occurredAt = "other-time";
    if (conflict === "wrong-action") events[0]!.type = "thread.unsettled";
    if (conflict === "wrong-thread") events[0]!.aggregateId = ThreadId.make("other-thread");
    if (conflict === "duplicate-settlement") events[1]!.type = "thread.settled";
    if (conflict === "overflow") while (events.length < 257) events.push({ ...events[1]! });
    if (conflict === "zero-events") events.length = 0;
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(captureNativeProviderSettlementProof({ receipt: proofReceipt, events })),
      ),
    );
    assert.deepEqual(yield* sql`SELECT proof_json FROM jones_workstreams_settlement_proofs`, [
      { proof_json: null },
    ]);
  }).pipe(Effect.provide(memory)),
);
