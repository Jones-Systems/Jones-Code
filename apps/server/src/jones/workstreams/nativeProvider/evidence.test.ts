import { assert, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as Evidence from "./evidence.ts";

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
      receipt: Option.some({
        commandId: "command-synthetic",
        aggregateKind: "thread",
        aggregateId: threadId,
        commandType: "thread.settle",
        acceptedAt: now,
        resultSequence: 1,
        status: "accepted",
      }),
      events: [{
        eventId: "event-synthetic",
        commandId: "command-synthetic",
        aggregateKind: "thread",
        aggregateId: threadId,
        sequence: 1,
        type: "thread.settled",
        occurredAt: now,
        applicationEventVersion: 2,
      }],
    });
    assert.deepEqual(yield* reader.readSnapshotByCommandId("absent", threadId), {
      receipt: Option.none(), events: [],
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
    assert.deepEqual(snapshot.events.map((event) => event.sequence),
      Array.from({ length: 257 }, (_, index) => index + 1));
    assert.strictEqual(Option.isNone(snapshot.receipt), true);
    assert.deepEqual(yield* reader.readSnapshotByCommandId("command' OR 1=1 --", threadId), {
      receipt: Option.none(), events: [],
    });
    assert.strictEqual((yield* reader.readSnapshotByCommandId("foreign-command", threadId)).events.length, 1);
  }).pipe(Effect.provide(memory)),
);

it.effect("retains conflicting domains, old versions, and zero-event receipts for consumer evaluation", () =>
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
    assert.deepEqual(snapshot.events.map((event) => [event.aggregateKind, event.aggregateId, event.applicationEventVersion]),
      [["project", "foreign-project", 1], ["thread", "foreign-thread", 2]]);
    for (const [commandId, status] of [["zero-events", "accepted"], ["rejected", "rejected"]] as const) {
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
    const absent = yield* Effect.flip(reader.readSnapshotByCommandId("command-synthetic", threadId));
    assert.strictEqual(absent instanceof Evidence.NativeProviderEvidenceReadError, true);
    if (!(absent instanceof Evidence.NativeProviderEvidenceReadError)) throw new Error("Expected typed failure.");
    assert.strictEqual(absent.reason, "reader_unavailable");
    assert.strictEqual(Object.hasOwn(absent, "cause"), false);
    const sql = yield* initialize;
    yield* sql`INSERT INTO orchestration_events VALUES
      (1, 'event', 'thread.settled', 'unsupported-aggregate', ${threadId}, ${now}, 'command-synthetic', 2)`;
    const invalid = yield* Effect.flip(reader.readSnapshotByCommandId("command-synthetic", threadId));
    if (!(invalid instanceof Evidence.NativeProviderEvidenceReadError)) throw new Error("Expected typed failure.");
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
    const invalid = yield* Effect.flip(reader.readSnapshotByCommandId("command-synthetic", threadId));
    if (!(invalid instanceof Evidence.NativeProviderEvidenceReadError)) throw new Error("Expected typed failure.");
    assert.strictEqual(invalid.reason, "invalid_snapshot");
  }).pipe(Effect.provide(memory)),
);
