import { assert, it } from "@effect/vitest";
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import eventsMigration from "../Migrations/001_OrchestrationEvents.ts";
import receiptsMigration from "../Migrations/002_OrchestrationCommandReceipts.ts";
import * as NativeCommandEventMetadata from "../Services/NativeCommandEventMetadata.ts";
import * as MetadataLayer from "./NativeCommandEventMetadata.ts";

const memory = MetadataLayer.layer.pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);
const now = "2026-01-01T00:00:00.000Z";
const initialize = Effect.gen(function* () {
  yield* eventsMigration;
  yield* receiptsMigration;
  const sql = yield* SqlClient.SqlClient;
  yield* sql`ALTER TABLE orchestration_command_receipts ADD COLUMN command_type TEXT NOT NULL DEFAULT ''`;
  yield* sql`ALTER TABLE orchestration_events ADD COLUMN application_event_version INTEGER NOT NULL DEFAULT 1`;
});

it.effect("queries only the eight event and seven receipt scalar columns", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    // These tables deliberately omit every private column: selecting one is a SQL failure.
    yield* sql`CREATE TABLE orchestration_events (
      sequence INTEGER, event_id TEXT, event_type TEXT, aggregate_kind TEXT,
      stream_id TEXT, occurred_at TEXT, command_id TEXT, application_event_version INTEGER
    )`;
    yield* sql`CREATE TABLE orchestration_command_receipts (
      command_id TEXT, aggregate_kind TEXT, aggregate_id TEXT, command_type TEXT,
      accepted_at TEXT, result_sequence INTEGER, status TEXT
    )`;
    yield* sql`INSERT INTO orchestration_events VALUES
      (1, 'event', 'thread.settled', 'thread', 'thread', ${now}, 'command', 2)`;
    yield* sql`INSERT INTO orchestration_command_receipts VALUES
      ('command', 'thread', 'thread', 'thread.settle', ${now}, 1, 'accepted')`;
    const service = yield* NativeCommandEventMetadata.NativeCommandEventMetadata;
    const row = {
      sequence: 1,
      eventId: "event",
      type: "thread.settled",
      aggregateKind: "thread",
      aggregateId: "thread",
      occurredAt: now,
      commandId: "command",
      applicationEventVersion: 2,
    } satisfies NativeCommandEventMetadata.NativeCommandEventMetadataRow;
    assert.deepEqual(yield* service.readMetadataByCommandId("command"), [row]);
    assert.deepEqual(yield* service.readSnapshotByCommandId("command"), {
      receipt: Option.some<NativeCommandEventMetadata.NativeCommandReceipt>({
        commandId: "command",
        aggregateKind: "thread",
        aggregateId: "thread",
        commandType: "thread.settle",
        acceptedAt: now,
        resultSequence: 1,
        status: "accepted",
      }),
      events: [row],
    });
  }).pipe(Effect.provide(memory)),
);

it.effect("command lookup is sorted and retains the 257th overflow sentinel", () =>
  Effect.gen(function* () {
    yield* initialize;
    const sql = yield* SqlClient.SqlClient;
    yield* sql`WITH RECURSIVE numbers(n) AS (
      SELECT 258 UNION ALL SELECT n - 1 FROM numbers WHERE n > 1
    ) INSERT INTO orchestration_events
      (sequence, event_id, aggregate_kind, stream_id, stream_version, event_type,
        occurred_at, command_id, actor_kind, payload_json, metadata_json)
      SELECT n, 'event-' || n, 'thread', 'thread', n, 'thread.settled', ${now},
        'command', 'private-actor', 'not-json-private-payload', 'not-json-private-metadata'
      FROM numbers ORDER BY n DESC`;
    yield* sql`INSERT INTO orchestration_events
      (sequence, event_id, aggregate_kind, stream_id, stream_version, event_type,
        occurred_at, command_id, actor_kind, payload_json, metadata_json)
      VALUES (259, 'foreign-event', 'thread', 'thread', 259, 'thread.settled',
        ${now}, 'foreign-command', 'private-actor', 'private-payload', 'private-metadata')`;
    const service = yield* NativeCommandEventMetadata.NativeCommandEventMetadata;
    const rows = yield* service.readMetadataByCommandId("command");
    assert.strictEqual(rows.length, 257);
    assert.deepEqual(
      rows.map((row) => row.sequence),
      Array.from({ length: 257 }, (_, i) => i + 1),
    );
    assert.strictEqual(
      rows.every((row) => row.commandId === "command"),
      true,
    );
    assert.strictEqual((yield* service.readMetadataByCommandId("foreign-command")).length, 1);
    assert.deepEqual(yield* service.readMetadataByCommandId("command' OR 1=1 --"), []);
    assert.deepEqual(yield* service.readSnapshotByCommandId("absent"), {
      receipt: Option.none(),
      events: [],
    });
  }).pipe(Effect.provide(memory)),
);

it.effect(
  "foreign aggregates and zero-event receipts remain visible for fail-closed association",
  () =>
    Effect.gen(function* () {
      yield* initialize;
      const sql = yield* SqlClient.SqlClient;
      yield* sql`INSERT INTO orchestration_events
      (event_id, aggregate_kind, stream_id, stream_version, event_type,
        occurred_at, command_id, actor_kind, payload_json, metadata_json)
      VALUES ('project-event', 'project', 'foreign-project', 1, 'project.created',
        ${now}, 'command', 'private-actor', '{}', '{}'),
        ('thread-event', 'thread', 'foreign-thread', 1, 'thread.settled',
        ${now}, 'command', 'private-actor', '{}', '{}')`;
      yield* sql`INSERT INTO orchestration_command_receipts
      (command_id, aggregate_kind, aggregate_id, command_type, accepted_at, result_sequence, status, error)
      VALUES ('command', 'thread', 'thread', 'thread.settle', ${now}, 2, 'accepted', NULL),
        ('zero-events', 'thread', 'thread', 'thread.settle', ${now}, 2, 'accepted', NULL),
        ('rejected', 'thread', 'thread', 'thread.settle', ${now}, 2, 'rejected', 'private-error-text')`;
      const service = yield* NativeCommandEventMetadata.NativeCommandEventMetadata;
      const snapshot = yield* service.readSnapshotByCommandId("command");
      assert.deepEqual(
        snapshot.events.map((row) => [row.aggregateKind, row.aggregateId]),
        [
          ["project", "foreign-project"],
          ["thread", "foreign-thread"],
        ],
      );
      assert.strictEqual(Option.getOrThrow(snapshot.receipt).aggregateId, "thread");
      for (const [commandId, status] of [
        ["zero-events", "accepted"],
        ["rejected", "rejected"],
      ]) {
        const observed = yield* service.readSnapshotByCommandId(commandId!);
        assert.deepEqual(observed.events, []);
        const receipt = Option.getOrThrow(observed.receipt);
        assert.strictEqual(receipt.status, status);
        assert.strictEqual(Object.hasOwn(receipt, "error"), false);
      }
    }).pipe(Effect.provide(memory)),
);

it.effect("missing schema and invalid scalar rows return content-free typed failures", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const service = yield* NativeCommandEventMetadata.NativeCommandEventMetadata;
    const missing = yield* service.readSnapshotByCommandId("command").pipe(Effect.result);
    assert.strictEqual(missing._tag, "Failure");
    if (missing._tag === "Failure") {
      assert.strictEqual(missing.failure._tag, "NativeCommandEventMetadataError");
      assert.strictEqual(missing.failure.operation, "readSnapshotByCommandId");
      assert.strictEqual(missing.failure.message, "Native command evidence could not be read.");
    }
    yield* initialize;
    yield* sql`INSERT INTO orchestration_events
      (event_id, aggregate_kind, stream_id, stream_version, event_type,
        occurred_at, command_id, actor_kind, payload_json, metadata_json)
      VALUES ('event', 'unsupported-aggregate', 'thread', 1, 'thread.settled',
        ${now}, 'command', 'actor', '{}', '{}')`;
    const invalid = yield* service.readMetadataByCommandId("command").pipe(Effect.result);
    assert.strictEqual(invalid._tag, "Failure");
    if (invalid._tag === "Failure") {
      assert.strictEqual(invalid.failure.operation, "readMetadataByCommandId");
      assert.strictEqual(Object.hasOwn(invalid.failure, "cause"), false);
    }
  }).pipe(Effect.provide(memory)),
);

it.effect(
  "receipt and metadata share the read transaction snapshot across a concurrent commit",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "native-command-snapshot-" });
      const filename = `${directory}/evidence.sqlite`;
      const writerContext = yield* Layer.build(NodeSqliteClient.layer({ filename }));
      const writer = Context.get(writerContext, SqlClient.SqlClient);
      yield* initialize.pipe(Effect.provide(writerContext));
      yield* writer`PRAGMA journal_mode = WAL`;
      yield* writer`INSERT INTO orchestration_command_receipts
      (command_id, aggregate_kind, aggregate_id, command_type, accepted_at, result_sequence, status)
      VALUES ('command', 'thread', 'thread', 'thread.settle', ${now}, 0, 'accepted')`;
      const readerContext = yield* Layer.build(
        MetadataLayer.layer.pipe(
          Layer.provideMerge(NodeSqliteClient.layer({ filename, readonly: true })),
        ),
      );
      const reader = Context.get(readerContext, SqlClient.SqlClient);
      const service = Context.get(
        readerContext,
        NativeCommandEventMetadata.NativeCommandEventMetadata,
      );
      yield* reader.withTransaction(
        Effect.gen(function* () {
          yield* reader`SELECT command_id FROM orchestration_command_receipts`;
          yield* writer.withTransaction(
            Effect.gen(function* () {
              yield* writer`INSERT INTO orchestration_events
          (event_id, aggregate_kind, stream_id, stream_version, event_type,
            occurred_at, command_id, actor_kind, payload_json, metadata_json)
          VALUES ('event', 'thread', 'thread', 1, 'thread.settled', ${now}, 'command', 'actor', '{}', '{}')`;
              yield* writer`UPDATE orchestration_command_receipts SET result_sequence = 1 WHERE command_id = 'command'`;
            }),
          );
          const snapshot = yield* service.readSnapshotByCommandId("command");
          assert.strictEqual(Option.getOrThrow(snapshot.receipt).resultSequence, 0);
          assert.deepEqual(snapshot.events, []);
        }),
      );
      const after = yield* service.readSnapshotByCommandId("command");
      assert.strictEqual(Option.getOrThrow(after.receipt).resultSequence, 1);
      assert.strictEqual(after.events[0]?.sequence, 1);
      assert.strictEqual(after.events[0]?.occurredAt, Option.getOrThrow(after.receipt).acceptedAt);
    }).pipe(Effect.scoped, Effect.provide(NodeFileSystem.layer)),
);

it.effect("same-command version-1 evidence is returned and distinguishable from version 2", () =>
  Effect.gen(function* () {
    yield* initialize;
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO orchestration_events
      (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
        command_id, actor_kind, payload_json, metadata_json, application_event_version)
      VALUES ('legacy-event', 'thread', 'thread', 1, 'thread.settled', ${now},
        'command', 'private-actor', 'not-json-private-payload', 'not-json-private-metadata', 1),
        ('v2-event', 'thread', 'thread', 2, 'thread.settled', ${now},
        'command', 'private-actor', 'not-json-private-payload', 'not-json-private-metadata', 2)`;
    yield* sql`INSERT INTO orchestration_command_receipts
      (command_id, aggregate_kind, aggregate_id, command_type, accepted_at, result_sequence, status)
      VALUES ('command', 'thread', 'thread', 'thread.settle', ${now}, 2, 'accepted')`;
    const service = yield* NativeCommandEventMetadata.NativeCommandEventMetadata;
    const rows = yield* service.readMetadataByCommandId("command");
    assert.deepEqual(
      rows.map((row) => row.applicationEventVersion),
      [1, 2],
    );
    assert.deepEqual(
      rows.filter((row) => row.applicationEventVersion !== 2).map((row) => row.eventId),
      ["legacy-event"],
    );
    assert.deepEqual((yield* service.readSnapshotByCommandId("command")).events, rows);
  }).pipe(Effect.provide(memory)),
);
