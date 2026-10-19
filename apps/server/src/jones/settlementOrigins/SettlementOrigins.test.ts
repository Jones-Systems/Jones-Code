import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as SettlementOrigins from "./SettlementOrigins.ts";
import { withClientSettlementOrigin } from "./clientIngress.ts";

const database = SqlitePersistence.layerMemory;
const stores = Layer.mergeAll(
  EventStore.layer,
  ProjectionStore.layer,
  SettlementOrigins.layer,
).pipe(Layer.provideMerge(database));
const testLayer = EventSink.layer.pipe(Layer.provideMerge(stores));
const settledAt = DateTime.makeUnsafe("2026-10-19T12:00:00.123Z");
const settledAtMs = DateTime.toEpochMillis(settledAt);

const createThread = Effect.fnUntraced(function* (name: string) {
  const sink = yield* EventSink.EventSinkV2;
  const threadId = ThreadId.make(`thread:origins:${name}`);
  const instanceId = ProviderInstanceId.make("codex");
  const thread: OrchestrationV2AppThread = {
    id: threadId,
    projectId: ProjectId.make("project:origins"),
    title: name,
    providerInstanceId: instanceId,
    modelSelection: { instanceId, model: "synthetic" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: settledAt,
    updatedAt: settledAt,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
    createdBy: "user",
    creationSource: "web",
  };
  yield* sink.write({
    events: [
      {
        id: EventId.make(`created:${name}`),
        threadId,
        occurredAt: settledAt,
        type: "thread.created",
        payload: thread,
      },
    ],
  });
  return thread;
});

// Persist the same event outcomes as settlement/automatic settlement and no-op
// retries. This fixture exercises real serialization, projections and SQLite.
const writeSettlement = Effect.fnUntraced(function* (
  thread: OrchestrationV2AppThread,
  commandId: CommandId,
  at = settledAt,
) {
  const sink = yield* EventSink.EventSinkV2;
  return yield* sink.write({
    commandId,
    events: [
      {
        id: EventId.make(`event:${commandId}`),
        threadId: thread.id,
        occurredAt: at,
        type: "thread.settled",
        payload: { ...thread, settledOverride: "settled", settledAt: at, updatedAt: at },
      },
    ],
  });
});

it.effect(
  "classifies client, agent, MCP/self/peer/workstream and automatic event origins in request order",
  () =>
    Effect.gen(function* () {
      const service = yield* SettlementOrigins.SettlementOrigins;
      const sql = yield* SqlClient.SqlClient;
      const items = [];
      for (const name of ["client", "agent", "mcp", "self", "peer", "workstream", "automatic"]) {
        const thread = yield* createThread(name);
        const commandId = CommandId.make(`settle:${name}`);
        if (name === "client") {
          yield* withClientSettlementOrigin(
            { type: "thread.settle", commandId, threadId: thread.id },
            Effect.gen(function* () {
              const markers =
                yield* sql`SELECT command_id FROM jones_client_settlement_commands WHERE command_id = ${commandId}`;
              assert.equal(markers.length, 1, "marker must be committed before enqueue starts");
              return yield* writeSettlement(thread, commandId);
            }),
          );
        } else if (name === "automatic") {
          yield* withClientSettlementOrigin(
            { type: "thread.auto-settle", commandId, threadId: thread.id, snapshotAt: settledAt },
            writeSettlement(thread, commandId),
          );
        } else {
          yield* writeSettlement(thread, commandId);
        }
        items.push({ threadId: thread.id, settledAtMs });
      }
      const result = yield* service.resolve({ items });
      assert.deepEqual(
        result.items,
        items.map((item, index) => ({ ...item, origin: index === 0 ? "client" : "other" })),
      );
      assert.equal((yield* sql`SELECT command_id FROM jones_client_settlement_commands`).length, 1);
    }).pipe(Effect.provide(testLayer)),
);

it.effect("more than 64 later manual no-ops cannot relabel the original agent transition", () =>
  Effect.gen(function* () {
    const service = yield* SettlementOrigins.SettlementOrigins;
    const sql = yield* SqlClient.SqlClient;
    const thread = yield* createThread("duplicates");
    yield* writeSettlement(thread, CommandId.make("agent:original"));
    for (let index = 0; index < 70; index++) {
      const commandId = CommandId.make(`manual:duplicate:${index}`);
      yield* withClientSettlementOrigin(
        { type: "thread.settle", commandId, threadId: thread.id },
        writeSettlement(thread, commandId),
      );
    }
    const events = yield* sql<{ count: number }>`SELECT COUNT(*) AS count FROM orchestration_events
      WHERE stream_id = ${thread.id} AND event_type = 'thread.settled' AND application_event_version = 2`;
    assert.equal(events[0]?.count, 71);
    assert.equal(
      (yield* service.resolve({ items: [{ threadId: thread.id, settledAtMs }] })).items[0]?.origin,
      "other",
    );
  }).pipe(Effect.provide(testLayer)),
);

it.effect("walks past old settlement epochs and compares exact milliseconds", () =>
  Effect.gen(function* () {
    const service = yield* SettlementOrigins.SettlementOrigins;
    const thread = yield* createThread("epochs");
    for (let index = 0; index < 70; index++) {
      yield* writeSettlement(
        thread,
        CommandId.make(`old:${index}`),
        DateTime.makeUnsafe(settledAtMs - 1000 + index),
      );
    }
    const commandId = CommandId.make("manual:current");
    yield* withClientSettlementOrigin(
      { type: "thread.settle", commandId, threadId: thread.id },
      writeSettlement(thread, commandId),
    );
    const result = yield* service.resolve({
      items: [
        { threadId: thread.id, settledAtMs },
        { threadId: thread.id, settledAtMs: settledAtMs + 1 },
        { threadId: thread.id, settledAtMs: settledAtMs - 1000 },
      ],
    });
    assert.deepEqual(
      result.items.map((item) => item.origin),
      ["client", "unknown", "other"],
    );
  }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "failed manual enqueue and missing-thread requests do not suppress later agent events",
  () =>
    Effect.gen(function* () {
      const service = yield* SettlementOrigins.SettlementOrigins;
      const sql = yield* SqlClient.SqlClient;
      const thread = yield* createThread("failure");
      const failed = yield* Effect.exit(
        withClientSettlementOrigin(
          {
            type: "thread.settle",
            commandId: CommandId.make("manual:failed"),
            threadId: thread.id,
          },
          Effect.fail("synthetic dispatch failure"),
        ),
      );
      assert.ok(Exit.isFailure(failed));
      assert.equal(
        (yield* service.resolve({ items: [{ threadId: thread.id, settledAtMs }] })).items[0]
          ?.origin,
        "unknown",
      );
      yield* writeSettlement(thread, CommandId.make("agent:after-failure"));
      assert.equal(
        (yield* service.resolve({ items: [{ threadId: thread.id, settledAtMs }] })).items[0]
          ?.origin,
        "other",
      );
      yield* service.recordClientSettle({
        commandId: CommandId.make("manual:missing"),
        threadId: ThreadId.make("missing"),
      });
      assert.equal(
        (yield* sql`SELECT command_id FROM jones_client_settlement_commands WHERE command_id = 'manual:missing'`)
          .length,
        0,
      );
      assert.equal(
        (yield* service.resolve({ items: [{ threadId: ThreadId.make("missing"), settledAtMs }] }))
          .items[0]?.origin,
        "unknown",
      );
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "a recording SQL failure still enqueues, and replaying accepted agent IDs stays other",
  () =>
    Effect.gen(function* () {
      const service = yield* SettlementOrigins.SettlementOrigins;
      const sql = yield* SqlClient.SqlClient;
      const thread = yield* createThread("record-failure");
      yield* sql`CREATE TRIGGER jones_test_reject_origin BEFORE INSERT ON jones_client_settlement_commands BEGIN SELECT RAISE(ABORT, 'synthetic recording failure'); END`;
      const commandId = CommandId.make("client:record-fails");
      yield* withClientSettlementOrigin(
        { type: "thread.settle", commandId, threadId: thread.id },
        writeSettlement(thread, commandId),
      );
      assert.equal(
        (yield* service.resolve({ items: [{ threadId: thread.id, settledAtMs }] })).items[0]
          ?.origin,
        "other",
      );
      yield* sql`DROP TRIGGER jones_test_reject_origin`;
      yield* sql`INSERT INTO orchestration_command_receipts
      (command_id, aggregate_kind, aggregate_id, command_type, accepted_at, result_sequence, status, error)
      VALUES (${commandId}, 'thread', ${thread.id}, 'thread.settle', ${DateTime.formatIso(settledAt)}, 1, 'accepted', NULL)`;
      yield* service.recordClientSettle({ commandId, threadId: thread.id });
      assert.equal((yield* sql`SELECT command_id FROM jones_client_settlement_commands`).length, 0);
      assert.equal(
        (yield* service.resolve({ items: [{ threadId: thread.id, settledAtMs }] })).items[0]
          ?.origin,
        "other",
      );
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "migration is sparse, has no foreign keys, and missing event/thread evidence stays unknown",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const service = yield* SettlementOrigins.SettlementOrigins;
      assert.equal((yield* sql`SELECT command_id FROM jones_client_settlement_commands`).length, 0);
      assert.equal(
        (yield* sql`PRAGMA foreign_key_list(jones_client_settlement_commands)`).length,
        0,
      );
      const ledger = yield* sql<{
        name: string;
      }>`SELECT name FROM jones_sql_migrations WHERE migration_id = 111`;
      assert.equal(ledger[0]?.name, "ClientSettlementCommands");
      const thread = yield* createThread("missing-evidence");
      const commandId = CommandId.make("manual:evidence");
      yield* service.recordClientSettle({ commandId, threadId: thread.id });
      yield* writeSettlement(thread, commandId);
      yield* sql`DELETE FROM orchestration_events WHERE command_id = ${commandId}`;
      assert.equal(
        (yield* service.resolve({ items: [{ threadId: thread.id, settledAtMs }] })).items[0]
          ?.origin,
        "unknown",
      );
      yield* sql`DELETE FROM orchestration_v2_projection_threads WHERE thread_id = ${thread.id}`;
      assert.equal(
        (yield* service.resolve({ items: [{ threadId: thread.id, settledAtMs }] })).items[0]
          ?.origin,
        "unknown",
      );
      assert.equal((yield* sql`SELECT command_id FROM jones_client_settlement_commands`).length, 1);
    }).pipe(Effect.provide(testLayer)),
);

it.effect("an unavailable Jones table is conservative and cannot block client dispatch", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const service = yield* SettlementOrigins.SettlementOrigins;
    const thread = yield* createThread("missing-table");
    yield* sql`DROP TABLE jones_client_settlement_commands`;
    const commandId = CommandId.make("client:no-table");
    yield* withClientSettlementOrigin(
      { type: "thread.settle", commandId, threadId: thread.id },
      writeSettlement(thread, commandId),
    );
    assert.equal(
      (yield* service.resolve({ items: [{ threadId: thread.id, settledAtMs }] })).items[0]?.origin,
      "unknown",
    );
  }).pipe(Effect.provide(testLayer)),
);
