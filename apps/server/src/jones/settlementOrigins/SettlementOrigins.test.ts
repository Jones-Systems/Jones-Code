import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import { pruneEventLogLikeUpstream } from "../persistence/prunedEventLog.testkit.ts";
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

it.effect(
  "binds project items to the exact self-settlement command and ignores cancelled or foreign associations",
  () =>
    Effect.gen(function* () {
      const service = yield* SettlementOrigins.SettlementOrigins;
      const sink = yield* EventSink.EventSinkV2;
      const sql = yield* SqlClient.SqlClient;
      for (const kind of ["matching", "cancelled", "foreign-thread", "wrong-command"] as const) {
        const thread = yield* createThread(`project-items-${kind}`);
        const commandId = CommandId.make(`project-items-${kind}-request`);
        const intent = {
          commandId,
          runId: RunId.make("run"),
          mcpCredentialId: "credential",
          providerSessionId: ProviderSessionId.make("session"),
          providerInstanceId: ProviderInstanceId.make("codex"),
        };
        const projectRemaining = ["Remaining project work", "A second item"];
        yield* sink.commitCommand({
          commandId,
          threadId: thread.id,
          commandType: "thread.metadata.update",
          acceptedAt: settledAt,
          effects: [],
          events: [
            {
              id: EventId.make(`${commandId}-intent`),
              type: "thread.metadata-updated",
              threadId: thread.id,
              occurredAt: settledAt,
              payload: { ...thread, selfSettlement: intent },
            },
          ],
          selfSettlementProjectRemaining: {
            commandId,
            threadId: thread.id,
            items: projectRemaining,
          },
        });
        if (kind === "foreign-thread")
          yield* sql`UPDATE jones_self_settlement_project_items SET thread_id = 'another-thread' WHERE command_id = ${commandId}`;
        yield* writeSettlement(
          { ...thread, selfSettlement: kind === "cancelled" ? null : intent },
          CommandId.make(kind === "wrong-command" ? "unrelated-settlement" : `${commandId}:settle`),
        );
        const result = (yield* service.resolve({ items: [{ threadId: thread.id, settledAtMs }] }))
          .items[0]!;
        assert.equal(result.origin, "other");
        assert.deepEqual(
          result.projectRemaining,
          kind === "matching" ? projectRemaining : undefined,
        );
      }
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "missing project-item storage rejects supplied data but leaves old acceptance and lookup usable",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const sink = yield* EventSink.EventSinkV2;
      const service = yield* SettlementOrigins.SettlementOrigins;
      const thread = yield* createThread("missing-project-items");
      yield* sql`DROP TABLE jones_self_settlement_project_items`;
      const commandId = CommandId.make("missing-project-items-request");
      const input = {
        commandId,
        threadId: thread.id,
        commandType: "thread.metadata.update",
        acceptedAt: settledAt,
        effects: [],
        events: [
          {
            id: EventId.make("missing-project-items-intent"),
            type: "thread.metadata-updated" as const,
            threadId: thread.id,
            occurredAt: settledAt,
            payload: thread,
          },
        ],
      };
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sink.commitCommand({
              ...input,
              selfSettlementProjectRemaining: {
                commandId,
                threadId: thread.id,
                items: ["Pending work"],
              },
            }),
          ),
        ),
      );
      assert.lengthOf(
        yield* sql`SELECT 1 FROM orchestration_command_receipts WHERE command_id = ${commandId}`,
        0,
      );
      assert.lengthOf(
        yield* sql`SELECT 1 FROM orchestration_events WHERE command_id = ${commandId}`,
        0,
      );
      yield* sink.commitCommand(input);
      yield* writeSettlement(thread, CommandId.make("missing-project-items-settled"));
      assert.deepEqual(
        (yield* service.resolve({ items: [{ threadId: thread.id, settledAtMs }] })).items,
        [{ threadId: thread.id, settledAtMs, origin: "other" }],
      );
    }).pipe(Effect.provide(testLayer)),
);

const eventHead = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{
    readonly head: number;
  }>`SELECT MAX(sequence) AS head FROM orchestration_events`;
  return rows[0]!.head;
});

it.effect(
  "a settlement event that is still its stream's head survives upstream pruning with its origin",
  () =>
    Effect.gen(function* () {
      const service = yield* SettlementOrigins.SettlementOrigins;
      const thread = yield* createThread("pruned-head");
      const commandId = CommandId.make("manual:pruned-head");
      yield* withClientSettlementOrigin(
        { type: "thread.settle", commandId, threadId: thread.id },
        writeSettlement(thread, commandId),
      );
      assert.deepEqual(yield* pruneEventLogLikeUpstream(yield* eventHead), []);
      assert.equal(
        (yield* service.resolve({ items: [{ threadId: thread.id, settledAtMs }] })).items[0]
          ?.origin,
        "client",
      );
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "a settlement event pruned by upstream reports unknown, and only the retained head keeps an origin",
  () =>
    Effect.gen(function* () {
      const service = yield* SettlementOrigins.SettlementOrigins;
      const thread = yield* createThread("pruned-older");
      const older = CommandId.make("manual:pruned-older");
      const newer = CommandId.make("manual:pruned-newer");
      const newerAt = DateTime.makeUnsafe(settledAtMs + 60_000);
      yield* withClientSettlementOrigin(
        { type: "thread.settle", commandId: older, threadId: thread.id },
        writeSettlement(thread, older),
      );
      yield* withClientSettlementOrigin(
        { type: "thread.settle", commandId: newer, threadId: thread.id },
        writeSettlement(thread, newer, newerAt),
      );
      assert.lengthOf(yield* pruneEventLogLikeUpstream(yield* eventHead), 1);
      const result = yield* service.resolve({
        items: [
          { threadId: thread.id, settledAtMs },
          { threadId: thread.id, settledAtMs: settledAtMs + 60_000 },
        ],
      });
      // The older marker is still stored, but the event naming that settlement is gone: never a guess.
      assert.deepEqual(
        result.items.map((item) => item.origin),
        ["unknown", "client"],
      );
    }).pipe(Effect.provide(testLayer)),
);

it.effect("a settlement older than the marker retention window reports unknown, not other", () =>
  Effect.gen(function* () {
    const service = yield* SettlementOrigins.SettlementOrigins;
    const retention = yield* SettlementOrigins.makeRetention;
    const sql = yield* SqlClient.SqlClient;
    const thread = yield* createThread("trust-window");
    yield* TestClock.setTime(settledAtMs);
    const clientCommand = CommandId.make("manual:trust-window-client");
    yield* withClientSettlementOrigin(
      { type: "thread.settle", commandId: clientCommand, threadId: thread.id },
      writeSettlement(thread, clientCommand),
    );
    const agentThread = yield* createThread("trust-window-agent");
    yield* writeSettlement(agentThread, CommandId.make("agent:trust-window"));
    const items = [
      { threadId: thread.id, settledAtMs },
      { threadId: agentThread.id, settledAtMs },
    ];
    const origins = () =>
      service.resolve({ items }).pipe(Effect.map(({ items: rows }) => rows.map((r) => r.origin)));
    assert.deepEqual(yield* origins(), ["client", "other"]);
    yield* TestClock.setTime(settledAtMs + Duration.toMillis(Duration.days(28)));
    assert.deepEqual(yield* origins(), ["client", "other"]);
    // Past the window a missing marker could mean "pruned client settle": say unknown.
    yield* TestClock.setTime(settledAtMs + Duration.toMillis(Duration.days(31)));
    assert.deepEqual(yield* origins(), ["client", "unknown"]);
    // Pruning the client marker degrades it the same way instead of relabeling it other.
    assert.deepEqual(yield* retention.pruneExpired, { clientCommands: 1, projectItems: 0 });
    assert.deepEqual(yield* origins(), ["unknown", "unknown"]);
    assert.lengthOf(yield* sql`SELECT command_id FROM jones_client_settlement_commands`, 0);
  }).pipe(Effect.provide(testLayer)),
);

it.effect("retention deletes only origin rows older than the window, in both tables", () =>
  Effect.gen(function* () {
    const retention = yield* SettlementOrigins.makeRetention;
    const sql = yield* SqlClient.SqlClient;
    yield* TestClock.setTime(Date.parse("2026-12-01T00:00:00.000Z"));
    // The window is 30 days, so the cutoff is exactly 2026-11-01T00:00:00.000Z and is exclusive.
    const rows = [
      ["old", "2026-10-31T23:59:59.999Z"],
      ["edge", "2026-11-01T00:00:00.000Z"],
      ["fresh", "2026-11-30T00:00:00.000Z"],
    ] as const;
    for (const [id, recordedAt] of rows) {
      yield* sql`INSERT INTO jones_client_settlement_commands(command_id, thread_id, recorded_at)
        VALUES (${id}, 'thread', ${recordedAt})`;
      yield* sql`INSERT INTO jones_self_settlement_project_items(command_id, thread_id, items_json, recorded_at)
        VALUES (${id}, 'thread', '[]', ${recordedAt})`;
    }
    assert.deepEqual(yield* retention.pruneExpired, { clientCommands: 1, projectItems: 1 });
    const remaining = (
      table: "jones_client_settlement_commands" | "jones_self_settlement_project_items",
    ) =>
      sql<{
        readonly command_id: string;
      }>`SELECT command_id FROM ${sql(table)} ORDER BY recorded_at`.pipe(
        Effect.map((found) => found.map((row) => row.command_id)),
      );
    assert.deepEqual(yield* remaining("jones_client_settlement_commands"), ["edge", "fresh"]);
    assert.deepEqual(yield* remaining("jones_self_settlement_project_items"), ["edge", "fresh"]);
    assert.deepEqual(yield* retention.pruneExpired, { clientCommands: 0, projectItems: 0 });
  }).pipe(Effect.provide(testLayer)),
);

it.effect("retention works through many batches", () =>
  Effect.gen(function* () {
    const retention = yield* SettlementOrigins.makeRetention;
    const sql = yield* SqlClient.SqlClient;
    yield* TestClock.setTime(Date.parse("2026-12-01T00:00:00.000Z"));
    yield* sql`WITH RECURSIVE numbers(n) AS (
      SELECT 1 UNION ALL SELECT n + 1 FROM numbers WHERE n < 450
    ) INSERT INTO jones_client_settlement_commands(command_id, thread_id, recorded_at)
      SELECT 'stale:' || n, 'thread', '2026-01-01T00:00:00.000Z' FROM numbers`;
    assert.deepEqual(yield* retention.pruneExpired, { clientCommands: 450, projectItems: 0 });
    assert.lengthOf(yield* sql`SELECT command_id FROM jones_client_settlement_commands`, 0);
  }).pipe(Effect.provide(testLayer)),
);

it.effect("retention tolerates origin tables that are not installed", () =>
  Effect.gen(function* () {
    const retention = yield* SettlementOrigins.makeRetention;
    const sql = yield* SqlClient.SqlClient;
    yield* sql`DROP TABLE jones_client_settlement_commands`;
    yield* sql`DROP TABLE jones_self_settlement_project_items`;
    assert.deepEqual(yield* retention.pruneExpired, { clientCommands: 0, projectItems: 0 });
  }).pipe(Effect.provide(testLayer)),
);

it.effect("a failing retention pass is a typed error that never reaches dispatch", () =>
  Effect.gen(function* () {
    const retention = yield* SettlementOrigins.makeRetention;
    const sql = yield* SqlClient.SqlClient;
    yield* TestClock.setTime(Date.parse("2026-12-01T00:00:00.000Z"));
    yield* sql`INSERT INTO jones_client_settlement_commands(command_id, thread_id, recorded_at)
      VALUES ('stale', 'thread', '2026-01-01T00:00:00.000Z')`;
    yield* sql`CREATE TRIGGER jones_test_reject_prune BEFORE DELETE ON jones_client_settlement_commands BEGIN SELECT RAISE(ABORT, 'synthetic prune failure'); END`;
    const failure = yield* retention.pruneExpired.pipe(Effect.flip);
    assert.equal(failure._tag, "SettlementOriginsRetentionError");
    assert.lengthOf(yield* sql`SELECT command_id FROM jones_client_settlement_commands`, 1);
  }).pipe(Effect.provide(testLayer)),
);

it.effect("the retention layer sweeps when built and again every six hours", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* TestClock.setTime(Date.parse("2026-12-01T00:00:00.000Z"));
    const insertStale = (id: string) =>
      sql`INSERT INTO jones_client_settlement_commands(command_id, thread_id, recorded_at)
        VALUES (${id}, 'thread', '2026-01-01T00:00:00.000Z')`;
    const remaining = sql<{
      readonly total: number;
    }>`SELECT COUNT(*) AS total FROM jones_client_settlement_commands`.pipe(
      Effect.map((found) => found[0]!.total),
    );
    const settle = Effect.forEach(Array.from({ length: 50 }), () => Effect.yieldNow);
    yield* insertStale("stale-at-start");
    yield* Layer.build(SettlementOrigins.retentionLayer);
    yield* TestClock.adjust(Duration.millis(1));
    yield* settle;
    assert.equal(yield* remaining, 0);
    yield* insertStale("stale-later");
    yield* TestClock.adjust(Duration.hours(5));
    yield* settle;
    assert.equal(yield* remaining, 1, "the next sweep is not due yet");
    yield* TestClock.adjust(Duration.hours(1));
    yield* settle;
    assert.equal(yield* remaining, 0);
  }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

// recordClientSettle runs in the dispatch path, so these cases replace only its transaction.
const withTransactionOutcome = <E>(outcome: Effect.Effect<never, E>) =>
  Effect.gen(function* () {
    const real = yield* SqlClient.SqlClient;
    const sql = new Proxy(real, {
      get: (target, property, receiver) =>
        property === "withTransaction" ? () => outcome : Reflect.get(target, property, receiver),
    });
    return yield* SettlementOrigins.SettlementOrigins.pipe(
      Effect.provide(
        SettlementOrigins.layer.pipe(Layer.provide(Layer.succeed(SqlClient.SqlClient, sql))),
      ),
    );
  });

it.effect("an interrupted origin recording is interrupted, not absorbed as success", () =>
  Effect.gen(function* () {
    const service = yield* withTransactionOutcome(Effect.interrupt);
    const exit = yield* Effect.exit(
      service.recordClientSettle({
        commandId: CommandId.make("client:interrupted"),
        threadId: ThreadId.make("thread:interrupted"),
      }),
    );
    assert.isTrue(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause));
  }).pipe(Effect.provide(database)),
);

it.effect("a failed or crashed origin recording is still absorbed so dispatch continues", () =>
  Effect.gen(function* () {
    for (const outcome of [Effect.die("synthetic defect"), Effect.fail("synthetic failure")]) {
      const service = yield* withTransactionOutcome(outcome);
      const exit = yield* Effect.exit(
        service.recordClientSettle({
          commandId: CommandId.make("client:absorbed"),
          threadId: ThreadId.make("thread:absorbed"),
        }),
      );
      assert.isTrue(Exit.isSuccess(exit));
    }
  }).pipe(Effect.provide(database)),
);
