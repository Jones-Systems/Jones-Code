import { assert, it } from "@effect/vitest";
import {
  EventId,
  NodeId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as Sqlite from "../../persistence/Sqlite.ts";
import {
  changesPlannedControl,
  type PlannedThreadSnapshot,
} from "../updates/plannedContinuityPolicy.ts";
import * as Fixture from "./OrphanedRunRecords.testkit.ts";
import * as OrphanedRunRecords from "./OrphanedRunRecords.ts";

const stores = Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provideMerge(Sqlite.layerMemory),
);
const eventLayer = Layer.merge(EventSink.layer, IdAllocator.layer).pipe(Layer.provideMerge(stores));
const testLayer = OrphanedRunRecords.layer.pipe(Layer.provideMerge(eventLayer));

// Read the projection rows directly: deleted threads have no readable thread projection.
const statusOf = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<{ id: string; status: string }>`
      SELECT run_id AS id, status FROM orchestration_v2_projection_runs WHERE thread_id = ${threadId}
      UNION ALL SELECT node_id, status FROM orchestration_v2_projection_nodes WHERE thread_id = ${threadId}
      UNION ALL SELECT provider_turn_id, status FROM orchestration_v2_projection_provider_turns
        WHERE thread_id = ${threadId}
      UNION ALL SELECT turn_item_id, status FROM orchestration_v2_projection_turn_items
        WHERE thread_id = ${threadId}
      UNION ALL SELECT subagent_id, status FROM orchestration_v2_projection_subagents
        WHERE thread_id = ${threadId}`;
    return new Map(rows.map((row) => [row.id, row.status]));
  });

/** A thread whose single run has `status`, with a running provider turn, tool call and reasoning item under it. */
const singleRunThread = (
  prefix: string,
  status: OrchestrationV2Run["status"],
  now: DateTime.Utc,
) => {
  const threadId = ThreadId.make(`${prefix}:thread`);
  const providerThreadId = ProviderThreadId.make(`${prefix}:provider-thread`);
  const runId = RunId.make(`${prefix}:run`);
  const attemptId = RunAttemptId.make(`${prefix}:attempt`);
  const rootNodeId = NodeId.make(`${prefix}:node:root`);
  const providerTurnId = ProviderTurnId.make(`${prefix}:provider-turn`);
  const toolNodeId = NodeId.make(`${prefix}:node:tool`);
  const itemId = TurnItemId.make(`${prefix}:item:reasoning`);
  const ended = ["completed", "interrupted", "failed", "cancelled"].includes(status);
  const common = { threadId, providerThreadId, now };
  const events: Array<OrchestrationV2DomainEvent> = [
    Fixture.threadCreated(threadId, now, providerThreadId),
    Fixture.providerThreadUpdated(threadId, providerThreadId, now),
    Fixture.runUpdated(
      Fixture.makeRun({
        ...common,
        id: runId,
        ordinal: 1,
        status,
        rootNodeId,
        activeAttemptId: attemptId,
      }),
    ),
    Fixture.attemptCreated({
      ...common,
      runId,
      id: attemptId,
      rootNodeId,
      providerTurnId,
      status: ended ? "completed" : "running",
    }),
    Fixture.nodeUpdated(
      Fixture.makeNode({
        ...common,
        id: rootNodeId,
        runId,
        rootNodeId,
        kind: "root_turn",
        status: ended ? "completed" : "running",
        providerTurnId,
      }),
    ),
    Fixture.providerTurnUpdated(
      threadId,
      Fixture.makeProviderTurn({
        ...common,
        id: providerTurnId,
        nodeId: rootNodeId,
        runAttemptId: attemptId,
        ordinal: 1,
        status: "running",
      }),
      runId,
    ),
    Fixture.nodeUpdated(
      Fixture.makeNode({
        ...common,
        id: toolNodeId,
        runId,
        rootNodeId,
        kind: "tool_call",
        status: "running",
        providerTurnId,
      }),
    ),
    Fixture.turnItemUpdated(
      Fixture.reasoningItem({
        ...common,
        id: itemId,
        runId,
        nodeId: null,
        providerTurnId,
        ordinal: 1,
        status: "running",
      }),
    ),
  ];
  return {
    events,
    threadId,
    runId,
    rootNodeId,
    providerTurnId,
    toolNodeId,
    itemId,
    providerThreadId,
    common,
  };
};

it.layer(testLayer)("OrphanedRunRecords", (it) => {
  it.effect.each(["preparing", "starting", "running", "waiting", "queued"] as const)(
    "leaves every record under a %s run open",
    (status) =>
      Effect.gen(function* () {
        const sink = yield* EventSink.EventSinkV2;
        const fixture = singleRunThread(`live-${status}`, status, yield* DateTime.now);
        yield* sink.write({ events: fixture.events });
        const before = yield* statusOf(fixture.threadId);

        const summary = yield* (yield* OrphanedRunRecords.OrphanedRunRecords).closeAtStartup;

        assert.deepStrictEqual(yield* statusOf(fixture.threadId), before);
        assert.strictEqual(summary.threads, 0);
      }),
  );

  it.effect(
    "closes an ended run's turn, node and item once, with the exempt recovery receipt",
    () =>
      Effect.gen(function* () {
        const sink = yield* EventSink.EventSinkV2;
        const events = yield* EventStore.EventStoreV2;
        const sql = yield* SqlClient.SqlClient;
        const service = yield* OrphanedRunRecords.OrphanedRunRecords;
        const fixture = singleRunThread("ended", "failed", yield* DateTime.now);
        yield* sink.write({ events: fixture.events });
        yield* TestClock.adjust("5 minutes");
        const closedAt = yield* DateTime.now;

        const summary = yield* service.closeAtStartup;

        const status = yield* statusOf(fixture.threadId);
        assert.strictEqual(status.get(fixture.providerTurnId), "interrupted");
        assert.strictEqual(status.get(fixture.toolNodeId), "interrupted");
        assert.strictEqual(status.get(fixture.itemId), "interrupted");
        assert.strictEqual(status.get(fixture.rootNodeId), "completed");
        assert.strictEqual(status.get(fixture.runId), "failed");
        assert.deepStrictEqual(summary, {
          threads: 1,
          providerTurns: 1,
          nodes: 1,
          turnItems: 1,
          skipped: 0,
        });

        const commandId = OrphanedRunRecords.orphanedRecordsCommandId(fixture.threadId, closedAt);
        assert.strictEqual(
          commandId,
          `command:runtime-reconcile:orphaned-records:${fixture.threadId}:${DateTime.formatIso(closedAt)}`,
        );
        const receipts = yield* sql<{
          command_id: string;
          command_type: string;
          accepted_at: string;
        }>`SELECT command_id, command_type, accepted_at FROM orchestration_command_receipts
          WHERE command_id = ${commandId}`;
        assert.strictEqual(receipts.length, 1);
        assert.strictEqual(
          receipts[0]!.command_type,
          OrphanedRunRecords.ORPHANED_RECORDS_COMMAND_TYPE,
        );
        // A planned update continuation still resumes across this receipt.
        assert.strictEqual(
          changesPlannedControl(
            receipts[0]!,
            { threadId: fixture.threadId } as PlannedThreadSnapshot,
            new Set(),
          ),
          false,
        );

        const sequence = yield* events.latestSequence();
        yield* TestClock.adjust("5 minutes");
        const again = yield* service.closeAtStartup;
        assert.strictEqual(yield* events.latestSequence(), sequence);
        assert.deepStrictEqual(again, {
          threads: 0,
          providerTurns: 0,
          nodes: 0,
          turnItems: 0,
          skipped: 0,
        });
      }),
  );

  it.effect("leaves delegated tasks and background-capable items to their owners", () =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const now = yield* DateTime.now;
      const fixture = singleRunThread("owned", "completed", now);
      const { threadId, runId, rootNodeId, providerTurnId, providerThreadId, common } = fixture;
      const taskNodeId = NodeId.make("owned:node:task");
      const commandItemId = TurnItemId.make("owned:item:command");
      const instanceId = ProviderInstanceId.make("codex");
      yield* sink.write({
        events: [
          ...fixture.events,
          Fixture.nodeUpdated(
            Fixture.makeNode({
              ...common,
              id: taskNodeId,
              runId,
              rootNodeId,
              kind: "subagent",
              status: "running",
              providerTurnId,
            }),
          ),
          {
            id: EventId.make("owned:event:task"),
            type: "subagent.updated",
            threadId,
            runId,
            nodeId: taskNodeId,
            providerInstanceId: instanceId,
            occurredAt: now,
            payload: {
              id: taskNodeId,
              threadId,
              runId,
              parentNodeId: rootNodeId,
              origin: "app_owned",
              createdBy: "agent",
              driver: ProviderDriverKind.make("codex"),
              providerInstanceId: instanceId,
              providerThreadId: null,
              childThreadId: ThreadId.make("owned:child-thread"),
              nativeTaskRef: null,
              prompt: "Delegated work",
              title: null,
              model: null,
              status: "running",
              result: null,
              startedAt: now,
              completedAt: null,
              updatedAt: now,
            },
          },
          Fixture.turnItemUpdated({
            id: commandItemId,
            threadId,
            runId,
            nodeId: null,
            providerThreadId,
            providerTurnId,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: 2,
            status: "running",
            title: null,
            startedAt: now,
            completedAt: null,
            updatedAt: now,
            type: "command_execution",
            input: "sleep 1000",
          }),
        ],
      });

      yield* (yield* OrphanedRunRecords.OrphanedRunRecords).closeAtStartup;

      const status = yield* statusOf(threadId);
      assert.strictEqual(status.get(taskNodeId), "running");
      assert.strictEqual(status.get(commandItemId), "running");
      assert.strictEqual(status.get(fixture.toolNodeId), "interrupted");
      assert.strictEqual(status.get(providerTurnId), "interrupted");
    }),
  );

  it.effect("skips deleted threads", () =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const now = yield* DateTime.now;
      const fixture = singleRunThread("deleted", "completed", now);
      const [created, ...rest] = fixture.events;
      if (created?.type !== "thread.created") throw new Error("fixture starts with its thread");
      yield* sink.write({
        events: [{ ...created, payload: { ...created.payload, deletedAt: now } }, ...rest],
      });
      const before = yield* statusOf(fixture.threadId);

      yield* (yield* OrphanedRunRecords.OrphanedRunRecords).closeAtStartup;

      assert.deepStrictEqual(yield* statusOf(fixture.threadId), before);
    }),
  );
});
