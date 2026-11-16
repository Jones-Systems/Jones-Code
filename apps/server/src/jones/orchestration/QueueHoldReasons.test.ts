import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import type { ProviderAdapterV2Shape } from "@t3tools/provider-core/server/ProviderAdapter";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import * as EffectWorker from "../../orchestration-v2/EffectWorker.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderRuntimeRecovery from "../../orchestration-v2/ProviderRuntimeRecoveryService.ts";
import * as ProviderReplayHarness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ServerSettings from "../../serverSettings.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
import QueueHoldsMigration from "../persistence/Migrations/147_JonesQueueHolds.ts";
import {
  isCompletedRestartContinuation,
  type QueueHold,
  type QueueHoldReason,
  restartHoldsEndedBy,
} from "./QueueHold.ts";
import { classifyQueueHoldEvents, makeQueueHoldStore } from "./QueueHoldStore.ts";

const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "gpt-5.1-codex" };
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("Queue tests inspect durable start intent without a provider"),
} as ProviderAdapterV2Shape;
const database = SqlitePersistence.layerMemory;
const testLayer = Layer.mergeAll(
  database,
  EffectOutbox.layer.pipe(Layer.provide(database)),
  ProjectionStore.layer.pipe(Layer.provide(database)),
  ProviderReplayHarness.layerWithRegistry(
    { name: "queue-hold-reasons" },
    ProviderAdapterRegistry.layerFromAdapters([adapter]),
    { databaseLayer: database, runEffectWorker: false },
  ),
);

const fixture = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const store = yield* ProjectionStore.ProjectionStoreV2;
  const sink = yield* EventSink.EventSinkV2;
  const sql = yield* SqlClient.SqlClient;
  const holds = makeQueueHoldStore(sql, makeJonesTablePresence(sql));
  const threadId = ThreadId.make("thread:queue-hold-reasons");
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make("create"),
    threadId,
    projectId: ProjectId.make("project:queue-hold-reasons"),
    title: "Queue hold reasons",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdBy: "user",
    creationSource: "web",
  });
  const send = (
    text: string,
    type: "start_immediately" | "queue_after_active",
    source: "peer" | "user" = "user",
  ) =>
    orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(`send:${text}`),
      threadId,
      messageId: MessageId.make(`message:${text}`),
      text,
      attachments: [],
      dispatchMode: { type },
      createdBy: source === "user" ? "user" : "agent",
      creationSource: source === "user" ? "web" : "mcp",
      ...(source === "peer" ? { senderThreadId: ThreadId.make("thread:peer") } : {}),
    });
  const projection = () => store.getThreadProjection(threadId);
  const run = (text: string) =>
    projection().pipe(
      Effect.map((current) =>
        current.runs.find((candidate) => candidate.userMessageId === `message:${text}`)!,
      ),
    );
  /** The recorded hold of a message's run, if any. */
  const hold = (text: string) =>
    Effect.gen(function* () {
      const current = yield* run(text);
      return (yield* holds.get([current.id])).get(current.id);
    });
  /** Holds a run in the projection and records its reason, bypassing the sink as a fixture. */
  const putHold = (held: OrchestrationV2Run, reason: QueueHoldReason, heldAt: DateTime.Utc) =>
    Effect.gen(function* () {
      yield* store.apply({
        id: EventId.make(`fixture:${held.id}:${reason}`),
        type: "run.updated",
        threadId,
        runId: held.id,
        occurredAt: heldAt,
        payload: { ...held, queueHeld: true },
      });
      yield* holds.record([{ runId: held.id, threadId, reason, heldAt }]);
    });
  const holdCount = sql<{
    readonly count: number;
  }>`SELECT COUNT(*) AS count FROM jones_queue_holds`.pipe(Effect.map((rows) => rows[0]!.count));
  /** Writes a terminal run through the sink so the queue reactor drains the thread. */
  const end = (ended: OrchestrationV2Run, status: "completed" | "failed" | "interrupted") =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      yield* sink.write({
        events: [
          {
            id: EventId.make(`terminal:${ended.id}:${status}`),
            type: "run.updated",
            threadId,
            runId: ended.id,
            occurredAt: now,
            payload: { ...ended, status, completedAt: now },
          },
        ],
      });
    });
  const updates = yield* Queue.unbounded<OrchestrationV2Run>();
  yield* sink.stream({ threadId, afterSequence: yield* sink.latestSequence() }).pipe(
    Stream.runForEach((stored) => {
      if (stored.event.type !== "run.updated") return Effect.void;
      const payload = stored.event.payload;
      return Queue.offer(updates, payload);
    }),
    Effect.forkScoped,
  );
  /** Waits for the sink to publish a run update that matches. */
  const updated = (matches: (run: OrchestrationV2Run) => boolean) =>
    Effect.gen(function* () {
      while (true) {
        const next = yield* Queue.take(updates);
        if (matches(next)) return next;
      }
    });
  /** Waits for the reactor to start this run. */
  const starts = (id: RunId) => updated((run) => run.id === id && run.status === "starting");
  return {
    orchestrator,
    store,
    sink,
    sql,
    holds,
    threadId,
    send,
    projection,
    run,
    hold,
    putHold,
    holdCount,
    end,
    updated,
    starts,
  };
});

describe("hold reasons", () => {
  it.effect("Stop records a stop hold with its time", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.send("initial", "start_immediately");
      yield* f.send("queued", "queue_after_active");
      const before = yield* DateTime.now;
      yield* f.orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make("stop"),
        threadId: f.threadId,
      });
      assert.isTrue((yield* f.run("queued")).queueHeld);
      const recorded = yield* f.hold("queued");
      assert.equal(recorded?.reason, "stop");
      assert.isTrue(
        recorded !== undefined && DateTime.isGreaterThanOrEqualTo(recorded.heldAt, before),
      );
      // The reason lives only in the Jones table; upstream run rows and events keep their shape.
      const leaked = yield* f.sql<{ readonly count: number }>`
        SELECT
          (SELECT COUNT(*) FROM orchestration_v2_projection_runs
            WHERE payload_json LIKE '%queueHeldReason%' OR payload_json LIKE '%queueHeldAt%')
          + (SELECT COUNT(*) FROM orchestration_events
            WHERE payload_json LIKE '%queueHeldReason%' OR payload_json LIKE '%queueHeldAt%')
          AS count`;
      assert.equal(leaked[0]?.count, 0);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("a message queued behind held work inherits the hold, and resume clears it", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.send("active", "start_immediately");
      yield* f.send("cohort", "queue_after_active");
      yield* f.putHold(yield* f.run("cohort"), "stop", yield* DateTime.now);
      yield* f.send("arrival", "queue_after_active", "peer");
      assert.isTrue((yield* f.run("arrival")).queueHeld);
      assert.equal((yield* f.hold("arrival"))?.reason, "inherited");
      // A new message never releases the hold it joined.
      assert.equal((yield* f.hold("cohort"))?.reason, "stop");

      yield* f.end(yield* f.run("active"), "completed");
      yield* f.orchestrator.dispatch({
        type: "queue.resume",
        commandId: CommandId.make("resume"),
        threadId: f.threadId,
      });
      // Release deletes the recorded holds in the same write.
      assert.equal(yield* f.holdCount, 0);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("a provider failure holds the queue and keeps an earlier Stop reason", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.send("active", "start_immediately");
      yield* f.send("stopped", "queue_after_active");
      yield* f.send("waiting", "queue_after_active");
      yield* f.putHold(yield* f.run("stopped"), "stop", yield* DateTime.now);
      const active = yield* f.run("active");
      const now = yield* DateTime.now;
      yield* f.sink.write({
        events: [
          {
            id: EventId.make("provider:error"),
            type: "turn-item.updated",
            threadId: f.threadId,
            runId: active.id,
            nodeId: active.rootNodeId!,
            occurredAt: now,
            payload: {
              id: TurnItemId.make("provider:error"),
              type: "error",
              threadId: f.threadId,
              runId: active.id,
              nodeId: active.rootNodeId!,
              providerThreadId: active.providerThreadId,
              providerTurnId: null,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: 2,
              status: "failed",
              title: "Provider failure",
              startedAt: now,
              completedAt: now,
              updatedAt: now,
              failure: {
                class: "provider_error",
                message: "Provider failed",
                code: "provider",
                retryable: false,
              },
            },
          },
        ],
      });
      const waiting = yield* f.run("waiting");
      yield* f.end(active, "failed");
      yield* f.updated((run) => run.id === waiting.id && run.queueHeld === true);
      assert.equal((yield* f.hold("waiting"))?.reason, "provider_failure");
      assert.equal((yield* f.hold("stopped"))?.reason, "stop");
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("startup recovery's hold classifies as a restart hold", () => {
    const threadId = ThreadId.make("thread_restart_hold");
    const runId = RunId.make("run_restart_hold");
    let committed: Parameters<EventSink.EventSinkV2["Service"]["commitCommand"]>[0] | null = null;
    const projection = {
      thread: { id: threadId },
      runtimeRequests: [],
      providerSessions: [],
      providerThreads: [],
      providerTurns: [],
      runs: [
        {
          id: runId,
          status: "queued",
          queuePosition: 1,
          providerInstanceId: instanceId,
          activeAttemptId: RunAttemptId.make("attempt_restart_hold"),
          rootNodeId: NodeId.make("node_restart_hold"),
          userMessageId: MessageId.make("message_restart_hold"),
        },
      ],
      attempts: [],
      nodes: [],
      subagents: [],
      messages: [],
      turnItems: [],
    } as unknown as OrchestrationV2ThreadProjection;
    const layer = ProviderRuntimeRecovery.layer.pipe(
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getRecoveryThreadIds: () => Effect.succeed([threadId]),
            getRuntimeRecoveryProjection: () => Effect.succeed(projection),
            getThreadRecords: () => Effect.succeed(projection),
            // Upstream recovery reads queued messages by ID; the fixture has none.
            getMessagesById: () => Effect.succeed(new Map()),
          }),
          Layer.mock(EventSink.EventSinkV2)({
            commitCommand: (input) => {
              committed = input;
              return Effect.succeed({ committed: true, cancelledEffectCount: 0 } as never);
            },
          }),
          IdAllocator.layer,
          Layer.mock(EffectWorker.OrchestrationEffectWorkerV2)({
            runRecoveryOnce: Effect.succeed(false),
          }),
          Layer.mock(EffectOutbox.EffectOutboxV2)({
            reconcileAfterProcessLoss: Effect.succeed({ requeued: 0, cancelled: 0 }),
          }),
        ),
      ),
    );
    return Effect.gen(function* () {
      yield* (yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService).reconcile("startup");
      const event = committed?.events.find((candidate) => candidate.type === "run.updated");
      assert.isTrue(event?.type === "run.updated");
      if (event?.type !== "run.updated") return;
      assert.isTrue(event.payload.queueHeld);
      assert.isFalse("queueHeldReason" in event.payload || "queueHeldAt" in event.payload);
      const classified = classifyQueueHoldEvents(committed!.events, {
        commandType: committed!.commandType,
      });
      assert.deepEqual(
        classified.holds.map((hold) => [hold.runId, hold.reason]),
        [[runId, "restart"]],
      );
    }).pipe(Effect.provide(layer));
  });
});

describe("restart continuation release", () => {
  const setup = Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.send("stop-target", "start_immediately");
    yield* f.send("stopped", "queue_after_active");
    yield* f.orchestrator.dispatch({
      type: "thread.stop",
      commandId: CommandId.make("stop"),
      threadId: f.threadId,
    });
    // The continuation's message ID is `message:restart-continuation:<source run>`.
    yield* f.send("restart-continuation:run_source", "start_immediately");
    const continuation = yield* f.run("restart-continuation:run_source");
    assert.equal(continuation.status, "starting");
    for (const text of ["first", "second", "free"]) yield* f.send(text, "queue_after_active");
    const now = yield* DateTime.now;
    for (const text of ["first", "second"]) yield* f.putHold(yield* f.run(text), "restart", now);
    return { ...f, continuation };
  });

  it.effect("a completed continuation releases restart holds in order and keeps Stop holds", () =>
    Effect.gen(function* () {
      const f = yield* setup;
      yield* f.end(f.continuation, "completed");
      // The next start after the continuation is the first restart-held message, not `free`.
      const next = yield* f.updated(
        (run) =>
          run.status === "starting" &&
          ["message:first", "message:second", "message:free"].includes(run.userMessageId),
      );
      assert.equal(next.userMessageId, "message:first");
      const second = yield* f.run("second");
      assert.equal(second.status, "queued");
      assert.isFalse(second.queueHeld);
      assert.isUndefined(yield* f.hold("second"));
      assert.isTrue((yield* f.run("stopped")).queueHeld);
      assert.equal((yield* f.hold("stopped"))?.reason, "stop");

      assert.equal((yield* f.run("free")).status, "queued");
      yield* f.end(yield* f.run("first"), "completed");
      yield* f.starts(second.id);
      assert.equal((yield* f.run("free")).status, "queued");
      yield* f.end(yield* f.run("second"), "completed");
      yield* f.starts((yield* f.run("free")).id);
      assert.equal((yield* f.run("stopped")).status, "queued");
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect.each(["failed", "interrupted"] as const)(
    "a %s continuation releases nothing",
    (status) =>
      Effect.gen(function* () {
        const f = yield* setup;
        yield* f.end(f.continuation, status);
        // The reactor reached the queue: the only unheld message started.
        yield* f.starts((yield* f.run("free")).id);
        for (const text of ["first", "second"]) {
          const run = yield* f.run(text);
          assert.equal(run.status, "queued");
          assert.isTrue(run.queueHeld);
          assert.equal((yield* f.hold(text))?.reason, "restart");
        }
        assert.equal((yield* f.hold("stopped"))?.reason, "stop");
      }).pipe(Effect.provide(testLayer)),
  );

  it.effect("Stop during the continuation turns restart holds into Stop holds", () =>
    Effect.gen(function* () {
      const f = yield* setup;
      const stoppedAt = yield* DateTime.now;
      yield* f.orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make("stop-continuation"),
        threadId: f.threadId,
      });
      for (const text of ["first", "second", "free"]) {
        const run = yield* f.run(text);
        assert.equal(run.status, "queued");
        assert.isTrue(run.queueHeld);
        const recorded = yield* f.hold(text);
        assert.equal(recorded?.reason, "stop");
        assert.isTrue(recorded !== undefined && DateTime.Equivalence(recorded.heldAt, stoppedAt));
      }
      // Even a completed continuation record would now end nothing.
      const projection = yield* f.projection();
      assert.deepEqual(
        restartHoldsEndedBy(
          { ...f.continuation, status: "completed", completedAt: stoppedAt },
          projection.runs,
          yield* f.holds.get(projection.runs.map((run) => run.id)),
        ),
        [],
      );
    }).pipe(Effect.provide(testLayer)),
  );
});

describe("hold helpers", () => {
  const at = DateTime.makeUnsafe("2026-11-10T00:00:00.000Z");
  const later = DateTime.makeUnsafe("2026-11-10T01:00:00.000Z");
  const base = {
    id: RunId.make("run:queued"),
    threadId: ThreadId.make("thread:helpers"),
    status: "queued",
    queueHeld: true,
    userMessageId: MessageId.make("message:queued"),
    completedAt: null,
  } as unknown as OrchestrationV2Run;
  const runWith = (id: string, fields: Partial<OrchestrationV2Run> = {}) =>
    ({ ...base, id: RunId.make(id), ...fields }) as OrchestrationV2Run;

  it("ends only recorded restart holds from before a completed continuation finished", () => {
    const continuation = runWith("run:continuation", {
      userMessageId: MessageId.make("message:restart-continuation:run:source"),
      status: "completed",
      completedAt: at,
      queueHeld: false,
    });
    const restart = runWith("run:restart");
    const laterRestart = runWith("run:later");
    const stop = runWith("run:stop");
    const legacy = runWith("run:legacy");
    // A stale row for a run that is no longer held ends nothing.
    const released = runWith("run:released", { queueHeld: false });
    const holds = new Map<RunId, QueueHold>([
      [restart.id, { reason: "restart", heldAt: at }],
      [laterRestart.id, { reason: "restart", heldAt: later }],
      [stop.id, { reason: "stop", heldAt: at }],
      [released.id, { reason: "restart", heldAt: at }],
    ]);
    const runs = [restart, laterRestart, stop, legacy, released];
    assert.isTrue(isCompletedRestartContinuation(continuation));
    assert.deepEqual(
      restartHoldsEndedBy(continuation, runs, holds).map((run) => run.id),
      [restart.id],
    );
    for (const status of ["failed", "interrupted", "cancelled"] as const) {
      assert.deepEqual(restartHoldsEndedBy({ ...continuation, status }, runs, holds), []);
    }
    assert.deepEqual(
      restartHoldsEndedBy(
        { ...continuation, userMessageId: MessageId.make("message:typed") },
        runs,
        holds,
      ),
      [],
    );
  });

  const event = (
    type: "run.created" | "run.updated",
    run: OrchestrationV2Run,
  ): OrchestrationV2DomainEvent =>
    ({
      id: EventId.make(`event:${run.id}`),
      type,
      threadId: run.threadId,
      runId: run.id,
      occurredAt: later,
      payload: run,
    }) as OrchestrationV2DomainEvent;

  it("classifies holds only from the sites that place them", () => {
    const held = runWith("run:held");
    const reasons = (
      events: ReadonlyArray<OrchestrationV2DomainEvent>,
      origin?: Parameters<typeof classifyQueueHoldEvents>[1],
    ) => classifyQueueHoldEvents(events, origin).holds.map((hold) => hold.reason);
    assert.deepEqual(reasons([event("run.created", held)]), ["inherited"]);
    assert.deepEqual(reasons([event("run.updated", held)], { commandType: "thread.stop" }), [
      "stop",
    ]);
    assert.deepEqual(reasons([event("run.updated", held)], { commandType: "run.interrupt" }), [
      "stop",
    ]);
    assert.deepEqual(
      reasons([event("run.updated", held)], { commandType: "provider-runtime.reconcile" }),
      ["restart"],
    );
    // Other writers re-emit a held run unchanged; they record nothing.
    assert.deepEqual(reasons([event("run.updated", held)]), []);
    assert.deepEqual(reasons([event("run.updated", held)], { commandType: "queue.reorder" }), []);
    assert.deepEqual(
      reasons([event("run.updated", held)], {
        commandType: "provider-runtime.reconcile-orphaned-records",
      }),
      [],
    );
    assert.deepEqual(
      reasons([event("run.updated", held), event("run.updated", runWith("run:other"))], {
        queueHold: { reason: "provider_failure", runIds: [held.id] },
      }),
      ["provider_failure"],
    );
    const [recorded] = classifyQueueHoldEvents([event("run.created", held)], undefined).holds;
    assert.isTrue(recorded !== undefined && DateTime.Equivalence(recorded.heldAt, later));
  });

  it("forgets holds when a run leaves the queue, is released, or its thread is deleted", () => {
    const result = classifyQueueHoldEvents(
      [
        event("run.updated", runWith("run:resumed", { queueHeld: false })),
        event("run.updated", runWith("run:cancelled", { status: "cancelled" })),
        event("run.updated", runWith("run:running", { status: "running", queueHeld: false })),
        event("run.updated", runWith("run:unheld", { queueHeld: undefined })),
        {
          id: EventId.make("event:deleted"),
          type: "thread.deleted",
          threadId: base.threadId,
          occurredAt: later,
          payload: {},
        } as unknown as OrchestrationV2DomainEvent,
      ],
      { commandType: "thread.stop" },
    );
    assert.deepEqual(result.released, [RunId.make("run:resumed"), RunId.make("run:cancelled")]);
    assert.deepEqual(result.deletedThreads, [base.threadId]);
    assert.deepEqual(result.holds, []);
  });
});

describe("hold store", () => {
  const threadId = ThreadId.make("thread:store");
  const runId = RunId.make("run:store");
  const heldAt = DateTime.makeUnsafe("2026-11-10T00:00:00.000Z");

  it.effect("a database without the Jones table reads every hold as unrecorded", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const holds = makeQueueHoldStore(sql, makeJonesTablePresence(sql));
      yield* holds.record([{ runId, threadId, reason: "stop", heldAt }]);
      yield* holds.forget([runId]);
      yield* holds.forgetThreads([threadId]);
      assert.equal((yield* holds.get([runId])).size, 0);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("records replace, unreadable rows are ignored, and a failed write leaves no row", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* QueueHoldsMigration;
      const holds = makeQueueHoldStore(sql, makeJonesTablePresence(sql));
      yield* holds.record([{ runId, threadId, reason: "restart", heldAt }]);
      const later = DateTime.makeUnsafe("2026-11-10T02:00:00.000Z");
      yield* holds.record([{ runId, threadId, reason: "stop", heldAt: later }]);
      const recorded = (yield* holds.get([runId])).get(runId);
      assert.equal(recorded?.reason, "stop");
      assert.isTrue(recorded !== undefined && DateTime.Equivalence(recorded.heldAt, later));

      yield* sql`INSERT INTO jones_queue_holds VALUES ('run:future', ${threadId}, 'future', '2026-11-10T00:00:00.000Z')`;
      yield* sql`INSERT INTO jones_queue_holds VALUES ('run:garbled', ${threadId}, 'stop', 'not a time')`;
      const read = yield* holds.get([RunId.make("run:future"), RunId.make("run:garbled"), runId]);
      assert.deepEqual([...read.keys()], [runId]);

      const failed = RunId.make("run:failed");
      yield* sql
        .withTransaction(
          holds
            .record([{ runId: failed, threadId, reason: "stop", heldAt }])
            .pipe(Effect.andThen(Effect.fail("rejected" as const))),
        )
        .pipe(Effect.ignore);
      assert.equal((yield* holds.get([failed])).size, 0);

      yield* holds.forgetThreads([threadId]);
      assert.equal((yield* holds.get([runId])).size, 0);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );
});
