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
  type OrchestrationV2Run,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import type { ProviderAdapterV2Shape } from "@t3tools/provider-core/server/ProviderAdapter";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
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
import {
  holdQueuedRun,
  isCompletedRestartContinuation,
  releaseQueuedRunHold,
  restartHoldsEndedBy,
} from "./QueueHold.ts";

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
  const put = (next: OrchestrationV2Run) =>
    Effect.gen(function* () {
      yield* store.apply({
        id: EventId.make(`fixture:${next.id}:${next.status}:${String(next.queueHeldReason)}`),
        type: "run.updated",
        threadId,
        runId: next.id,
        occurredAt: yield* DateTime.now,
        payload: next,
      });
    });
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
  return { orchestrator, store, sink, threadId, send, projection, run, put, end, updated, starts };
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
      const held = yield* f.run("queued");
      assert.isTrue(held.queueHeld);
      assert.equal(held.queueHeldReason, "stop");
      assert.isTrue(
        held.queueHeldAt !== undefined && DateTime.isGreaterThanOrEqualTo(held.queueHeldAt, before),
      );
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("a message queued behind held work inherits the hold, and resume clears it", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.send("active", "start_immediately");
      yield* f.send("cohort", "queue_after_active");
      yield* f.put(holdQueuedRun(yield* f.run("cohort"), "stop", yield* DateTime.now));
      yield* f.send("arrival", "queue_after_active", "peer");
      const arrival = yield* f.run("arrival");
      assert.isTrue(arrival.queueHeld);
      assert.equal(arrival.queueHeldReason, "inherited");
      assert.isDefined(arrival.queueHeldAt);
      // A new message never releases the hold it joined.
      assert.equal((yield* f.run("cohort")).queueHeldReason, "stop");

      yield* f.end(yield* f.run("active"), "completed");
      yield* f.orchestrator.dispatch({
        type: "queue.resume",
        commandId: CommandId.make("resume"),
        threadId: f.threadId,
      });
      for (const run of (yield* f.projection()).runs) {
        assert.isUndefined(run.queueHeldReason);
        assert.isUndefined(run.queueHeldAt);
      }
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("a provider failure holds the queue and keeps an earlier Stop reason", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.send("active", "start_immediately");
      yield* f.send("stopped", "queue_after_active");
      yield* f.send("waiting", "queue_after_active");
      yield* f.put(holdQueuedRun(yield* f.run("stopped"), "stop", yield* DateTime.now));
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
      const held = yield* f.updated((run) => run.id === waiting.id && run.queueHeld === true);
      assert.equal(held.queueHeldReason, "provider_failure");
      assert.isDefined(held.queueHeldAt);
      assert.equal((yield* f.run("waiting")).queueHeldReason, "provider_failure");
      assert.equal((yield* f.run("stopped")).queueHeldReason, "stop");
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("startup recovery records a restart hold", () => {
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
      assert.equal(event.payload.queueHeldReason, "restart");
      assert.isDefined(event.payload.queueHeldAt);
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
    for (const text of ["first", "second"]) {
      yield* f.put(holdQueuedRun(yield* f.run(text), "restart", now));
    }
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
      assert.isUndefined(second.queueHeldReason);
      assert.isUndefined(second.queueHeldAt);
      const stopped = yield* f.run("stopped");
      assert.isTrue(stopped.queueHeld);
      assert.equal(stopped.queueHeldReason, "stop");

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
          assert.equal(run.queueHeldReason, "restart");
        }
        assert.equal((yield* f.run("stopped")).queueHeldReason, "stop");
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
        assert.equal(run.queueHeldReason, "stop");
        assert.isTrue(DateTime.Equivalence(run.queueHeldAt!, stoppedAt));
      }
      // Even a completed continuation record would now end nothing.
      const projection = yield* f.projection();
      assert.deepEqual(
        restartHoldsEndedBy(
          { ...f.continuation, status: "completed", completedAt: stoppedAt },
          projection.runs,
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
    userMessageId: MessageId.make("message:queued"),
    completedAt: null,
  } as unknown as OrchestrationV2Run;

  it("keeps the first hold's reason and time", () => {
    const held = holdQueuedRun(base, "stop", at);
    const again = holdQueuedRun(held, "provider_failure", later);
    assert.equal(again.queueHeldReason, "stop");
    assert.isTrue(DateTime.Equivalence(again.queueHeldAt!, at));
    const released = releaseQueuedRunHold(again);
    assert.isFalse(released.queueHeld);
    assert.isFalse("queueHeldReason" in released || "queueHeldAt" in released);
  });

  it("ends only restart holds recorded before a completed continuation finished", () => {
    const continuation = {
      ...base,
      id: RunId.make("run:continuation"),
      userMessageId: MessageId.make("message:restart-continuation:run:source"),
      status: "completed",
      completedAt: at,
    } as OrchestrationV2Run;
    const restart = holdQueuedRun({ ...base, id: RunId.make("run:restart") }, "restart", at);
    const laterRestart = holdQueuedRun({ ...base, id: RunId.make("run:later") }, "restart", later);
    const stop = holdQueuedRun({ ...base, id: RunId.make("run:stop") }, "stop", at);
    const legacy = { ...base, id: RunId.make("run:legacy"), queueHeld: true };
    assert.isTrue(isCompletedRestartContinuation(continuation));
    assert.deepEqual(
      restartHoldsEndedBy(continuation, [restart, laterRestart, stop, legacy]).map((run) => run.id),
      [restart.id],
    );
    for (const status of ["failed", "interrupted", "cancelled"] as const) {
      assert.deepEqual(restartHoldsEndedBy({ ...continuation, status }, [restart]), []);
    }
    assert.deepEqual(
      restartHoldsEndedBy({ ...continuation, userMessageId: MessageId.make("message:typed") }, [
        restart,
      ]),
      [],
    );
  });
});
