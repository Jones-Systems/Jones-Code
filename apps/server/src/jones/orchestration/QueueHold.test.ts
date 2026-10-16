import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  ScheduledTaskId,
  TurnItemId,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import type { ProviderAdapterV2Shape } from "../../orchestration-v2/ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";

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
    { name: "queue-hold" },
    ProviderAdapterRegistry.layerFromAdapters([adapter]),
    { databaseLayer: database, runEffectWorker: false },
  ),
);

const fixture = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const store = yield* ProjectionStore.ProjectionStoreV2;
  const threadId = ThreadId.make("thread:queue-hold");
  const now = yield* DateTime.now;
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make("create"),
    threadId,
    projectId: ProjectId.make("project:queue-hold"),
    title: "Queue hold",
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
    source: "peer" | "scheduled" | "user" = "user",
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
      ...(source === "scheduled"
        ? { scheduledTaskId: ScheduledTaskId.make("scheduled-task") }
        : {}),
    });
  const projection = () => store.getThreadProjection(threadId);
  const put = (run: OrchestrationV2Run) =>
    store.apply({
      id: EventId.make(`fixture:${run.id}:${run.status}`),
      type: "run.updated",
      threadId,
      runId: run.id,
      occurredAt: now,
      payload: run,
    });
  const finish = (run: OrchestrationV2Run) =>
    put({ ...run, status: "completed", completedAt: now });
  yield* send("initial", "start_immediately");
  yield* send("old", "queue_after_active");
  yield* orchestrator.dispatch({
    type: "thread.stop",
    commandId: CommandId.make("stop"),
    threadId,
  });
  const held = (yield* projection()).runs.find(
    (run) => run.userMessageId === MessageId.make("message:old"),
  )!;
  assert.isTrue(held.queueHeld);
  return { orchestrator, store, threadId, send, projection, put, finish, held };
});

it.effect.each(["peer", "scheduled", "user"] as const)(
  "new %s work behind a later run starts once while the stopped queue stays held",
  (source) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      assert.equal(yield* f.orchestrator.resumeQueuedRuns, 0);
      yield* f.send("later", "start_immediately");
      const later = (yield* f.projection()).runs.at(-1)!;
      yield* f.send("peer", "queue_after_active", source);
      const peer = (yield* f.projection()).runs.at(-1)!;
      assert.isFalse(peer.queueHeld === true);
      assert.isFalse(yield* f.store.canStartQueuedRun(f.threadId));
      const sink = yield* EventSink.EventSinkV2;
      const started = yield* Queue.unbounded<string>();
      yield* sink
        .stream({ threadId: f.threadId, afterSequence: yield* sink.latestSequence() })
        .pipe(
          Stream.runForEach((stored) =>
            stored.event.type === "run.updated" && stored.event.payload.status === "starting"
              ? Queue.offer(started, stored.event.payload.id)
              : Effect.void,
          ),
          Effect.forkScoped,
        );
      const now = yield* DateTime.now;
      yield* sink.write({
        events: [
          {
            id: EventId.make("terminal:later"),
            type: "run.updated",
            threadId: f.threadId,
            runId: later.id,
            occurredAt: now,
            payload: { ...later, status: "completed", completedAt: now },
          },
        ],
      });
      assert.equal(yield* Queue.take(started), peer.id);
      yield* f.orchestrator.resumeQueuedRuns;
      const runs = (yield* f.projection()).runs;
      assert.equal(runs.find((run) => run.id === peer.id)?.status, "starting");
      assert.equal(runs.find((run) => run.id === f.held.id)?.status, "queued");
      assert.isTrue(runs.find((run) => run.id === f.held.id)?.queueHeld);
      const effects = yield* (yield* EffectOutbox.EffectOutboxV2).listByThreadId(f.threadId);
      assert.lengthOf(
        effects.filter(
          (effect) =>
            effect.request.type === "provider-turn.start" && effect.request.runId === peer.id,
        ),
        1,
      );
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "a held arrival in the active cohort holds subsequent arrivals until explicit resume",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.send("later", "start_immediately");
      const later = (yield* f.projection()).runs.at(-1)!;
      yield* f.send("cohort", "queue_after_active");
      const cohort = (yield* f.projection()).runs.at(-1)!;
      yield* f.put({ ...cohort, queueHeld: true });
      yield* f.send("peer", "queue_after_active", "peer");
      assert.isTrue((yield* f.projection()).runs.at(-1)!.queueHeld);
      yield* f.finish(later);
      assert.equal(yield* f.orchestrator.resumeQueuedRuns, 0);
      yield* f.orchestrator.dispatch({
        type: "queue.resume",
        commandId: CommandId.make("resume"),
        threadId: f.threadId,
      });
      const runs = (yield* f.projection()).runs;
      assert.isFalse(runs.some((run) => run.status === "queued" && run.queueHeld === true));
      assert.equal(runs.find((run) => run.id === f.held.id)?.status, "starting");
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "a start failure is attributed to eligible work rather than the historical held head",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.send("later", "start_immediately");
      const later = (yield* f.projection()).runs.at(-1)!;
      yield* f.send("broken", "queue_after_active", "peer");
      const broken = (yield* f.projection()).runs.at(-1)!;
      yield* f.put({ ...broken, rootNodeId: null });
      yield* f.finish(later);
      yield* f.orchestrator.resumeQueuedRuns;
      const runs = (yield* f.projection()).runs;
      assert.equal(runs.find((run) => run.id === broken.id)?.status, "failed");
      assert.equal(runs.find((run) => run.id === f.held.id)?.status, "queued");
      assert.isTrue(runs.find((run) => run.id === f.held.id)?.queueHeld);
      const effects = yield* (yield* EffectOutbox.EffectOutboxV2).listByThreadId(f.threadId);
      assert.isFalse(
        effects.some(
          (effect) =>
            effect.request.type === "provider-turn.start" && effect.request.runId === broken.id,
        ),
      );
    }).pipe(Effect.provide(testLayer)),
);

it.effect("eligible completion priority and FIFO survive historical held completion work", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.send("later", "start_immediately");
    const later = (yield* f.projection()).runs.at(-1)!;
    for (const text of ["first", "second", "completion"]) yield* f.send(text, "queue_after_active");
    const before = yield* f.projection();
    for (const message of before.messages.filter((message) =>
      ["old", "completion"].includes(message.text),
    )) {
      yield* f.store.apply({
        id: EventId.make(`completion:${message.id}`),
        type: "message.updated",
        threadId: f.threadId,
        occurredAt: yield* DateTime.now,
        payload: {
          ...message,
          delegatedCompletion: { parentRunId: later.id, generation: 1, taskIds: [] },
        },
      });
    }
    yield* f.finish(later);
    for (const text of ["completion", "first", "second"]) {
      yield* f.orchestrator.resumeQueuedRuns;
      const projection = yield* f.projection();
      const active = projection.runs.find((run) => run.status === "starting")!;
      assert.equal(
        projection.messages.find((message) => message.id === active.userMessageId)?.text,
        text,
      );
      yield* f.finish(active);
    }
    assert.equal(yield* f.orchestrator.resumeQueuedRuns, 0);
    assert.isTrue((yield* f.projection()).runs.find((run) => run.id === f.held.id)?.queueHeld);
  }).pipe(Effect.provide(testLayer)),
);

it.effect("a validation failure still permits eligible work behind historical holds", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.send("later", "start_immediately");
    const later = (yield* f.projection()).runs.at(-1)!;
    yield* f.send("recovery", "queue_after_active", "peer");
    const recovery = (yield* f.projection()).runs.at(-1)!;
    const sink = yield* EventSink.EventSinkV2;
    const started = yield* Queue.unbounded<string>();
    yield* sink.stream({ threadId: f.threadId, afterSequence: yield* sink.latestSequence() }).pipe(
      Stream.runForEach((stored) =>
        stored.event.type === "run.updated" && stored.event.payload.status === "starting"
          ? Queue.offer(started, stored.event.payload.id)
          : Effect.void,
      ),
      Effect.forkScoped,
    );
    const now = yield* DateTime.now;
    yield* sink.write({
      events: [
        {
          id: EventId.make("validation:error"),
          type: "turn-item.updated",
          threadId: f.threadId,
          runId: later.id,
          nodeId: later.rootNodeId!,
          occurredAt: now,
          payload: {
            id: TurnItemId.make("validation:error"),
            type: "error",
            threadId: f.threadId,
            runId: later.id,
            nodeId: later.rootNodeId!,
            providerThreadId: later.providerThreadId,
            providerTurnId: null,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: 2,
            status: "failed",
            title: "Validation failure",
            startedAt: now,
            completedAt: now,
            updatedAt: now,
            failure: {
              class: "validation_error",
              message: "Invalid input",
              code: "validation",
              retryable: false,
            },
          },
        },
        {
          id: EventId.make("validation:failed"),
          type: "run.updated",
          threadId: f.threadId,
          runId: later.id,
          occurredAt: now,
          payload: { ...later, status: "failed", completedAt: now },
        },
      ],
    });
    assert.equal(yield* Queue.take(started), recovery.id);
    const runs = (yield* f.projection()).runs;
    assert.isTrue(runs.find((run) => run.id === f.held.id)?.queueHeld);
    assert.equal(runs.find((run) => run.id === f.held.id)?.status, "queued");
  }).pipe(Effect.provide(testLayer)),
);
