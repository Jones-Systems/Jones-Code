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
import type { ProviderAdapterV2Shape } from "@t3tools/provider-core/server/ProviderAdapter";
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

type QueueSource = "peer" | "scheduled" | "server" | "user";
const fixtureFor = (
  oldSource: QueueSource = "user",
  client: "web" | "mobile" = "web",
  withOld = true,
  stopInitial = true,
) =>
  Effect.gen(function* () {
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
      type: "start_immediately" | "queue_after_active" | "defer_start",
      source: QueueSource = "user",
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
        creationSource: source === "user" ? client : source === "server" ? "server" : "mcp",
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
    if (withOld) yield* send("old", "queue_after_active", oldSource);
    if (stopInitial)
      yield* orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make("stop"),
        threadId,
      });
    const held = (yield* projection()).runs.find(
      (run) => run.userMessageId === MessageId.make("message:old"),
    )!;
    if (withOld && stopInitial) assert.isTrue(held.queueHeld);
    return { orchestrator, store, threadId, send, projection, put, finish, held };
  });
const fixture = fixtureFor();

it.effect(
  "a same-cohort unknown hold admits a buffered wake but remains held through both completions",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const sink = yield* EventSink.EventSinkV2;
      const now = yield* DateTime.now;
      const thread = (yield* f.projection()).thread;
      yield* sink.write({
        events: [
          {
            id: EventId.make("wake:resolved-worktree"),
            type: "thread.metadata-updated",
            threadId: f.threadId,
            occurredAt: now,
            payload: { ...thread, worktreePath: process.cwd() },
          },
        ],
      });
      yield* f.send("active", "start_immediately");
      const active = (yield* f.projection()).runs.at(-1)!;
      assert.equal(active.status, "starting");
      yield* f.send("same-cohort", "queue_after_active");
      const sameCohort = { ...(yield* f.projection()).runs.at(-1)!, queueHeld: true };
      assert.isAbove(sameCohort.ordinal, active.ordinal);
      // A recorded synthetic hold is deliberately unknown, with no automatic failure origin.
      yield* sink.write({
        events: [
          {
            id: EventId.make("wake:unknown-hold"),
            type: "run.updated",
            threadId: f.threadId,
            runId: sameCohort.id,
            occurredAt: now,
            payload: sameCohort,
          },
        ],
      });
      yield* f.orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("wake:notification"),
        threadId: f.threadId,
        messageId: MessageId.make("wake:notification"),
        text: "Buffered native output ready",
        attachments: [],
        createdBy: "agent",
        creationSource: "provider",
        dispatchMode: { type: "queue_after_active" },
        notification: {
          source: { kind: "background_task" },
          outcome: "updated",
          summary: "Buffered native output ready",
        },
      });
      const wake = (yield* f.projection()).runs.at(-1)!;
      assert.equal(wake.status, "queued");
      assert.isFalse(wake.queueHeld === true);
      yield* f.send("server-continuation", "queue_after_active", "server");
      const continuation = (yield* f.projection()).runs.at(-1)!;
      assert.isTrue(continuation.queueHeld);
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
      yield* sink.write({
        events: [
          {
            id: EventId.make("wake:active-complete"),
            type: "run.updated",
            threadId: f.threadId,
            runId: active.id,
            occurredAt: now,
            payload: { ...active, status: "completed", completedAt: now },
          },
        ],
      });
      assert.equal(yield* Queue.take(started), wake.id);
      const afterActive = yield* f.projection();
      assert.deepEqual(
        afterActive.runs.find((run) => run.id === f.held.id),
        f.held,
      );
      assert.deepEqual(
        afterActive.runs.find((run) => run.id === sameCohort.id),
        sameCohort,
      );
      assert.deepEqual(
        afterActive.runs.find((run) => run.id === continuation.id),
        continuation,
      );
      // A separate thread supplies the event-based witness after the wake completion.
      const witnessId = ThreadId.make("thread:wake-listener-witness");
      yield* f.orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("witness:create"),
        threadId: witnessId,
        projectId: thread.projectId,
        title: "Synthetic listener witness",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
      for (const key of ["active", "queued"])
        yield* f.orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`witness:${key}`),
          threadId: witnessId,
          messageId: MessageId.make(`witness:${key}`),
          text: key,
          attachments: [],
          createdBy: "user",
          creationSource: "web",
          dispatchMode: { type: key === "active" ? "start_immediately" : "queue_after_active" },
        });
      const witnessRuns = (yield* f.store.getThreadProjection(witnessId)).runs;
      const witnessStarts = yield* Queue.unbounded<string>();
      yield* sink.stream({ threadId: witnessId, afterSequence: yield* sink.latestSequence() }).pipe(
        Stream.runForEach((stored) =>
          stored.event.type === "run.updated" && stored.event.payload.status === "starting"
            ? Queue.offer(witnessStarts, stored.event.payload.id)
            : Effect.void,
        ),
        Effect.forkScoped,
      );
      const runningWake = afterActive.runs.find((run) => run.id === wake.id)!;
      yield* sink.write({
        events: [
          {
            id: EventId.make("wake:complete"),
            type: "run.updated",
            threadId: f.threadId,
            runId: wake.id,
            occurredAt: now,
            payload: { ...runningWake, status: "completed", completedAt: now },
          },
        ],
      });
      yield* sink.write({
        events: [
          {
            id: EventId.make("wake:witness-complete"),
            type: "run.updated",
            threadId: witnessId,
            runId: witnessRuns[0]!.id,
            occurredAt: now,
            payload: { ...witnessRuns[0]!, status: "completed", completedAt: now },
          },
        ],
      });
      assert.equal(yield* Queue.take(witnessStarts), witnessRuns[1]!.id);
      const afterWake = yield* f.projection();
      for (const run of [f.held, sameCohort, continuation])
        assert.deepEqual(
          afterWake.runs.find((candidate) => candidate.id === run.id),
          run,
        );
      const effects = yield* (yield* EffectOutbox.EffectOutboxV2).listByThreadId(f.threadId);
      assert.lengthOf(
        effects.filter(
          (effect) =>
            effect.request.type === "provider-turn.start" && effect.request.runId === wake.id,
        ),
        1,
      );
      assert.lengthOf(
        effects.filter(
          (effect) =>
            effect.request.type === "provider-turn.start" &&
            effect.request.runId === continuation.id,
        ),
        0,
      );
    }).pipe(Effect.provide(testLayer)),
);

it.effect.each(["stop-after-failure", "failure-rewrites-stop", "unknown-hold"] as const)(
  "recorded provenance retains an older held cohort through %s and later EventSink completion",
  (mode) =>
    Effect.gen(function* () {
      const f = yield* fixtureFor("peer", "web", true, mode === "failure-rewrites-stop");
      const sink = yield* EventSink.EventSinkV2;
      if (mode === "failure-rewrites-stop") yield* f.send("failing-later", "start_immediately");
      const source = (yield* f.projection()).runs.findLast((run) => run.status === "starting")!;
      const now = yield* DateTime.now;
      if (mode === "failure-rewrites-stop") yield* f.send("failure-cohort", "queue_after_active");
      const beforeFailure = yield* sink.latestSequence();
      if (mode === "unknown-hold") {
        // Deliberately unrecorded projection mutation has no automatic-origin proof.
        yield* f.put({ ...f.held, queueHeld: true });
        yield* f.finish(source);
      } else {
        yield* sink.write({
          events: [
            {
              id: EventId.make("provenance:failure"),
              type: "turn-item.updated",
              threadId: f.threadId,
              runId: source.id,
              occurredAt: now,
              payload: {
                id: TurnItemId.make("provenance:failure"),
                type: "error",
                threadId: f.threadId,
                runId: source.id,
                nodeId: source.rootNodeId!,
                providerThreadId: source.providerThreadId,
                providerTurnId: null,
                nativeItemRef: null,
                parentItemId: null,
                ordinal: 2,
                status: "failed",
                title: "Synthetic provider failure",
                startedAt: now,
                completedAt: now,
                updatedAt: now,
                failure: {
                  class: "provider_error",
                  code: "provider_failed",
                  message: "Synthetic provider failure",
                  retryable: false,
                },
              },
            },
            {
              id: EventId.make("provenance:failed"),
              type: "run.updated",
              threadId: f.threadId,
              runId: source.id,
              occurredAt: now,
              payload: { ...source, status: "failed", completedAt: now },
            },
          ],
        });
        yield* sink
          .stream({ threadId: f.threadId, afterSequence: beforeFailure, eventType: "run.updated" })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "run.updated" &&
                stored.event.payload.id === f.held.id &&
                stored.event.payload.queueHeld === true,
            ),
            Stream.take(1),
            Stream.runDrain,
          );
        if (mode === "stop-after-failure")
          yield* f.orchestrator.dispatch({
            type: "thread.stop",
            commandId: CommandId.make("stop:after-failure"),
            threadId: f.threadId,
          });
      }
      assert.isTrue((yield* f.projection()).runs.find((run) => run.id === f.held.id)?.queueHeld);
      yield* f.send("recovery", "start_immediately");
      const recovery = (yield* f.projection()).runs.at(-1)!;
      yield* f.send("witness", "queue_after_active", "peer");
      const witness = (yield* f.projection()).runs.at(-1)!;
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
      yield* sink.write({
        events: [
          {
            id: EventId.make("provenance:recovered"),
            type: "run.updated",
            threadId: f.threadId,
            runId: recovery.id,
            occurredAt: now,
            payload: { ...recovery, status: "completed", completedAt: now },
          },
        ],
      });
      assert.equal(yield* Queue.take(started), witness.id);
      const held = (yield* f.projection()).runs.find((run) => run.id === f.held.id)!;
      assert.equal(held.status, "queued");
      assert.isTrue(held.queueHeld);
      const effects = yield* (yield* EffectOutbox.EffectOutboxV2).listByThreadId(f.threadId);
      assert.lengthOf(
        effects.filter(
          (effect) =>
            effect.request.type === "provider-turn.start" && effect.request.runId === f.held.id,
        ),
        0,
      );
      assert.lengthOf(
        effects.filter(
          (effect) =>
            effect.request.type === "provider-turn.start" && effect.request.runId === witness.id,
        ),
        1,
      );
    }).pipe(Effect.provide(testLayer)),
);

it.effect("a stopped-source notification remains held even when Stop had no queued siblings", () =>
  Effect.gen(function* () {
    const f = yield* fixtureFor("user", "web", false);
    assert.lengthOf((yield* f.projection()).runs, 1);
    yield* f.orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("notification:after-stop"),
      threadId: f.threadId,
      messageId: MessageId.make("notification:after-stop"),
      text: "Background work completed",
      attachments: [],
      createdBy: "agent",
      creationSource: "provider",
      dispatchMode: { type: "queue_after_active" },
      notification: {
        source: { kind: "background_task" },
        outcome: "updated",
        summary: "Background work completed",
      },
    });
    const wake = (yield* f.projection()).runs.at(-1)!;
    assert.equal(wake.status, "queued");
    assert.isTrue(wake.queueHeld);
    assert.equal(yield* f.orchestrator.resumeQueuedRuns, 0);
    assert.isFalse(
      (yield* (yield* EffectOutbox.EffectOutboxV2).listByThreadId(f.threadId)).some(
        (effect) =>
          effect.request.type === "provider-turn.start" && effect.request.runId === wake.id,
      ),
    );
  }).pipe(Effect.provide(testLayer)),
);

it.effect.each(["active", "stopped", "worktree"] as const)(
  "a buffered provider wake respects its %s admission source",
  (mode) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.send("later", mode === "worktree" ? "defer_start" : "start_immediately");
      const active = (yield* f.projection()).runs.at(-1)!;
      if (mode === "worktree")
        yield* f.put({ ...active, workspacePreparation: { type: "worktree", baseRef: "main" } });
      yield* f.send("current-held", "queue_after_active");
      const held = (yield* f.projection()).runs.at(-1)!;
      yield* f.put({ ...held, queueHeld: true });
      if (mode === "stopped") {
        yield* f.orchestrator.dispatch({
          type: "thread.stop",
          commandId: CommandId.make("later:stop"),
          threadId: f.threadId,
        });
        yield* f.put({ ...active, status: "running", completedAt: null });
      }
      yield* f.orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("notification:buffered"),
        threadId: f.threadId,
        messageId: MessageId.make("notification:buffered"),
        text: "Buffered output ready",
        attachments: [],
        createdBy: "agent",
        creationSource: "provider",
        dispatchMode: { type: "queue_after_active" },
        notification: {
          source: { kind: "background_task" },
          outcome: "updated",
          summary: "Buffered output ready",
        },
      });
      const after = yield* f.projection();
      assert.equal(after.runs.at(-1)?.status, "queued");
      assert.equal(after.runs.at(-1)?.queueHeld === true, mode !== "active");
      for (const id of [f.held.id, held.id])
        assert.isTrue(after.runs.find((run) => run.id === id)?.queueHeld);
    }).pipe(Effect.provide(testLayer)),
);

it.effect("paused-goal controls and replay of an older Stop cannot release its held cohort", () =>
  Effect.gen(function* () {
    const f = yield* fixtureFor("peer");
    yield* f.send("later", "start_immediately");
    const before = yield* f.projection();
    const active = before.runs.at(-1)!;
    const provider = before.providerThreads.find(
      (thread) => thread.id === active.providerThreadId,
    )!;
    yield* f.store.apply({
      id: EventId.make("goal:paused"),
      type: "provider-thread.updated",
      threadId: f.threadId,
      occurredAt: yield* DateTime.now,
      payload: { ...provider, goal: { objective: "Retained synthetic goal", status: "paused" } },
    });
    yield* f.send("/goal resume", "queue_after_active");
    const control = (yield* f.projection()).runs.at(-1)!;
    assert.isFalse(control.queueHeld === true);
    yield* f.orchestrator.dispatch({
      type: "thread.stop",
      commandId: CommandId.make("stop"),
      threadId: f.threadId,
    });
    const afterReplay = yield* f.projection();
    assert.equal(afterReplay.runs.find((run) => run.id === active.id)?.status, "starting");
    assert.isTrue(afterReplay.runs.find((run) => run.id === f.held.id)?.queueHeld);
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
          id: EventId.make("goal:later-completed"),
          type: "run.updated",
          threadId: f.threadId,
          runId: active.id,
          occurredAt: now,
          payload: { ...active, status: "completed", completedAt: now },
        },
      ],
    });
    assert.equal(yield* Queue.take(started), control.id);
    const after = yield* f.projection();
    assert.equal(after.runs.find((run) => run.id === f.held.id)?.status, "queued");
    assert.isTrue(after.runs.find((run) => run.id === f.held.id)?.queueHeld);
    assert.equal(
      after.providerThreads.find((thread) => thread.id === provider.id)?.goal?.status,
      "paused",
    );
  }).pipe(Effect.provide(testLayer)),
);

it.effect.each(["web", "mobile"] as const)(
  "%s plain sends preserve the stopped cohort while settling and when idle",
  (client) =>
    Effect.gen(function* () {
      const f = yield* fixtureFor("user", client);
      const stopped = (yield* f.projection()).runs.find(
        (run) => run.userMessageId === MessageId.make("message:initial"),
      )!;
      // A provider may still report the stopped run as running until its interrupt settles.
      yield* f.put({ ...stopped, status: "running", completedAt: null });
      yield* f.send("during-stop", "queue_after_active");
      const during = (yield* f.projection()).runs.at(-1)!;
      assert.equal(during.status, "queued");
      assert.isTrue(during.queueHeld);
      assert.isTrue((yield* f.projection()).runs.find((run) => run.id === f.held.id)?.queueHeld);
      yield* f.put(stopped);
      yield* f.send("fresh-idle", "start_immediately");
      const fresh = (yield* f.projection()).runs.at(-1)!;
      assert.equal(fresh.status, "starting");
      yield* f.send("new-cohort", "queue_after_active");
      const next = (yield* f.projection()).runs.at(-1)!;
      assert.isFalse(next.queueHeld === true);
      yield* f.finish(fresh);
      assert.equal(yield* f.orchestrator.resumeQueuedRuns, 1);
      const after = yield* f.projection();
      assert.equal(after.runs.find((run) => run.id === next.id)?.status, "starting");
      for (const id of [f.held.id, during.id]) {
        assert.equal(after.runs.find((run) => run.id === id)?.status, "queued");
        assert.isTrue(after.runs.find((run) => run.id === id)?.queueHeld);
      }
    }).pipe(Effect.provide(testLayer)),
);

it.effect.each(["peer", "server", "scheduled"] as const)(
  "EventSink completion of unrelated work cannot release an old Stop-held %s message",
  (source) =>
    Effect.gen(function* () {
      const f = yield* fixtureFor(source);
      yield* f.send("unrelated", "start_immediately");
      const later = (yield* f.projection()).runs.at(-1)!;
      yield* f.send("eligible", "queue_after_active", "peer");
      const eligible = (yield* f.projection()).runs.at(-1)!;
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
            id: EventId.make("unrelated:completed"),
            type: "run.updated",
            threadId: f.threadId,
            runId: later.id,
            occurredAt: now,
            payload: { ...later, status: "completed", completedAt: now },
          },
        ],
      });
      assert.equal(yield* Queue.take(started), eligible.id);
      const after = yield* f.projection();
      assert.equal(after.runs.find((run) => run.id === f.held.id)?.status, "queued");
      assert.isTrue(after.runs.find((run) => run.id === f.held.id)?.queueHeld);
      const effects = yield* (yield* EffectOutbox.EffectOutboxV2).listByThreadId(f.threadId);
      assert.lengthOf(
        effects.filter(
          (effect) =>
            effect.request.type === "provider-turn.start" && effect.request.runId === eligible.id,
        ),
        1,
      );
      assert.lengthOf(
        effects.filter(
          (effect) =>
            effect.request.type === "provider-turn.start" && effect.request.runId === f.held.id,
        ),
        0,
      );
    }).pipe(Effect.provide(testLayer)),
);

it.effect("preparation retry releases its own cohort and retains the older Stop-held cohort", () =>
  Effect.gen(function* () {
    const f = yield* fixtureFor("scheduled");
    yield* f.send("prepare", "defer_start");
    const preparing = (yield* f.projection()).runs.at(-1)!;
    assert.equal(preparing.status, "preparing");
    yield* f.put({ ...preparing, workspacePreparation: { type: "worktree", baseRef: "main" } });
    yield* f.send("preparation-cohort", "queue_after_active", "peer");
    const own = (yield* f.projection()).runs.at(-1)!;
    yield* f.orchestrator.dispatch({
      type: "prepared-run.fail",
      commandId: CommandId.make("prepare:fail"),
      threadId: f.threadId,
      runId: preparing.id,
      failure: {
        class: "validation_error",
        message: "Synthetic preparation failure",
        code: null,
        retryable: false,
      },
    });
    // Failure holds the pending cohort through the real EventSink listener.
    const sink = yield* EventSink.EventSinkV2;
    yield* sink.stream({ threadId: f.threadId, afterSequence: 0, eventType: "run.updated" }).pipe(
      Stream.filter(
        (stored) =>
          stored.event.type === "run.updated" &&
          stored.event.payload.id === own.id &&
          stored.event.payload.queueHeld === true,
      ),
      Stream.take(1),
      Stream.runDrain,
    );
    assert.isTrue((yield* f.projection()).runs.find((run) => run.id === own.id)?.queueHeld);
    const retry = {
      type: "prepared-run.retry" as const,
      commandId: CommandId.make("prepare:retry"),
      threadId: f.threadId,
      runId: preparing.id,
    };
    yield* f.orchestrator.dispatch(retry);
    yield* f.orchestrator.dispatch(retry);
    const after = yield* f.projection();
    assert.equal(after.runs.find((run) => run.id === preparing.id)?.status, "preparing");
    assert.isFalse(after.runs.find((run) => run.id === own.id)?.queueHeld);
    assert.isTrue(after.runs.find((run) => run.id === f.held.id)?.queueHeld);
    assert.equal(after.runs.find((run) => run.id === f.held.id)?.status, "queued");
    assert.equal(yield* f.orchestrator.resumeQueuedRuns, 0);
  }).pipe(Effect.provide(testLayer)),
);

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

it.effect(
  "selected held send starts once across retries and preserves the remaining historical queue order",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.send("later", "start_immediately");
      yield* f.send("selected", "queue_after_active");
      yield* f.send("after-selected", "queue_after_active");
      yield* f.orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make("stop-later"),
        threadId: f.threadId,
      });
      const before = yield* f.projection();
      const selected = before.runs.find(
        (run) => run.userMessageId === MessageId.make("message:selected"),
      )!;
      assert.isTrue(selected.queueHeld);
      assert.isTrue(before.runs.find((run) => run.id === f.held.id)?.queueHeld);
      const command = {
        type: "queued-run.start" as const,
        commandId: CommandId.make("send-selected"),
        threadId: f.threadId,
        runId: selected.id,
      };
      yield* f.orchestrator.dispatch(command);
      yield* f.orchestrator.dispatch(command);
      const after = yield* f.projection();
      const starting = after.runs.find((run) => run.id === selected.id)!;
      assert.equal(starting.status, "starting");
      assert.deepEqual(
        after.runs.filter((run) => run.status === "queued").map((run) => run.userMessageId),
        [f.held.userMessageId, MessageId.make("message:after-selected")],
      );
      assert.isFalse(after.runs.some((run) => run.status === "queued" && run.queueHeld));
      yield* f.finish(starting);
      yield* f.orchestrator.dispatch(command);
      assert.isFalse((yield* f.projection()).runs.some((run) => run.status === "starting"));
      const effects = yield* (yield* EffectOutbox.EffectOutboxV2).listByThreadId(f.threadId);
      assert.lengthOf(
        effects.filter(
          (effect) =>
            effect.request.type === "provider-turn.start" && effect.request.runId === selected.id,
        ),
        1,
      );
      yield* f.orchestrator.resumeQueuedRuns;
      assert.equal(
        (yield* f.projection()).runs.find((run) => run.id === f.held.id)?.status,
        "starting",
      );
    }).pipe(Effect.provide(testLayer)),
);
