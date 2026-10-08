import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CheckpointScopeId,
  CommandId,
  EventId,
  MessageId,
  ProviderTurnId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import { WORK_MODE_INTERVAL_MS, WORK_MODE_SENTINEL } from "@t3tools/shared/jones/workMode";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as Scheduler from "../../scheduling/Scheduler.ts";
import * as CheckpointService from "../../orchestration-v2/CheckpointService.ts";
import * as CommandPolicy from "../../orchestration-v2/CommandPolicy.ts";
import * as CommandReceipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as ContextHandoff from "../../orchestration-v2/ContextHandoffService.ts";
import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as IdAllocator from "../../orchestration-v2/IdAllocator.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import type { ProviderAdapterV2SessionRuntime } from "../../orchestration-v2/ProviderAdapter.ts";
import * as ProviderAdapters from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderSessions from "../../orchestration-v2/ProviderSessionManager.ts";
import * as ProviderSwitch from "../../orchestration-v2/ProviderSwitchService.ts";
import * as RuntimePolicy from "../../orchestration-v2/RuntimePolicy.ts";
import * as ThreadFork from "../../orchestration-v2/ThreadForkService.ts";
import * as TurnItemPositions from "../../orchestration-v2/TurnItemPositionStore.ts";
import { workModeFixture } from "./Fixtures.testkit.ts";
import { workModeCandidate, workModeCommand } from "./Policy.ts";
import * as WorkMode from "./WorkMode.ts";

const fixture = workModeFixture();
const session = fixture.providerSessions[0]!;
const unexpected = () => Effect.die("Unexpected provider operation in admission test");
const runtime: ProviderAdapterV2SessionRuntime = {
  instanceId: session.providerInstanceId,
  driver: session.driver,
  providerSessionId: session.id,
  providerSession: session,
  events: Stream.empty,
  ensureThread: unexpected,
  resumeThread: unexpected,
  startTurn: unexpected,
  steerTurn: unexpected,
  interruptTurn: unexpected,
  respondToRuntimeRequest: unexpected,
  readThreadSnapshot: unexpected,
  rollbackThread: unexpected,
  forkThread: unexpected,
};

const database = SqlitePersistence.layerMemory;
const stores = Layer.mergeAll(
  EventStore.layer,
  ProjectionStore.layer,
  ProjectStore.layer,
  CommandReceipts.layer,
  EffectOutbox.layer,
  TurnItemPositions.layer,
).pipe(Layer.provideMerge(database));
const sink = EventSink.layerFromStores.pipe(Layer.provide(stores));
const dependencies = Layer.mergeAll(
  stores,
  sink,
  IdAllocator.layer,
  CommandPolicy.layer,
  RuntimePolicy.layer,
  ServerSettings.layerTest({ workModeEnabled: true }).pipe(Layer.orDie),
  Layer.mock(ContextHandoff.ContextHandoffServiceV2)({}),
  Layer.mock(ProviderSwitch.ProviderSwitchServiceV2)({}),
  Layer.mock(ThreadFork.ThreadForkServiceV2)({}),
  Layer.mock(CheckpointService.CheckpointServiceV2)({
    prepareRootRunScope: (input) =>
      Effect.succeed({
        id: CheckpointScopeId.make(`scope:${input.runId}`),
        threadId: input.threadId,
        runId: input.runId,
        nodeId: input.rootNodeId,
        parentScopeId: null,
        providerThreadId: input.providerThreadId,
        kind: "root_run" as const,
        ordinalWithinParent: 0,
        advancesAppRunCount: true,
        cwd: input.cwd,
        createdAt: input.createdAt,
      }),
    ensureScope: (scope) => Effect.succeed(scope),
  }),
  ProviderAdapters.layerFromAdapters([
    {
      instanceId: session.providerInstanceId,
      driver: session.driver,
      getCapabilities: () => Effect.succeed(session.capabilities),
      planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
      openSession: unexpected,
    },
  ]),
  Layer.mock(ProviderSessions.ProviderSessionManagerV2)({
    get: () => Effect.succeed(Option.some(runtime)),
  }),
  Layer.mock(Scheduler.Scheduler)({ register: () => Effect.void }),
  NodeServices.layer,
);
const orchestratorLayer = Orchestrator.layer.pipe(Layer.provideMerge(dependencies));
const testLayer = WorkMode.layer.pipe(Layer.provideMerge(orchestratorLayer));

const seed = Effect.gen(function* () {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const now = yield* DateTime.now;
  const at = (value: OrchestrationV2ThreadProjection) => ({
    ...value,
    thread: { ...value.thread, createdAt: now, updatedAt: now },
    runs: value.runs.map((run) => ({ ...run, requestedAt: now, startedAt: now, completedAt: now })),
    messages: value.messages.map((message) => ({ ...message, createdAt: now, updatedAt: now })),
  });
  const value = at(workModeFixture());
  const events: ReadonlyArray<OrchestrationV2DomainEvent> = [
    {
      id: EventId.make("seed:thread"),
      type: "thread.created",
      threadId: value.thread.id,
      occurredAt: now,
      payload: value.thread,
    },
    {
      id: EventId.make("seed:run"),
      type: "run.created",
      threadId: value.thread.id,
      occurredAt: now,
      payload: value.runs[0]!,
    },
    {
      id: EventId.make("seed:message"),
      type: "message.updated",
      threadId: value.thread.id,
      occurredAt: now,
      payload: value.messages[0]!,
    },
    {
      id: EventId.make("seed:session"),
      type: "provider-session.attached",
      threadId: value.thread.id,
      occurredAt: now,
      payload: value.providerSessions[0]!,
    },
    {
      id: EventId.make("seed:context"),
      type: "provider-thread.updated",
      threadId: value.thread.id,
      occurredAt: now,
      payload: value.providerThreads[0]!,
    },
    {
      id: EventId.make("seed:turn"),
      type: "provider-turn.updated",
      threadId: value.thread.id,
      occurredAt: now,
      payload: value.providerTurns[0]!,
    },
  ];
  for (const event of events) yield* projections.apply(event);
  return value.thread.id;
});

function completeLatestRun() {
  return Effect.gen(function* () {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const projection = yield* projections.getThreadProjection(fixture.thread.id);
    const run = projection.runs.toSorted((a, b) => b.ordinal - a.ordinal)[0]!;
    const owner = projection.providerThreads.find((thread) => thread.id === run.providerThreadId)!;
    const now = yield* DateTime.now;
    yield* projections.apply({
      id: EventId.make(`complete:${run.id}`),
      type: "run.updated",
      threadId: fixture.thread.id,
      occurredAt: now,
      payload: { ...run, status: "completed", startedAt: now, completedAt: now },
    });
    yield* projections.apply({
      id: EventId.make(`turn:${run.id}`),
      type: "provider-turn.updated",
      threadId: fixture.thread.id,
      occurredAt: now,
      payload: {
        ...fixture.providerTurns[0]!,
        id: ProviderTurnId.make(`turn:${run.id}`),
        providerThreadId: owner.id,
        nodeId: run.rootNodeId!,
        runAttemptId: run.activeAttemptId,
        ordinal: run.ordinal,
        status: "completed",
        startedAt: now,
        completedAt: now,
      },
    });
    yield* projections.apply({
      id: EventId.make(`idle:${run.id}`),
      type: "provider-thread.updated",
      threadId: fixture.thread.id,
      occurredAt: now,
      payload: { ...owner, status: "idle", updatedAt: now },
    });
  });
}

it.effect(
  "sweeps nine virtual 55-minute rounds without per-thread timers or duplicate dispatch",
  () =>
    Effect.gen(function* () {
      const threadId = yield* seed;
      const workMode = yield* WorkMode.WorkMode;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      yield* workMode.sweep;
      assert.lengthOf((yield* projections.getThreadProjection(threadId)).runs, 1);
      for (let round = 1; round <= 9; round++) {
        yield* TestClock.adjust(WORK_MODE_INTERVAL_MS);
        yield* workMode.sweep;
        yield* workMode.sweep;
        const projection = yield* projections.getThreadProjection(threadId);
        assert.lengthOf(projection.runs, round + 1);
        assert.equal(projection.runs.at(-1)!.status, "starting");
        const sent = projection.messages.find(
          (message) => message.id === projection.runs.at(-1)!.userMessageId,
        )!;
        assert.equal(sent.text, WORK_MODE_SENTINEL);
        assert.equal(sent.createdBy, "system");
        assert.equal(sent.creationSource, "server");
        yield* completeLatestRun();
      }
    }).pipe(Effect.provide(testLayer)),
);

it.effect("regular activity resets sweep timing and disabling stops later sweeps", () =>
  Effect.gen(function* () {
    const threadId = yield* seed;
    const workMode = yield* WorkMode.WorkMode;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const settings = yield* ServerSettings.ServerSettingsService;
    yield* TestClock.adjust(WORK_MODE_INTERVAL_MS - 1);
    const now = yield* DateTime.now;
    yield* projections.apply({
      id: EventId.make("regular:message"),
      type: "message.updated",
      threadId,
      occurredAt: now,
      payload: {
        ...fixture.messages[0]!,
        id: MessageId.make("regular-message"),
        createdAt: now,
        updatedAt: now,
      },
    });
    yield* TestClock.adjust(1);
    yield* workMode.sweep;
    assert.lengthOf((yield* projections.getThreadProjection(threadId)).runs, 1);
    yield* TestClock.adjust(WORK_MODE_INTERVAL_MS - 1);
    yield* workMode.sweep;
    assert.lengthOf((yield* projections.getThreadProjection(threadId)).runs, 2);
    yield* completeLatestRun();
    yield* settings.updateSettings({ workModeEnabled: false });
    yield* TestClock.adjust(WORK_MODE_INTERVAL_MS);
    yield* workMode.sweep;
    assert.lengthOf((yield* projections.getThreadProjection(threadId)).runs, 2);
  }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "reconciles persisted acceptance after rebuilding the actual orchestrator, even after disable",
  () =>
    Effect.gen(function* () {
      const threadId = yield* seed;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const settings = yield* ServerSettings.ServerSettingsService;
      yield* TestClock.adjust(WORK_MODE_INTERVAL_MS);
      const candidate = workModeCandidate(
        (yield* projections.getThreadShell(threadId))!,
        DateTime.toEpochMillis(yield* DateTime.now),
      )!;
      assert.equal(yield* orchestrator.requestWorkMode(candidate), "dispatched");
      yield* settings.updateSettings({ workModeEnabled: false });
      const replay = Effect.gen(function* () {
        const restarted = yield* Orchestrator.OrchestratorV2;
        return yield* restarted.requestWorkMode(candidate);
      }).pipe(Effect.provide(Orchestrator.layer));
      assert.equal(yield* replay, "acknowledged");
      assert.lengthOf((yield* projections.getThreadProjection(threadId)).runs, 2);
      const receipts = yield* CommandReceipts.CommandReceiptStoreV2;
      assert.isTrue(
        Option.isSome(yield* receipts.getByCommandId(workModeCommand(candidate).commandId)),
      );
    }).pipe(Effect.provide(testLayer)),
);

it.effect("a concurrent ordinary start wins the lock and keepwarm skips without queueing", () =>
  Effect.gen(function* () {
    const threadId = yield* seed;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    yield* TestClock.adjust(WORK_MODE_INTERVAL_MS);
    const candidate = workModeCandidate(
      (yield* projections.getThreadShell(threadId))!,
      DateTime.toEpochMillis(yield* DateTime.now),
    )!;
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    // Hold the ordinary command inside the actual lock through a checkpoint boundary.
    const checkpoint = yield* CheckpointService.CheckpointServiceV2;
    const guardedCheckpoint = Layer.succeed(CheckpointService.CheckpointServiceV2, {
      ...checkpoint,
      prepareRootRunScope: (input) =>
        Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
          Effect.andThen(checkpoint.prepareRootRunScope(input)),
        ),
    });
    const race = Effect.gen(function* () {
      const fresh = yield* Orchestrator.OrchestratorV2;
      const normal = yield* Effect.forkChild(
        fresh.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("ordinary-start"),
          messageId: MessageId.make("ordinary-start"),
          threadId,
          text: "Do regular work",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        }),
      );
      yield* Effect.raceFirst(
        Deferred.await(entered),
        Fiber.join(normal).pipe(
          Effect.andThen(
            Effect.die("Ordinary start completed before reaching its checkpoint barrier."),
          ),
        ),
      );
      const keepwarm = yield* Effect.forkChild(fresh.requestWorkMode(candidate));
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(normal);
      assert.equal(yield* Fiber.join(keepwarm), "skipped");
    }).pipe(Effect.provide(Layer.fresh(Orchestrator.layer).pipe(Layer.provide(guardedCheckpoint))));
    yield* race;
    const projection = yield* projections.getThreadProjection(threadId);
    assert.lengthOf(projection.runs, 2);
    assert.isFalse(projection.runs.some((run) => run.status === "queued"));
    assert.isFalse(projection.messages.some((message) => message.text === WORK_MODE_SENTINEL));
  }).pipe(Effect.provide(testLayer)),
);

it.effect("disable is serialized against admission through durable dispatch", () =>
  Effect.gen(function* () {
    const threadId = yield* seed;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const settings = yield* ServerSettings.ServerSettingsService;
    const checkpoint = yield* CheckpointService.CheckpointServiceV2;
    yield* TestClock.adjust(WORK_MODE_INTERVAL_MS);
    const candidate = workModeCandidate(
      (yield* projections.getThreadShell(threadId))!,
      DateTime.toEpochMillis(yield* DateTime.now),
    )!;
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const disabling = yield* Deferred.make<void>();
    const guardedCheckpoint = Layer.succeed(CheckpointService.CheckpointServiceV2, {
      ...checkpoint,
      prepareRootRunScope: (input) =>
        Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
          Effect.andThen(checkpoint.prepareRootRunScope(input)),
        ),
    });
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const admission = yield* Effect.forkChild(orchestrator.requestWorkMode(candidate));
      yield* Effect.raceFirst(
        Deferred.await(entered),
        Fiber.join(admission).pipe(
          Effect.andThen(
            Effect.die("Work Mode admission completed before reaching its checkpoint barrier."),
          ),
        ),
      );
      const disable = yield* Effect.forkChild(
        Deferred.succeed(disabling, undefined).pipe(
          Effect.andThen(settings.updateSettings({ workModeEnabled: false })),
        ),
      );
      yield* Deferred.await(disabling);
      assert.isTrue((yield* settings.getSettings).workModeEnabled);
      yield* Deferred.succeed(release, undefined);
      assert.equal(yield* Fiber.join(admission), "dispatched");
      assert.isFalse((yield* Fiber.join(disable)).workModeEnabled);
      yield* completeLatestRun();
      yield* TestClock.adjust(WORK_MODE_INTERVAL_MS);
      const next = workModeCandidate(
        (yield* projections.getThreadShell(threadId))!,
        DateTime.toEpochMillis(yield* DateTime.now),
      )!;
      assert.equal(yield* orchestrator.requestWorkMode(next), "skipped");
    }).pipe(Effect.provide(Layer.fresh(Orchestrator.layer).pipe(Layer.provide(guardedCheckpoint))));
    assert.lengthOf((yield* projections.getThreadProjection(threadId)).runs, 2);
  }).pipe(Effect.provide(testLayer)),
);

it.effect("a completed persisted context without a live session never starts a run", () =>
  Effect.gen(function* () {
    const threadId = yield* seed;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    yield* TestClock.adjust(WORK_MODE_INTERVAL_MS);
    const candidate = workModeCandidate(
      (yield* projections.getThreadShell(threadId))!,
      DateTime.toEpochMillis(yield* DateTime.now),
    )!;
    const cold = Layer.mock(ProviderSessions.ProviderSessionManagerV2)({
      get: () => Effect.succeed(Option.none()),
    });
    assert.equal(
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        return yield* orchestrator.requestWorkMode(candidate);
      }).pipe(Effect.provide(Layer.fresh(Orchestrator.layer).pipe(Layer.provide(cold)))),
      "skipped",
    );
    assert.lengthOf((yield* projections.getThreadProjection(threadId)).runs, 1);
    const receipts = yield* CommandReceipts.CommandReceiptStoreV2;
    assert.isTrue(
      Option.isNone(yield* receipts.getByCommandId(workModeCommand(candidate).commandId)),
    );
  }).pipe(Effect.provide(testLayer)),
);
