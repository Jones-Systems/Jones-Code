import { expect, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  EnvironmentId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ThreadId,
  RunId,
  type OrchestrationV2ProviderGoal,
  type ThreadGoalInput,
} from "@t3tools/contracts";
import type {
  ProviderAdapterV2Shape,
  ProviderAdapterV2SessionRuntime,
} from "@t3tools/provider-core/server/ProviderAdapter";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import { pruneEventLogLikeUpstream } from "../persistence/prunedEventLog.testkit.ts";
import * as Sqlite from "../../persistence/Sqlite.ts";
import * as ServerConfig from "../../config.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Registry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import * as Sessions from "../../orchestration-v2/ProviderSessionManager.ts";
import * as Replay from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import * as GoalStart from "./GoalStartDispatch.ts";
import * as GoalControl from "./GoalControl.ts";

const instanceId = ProviderInstanceId.make("codex");
const threadId = ThreadId.make("armed-goal-thread");
const sessionId = ProviderSessionId.make("armed-goal-session");
const modelSelection = { instanceId, model: "test" };
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("This fixture records starts without launching a provider"),
} as ProviderAdapterV2Shape;
const database = Sqlite.layerMemory;
const ResumeSubscription = Context.Reference<boolean>("test/ResumeSubscription", {
  defaultValue: () => false,
});
const replayLayer = (
  eventStoreLayer: Layer.Layer<EventStore.EventStoreV2, never, SqlClient.SqlClient>,
) =>
  Replay.layerWithRegistry({ name: "armed-goal-resume" }, Registry.layerFromAdapters([adapter]), {
    databaseLayer: database,
    eventStoreLayer,
    runEffectWorker: false,
    serverConfigLayer: ServerConfig.layerTest(process.cwd(), { prefix: "armed-goal-resume-" }).pipe(
      Layer.provide(NodeServices.layer),
      Layer.orDie,
    ),
  });
const threadLayer = (replay: ReturnType<typeof replayLayer>) =>
  Layer.effect(
    Threads.ThreadManagementService,
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      return Threads.ThreadManagementService.of({
        getThreadShell: orchestrator.getThreadShell,
        getThreadRecords: orchestrator.getThreadRecords,
        getProjectThreadRecords: (
          input: Parameters<
            Threads.ThreadManagementService["Service"]["getProjectThreadRecords"]
          >[0],
          fields: Parameters<
            Threads.ThreadManagementService["Service"]["getProjectThreadRecords"]
          >[1],
        ) => orchestrator.getThreadRecords(input.threadId, fields),
        dispatch: orchestrator.dispatch,
      } as unknown as Threads.ThreadManagementService["Service"]);
    }),
  ).pipe(Layer.provide(replay));
const scope: McpInvocationScope = {
  environmentId: EnvironmentId.make("synthetic"),
  capabilities: new Set(["orchestration"]),
  issuedAt: 0,
  requestNamespace: "armed-test",
  client: undefined,
  thread: { threadId, providerInstanceId: instanceId, providerSessionId: "synthetic-credential" },
};
function fixtureLayer(status: "paused" | "blocked" = "paused") {
  let goal: OrchestrationV2ProviderGoal | null = {
    objective: "Complete the assigned project",
    status,
    tokensUsed: 42,
  };
  let attached = true;
  let stopped = false;
  let mutations = 0;
  let startCalls = 0;
  let observeCalls = 0;
  let afterRead: (() => void) | undefined;
  let loseObservation = false;
  let beforeSubscription = Effect.void;
  let afterRegistration: Effect.Effect<void> | undefined;
  let beforeConsume: Effect.Effect<void> | undefined;
  let beforeReplay: Effect.Effect<void> | undefined;
  let afterReplay: Effect.Effect<void> | undefined;
  let afterNativeRead: Effect.Effect<void> | undefined;
  let registrationHighWater: number | undefined;
  const observedHistory = Layer.effect(
    EventStore.EventStoreV2,
    Effect.gen(function* () {
      const store = yield* EventStore.EventStoreV2;
      return EventStore.EventStoreV2.of({
        ...store,
        latestSequence: (input) =>
          store.latestSequence(input).pipe(
            Effect.tap(
              Effect.fnUntraced(function* (highWater) {
                if (
                  !(yield* ResumeSubscription) ||
                  input?.threadId !== undefined ||
                  afterRegistration === undefined
                )
                  return;
                registrationHighWater = highWater;
                const effect = afterRegistration;
                afterRegistration = undefined;
                yield* effect;
              }),
            ),
          ),
        read: (input) => {
          // EventSink's registration replay has a page limit. These gates surround only
          // GoalControl's finite stored replay, preserving real storage and event values.
          if (input?.throughSequence === undefined || input.limit !== undefined)
            return store.read(input);
          return Stream.unwrap(
            Effect.gen(function* () {
              const before = beforeReplay;
              const after = afterReplay;
              beforeReplay = undefined;
              afterReplay = undefined;
              if (before !== undefined) yield* before;
              return Stream.concat(
                store.read(input),
                Stream.fromEffect(after ?? Effect.void).pipe(Stream.drain),
              );
            }),
          );
        },
      });
    }),
  ).pipe(Layer.provide(EventStore.layer));
  const replay = replayLayer(observedHistory);
  const threads = threadLayer(replay);
  type StartInput = Parameters<GoalStart.GoalStartDispatch["Service"]["start"]>[0];
  let beforeStart: Effect.Effect<void> | ((input: StartInput) => Effect.Effect<void>) = Effect.void;
  let readers = 0;
  let closed: Queue.Queue<void>;
  let captures: Queue.Queue<void>;
  let attempts: Queue.Queue<void>;
  const signals = Layer.effectDiscard(
    Effect.gen(function* () {
      closed = yield* Queue.unbounded<void>();
      captures = yield* Queue.unbounded<void>();
      attempts = yield* Queue.unbounded<void>();
    }),
  );
  let runtime = {
    instanceId,
    providerSessionId: sessionId,
    controlGoal: (input) =>
      Effect.gen(function* () {
        if (!(yield* input.revalidate))
          return { outcome: "stale" as const, goal: null, reason: "stale" };
        if (input.action !== "read") {
          mutations++;
          goal =
            input.action === "clear"
              ? null
              : {
                  ...goal!,
                  ...(input.action === "pause"
                    ? { status: "paused" as const }
                    : { objective: input.objective! }),
                };
        }
        if (input.action === "read") {
          const callback = afterRead;
          afterRead = undefined;
          callback?.();
          const effect = afterNativeRead;
          afterNativeRead = undefined;
          if (effect !== undefined) yield* effect;
        }
        return {
          outcome: input.action === "read" ? ("observed" as const) : ("applied" as const),
          goal,
          reason: "native-read",
        };
      }),
  } as ProviderAdapterV2SessionRuntime;
  const sessions = Layer.mock(Sessions.ProviderSessionManagerV2)({
    get: () => Effect.sync(() => (stopped ? Option.none() : Option.some(runtime))),
    isMcpCallerAttached: (input) =>
      Effect.succeed(
        attached &&
          input.mcpCredentialId === "synthetic-credential" &&
          input.providerSessionId === sessionId,
      ),
  });
  const observedSink = Layer.effect(
    EventSink.EventSinkV2,
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      // Observe resource lifetime only; all subscription, replay, and event values are real.
      const stream = ((input: Parameters<typeof sink.stream>[0]) =>
        Stream.unwrap(
          Effect.sync(() => {
            readers++;
            return Stream.unwrap(beforeSubscription.pipe(Effect.as(sink.stream(input)))).pipe(
              Stream.provideService(ResumeSubscription, true),
              Stream.tap(() => {
                const effect = beforeConsume;
                beforeConsume = undefined;
                return effect ?? Effect.void;
              }),
              Stream.ensuring(
                Effect.sync(() => {
                  readers--;
                  if (readers === 0) Queue.offerUnsafe(closed, undefined);
                }),
              ),
            );
          }),
        )) as typeof sink.stream;
      return EventSink.EventSinkV2.of({ ...sink, stream });
    }),
  ).pipe(Layer.provide(replay));
  const observedStarts = Layer.effect(
    GoalStart.GoalStartDispatch,
    Effect.gen(function* () {
      const starts = yield* GoalStart.GoalStartDispatch;
      return GoalStart.GoalStartDispatch.of({
        capture: (id) =>
          starts.capture(id).pipe(Effect.tap(() => Queue.offer(captures, undefined))),
        observe: (id, request) =>
          Effect.sync(() => {
            observeCalls++;
          }).pipe(
            Effect.andThen(
              loseObservation
                ? Effect.succeed({
                    outcome: "unknown" as const,
                    reason: "dispatch_readback_unresolved",
                  })
                : starts.observe(id, request),
            ),
          ),
        start: (input) =>
          Effect.gen(function* () {
            startCalls++;
            yield* typeof beforeStart === "function" ? beforeStart(input) : beforeStart;
            const result = yield* starts.start(input);
            yield* Queue.offer(attempts, undefined);
            return loseObservation
              ? { outcome: "unknown" as const, reason: "dispatch_readback_unresolved" }
              : result;
          }),
      });
    }),
  ).pipe(Layer.provide(GoalStart.layer.pipe(Layer.provide(threads), Layer.provide(replay))));
  const layer = GoalControl.layerFromEventStore.pipe(
    Layer.provide(observedHistory),
    Layer.provide(observedStarts),
    Layer.provide(observedSink),
    Layer.provide(sessions),
    Layer.provide(NodeCrypto.layer),
    Layer.provide(threads),
    Layer.provideMerge(replay),
    Layer.provideMerge(database),
    Layer.provide(signals),
  );
  return {
    layer,
    mutations: () => mutations,
    starts: () => startCalls,
    observations: () => observeCalls,
    waitClosed: Effect.suspend(() => Queue.take(closed)),
    waitCapture: Effect.suspend(() => Queue.take(captures)),
    waitAttempt: Effect.suspend(() => Queue.take(attempts)),
    readers: () => readers,
    afterRegistration: (effect: Effect.Effect<void>) => {
      afterRegistration = effect;
    },
    registrationHighWater: () => registrationHighWater,
    beforeConsume: (effect: Effect.Effect<void>) => {
      beforeConsume = effect;
    },
    beforeReplay: (effect: Effect.Effect<void>) => {
      beforeReplay = effect;
    },
    afterReplay: (effect: Effect.Effect<void>) => {
      afterReplay = effect;
    },
    afterNextReadEffect: (effect: Effect.Effect<void>) => {
      afterNativeRead = effect;
    },
    beforeSubscription: (effect: Effect.Effect<void>) => {
      beforeSubscription = effect;
    },
    goal: (value: OrchestrationV2ProviderGoal | null) => {
      goal = value;
    },
    lose: (value: boolean) => {
      loseObservation = value;
    },
    beforeStart: (effect: Effect.Effect<void> | ((input: StartInput) => Effect.Effect<void>)) => {
      beforeStart = effect;
    },
    afterNextRead: (callback: () => void) => {
      afterRead = callback;
    },
    replaceRuntime: () => {
      runtime = { ...runtime };
    },
    detach: () => {
      attached = false;
    },
    stop: () => {
      stopped = true;
    },
  };
}
const setup = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const sink = yield* EventSink.EventSinkV2;
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make("create"),
    threadId,
    projectId: ProjectId.make("project"),
    title: "Goal",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdBy: "user",
    creationSource: "web",
  });
  yield* orchestrator.dispatch({
    type: "message.dispatch",
    commandId: CommandId.make("owner"),
    messageId: MessageId.make("owner"),
    threadId,
    text: "Finish the project",
    attachments: [],
    createdBy: "user",
    creationSource: "web",
    dispatchMode: { type: "start_immediately" },
  });
  const projection = yield* orchestrator.getThreadProjection(threadId);
  const provider = projection.providerThreads[0]!;
  const owner = {
    ...projection.runs[0]!,
    providerThreadId: provider.id,
    status: "running" as const,
  };
  const now = yield* DateTime.now;
  yield* sink.write({
    events: [
      {
        id: EventId.make("running"),
        type: "run.updated",
        threadId,
        runId: owner.id,
        occurredAt: now,
        payload: owner,
      },
      {
        id: EventId.make("attach"),
        type: "provider-thread.updated",
        threadId,
        occurredAt: now,
        payload: {
          ...provider,
          goal: { objective: "Complete the assigned project", status: "paused", tokensUsed: 42 },
          providerSessionId: sessionId,
          nativeThreadRef: { driver: adapter.driver, nativeId: "native", strength: "strong" },
        },
      },
    ],
  });
  const service = yield* GoalControl.GoalControl;
  const read = yield* service.execute(scope, { action: "read" });
  const input: ThreadGoalInput = {
    action: "resume",
    clientRequestId: "resume",
    controlToken: read.controlToken,
  };
  const complete = Effect.fn("test.complete")(function* () {
    const mark = yield* sink.latestSequence({ threadId });
    yield* sink.write({
      events: [
        {
          id: EventId.make("completed"),
          type: "run.updated",
          threadId,
          runId: owner.id,
          occurredAt: now,
          payload: { ...owner, status: "completed", completedAt: now },
        },
      ],
    });
    return mark;
  });
  let heartbeat = 0;
  const tick = Effect.fn("test.tick")(function* () {
    const thread = (yield* orchestrator.getThreadProjection(threadId)).thread;
    yield* sink.write({
      events: [
        {
          id: EventId.make(`tick:${heartbeat++}`),
          type: "thread.metadata-updated",
          threadId,
          occurredAt: now,
          payload: thread,
        },
      ],
    });
  });
  const stop = Effect.fn("test.stop")(function* () {
    yield* orchestrator.dispatch({
      type: "thread.stop",
      commandId: CommandId.make("stop"),
      threadId,
    });
    const sql = yield* SqlClient.SqlClient;
    const markers = yield* sql<{
      readonly sequence: number;
    }>`SELECT sequence FROM orchestration_events
      WHERE stream_id = ${threadId} AND event_type = 'turn-item.updated'
        AND json_extract(payload_json, '$.type') = 'run_interrupt_request'`;
    expect(markers).toHaveLength(1);
    expect((yield* orchestrator.getThreadProjection(threadId)).runs[0]?.status).toBe("completed");
    return markers[0]!.sequence;
  });
  // Query a receipt without impersonating the ended foreground caller. The fixture shares
  // its namespace deliberately so the public idempotency result can be inspected.
  const receipt = (
    input = {
      action: "resume",
      clientRequestId: "resume",
      controlToken: read.controlToken,
    } as ThreadGoalInput,
  ) =>
    service.execute(
      {
        ...scope,
        thread: undefined,
        client: { sessionId: "synthetic-client", label: "Test", access: "full-access" },
      },
      { ...input, threadId },
    );
  const nextStart = (mark: number) =>
    sink.stream({ threadId, afterSequence: mark }).pipe(
      Stream.filter(({ event }) => event.type === "run.created" && event.payload.id !== owner.id),
      Stream.take(1),
      Stream.runHead,
    );
  return {
    service,
    sink,
    orchestrator,
    owner,
    provider,
    now,
    input,
    complete,
    stop,
    nextStart,
    tick,
    receipt,
  };
});
const makeGate = Effect.gen(function* () {
  const entered = yield* Deferred.make<void>();
  const release = yield* Deferred.make<void>();
  return {
    entered: Deferred.await(entered),
    release: Deferred.succeed(release, undefined),
    hold: Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
  };
});
it.effect("pruned Stop before registration discards a completed owner's resume", () => {
  const fixture = fixtureLayer();
  return Effect.gen(function* () {
    const f = yield* setup;
    const release = yield* Deferred.make<void>();
    fixture.beforeSubscription(Deferred.await(release));
    yield* f.service.execute(scope, f.input);
    yield* f.complete();
    const marker = yield* f.stop();
    yield* f.tick();
    const head = yield* f.sink.latestSequence({ threadId });
    // Deterministic keep-rule deletion bypasses the production retention delay.
    expect(yield* pruneEventLogLikeUpstream(head)).toContain(marker);
    expect(yield* f.sink.latestSequence({ threadId })).toBe(head);
    yield* Deferred.succeed(release, undefined);
    yield* fixture.waitClosed;
    expect(fixture.starts()).toBe(0);
    expect(fixture.readers()).toBe(0);
    expect(yield* f.receipt()).toMatchObject({
      outcome: "stale",
      reason: "resume_discarded_event_history_incomplete",
      replayed: true,
    });
  }).pipe(Effect.scoped, Effect.provide(fixture.layer));
});
it.effect("pruning after registration high water discards replay at or below that water", () => {
  const fixture = fixtureLayer();
  return Effect.gen(function* () {
    const f = yield* setup;
    const subscribe = yield* makeGate;
    const registered = yield* makeGate;
    fixture.beforeSubscription(subscribe.hold);
    fixture.afterRegistration(registered.hold);
    yield* f.service.execute(scope, f.input);
    yield* subscribe.entered;
    yield* f.complete();
    const marker = yield* f.stop();
    yield* f.tick();
    const head = yield* f.sink.latestSequence({ threadId });
    yield* subscribe.release;
    yield* registered.entered;
    expect(fixture.registrationHighWater()).toBe(head);
    expect(marker).toBeLessThanOrEqual(head);
    expect(yield* pruneEventLogLikeUpstream(head)).toContain(marker);
    yield* registered.release;
    yield* fixture.waitClosed;
    expect(fixture.starts()).toBe(0);
    expect(fixture.readers()).toBe(0);
    expect(yield* f.receipt()).toMatchObject({
      outcome: "stale",
      reason: "resume_discarded_event_history_incomplete",
    });
  }).pipe(Effect.scoped, Effect.provide(fixture.layer));
});

it.effect.each(["before storage read", "after collection"] as const)(
  "pruning during finite replay %s is checked before processing or cursor advance",
  (stage) => {
    const fixture = fixtureLayer();
    return Effect.gen(function* () {
      const f = yield* setup;
      const subscribe = yield* makeGate;
      const replaying = yield* makeGate;
      fixture.beforeSubscription(subscribe.hold);
      if (stage === "before storage read") fixture.beforeReplay(replaying.hold);
      else fixture.afterReplay(replaying.hold);
      yield* f.service.execute(scope, f.input);
      yield* subscribe.entered;
      yield* f.complete();
      const marker = yield* f.stop();
      yield* f.tick();
      const head = yield* f.sink.latestSequence({ threadId });
      yield* subscribe.release;
      yield* replaying.entered;
      expect(yield* pruneEventLogLikeUpstream(head)).toContain(marker);
      yield* replaying.release;
      yield* fixture.waitClosed;
      expect(fixture.starts()).toBe(0);
      expect(fixture.readers()).toBe(0);
      // Even a collected Stop is processed only after the post-read completeness proof.
      expect(yield* f.receipt()).toMatchObject({
        outcome: "stale",
        reason: "resume_discarded_event_history_incomplete",
      });
    }).pipe(Effect.scoped, Effect.provide(fixture.layer));
  },
);

it.effect("a buffered live Stop missing from the later sweep cannot authorize resume", () => {
  const fixture = fixtureLayer();
  return Effect.gen(function* () {
    const f = yield* setup;
    const registered = yield* makeGate;
    const consuming = yield* makeGate;
    fixture.afterRegistration(registered.hold);
    yield* f.service.execute(scope, f.input);
    yield* registered.entered;
    fixture.beforeConsume(consuming.hold);
    yield* registered.release;
    yield* f.complete();
    yield* consuming.entered;
    const marker = yield* f.stop();
    yield* f.tick();
    expect(yield* pruneEventLogLikeUpstream(yield* f.sink.latestSequence({ threadId }))).toContain(
      marker,
    );
    yield* consuming.release;
    yield* fixture.waitClosed;
    expect(fixture.starts()).toBe(0);
    expect(fixture.readers()).toBe(0);
    expect(yield* f.receipt()).toMatchObject({
      outcome: "stale",
      reason: "resume_discarded_event_history_incomplete",
    });
  }).pipe(Effect.scoped, Effect.provide(fixture.layer));
});

it.effect("a pruning boundary equal to the original cursor allows complete replay", () => {
  const fixture = fixtureLayer();
  return Effect.gen(function* () {
    const f = yield* setup;
    const cursor = yield* f.sink.latestSequence({ threadId });
    yield* pruneEventLogLikeUpstream(cursor);
    yield* f.service.execute(scope, f.input);
    const mark = yield* f.complete();
    yield* f.nextStart(mark);
    yield* fixture.waitClosed;
    expect(fixture.starts()).toBe(1);
    expect(yield* f.receipt()).toMatchObject({ outcome: "accepted" });
  }).pipe(Effect.scoped, Effect.provide(fixture.layer));
});

it.effect("an unchanged thread high water needs no replay despite a later global boundary", () => {
  const fixture = fixtureLayer();
  return Effect.gen(function* () {
    const f = yield* setup;
    const native = yield* makeGate;
    yield* f.service.execute(scope, f.input);
    fixture.afterNextReadEffect(native.hold);
    const mark = yield* f.complete();
    yield* native.entered;
    const head = yield* f.sink.latestSequence({ threadId });
    yield* f.orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("other"),
      threadId: ThreadId.make("other"),
      projectId: ProjectId.make("project"),
      title: "Other",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    const globalHead = yield* f.sink.latestSequence();
    expect(globalHead).toBeGreaterThan(head);
    yield* pruneEventLogLikeUpstream(globalHead);
    expect(yield* f.sink.latestSequence({ threadId })).toBe(head);
    yield* native.release;
    yield* f.nextStart(mark);
    yield* fixture.waitClosed;
    expect(fixture.starts()).toBe(1);
  }).pipe(Effect.scoped, Effect.provide(fixture.layer));
});

it.effect("the second sweep catches history pruned during the fresh native read", () => {
  const fixture = fixtureLayer();
  return Effect.gen(function* () {
    const f = yield* setup;
    const native = yield* makeGate;
    yield* f.service.execute(scope, f.input);
    fixture.afterNextReadEffect(native.hold);
    yield* f.complete();
    yield* native.entered;
    const marker = yield* f.stop();
    yield* f.tick();
    expect(yield* pruneEventLogLikeUpstream(yield* f.sink.latestSequence({ threadId }))).toContain(
      marker,
    );
    yield* native.release;
    yield* fixture.waitClosed;
    expect(fixture.starts()).toBe(0);
    expect(yield* f.receipt()).toMatchObject({
      reason: "resume_discarded_event_history_incomplete",
    });
  }).pipe(Effect.scoped, Effect.provide(fixture.layer));
});

it.effect("a regressed high water discards intent rather than assuming no new events", () => {
  const fixture = fixtureLayer();
  return Effect.gen(function* () {
    const f = yield* setup;
    const consuming = yield* makeGate;
    const cursor = yield* f.sink.latestSequence({ threadId });
    fixture.beforeConsume(consuming.hold);
    yield* f.service.execute(scope, f.input);
    yield* f.complete();
    yield* consuming.entered;
    const sql = yield* SqlClient.SqlClient;
    // Deliberately model an out-of-band reset; normal pruning retains stream heads.
    yield* sql`DELETE FROM orchestration_events WHERE stream_id = ${threadId} AND sequence >= ${cursor}`;
    expect(yield* f.sink.latestSequence({ threadId })).toBeLessThan(cursor);
    yield* consuming.release;
    yield* fixture.waitClosed;
    expect(fixture.starts()).toBe(0);
    expect(yield* f.receipt()).toMatchObject({
      reason: "resume_discarded_event_history_incomplete",
    });
  }).pipe(Effect.scoped, Effect.provide(fixture.layer));
});

it.effect.each(["pre-read", "post-read"] as const)(
  "%s boundary failure disposes intent",
  (stage) => {
    const fixture = fixtureLayer();
    return Effect.gen(function* () {
      const f = yield* setup;
      const reading = yield* makeGate;
      if (stage === "pre-read") fixture.beforeConsume(reading.hold);
      else fixture.beforeReplay(reading.hold);
      yield* f.service.execute(scope, f.input);
      yield* f.complete();
      yield* reading.entered;
      const sql = yield* SqlClient.SqlClient;
      yield* sql`DROP TABLE orchestration_v2_projection_metadata`;
      yield* reading.release;
      yield* fixture.waitClosed;
      expect(fixture.starts()).toBe(0);
      expect(fixture.readers()).toBe(0);
      expect(yield* f.receipt()).toMatchObject({
        reason: "resume_discarded_event_subscription_failed",
      });
    }).pipe(Effect.scoped, Effect.provide(fixture.layer));
  },
);

it.effect.each(["pruned", "boundary unavailable"] as const)(
  "%s history after an unknown start preserves uncertainty and never resends",
  (change) => {
    const fixture = fixtureLayer();
    return Effect.gen(function* () {
      const f = yield* setup;
      yield* f.service.execute(scope, f.input);
      fixture.lose(true);
      const mark = yield* f.complete();
      yield* f.nextStart(mark);
      expect(yield* f.receipt()).toMatchObject({ outcome: "unknown" });
      const consuming = yield* makeGate;
      fixture.beforeConsume(consuming.hold);
      yield* f.tick();
      yield* consuming.entered;
      // A buffered post-start event may already have been swept. Add a later head so
      // this evaluation must prove a nonempty interval, including the failing read.
      yield* f.tick();
      if (change === "pruned") {
        yield* pruneEventLogLikeUpstream(yield* f.sink.latestSequence({ threadId }));
      } else {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`DROP TABLE orchestration_v2_projection_metadata`;
      }
      yield* consuming.release;
      yield* fixture.waitClosed;
      expect(yield* f.receipt()).toMatchObject({
        outcome: "unknown",
        reason:
          change === "pruned"
            ? "resume_discarded_event_history_incomplete"
            : "resume_discarded_event_subscription_failed",
      });
      expect(yield* f.receipt({ ...f.input, clientRequestId: "new-key" })).toMatchObject({
        outcome: "unknown",
        reason: "prior_mutation_unresolved",
      });
      if (change === "pruned") yield* f.tick();
      expect(fixture.starts()).toBe(1);
      expect(fixture.readers()).toBe(0);
    }).pipe(Effect.scoped, Effect.provide(fixture.layer));
  },
);

it.effect.each(["paused", "blocked"] as const)(
  "arms %s self-resume without native mutation and starts once at idle",
  (status) => {
    const fixture = fixtureLayer(status);
    return Effect.gen(function* () {
      const f = yield* setup;
      const state = yield* f.orchestrator.getThreadProjection(threadId);
      expect((yield* f.orchestrator.getThreadShell(threadId))?.activeRunId).toBe(f.owner.id);
      expect(state.runs[0]?.providerThreadId).toBe(f.provider.id);
      // Closing an invocation scope must not close the application's listener.
      expect(yield* Effect.scoped(f.service.execute(scope, f.input))).toMatchObject({
        outcome: "busy",
        reason: "resume_armed_session_local_until_idle",
      });
      expect(fixture.mutations()).toBe(0);
      expect((yield* f.orchestrator.getThreadProjection(threadId)).runs).toHaveLength(1);
      const mark = yield* f.complete();
      expect(Option.isSome(yield* f.nextStart(mark))).toBe(true);
      expect((yield* f.orchestrator.getThreadProjection(threadId)).runs).toHaveLength(2);
      yield* fixture.waitClosed;
      expect(fixture.readers()).toBe(0);
      expect(yield* f.receipt()).toMatchObject({ outcome: "accepted", replayed: true });
    }).pipe(Effect.scoped, Effect.provide(fixture.layer));
  },
);

it.effect.each(["pause", "clear", "replace", "resume"] as const)(
  "a later %s disposes the previous key, even with unchanged state",
  (action) => {
    const fixture = fixtureLayer();
    return Effect.gen(function* () {
      const f = yield* setup;
      yield* f.service.execute(scope, f.input);
      const newer = {
        ...f.input,
        action,
        clientRequestId: "newer",
        ...(action === "replace" ? { objective: "Complete the assigned project" } : {}),
      };
      yield* f.service.execute(scope, newer);
      yield* fixture.waitClosed;
      expect(yield* f.receipt()).toMatchObject({
        outcome: "stale",
        reason: "resume_discarded_control_superseded",
        replayed: true,
      });
      const mark = yield* f.complete();
      if (action === "resume") {
        yield* f.nextStart(mark);
        yield* fixture.waitClosed;
        expect(fixture.starts()).toBe(1);
      } else expect(fixture.starts()).toBe(0);
    }).pipe(Effect.scoped, Effect.provide(fixture.layer));
  },
);

it.effect.each(["failed", "cancelled", "interrupted", "rolled_back"] as const)(
  "disposes a caller ending %s",
  (status) => {
    const fixture = fixtureLayer();
    return Effect.gen(function* () {
      const f = yield* setup;
      yield* f.service.execute(scope, f.input);
      yield* f.sink.write({
        events: [
          {
            id: EventId.make("end"),
            type: "run.updated",
            threadId,
            runId: f.owner.id,
            occurredAt: f.now,
            payload: { ...f.owner, status, completedAt: f.now },
          },
        ],
      });
      yield* fixture.waitClosed;
      expect(yield* f.receipt()).toMatchObject({
        outcome: "stale",
        reason: "resume_discarded_caller_did_not_complete",
      });
      expect(fixture.starts()).toBe(0);
    }).pipe(Effect.scoped, Effect.provide(fixture.layer));
  },
);

it.effect.each([
  "binding",
  "session",
  "credential",
  "retirement",
  "runtime",
  "archive",
  "owner",
  "stop",
] as const)("disposes on %s and releases the listener", (change) => {
  const fixture = fixtureLayer();
  return Effect.gen(function* () {
    const f = yield* setup;
    yield* f.service.execute(scope, f.input);
    if (change === "credential" || change === "retirement" || change === "runtime") {
      if (change === "credential") fixture.detach();
      else if (change === "runtime") fixture.replaceRuntime();
      else fixture.stop();
      yield* f.tick();
    } else if (change === "binding" || change === "session") {
      yield* f.sink.write({
        events: [
          {
            id: EventId.make("changed"),
            type: "provider-thread.updated",
            threadId,
            occurredAt: f.now,
            payload: {
              ...f.provider,
              providerSessionId:
                change === "session" ? ProviderSessionId.make("different") : sessionId,
              nativeThreadRef: {
                driver: adapter.driver,
                nativeId: "different",
                strength: "strong",
              },
            },
          },
        ],
      });
    } else if (change === "owner") {
      yield* f.orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("new-owner"),
        messageId: MessageId.make("new-owner"),
        threadId,
        text: "Different owner instruction",
        attachments: [],
        createdBy: "user",
        creationSource: "web",
        dispatchMode: { type: "queue_after_active" },
      });
    } else if (change === "stop") {
      yield* f.orchestrator.dispatch({
        type: "run.interrupt",
        commandId: CommandId.make("stop"),
        threadId,
        runId: f.owner.id,
      });
    } else {
      const thread = (yield* f.orchestrator.getThreadProjection(threadId)).thread;
      yield* f.sink.write({
        events: [
          {
            id: EventId.make("archive"),
            type: "thread.archived",
            threadId,
            occurredAt: f.now,
            payload: { ...thread, archivedAt: f.now },
          },
        ],
      });
    }
    yield* fixture.waitClosed;
    expect(fixture.readers()).toBe(0);
    expect(fixture.starts()).toBe(0);
  }).pipe(Effect.scoped, Effect.provide(fixture.layer));
});

it.effect(
  "waits for session idle after caller completion and still rejects direct ended-caller mutations",
  () => {
    const fixture = fixtureLayer();
    return Effect.gen(function* () {
      const f = yield* setup;
      const session = {
        id: sessionId,
        driver: adapter.driver,
        providerInstanceId: instanceId,
        cwd: process.cwd(),
        model: "test",
        capabilities: CodexProviderCapabilitiesV2,
        createdAt: f.now,
        updatedAt: f.now,
        lastError: null,
      };
      yield* f.sink.write({
        events: [
          {
            id: EventId.make("session-running"),
            type: "provider-session.attached",
            threadId,
            occurredAt: f.now,
            payload: { ...session, status: "running" },
          },
        ],
      });
      yield* f.service.execute(scope, f.input);
      const mark = yield* f.complete();
      yield* fixture.waitCapture;
      expect(fixture.starts()).toBe(0);
      expect(
        yield* f.service
          .execute(scope, { ...f.input, clientRequestId: "direct-ended" })
          .pipe(Effect.flip),
      ).toMatchObject({ code: "parent_not_active" });
      yield* f.sink.write({
        events: [
          {
            id: EventId.make("session-ready"),
            type: "provider-session.updated",
            threadId,
            occurredAt: f.now,
            payload: { ...session, status: "ready" },
          },
        ],
      });
      yield* f.nextStart(mark);
      yield* fixture.waitClosed;
      expect(fixture.starts()).toBe(1);
    }).pipe(Effect.scoped, Effect.provide(fixture.layer));
  },
);

it.effect.each(["active", "changed", "missing"] as const)(
  "fresh native %s state determines consumption",
  (change) => {
    const fixture = fixtureLayer();
    return Effect.gen(function* () {
      const f = yield* setup;
      yield* f.service.execute(scope, f.input);
      fixture.goal(
        change === "missing"
          ? null
          : {
              objective:
                change === "changed" ? "Different objective" : "Complete the assigned project",
              status: "active",
              tokensUsed: 42,
            },
      );
      yield* f.complete();
      yield* fixture.waitClosed;
      expect(fixture.starts()).toBe(0);
      expect(yield* f.receipt()).toMatchObject({
        outcome: change === "active" ? "observed" : "stale",
        reason:
          change === "active" ? "resume_satisfied_by_native_read" : "resume_discarded_goal_changed",
      });
    }).pipe(Effect.scoped, Effect.provide(fixture.layer));
  },
);

it.effect("unknown starts are observed under the original request without resend", () => {
  const fixture = fixtureLayer();
  return Effect.gen(function* () {
    const f = yield* setup;
    yield* f.service.execute(scope, f.input);
    fixture.lose(true);
    const mark = yield* f.complete();
    yield* f.nextStart(mark);
    // Receipt query serializes behind consumption and reconciles the same exact command.
    expect(yield* f.receipt()).toMatchObject({ outcome: "unknown" });
    fixture.lose(false);
    yield* f.tick();
    yield* fixture.waitClosed;
    expect(fixture.starts()).toBe(1);
    expect(fixture.observations()).toBeGreaterThan(0);
    expect(yield* f.receipt()).toMatchObject({ outcome: "accepted" });
  }).pipe(Effect.scoped, Effect.provide(fixture.layer));
});

it.effect("a racing owner admission cannot bypass the real guard", () => {
  const fixture = fixtureLayer();
  return Effect.gen(function* () {
    const f = yield* setup;
    yield* f.service.execute(scope, f.input);
    const message = (yield* f.orchestrator.getThreadProjection(threadId)).messages[0]!;
    const raced = {
      ...f.owner,
      id: RunId.make("racing-owner"),
      ordinal: 2,
      userMessageId: MessageId.make("racing-owner"),
    };
    fixture.beforeStart(
      f.sink
        .write({
          events: [
            {
              id: EventId.make("racing-owner-run"),
              type: "run.created",
              threadId,
              runId: raced.id,
              occurredAt: f.now,
              payload: raced,
            },
            {
              id: EventId.make("racing-owner-message"),
              type: "message.updated",
              threadId,
              runId: raced.id,
              occurredAt: f.now,
              payload: { ...message, id: raced.userMessageId, runId: raced.id },
            },
          ],
        })
        .pipe(Effect.orDie, Effect.asVoid),
    );
    yield* f.complete();
    yield* fixture.waitAttempt;
    yield* fixture.waitClosed;
    expect(fixture.starts()).toBe(1);
    expect(
      (yield* f.orchestrator.getThreadProjection(threadId)).runs.filter((run) =>
        run.userMessageId.startsWith("goal:"),
      ),
    ).toHaveLength(0);
  }).pipe(Effect.scoped, Effect.provide(fixture.layer));
});

it.effect("persisted server result wakes preserve the intent through their own run", () => {
  const fixture = fixtureLayer();
  return Effect.gen(function* () {
    const f = yield* setup;
    yield* f.service.execute(scope, f.input);
    const message = (yield* f.orchestrator.getThreadProjection(threadId)).messages[0]!;
    const wake = {
      ...f.owner,
      id: RunId.make("result-wake"),
      ordinal: 2,
      userMessageId: MessageId.make("result-wake"),
    };
    yield* f.sink.write({
      events: [
        {
          id: EventId.make("wake-run"),
          type: "run.created",
          threadId,
          runId: wake.id,
          occurredAt: f.now,
          payload: wake,
        },
        {
          id: EventId.make("wake-message"),
          type: "message.updated",
          threadId,
          runId: wake.id,
          occurredAt: f.now,
          payload: {
            ...message,
            id: wake.userMessageId,
            runId: wake.id,
            createdBy: "agent",
            creationSource: "server",
            notification: {
              source: { kind: "background_task" },
              outcome: "completed",
              summary: "Result",
            },
          },
        },
      ],
    });
    yield* f.complete();
    yield* fixture.waitCapture;
    expect(fixture.starts()).toBe(0);
    const mark = yield* f.sink.latestSequence({ threadId });
    yield* f.sink.write({
      events: [
        {
          id: EventId.make("wake-complete"),
          type: "run.updated",
          threadId,
          runId: wake.id,
          occurredAt: f.now,
          payload: { ...wake, status: "completed", completedAt: f.now },
        },
      ],
    });
    yield* f.nextStart(mark);
    yield* fixture.waitClosed;
    expect(fixture.starts()).toBe(1);
  }).pipe(Effect.scoped, Effect.provide(fixture.layer));
});

it.effect("duplicate requests and terminal updates produce one correlated start", () => {
  const fixture = fixtureLayer();
  return Effect.gen(function* () {
    const f = yield* setup;
    const armed = yield* f.service.execute(scope, f.input);
    expect(yield* f.service.execute(scope, f.input)).toEqual({ ...armed, replayed: true });
    expect(
      yield* f.service.execute(scope, { ...f.input, action: "pause" }).pipe(Effect.flip),
    ).toMatchObject({ code: "invalid_request" });
    const mark = yield* f.complete();
    yield* f.sink.write({
      events: [
        {
          id: EventId.make("completed-again"),
          type: "run.updated",
          threadId,
          runId: f.owner.id,
          occurredAt: f.now,
          payload: { ...f.owner, status: "completed", completedAt: f.now },
        },
      ],
    });
    yield* f.nextStart(mark);
    yield* fixture.waitClosed;
    yield* f.tick();
    expect(
      (yield* f.orchestrator.getThreadProjection(threadId)).runs.filter((run) =>
        run.userMessageId.startsWith("goal:"),
      ),
    ).toHaveLength(1);
  }).pipe(Effect.scoped, Effect.provide(fixture.layer));
});

it.effect("application scope closure releases a still-armed subscription", () => {
  const fixture = fixtureLayer();
  return Effect.gen(function* () {
    const f = yield* setup;
    yield* f.service.execute(scope, f.input);
    yield* f.tick();
    yield* fixture.waitCapture;
    expect(fixture.readers()).toBe(1);
  }).pipe(
    Effect.scoped,
    Effect.provide(fixture.layer),
    Effect.tap(
      Effect.sync(() => {
        expect(fixture.readers()).toBe(0);
        expect(fixture.starts()).toBe(0);
      }),
    ),
  );
});

it.effect("caps only proved rejected admissions at three attempts", () => {
  const fixture = fixtureLayer();
  return Effect.gen(function* () {
    const f = yield* setup;
    yield* f.service.execute(scope, f.input);
    // Persist a proved no-effect refusal at the admission boundary. The actual start
    // service and guard read the real immutable command receipt on every attempt.
    fixture.beforeStart((input) =>
      f.sink
        .commitRejectedCommand({
          commandId: CommandId.make(`goal:${input.requestId}`),
          threadId,
          commandType: "message.dispatch",
          rejectedAt: f.now,
          error: "synthetic no-effect refusal",
        })
        .pipe(Effect.orDie, Effect.asVoid),
    );
    yield* f.complete();
    for (let attempt = 1; attempt <= 3; attempt++) {
      yield* fixture.waitAttempt;
      yield* f.receipt();
      expect(fixture.starts()).toBe(attempt);
      if (attempt < 3) yield* f.tick();
    }
    yield* fixture.waitClosed;
    expect(yield* f.receipt()).toMatchObject({
      outcome: "stale",
      reason: "resume_discarded_dispatch_attempts_exhausted",
    });
    expect(fixture.readers()).toBe(0);
    expect(fixture.starts()).toBe(3);
  }).pipe(Effect.scoped, Effect.provide(fixture.layer));
});

it.effect("captured capability and mode ceilings remain enforced after the caller ends", () => {
  const fixture = fixtureLayer();
  return Effect.gen(function* () {
    const f = yield* setup;
    const invocation = { ...scope, capabilities: new Set(scope.capabilities) };
    yield* f.service.execute(invocation, f.input);
    invocation.capabilities.clear();
    yield* f.complete();
    yield* fixture.waitClosed;
    expect(fixture.starts()).toBe(0);
    expect(yield* f.receipt()).toMatchObject({
      reason: "resume_discarded_binding_or_access_changed",
    });
  }).pipe(Effect.scoped, Effect.provide(fixture.layer));
});

it.effect("a deferred start cannot exceed its captured runtime mode", () => {
  const fixture = fixtureLayer();
  return Effect.gen(function* () {
    const f = yield* setup;
    const thread = (yield* f.orchestrator.getThreadProjection(threadId)).thread;
    yield* f.sink.write({
      events: [
        {
          id: EventId.make("lower-mode"),
          type: "thread.metadata-updated",
          threadId,
          occurredAt: f.now,
          payload: { ...thread, runtimeMode: "auto" },
        },
      ],
    });
    yield* f.service.execute(scope, f.input);
    yield* f.sink.write({
      events: [
        {
          id: EventId.make("raise-mode"),
          type: "thread.metadata-updated",
          threadId,
          occurredAt: f.now,
          payload: thread,
        },
      ],
    });
    yield* fixture.waitClosed;
    expect(fixture.starts()).toBe(0);
    expect(yield* f.receipt()).toMatchObject({
      reason: "resume_discarded_binding_or_access_changed",
    });
  }).pipe(Effect.scoped, Effect.provide(fixture.layer));
});

it.effect("attachment loss during the fresh native read disposes without admission", () => {
  const fixture = fixtureLayer();
  return Effect.gen(function* () {
    const f = yield* setup;
    yield* f.service.execute(scope, f.input);
    fixture.afterNextRead(fixture.detach);
    yield* f.complete();
    yield* fixture.waitClosed;
    expect(fixture.starts()).toBe(0);
    expect(yield* f.receipt()).toMatchObject({
      reason: "resume_discarded_binding_or_access_changed",
    });
  }).pipe(Effect.scoped, Effect.provide(fixture.layer));
});
