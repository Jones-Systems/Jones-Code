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
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
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
const replay = Replay.layerWithRegistry(
  { name: "armed-goal-resume" },
  Registry.layerFromAdapters([adapter]),
  {
    databaseLayer: database,
    runEffectWorker: false,
    serverConfigLayer: ServerConfig.layerTest(process.cwd(), { prefix: "armed-goal-resume-" }).pipe(
      Layer.provide(NodeServices.layer),
      Layer.orDie,
    ),
  },
);
const threads = Layer.effect(
  Threads.ThreadManagementService,
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    return Threads.ThreadManagementService.of({
      getThreadShell: orchestrator.getThreadShell,
      getThreadRecords: orchestrator.getThreadRecords,
      getProjectThreadRecords: (
        input: Parameters<Threads.ThreadManagementService["Service"]["getProjectThreadRecords"]>[0],
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
            return sink.stream(input).pipe(
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
  const layer = GoalControl.layer.pipe(
    Layer.provide(EventStore.layer.pipe(Layer.provide(database))),
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
    nextStart,
    tick,
    receipt,
  };
});
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
