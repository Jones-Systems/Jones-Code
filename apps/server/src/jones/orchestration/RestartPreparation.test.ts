import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as Sqlite from "../../persistence/Sqlite.ts";
import type * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import * as RestartPreparation from "./RestartPreparation.ts";
import { SteerOutcomeUnknown } from "./LateSteer.ts";

const stores = Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provideMerge(Sqlite.layerMemory),
);
const eventLayer = EventSink.layer.pipe(Layer.provideMerge(stores));
const testLayer = RestartPreparation.layer.pipe(Layer.provideMerge(eventLayer));

const fixture = (includeOldRoot = false) => Effect.gen(function* () {
  const sink = yield* EventSink.EventSinkV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const preparation = yield* RestartPreparation.RestartPreparation;
  const now = yield* DateTime.now;
  const threadId = ThreadId.make("thread:restart-preparation");
  const runId = RunId.make("run:restart-preparation");
  const oldAttemptId = RunAttemptId.make("attempt:old");
  const attemptId = RunAttemptId.make("attempt:new");
  const nodeId = NodeId.make("node:new");
  const providerTurnId = ProviderTurnId.make("turn:old");
  const providerThreadId = ProviderThreadId.make("provider-thread:restart");
  const providerSessionId = ProviderSessionId.make("session:old");
  const providerInstanceId = ProviderInstanceId.make("instance-old");
  const driver = ProviderDriverKind.make("codex");
  const modelSelection = { instanceId: providerInstanceId, model: "fixture" };
  const common = { threadId, occurredAt: now };
  const run = {
    id: runId,
    threadId,
    ordinal: 1,
    providerInstanceId,
    modelSelection,
    providerThreadId,
    userMessageId: MessageId.make("message:initial"),
    rootNodeId: nodeId,
    activeAttemptId: attemptId,
    status: "starting" as const,
    requestedAt: now,
    startedAt: null,
    completedAt: null,
    checkpointId: null,
    contextHandoffId: null,
  };
  const attempt = {
    id: attemptId,
    runId,
    attemptOrdinal: 2,
    rootNodeId: nodeId,
    providerInstanceId,
    providerThreadId,
    providerTurnId: null,
    reason: "steering_restart" as const,
    status: "pending" as const,
    providerSettlement: null,
    startedAt: null,
    completedAt: null,
  };
  const node = {
    id: nodeId,
    threadId,
    runId,
    parentNodeId: null,
    rootNodeId: nodeId,
    kind: "root_turn" as const,
    status: "pending" as const,
    countsForRun: true,
    providerThreadId,
    providerTurnId: null,
    nativeItemRef: null,
    runtimeRequestId: null,
    checkpointScopeId: null,
    startedAt: null,
    completedAt: null,
  };
  const oldTurn = {
    id: providerTurnId,
    providerThreadId,
    nodeId: NodeId.make("node:old"),
    runAttemptId: oldAttemptId,
    nativeTurnRef: { driver, nativeId: "native-turn:old", strength: "strong" as const },
    ordinal: 1,
    status: "running" as const,
    startedAt: now,
    completedAt: null,
  };
  const binding = {
    threadId,
    providerThreadId,
    providerSessionId,
    providerInstanceId,
    driver,
    nativeThreadId: "native-thread:old",
    runtimeGeneration: "generation:old",
  };
  const initial: ReadonlyArray<OrchestrationV2DomainEvent> = [
    {
      ...common,
      id: EventId.make("fixture:thread"),
      type: "thread.created",
      payload: {
        id: threadId,
        projectId: ProjectId.make("project:fixture"),
        title: "Fixture",
        providerInstanceId,
        modelSelection,
        createdBy: "user",
        creationSource: "web",
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        activeProviderThreadId: providerThreadId,
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
        deletedAt: null,
      },
    },
    {
      ...common,
      id: EventId.make("fixture:provider-thread"),
      type: "provider-thread.updated",
      payload: {
        id: providerThreadId,
        driver,
        providerInstanceId,
        providerSessionId: ProviderSessionId.make("session:new"),
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: { driver, nativeId: binding.nativeThreadId, strength: "strong" },
        nativeConversationHeadRef: null,
        status: "not_loaded",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
        runtimeIdentity: {
          runtimeGeneration: binding.runtimeGeneration,
          requested: {
            providerInstanceId,
            providerDriver: driver,
            model: "fixture",
            serviceTier: null,
          },
          observed: {
            backend: { status: "unknown" },
            model: { status: "unknown" },
            account: { status: "unknown" },
            serviceTier: { status: "unknown" },
          },
        },
      },
    },
    {
      ...common,
      id: EventId.make("fixture:old-attempt"),
      type: "run-attempt.created",
      payload: {
        ...attempt,
        id: oldAttemptId,
        rootNodeId: oldTurn.nodeId,
        attemptOrdinal: 1,
        providerTurnId,
        status: "superseded",
        startedAt: now,
        completedAt: now,
      },
    },
    {
      ...common,
      id: EventId.make("fixture:old-turn"),
      type: "provider-turn.updated",
      payload: oldTurn,
    },
  ];
  yield* sink.write({ events: initial });
  const commandId = CommandId.make("command:restart-preparation");
  yield* sink.write({
    commandId,
    events: [
      { ...common, id: EventId.make("fixture:run"), type: "run.updated", payload: run },
      {
        ...common,
        id: EventId.make("fixture:attempt"),
        type: "run-attempt.created",
        payload: attempt,
      },
      ...(includeOldRoot ? [{ ...common, id: EventId.make("fixture:old-node"), type: "node.updated" as const,
      payload: { ...node, id: oldTurn.nodeId, rootNodeId: oldTurn.nodeId, status: "interrupted" as const, completedAt: now },
    }] : []),
    { ...common, id: EventId.make("fixture:node"), type: "node.updated", payload: node },
    ],
  });
  const effect = {
    id: "effect:restart-preparation",
    commandId,
    threadId,
    request: {
      type: "provider-turn.restart" as const,
      providerSessionId,
      providerThreadId,
      providerTurnId,
      interruptedAttemptId: oldAttemptId,
      runId,
      sessionTransition: {
        type: "replace" as const,
        replacementProviderSessionId: ProviderSessionId.make("session:new"),
      },
    },
    status: "running" as const,
    attemptCount: 1,
    leaseOwner: "worker",
    leaseExpiresAt: DateTime.formatIso(now),
    availableAt: DateTime.formatIso(now),
    createdAt: DateTime.formatIso(now),
    updatedAt: DateTime.formatIso(now),
    completedAt: null,
    lastError: null,
  } satisfies EffectOutbox.OrchestrationEffectV2;
  const completeOld = sink
    .write({
      events: [
        {
          ...common,
          id: EventId.make("fixture:terminal"),
          type: "provider-turn.updated",
          payload: { ...oldTurn, status: "interrupted", completedAt: now },
        },
      ],
    })
    .pipe(
      Effect.asVoid,
      Effect.mapError(
        (cause) =>
          new RestartPreparation.RestartPreparationError({ threadId, stage: "terminal", cause }),
      ),
    );
  const target = { threadId, ...effect.request };
  return {
    sink,
    projections,
    preparation,
    effect,
    target,
    completeOld,
    run,
    attempt,
    node,
    now,
    binding,
  };
});

it.effect(
  "waits the elapsed terminal budget, observes a racing persisted terminal, and accepts preexisting evidence",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const wait = RestartPreparation.awaitRestartTerminal({
        target: f.target,
        projections: f.projections,
        sink: f.sink,
      });
      const fiber = yield* wait.pipe(Effect.forkScoped);
      yield* TestClock.adjust(29_999);
      assert.isUndefined(fiber.pollUnsafe());
      yield* f.completeOld;
      yield* Fiber.join(fiber);
      yield* wait;
    }).pipe(Effect.provide(testLayer)),
);

it.effect("fails at 30 seconds without terminal evidence", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    const fiber = yield* RestartPreparation.awaitRestartTerminal({
      target: f.target,
      projections: f.projections,
      sink: f.sink,
    }).pipe(Effect.exit, Effect.forkScoped);
    yield* TestClock.adjust(RestartPreparation.RESTART_TERMINAL_TIMEOUT_MS);
    const result = yield* Fiber.join(fiber);
    assert.isTrue(Exit.isFailure(result));
    if (Exit.isFailure(result))
      assert.equal(
        RestartPreparation.findRestartPreparationFailure(result.cause)?._tag,
        "RestartPreparationError",
      );
  }).pipe(Effect.provide(testLayer)),
);

it.effect.each(["interrupt", "detach", "cancel"] as const)(
  "fails only the pending successor after %s and retains old uncertainty",
  (stage) =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const entered = yield* Deferred.make<void>();
      const calls: string[] = [];
      const run = f.preparation.run({
        effect: f.effect,
        interrupt:
          stage === "interrupt"
            ? Effect.fail(
                new RestartPreparation.RestartPreparationError({
                  threadId: f.target.threadId,
                  stage: "interrupt",
                  cause: "ambiguous interrupt",
                }),
              )
            : stage === "cancel"
              ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never))
              : f.completeOld,
        detach: Effect.sync(() => calls.push("detach")).pipe(
          Effect.andThen(
            Effect.fail(
              new RestartPreparation.RestartPreparationError({
                threadId: f.target.threadId,
                stage: "detach",
                cause: "unknown detach",
              }),
            ),
          ),
        ),
        start: Effect.sync(() => {
          calls.push("start");
        }),
      });
      if (stage === "cancel") {
        const fiber = yield* run.pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        yield* Fiber.interrupt(fiber);
      } else assert.isTrue(Exit.isFailure(yield* Effect.exit(run)));
      const rows = yield* f.projections.getThreadRecords(f.target.threadId, [
        "runs",
        "attempts",
        "nodes",
        "providerTurns",
      ]);
      assert.equal(rows.runs[0]?.status, "failed");
      assert.equal(rows.attempts.find((a) => a.id === f.attempt.id)?.providerSettlement, null);
      assert.equal(
        rows.attempts.find((a) => a.id === f.target.interruptedAttemptId)?.status,
        "superseded",
      );
      assert.notInclude(calls, "start");
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(f.preparation.assertThreadOpen(f.target.threadId))),
      );
      yield* f.preparation.resolveAfterTeardown({ ...f.binding, runtimeGeneration: "wrong" });
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(f.preparation.assertThreadOpen(f.target.threadId))),
      );
      if (stage === "detach") {
        yield* f.preparation.resolveAfterTeardown(f.binding);
      } else yield* f.completeOld;
      yield* f.preparation.assertThreadOpen(f.target.threadId);
      assert.equal(
        (yield* f.projections.getThreadRecords(f.target.threadId, ["runs"])).runs[0]?.status,
        "failed",
      );
      yield* f.preparation.run({
        effect: f.effect,
        interrupt: Effect.die("no replay"),
        detach: Effect.die("no replay"),
        start: Effect.die("no revival"),
      });
    }).pipe(Effect.provide(testLayer)),
);

it.effect("orders preparation and starts once; a newer successor survives stale cleanup", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    const calls: string[] = [];
    yield* f.preparation.run({
      effect: f.effect,
      interrupt: Effect.sync(() => {
        calls.push("interrupt");
      }).pipe(Effect.andThen(f.completeOld)),
      detach: Effect.sync(() => {
        calls.push("detach");
      }),
      start: Effect.sync(() => {
        calls.push("start");
      }),
    });
    assert.deepEqual(calls, ["interrupt", "detach", "start"]);
    const newer = { ...f.run, activeAttemptId: RunAttemptId.make("attempt:newer") };
    const result = yield* Effect.exit(
      f.preparation.run({
        effect: f.effect,
        interrupt: f.sink
          .write({
            events: [
              {
                id: EventId.make("fixture:newer"),
                type: "run.updated",
                threadId: f.target.threadId,
                occurredAt: f.now,
                payload: newer,
              },
            ],
          })
          .pipe(
            Effect.orDie,
            Effect.andThen(
              Effect.fail(
                new RestartPreparation.RestartPreparationError({
                  threadId: f.target.threadId,
                  stage: "interrupt",
                  cause: "ambiguous interrupt",
                }),
              ),
            ),
          ),
        detach: Effect.die("no detach"),
        start: Effect.die("no start"),
      }),
    );
    assert.isTrue(Exit.isFailure(result));
    assert.deepEqual(
      (yield* f.projections.getThreadRecords(f.target.threadId, ["runs"])).runs[0],
      newer,
    );
  }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "records one bounded steer explanation without changing native settlement or batch identity",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const effect = {
        ...f.effect,
        request: {
          type: "provider-turn.steer-batch" as const,
          providerSessionId: f.target.providerSessionId,
          providerThreadId: f.target.providerThreadId,
          providerTurnId: f.target.providerTurnId,
          messageId: f.run.userMessageId,
          messageIds: [f.run.userMessageId, MessageId.make("message:second")],
        },
      };
      const error = new SteerOutcomeUnknown({
        instanceId: f.binding.providerInstanceId,
        nativeThreadId: f.binding.nativeThreadId,
        nativeTurnId: "native-turn:old",
      });
      yield* f.preparation.recordSteerFailure(effect, error);
      assert.lengthOf(
        (yield* f.projections.getThreadRecords(f.target.threadId, ["turnItems"])).turnItems,
        0,
      );
      yield* f.sink.write({
        events: [
          {
            id: EventId.make("fixture:steer-current"),
            type: "run.updated",
            threadId: f.target.threadId,
            occurredAt: f.now,
            payload: {
              ...f.run,
              activeAttemptId: f.target.interruptedAttemptId,
              status: "running",
            },
          },
        ],
      });
      yield* f.preparation.recordSteerFailure(effect, error);
      yield* f.preparation.recordSteerFailure(effect, error);
      const rows = yield* f.projections.getThreadRecords(f.target.threadId, [
        "turnItems",
        "runs",
        "attempts",
        "providerTurns",
      ]);
      assert.lengthOf(rows.turnItems, 1);
      assert.equal(rows.runs[0]?.status, "running");
      assert.equal(rows.providerTurns[0]?.status, "running");
      assert.equal(rows.attempts.find((a) => a.id === f.attempt.id)?.providerSettlement, null);
    }).pipe(Effect.provide(testLayer)),
);

it.effect("selects the committed successor root when the command records the old root first", () =>
  Effect.gen(function* () {
    const f = yield* fixture(true);
    const calls: string[] = [];
    yield* f.preparation.run({ effect: f.effect,
      interrupt: f.completeOld.pipe(Effect.tap(() => Effect.sync(() => { calls.push("interrupt"); }))),
      detach: Effect.sync(() => { calls.push("detach"); }),
      start: Effect.sync(() => { calls.push("start"); }),
    });
    assert.deepEqual(calls, ["interrupt", "detach", "start"]);
  }).pipe(Effect.provide(testLayer)),
);
