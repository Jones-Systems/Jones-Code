import { assert, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import {
  CommandId,
  NodeId,
  ProviderDriverKind,
  RunId,
  RuntimeRequestId,
  ThreadId,
  type OrchestrationV2Subagent,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Console from "effect/Console";
import * as Deferred from "effect/Deferred";
import * as Exit from "effect/Exit";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Outbox from "../../orchestration-v2/EffectOutbox.ts";
import * as Forwarding from "../../peer/PeerForwarding.ts";
import { readMonitorSettlementBlocker } from "../threadMonitor/guardedSettle.ts";
import * as Changes from "./DependencyChanges.ts";
import * as Reactor from "./DependencyReactor.ts";
import * as Watch from "./RequestWatch.ts";
import * as Fixture from "./testkit.ts";
import { notification } from "./Wake.ts";

import { runtime } from "./runtimeTestkit.ts";

const deliverQueuedWake = Effect.fnUntraced(function* (
  f: Effect.Success<typeof Fixture.seed>,
  key: string,
  ordinal = 2,
  queueHeld = false,
) {
  const input = f.requestInput(key);
  yield* f.send(key, f.receiverRun, {
    ...input,
    localDependency: {
      ...input.localDependency!,
      waiterGeneration: yield* f.store.waiterGeneration(f.waiter.id),
    },
  });
  yield* f.sql.withTransaction(
    f.store.answer(f.receiver.id, input.requestId, "fulfilled", "Delivered result", null),
  );
  const dependency = (yield* f.store.get(`dependency:${key}`))!;
  const wakeRun = f.run(`${key}-wake`, f.waiter.id, "queued", ordinal);
  const wake = {
    ...f.message(`${key}-message`, wakeRun),
    notification: notification(dependency),
    creationSource: "server" as const,
  };
  const run = { ...wakeRun, userMessageId: wake.id, queueHeld };
  yield* f.sink.commitCommand({
    commandId: CommandId.make(`accept:${key}-wake`),
    threadId: f.waiter.id,
    commandType: "message.dispatch",
    acceptedAt: Fixture.time,
    dependencyWake: {
      dependencyId: dependency.dependencyId,
      generation: dependency.generation,
      waiterThreadId: f.waiter.id,
      messageId: wake.id,
    },
    events: [
      f.event({ type: "run.created", threadId: f.waiter.id, runId: run.id, payload: run }),
      f.event({ type: "message.updated", threadId: f.waiter.id, runId: run.id, payload: wake }),
    ],
    effects: [],
  });
  return { dependency, run };
});

it.effect(
  "authoritative manual and automatic settlement reject a completed waiter with unresolved results",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      yield* f.send("settlement");
      const manual = yield* orchestrator
        .dispatch({
          type: "thread.settle",
          commandId: CommandId.make("settle-waiter"),
          threadId: f.waiter.id,
        })
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(manual));
      const automatic = yield* orchestrator
        .dispatch({
          type: "thread.auto-settle",
          commandId: CommandId.make("auto-settle-waiter"),
          threadId: f.waiter.id,
          snapshotAt: Fixture.time,
        })
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(automatic));
      assert.equal((yield* f.projection.getThread(f.waiter.id)).settledAt, null);
      assert.equal((yield* f.store.duties(f.waiter.id)).outgoing, 1);
    }).pipe(Effect.provide(runtime)),
);

it.effect.each([false, true])(
  "eventless Stop commits the release with its receipt and replay cannot release newer work (unreadable child: %s)",
  (unreadableChild) =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      yield* f.send("eventless-stop");
      if (unreadableChild) {
        const childId = ThreadId.make("unreadable-stop-child");
        const childRunId = RunId.make("unreadable-stop-child-run");
        const task: OrchestrationV2Subagent = {
          id: NodeId.make("unreadable-stop-task"),
          threadId: f.waiter.id,
          runId: f.waiterRun.id,
          parentNodeId: NodeId.make("unreadable-stop-parent-node"),
          origin: "app_owned",
          createdBy: "agent",
          driver: ProviderDriverKind.make("codex"),
          providerInstanceId: f.waiter.providerInstanceId,
          providerThreadId: null,
          childThreadId: childId,
          nativeTaskRef: null,
          prompt: "Unreadable delegated child",
          title: null,
          model: null,
          status: "running",
          result: null,
          startedAt: Fixture.time,
          completedAt: null,
          updatedAt: Fixture.time,
        };
        yield* f.sink.write({
          events: [
            f.event({
              type: "thread.created",
              threadId: childId,
              payload: { ...f.receiver, id: childId },
            }),
            f.event({
              type: "run.created",
              threadId: childId,
              runId: childRunId,
              payload: { ...f.receiverRun, id: childRunId, threadId: childId },
            }),
            f.event({ type: "subagent.updated", threadId: f.waiter.id, payload: task }),
          ],
        });
        yield* f.sql`UPDATE orchestration_v2_projection_runs SET payload_json = '{"obsolete":true}' WHERE thread_id = ${childId}`;
      }
      const command = {
        type: "thread.stop" as const,
        commandId: CommandId.make("stop-eventless"),
        threadId: f.waiter.id,
      };
      const accepted = yield* orchestrator.dispatch(command);
      if (!unreadableChild) assert.deepEqual(accepted.storedEvents, []);
      else
        assert.isFalse(
          accepted.storedEvents.some(
            ({ event }) =>
              event.threadId === ThreadId.make("unreadable-stop-child") &&
              (event.type === "run.updated" ||
                event.type === "run-attempt.updated" ||
                event.type === "node.updated") &&
              event.payload.status === "cancelled",
          ),
        );
      assert.equal((yield* f.store.get("dependency:eventless-stop"))?.state, "released");
      if (unreadableChild) {
        const outbox = yield* Outbox.EffectOutboxV2;
        assert.include(
          (yield* outbox.listByCommandId(command.commandId)).map((effect) => effect.request.type),
          "delegated-tasks.stop",
        );
      }
      const epoch = yield* f.store.waiterGeneration(f.waiter.id);
      assert.equal(epoch, 1);
      const next = f.requestInput("new-wait");
      yield* f.send("new-wait", f.receiverRun, {
        ...next,
        localDependency: { ...next.localDependency!, waiterGeneration: epoch },
      });
      const replay = yield* orchestrator.dispatch(command);
      assert.equal(replay.replayed, true);
      assert.equal(yield* f.store.waiterGeneration(f.waiter.id), epoch);
      assert.equal((yield* f.store.get("dependency:new-wait"))?.state, "awaiting");
      assert.equal(
        (yield* f.projection.getThreadRecords(f.receiver.id, ["runs"])).runs[0]?.status,
        "running",
      );
    }).pipe(Effect.provide(runtime)),
);

it.effect("Stop permits Settle after holding a delivered dependency's queued wake", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const { dependency, run: wakeRun } = yield* deliverQueuedWake(f, "delivered-stop");
    assert.equal((yield* f.store.get(dependency.dependencyId))?.state, "delivered");
    assert.equal((yield* f.store.duties(f.waiter.id)).outgoing, 1);
    yield* orchestrator.dispatch({
      type: "thread.stop",
      commandId: CommandId.make("stop:delivered-dependency"),
      threadId: f.waiter.id,
    });
    const stopped = yield* f.projection.getThreadRecords(f.waiter.id, ["runs"]);
    const held = stopped.runs.find((run) => run.id === wakeRun.id)!;
    assert.equal(held.status, "queued");
    assert.isTrue(held.queueHeld);
    assert.equal((yield* f.store.duties(f.waiter.id)).outgoing, 1);
    const settle = {
      type: "thread.settle" as const,
      commandId: CommandId.make("settle:delivered-dependency"),
      threadId: f.waiter.id,
    };
    const accepted = yield* orchestrator.dispatch(settle);
    assert.isNotNull((yield* f.projection.getThread(f.waiter.id)).settledAt);
    assert.equal((yield* f.store.duties(f.waiter.id)).outgoing, 0);
    assert.equal(
      (yield* f.projection.getThreadRecords(f.waiter.id, ["runs"])).runs.find(
        (run) => run.id === wakeRun.id,
      )?.status,
      "cancelled",
    );
    const outbox = yield* Outbox.EffectOutboxV2;
    const acceptedEffects = yield* outbox.listByCommandId(settle.commandId);
    const acceptedReceipt =
      yield* f.sql`SELECT * FROM orchestration_command_receipts WHERE command_id = ${settle.commandId}`;
    assert.equal(acceptedReceipt.length, 1);
    const epoch = yield* f.store.waiterGeneration(f.waiter.id);
    yield* orchestrator.dispatch({
      type: "thread.unsettle",
      commandId: CommandId.make("reopen:delivered-dependency"),
      threadId: f.waiter.id,
      reason: "user",
    });
    const newer = yield* deliverQueuedWake(f, "new-delivered-wake", 3, true);
    const pending = f.requestInput("new-unresolved-wait");
    yield* f.send("new-unresolved-wait", f.receiverRun, {
      ...pending,
      localDependency: { ...pending.localDependency!, waiterGeneration: epoch },
    });
    const sequence = yield* f.store.sequence;
    assert.equal((yield* f.store.duties(f.waiter.id)).outgoing, 2);
    const replay = yield* orchestrator.dispatch(settle);
    assert.equal(replay.replayed, true);
    assert.equal(replay.sequence, accepted.sequence);
    assert.deepEqual(
      replay.storedEvents.map(({ sequence, event }) => [sequence, event.id]),
      accepted.storedEvents.map(({ sequence, event }) => [sequence, event.id]),
    );
    assert.deepEqual(
      yield* f.sql`SELECT * FROM orchestration_command_receipts WHERE command_id = ${settle.commandId}`,
      acceptedReceipt,
    );
    assert.deepEqual(
      yield* f.sql`SELECT * FROM orchestration_events WHERE sequence > ${sequence} AND command_id = ${settle.commandId}`,
      [],
    );
    assert.equal(yield* f.store.waiterGeneration(f.waiter.id), epoch);
    assert.isNull((yield* f.projection.getThread(f.waiter.id)).settledAt);
    assert.equal((yield* f.store.get("dependency:new-unresolved-wait"))?.state, "awaiting");
    assert.equal((yield* f.store.get(newer.dependency.dependencyId))?.state, "delivered");
    assert.equal((yield* f.store.duties(f.waiter.id)).outgoing, 2);
    assert.equal(
      (yield* f.projection.getThreadRecords(f.waiter.id, ["runs"])).runs.find(
        (run) => run.id === newer.run.id,
      )?.status,
      "queued",
    );
    assert.deepEqual(yield* outbox.listByCommandId(settle.commandId), acceptedEffects);
  }).pipe(Effect.provide(runtime)),
);

it.effect.each([
  "queued user",
  "live run",
  "blocking approval",
  "unresolved result",
  "guarded",
  "automatic",
] as const)("settlement with a delivered queued wake refuses %s without cancelling it", (blocker) =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const outbox = yield* Outbox.EffectOutboxV2;
    const { dependency, run: wake } = yield* deliverQueuedWake(f, "blocked-settle");
    if (blocker === "queued user" || blocker === "live run") {
      const run = f.run(
        "blocking-run",
        f.waiter.id,
        blocker === "queued user" ? "queued" : "running",
        3,
      );
      yield* f.sink.write({
        events: [
          f.event({ type: "run.created", threadId: f.waiter.id, runId: run.id, payload: run }),
          f.event({
            type: "message.updated",
            threadId: f.waiter.id,
            runId: run.id,
            payload: f.message(run.userMessageId, run),
          }),
        ],
      });
    } else if (blocker === "blocking approval") {
      yield* f.sink.write({
        events: [
          f.event({
            type: "runtime-request.updated",
            threadId: f.waiter.id,
            payload: {
              id: RuntimeRequestId.make("blocking-approval"),
              nodeId: NodeId.make("approval-node"),
              providerTurnId: null,
              nativeRequestRef: null,
              kind: "permission",
              status: "pending",
              responseCapability: { type: "not_resumable", reason: "Approval required" },
              createdAt: Fixture.time,
              resolvedAt: null,
            },
          }),
        ],
      });
    } else if (blocker === "unresolved result") {
      yield* f.send("blocking-result");
    }
    const commandId = CommandId.make(`settle-blocked:${blocker}`);
    const sequence = yield* f.store.sequence;
    const generation = yield* f.store.waiterGeneration(f.waiter.id);
    const expectedUpdatedAt = DateTime.formatIso(
      (yield* f.projection.getThread(f.waiter.id)).updatedAt,
    );
    const rejected = yield* orchestrator
      .dispatch(
        blocker === "automatic"
          ? {
              type: "thread.auto-settle",
              commandId,
              threadId: f.waiter.id,
              snapshotAt: Fixture.time,
            }
          : {
              type: "thread.settle",
              commandId,
              threadId: f.waiter.id,
              ...(blocker === "guarded"
                ? { guard: { policy: "preserve_pending_work" as const, expectedUpdatedAt } }
                : {}),
            },
      )
      .pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(rejected));
    assert.isNull((yield* f.projection.getThread(f.waiter.id)).settledAt);
    assert.equal(
      (yield* f.projection.getThreadRecords(f.waiter.id, ["runs"])).runs.find(
        (run) => run.id === wake.id,
      )?.status,
      "queued",
    );
    assert.equal((yield* f.store.get(dependency.dependencyId))?.state, "delivered");
    assert.equal(
      (yield* f.store.duties(f.waiter.id)).outgoing,
      blocker === "unresolved result" ? 2 : 1,
    );
    assert.equal(yield* f.store.waiterGeneration(f.waiter.id), generation);
    assert.equal(yield* f.store.sequence, sequence);
    assert.deepEqual(yield* outbox.listByCommandId(commandId), []);
    assert.deepEqual(
      yield* f.sql`SELECT * FROM orchestration_command_receipts WHERE command_id = ${commandId} AND status = 'accepted'`,
      [],
    );
  }).pipe(Effect.provide(runtime)),
);

it.effect("raw delegated completion and guarded settlement inspect descendant result duties", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const taskId = NodeId.make("parent-task");
    const task: OrchestrationV2Subagent = {
      id: taskId,
      threadId: f.receiver.id,
      runId: f.receiverRun.id,
      parentNodeId: NodeId.make("parent-node"),
      origin: "app_owned",
      createdBy: "agent",
      driver: ProviderDriverKind.make("codex"),
      providerInstanceId: f.receiver.providerInstanceId,
      providerThreadId: null,
      childThreadId: f.waiter.id,
      nativeTaskRef: null,
      prompt: "Result dependent child",
      title: null,
      model: null,
      status: "completed",
      result: "Previously completed task",
      startedAt: Fixture.time,
      completedAt: Fixture.time,
      updatedAt: Fixture.time,
    };
    const child = {
      ...f.waiter,
      lineage: {
        rootThreadId: f.receiver.id,
        parentThreadId: f.receiver.id,
        relationshipToParent: "subagent" as const,
      },
      forkedFrom: { type: "node" as const, nodeId: taskId },
      settledAt: Fixture.time,
    };
    yield* f.sink.write({
      events: [
        f.event({ type: "thread.metadata-updated", threadId: f.waiter.id, payload: child }),
        f.event({
          type: "run.updated",
          threadId: f.receiver.id,
          runId: f.receiverRun.id,
          payload: { ...f.receiverRun, status: "completed", completedAt: Fixture.time },
        }),
        f.event({ type: "subagent.updated", threadId: f.receiver.id, payload: task }),
      ],
    });
    yield* f.send("child-waits");
    assert.equal(
      yield* readMonitorSettlementBlocker(
        f.projection,
        f.receiver.id,
        DateTime.formatIso(Fixture.time),
      ),
      "dependency",
    );
    assert.equal(yield* orchestrator.delegatedTaskResultPending(f.waiter.id), true);
    yield* f.sink.write({
      events: [
        f.event({
          type: "subagent.updated",
          threadId: f.receiver.id,
          payload: { ...task, status: "running", result: null, completedAt: null },
        }),
      ],
    });
    yield* orchestrator.finalizeDelegatedTask(f.waiter.id);
    const parent = yield* f.projection.getThreadRecords(f.receiver.id, [
      "subagents",
      "contextTransfers",
    ]);
    assert.equal(parent.subagents.find((item) => item.id === taskId)?.result, null);
    assert.isFalse(parent.contextTransfers.some((transfer) => transfer.type === "subagent_result"));
  }).pipe(Effect.provide(runtime)),
);

it.effect.each(["pending", "running"] as const)(
  "CR-R1: the real reactor cannot finalize an interrupted child with a %s restart continuation",
  (continuationStatus) =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const changes = yield* Changes.DependencyChanges;
      const outbox = yield* Outbox.EffectOutboxV2;
      const taskId = NodeId.make("restarting-child-task");
      const task: OrchestrationV2Subagent = {
        id: taskId,
        threadId: f.receiver.id,
        runId: f.receiverRun.id,
        parentNodeId: NodeId.make("parent-node"),
        origin: "app_owned",
        createdBy: "agent",
        driver: ProviderDriverKind.make("codex"),
        providerInstanceId: f.waiter.providerInstanceId,
        providerThreadId: null,
        childThreadId: f.waiter.id,
        nativeTaskRef: null,
        prompt: "Continue interrupted work",
        title: null,
        model: null,
        status: "running",
        result: null,
        startedAt: Fixture.time,
        completedAt: null,
        updatedAt: Fixture.time,
      };
      const child = {
        ...f.waiter,
        lineage: {
          rootThreadId: f.receiver.id,
          parentThreadId: f.receiver.id,
          relationshipToParent: "subagent" as const,
        },
        forkedFrom: { type: "node" as const, nodeId: taskId },
      };
      const interrupted = { ...f.waiterRun, status: "interrupted" as const };
      yield* f.sink.writeWithEffects({
        commandId: CommandId.make("restart-child-fixture"),
        events: [
          f.event({ type: "thread.metadata-updated", threadId: child.id, payload: child }),
          f.event({ type: "subagent.updated", threadId: f.receiver.id, payload: task }),
          f.event({
            type: "run.updated",
            threadId: child.id,
            runId: interrupted.id,
            payload: interrupted,
          }),
        ],
        effects: [
          {
            id: `effect:restart-continuation:${interrupted.id}`,
            commandId: CommandId.make("restart-child-fixture"),
            threadId: child.id,
            request: { type: "provider-runtime.continue", sourceRunId: interrupted.id },
          },
        ],
      });
      if (continuationStatus === "running") {
        yield* f.sql`UPDATE orchestration_v2_effect_outbox SET status = 'running', lease_owner = 'test-worker'
        WHERE effect_id = ${`effect:restart-continuation:${interrupted.id}`}`;
      }
      assert.isTrue(yield* orchestrator.restartContinuationPending(interrupted));
      const finalized = yield* Queue.unbounded<boolean>();
      const recordedStages = new Set<string>();
      const stage = (value: string) =>
        Effect.suspend(() => {
          if (recordedStages.has(value)) return Effect.void;
          recordedStages.add(value);
          return Effect.logInfo("CR-R1", continuationStatus, child.id, interrupted.id, value).pipe(
            Effect.provideService(Console.Console, Console.Console.defaultValue()),
          );
        });
      // Transparent instrumentation waits for the real consumer; neither API is stubbed.
      let finalizerCalls = 0;
      const signal = <A, E, R>(id: typeof child.id, effect: Effect.Effect<A, E, R>) => {
        if (id !== child.id) return effect;
        const call = ++finalizerCalls;
        const receipt = (phase: string) =>
          call <= 8 ? stage(`finalizer:${call}:${phase}`) : Effect.void;
        return receipt("enter").pipe(
          Effect.andThen(effect),
          Effect.tap(() => receipt("exit")),
          Effect.tap(() =>
            orchestrator
              .restartContinuationPending(interrupted)
              .pipe(
                Effect.flatMap((pending) =>
                  receipt(`pending:${pending}`).pipe(
                    Effect.andThen(Queue.offer(finalized, pending)),
                  ),
                ),
              ),
          ),
        );
      };
      // Delay only consumption of the actual captured stream, never its baseline.
      // The run update commits before this gate opens, forcing real EventSink replay.
      const consumeDomainEvents = yield* Deferred.make<void>();
      let acquired = false;
      let fixturePublished = false;
      const observed = Layer.succeed(Orchestrator.OrchestratorV2, {
        ...orchestrator,
        acquireDomainEvents: stage("domain-acquire-enter").pipe(
          Effect.andThen(orchestrator.acquireDomainEvents),
          Effect.tap(() =>
            Effect.sync(() => {
              assert.isFalse(fixturePublished);
              acquired = true;
            }).pipe(Effect.andThen(stage("domain-acquire-exit"))),
          ),
          Effect.map((stream) =>
            Stream.unwrap(
              stage("domain-consume-enter").pipe(
                Effect.andThen(Deferred.await(consumeDomainEvents)),
                Effect.tap(() => stage("domain-consume-released")),
                Effect.as(
                  stream.pipe(
                    Stream.tap((event) =>
                      event.type === "run.updated" && event.threadId === child.id
                        ? stage("domain-run-received")
                        : Effect.void,
                    ),
                  ),
                ),
              ),
            ),
          ),
        ),
        finalizeDelegatedTask: (id) => signal(id, orchestrator.finalizeDelegatedTask(id)),
        recoverDelegatedTask: (id, runId) =>
          signal(id, orchestrator.recoverDelegatedTask(id, runId)),
      });
      yield* Effect.gen(function* () {
        const reactor = yield* Reactor.DependencyReactor;
        yield* stage("reactor-start-enter");
        yield* reactor.start();
        yield* stage("reactor-start-exit");
        assert.isTrue(acquired);
        // A run update on a child with no dependencies still reaches the reactor.
        yield* stage("sink-write-enter");
        yield* f.sink.write({
          events: [
            f.event({
              type: "run.updated",
              threadId: child.id,
              runId: interrupted.id,
              payload: interrupted,
            }),
          ],
        });
        fixturePublished = true;
        yield* stage("sink-write-exit");
        yield* Deferred.succeed(consumeDomainEvents, undefined);
        yield* stage("first-queue-enter");
        assert.isTrue(yield* Queue.take(finalized));
        yield* stage("first-queue-exit");
        const pending = yield* f.projection.getThreadRecords(f.receiver.id, [
          "subagents",
          "contextTransfers",
        ]);
        assert.equal(pending.subagents.find((item) => item.id === taskId)?.result, null);
        assert.isFalse(pending.contextTransfers.some((item) => item.type === "subagent_result"));
        assert.isTrue(yield* orchestrator.delegatedTaskResultPending(child.id));
        yield* stage("cancel-enter");
        yield* outbox.cancelUnsettled({
          threadId: child.id,
          effectTypes: ["provider-runtime.continue"],
          reason: "Continuation declined",
        });
        yield* stage("cancel-exit");
        assert.isFalse(yield* orchestrator.restartContinuationPending(interrupted));
        yield* stage("changes-publish-enter");
        yield* changes.publish({ threadIds: [child.id], sequence: yield* f.store.sequence });
        yield* stage("changes-publish-exit");
        yield* stage("second-stream-enter");
        yield* Stream.fromQueue(finalized).pipe(
          Stream.filter((pending) => !pending),
          Stream.runHead,
        );
        yield* stage("second-stream-exit");
        const settled = yield* f.projection.getThreadRecords(f.receiver.id, [
          "subagents",
          "contextTransfers",
        ]);
        assert.isNotNull(settled.subagents.find((item) => item.id === taskId)?.result);
        assert.lengthOf(
          settled.contextTransfers.filter((item) => item.type === "subagent_result"),
          1,
        );
      }).pipe(
        Effect.provide(
          Reactor.layer.pipe(
            Layer.provide(Watch.layer),
            Layer.provide(observed),
            Layer.provide(NodeCrypto.layer),
            Layer.provide(
              Layer.mock(Forwarding.PeerForwarding)({
                call: () => Effect.die("This local regression must not call a peer"),
              }),
            ),
          ),
        ),
      );
    }).pipe(Effect.provide(runtime)),
);
