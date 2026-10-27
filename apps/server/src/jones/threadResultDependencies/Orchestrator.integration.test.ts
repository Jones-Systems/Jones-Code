import { assert, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import {
  CommandId,
  NodeId,
  ProviderDriverKind,
  type OrchestrationV2Subagent,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
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

import { runtime } from "./runtimeTestkit.ts";

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

it.effect(
  "eventless Stop commits the release with its receipt and replay cannot release newer work",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      yield* f.send("eventless-stop");
      const command = {
        type: "thread.stop" as const,
        commandId: CommandId.make("stop-eventless"),
        threadId: f.waiter.id,
      };
      const accepted = yield* orchestrator.dispatch(command);
      assert.deepEqual(accepted.storedEvents, []);
      assert.equal((yield* f.store.get("dependency:eventless-stop"))?.state, "released");
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
      // Transparent instrumentation waits for the real consumer; neither finalization API is stubbed.
      const signal = <A, E, R>(id: typeof child.id, effect: Effect.Effect<A, E, R>) =>
        id !== child.id
          ? effect
          : effect.pipe(
              Effect.tap(() =>
                orchestrator
                  .restartContinuationPending(interrupted)
                  .pipe(Effect.flatMap((pending) => Queue.offer(finalized, pending))),
              ),
            );
      const observed = Layer.succeed(Orchestrator.OrchestratorV2, {
        ...orchestrator,
        finalizeDelegatedTask: (id) => signal(id, orchestrator.finalizeDelegatedTask(id)),
        recoverDelegatedTask: (id, runId) =>
          signal(id, orchestrator.recoverDelegatedTask(id, runId)),
      });
      yield* Effect.gen(function* () {
        const reactor = yield* Reactor.DependencyReactor;
        yield* reactor.start();
        // A run update on a child with no dependencies still reaches the reactor.
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
        assert.isTrue(yield* Queue.take(finalized));
        const pending = yield* f.projection.getThreadRecords(f.receiver.id, [
          "subagents",
          "contextTransfers",
        ]);
        assert.equal(pending.subagents.find((item) => item.id === taskId)?.result, null);
        assert.isFalse(pending.contextTransfers.some((item) => item.type === "subagent_result"));
        assert.isTrue(yield* orchestrator.delegatedTaskResultPending(child.id));
        yield* outbox.cancelUnsettled({
          threadId: child.id,
          effectTypes: ["provider-runtime.continue"],
          reason: "Continuation declined",
        });
        assert.isFalse(yield* orchestrator.restartContinuationPending(interrupted));
        yield* changes.publish({ threadIds: [child.id], sequence: yield* f.store.sequence });
        yield* Stream.fromQueue(finalized).pipe(
          Stream.filter((pending) => !pending),
          Stream.runHead,
        );
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
