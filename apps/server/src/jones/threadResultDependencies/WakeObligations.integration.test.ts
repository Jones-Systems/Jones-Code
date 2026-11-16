import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  MessageId,
  ProviderThreadId,
  ProviderInstanceId,
  ContextTransferId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Handoff from "../../orchestration-v2/ContextHandoffService.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import { historicalMessage } from "@t3tools/provider-core/server/handoffBudget";
import * as Forwarding from "../../peer/PeerForwarding.ts";
import * as Changes from "./DependencyChanges.ts";
import * as Reactor from "./DependencyReactor.ts";
import * as Watch from "./RequestWatch.ts";
import { makeDependencyStore } from "./DependencyStore.ts";
import { DependencyWakeCommit } from "./DependencyCommit.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
import { notification } from "./Wake.ts";
import * as Fixture from "../startRefusal/integration.testkit.ts";

const layer = Fixture.runtime.pipe(
  Layer.provideMerge(Changes.layer),
  Layer.provideMerge(Handoff.layer.pipe(Layer.provide(IdAllocator.layer))),
);
const prepare = Effect.gen(function* () {
  const f = yield* Fixture.seed;
  const sql = yield* SqlClient.SqlClient;
  const store = makeDependencyStore(sql, makeJonesTablePresence(sql));
  yield* f.sink.write({
    events: [
      yield* f.event({
        type: "run.updated",
        threadId: f.threadId,
        runId: f.run.id,
        payload: { ...f.run, status: "completed" },
      }),
    ],
  });
  yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* store.prepare({
        dependencyId: "peer",
        operationId: "operation:peer",
        waiterThreadId: f.threadId,
        waiterRunId: f.parent.id,
        targetEnvironmentId: "remote",
        targetThreadId: f.threadId,
        targetLabel: "Peer result",
        scope: {
          environmentId: EnvironmentId.make("local"),
          requestNamespace: "peer",
          thread: {
            threadId: f.threadId,
            providerSessionId: "fixture",
            providerInstanceId: f.run.providerInstanceId,
          },
        },
        forwardedRequestKey: "peer",
        requestDigest: "peer",
        waiterGeneration: 0,
      });
      yield* store.resolve("peer", 0, "fulfilled", "PEER_HISTORY_MARKER");
    }),
  );
  const dispatch = (generation: number, key: string) =>
    Effect.gen(function* () {
      const dependency = (yield* store.get("peer"))!;
      const note = notification(dependency);
      const messageId = MessageId.make(`message:peer:${key}`);
      yield* f.orchestrator
        .dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`dispatch:peer:${key}`),
          threadId: f.threadId,
          messageId,
          text: note.detail ?? note.summary,
          notification: note,
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "agent",
          creationSource: "server",
        })
        .pipe(
          Effect.provideService(DependencyWakeCommit, {
            dependencyId: "peer",
            generation,
            waiterThreadId: f.threadId,
            messageId,
          }),
        );
      return (yield* f.orchestrator.getThreadProjection(f.threadId)).runs.find(
        (r) => r.userMessageId === messageId,
      )!;
    });
  const run = yield* dispatch(0, "refused");
  return { ...f, sql, store, dispatch, run };
});

it.effect(
  "real provider-control refusal atomically holds a peer wake and accepted start drives exactly one successor through the reactor",
  () =>
    Effect.gen(function* () {
      const f = yield* prepare;
      yield* f.start.start({ threadId: f.threadId, runId: f.run.id }).pipe(Effect.result);
      assert.equal((yield* f.store.get("peer"))?.wakeHold, "held");
      assert.equal((yield* f.store.duties(f.threadId)).outgoing, 1);
      assert.lengthOf(yield* f.sql`SELECT * FROM jones_peer_wake_bindings`, 1);
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const refused = (yield* f.orchestrator.getThreadProjection(f.threadId)).turnItems.find(
        (item) => item.runId === f.run.id && item.type === "notification",
      );
      assert.isDefined(refused);
      assert.equal(historicalMessage(refused!), null);
      const retry = yield* projectionStore.getTurnStartHistory(f.threadId);
      assert.isFalse(JSON.stringify(retry).includes("PEER_HISTORY_MARKER"));
      const snapshot = yield* f.orchestrator.getThreadProjection(f.threadId);
      const handoff = yield* Handoff.ContextHandoffServiceV2;
      for (const strategy of ["delta_since_target_last_seen", "full_thread_summary"] as const) {
        const assembled = yield* handoff.prepareProviderHandoff({
          threadId: f.threadId,
          targetRunId: f.run.id,
          transferId: ContextTransferId.make(`transfer:${strategy}`),
          fromProviderThreadIds: snapshot.providerThreads.map((t) => t.id),
          toProviderThreadId: ProviderThreadId.make("provider:successor"),
          fromProviderInstanceId: f.run.providerInstanceId,
          toProviderInstanceId: ProviderInstanceId.make("codex"),
          coveredRunOrdinals: { from: 1, to: f.run.ordinal },
          strategy,
          items: snapshot.turnItems,
          runs: snapshot.runs,
          createdAt: f.run.requestedAt,
        });
        assert.isFalse(JSON.stringify(assembled).includes("PEER_HISTORY_MARKER"));
      }
      const before = snapshot.runs.length;
      yield* Effect.gen(function* () {
        yield* (yield* Reactor.DependencyReactor).start();
        assert.equal((yield* f.orchestrator.getThreadProjection(f.threadId)).runs.length, before);
        const changes = yield* Changes.DependencyChanges;
        const updates = yield* changes.subscribe;
        yield* changes.publish({ sequence: yield* f.store.sequence, threadIds: [f.threadId] });
        yield* f.sink.write({
          events: [
            yield* f.event({
              type: "thread.unarchived",
              threadId: f.threadId,
              payload: snapshot.thread,
            }),
            yield* f.event({
              type: "thread.unsettled",
              threadId: f.threadId,
              payload: snapshot.thread,
            }),
            yield* f.event({
              type: "run.updated",
              threadId: f.threadId,
              runId: f.run.id,
              payload: { ...f.run, status: "failed" },
            }),
          ],
        });
        const gate = yield* Fixture.Gate;
        gate.allow = false;
        yield* f.witness("peer-blocked");
        yield* Stream.fromSubscription(updates).pipe(
          Stream.mapEffect(() => f.store.get("peer")),
          Stream.filter((d) => d?.wakeHold === "held"),
          Stream.take(1),
          Stream.runDrain,
        );
        assert.equal(gate.calls, 1);
        assert.equal(
          (yield* f.orchestrator.getThreadProjection(f.threadId)).runs.length,
          before + 1,
        );
        gate.allow = true;
        yield* changes.publish({ sequence: yield* f.store.sequence, threadIds: [f.threadId] });
        assert.equal((yield* f.store.get("peer"))?.wakeHold, "held");
        const blocked = (yield* f.orchestrator.getThreadProjection(f.threadId)).runs.find(
          (r) => r.userMessageId === MessageId.make("message:witness:peer-blocked"),
        )!;
        yield* f.sink.write({
          events: [
            yield* f.event({
              type: "run.updated",
              threadId: f.threadId,
              runId: blocked.id,
              payload: { ...blocked, status: "completed" },
            }),
          ],
        });
        const event = yield* f.witness("peer-release");
        yield* Stream.fromSubscription(updates).pipe(
          Stream.mapEffect(() => f.store.get("peer")),
          Stream.filter((d) => d?.state === "delivered"),
          Stream.take(1),
          Stream.runDrain,
        );
        const current = (yield* f.store.get("peer"))!;
        assert.equal(current.generation, 1);
        assert.equal(current.wakeHold, "consumed");
        const projection = yield* f.orchestrator.getThreadProjection(f.threadId);
        const successor = projection.messages.find((m) => m.id === current.wakeMessageId)!;
        assert.equal(successor.notification?.detail?.match(/PEER_HISTORY_MARKER/g)?.length, 1);
        yield* f.sink.write({
          events: [
            {
              ...event.event,
              id: (yield* f.event({
                type: "run.updated",
                threadId: f.threadId,
                runId: f.run.id,
                payload: f.run,
              })).id,
            },
          ],
        });
        assert.equal(
          (yield* f.orchestrator.getThreadProjection(f.threadId)).runs.length,
          projection.runs.length,
        );
        const witnessRun = projection.runs.find((r) => r.id === event.event.runId)!;
        yield* f.sink.write({
          events: [
            yield* f.event({
              type: "run.updated",
              threadId: f.threadId,
              runId: witnessRun.id,
              payload: { ...witnessRun, status: "completed" },
            }),
          ],
        });
        const successorRun = projection.runs.find((r) => r.id === successor.runId)!;
        if (
          (yield* f.orchestrator.getThreadProjection(f.threadId)).runs.find(
            (r) => r.id === successorRun.id,
          )?.status === "queued"
        )
          yield* f.orchestrator.dispatch({
            type: "queue.resume",
            commandId: CommandId.make("promote:peer"),
            threadId: f.threadId,
          });
        yield* f.start.start({ threadId: f.threadId, runId: successorRun.id }).pipe(Effect.result);
        assert.equal((yield* f.store.get("peer"))?.wakeHold, "held");
        assert.equal((yield* f.store.get("peer"))?.generation, 2);
        const recoveredHistory = yield* projectionStore.getTurnStartHistory(f.threadId);
        assert.isFalse(JSON.stringify(recoveredHistory).includes("PEER_HISTORY_MARKER"));
        const reRefused = yield* f.orchestrator.getThreadProjection(f.threadId);
        assert.equal(
          reRefused.turnItems.filter(
            (i) => i.runId === successorRun.id && i.type === "notification",
          ).length,
          1,
        );
        yield* f.sink.write({
          events: [
            {
              ...event.event,
              id: (yield* f.event({
                type: "run.updated",
                threadId: f.threadId,
                runId: f.run.id,
                payload: f.run,
              })).id,
            },
          ],
        });
        assert.equal((yield* f.store.get("peer"))?.wakeHold, "held");
        assert.equal((yield* f.store.get("peer"))?.generation, 2);
      }).pipe(
        Effect.provide(
          Reactor.layer.pipe(
            Layer.provideMerge(Watch.layer),
            Layer.provide(NodeCrypto.layer),
            Layer.provide(Layer.mock(Forwarding.PeerForwarding)({})),
          ),
        ),
      );
    }).pipe(Effect.provide(layer)),
);
