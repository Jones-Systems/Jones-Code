import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fixture from "./integration.testkit.ts";
import { historicalMessage } from "@t3tools/provider-core/server/handoffBudget";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";

it.effect("retains a completed child's claim when its admitted delivery is control-held", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    yield* f.refuse;
    const after = yield* f.orchestrator.getThreadProjection(f.threadId);
    const delivery = after.runs.find((run) => run.id === f.parent.id)!.delegatedCompletion!
      .delivery;
    assert.isNotNull(delivery);
    assert.equal(delivery!.generation, f.delivery.generation + 1);
    assert.equal(after.subagents[0]!.completionDelivery!.state, "claimed");
  }).pipe(Effect.provide(Fixture.runtime)),
);

import {
  CommandId,
  MessageId,
  NodeId,
  ProviderDriverKind,
  ProviderTurnId,
  type ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as Fiber from "effect/Fiber";
import * as SqlClient from "effect/sql/SqlClient";
import * as ThreadDispatch from "../../orchestration-v2/ThreadCommandExecutor.ts";
import * as Store from "./Store.ts";

const heldTasks = (count = 1) =>
  Effect.gen(function* () {
    const f = yield* Fixture.seedTasks(count);
    yield* f.refuse;
    const sql = yield* SqlClient.SqlClient;
    const read = sql<{
      state: string;
      generation: number;
      message_id: string;
      release_event_id: string | null;
    }>`
    SELECT state, generation, message_id, release_event_id FROM jones_start_refusal_holds ORDER BY generation`;
    return { ...f, sql, read };
  });
const held = heldTasks();
const joinRelease = Effect.fnUntraced(function* (threadId: ThreadId) {
  const gate = yield* Fixture.Gate;
  yield* Queue.take(gate.entered);
  yield* (yield* ThreadDispatch.ThreadCommandExecutor).withLock(threadId, Effect.void);
});
const dispatchHeld = Effect.fnUntraced(function* (f: Effect.Success<typeof held>, key: string) {
  const p = yield* f.orchestrator.getThreadProjection(f.threadId);
  const delivery = p.runs.find((r) => r.id === f.parent.id)!.delegatedCompletion!.delivery!;
  const h = { message_id: delivery.messageId, generation: delivery.generation };
  return yield* f.orchestrator.dispatch({
    type: "message.dispatch",
    commandId: CommandId.make(`held:${key}`),
    threadId: f.threadId,
    messageId: MessageId.make(h.message_id),
    text: "Completed child result",
    attachments: [],
    modelSelection: Fixture.selection,
    dispatchMode: { type: "queue_after_active" },
    delegatedCompletion: {
      parentRunId: f.parent.id,
      generation: h.generation,
      taskIds: delivery.taskIds,
    },
    createdBy: "agent",
    creationSource: "server",
  });
});

const acceptCurrent = Effect.fnUntraced(function* (f: Effect.Success<typeof held>, key: string) {
  const p = yield* f.orchestrator.getThreadProjection(f.threadId);
  const delivery = p.runs.find((r) => r.id === f.parent.id)!.delegatedCompletion!.delivery!;
  yield* dispatchHeld(f, key);
  const command = {
    type: "notification.delivery.accept" as const,
    commandId: CommandId.make(`accept:${key}`),
    threadId: f.threadId,
    messageId: delivery.messageId,
  };
  yield* f.orchestrator.dispatch(command);
  yield* f.orchestrator.dispatch(command);
  return delivery;
});

it.effect("holds both offers and direct admission, and survives repeated recovery", () =>
  Effect.gen(function* () {
    const f = yield* held;
    const offers = yield* Fixture.Offers;
    const count = offers.requests.length;
    assert.equal((yield* f.read)[0]!.state, "held");
    assert.equal((yield* dispatchHeld(f, "blocked").pipe(Effect.result))._tag, "Failure");
    for (let i = 0; i < 3; i++) yield* f.orchestrator.recoverDelegatedTasks;
    assert.equal(offers.requests.length, count);
    assert.lengthOf(yield* f.read, 1);
    assert.equal((yield* f.read)[0]!.state, "held");
  }).pipe(Effect.provide(Fixture.runtime)),
);

it.effect.each([
  "wake-policy",
  "remote-complete",
  "notification-accept",
  "acknowledge-other",
  "duplicate-finalization",
] as const)("the %s offer route cannot bypass a held claim", (route) =>
  Effect.gen(function* () {
    const f = yield* held;
    const offers = yield* Fixture.Offers;
    const count = offers.requests.length;
    if (route === "wake-policy")
      yield* f.orchestrator.dispatch({
        type: "delegated_task.wake-policy",
        commandId: CommandId.make("offer:policy"),
        parentThreadId: f.threadId,
        taskId: f.taskId,
        completionWake: "settled_only",
      });
    if (route === "remote-complete")
      yield* f.orchestrator.dispatch({
        type: "delegated_task.remote.complete",
        commandId: CommandId.make("complete:child"),
        parentThreadId: f.threadId,
        taskId: f.taskId,
        status: "completed",
        result: "Retain this result",
        stoppedThere: "ran-out",
      });
    if (route === "notification-accept")
      yield* f.orchestrator.dispatch({
        type: "notification.delivery.accept",
        commandId: CommandId.make("offer:notification"),
        threadId: f.threadId,
        messageId: MessageId.make("message:parent"),
      });
    if (route === "acknowledge-other") {
      const p = yield* f.orchestrator.getThreadProjection(f.threadId);
      const task = p.subagents[0]!;
      const otherId = NodeId.make("task:other-cohort");
      yield* f.sink.write({
        events: [
          yield* f.event({
            type: "subagent.updated",
            threadId: f.threadId,
            payload: {
              ...task,
              id: otherId,
              runId: null,
              completionDelivery: { state: "delivered", observedByRunId: null },
            },
          }),
        ],
      });
      yield* f.orchestrator.dispatch({
        type: "delegated_task.completion-delivery.acknowledge",
        commandId: CommandId.make("offer:ack"),
        parentThreadId: f.threadId,
        taskId: otherId,
        observedByRunId: null,
      });
    }
    if (route === "duplicate-finalization") {
      const p = yield* f.orchestrator.getThreadProjection(f.threadId);
      yield* f.sink.write({
        events: [
          yield* f.event({
            type: "run.updated",
            threadId: f.threadId,
            runId: f.run.id,
            payload: p.runs.find((r) => r.id === f.run.id)!,
          }),
        ],
      });
      yield* f.orchestrator.recoverDelegatedTasks;
    }
    assert.equal(offers.requests.length, count);
    assert.equal((yield* f.read)[0]!.state, "held");
  }).pipe(Effect.provide(Fixture.runtime)),
);

it.effect("pre-ack, old-attempt and unsupported-driver running updates never arm", () =>
  Effect.gen(function* () {
    const f = yield* held;
    const store = yield* Store.StartRefusals;
    const preack = yield* f.witness("preack", false);
    assert.deepEqual(yield* store.armedBy(preack), []);
    const snapshot = yield* f.orchestrator.getThreadProjection(f.threadId);
    const attempt = snapshot.attempts.find((a) => a.id === f.run.activeAttemptId)!;
    const now = yield* DateTime.now;
    yield* f.sink.write({
      events: [
        yield* f.event({
          type: "provider-turn.updated",
          threadId: f.threadId,
          runId: f.run.id,
          driver: Fixture.driver,
          payload: {
            id: ProviderTurnId.make("turn:old"),
            nodeId: attempt.rootNodeId,
            providerThreadId: attempt.providerThreadId,
            runAttemptId: attempt.id,
            ordinal: Math.max(0, ...snapshot.providerTurns.map((t) => t.ordinal)) + 1,
            status: "running",
            nativeTurnRef: { driver: Fixture.driver, nativeId: "old", strength: "strong" },
            startedAt: now,
            completedAt: null,
          },
        }),
      ],
    });
    const unsupported = yield* f.witness(
      "unsupported",
      true,
      true,
      ProviderDriverKind.make("claudeAgent"),
    );
    assert.isDefined(
      unsupported.event.type === "provider-turn.updated" && unsupported.event.payload.nativeTurnRef,
    );
    assert.deepEqual(yield* store.armedBy(unsupported), []);
    const missingBirth = yield* f.witness("missing-birth", true, false);
    assert.deepEqual(yield* store.armedBy(missingBirth), []);
    assert.equal((yield* f.read)[0]!.state, "held");
    assert.equal((yield* Fixture.Gate).calls, 0);
  }).pipe(Effect.provide(Fixture.runtime)),
);

it.effect(
  "a fresh accepted start revalidates the gates and consumes one release, never the same callback twice",
  () =>
    Effect.gen(function* () {
      const f = yield* held;
      const gate = yield* Fixture.Gate;
      const store = yield* Store.StartRefusals;
      const offers = yield* Fixture.Offers;
      const count = offers.requests.length;
      gate.allow = false;
      const preack = yield* f.witness("first", false);
      assert.equal(gate.calls, 0);
      if (preack.event.type !== "provider-turn.updated")
        return yield* Effect.die("Expected the synthetic provider turn");
      const first = (yield* f.sink.write({
        events: [
          yield* f.event({
            ...preack.event,
            payload: {
              ...preack.event.payload,
              nativeTurnRef: { driver: Fixture.driver, nativeId: "first", strength: "strong" },
            },
          }),
        ],
      }))[0]!;
      yield* joinRelease(f.threadId);
      assert.equal((yield* f.read)[0]!.state, "held");
      gate.allow = true;
      yield* store.events([first]);
      assert.deepEqual(yield* store.armedBy(first), []);
      assert.equal(offers.requests.length, count);
      const second = yield* f.witness("second");
      yield* joinRelease(f.threadId);
      assert.equal((yield* f.read)[0]!.state, "consumed");
      assert.equal(offers.requests.length, count + 1);
      yield* store.events([second]);
      yield* f.orchestrator.recoverDelegatedTasks;
      assert.equal((yield* f.read)[0]!.state, "consumed");
      assert.equal(gate.calls, 2);
      // Message-ID admission is idempotent even when recovery offers it again.
      yield* dispatchHeld(f, "accepted");
      yield* dispatchHeld(f, "accepted");
      assert.equal(
        (yield* dispatchHeld(f, "accepted-different-id").pipe(Effect.result))._tag,
        "Failure",
      );
      const p = yield* f.orchestrator.getThreadProjection(f.threadId);
      const messageId = (yield* f.read)[0]!.message_id;
      assert.equal(p.messages.filter((m) => m.id === messageId).length, 1);
    }).pipe(Effect.provide(Fixture.runtime)),
);

it.effect.each(["stop", "acknowledge", "supersede"] as const)(
  "%s wins against an armed release without reviving the result",
  (action) =>
    Effect.gen(function* () {
      const f = yield* held;
      const store = yield* Store.StartRefusals;
      const witness = yield* f.witness(`race:${action}`);
      const [armed] = yield* store.armedBy(witness);
      assert.isDefined(armed);
      if (action === "stop")
        yield* f.orchestrator.dispatch({
          type: "thread.stop",
          threadId: f.threadId,
          stopDelegatedTasks: false,
          commandId: CommandId.make("race:stop"),
        });
      if (action === "acknowledge")
        yield* f.orchestrator.dispatch({
          type: "delegated_task.completion-delivery.acknowledge",
          commandId: CommandId.make("race:ack"),
          parentThreadId: f.threadId,
          taskId: f.taskId,
          observedByRunId: f.parent.id,
        });
      if (action === "supersede") {
        const p = yield* f.orchestrator.getThreadProjection(f.threadId);
        const parent = p.runs.find((r) => r.id === f.parent.id)!;
        yield* f.sink.write({
          events: [
            yield* f.event({
              type: "run.updated",
              threadId: f.threadId,
              runId: parent.id,
              payload: {
                ...parent,
                delegatedCompletion: { ...parent.delegatedCompletion!, delivery: null },
              },
            }),
          ],
        });
      }
      assert.equal((yield* f.read)[0]!.state, "void");
      assert.isFalse(yield* store.finishArming(armed!, true));
      yield* f.orchestrator.recoverDelegatedTasks;
      assert.equal((yield* f.read)[0]!.state, "void");
    }).pipe(Effect.provide(Fixture.runtimeWithoutProbe)),
);

it.effect("restart leaves armed work held and completes only an already consumed offer", () =>
  Effect.gen(function* () {
    const f = yield* held;
    const store = yield* Store.StartRefusals;
    const offers = yield* Fixture.Offers;
    const count = offers.requests.length;
    const witness = yield* f.witness("crash");
    const [armed] = yield* store.armedBy(witness);
    assert.isDefined(armed);
    yield* f.orchestrator.recoverDelegatedTasks;
    assert.equal((yield* f.read)[0]!.state, "armed");
    assert.equal(offers.requests.length, count);
    assert.equal((yield* dispatchHeld(f, "armed-blocked").pipe(Effect.result))._tag, "Failure");
    assert.isTrue(yield* store.finishArming(armed!, true));
    yield* f.orchestrator.recoverDelegatedTasks;
    assert.equal(offers.requests.length, count + 1);
    assert.isFalse(yield* store.finishArming(armed!, true));
  }).pipe(Effect.provide(Fixture.runtimeWithoutProbe)),
);

it.effect.each([
  { missing: "both tables", runtime: Fixture.legacyRuntime },
  { missing: "eligibility table", runtime: Fixture.missingAttemptsRuntime },
  { missing: "hold table", runtime: Fixture.missingHoldsRuntime },
])("missing $missing preserves the prior delivered disposition", ({ runtime }) =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    yield* f.refuse;
    const p = yield* f.orchestrator.getThreadProjection(f.threadId);
    assert.equal(p.subagents[0]!.completionDelivery!.state, "delivered");
    assert.equal(p.runs.find((r) => r.id === f.parent.id)!.delegatedCompletion!.delivery, null);
  }).pipe(Effect.provide(runtime)),
);

it.effect(
  "a crash after certification but before successor reservation retains the source claim for recovery",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TEMP TRIGGER fail_hold BEFORE INSERT ON jones_start_refusal_holds
    BEGIN SELECT RAISE(ABORT, 'synthetic reservation failure'); END`;
      yield* f.start.start({ threadId: f.threadId, runId: f.run.id }).pipe(Effect.result);
      yield* f.orchestrator.recoverDelegatedTasks;
      const failed = yield* f.orchestrator.getThreadProjection(f.threadId);
      assert.equal(failed.subagents[0]!.completionDelivery!.state, "claimed");
      assert.equal(
        failed.runs.find((r) => r.id === f.parent.id)!.delegatedCompletion!.delivery!.messageId,
        f.delivery.messageId,
      );
      assert.lengthOf(yield* sql`SELECT 1 FROM jones_start_refusal_holds`, 0);
      yield* sql`DROP TRIGGER fail_hold`;
      yield* f.orchestrator.recoverDelegatedTasks;
      yield* f.orchestrator.recoverDelegatedTasks;
      assert.lengthOf(yield* sql`SELECT 1 FROM jones_start_refusal_holds`, 1);
    }).pipe(Effect.provide(Fixture.runtimeWithoutProbe)),
);

it.effect(
  "a failed certification transaction can retry without leaving a false terminal certificate",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TEMP TRIGGER fail_refusal BEFORE UPDATE ON jones_start_refusal_attempts
    WHEN NEW.state = 'refused' BEGIN SELECT RAISE(ABORT, 'synthetic refusal failure'); END`;
      yield* f.start.start({ threadId: f.threadId, runId: f.run.id }).pipe(Effect.result);
      const failed = yield* f.orchestrator.getThreadProjection(f.threadId);
      assert.equal(failed.runs.find((r) => r.id === f.run.id)!.status, "starting");
      assert.equal(
        (yield* sql`SELECT state FROM jones_start_refusal_attempts`)[0]!.state,
        "eligible",
      );
      yield* sql`DROP TRIGGER fail_refusal`;
      yield* f.refuse;
      assert.equal((yield* sql`SELECT state FROM jones_start_refusal_holds`)[0]!.state, "held");
    }).pipe(Effect.provide(Fixture.runtimeWithoutProbe)),
);

it.effect("successor commit revalidates the exact claimed task set in its transaction", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const sql = yield* SqlClient.SqlClient;
    const dispatch = yield* ThreadDispatch.ThreadCommandExecutor;
    yield* dispatch.withLock(
      f.threadId,
      Effect.gen(function* () {
        yield* f.start.start({ threadId: f.threadId, runId: f.run.id }).pipe(Effect.result);
        const p = yield* f.orchestrator.getThreadProjection(f.threadId);
        const parent = p.runs.find((r) => r.id === f.parent.id)!;
        const successor = {
          ...f.delivery,
          generation: f.delivery.generation + 1,
          messageId: MessageId.make("message:stale-successor"),
        };
        yield* f.sink.write({
          events: [
            yield* f.event({
              type: "subagent.updated",
              threadId: f.threadId,
              payload: {
                ...p.subagents[0]!,
                completionDelivery: { state: "acknowledged", observedByRunId: parent.id },
              },
            }),
          ],
        });
        const written = yield* f.sink.writeWithEffects({
          startRefusalHold: {
            threadId: f.threadId,
            parentRunId: parent.id,
            refusedRunId: f.run.id,
            source: f.delivery,
            successor,
          },
          updateDelegatedCompletionOnly: true,
          effects: [],
          events: [
            yield* f.event({
              type: "run.updated",
              threadId: f.threadId,
              runId: parent.id,
              payload: {
                ...parent,
                delegatedCompletion: {
                  ...parent.delegatedCompletion!,
                  delivery: successor,
                  nextGeneration: successor.generation + 1,
                },
              },
            }),
          ],
        });
        assert.lengthOf(written, 0);
        assert.lengthOf(yield* sql`SELECT 1 FROM jones_start_refusal_holds`, 0);
      }),
    );
  }).pipe(Effect.provide(Fixture.runtimeWithoutProbe)),
);

it.effect(
  "a re-refusal holds the next generation and cannot spend the previous acceptance twice",
  () =>
    Effect.gen(function* () {
      const f = yield* held;
      const store = yield* Store.StartRefusals;
      const offers = yield* Fixture.Offers;
      const first = yield* f.witness("first-generation");
      yield* joinRelease(f.threadId);
      assert.equal((yield* f.read)[0]!.state, "consumed");
      const current = yield* f.orchestrator.getThreadProjection(f.threadId);
      const acceptedRun = current.runs.find((r) => r.id === first.event.runId)!;
      const acceptedAttempt = current.attempts.find((a) => a.id === acceptedRun.activeAttemptId)!;
      const turn = current.providerTurns.find((t) => t.runAttemptId === acceptedAttempt.id)!;
      const now = yield* DateTime.now;
      yield* f.sink.write({
        events: [
          yield* f.event({
            type: "run.updated",
            threadId: f.threadId,
            runId: acceptedRun.id,
            payload: { ...acceptedRun, status: "completed", completedAt: now },
          }),
          yield* f.event({
            type: "run-attempt.updated",
            threadId: f.threadId,
            runId: acceptedRun.id,
            payload: { ...acceptedAttempt, status: "completed", completedAt: now },
          }),
          yield* f.event({
            type: "provider-turn.updated",
            threadId: f.threadId,
            runId: acceptedRun.id,
            driver: Fixture.driver,
            payload: { ...turn, status: "completed", completedAt: now },
          }),
        ],
      });
      yield* dispatchHeld(f, "first-generation");
      const admitted = yield* f.orchestrator.getThreadProjection(f.threadId);
      const messageId = (yield* f.read)[0]!.message_id;
      const successor = admitted.runs.find((r) => r.userMessageId === messageId)!;
      assert.equal(successor.status, "starting");
      const nextHold = yield* f.sink
        .stream({ threadId: f.threadId, eventType: "run.updated" })
        .pipe(
          Stream.filter(
            ({ event }) =>
              event.type === "run.updated" &&
              event.payload.id === f.parent.id &&
              event.payload.delegatedCompletion?.delivery?.generation === f.delivery.generation + 2,
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkChild,
        );
      yield* Effect.yieldNow;
      yield* f.start.start({ threadId: f.threadId, runId: successor.id }).pipe(Effect.result);
      yield* Fiber.join(nextHold);
      assert.lengthOf(yield* f.read, 2);
      assert.equal((yield* f.read)[1]!.state, "held");
      const count = offers.requests.length;
      yield* store.events([first]);
      yield* f.orchestrator.recoverDelegatedTasks;
      assert.deepEqual(yield* store.armedBy(first), []);
      assert.equal((yield* f.read)[1]!.state, "held");
      assert.equal(offers.requests.length, count);
      yield* f.witness("next-generation");
      yield* joinRelease(f.threadId);
      assert.equal((yield* f.read)[1]!.state, "consumed");
      assert.equal(offers.requests.length, count + 1);
    }).pipe(Effect.provide(Fixture.runtime)),
);

it.effect(
  "retained refusal messages remain notifications excluded from recovered provider history",
  () =>
    Effect.gen(function* () {
      const f = yield* held;
      const projection = yield* ProjectionStore.ProjectionStoreV2;
      yield* f.orchestrator.recoverDelegatedTasks;
      const items = yield* projection.getTurnStartHistory(f.threadId);
      const p = yield* f.orchestrator.getThreadProjection(f.threadId);
      const notice = p.turnItems.find(
        (item) => item.runId === f.run.id && item.type === "notification",
      );
      assert.isDefined(notice);
      assert.equal(historicalMessage(notice!), null);
      assert.isFalse(items.some((item) => item.id === notice!.id));
      assert.isTrue(
        p.messages.some(
          (message) =>
            message.id === f.delivery.messageId &&
            message.delegatedCompletion?.parentRunId === f.parent.id,
        ),
      );
      assert.isFalse(
        items
          .flatMap((item) => historicalMessage(item) ?? [])
          .some((item) => item.kind === "notification"),
      );
      assert.equal(
        p.subagents.find((task) => task.id === f.taskId)?.completionDelivery?.state,
        "claimed",
      );
    }).pipe(Effect.provide(Fixture.runtime)),
);

it.effect.each(["held", "armed"] as const)(
  "a reconstructed orchestrator never releases persisted %s work",
  (state) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const retained = yield* Effect.gen(function* () {
        const f = yield* held;
        if (state === "armed") yield* f.witness("before-restart");
        assert.equal((yield* f.read)[0]!.state, state);
        return { threadId: f.threadId, parentRunId: f.parent.id };
      }).pipe(Effect.provide(Fixture.runtimeOn(sql, false)));
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const offers = yield* Fixture.Offers;
        yield* orchestrator.recoverDelegatedTasks;
        yield* orchestrator.recoverDelegatedTasks;
        assert.lengthOf(offers.requests, 0);
        assert.equal((yield* sql`SELECT state FROM jones_start_refusal_holds`)[0]!.state, state);
        const projection = yield* orchestrator.getThreadProjection(retained.threadId);
        assert.isNotNull(
          projection.runs.find((run) => run.id === retained.parentRunId)!.delegatedCompletion!
            .delivery,
        );
        assert.equal(projection.subagents[0]!.completionDelivery!.state, "claimed");
      }).pipe(Effect.provide(Fixture.runtimeOn(sql, true)));
    }).pipe(Effect.provide(Fixture.database)),
);

it.effect("late siblings preserve the immutable refused membership and remain pending", () =>
  Effect.gen(function* () {
    const f = yield* held;
    yield* f.orchestrator.dispatch({
      type: "delegated_task.wake-policy",
      commandId: CommandId.make("policy:claimed-member"),
      parentThreadId: f.threadId,
      taskId: f.taskId,
      completionWake: "settled_only",
    });
    const before = yield* f.orchestrator.getThreadProjection(f.threadId);
    assert.equal(before.subagents[0]!.completionDelivery!.state, "claimed");
    const siblingId = NodeId.make("task:late-sibling");
    yield* f.sink.write({
      events: [
        yield* f.event({
          type: "subagent.updated",
          threadId: f.threadId,
          runId: f.parent.id,
          payload: {
            ...before.subagents[0]!,
            id: siblingId,
            status: "running",
            completionDelivery: { state: "pending", observedByRunId: null },
            result: null,
          },
        }),
      ],
    });
    yield* f.orchestrator.dispatch({
      type: "delegated_task.remote.complete",
      commandId: CommandId.make("complete:late-sibling"),
      parentThreadId: f.threadId,
      taskId: siblingId,
      status: "completed",
      result: "Late sibling result",
      stoppedThere: "ran-out",
    });
    const after = yield* f.orchestrator.getThreadProjection(f.threadId);
    assert.deepEqual(
      after.runs.find((r) => r.id === f.parent.id)!.delegatedCompletion!.delivery!.taskIds,
      [f.taskId],
    );
    assert.equal(
      after.subagents.find((t) => t.id === siblingId)!.completionDelivery!.state,
      "pending",
    );
    assert.equal((yield* f.read)[0]!.state, "held");
    yield* f.orchestrator.recoverDelegatedTasks;
    const witness = yield* f.witness("late-sibling-release");
    yield* joinRelease(f.threadId);
    const first = yield* acceptCurrent(f, "first-child");
    assert.deepEqual(first.taskIds, [f.taskId]);
    const second = yield* acceptCurrent(f, "late-child");
    assert.deepEqual(second.taskIds, [siblingId]);
    yield* (yield* Store.StartRefusals).events([witness]);
    yield* f.orchestrator.recoverDelegatedTasks;
    const delivered = yield* f.orchestrator.getThreadProjection(f.threadId);
    assert.deepEqual(
      delivered.subagents.map((t) => t.completionDelivery!.state),
      ["delivered", "delivered"],
    );
    assert.isNull(delivered.runs.find((r) => r.id === f.parent.id)!.delegatedCompletion!.delivery);
    for (const delivery of [first, second])
      assert.lengthOf(
        delivered.runs.filter((r) => r.userMessageId === delivery.messageId),
        1,
      );
  }).pipe(Effect.provide(Fixture.runtime)),
);

it.effect("partial acknowledgement reserves a held remainder under a fresh key", () =>
  Effect.gen(function* () {
    const f = yield* heldTasks(3);
    const sql = yield* SqlClient.SqlClient;
    const before = yield* f.orchestrator.getThreadProjection(f.threadId);
    const old = before.runs.find((r) => r.id === f.parent.id)!.delegatedCompletion!.delivery!;
    yield* f.orchestrator.dispatch({
      type: "delegated_task.completion-delivery.acknowledge",
      commandId: CommandId.make("ack:partial"),
      parentThreadId: f.threadId,
      taskId: f.taskId,
      observedByRunId: f.parent.id,
    });
    const after = yield* f.orchestrator.getThreadProjection(f.threadId);
    const remainder = after.runs.find((r) => r.id === f.parent.id)!.delegatedCompletion!.delivery!;
    assert.notEqual(remainder.messageId, old.messageId);
    assert.equal(remainder.generation, old.generation + 1);
    assert.deepEqual(
      remainder.taskIds,
      old.taskIds.filter((id) => id !== f.taskId),
    );
    assert.deepEqual(
      (yield* sql`SELECT state FROM jones_start_refusal_holds ORDER BY generation`).map(
        (r) => r.state,
      ),
      ["void", "held"],
    );
    const store = yield* Store.StartRefusals;
    const stale = yield* f.witness("remainder-stale");
    yield* joinRelease(f.threadId);
    const nextTask = remainder.taskIds[0]!;
    const ack = {
      type: "delegated_task.completion-delivery.acknowledge" as const,
      commandId: CommandId.make("ack:second"),
      parentThreadId: f.threadId,
      taskId: nextTask,
      observedByRunId: f.parent.id,
    };
    yield* f.orchestrator.dispatch(ack);
    yield* f.orchestrator.dispatch({ ...ack, commandId: CommandId.make("ack:second-duplicate") });
    yield* store.events([stale]);
    yield* f.orchestrator.recoverDelegatedTasks;
    assert.deepEqual(
      (yield* f.read).map((r) => r.state),
      ["void", "void", "held"],
    );
    yield* f.witness("remainder-fresh");
    yield* joinRelease(f.threadId);
    const final = yield* acceptCurrent(f, "only-unacknowledged");
    assert.deepEqual(final.taskIds, [old.taskIds[2]!]);
    yield* f.orchestrator.recoverDelegatedTasks;
    const delivered = yield* f.orchestrator.getThreadProjection(f.threadId);
    assert.deepEqual(
      delivered.subagents.map((t) => t.completionDelivery!.state),
      ["acknowledged", "acknowledged", "delivered"],
    );
    assert.isNull(delivered.runs.find((r) => r.id === f.parent.id)!.delegatedCompletion!.delivery);
  }).pipe(Effect.provide(Fixture.runtime)),
);

it.effect("remainder reservation failure rolls back acknowledgement and its command receipt", () =>
  Effect.gen(function* () {
    const f = yield* heldTasks(2);
    yield* f.sql`CREATE TEMP TRIGGER fail_remainder BEFORE INSERT ON jones_start_refusal_holds
      BEGIN SELECT RAISE(ABORT, 'synthetic remainder failure'); END`;
    const command = {
      type: "delegated_task.completion-delivery.acknowledge" as const,
      commandId: CommandId.make("ack:transaction"),
      parentThreadId: f.threadId,
      taskId: f.taskId,
      observedByRunId: f.parent.id,
    };
    assert.equal((yield* f.orchestrator.dispatch(command).pipe(Effect.result))._tag, "Failure");
    assert.deepEqual(
      (yield* f.read).map((r) => r.state),
      ["held"],
    );
    const before = yield* f.orchestrator.getThreadProjection(f.threadId);
    assert.isTrue(before.subagents.every((t) => t.completionDelivery!.state === "claimed"));
    yield* f.sql`DROP TRIGGER fail_remainder`;
    yield* f.orchestrator.dispatch(command);
    assert.deepEqual(
      (yield* f.read).map((r) => r.state),
      ["void", "held"],
    );
  }).pipe(Effect.provide(Fixture.runtimeWithoutProbe)),
);

it.effect.each(["stop", "changed-membership"] as const)(
  "reconstructed remainder retains its correlation and %s cannot resurrect it",
  (action) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const retained = yield* Effect.gen(function* () {
        const f = yield* heldTasks(3);
        yield* f.orchestrator.dispatch({
          type: "delegated_task.completion-delivery.acknowledge",
          commandId: CommandId.make("ack:before-restart"),
          parentThreadId: f.threadId,
          taskId: f.taskId,
          observedByRunId: f.parent.id,
        });
        const witness = yield* f.witness("remainder-before-restart");
        const armed = (yield* (yield* Store.StartRefusals).armedBy(witness))[0]!;
        assert.isDefined(armed);
        const p = yield* f.orchestrator.getThreadProjection(f.threadId);
        return {
          witness,
          armed,
          threadId: f.threadId,
          parentRunId: f.parent.id,
          delivery: p.runs.find((r) => r.id === f.parent.id)!.delegatedCompletion!.delivery!,
          event: f.event,
        };
      }).pipe(Effect.provide(Fixture.runtimeOn(sql, false)));
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const store = yield* Store.StartRefusals;
        const sink = yield* EventSink.EventSinkV2;
        const key = {
          threadId: retained.threadId,
          parentRunId: retained.parentRunId,
          generation: retained.delivery.generation,
          messageId: retained.delivery.messageId,
        };
        assert.isTrue(yield* store.canRekey(key));
        yield* orchestrator.recoverDelegatedTasks;
        yield* orchestrator.recoverDelegatedTasks;
        assert.lengthOf((yield* Fixture.Offers).requests, 0);
        if (action === "stop")
          yield* orchestrator.dispatch({
            type: "thread.stop",
            threadId: retained.threadId,
            stopDelegatedTasks: false,
            commandId: CommandId.make("stop:remainder"),
          });
        else {
          const p = yield* orchestrator.getThreadProjection(retained.threadId);
          const parent = p.runs.find((r) => r.id === retained.parentRunId)!;
          const task = p.subagents.find((t) => t.id === retained.delivery.taskIds[0])!;
          yield* sink.write({
            events: [
              yield* retained.event({
                type: "subagent.updated",
                threadId: retained.threadId,
                payload: {
                  ...task,
                  completionDelivery: { state: "acknowledged", observedByRunId: parent.id },
                },
              }),
              yield* retained.event({
                type: "run.updated",
                threadId: retained.threadId,
                runId: parent.id,
                payload: {
                  ...parent,
                  delegatedCompletion: {
                    ...parent.delegatedCompletion!,
                    delivery: { ...retained.delivery, taskIds: retained.delivery.taskIds.slice(1) },
                  },
                },
              }),
            ],
          });
        }
        assert.isFalse(yield* store.canRekey(key));
        assert.isFalse(yield* store.finishArming(retained.armed, true));
        yield* store.events([retained.witness]);
        assert.isTrue(yield* store.blocked(key));
        assert.deepEqual(
          (yield* sql`SELECT state FROM jones_start_refusal_holds ORDER BY generation`).map(
            (r) => r.state,
          ),
          ["void", "void"],
        );
        yield* orchestrator.recoverDelegatedTasks;
        assert.lengthOf((yield* Fixture.Offers).requests, 0);
      }).pipe(Effect.provide(Fixture.runtimeOn(sql, true)));
    }).pipe(Effect.provide(Fixture.database)),
);
