import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunId,
  RuntimeRequestId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as TestClock from "effect/testing/TestClock";

import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import * as EffectWorker from "../../orchestration-v2/EffectWorker.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";

const layerOutbox = Layer.fresh(
  EffectOutbox.layer.pipe(Layer.provideMerge(SqlitePersistence.layerMemory)),
);

const pending = (
  id: string,
  kind: "action" | "title" | "interrupt" | "delegated-stop" | "runtime-response",
  threadId = ThreadId.make(`thread:${id}`),
): EffectOutbox.PendingOrchestrationEffectV2 => ({
  id,
  commandId: CommandId.make(`command:${id}`),
  threadId,
  request:
    kind === "title"
      ? { type: "thread-title.generate", kind: { type: "regenerate" } }
      : kind === "interrupt"
        ? {
            type: "provider-turn.interrupt",
            providerSessionId: ProviderSessionId.make(`session:${id}`),
            providerThreadId: ProviderThreadId.make(`provider-thread:${id}`),
            providerTurnId: ProviderTurnId.make(`turn:${id}`),
          }
        : kind === "delegated-stop"
          ? { type: "delegated-tasks.stop" }
          : kind === "runtime-response"
            ? {
                type: "runtime-request.respond",
                providerSessionId: ProviderSessionId.make(`session:${id}`),
                requestId: RuntimeRequestId.make(`request:${id}`),
              }
            : { type: "provider-turn.start", runId: RunId.make(`run:${id}`) },
});

const makeFixture = Effect.gen(function* () {
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const started = yield* Queue.unbounded<string>();
  const attempts = yield* Ref.make({ urgent: 0, action: 0, mixed: 0 });
  const gates = new Map<string, Deferred.Deferred<void>>();
  const gate = (id: string) => {
    const existing = gates.get(id);
    if (existing !== undefined) return existing;
    const created = Deferred.makeUnsafe<void>();
    gates.set(id, created);
    return created;
  };
  const observedOutbox = EffectOutbox.EffectOutboxV2.of({
    ...outbox,
    claimNext: (input) =>
      Ref.update(attempts, (counts) =>
        input.urgentOnly === true
          ? { ...counts, urgent: counts.urgent + 1 }
          : input.excludeTitleGeneration === true
            ? { ...counts, action: counts.action + 1 }
            : { ...counts, mixed: counts.mixed + 1 },
      ).pipe(Effect.andThen(outbox.claimNext(input))),
  });
  const executions = yield* Ref.make({ active: 0, maximum: 0 });
  const executor = EffectWorker.OrchestrationEffectExecutorV2.of({
    execute: (effect) =>
      Ref.update(executions, ({ active, maximum }) => ({
        active: active + 1,
        maximum: Math.max(maximum, active + 1),
      })).pipe(
        Effect.andThen(Queue.offer(started, effect.id)),
        Effect.andThen(Deferred.await(gate(effect.id))),
        Effect.ensuring(
          Ref.update(executions, (counts) => ({ ...counts, active: counts.active - 1 })),
        ),
      ),
  });
  const worker = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
    Effect.provide(
      EffectWorker.layerWithOptions({ workerId: "action-admission-test" }).pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(EffectOutbox.EffectOutboxV2, observedOutbox),
            Layer.succeed(EffectWorker.OrchestrationEffectExecutorV2, executor),
          ),
        ),
      ),
    ),
  );
  return {
    outbox,
    attempts,
    executions,
    started,
    release: (id: string) => Deferred.succeed(gate(id), undefined),
    start: (concurrency = 4) =>
      EffectWorker.runDaemonWithOptions({ concurrency, livenessPollIntervalMs: 30_000 }).pipe(
        Effect.provideService(EffectWorker.OrchestrationEffectWorkerV2, worker),
        Effect.forkScoped,
      ),
  };
});

it.effect("RES-1 preserves ordinary capacity while two titles are held", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    const titles = Array.from({ length: 4 }, (_, index) => pending(`title:${index}`, "title"));
    yield* f.outbox.enqueue(titles);
    yield* f.start();
    const started = yield* Effect.forEach(titles.slice(0, 2), () => Queue.take(f.started));
    assert.deepEqual(
      started.toSorted(),
      titles.slice(0, 2).map(({ id }) => id),
    );

    yield* TestClock.adjust("1 second");
    for (const title of titles.slice(2)) {
      const pendingTitle = Option.getOrThrow(yield* f.outbox.get(title.id));
      assert.equal(pendingTitle.status, "pending");
      assert.equal(pendingTitle.attemptCount, 0);
    }
    assert.deepEqual(yield* Ref.get(f.executions), { active: 2, maximum: 2 });
    assert.equal(yield* Ref.get(f.attempts).pipe(Effect.map((counts) => counts.action)), 1);

    const action = pending("unrelated-action", "action");
    yield* f.outbox.enqueue([action]);
    yield* f.outbox.notifyAvailable();
    assert.equal(yield* Queue.take(f.started), action.id);
    for (const effect of [...titles.slice(0, 2), action]) {
      assert.equal(Option.getOrThrow(yield* f.outbox.get(effect.id)).status, "running");
    }
    assert.equal(Option.getOrThrow(yield* f.outbox.get(titles[2]!.id)).attemptCount, 0);
  }).pipe(Effect.scoped, Effect.provide(layerOutbox.pipe(Layer.provideMerge(TestClock.layer())))),
);

it.effect(
  "RES-1 admits urgent work with three ordinary executions held and never adds a fifth executor",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      const actions = Array.from({ length: 5 }, (_, index) => pending(`action:${index}`, "action"));
      yield* f.outbox.enqueue(actions);
      yield* f.start();
      yield* TestClock.adjust("0 millis");
      assert.deepEqual(yield* Ref.get(f.executions), { active: 3, maximum: 3 });
      const started = yield* Effect.forEach(actions.slice(0, 3), () => Queue.take(f.started));
      assert.deepEqual(
        started.toSorted(),
        actions.slice(0, 3).map(({ id }) => id),
      );

      const urgent = pending("urgent-interrupt", "interrupt");
      yield* f.outbox.enqueue([urgent]);
      yield* f.outbox.notifyAvailable();
      yield* TestClock.adjust("0 millis");
      assert.equal(Option.getOrThrow(yield* f.outbox.get(urgent.id)).status, "running");
      assert.equal(yield* Queue.take(f.started), urgent.id);
      assert.deepEqual(yield* Ref.get(f.executions), { active: 4, maximum: 4 });
      for (const action of actions.slice(0, 3)) {
        assert.equal(Option.getOrThrow(yield* f.outbox.get(action.id)).status, "running");
      }
      for (const action of actions.slice(3)) {
        assert.equal(Option.getOrThrow(yield* f.outbox.get(action.id)).attemptCount, 0);
      }

      yield* f.release(urgent.id);
      yield* f.outbox.awaitCompletion([urgent.id]);
      yield* TestClock.adjust("0 millis");
      assert.deepEqual(yield* Ref.get(f.executions), { active: 3, maximum: 4 });
      assert.equal(Option.getOrThrow(yield* f.outbox.get(actions[3]!.id)).attemptCount, 0);
      yield* f.release(actions[0]!.id);
      yield* f.outbox.awaitCompletion([actions[0]!.id]);
      assert.equal(yield* Queue.take(f.started), actions[3]!.id);
      assert.deepEqual(yield* Ref.get(f.executions), { active: 3, maximum: 4 });
    }).pipe(Effect.scoped, Effect.provide(layerOutbox.pipe(Layer.provideMerge(TestClock.layer())))),
);

it.effect("CORE-A4 wakes a mixed worker promptly when the reserved worker is already waiting", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    yield* f.start(2);
    yield* TestClock.adjust("0 millis");
    assert.deepEqual(yield* Ref.get(f.attempts), { urgent: 0, action: 1, mixed: 1 });

    const title = pending("new-title-wake", "title");
    yield* f.outbox.enqueue([title]);
    yield* f.outbox.notifyAvailable();
    yield* TestClock.adjust("1 millis");
    assert.equal(Option.getOrThrow(yield* f.outbox.get(title.id)).status, "running");
    assert.equal(yield* Queue.take(f.started), title.id);
    assert.equal(yield* Ref.get(f.attempts).pipe(Effect.map((counts) => counts.action)), 2);
  }).pipe(Effect.scoped, Effect.provide(layerOutbox.pipe(Layer.provideMerge(TestClock.layer())))),
);

it.effect("CORE-A4 does not poll claims for due titles on the reserved worker", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    const titles = [pending("held-title", "title"), pending("waiting-title", "title")];
    yield* f.outbox.enqueue(titles);
    yield* f.start(2);
    assert.equal(yield* Queue.take(f.started), titles[0]!.id);
    yield* TestClock.adjust("0 millis");
    assert.deepEqual(yield* Ref.get(f.attempts), { urgent: 0, action: 1, mixed: 1 });

    yield* TestClock.adjust("29999 millis");
    assert.deepEqual(yield* Ref.get(f.attempts), { urgent: 0, action: 1, mixed: 1 });
    assert.equal(Option.getOrThrow(yield* f.outbox.get(titles[1]!.id)).attemptCount, 0);
    yield* TestClock.adjust("1 millis");
    assert.deepEqual(yield* Ref.get(f.attempts), { urgent: 0, action: 2, mixed: 1 });
  }).pipe(Effect.scoped, Effect.provide(layerOutbox.pipe(Layer.provideMerge(TestClock.layer())))),
);

it.effect(
  "CORE-A4 preserves action FIFO across retries while title ordering stays independent",
  () =>
    Effect.gen(function* () {
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const threadId = ThreadId.make("thread:ordered-action-admission");
      const first = pending("z-first-action", "action", threadId);
      const later = pending("a-later-action", "action", threadId);
      const title = pending("b-first-title", "title", threadId);
      const laterTitle = pending("c-later-title", "title", threadId);
      const workerId = "ordered-action-worker";
      const claim = outbox.claimNext({
        workerId,
        leaseDurationMs: 30_000,
        excludeTitleGeneration: true,
      });
      yield* outbox.enqueue([first]);
      assert.equal(Option.getOrThrow(yield* claim).id, first.id);
      assert.isTrue(
        yield* outbox.retry({ effectId: first.id, workerId, error: "retry", delayMs: 1_000 }),
      );
      const retryAt = Option.getOrThrow(yield* outbox.get(first.id)).availableAt;
      yield* outbox.enqueue([later, title, laterTitle]);

      assert.isTrue(Option.isNone(yield* claim));
      assert.equal(
        DateTime.formatIso(
          Option.getOrThrow(yield* outbox.nextClaimableAt({ excludeTitleGeneration: true })),
        ),
        retryAt,
      );
      assert.equal(
        DateTime.formatIso(Option.getOrThrow(yield* outbox.nextClaimableAt())),
        Option.getOrThrow(yield* outbox.get(title.id)).availableAt,
      );
      assert.equal(
        Option.getOrThrow(yield* outbox.claimNext({ workerId, leaseDurationMs: 30_000 })).id,
        title.id,
      );
      assert.isTrue(Option.isNone(yield* outbox.claimNext({ workerId, leaseDurationMs: 30_000 })));
      yield* TestClock.adjust("999 millis");
      assert.isTrue(Option.isNone(yield* claim));
      yield* TestClock.adjust("1 millis");
      assert.equal(Option.getOrThrow(yield* claim).id, first.id);
      assert.isTrue(Option.isNone(yield* claim));
      assert.isTrue(Option.isNone(yield* outbox.nextClaimableAt({ excludeTitleGeneration: true })));
      assert.isTrue(yield* outbox.succeed({ effectId: first.id, workerId }));
      assert.equal(Option.getOrThrow(yield* claim).id, later.id);
      assert.equal(Option.getOrThrow(yield* outbox.get(title.id)).status, "running");
      assert.equal(Option.getOrThrow(yield* outbox.get(laterTitle.id)).attemptCount, 0);
      assert.isTrue(yield* outbox.succeed({ effectId: title.id, workerId }));
      assert.equal(
        Option.getOrThrow(yield* outbox.claimNext({ workerId, leaseDurationMs: 30_000 })).id,
        laterTitle.id,
      );
    }).pipe(Effect.scoped, Effect.provide(layerOutbox.pipe(Layer.provideMerge(TestClock.layer())))),
);

it.effect("CORE-A4 keeps one worker mixed and serial at concurrency one", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    const threadId = ThreadId.make("thread:single-admission-worker");
    const title = pending("a-single-title", "title", threadId);
    const action = pending("b-single-action", "action", threadId);
    yield* f.outbox.enqueue([title, action]);
    yield* f.start(1);
    assert.equal(yield* Queue.take(f.started), title.id);
    yield* TestClock.adjust("0 millis");
    assert.equal(Option.getOrThrow(yield* f.outbox.get(action.id)).attemptCount, 0);
    assert.deepEqual(yield* Ref.get(f.attempts), { urgent: 0, action: 0, mixed: 1 });
    yield* f.release(title.id);
    yield* f.outbox.awaitCompletion([title.id]);
    assert.equal(yield* Queue.take(f.started), action.id);
    assert.deepEqual(yield* Ref.get(f.attempts), { urgent: 0, action: 0, mixed: 2 });
  }).pipe(Effect.scoped, Effect.provide(layerOutbox.pipe(Layer.provideMerge(TestClock.layer())))),
);

it.effect("CORE-A4 runs an action at its durable deadline while a title is held", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    const title = pending("held-deadline-title", "title");
    const action = {
      ...pending("future-action", "action"),
      availableAt: DateTime.add(yield* DateTime.now, { milliseconds: 100 }),
    };
    yield* f.outbox.enqueue([title, action]);
    yield* f.start(2);
    assert.equal(yield* Queue.take(f.started), title.id);
    yield* TestClock.adjust("99 millis");
    assert.equal(Option.getOrThrow(yield* f.outbox.get(action.id)).attemptCount, 0);
    yield* TestClock.adjust("1 millis");
    assert.equal(Option.getOrThrow(yield* f.outbox.get(action.id)).status, "running");
    assert.equal(yield* Queue.take(f.started), action.id);
    assert.equal(Option.getOrThrow(yield* f.outbox.get(title.id)).status, "running");
  }).pipe(Effect.scoped, Effect.provide(layerOutbox.pipe(Layer.provideMerge(TestClock.layer())))),
);

it.effect("CORE-A4 retains future title deadlines for mixed workers", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    const title = {
      ...pending("future-title", "title"),
      availableAt: DateTime.add(yield* DateTime.now, { milliseconds: 100 }),
    };
    yield* f.outbox.enqueue([title]);
    yield* f.start(2);
    yield* TestClock.adjust("99 millis");
    assert.equal(Option.getOrThrow(yield* f.outbox.get(title.id)).attemptCount, 0);
    yield* TestClock.adjust("1 millis");
    assert.equal(Option.getOrThrow(yield* f.outbox.get(title.id)).status, "running");
    assert.equal(yield* Queue.take(f.started), title.id);
    assert.equal(yield* Ref.get(f.attempts).pipe(Effect.map((counts) => counts.action)), 1);
  }).pipe(Effect.scoped, Effect.provide(layerOutbox.pipe(Layer.provideMerge(TestClock.layer())))),
);

it.effect("CORE-A4 stops the notification relay with the daemon scope", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    const daemon = yield* f.start(2);
    yield* TestClock.adjust("0 millis");
    yield* Fiber.interrupt(daemon);
    yield* f.outbox.notifyAvailable();
    const received = yield* Deferred.make<void>();
    yield* f.outbox.awaitAvailable.pipe(
      Effect.andThen(Deferred.succeed(received, undefined)),
      Effect.forkScoped,
    );
    yield* TestClock.adjust("0 millis");
    assert.isTrue(yield* Deferred.isDone(received));
  }).pipe(Effect.scoped, Effect.provide(layerOutbox.pipe(Layer.provideMerge(TestClock.layer())))),
);

it.effect("RES-1 wakes urgent and non-title workers independently with three executors", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    const title = pending("three-held-title", "title");
    yield* f.outbox.enqueue([title]);
    yield* f.start(3);
    assert.equal(yield* Queue.take(f.started), title.id);
    yield* TestClock.adjust("0 millis");

    const action = pending("three-action-wake", "action");
    yield* f.outbox.enqueue([action]);
    yield* f.outbox.notifyAvailable();
    yield* TestClock.adjust("0 millis");
    assert.equal(Option.getOrThrow(yield* f.outbox.get(action.id)).status, "running");
    assert.equal(yield* Queue.take(f.started), action.id);

    const urgent = pending("three-urgent-wake", "runtime-response");
    yield* f.outbox.enqueue([urgent]);
    yield* f.outbox.notifyAvailable();
    yield* TestClock.adjust("0 millis");
    assert.equal(Option.getOrThrow(yield* f.outbox.get(urgent.id)).status, "running");
    assert.equal(yield* Queue.take(f.started), urgent.id);
    assert.deepEqual(yield* Ref.get(f.executions), { active: 3, maximum: 3 });
  }).pipe(Effect.scoped, Effect.provide(layerOutbox.pipe(Layer.provideMerge(TestClock.layer())))),
);

it.effect(
  "RES-1 retains two ordinary executors without an urgent reservation at concurrency two",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      const actions = [pending("two-action:0", "action"), pending("two-action:1", "action")];
      yield* f.outbox.enqueue(actions);
      yield* f.start(2);
      yield* TestClock.adjust("0 millis");
      assert.deepEqual(yield* Ref.get(f.executions), { active: 2, maximum: 2 });
      const urgent = pending("two-waiting-urgent", "interrupt");
      yield* f.outbox.enqueue([urgent]);
      yield* f.outbox.notifyAvailable();
      yield* TestClock.adjust("0 millis");
      assert.equal(Option.getOrThrow(yield* f.outbox.get(urgent.id)).attemptCount, 0);
      yield* f.release(actions[0]!.id);
      yield* f.outbox.awaitCompletion([actions[0]!.id]);
      yield* TestClock.adjust("0 millis");
      assert.equal(Option.getOrThrow(yield* f.outbox.get(urgent.id)).status, "running");
      assert.deepEqual(yield* Ref.get(f.executions), { active: 2, maximum: 2 });
    }).pipe(Effect.scoped, Effect.provide(layerOutbox.pipe(Layer.provideMerge(TestClock.layer())))),
);

it.effect("RES-1 urgent claims and deadlines share the allowlist and durable availability", () =>
  Effect.gen(function* () {
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const now = yield* DateTime.now;
    const steer: EffectOutbox.PendingOrchestrationEffectV2 = {
      ...pending("excluded-steer", "action"),
      request: {
        type: "provider-turn.steer",
        providerSessionId: ProviderSessionId.make("session:steer"),
        providerThreadId: ProviderThreadId.make("provider-thread:steer"),
        providerTurnId: ProviderTurnId.make("turn:steer"),
        messageId: MessageId.make("message:steer"),
      },
    };
    const excluded = [
      pending("excluded-action", "action"),
      pending("excluded-title", "title"),
      steer,
    ];
    const urgent = (["interrupt", "delegated-stop", "runtime-response"] as const).map(
      (kind, index) => ({
        ...pending(`future-urgent:${index}`, kind),
        availableAt: DateTime.add(now, { milliseconds: 100 }),
      }),
    );
    yield* outbox.enqueue([...excluded, ...urgent]);
    const claim = outbox.claimNext({
      workerId: "urgent-eligibility",
      leaseDurationMs: 30_000,
      urgentOnly: true,
    });
    assert.isTrue(Option.isNone(yield* claim));
    assert.equal(
      DateTime.toEpochMillis(
        Option.getOrThrow(yield* outbox.nextClaimableAt({ urgentOnly: true })),
      ),
      DateTime.toEpochMillis(now) + 100,
    );
    yield* TestClock.adjust("99 millis");
    assert.isTrue(Option.isNone(yield* claim));
    yield* TestClock.adjust("1 millis");
    for (const effect of urgent) {
      assert.equal(Option.getOrThrow(yield* claim).id, effect.id);
    }
    assert.isTrue(Option.isNone(yield* claim));
    assert.isTrue(Option.isNone(yield* outbox.nextClaimableAt({ urgentOnly: true })));
    for (const effect of excluded) {
      assert.equal(Option.getOrThrow(yield* outbox.get(effect.id)).attemptCount, 0);
    }
  }).pipe(Effect.scoped, Effect.provide(layerOutbox.pipe(Layer.provideMerge(TestClock.layer())))),
);

it.effect(
  "RES-1 preserves same-thread predecessors across backoff and running leases for urgent work",
  () =>
    Effect.gen(function* () {
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const threadId = ThreadId.make("thread:urgent-predecessor");
      const first = pending("first-ordinary-predecessor", "action", threadId);
      const urgent = pending("later-urgent", "interrupt", threadId);
      const workerId = "urgent-predecessor-worker";
      yield* outbox.enqueue([first]);
      assert.equal(
        Option.getOrThrow(yield* outbox.claimNext({ workerId, leaseDurationMs: 30_000 })).id,
        first.id,
      );
      assert.isTrue(
        yield* outbox.retry({ effectId: first.id, workerId, error: "retry", delayMs: 1_000 }),
      );
      yield* outbox.enqueue([urgent]);
      const claim = outbox.claimNext({ workerId, leaseDurationMs: 30_000, urgentOnly: true });
      assert.isTrue(Option.isNone(yield* claim));
      assert.isTrue(Option.isNone(yield* outbox.nextClaimableAt({ urgentOnly: true })));
      yield* TestClock.adjust("1 second");
      assert.isTrue(Option.isNone(yield* claim));
      assert.isTrue(Option.isNone(yield* outbox.nextClaimableAt({ urgentOnly: true })));
      assert.equal(
        Option.getOrThrow(yield* outbox.claimNext({ workerId, leaseDurationMs: 30_000 })).id,
        first.id,
      );
      assert.isTrue(Option.isNone(yield* claim));
      assert.isTrue(Option.isNone(yield* outbox.nextClaimableAt({ urgentOnly: true })));
      assert.isTrue(yield* outbox.succeed({ effectId: first.id, workerId }));
      assert.isTrue(Option.isSome(yield* outbox.nextClaimableAt({ urgentOnly: true })));
      assert.equal(Option.getOrThrow(yield* claim).id, urgent.id);
      assert.isTrue(
        yield* outbox.retry({ effectId: urgent.id, workerId, error: "retry", delayMs: 100 }),
      );
      const retryAt = Option.getOrThrow(yield* outbox.get(urgent.id)).availableAt;
      assert.equal(
        DateTime.formatIso(Option.getOrThrow(yield* outbox.nextClaimableAt({ urgentOnly: true }))),
        retryAt,
      );
      yield* TestClock.adjust("99 millis");
      assert.isTrue(Option.isNone(yield* claim));
      yield* TestClock.adjust("1 millis");
      assert.equal(Option.getOrThrow(yield* claim).id, urgent.id);
    }).pipe(Effect.scoped, Effect.provide(layerOutbox.pipe(Layer.provideMerge(TestClock.layer())))),
);

it.effect("RES-1 ignores ineligible due rows until a broadcast or liveness wake", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    const actions = Array.from({ length: 4 }, (_, index) =>
      pending(`held-ordinary:${index}`, "action"),
    );
    const blockedUrgent = pending("blocked-urgent", "interrupt", actions[0]!.threadId);
    yield* f.outbox.enqueue([...actions, blockedUrgent]);
    yield* f.start();
    yield* TestClock.adjust("0 millis");
    assert.deepEqual(yield* Ref.get(f.attempts), { urgent: 1, action: 1, mixed: 2 });
    yield* TestClock.adjust("29999 millis");
    assert.deepEqual(yield* Ref.get(f.attempts), { urgent: 1, action: 1, mixed: 2 });
    yield* TestClock.adjust("1 millis");
    assert.deepEqual(yield* Ref.get(f.attempts), { urgent: 2, action: 1, mixed: 2 });
    yield* f.outbox.notifyAvailable();
    yield* TestClock.adjust("0 millis");
    assert.deepEqual(yield* Ref.get(f.attempts), { urgent: 3, action: 1, mixed: 2 });
    assert.equal(Option.getOrThrow(yield* f.outbox.get(blockedUrgent.id)).attemptCount, 0);
    assert.equal(Option.getOrThrow(yield* f.outbox.get(actions[3]!.id)).attemptCount, 0);
  }).pipe(Effect.scoped, Effect.provide(layerOutbox.pipe(Layer.provideMerge(TestClock.layer())))),
);

it.effect(
  "RES-1 runs an eligible urgent effect at its deadline while ordinary capacity is full",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      const actions = Array.from({ length: 4 }, (_, index) =>
        pending(`deadline-ordinary:${index}`, "action"),
      );
      const urgent = {
        ...pending("deadline-urgent", "delegated-stop"),
        availableAt: DateTime.add(yield* DateTime.now, { milliseconds: 100 }),
      };
      yield* f.outbox.enqueue([...actions, urgent]);
      yield* f.start();
      yield* TestClock.adjust("99 millis");
      assert.equal(Option.getOrThrow(yield* f.outbox.get(urgent.id)).attemptCount, 0);
      assert.equal(yield* Ref.get(f.attempts).pipe(Effect.map((counts) => counts.urgent)), 1);
      yield* TestClock.adjust("1 millis");
      assert.equal(Option.getOrThrow(yield* f.outbox.get(urgent.id)).status, "running");
      assert.deepEqual(yield* Ref.get(f.executions), { active: 4, maximum: 4 });
      assert.equal(Option.getOrThrow(yield* f.outbox.get(actions[3]!.id)).attemptCount, 0);
    }).pipe(Effect.scoped, Effect.provide(layerOutbox.pipe(Layer.provideMerge(TestClock.layer())))),
);
