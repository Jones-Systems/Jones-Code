import { assert, it } from "@effect/vitest";
import { CommandId, RunId, ThreadId } from "@t3tools/contracts";
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
  kind: "action" | "title",
  threadId = ThreadId.make(`thread:${id}`),
): EffectOutbox.PendingOrchestrationEffectV2 => ({
  id,
  commandId: CommandId.make(`command:${id}`),
  threadId,
  request:
    kind === "title"
      ? { type: "thread-title.generate", kind: { type: "regenerate" } }
      : { type: "provider-turn.start", runId: RunId.make(`run:${id}`) },
});

const makeFixture = Effect.gen(function* () {
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const started = yield* Queue.unbounded<string>();
  const attempts = yield* Ref.make({ action: 0, mixed: 0 });
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
        input.excludeTitleGeneration === true
          ? { ...counts, action: counts.action + 1 }
          : { ...counts, mixed: counts.mixed + 1 },
      ).pipe(Effect.andThen(outbox.claimNext(input))),
  });
  const executor = EffectWorker.OrchestrationEffectExecutorV2.of({
    execute: (effect) =>
      Queue.offer(started, effect.id).pipe(Effect.andThen(Deferred.await(gate(effect.id)))),
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
    started,
    release: (id: string) => Deferred.succeed(gate(id), undefined),
    start: (concurrency = 4) =>
      EffectWorker.runDaemonWithOptions({ concurrency, livenessPollIntervalMs: 30_000 }).pipe(
        Effect.provideService(EffectWorker.OrchestrationEffectWorkerV2, worker),
        Effect.forkScoped,
      ),
  };
});

it.effect("CORE-A4 reserves one of four workers for actions while three titles are held", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    const titles = Array.from({ length: 4 }, (_, index) => pending(`title:${index}`, "title"));
    yield* f.outbox.enqueue(titles);
    yield* f.start();
    const started = yield* Effect.forEach(titles.slice(0, 3), () => Queue.take(f.started));
    assert.deepEqual(
      started.toSorted(),
      titles.slice(0, 3).map(({ id }) => id),
    );

    yield* TestClock.adjust("1 second");
    const pendingTitle = Option.getOrThrow(yield* f.outbox.get(titles[3]!.id));
    assert.equal(pendingTitle.status, "pending");
    assert.equal(pendingTitle.attemptCount, 0);
    assert.equal(yield* Ref.get(f.attempts).pipe(Effect.map((counts) => counts.action)), 1);

    const action = pending("unrelated-action", "action");
    yield* f.outbox.enqueue([action]);
    yield* f.outbox.notifyAvailable();
    assert.equal(yield* Queue.take(f.started), action.id);
    for (const effect of [...titles.slice(0, 3), action]) {
      assert.equal(Option.getOrThrow(yield* f.outbox.get(effect.id)).status, "running");
    }
    assert.equal(Option.getOrThrow(yield* f.outbox.get(titles[3]!.id)).attemptCount, 0);
  }).pipe(Effect.scoped, Effect.provide(layerOutbox), Effect.provide(TestClock.layer())),
);

it.effect("CORE-A4 admits four concurrent actions without adding workers", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    const actions = Array.from({ length: 5 }, (_, index) => pending(`action:${index}`, "action"));
    yield* f.outbox.enqueue(actions);
    yield* f.start();
    const started = yield* Effect.forEach(actions.slice(0, 4), () => Queue.take(f.started));
    assert.deepEqual(
      started.toSorted(),
      actions.slice(0, 4).map(({ id }) => id),
    );
    yield* TestClock.adjust("0 millis");
    for (const action of actions.slice(0, 4)) {
      assert.equal(Option.getOrThrow(yield* f.outbox.get(action.id)).status, "running");
    }
    assert.equal(Option.getOrThrow(yield* f.outbox.get(actions[4]!.id)).attemptCount, 0);

    yield* f.release(actions[0]!.id);
    yield* f.outbox.awaitCompletion([actions[0]!.id]);
    assert.equal(yield* Queue.take(f.started), actions[4]!.id);
    for (const action of actions.slice(1)) {
      assert.equal(Option.getOrThrow(yield* f.outbox.get(action.id)).status, "running");
    }
  }).pipe(Effect.scoped, Effect.provide(layerOutbox), Effect.provide(TestClock.layer())),
);

it.effect("CORE-A4 wakes a mixed worker promptly when the reserved worker is already waiting", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    yield* f.start(2);
    yield* TestClock.adjust("0 millis");
    assert.deepEqual(yield* Ref.get(f.attempts), { action: 1, mixed: 1 });

    const title = pending("new-title-wake", "title");
    yield* f.outbox.enqueue([title]);
    yield* f.outbox.notifyAvailable();
    yield* TestClock.adjust("1 millis");
    assert.equal(Option.getOrThrow(yield* f.outbox.get(title.id)).status, "running");
    assert.equal(yield* Queue.take(f.started), title.id);
    assert.equal(yield* Ref.get(f.attempts).pipe(Effect.map((counts) => counts.action)), 2);
  }).pipe(Effect.scoped, Effect.provide(layerOutbox), Effect.provide(TestClock.layer())),
);

it.effect("CORE-A4 does not poll claims for due titles on the reserved worker", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    const titles = [pending("held-title", "title"), pending("waiting-title", "title")];
    yield* f.outbox.enqueue(titles);
    yield* f.start(2);
    assert.equal(yield* Queue.take(f.started), titles[0]!.id);
    yield* TestClock.adjust("0 millis");
    assert.deepEqual(yield* Ref.get(f.attempts), { action: 1, mixed: 1 });

    yield* TestClock.adjust("29999 millis");
    assert.deepEqual(yield* Ref.get(f.attempts), { action: 1, mixed: 1 });
    assert.equal(Option.getOrThrow(yield* f.outbox.get(titles[1]!.id)).attemptCount, 0);
    yield* TestClock.adjust("1 millis");
    assert.deepEqual(yield* Ref.get(f.attempts), { action: 2, mixed: 1 });
  }).pipe(Effect.scoped, Effect.provide(layerOutbox), Effect.provide(TestClock.layer())),
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
    }).pipe(Effect.scoped, Effect.provide(layerOutbox), Effect.provide(TestClock.layer())),
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
    assert.deepEqual(yield* Ref.get(f.attempts), { action: 0, mixed: 1 });
    yield* f.release(title.id);
    yield* f.outbox.awaitCompletion([title.id]);
    assert.equal(yield* Queue.take(f.started), action.id);
    assert.deepEqual(yield* Ref.get(f.attempts), { action: 0, mixed: 2 });
  }).pipe(Effect.scoped, Effect.provide(layerOutbox), Effect.provide(TestClock.layer())),
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
  }).pipe(Effect.scoped, Effect.provide(layerOutbox), Effect.provide(TestClock.layer())),
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
  }).pipe(Effect.scoped, Effect.provide(layerOutbox), Effect.provide(TestClock.layer())),
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
  }).pipe(Effect.scoped, Effect.provide(layerOutbox), Effect.provide(TestClock.layer())),
);
