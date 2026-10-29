import { assert, it } from "@effect/vitest";
import { CommandId, RunId, ThreadId } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import { TestClock } from "effect/testing";

import * as EffectOutbox from "../../../orchestration-v2/EffectOutbox.ts";
import * as EffectWorker from "../../../orchestration-v2/EffectWorker.ts";
import * as Sqlite from "../../../persistence/Sqlite.ts";
import { makeCodexProducerRetirement } from "./CodexRetirement.ts";
import { CODEX_STARTUP_BUDGET_MS, makeCodexStartup } from "./CodexStartup.ts";

const database = Sqlite.layerMemory;
const testLayer = EffectOutbox.layer.pipe(Layer.provideMerge(database));

it.effect(
  "the default daemon recovers three ordinary slots before a silent producer physically exits",
  () =>
    Effect.gen(function* () {
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const startup = makeCodexStartup();
      const scope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
      const producer = { generation: "silent-startup-producer", scope };
      const healthyScope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(healthyScope, Exit.void));
      const healthyProducer = { generation: "independent-healthy-producer", scope: healthyScope };
      const lifecyclePermit = yield* Semaphore.make(1);
      const entered = yield* Queue.unbounded<string>();
      const sent = yield* Deferred.make<void>();
      const response = yield* Deferred.make<string>();
      const observed = yield* Deferred.make<void>();
      const physicalExit = yield* Deferred.make<void>();
      const healthyFinished = yield* Deferred.make<void>();
      let dispatches = 0;
      let healthyDispatches = 0;
      let bindings = 0;
      let closes = 0;
      const settlements = new Map<string, number>();
      const retirement = yield* makeCodexProducerRetirement(
        Effect.sync(() => closes++).pipe(Effect.andThen(Scope.close(scope, Exit.void))),
      );
      retirement.captureProcessExit(Deferred.await(physicalExit));
      yield* retirement.observeProcessExit;
      yield* Effect.addFinalizer(() =>
        Deferred.succeed(physicalExit, undefined).pipe(Effect.asVoid),
      );
      const effects: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2> = Array.from(
        { length: 4 },
        (_, index) => ({
          id: `effect:startup:${index}`,
          commandId: CommandId.make(`command:startup:${index}`),
          threadId: ThreadId.make(`thread:startup:${index}`),
          request: { type: "provider-turn.start", runId: RunId.make(`run:startup:${index}`) },
        }),
      );
      const executor = Layer.succeed(EffectWorker.OrchestrationEffectExecutorV2, {
        execute: (effect) =>
          Effect.gen(function* () {
            yield* Queue.offer(entered, effect.id);
            if (effect.id === effects[3]!.id) {
              yield* startup.run(
                "ensure",
                startup.withinPermit(
                  startup.observe(healthyProducer, Effect.sync(() => ++healthyDispatches)),
                ),
              ).pipe(Effect.orDie);
              yield* Deferred.succeed(healthyFinished, undefined);
              return;
            }
            const result = yield* startup
              .run(
                "ensure",
                lifecyclePermit.withPermits(1)(
                  startup.withinPermit(
                    startup.observe(
                      producer,
                      Effect.sync(() => dispatches++).pipe(
                        Effect.andThen(Deferred.succeed(sent, undefined)),
                        Effect.andThen(Deferred.await(response)),
                        Effect.tap(() => Deferred.succeed(observed, undefined)),
                      ),
                    ).pipe(Effect.tap(() => Effect.sync(() => bindings++))),
                  ),
                ),
              )
              .pipe(Effect.result);
            assert.equal(result._tag, "Failure");
            // A handled provider load failure settles its run and returns to the
            // worker; the real start service's terminal handling is tested separately.
            settlements.set(effect.id, (settlements.get(effect.id) ?? 0) + 1);
          }),
      });
      yield* outbox.enqueue(effects);
      const daemon = yield* EffectWorker.runDaemon.pipe(
        Effect.provide(EffectWorker.layer.pipe(Layer.provide(executor))),
        Effect.forkScoped,
      );
      const occupied = yield* Effect.forEach([0, 1, 2], () => Queue.take(entered));
      assert.sameMembers(occupied, effects.slice(0, 3).map((effect) => effect.id));
      yield* Deferred.await(sent);
      yield* TestClock.adjust(0);
      assert.equal(dispatches, 1, "two claimed starts wait for the same lifecycle permit");
      assert.isFalse(yield* Deferred.isDone(healthyFinished));
      assert.equal(Option.getOrThrow(yield* outbox.get(effects[3]!.id)).attemptCount, 0);

      yield* TestClock.adjust(CODEX_STARTUP_BUDGET_MS);
      yield* Deferred.await(healthyFinished);
      yield* outbox.awaitCompletion(effects.map((effect) => effect.id));
      assert.isFalse(yield* Deferred.isDone(physicalExit));
      assert.equal(closes, 0, "elapsed time cannot close a shared producer");
      assert.isTrue(startup.isUncertain(producer));
      assert.isFalse(startup.isUncertain(healthyProducer));
      assert.equal(dispatches, 1);
      assert.equal(healthyDispatches, 1);
      for (const effect of effects) {
        const row = Option.getOrThrow(yield* outbox.get(effect.id));
        assert.equal(row.status, "succeeded");
        assert.equal(row.attemptCount, 1);
        assert.isNull(row.leaseOwner);
        if (effect.id !== effects[3]!.id) assert.equal(settlements.get(effect.id), 1);
      }

      yield* Deferred.succeed(response, "late-unbound-thread");
      yield* Deferred.await(observed);
      assert.equal(bindings, 0);
      assert.equal(dispatches, 1);
      assert.isTrue(startup.isUncertain(producer));
      const closing = yield* retirement.close.pipe(Effect.forkChild);
      yield* TestClock.adjust(0);
      assert.isUndefined(closing.pollUnsafe(), "completed scope finalizers are not physical exit");
      yield* Deferred.succeed(physicalExit, undefined);
      yield* Fiber.join(closing);
      assert.equal(closes, 1);
      yield* Fiber.interrupt(daemon);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
);
