import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import { OwnerSelectionIngress, withOwnerSelectionIngress } from "./ownerIngress.ts";

it.effect(
  "owner authority is scoped to the submitted effect across a startup worker boundary",
  () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<Effect.Effect<boolean>>();
      const observed = yield* Deferred.make<boolean>();
      const worker = yield* Queue.take(queue).pipe(
        Effect.flatten,
        Effect.flatMap((value) => Deferred.succeed(observed, value)),
        Effect.forkScoped,
      );
      assert.isFalse(yield* OwnerSelectionIngress);
      yield* Queue.offer(queue, OwnerSelectionIngress.pipe(withOwnerSelectionIngress));
      assert.isTrue(yield* Deferred.await(observed));
      yield* Fiber.join(worker);
      assert.isFalse(yield* OwnerSelectionIngress);
    }).pipe(Effect.scoped),
);
