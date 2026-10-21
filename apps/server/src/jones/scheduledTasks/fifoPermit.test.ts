import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { makeFifoPermit } from "./fifoPermit.ts";

it.effect("a caller arriving as the permit is released waits behind the one already waiting", () =>
  Effect.gen(function* () {
    const permit = yield* makeFifoPermit;
    const order: Array<string> = [];
    const firstHeld = yield* Deferred.make<void>();
    const finishFirst = yield* Deferred.make<void>();
    const first = yield* permit
      .withPermit(
        Deferred.succeed(firstHeld, undefined).pipe(Effect.andThen(Deferred.await(finishFirst))),
      )
      .pipe(Effect.forkChild);
    yield* Deferred.await(firstHeld);
    const waiting = yield* permit
      .withPermit(Effect.sync(() => order.push("waiting")))
      .pipe(Effect.forkChild);
    yield* Effect.yieldNow;

    // The release and the next request happen in one step, before the waiter can wake.
    yield* Deferred.succeed(finishFirst, undefined);
    yield* Fiber.join(first);
    yield* permit.withPermit(Effect.sync(() => order.push("late")));
    yield* Fiber.join(waiting);

    assert.deepEqual(order, ["waiting", "late"]);
  }),
);

it.effect("an interrupted waiter leaves the line without holding up the next one", () =>
  Effect.gen(function* () {
    const permit = yield* makeFifoPermit;
    const order: Array<string> = [];
    const finishFirst = yield* Deferred.make<void>();
    const first = yield* permit.withPermit(Deferred.await(finishFirst)).pipe(Effect.forkChild);
    yield* Effect.yieldNow;
    const leaving = yield* permit
      .withPermit(Effect.sync(() => order.push("leaving")))
      .pipe(Effect.forkChild);
    const next = yield* permit
      .withPermit(Effect.sync(() => order.push("next")))
      .pipe(Effect.forkChild);
    yield* Effect.yieldNow;

    yield* Fiber.interrupt(leaving);
    yield* Deferred.succeed(finishFirst, undefined);
    yield* Fiber.join(first);
    yield* Fiber.join(next);

    assert.deepEqual(order, ["next"]);
  }),
);

it.effect("an interrupted holder releases the permit", () =>
  Effect.gen(function* () {
    const permit = yield* makeFifoPermit;
    const holder = yield* permit.withPermit(Effect.never).pipe(Effect.forkChild);
    yield* Effect.yieldNow;
    const next = yield* permit.withPermit(Effect.succeed("ran")).pipe(Effect.forkChild);
    yield* Effect.yieldNow;

    yield* Fiber.interrupt(holder);

    assert.equal(yield* Fiber.join(next), "ran");
  }),
);
