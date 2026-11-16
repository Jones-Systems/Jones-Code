import { expect, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";

import * as FollowerSupervision from "./FollowerSupervision.ts";

it.effect.each(["defect", "typed failure"] as const)(
  "%s retries with the existing exponential delay capped at five minutes",
  (failure) =>
    Effect.gen(function* () {
      let attempts = 0;
      const fiber = yield* FollowerSupervision.retry(
        Effect.suspend(() => {
          attempts++;
          if (attempts === 11) return Effect.succeed("settled");
          return failure === "defect" ? Effect.die("synthetic defect") : Effect.fail("unavailable");
        }),
        { operation: "follow" },
      ).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      expect(attempts).toBe(1);
      for (const seconds of [2, 4, 8, 16, 32, 64, 128, 256, 300, 300]) {
        const before = attempts;
        yield* TestClock.adjust(`${seconds - 1} seconds`);
        expect(attempts).toBe(before);
        yield* TestClock.adjust("1 second");
        expect(attempts).toBe(before + 1);
      }
      expect(yield* Fiber.join(fiber)).toBe("settled");
    }),
);

it.effect.each(["alone", "with defect", "with typed failure"] as const)(
  "an interruption %s ends supervision without another attempt",
  (interruption) =>
    Effect.gen(function* () {
      let attempts = 0;
      const exit = yield* FollowerSupervision.retry(
        Effect.suspend(() => {
          attempts++;
          return Effect.failCause(
            interruption === "alone"
              ? Cause.interrupt()
              : Cause.combine(
                  Cause.interrupt(),
                  interruption === "with defect" ? Cause.die("defect") : Cause.fail("unavailable"),
                ),
          );
        }),
        { operation: "follow" },
      ).pipe(Effect.exit);
      expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true);
      yield* TestClock.adjust("10 minutes");
      expect(attempts).toBe(1);
    }),
);

it.effect("closing the owning scope during backoff never re-arms the operation", () =>
  Effect.gen(function* () {
    let attempts = 0;
    const failed = yield* Deferred.make<void>();
    const owner = yield* Effect.scoped(
      FollowerSupervision.retry(
        Effect.suspend(() => {
          attempts++;
          return Deferred.succeed(failed, undefined).pipe(
            Effect.andThen(Effect.die("synthetic defect")),
          );
        }),
        { operation: "scan" },
      ).pipe(Effect.forkScoped, Effect.andThen(Effect.never)),
    ).pipe(Effect.forkChild);
    yield* Deferred.await(failed);
    yield* Fiber.interrupt(owner);
    yield* TestClock.adjust("10 minutes");
    expect(attempts).toBe(1);
  }),
);
