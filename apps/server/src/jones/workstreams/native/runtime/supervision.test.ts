import { expect, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import { captureLogs } from "./supervision.testkit.ts";
import { makeSingleFlight, superviseLoop } from "./supervision.ts";

it.effect(
  "consecutive failures back off to a bounded wait and one success restores the cadence",
  () => {
    const capture = captureLogs();
    let calls = 0;
    let healthy = false;
    return Effect.scoped(
      Effect.gen(function* () {
        yield* superviseLoop({
          loop: "unit",
          code: (error: string) => error,
          iteration: Effect.suspend(() => {
            calls++;
            return healthy ? Effect.void : Effect.fail("synthetic");
          }),
        }).pipe(Effect.forkScoped);
        yield* TestClock.adjust(0);
        expect(calls).toBe(1);
        // The first failure keeps the base cadence; every further one doubles the wait up to the cap.
        const waits = [30_000, 60_000, 120_000, 240_000, 300_000, 300_000];
        for (const [index, wait] of waits.entries()) {
          yield* TestClock.adjust(wait - 1);
          expect(calls).toBe(index + 1);
          yield* TestClock.adjust(1);
          expect(calls).toBe(index + 2);
        }
        expect(capture.at("Warn").map((entry) => entry.annotations.retryInMs)).toEqual([
          30_000, 60_000, 120_000, 240_000, 300_000, 300_000, 300_000,
        ]);
        expect(capture.at("Warn").map((entry) => entry.annotations.consecutiveFailures)).toEqual([
          1, 2, 3, 4, 5, 6, 7,
        ]);
        healthy = true;
        yield* TestClock.adjust(300_000);
        expect(calls).toBe(8);
        expect(capture.at("Info").map((entry) => entry.annotations)).toEqual([
          { loop: "unit", afterFailures: 7 },
        ]);
        yield* TestClock.adjust(29_999);
        expect(calls).toBe(8);
        yield* TestClock.adjust(1);
        expect(calls).toBe(9);
        // Healthy iterations log nothing.
        expect(capture.logs).toHaveLength(8);
      }).pipe(Effect.provide(capture.layer)),
    );
  },
);

it.effect("results reach onSuccess only from successful iterations", () => {
  const seen: Array<number> = [];
  let calls = 0;
  return Effect.scoped(
    Effect.gen(function* () {
      yield* superviseLoop({
        loop: "unit",
        code: (error: string) => error,
        iteration: Effect.suspend(() => {
          calls++;
          return calls === 2 ? Effect.fail("synthetic") : Effect.succeed(calls);
        }),
        onSuccess: (result) => Effect.sync(() => void seen.push(result)),
      }).pipe(Effect.forkScoped);
      yield* TestClock.adjust(0);
      yield* TestClock.adjust(30_000);
      yield* TestClock.adjust(30_000);
      expect(calls).toBe(3);
      expect(seen).toEqual([1, 3]);
    }),
  );
});

it.effect("interruption of the iteration ends the loop without a failure record", () => {
  const capture = captureLogs();
  return Effect.gen(function* () {
    const loop = yield* superviseLoop({
      loop: "unit",
      code: (error: string) => error,
      iteration: Effect.interrupt,
    }).pipe(Effect.forkChild);
    const exit = yield* Fiber.await(loop);
    expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true);
    expect(capture.at("Warn")).toHaveLength(0);
  }).pipe(Effect.provide(capture.layer));
});

it.effect("a busy flight skips without waiting and is released when its run completes", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const flight = yield* makeSingleFlight<string, never>({
        whenBusy: { skip: "busy" },
        starterCancels: true,
      });
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let bodies = 0;
      const body = Effect.gen(function* () {
        bodies++;
        yield* Deferred.succeed(started, undefined);
        yield* Deferred.await(release);
        return "ran";
      });
      const first = yield* flight.run(body).pipe(Effect.forkScoped);
      yield* Deferred.await(started);
      expect(
        yield* Effect.all([flight.run(body), flight.run(body)], { concurrency: "unbounded" }),
      ).toEqual(["busy", "busy"]);
      yield* Deferred.succeed(release, undefined);
      expect(yield* Fiber.join(first)).toBe("ran");
      expect(bodies).toBe(1);
      expect(yield* flight.run(Effect.succeed("again"))).toBe("again");
    }),
  ),
);

it.effect("joined callers share one run's outcome, including its failure", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const flight = yield* makeSingleFlight<string, string>({
        whenBusy: "join",
        starterCancels: false,
      });
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let bodies = 0;
      const body = Effect.gen(function* () {
        bodies++;
        yield* Deferred.succeed(started, undefined);
        yield* Deferred.await(release);
        return yield* Effect.fail("synthetic");
      });
      const callers = yield* Effect.forEach([1, 2, 3], () =>
        flight.run(body).pipe(Effect.result, Effect.forkScoped),
      );
      yield* Deferred.await(started);
      yield* TestClock.adjust(0);
      yield* Deferred.succeed(release, undefined);
      const results = yield* Effect.forEach(callers, Fiber.join);
      expect(results.map((result) => result._tag)).toEqual(["Failure", "Failure", "Failure"]);
      expect(bodies).toBe(1);
      expect(yield* flight.run(Effect.succeed("next"))).toBe("next");
    }),
  ),
);

it.effect("a defect releases the flight so the next caller starts a new run", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const flight = yield* makeSingleFlight<string, never>({
        whenBusy: { skip: "busy" },
        starterCancels: true,
      });
      const defect = yield* Effect.exit(flight.run(Effect.die(new Error("synthetic defect"))));
      expect(Exit.isFailure(defect) && Cause.hasDies(defect.cause)).toBe(true);
      expect(yield* flight.run(Effect.succeed("recovered"))).toBe("recovered");
    }),
  ),
);

it.effect(
  "starterCancels interrupts the run with the caller that started it and frees the flight",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const flight = yield* makeSingleFlight<string, never>({
          whenBusy: { skip: "busy" },
          starterCancels: true,
        });
        const started = yield* Deferred.make<void>();
        const interrupted = yield* Deferred.make<void>();
        const starter = yield* flight
          .run(
            Deferred.succeed(started, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined)),
            ),
          )
          .pipe(Effect.forkScoped);
        yield* Deferred.await(started);
        yield* Fiber.interrupt(starter);
        yield* Deferred.await(interrupted);
        expect(yield* flight.run(Effect.succeed("fresh"))).toBe("fresh");
      }),
    ),
);

it.effect("without starterCancels a departing starter leaves the run to the owner scope", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const interrupted = yield* Deferred.make<void>();
    let bodies = 0;
    const body = Effect.gen(function* () {
      bodies++;
      yield* Deferred.succeed(started, undefined);
      yield* Deferred.await(release);
      return "ran";
    }).pipe(Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined)));
    yield* Effect.scoped(
      Effect.gen(function* () {
        const flight = yield* makeSingleFlight<string, never>({
          whenBusy: "join",
          starterCancels: false,
        });
        const starter = yield* flight.run(body).pipe(Effect.forkScoped);
        yield* Deferred.await(started);
        yield* Fiber.interrupt(starter);
        // The departed starter did not cancel the run: a later caller joins it instead of starting another.
        expect(yield* Deferred.isDone(interrupted)).toBe(false);
        const joiner = yield* flight.run(body).pipe(Effect.forkScoped);
        yield* TestClock.adjust(0);
        yield* Deferred.succeed(release, undefined);
        expect(yield* Fiber.join(joiner)).toBe("ran");
        expect(bodies).toBe(1);
      }),
    );
  }),
);

it.effect("closing the owner scope interrupts the run in flight", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    const interrupted = yield* Deferred.make<void>();
    yield* Effect.scoped(
      Effect.gen(function* () {
        const flight = yield* makeSingleFlight<string, never>({
          whenBusy: "join",
          starterCancels: false,
        });
        yield* flight
          .run(
            Deferred.succeed(started, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined)),
            ),
          )
          .pipe(Effect.forkChild);
        yield* Deferred.await(started);
      }),
    );
    yield* Deferred.await(interrupted);
  }),
);
