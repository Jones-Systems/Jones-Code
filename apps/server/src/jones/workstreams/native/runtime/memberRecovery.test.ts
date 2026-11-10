import { expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import { RegistryError } from "../registry/session.ts";
import * as Application from "./memberApplication.ts";
import * as Worker from "./memberRecovery.ts";

// Only the application boundary is synthetic here; the composed authenticated H/M suite uses the real application.
const worker = (recoverOpen: Application.MemberApplication["Service"]["recoverOpen"]) =>
  Worker.layer.pipe(Layer.provide(Layer.mock(Application.MemberApplication)({ recoverOpen })));

it.effect("starts once, waits for completion and never queues missed cadence ticks", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const completed = yield* Deferred.make<void>();
    const second = yield* Deferred.make<void>();
    let calls = 0;
    yield* Effect.scoped(
      Effect.gen(function* () {
        yield* Worker.MemberRecovery;
        yield* Deferred.await(started);
        yield* TestClock.adjust(90_000);
        expect(calls).toBe(1);
        yield* Deferred.succeed(release, undefined);
        yield* Deferred.await(completed);
        yield* TestClock.adjust(29_999);
        expect(calls).toBe(1);
        yield* TestClock.adjust(1);
        yield* Deferred.await(second);
        expect(calls).toBe(2);
      }).pipe(
        Effect.provide(
          worker((options) =>
            Effect.gen(function* () {
              expect(options).toEqual({ dueOnly: true });
              calls++;
              if (calls === 1) {
                yield* Deferred.succeed(started, undefined);
                yield* Deferred.await(release);
                yield* Deferred.succeed(completed, undefined);
              } else yield* Deferred.succeed(second, undefined);
              return [];
            }),
          ),
        ),
      ),
    );
    yield* TestClock.adjust(300_000);
    expect(calls).toBe(2);
  }),
);

it.effect("coalesces overlapping timer/manual sweeps and shares their actual completion", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    let calls = 0;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const owner = yield* Worker.MemberRecovery;
        yield* Deferred.await(started);
        const waiters = yield* Effect.forEach([1, 2, 3, 4], () =>
          owner.sweep().pipe(Effect.forkScoped),
        );
        yield* TestClock.adjust(0);
        yield* Deferred.succeed(release, undefined);
        expect(yield* Effect.forEach(waiters, Fiber.join)).toEqual([[], [], [], []]);
        expect(calls).toBe(1);
      }).pipe(
        Effect.provide(
          worker(() =>
            Effect.gen(function* () {
              calls++;
              yield* Deferred.succeed(started, undefined);
              yield* Deferred.await(release);
              return [];
            }),
          ),
        ),
      ),
    );
  }),
);

it.effect("typed failures wait for the next cadence and later sweeps can recover", () =>
  Effect.gen(function* () {
    const first = yield* Deferred.make<void>();
    const second = yield* Deferred.make<void>();
    let calls = 0;
    yield* Effect.scoped(
      Effect.gen(function* () {
        yield* Worker.MemberRecovery;
        yield* Deferred.await(first);
        yield* TestClock.adjust(29_999);
        expect(calls).toBe(1);
        yield* TestClock.adjust(1);
        yield* Deferred.await(second);
        expect(calls).toBe(2);
      }).pipe(
        Effect.provide(
          worker(() =>
            Effect.gen(function* () {
              calls++;
              if (calls === 1) {
                yield* Deferred.succeed(first, undefined);
                return yield* new RegistryError({ code: "authority_fenced" });
              }
              yield* Deferred.succeed(second, undefined);
              return [];
            }),
          ),
        ),
      ),
    );
  }),
);

it.effect("leaving a waiter does not cancel the application's active recovery", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const completed = yield* Deferred.make<void>();
    let interrupted = false;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const owner = yield* Worker.MemberRecovery;
        yield* Deferred.await(started);
        const waiter = yield* owner.sweep().pipe(Effect.forkScoped);
        yield* Fiber.interrupt(waiter);
        expect(interrupted).toBe(false);
        const completion = yield* owner.sweep().pipe(Effect.forkScoped);
        yield* TestClock.adjust(0);
        yield* Deferred.succeed(release, undefined);
        yield* Deferred.await(completed);
        expect(yield* Fiber.join(completion)).toEqual([]);
      }).pipe(
        Effect.provide(
          worker(() =>
            Effect.gen(function* () {
              yield* Deferred.succeed(started, undefined);
              yield* Deferred.await(release);
              yield* Deferred.succeed(completed, undefined);
              return [];
            }).pipe(
              Effect.onInterrupt(() =>
                Effect.sync(() => {
                  interrupted = true;
                }),
              ),
            ),
          ),
        ),
      ),
    );
    expect(interrupted).toBe(false);
  }),
);

it.effect("disposing the application interrupts active work and fences escaped references", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    const interrupted = yield* Deferred.make<void>();
    let calls = 0;
    const owner = yield* Effect.scoped(
      Effect.gen(function* () {
        const service = yield* Worker.MemberRecovery;
        yield* Deferred.await(started);
        return service;
      }).pipe(
        Effect.provide(
          worker(() =>
            Effect.gen(function* () {
              calls++;
              yield* Deferred.succeed(started, undefined);
              return yield* Effect.never;
            }).pipe(Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined))),
          ),
        ),
      ),
    );
    yield* Deferred.await(interrupted);
    expect(yield* owner.sweep().pipe(Effect.flip)).toMatchObject({ code: "authority_fenced" });
    yield* TestClock.adjust(300_000);
    expect(calls).toBe(1);
  }),
);

it.effect("manual typed failures remain observable and release the pending result", () =>
  Effect.gen(function* () {
    const first = yield* Deferred.make<void>();
    let calls = 0;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const owner = yield* Worker.MemberRecovery;
        yield* Deferred.await(first);
        yield* TestClock.adjust(1);
        expect(yield* owner.sweep().pipe(Effect.flip)).toMatchObject({ code: "unavailable" });
        expect(yield* owner.sweep()).toEqual([]);
        expect(calls).toBe(3);
      }).pipe(
        Effect.provide(
          worker(() =>
            Effect.gen(function* () {
              calls++;
              if (calls === 2) return yield* new RegistryError({ code: "unavailable" });
              yield* Deferred.succeed(first, undefined);
              return [];
            }),
          ),
        ),
      ),
    );
  }),
);
