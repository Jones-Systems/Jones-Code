import { expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Application from "../rollups/application.ts";
import * as Evidence from "../rollups/evidence.ts";
import * as TestClock from "effect/testing/TestClock";
import * as Worker from "./rollupWorker.ts";

const outcome = (status: Application.RunOutcome["status"]): Application.RunOutcome => ({
  status,
  discovered: 2,
  expired_claims: 1,
  overloaded: 0,
  inspected: [],
});
// Only the application port is synthetic; scheduling, overlap and scope ownership use the real worker.
const worker = (processOnce: Application.RollupApplication["Service"]["processOnce"]) =>
  Worker.layer.pipe(Layer.provide(Layer.succeed(Application.RollupApplication, { processOnce })));

it.effect(
  "starts immediately, waits for completion, then sweeps at a finite cadence without catch-up",
  () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const completed = yield* Deferred.make<void>();
      const second = yield* Deferred.make<void>();
      let calls = 0;
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* Worker.NativeRollupWorker;
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
            worker(() =>
              Effect.gen(function* () {
                calls++;
                if (calls === 1) {
                  yield* Deferred.succeed(started, undefined);
                  yield* Deferred.await(release);
                  yield* Deferred.succeed(completed, undefined);
                } else yield* Deferred.succeed(second, undefined);
                return outcome("published");
              }),
            ),
          ),
        ),
      );
      yield* TestClock.adjust(300_000);
      expect(calls).toBe(2);
    }),
);

it.effect(
  "known application failures release the sweep and recover on the next cadence without immediate retries",
  () =>
    Effect.gen(function* () {
      const failed = yield* Deferred.make<void>();
      const recovered = yield* Deferred.make<void>();
      let calls = 0;
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* Worker.NativeRollupWorker;
          yield* Deferred.await(failed);
          yield* TestClock.adjust(29_999);
          expect(calls).toBe(1);
          yield* TestClock.adjust(1);
          yield* Deferred.await(recovered);
          expect(calls).toBe(2);
        }).pipe(
          Effect.provide(
            worker(() =>
              Effect.gen(function* () {
                calls++;
                if (calls === 1) {
                  yield* Deferred.succeed(failed, undefined);
                  return yield* new Evidence.RollupError({ code: "content_unavailable" });
                }
                yield* Deferred.succeed(recovered, undefined);
                return outcome("idle");
              }),
            ),
          ),
        ),
      );
    }),
);

it.effect(
  "coalesces manual and timer overlap and returns explicit application outcomes without retry duplication",
  () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const initialDone = yield* Deferred.make<void>();
      const manualStarted = yield* Deferred.make<void>();
      const manualRelease = yield* Deferred.make<void>();
      let calls = 0;
      yield* Effect.scoped(
        Effect.gen(function* () {
          const service = yield* Worker.NativeRollupWorker;
          yield* Deferred.await(started);
          const skipped = yield* Effect.all(
            Array.from({ length: 4 }, () => service.sweep()),
            { concurrency: "unbounded" },
          );
          expect(skipped).toEqual([null, null, null, null]);
          expect(calls).toBe(1);
          yield* Deferred.succeed(release, undefined);
          yield* Deferred.await(initialDone);
          // Let the background loop reach its wait before admitting the manual sweep.
          yield* TestClock.adjust(1);
          const manual = yield* service.sweep().pipe(Effect.forkScoped);
          yield* Deferred.await(manualStarted);
          yield* TestClock.adjust(30_000);
          expect(calls).toBe(2);
          expect(yield* service.sweep()).toBeNull();
          yield* Deferred.succeed(manualRelease, undefined);
          expect(yield* Fiber.join(manual)).toEqual(outcome("provider_unconfigured"));
          expect(yield* service.sweep()).toEqual(outcome("provider_failed"));
          expect(yield* service.sweep()).toEqual(outcome("superseded"));
          expect(calls).toBe(4);
        }).pipe(
          Effect.provide(
            worker(() =>
              Effect.gen(function* () {
                calls++;
                if (calls === 1) {
                  yield* Deferred.succeed(started, undefined);
                  yield* Deferred.await(release);
                  yield* Deferred.succeed(initialDone, undefined);
                  return outcome("idle");
                }
                if (calls === 2) {
                  yield* Deferred.succeed(manualStarted, undefined);
                  yield* Deferred.await(manualRelease);
                  return outcome("provider_unconfigured");
                }
                return outcome(calls === 3 ? "provider_failed" : "superseded");
              }),
            ),
          ),
        ),
      );
    }),
);

it.effect(
  "scope disposal interrupts an active background sweep and fences retained service references",
  () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const interrupted = yield* Deferred.make<void>();
      let calls = 0;
      const service = yield* Effect.scoped(
        Effect.gen(function* () {
          const service = yield* Worker.NativeRollupWorker;
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
      yield* TestClock.adjust(300_000);
      expect(yield* service.sweep()).toBeNull();
      expect(calls).toBe(1);
    }),
);

it.effect(
  "caller cancellation interrupts its admitted manual sweep and permits a later sweep",
  () =>
    Effect.gen(function* () {
      const initialDone = yield* Deferred.make<void>();
      const manualStarted = yield* Deferred.make<void>();
      const interrupted = yield* Deferred.make<void>();
      let calls = 0;
      yield* Effect.scoped(
        Effect.gen(function* () {
          const service = yield* Worker.NativeRollupWorker;
          yield* Deferred.await(initialDone);
          yield* TestClock.adjust(1);
          const manual = yield* service.sweep().pipe(Effect.forkScoped);
          yield* Deferred.await(manualStarted);
          yield* Fiber.interrupt(manual);
          yield* Deferred.await(interrupted);
          expect(yield* service.sweep()).toEqual(outcome("provider_unconfigured"));
          expect(calls).toBe(3);
        }).pipe(
          Effect.provide(
            worker(() =>
              Effect.gen(function* () {
                calls++;
                if (calls === 2) {
                  yield* Deferred.succeed(manualStarted, undefined);
                  return yield* Effect.never;
                }
                yield* Deferred.succeed(initialDone, undefined);
                return outcome("provider_unconfigured");
              }).pipe(Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined))),
            ),
          ),
        ),
      );
    }),
);

it.effect("manual typed errors remain observable and do not leave the sweep occupied", () =>
  Effect.gen(function* () {
    const initialDone = yield* Deferred.make<void>();
    let calls = 0;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const service = yield* Worker.NativeRollupWorker;
        yield* Deferred.await(initialDone);
        yield* TestClock.adjust(1);
        const failed = yield* service.sweep().pipe(Effect.result);
        expect(failed._tag).toBe("Failure");
        if (failed._tag === "Failure") expect(failed.failure.code).toBe("provider_timeout");
        expect(yield* service.sweep()).toEqual(outcome("idle"));
        expect(calls).toBe(3);
      }).pipe(
        Effect.provide(
          worker(() =>
            Effect.gen(function* () {
              calls++;
              if (calls === 2) return yield* new Evidence.RollupError({ code: "provider_timeout" });
              yield* Deferred.succeed(initialDone, undefined);
              return outcome("idle");
            }),
          ),
        ),
      ),
    );
  }),
);

it.effect(
  "disposing the owner scope also interrupts a manual sweep called from another scope",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const callerScope = yield* Effect.scope;
        const initialDone = yield* Deferred.make<void>();
        const manualStarted = yield* Deferred.make<void>();
        const interrupted = yield* Deferred.make<void>();
        let calls = 0;
        const manual = yield* Effect.scoped(
          Effect.gen(function* () {
            const service = yield* Worker.NativeRollupWorker;
            yield* Deferred.await(initialDone);
            yield* TestClock.adjust(1);
            const manual = yield* service.sweep().pipe(Effect.forkIn(callerScope));
            yield* Deferred.await(manualStarted);
            return manual;
          }).pipe(
            Effect.provide(
              worker(() =>
                Effect.gen(function* () {
                  calls++;
                  if (calls === 1) {
                    yield* Deferred.succeed(initialDone, undefined);
                    return outcome("idle");
                  }
                  yield* Deferred.succeed(manualStarted, undefined);
                  return yield* Effect.never;
                }).pipe(Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined))),
              ),
            ),
          ),
        );
        yield* Deferred.await(interrupted);
        const exit = yield* Fiber.await(manual);
        expect(exit._tag).toBe("Failure");
        yield* TestClock.adjust(300_000);
        expect(calls).toBe(2);
      }),
    ),
);
