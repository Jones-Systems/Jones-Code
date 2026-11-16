import { expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Result from "effect/Result";
import { makeFileWriteSerialization } from "./fileWriteSerialization.ts";

it.effect("serializes the whole same-file write/refresh while allowing another file", () =>
  Effect.gen(function* () {
    const serialize = makeFileWriteSerialization();
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const trace: string[] = [];
    const first = yield* serialize(
      "/workspace/file",
      Effect.gen(function* () {
        trace.push("write1");
        yield* Deferred.succeed(entered, undefined);
        yield* Deferred.await(release);
        trace.push("refresh1");
      }),
    ).pipe(Effect.forkChild);
    yield* Deferred.await(entered);
    yield* Effect.gen(function* () {
      const second = yield* serialize(
        "/workspace/file",
        Effect.sync(() => trace.push("write2/refresh2")),
      ).pipe(Effect.forkChild);
      yield* serialize(
        "/workspace/other",
        Effect.sync(() => trace.push("other")),
      );
      yield* Effect.yieldNow;
      expect(trace).toEqual(["write1", "other"]);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(first);
      yield* Fiber.join(second);
      expect(trace).toEqual(["write1", "other", "refresh1", "write2/refresh2"]);
    }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
  }),
);

it.effect("releases a cancelled waiter and a failed owner without replacing a live lock", () =>
  Effect.gen(function* () {
    const serialize = makeFileWriteSerialization();
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    let cancelledRan = false;
    const first = yield* serialize(
      "/file",
      Effect.gen(function* () {
        yield* Deferred.succeed(entered, undefined);
        yield* Deferred.await(release);
        return yield* Effect.fail("write failed");
      }),
    ).pipe(Effect.forkChild);
    yield* Deferred.await(entered);
    yield* Effect.gen(function* () {
      const cancelled = yield* serialize(
        "/file",
        Effect.sync(() => {
          cancelledRan = true;
        }),
      ).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* Fiber.interrupt(cancelled);
      const next = yield* serialize("/file", Effect.succeed("next")).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      expect(next.pollUnsafe()).toBeUndefined();
      yield* Deferred.succeed(release, undefined);
      expect(yield* Fiber.join(first).pipe(Effect.result)).toEqual(Result.fail("write failed"));
      expect(yield* Fiber.join(next)).toBe("next");
      expect(cancelledRan).toBe(false);
      expect(yield* serialize("/file", Effect.succeed("reused"))).toBe("reused");
    }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
  }),
);

it.effect("bounds same-file admission and frees capacity after cancellation", () =>
  Effect.gen(function* () {
    const serialize = makeFileWriteSerialization();
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const owner = yield* serialize(
      "/file",
      Effect.gen(function* () {
        yield* Deferred.succeed(entered, undefined);
        yield* Deferred.await(release);
      }),
    ).pipe(Effect.forkChild);
    yield* Deferred.await(entered);
    yield* Effect.gen(function* () {
      const waiters = yield* Effect.forEach(Array.from({ length: 7 }), () =>
        serialize("/file", Effect.void).pipe(Effect.forkChild),
      );
      yield* Effect.yieldNow;
      const overflow = yield* serialize("/file", Effect.die("overflow must not run")).pipe(
        Effect.result,
      );
      expect(Result.isFailure(overflow)).toBe(true);
      if (Result.isFailure(overflow)) expect(overflow.failure._tag).toBe("FileWriteBusyError");
      yield* Effect.forEach(waiters, Fiber.interrupt);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(owner);
      expect(yield* serialize("/file", Effect.succeed("released"))).toBe("released");
    }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
  }),
);
