import { it } from "@effect/vitest";
import { expect } from "vite-plus/test";
import { ThreadId } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Scope from "effect/Scope";
import { makeRetainedNativeLoads } from "./RetainedNativeLoads.ts";

it.effect("caller cancellation and local publication retry retain the original native load", () =>
  Effect.scoped(Effect.gen(function* () {
    const scope = yield* Scope.Scope;
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    let nativeCalls = 0;
    const retain = makeRetainedNativeLoads<number, string>(scope, () => "conflict");
    const thread = ThreadId.make("original");
    const native = Effect.gen(function* () {
      nativeCalls++;
      yield* Deferred.succeed(entered, undefined);
      yield* Deferred.await(release);
      return 42;
    });
    const caller = yield* retain(thread, "request-A", native).pipe(Effect.forkChild);
    yield* Deferred.await(entered);
    yield* Fiber.interrupt(caller);
    expect(Exit.isFailure(yield* retain(thread, "request-B", native).pipe(Effect.exit))).toBe(true);
    yield* Deferred.succeed(release, undefined);
    const first = yield* retain(thread, "request-A", native);
    // A failed local publication does not call complete; retry obtains the same result.
    const retry = yield* retain(thread, "request-A", native);
    expect(retry.value).toBe(first.value);
    expect(nativeCalls).toBe(1);
    yield* retry.complete;
    const successor = yield* retain(thread, "request-B", Effect.succeed(43));
    // An old completion cannot erase the successor's retained publication.
    yield* first.complete;
    expect(Exit.isFailure(yield* retain(thread, "request-C", native).pipe(Effect.exit))).toBe(true);
    yield* successor.complete;
  })),
);

it.effect("unknown native failures stay retained while unrelated threads load independently", () =>
  Effect.scoped(Effect.gen(function* () {
    const retain = makeRetainedNativeLoads<number, string>(yield* Scope.Scope, () => "conflict");
    let calls = 0;
    const failed = Effect.sync(() => { calls++; }).pipe(Effect.andThen(Effect.fail("unknown")));
    const thread = ThreadId.make("uncertain");
    yield* retain(thread, "same-request", failed).pipe(Effect.exit);
    yield* retain(thread, "same-request", failed).pipe(Effect.exit);
    expect(calls).toBe(1);
    const independent = yield* retain(ThreadId.make("independent"), "other", Effect.succeed(1));
    expect(independent.value).toBe(1);
  })),
);

it.effect("publication recovery uses retained local proof without repeating failed native work", () =>
  Effect.scoped(Effect.gen(function* () {
    const retain = makeRetainedNativeLoads<number, string>(yield* Scope.Scope, () => "conflict");
    const thread = ThreadId.make("publication-only-recovery");
    let nativeCalls = 0;
    let publicationCalls = 0;
    let proofAvailable = false;
    const native = Effect.sync(() => { nativeCalls++; }).pipe(Effect.andThen(Effect.fail("publication")));
    const publish = () => Effect.suspend(() => {
      publicationCalls++;
      return proofAvailable ? Effect.succeed(7) : Effect.fail("held");
    });
    yield* retain(thread, "original", native, publish).pipe(Effect.exit);
    proofAvailable = true;
    const completed = yield* retain(thread, "original", native, publish);
    expect(completed.value).toBe(7);
    expect(nativeCalls).toBe(1);
    expect(publicationCalls).toBe(1);
    yield* completed.complete;
  })),
);

it.effect("concurrent subscribers share the original native load before its first result", () =>
  Effect.scoped(Effect.gen(function* () {
    const retain = makeRetainedNativeLoads<number, string>(yield* Scope.Scope, () => "conflict");
    const release = yield* Deferred.make<void>();
    const entered = yield* Deferred.make<void>();
    let calls = 0;
    const load = Effect.gen(function* () {
      calls++;
      yield* Deferred.succeed(entered, undefined);
      yield* Deferred.await(release);
      return 42;
    });
    const thread = ThreadId.make("concurrent-original");
    const first = yield* retain(thread, "same", load).pipe(Effect.forkChild);
    const second = yield* retain(thread, "same", load).pipe(Effect.forkChild);
    yield* Deferred.await(entered);
    const unrelated = yield* retain(ThreadId.make("unrelated"), "new", Effect.succeed(7));
    expect(unrelated.value).toBe(7);
    yield* Deferred.succeed(release, undefined);
    const results = yield* Effect.all([Fiber.join(first), Fiber.join(second)]);
    expect(results.map((result) => result.value)).toEqual([42, 42]);
    expect(calls).toBe(1);
    yield* results[0]!.complete;
  })),
);
