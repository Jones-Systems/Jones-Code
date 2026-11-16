import { it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Exit from "effect/Exit";
import { TestClock } from "effect/testing";
import { expect } from "vite-plus/test";
import { makePrLookupGitStateCache } from "./prLookupGitState.ts";

it.effect("retains six Git keys across 600 worktrees for a five-minute TTL", () =>
  Effect.gen(function* () {
    const cache = makePrLookupGitStateCache(() => 0);
    let reads = 0;
    const keys = [
      "remotes",
      "branchRef",
      "defaultBranch",
      "config-branch",
      "config-origin",
      "config-merge",
    ];
    const sweep = Effect.forEach(
      Array.from({ length: 600 }, (_, index) => "/cwd/" + index),
      (cwd) =>
        Effect.forEach(keys, (key) =>
          cache.remember(
            cwd,
            key,
            false,
            Effect.sync(() => {
              reads++;
              return cwd + key;
            }),
          ),
        ),
    );
    const first = yield* sweep;
    expect(reads).toBe(3600);
    yield* TestClock.adjust("61 seconds");
    expect(yield* sweep).toEqual(first);
    expect(reads).toBe(3600);
    yield* TestClock.adjust("239 seconds");
    expect(yield* sweep).toEqual(first);
    expect(reads).toBe(7200);
  }),
);

it.effect("bounds cwd and per-cwd keys with read promotion", () =>
  Effect.gen(function* () {
    const cache = makePrLookupGitStateCache(() => 0);
    let reads = 0;
    const read = (cwd: string, key = "key") =>
      cache.remember(
        cwd,
        key,
        false,
        Effect.sync(() => {
          reads++;
          return cwd + key;
        }),
      );
    for (let index = 0; index < 2048; index++) yield* read("/" + index);
    yield* read("/0");
    yield* read("/2048");
    yield* read("/0");
    expect(reads).toBe(2049);
    yield* read("/1");
    expect(reads).toBe(2050);
    for (let index = 0; index < 64; index++) yield* read("/keys", String(index));
    yield* read("/keys", "0");
    yield* read("/keys", "64");
    yield* read("/keys", "0");
    expect(reads).toBe(2115);
    yield* read("/keys", "1");
    expect(reads).toBe(2116);
  }),
);

it.effect("coalesces nonfresh reads and keeps identical keys isolated by cwd", () =>
  Effect.gen(function* () {
    const cache = makePrLookupGitStateCache(() => 0);
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    let reads = 0;
    const read = cache.remember(
      "/a",
      "key",
      false,
      Effect.gen(function* () {
        reads++;
        yield* Deferred.succeed(entered, undefined);
        yield* Deferred.await(release);
        return "a";
      }),
    );
    const owner = yield* Effect.forkChild(read);
    yield* Deferred.await(entered);
    const waiter = yield* Effect.forkChild(read, { startImmediately: true });
    expect(yield* cache.remember("/b", "key", false, Effect.succeed("b"))).toBe("b");
    yield* Deferred.succeed(release, undefined);
    expect(yield* Fiber.join(owner)).toBe("a");
    expect(yield* Fiber.join(waiter)).toBe("a");
    expect(reads).toBe(1);
  }),
);

it.effect("a same-millisecond fresh read bypasses pending and wins over its older completion", () =>
  Effect.gen(function* () {
    const cache = makePrLookupGitStateCache(() => 0);
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const owner = yield* Effect.forkChild(
      cache.remember(
        "/a",
        "key",
        false,
        Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
          Effect.as("old"),
        ),
      ),
    );
    yield* Deferred.await(entered);
    expect(yield* cache.remember("/a", "key", true, Effect.succeed("new"))).toBe("new");
    yield* Deferred.succeed(release, undefined);
    expect(yield* Fiber.join(owner)).toBe("old");
    expect(yield* cache.remember("/a", "key", false, Effect.die("unexpected read"))).toBe("new");
  }),
);

it.effect(
  "invalidation stops new callers joining old pending reads or resurrecting old values",
  () =>
    Effect.gen(function* () {
      let epoch = 0;
      const cache = makePrLookupGitStateCache(() => epoch);
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const owner = yield* Effect.forkChild(
        cache.remember(
          "/a",
          "key",
          false,
          Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.as("old"),
          ),
        ),
      );
      yield* Deferred.await(entered);
      epoch++;
      cache.forget("/a");
      expect(yield* cache.remember("/a", "key", false, Effect.succeed("new"))).toBe("new");
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(owner);
      expect(yield* cache.remember("/a", "key", false, Effect.die("unexpected read"))).toBe("new");
    }),
);

it.effect("canceling a waiter leaves its owner alive", () =>
  Effect.gen(function* () {
    const cache = makePrLookupGitStateCache(() => 0);
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const read = cache.remember(
      "/a",
      "key",
      false,
      Deferred.succeed(entered, undefined).pipe(
        Effect.andThen(Deferred.await(release)),
        Effect.as("ok"),
      ),
    );
    const owner = yield* Effect.forkChild(read);
    yield* Deferred.await(entered);
    const waiter = yield* Effect.forkChild(read, { startImmediately: true });
    yield* Fiber.interrupt(waiter);
    yield* Deferred.succeed(release, undefined);
    expect(yield* Fiber.join(owner)).toBe("ok");
    expect(yield* cache.remember("/a", "key", false, Effect.die("unexpected read"))).toBe("ok");
  }),
);

it.effect("owner interruption wakes waiters to retry their own read", () =>
  Effect.gen(function* () {
    const cache = makePrLookupGitStateCache(() => 0);
    const entered = yield* Deferred.make<void>();
    const owner = yield* Effect.forkChild(
      cache.remember(
        "/a",
        "key",
        false,
        Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
      ),
    );
    yield* Deferred.await(entered);
    const waiter = yield* Effect.forkChild(
      cache.remember("/a", "key", false, Effect.succeed("retry")),
      { startImmediately: true },
    );
    yield* Fiber.interrupt(owner);
    expect(yield* Fiber.join(waiter)).toBe("retry");
  }),
);

it.effect("owner failure clears pending and allows a waiter to recover", () =>
  Effect.gen(function* () {
    const cache = makePrLookupGitStateCache(() => 0);
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const owner = yield* Effect.forkChild(
      cache
        .remember(
          "/a",
          "key",
          false,
          Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.andThen(Effect.fail("failed")),
          ),
        )
        .pipe(Effect.exit),
    );
    yield* Deferred.await(entered);
    const waiter = yield* Effect.forkChild(
      cache.remember("/a", "key", false, Effect.succeed("retry")),
      { startImmediately: true },
    );
    yield* Deferred.succeed(release, undefined);
    expect(Exit.isFailure(yield* Fiber.join(owner))).toBe(true);
    expect(yield* Fiber.join(waiter)).toBe("retry");
  }),
);
