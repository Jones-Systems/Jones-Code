import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import {
  backoffDelayMillis,
  canonicalJson,
  canonicalJsonStrict,
  isTerminalStatus,
  makeKeyedLock,
  TERMINAL_RETENTION,
  terminalRetentionCutoff,
  terminalRetentionCutoffMillis,
} from "./shared.ts";

it("canonicalJson sorts keys by code unit at every depth and drops undefined members", () => {
  // Persisted request fingerprints depend on these exact bytes; do not change this string.
  assert.equal(
    canonicalJson({
      b: [{ y: 1, x: undefined, X: 2 }, undefined],
      a: "text",
      c: { z: null, A: true },
      undefinedMember: undefined,
    }),
    '{"a":"text","b":[{"X":2,"y":1},],"c":{"A":true,"z":null}}',
  );
  // Code-unit order, not locale order: an uppercase key sorts before a lowercase one.
  assert.equal(canonicalJson({ a: 1, B: 2 }), '{"B":2,"a":1}');
  assert.equal(canonicalJson({ b: 1, a: 2 }), canonicalJson({ a: 2, b: 1 }));
});

it("canonicalJsonStrict matches canonicalJson for JSON and rejects what JSON cannot hold", () => {
  const value = { b: [1, "two", null, false], a: { d: 0.5, c: "x" } };
  assert.equal(canonicalJsonStrict(value), canonicalJson(value));
  for (const invalid of [
    { member: undefined },
    [undefined],
    { nested: { count: Number.NaN } },
    { count: Number.POSITIVE_INFINITY },
    { fn: () => 1 },
    { big: 1n },
  ])
    assert.throws(() => canonicalJsonStrict(invalid), "Input must be JSON");
});

it("backoffDelayMillis doubles from two seconds and caps at five minutes", () => {
  assert.deepEqual(
    [1, 2, 3, 4, 8, 9].map((failures) => backoffDelayMillis(failures, 1)),
    [2_000, 4_000, 8_000, 16_000, 256_000, 300_000],
  );
  assert.equal(backoffDelayMillis(1, 0.8), 1_600);
  assert.equal(backoffDelayMillis(9, 1.2), 300_000);
});

it("isTerminalStatus names the statuses that never move again", () => {
  for (const status of ["completed", "failed", "cancelled", "interrupted", "rolled_back"])
    assert.isTrue(isTerminalStatus(status), status);
  for (const status of ["idle", "pending", "running", "waiting", "unknown"])
    assert.isFalse(isTerminalStatus(status), status);
});

it.effect("the retention cutoff is thirty days before now", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(Date.parse("2026-11-10T12:00:00.000Z"));
    assert.equal(Duration.toMillis(TERMINAL_RETENTION), 30 * 24 * 60 * 60 * 1000);
    assert.equal(yield* terminalRetentionCutoff, "2026-10-11T12:00:00.000Z");
    assert.equal(yield* terminalRetentionCutoffMillis, Date.parse("2026-10-11T12:00:00.000Z"));
  }),
);

it.effect("a keyed lock serializes one key, runs other keys together, and forgets idle keys", () =>
  Effect.gen(function* () {
    const { locked, size } = makeKeyedLock<string>();
    const order: Array<string> = [];
    const gate = yield* Deferred.make<void>();
    const first = yield* locked(
      "a",
      Effect.gen(function* () {
        order.push("a1:start");
        yield* Deferred.await(gate);
        order.push("a1:end");
      }),
    ).pipe(Effect.forkChild);
    yield* Effect.yieldNow;
    const second = yield* locked(
      "a",
      Effect.sync(() => order.push("a2")),
    ).pipe(Effect.forkChild);
    yield* locked(
      "b",
      Effect.sync(() => order.push("b")),
    );
    yield* Effect.yieldNow;
    // "b" is not held up by "a"; "a2" waits for "a1".
    assert.deepEqual(order, ["a1:start", "b"]);
    assert.equal(size(), 1);
    yield* Deferred.succeed(gate, undefined);
    yield* Fiber.join(first);
    yield* Fiber.join(second);
    assert.deepEqual(order, ["a1:start", "b", "a1:end", "a2"]);
    assert.equal(size(), 0);
  }),
);

it.effect("a keyed lock releases its key on failure and on interruption", () =>
  Effect.gen(function* () {
    const { locked, size } = makeKeyedLock<string>();
    const failed = yield* locked("a", Effect.fail("boom")).pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(failed));
    assert.equal(size(), 0);
    const held = yield* Deferred.make<void>();
    const holder = yield* locked("a", Deferred.await(held)).pipe(Effect.forkChild);
    yield* Effect.yieldNow;
    const waiter = yield* locked("a", Effect.void).pipe(Effect.forkChild);
    yield* Effect.yieldNow;
    assert.equal(size(), 1);
    yield* Fiber.interrupt(waiter);
    assert.equal(size(), 1, "the holder still owns the key");
    yield* Fiber.interrupt(holder);
    assert.equal(size(), 0);
    // The key is usable again.
    yield* locked("a", Effect.void);
    assert.equal(size(), 0);
  }),
);

it.effect("a keyed lock does not grow with the number of distinct keys it has served", () =>
  Effect.gen(function* () {
    const { locked, size } = makeKeyedLock<number>();
    yield* Effect.forEach(
      Array.from({ length: 500 }, (_, index) => index),
      (key) => locked(key, Effect.void),
    );
    assert.equal(size(), 0);
  }),
);
