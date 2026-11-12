import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import { makePullRequestScopeLocks } from "./pullRequestScopeLocks.ts";

it.effect("shares a scope between readers and preserves FIFO order around writers", () =>
  Effect.gen(function* () {
    const locks = yield* makePullRequestScopeLocks;
    const entered = yield* Queue.unbounded<string>();
    const releaseReaders = yield* Deferred.make<void>();
    const releaseFirstWriter = yield* Deferred.make<void>();
    const releaseLaterReader = yield* Deferred.make<void>();
    const releaseLastWriter = yield* Deferred.make<void>();
    const hold = (name: string, release: Deferred.Deferred<void>) =>
      Queue.offer(entered, name).pipe(Effect.andThen(Deferred.await(release)));
    const first = yield* hold("first reader", releaseReaders).pipe(
      locks.withReadScopes(["pr"]),
      Effect.forkChild({ startImmediately: true }),
    );
    const second = yield* hold("second reader", releaseReaders).pipe(
      locks.withReadScopes(["pr"]),
      Effect.forkChild({ startImmediately: true }),
    );
    assert.deepStrictEqual(yield* Queue.takeN(entered, 2), ["first reader", "second reader"]);

    const firstWriter = yield* hold("first writer", releaseFirstWriter).pipe(
      locks.withWriteScope("pr"),
      Effect.forkChild({ startImmediately: true }),
    );
    const laterReader = yield* hold("later reader", releaseLaterReader).pipe(
      locks.withReadScopes(["pr"]),
      Effect.forkChild({ startImmediately: true }),
    );
    const lastWriter = yield* hold("last writer", releaseLastWriter).pipe(
      locks.withWriteScope("pr"),
      Effect.forkChild({ startImmediately: true }),
    );
    assert.strictEqual(locks.size, 1);
    yield* Deferred.succeed(releaseReaders, undefined);
    yield* Fiber.join(first);
    yield* Fiber.join(second);
    assert.strictEqual(yield* Queue.take(entered), "first writer");
    yield* Deferred.succeed(releaseFirstWriter, undefined);
    yield* Fiber.join(firstWriter);
    assert.strictEqual(yield* Queue.take(entered), "later reader");
    yield* Deferred.succeed(releaseLaterReader, undefined);
    yield* Fiber.join(laterReader);
    assert.strictEqual(yield* Queue.take(entered), "last writer");
    yield* Deferred.succeed(releaseLastWriter, undefined);
    yield* Fiber.join(lastWriter);
    assert.strictEqual(locks.size, 0);
  }),
);

it.effect("acquires distinct scopes in sorted order without changing the caller's array", () =>
  Effect.gen(function* () {
    const locks = yield* makePullRequestScopeLocks;
    const release = yield* Deferred.make<void>();
    const writer = yield* Deferred.await(release).pipe(
      locks.withWriteScope("a"),
      Effect.forkChild({ startImmediately: true }),
    );
    const scopes = ["b", "a", "b"];
    let readStarted = false;
    const reader = yield* Effect.sync(() => {
      readStarted = true;
    }).pipe(locks.withReadScopes(scopes), Effect.forkChild({ startImmediately: true }));

    assert.strictEqual(locks.size, 1);
    yield* Effect.void.pipe(locks.withWriteScope("b"));
    assert.isFalse(readStarted);
    assert.deepStrictEqual(scopes, ["b", "a", "b"]);
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(writer);
    yield* Fiber.join(reader);
    assert.isTrue(readStarted);
    assert.strictEqual(locks.size, 0);
  }),
);

it.effect(
  "releases earlier scopes and canceled waiters when a multi-scope read is interrupted",
  () =>
    Effect.gen(function* () {
      const locks = yield* makePullRequestScopeLocks;
      const blockingWriter = yield* Effect.never.pipe(
        locks.withWriteScope("b"),
        Effect.forkChild({ startImmediately: true }),
      );
      const reader = yield* Effect.void.pipe(
        locks.withReadScopes(["b", "a"]),
        Effect.forkChild({ startImmediately: true }),
      );
      const waitingWriter = yield* Effect.void.pipe(
        locks.withWriteScope("a"),
        Effect.forkChild({ startImmediately: true }),
      );
      assert.strictEqual(locks.size, 2);
      assert.isUndefined(waitingWriter.pollUnsafe());

      yield* Fiber.interrupt(reader);
      yield* Fiber.join(waitingWriter);
      assert.strictEqual(locks.size, 1);
      yield* Fiber.interrupt(blockingWriter);
      assert.strictEqual(locks.size, 0);
    }),
);

it.effect("unblocks shared readers when a queued writer is canceled and deletes idle records", () =>
  Effect.gen(function* () {
    const locks = yield* makePullRequestScopeLocks;
    const first = yield* Effect.never.pipe(
      locks.withReadScopes(["pr"]),
      Effect.forkChild({ startImmediately: true }),
    );
    const writer = yield* Effect.void.pipe(
      locks.withWriteScope("pr"),
      Effect.forkChild({ startImmediately: true }),
    );
    const entered = yield* Deferred.make<void>();
    const later = yield* Deferred.succeed(entered, undefined).pipe(
      Effect.andThen(Effect.never),
      locks.withReadScopes(["pr"]),
      Effect.forkChild({ startImmediately: true }),
    );
    assert.isFalse(yield* Deferred.isDone(entered));

    yield* Fiber.interrupt(writer);
    yield* Deferred.await(entered);
    assert.strictEqual(locks.size, 1);
    yield* Fiber.interrupt(first);
    assert.strictEqual(locks.size, 1);
    yield* Fiber.interrupt(later);
    assert.strictEqual(locks.size, 0);
  }),
);

it.effect("retains only active scope records after successful and failed holders", () =>
  Effect.gen(function* () {
    const locks = yield* makePullRequestScopeLocks;
    for (let index = 0; index < 32; index++) {
      const scope = `pr-${index}`;
      const failed = yield* Effect.fail("lookup failed").pipe(
        locks.withReadScopes([scope, "project"]),
        Effect.exit,
      );
      assert.isTrue(Exit.isFailure(failed));
      assert.strictEqual(locks.size, 0);
      const defect = yield* Effect.die("write defect").pipe(
        locks.withWriteScope(scope),
        Effect.exit,
      );
      assert.isTrue(Exit.isFailure(defect));
      assert.strictEqual(locks.size, 0);
      yield* Effect.void.pipe(locks.withReadScopes([scope, "project"]));
      yield* Effect.void.pipe(locks.withWriteScope(scope));
      assert.strictEqual(locks.size, 0);
    }
  }),
);
