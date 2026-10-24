import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { PullRequestOperationError } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Queue from "effect/Queue";
import * as KeyValueStore from "effect/persistence/KeyValueStore";
import * as PullRequestReadCache from "./PullRequestReadCache.ts";

const cacheLayer = (directory: string) =>
  PullRequestReadCache.make.pipe(Effect.provide(KeyValueStore.layerFileSystem(directory)));

it.layer(NodeServices.layer)("PR filesystem cache", (it) => {
  it.effect("clears in-flight reads before a new service can reuse them", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pr-cache-" });
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const cache = yield* cacheLayer(directory);
      const read = yield* cache
        .get(
          "summary",
          Deferred.succeed(started, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.as("old"),
          ),
          ["pr"],
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(started);
      const invalidate = yield* cache
        .invalidate("pr")
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(read);
      yield* Fiber.join(invalidate);
      const restarted = yield* cacheLayer(directory);
      assert.strictEqual(yield* restarted.get("summary", Effect.succeed("new"), ["pr"]), "new");
    }),
  );

  it.effect("invalidates A while B's lookup is blocked and preserves freshness after restart", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pr-cache-" });
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let aReads = 0;
      let bReads = 0;
      const readA = Effect.sync(() => `a-${++aReads}`);
      const cache = yield* cacheLayer(directory);
      assert.strictEqual(yield* cache.get("a", readA, ["a"]), "a-1");
      const readB = yield* cache
        .get(
          "b",
          Effect.gen(function* () {
            bReads++;
            yield* Deferred.succeed(started, undefined);
            yield* Deferred.await(release);
            return "b";
          }),
          ["b"],
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(started);

      // This must finish before B is released, including its persisted revision.
      yield* cache.invalidate("a");
      const restarted = yield* cacheLayer(directory);
      assert.strictEqual(yield* restarted.get("a", readA, ["a"]), "a-2");
      assert.strictEqual(yield* cache.get("a", readA, ["a"]), "a-2");
      yield* Deferred.succeed(release, undefined);
      assert.strictEqual(yield* Fiber.join(readB), "b");
      assert.strictEqual(yield* restarted.get("b", Effect.die("cache miss"), ["b"]), "b");
      assert.strictEqual(aReads, 2);
      assert.strictEqual(bReads, 1);
    }),
  );

  it.effect("invalidates without a read permit while 512 same-scope reads run concurrently", () =>
    Effect.gen(function* () {
      const cache = yield* PullRequestReadCache.make;
      const started = yield* Queue.unbounded<number>();
      const release = yield* Deferred.make<void>();
      let reads = 0;
      const pending = yield* Effect.forEach(
        Array.from({ length: 513 }, (_, index) => index),
        (index) =>
          cache.get(
            `key-${index}`,
            Effect.gen(function* () {
              reads++;
              yield* Queue.offer(started, index);
              yield* Deferred.await(release);
              return String(index);
            }),
            ["b"],
          ),
        { concurrency: "unbounded" },
      ).pipe(Effect.forkChild);
      yield* Queue.takeN(started, 512);
      assert.strictEqual(reads, 512);
      yield* cache.invalidate("a");
      assert.strictEqual(reads, 512);
      yield* Deferred.succeed(release, undefined);
      assert.deepStrictEqual(
        yield* Fiber.join(pending),
        Array.from({ length: 513 }, (_, index) => String(index)),
      );
      assert.strictEqual(reads, 513);
    }).pipe(Effect.provide(KeyValueStore.layerMemory)),
  );

  it.effect("orders a queued scope invalidation before later reads of that scope", () =>
    Effect.gen(function* () {
      const cache = yield* PullRequestReadCache.make;
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let reads = 0;
      const lookup = Effect.gen(function* () {
        const current = ++reads;
        if (current === 1) {
          yield* Deferred.succeed(started, undefined);
          yield* Deferred.await(release);
        }
        return current === 1 ? "old" : "fresh";
      });
      const read = cache.get("summary", lookup, ["z-project", "a-pr"]);
      const first = yield* read.pipe(Effect.forkChild);
      yield* Deferred.await(started);
      const invalidation = yield* cache
        .invalidate("a-pr")
        .pipe(Effect.forkChild({ startImmediately: true }));
      const later = yield* read.pipe(Effect.forkChild({ startImmediately: true }));
      assert.isUndefined(invalidation.pollUnsafe());
      assert.strictEqual(reads, 1);
      yield* Deferred.succeed(release, undefined);
      assert.strictEqual(yield* Fiber.join(first), "old");
      yield* Fiber.join(invalidation);
      assert.strictEqual(yield* Fiber.join(later), "fresh");
      assert.strictEqual(yield* read, "fresh");
      assert.strictEqual(reads, 2);
    }).pipe(Effect.provide(KeyValueStore.layerMemory)),
  );

  it.effect("persists unrelated concurrent invalidations without losing either revision", () =>
    Effect.gen(function* () {
      const backing = yield* KeyValueStore.KeyValueStore;
      const writing = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let writes = 0;
      const cache = yield* PullRequestReadCache.make.pipe(
        Effect.provideService(KeyValueStore.KeyValueStore, {
          ...backing,
          set: (key, value) =>
            Effect.gen(function* () {
              if (key === "revisions" && ++writes === 1) {
                yield* Deferred.succeed(writing, undefined);
                yield* Deferred.await(release);
              }
              yield* backing.set(key, value);
            }),
        }),
      );
      let aReads = 0;
      let bReads = 0;
      const readA = Effect.sync(() => `a-${++aReads}`);
      const readB = Effect.sync(() => `b-${++bReads}`);
      yield* cache.get("a", readA, ["a"]);
      yield* cache.get("b", readB, ["b"]);
      const a = yield* cache.invalidate("a").pipe(Effect.forkChild);
      yield* Deferred.await(writing);
      // Always release the deliberately blocked, uninterruptible revision write.
      yield* Effect.gen(function* () {
        const b = yield* cache.invalidate("b").pipe(Effect.forkChild({ startImmediately: true }));
        assert.strictEqual(writes, 1);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(a);
        yield* Fiber.join(b);
      }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));

      const stored = yield* backing.get("revisions");
      assert.deepStrictEqual(Object.keys(JSON.parse(stored ?? "{}")).toSorted(), ["a", "b"]);
      const restarted = yield* PullRequestReadCache.make;
      assert.strictEqual(yield* restarted.get("a", readA, ["a"]), "a-2");
      assert.strictEqual(yield* restarted.get("b", readB, ["b"]), "b-2");
      assert.strictEqual(aReads, 2);
      assert.strictEqual(bReads, 2);
      assert.strictEqual(writes, 2);
    }).pipe(Effect.provide(KeyValueStore.layerMemory)),
  );

  it.effect("keeps payload persistence outside the revision mutex", () =>
    Effect.gen(function* () {
      const backing = yield* KeyValueStore.KeyValueStore;
      const writing = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const cache = yield* PullRequestReadCache.make.pipe(
        Effect.provideService(KeyValueStore.KeyValueStore, {
          ...backing,
          set: (key, value) =>
            Effect.gen(function* () {
              if (key !== "revisions") {
                yield* Deferred.succeed(writing, undefined);
                yield* Deferred.await(release);
              }
              yield* backing.set(key, value);
            }),
        }),
      );
      let reads = 0;
      const pending = yield* cache
        .get(
          "b",
          Effect.sync(() => String(++reads)),
          ["b"],
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(writing);
      yield* cache.invalidate("a").pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
      assert.strictEqual(yield* Fiber.join(pending), "1");
      const restarted = yield* PullRequestReadCache.make;
      assert.strictEqual(yield* restarted.get("b", Effect.die("cache miss"), ["b"]), "1");
      assert.strictEqual(reads, 1);
    }).pipe(Effect.provide(KeyValueStore.layerMemory)),
  );

  it.effect("preserves caller order and duplicate positions in persisted revision vectors", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pr-cache-" });
      const expiresAt = DateTime.toEpochMillis(yield* DateTime.now) + 60_000;
      yield* fs.writeFileString(
        `${directory}/revisions`,
        JSON.stringify({
          z: { revision: "z-revision", expiresAt },
          a: { revision: "a-revision", expiresAt },
        }),
      );
      const scopes = ["z", "a", "z"];
      const cache = yield* cacheLayer(directory);
      yield* cache.get("summary", Effect.succeed("cached"), scopes);
      const entryKey = (yield* fs.readDirectory(directory)).find((key) => key.startsWith("pr-v2"));
      assert.isDefined(entryKey);
      const stored = yield* fs.readFileString(`${directory}/${entryKey}`);
      assert.include(stored, "z-revision:a-revision:z-revision");
      assert.deepStrictEqual(scopes, ["z", "a", "z"]);
      const restarted = yield* cacheLayer(directory);
      assert.strictEqual(
        yield* restarted.get("summary", Effect.die("cache miss"), scopes),
        "cached",
      );
    }),
  );

  it.effect("invalidates only the changed scope across restarts and coalesces its next reads", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pr-cache-" });
      let reads = 0;
      const lookup = Effect.sync(() => String(++reads));
      const cache = yield* cacheLayer(directory);
      yield* cache.get("first", lookup, ["project", "pr-1"]);
      yield* cache.get("second", lookup, ["project", "pr-2"]);
      yield* cache.get("third", lookup, ["other-project", "pr-3"]);
      yield* cache.invalidate("pr-1");
      const restarted = yield* cacheLayer(directory);
      const answers = yield* Effect.all(
        Array.from({ length: 10 }, () => restarted.get("first", lookup, ["project", "pr-1"])),
        { concurrency: 10 },
      );
      assert.deepStrictEqual(answers, Array(10).fill("4"));
      assert.strictEqual(yield* restarted.get("second", lookup, ["project", "pr-2"]), "2");
      yield* restarted.invalidate("project");
      const again = yield* cacheLayer(directory);
      assert.strictEqual(yield* again.get("second", lookup, ["project", "pr-2"]), "5");
      assert.strictEqual(yield* again.get("third", lookup, ["other-project", "pr-3"]), "3");
      assert.strictEqual(reads, 5);
      const files = (yield* fs.readDirectory(directory)).length;
      for (let index = 0; index < 3; index++) {
        yield* again.invalidate("pr-1");
        yield* again.get("first", lookup, ["project", "pr-1"]);
      }
      assert.strictEqual((yield* fs.readDirectory(directory)).length, files);
    }),
  );

  it.effect("shares a pending refresh without blocking an unrelated cached PR", () =>
    Effect.gen(function* () {
      const cache = yield* PullRequestReadCache.make;
      yield* cache.get("first", Effect.succeed("old"), ["pr-1"]);
      yield* cache.get("second", Effect.succeed("warm"), ["pr-2"]);
      yield* cache.invalidate("pr-1");
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let reads = 0;
      const refresh = cache.get(
        "first",
        Effect.gen(function* () {
          reads++;
          yield* Deferred.succeed(started, undefined);
          yield* Deferred.await(release);
          return "fresh";
        }),
        ["pr-1"],
      );
      const pending = yield* Effect.all(
        Array.from({ length: 10 }, () => refresh),
        {
          concurrency: 10,
        },
      ).pipe(Effect.forkChild);
      yield* Deferred.await(started);
      assert.strictEqual(yield* cache.get("second", Effect.die("cache miss"), ["pr-2"]), "warm");
      yield* Deferred.succeed(release, undefined);
      assert.deepStrictEqual(yield* Fiber.join(pending), Array(10).fill("fresh"));
      assert.strictEqual(reads, 1);
    }).pipe(Effect.provide(KeyValueStore.layerMemory)),
  );

  it.effect("does not persist failed GitHub reads", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pr-cache-" });
      const cache = yield* cacheLayer(directory);
      const error = new PullRequestOperationError({ operation: "summary", detail: "unavailable" });
      yield* cache.get("summary", Effect.fail(error)).pipe(Effect.flip);
      const restarted = yield* cacheLayer(directory);
      assert.strictEqual(yield* restarted.get("summary", Effect.succeed("recovered")), "recovered");
    }),
  );

  it.effect("resumes caching after a failed scope read", () =>
    Effect.gen(function* () {
      const backing = yield* KeyValueStore.KeyValueStore;
      let fail = true;
      let reads = 0;
      const cache = yield* PullRequestReadCache.make.pipe(
        Effect.provideService(KeyValueStore.KeyValueStore, {
          ...backing,
          get: (key) =>
            Effect.suspend(() => {
              if (!fail) return backing.get(key);
              fail = false;
              return Effect.fail(
                new KeyValueStore.KeyValueStoreError({ method: "get", message: "unavailable" }),
              );
            }),
        }),
      );
      const read = cache.get(
        "summary",
        Effect.sync(() => String(++reads)),
        ["pr"],
      );
      assert.strictEqual(yield* read, "1");
      assert.strictEqual(yield* read, "2");
      assert.strictEqual(yield* read, "2");
    }).pipe(Effect.provide(KeyValueStore.layerMemory)),
  );

  it.effect("uses the disabled fallback for reads queued behind a failed invalidation", () =>
    Effect.gen(function* () {
      const backing = yield* KeyValueStore.KeyValueStore;
      const writing = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const cache = yield* PullRequestReadCache.make.pipe(
        Effect.provideService(KeyValueStore.KeyValueStore, {
          ...backing,
          set: (key, value) =>
            key === "revisions"
              ? Deferred.succeed(writing, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.andThen(
                    Effect.fail(
                      new KeyValueStore.KeyValueStoreError({
                        method: "set",
                        message: "unavailable",
                      }),
                    ),
                  ),
                )
              : backing.set(key, value),
        }),
      );
      let reads = 0;
      const read = cache.get(
        "summary",
        Effect.sync(() => String(++reads)),
        ["pr"],
      );
      assert.strictEqual(yield* read, "1");
      const invalidation = yield* cache.invalidate("pr").pipe(Effect.forkChild);
      yield* Deferred.await(writing);
      yield* Effect.gen(function* () {
        const pending = yield* read.pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(invalidation);
        assert.strictEqual(yield* Fiber.join(pending), "2");
      }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
      assert.strictEqual(yield* read, "3");
      assert.strictEqual(reads, 3);
    }).pipe(Effect.provide(KeyValueStore.layerMemory)),
  );

  it.effect("cancels an invalidation waiting for its scope without disabling the cache", () =>
    Effect.gen(function* () {
      const cache = yield* PullRequestReadCache.make;
      const started = yield* Deferred.make<void>();
      const read = yield* cache
        .get("pending", Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)), [
          "pr",
        ])
        .pipe(Effect.forkChild);
      yield* Deferred.await(started);
      const invalidation = yield* cache
        .invalidate("pr")
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Fiber.interrupt(invalidation);

      assert.strictEqual(yield* cache.get("other", Effect.succeed("cached"), ["pr"]), "cached");
      assert.strictEqual(yield* cache.get("other", Effect.die("cache miss"), ["pr"]), "cached");
      yield* Fiber.interrupt(read);
      yield* cache.invalidate("pr");
      assert.strictEqual(yield* cache.get("other", Effect.succeed("fresh"), ["pr"]), "fresh");
    }).pipe(Effect.provide(KeyValueStore.layerMemory)),
  );

  it.effect(
    "cancels an invalidation waiting for the revision mutex without writing its scope",
    () =>
      Effect.gen(function* () {
        const backing = yield* KeyValueStore.KeyValueStore;
        const writing = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const cache = yield* PullRequestReadCache.make.pipe(
          Effect.provideService(KeyValueStore.KeyValueStore, {
            ...backing,
            set: (key, value) =>
              Effect.gen(function* () {
                if (key === "revisions") {
                  yield* Deferred.succeed(writing, undefined);
                  yield* Deferred.await(release);
                }
                yield* backing.set(key, value);
              }),
          }),
        );
        const first = yield* cache.invalidate("a").pipe(Effect.forkChild);
        yield* Deferred.await(writing);
        yield* Effect.gen(function* () {
          const canceled = yield* cache
            .invalidate("b")
            .pipe(Effect.forkChild({ startImmediately: true }));
          yield* Fiber.interrupt(canceled);
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(first);
        }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
        const before = yield* backing.get("revisions");
        assert.deepStrictEqual(Object.keys(JSON.parse(before ?? "{}")), ["a"]);
        yield* cache.invalidate("b");
        const after = yield* backing.get("revisions");
        assert.deepStrictEqual(Object.keys(JSON.parse(after ?? "{}")).toSorted(), ["a", "b"]);
      }).pipe(Effect.provide(KeyValueStore.layerMemory)),
  );

  it.effect("cancels abandoned reads without blocking invalidation", () =>
    Effect.gen(function* () {
      const cache = yield* PullRequestReadCache.make;
      const started = yield* Deferred.make<void>();
      const read = yield* cache
        .get("summary", Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)), [
          "pr",
        ])
        .pipe(Effect.forkChild);
      yield* Deferred.await(started);
      yield* Fiber.interrupt(read);
      yield* cache.invalidate("pr");
      assert.strictEqual(yield* cache.get("summary", Effect.succeed("fresh"), ["pr"]), "fresh");
    }).pipe(Effect.provide(KeyValueStore.layerMemory)),
  );

  it.effect(
    "finishes the in-memory revision update when invalidation is canceled after writing",
    () =>
      Effect.gen(function* () {
        const backing = yield* KeyValueStore.KeyValueStore;
        const written = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const cache = yield* PullRequestReadCache.make.pipe(
          Effect.provideService(KeyValueStore.KeyValueStore, {
            ...backing,
            set: (key, value) =>
              backing
                .set(key, value)
                .pipe(
                  Effect.andThen(
                    key === "revisions"
                      ? Deferred.succeed(written, undefined).pipe(
                          Effect.andThen(Deferred.await(release)),
                        )
                      : Effect.void,
                  ),
                ),
          }),
        );
        yield* cache.get("summary", Effect.succeed("old"), ["pr"]);
        const invalidation = yield* cache.invalidate("pr").pipe(Effect.forkChild);
        yield* Deferred.await(written);
        const interrupt = yield* Fiber.interrupt(invalidation).pipe(
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(interrupt);
        assert.strictEqual(yield* cache.get("summary", Effect.succeed("fresh"), ["pr"]), "fresh");
      }).pipe(Effect.provide(KeyValueStore.layerMemory)),
  );
});
