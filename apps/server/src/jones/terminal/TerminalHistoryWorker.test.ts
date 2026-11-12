import { it } from "@effect/vitest";
import { describe, expect } from "vite-plus/test";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import { makeTerminalHistoryWorker } from "./TerminalHistoryWorker.ts";

describe("terminal history persistence", () => {
  it.effect("interrupts a debounce already in progress when an urgent flush arrives", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const writes: string[] = [];
        const worker = yield* makeTerminalHistoryWorker<string>({
          debounceMs: 250,
          process: (key) =>
            Effect.sync(() => {
              writes.push(key);
            }),
        });
        for (const key of ["a", "b", "c"])
          yield* worker.enqueue(key, { history: "background", immediate: false });
        yield* TestClock.adjust(100);
        expect(writes).toEqual([]);
        yield* worker.enqueue("clear", { history: "", immediate: true });
        const drained = yield* Deferred.make<void>();
        yield* worker
          .drainKey("clear")
          .pipe(
            Effect.andThen(Deferred.succeed(drained, undefined)),
            Effect.forkChild({ startImmediately: true }),
          );
        yield* TestClock.adjust(0);
        expect(yield* Deferred.isDone(drained)).toBe(true);
        expect(writes).toEqual(["clear"]);
        yield* TestClock.adjust(249);
        expect(writes).toEqual(["clear"]);
        yield* TestClock.adjust(1);
        expect(writes).toEqual(["clear", "a"]);
        yield* TestClock.adjust(500);
        yield* worker.drainKey("c");
        expect(writes).toEqual(["clear", "a", "b", "c"]);
      }),
    ),
  );

  it.effect("flushes ahead of three dirty terminals without increasing background write rate", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const writes: string[] = [];
        const worker = yield* makeTerminalHistoryWorker<string>({
          debounceMs: 250,
          process: (key, history) =>
            Effect.sync(() => {
              writes.push(key + ":" + history);
            }),
        });
        for (const key of ["a", "b", "c"])
          yield* worker.enqueue(key, { history: "old", immediate: false });
        yield* worker.enqueue("a", { history: "latest", immediate: false });
        yield* worker.enqueue("clear", { history: "", immediate: true });
        const drained = yield* Deferred.make<void>();
        yield* worker
          .drainKey("clear")
          .pipe(
            Effect.andThen(Deferred.succeed(drained, undefined)),
            Effect.forkChild({ startImmediately: true }),
          );
        yield* TestClock.adjust(0);
        expect(yield* Deferred.isDone(drained)).toBe(true);
        expect(writes).toEqual(["clear:"]);
        yield* TestClock.adjust(249);
        expect(writes).toEqual(["clear:"]);
        yield* TestClock.adjust(1);
        expect(writes).toEqual(["clear:", "a:latest"]);
        yield* TestClock.adjust(250);
        expect(writes).toEqual(["clear:", "a:latest", "b:old"]);
        yield* TestClock.adjust(250);
        yield* worker.drainKey("c");
        expect(writes).toEqual(["clear:", "a:latest", "b:old", "c:old"]);
      }),
    ),
  );

  it.effect("waits for the in-progress write, then flushes before queued background work", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const writes: string[] = [];
        const started = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const worker = yield* makeTerminalHistoryWorker<string>({
          debounceMs: 250,
          process: (key, history) =>
            Effect.gen(function* () {
              writes.push(key + ":" + history);
              if (key === "active") {
                yield* Deferred.succeed(started, undefined);
                yield* Deferred.await(release);
              }
            }),
        });
        yield* worker.enqueue("active", { history: "first", immediate: false });
        yield* TestClock.adjust(250);
        yield* Deferred.await(started);
        yield* worker.enqueue("background", { history: "old", immediate: false });
        yield* worker.enqueue("clear", { history: "", immediate: true });
        const drained = yield* Deferred.make<void>();
        yield* worker
          .drainKey("clear")
          .pipe(
            Effect.andThen(Deferred.succeed(drained, undefined)),
            Effect.forkChild({ startImmediately: true }),
          );
        expect(yield* Deferred.isDone(drained)).toBe(false);
        expect(writes).toEqual(["active:first"]);
        yield* Deferred.succeed(release, undefined);
        yield* TestClock.adjust(0);
        expect(yield* Deferred.isDone(drained)).toBe(true);
        expect(writes).toEqual(["active:first", "clear:"]);
        yield* TestClock.adjust(249);
        expect(writes).toHaveLength(2);
        yield* TestClock.adjust(1);
        yield* worker.drainKey("background");
        expect(writes).toEqual(["active:first", "clear:", "background:old"]);
      }),
    ),
  );

  it.effect("keeps latest history ordered and does not lose it when one drain is interrupted", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const writes: string[] = [];
        const firstStarted = yield* Deferred.make<void>();
        const releaseFirst = yield* Deferred.make<void>();
        const latestStarted = yield* Deferred.make<void>();
        const releaseLatest = yield* Deferred.make<void>();
        const worker = yield* makeTerminalHistoryWorker<string>({
          debounceMs: 250,
          process: (_key, history) =>
            Effect.gen(function* () {
              writes.push(history);
              if (history === "first") {
                yield* Deferred.succeed(firstStarted, undefined);
                yield* Deferred.await(releaseFirst);
              } else {
                yield* Deferred.succeed(latestStarted, undefined);
                yield* Deferred.await(releaseLatest);
              }
            }),
        });
        yield* worker.enqueue("terminal", { history: "first", immediate: true });
        yield* Deferred.await(firstStarted);
        yield* worker.enqueue("terminal", { history: "intermediate", immediate: true });
        yield* worker.enqueue("terminal", { history: "latest", immediate: false });
        const cancelled = yield* worker.drainKey("terminal").pipe(Effect.forkChild);
        const done = yield* Deferred.make<void>();
        yield* worker
          .drainKey("terminal")
          .pipe(Effect.andThen(Deferred.succeed(done, undefined)), Effect.forkChild);
        yield* Fiber.interrupt(cancelled);
        expect(yield* Deferred.isDone(done)).toBe(false);
        yield* Deferred.succeed(releaseFirst, undefined);
        yield* Deferred.await(latestStarted);
        expect(yield* Deferred.isDone(done)).toBe(false);
        expect(writes).toEqual(["first", "latest"]);
        yield* Deferred.succeed(releaseLatest, undefined);
        yield* Deferred.await(done);
        yield* worker.drainKey("terminal");
        expect(writes).toEqual(["first", "latest"]);
      }),
    ),
  );

  it.effect("keeps a queued newer history after an earlier write defects", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const writes: string[] = [];
        const started = yield* Deferred.make<void>();
        const fail = yield* Deferred.make<void>();
        const worker = yield* makeTerminalHistoryWorker<string>({
          debounceMs: 250,
          process: (_key, history) =>
            Effect.gen(function* () {
              writes.push(history);
              if (history === "first") {
                yield* Deferred.succeed(started, undefined);
                yield* Deferred.await(fail);
                return yield* Effect.die("synthetic write defect");
              }
            }),
        });
        yield* worker.enqueue("terminal", { history: "first", immediate: true });
        yield* Deferred.await(started);
        yield* worker.enqueue("terminal", { history: "latest", immediate: true });
        yield* Deferred.succeed(fail, undefined);
        yield* worker.drainKey("terminal");
        expect(writes).toEqual(["first", "latest"]);
      }),
    ),
  );
});
