// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Logger from "effect/Logger";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import { vi } from "vite-plus/test";

import { makeEventNdjsonLogStore, type PendingRecord } from "../../../provider/EventNdjsonLogger.ts";
import type * as ResourceAttribution from "../../../resourceTelemetry/ResourceAttribution.ts";
import { SegmentBuffer } from "./SegmentBuffer.ts";

function record(id: string, bytes: number, stream: PendingRecord["stream"] = "native"): PendingRecord {
  return { line: id, bytes, stream, threadSegment: "test" };
}

it.effect("keeps detached batches charged to both caps and releases each reservation once", () => Effect.sync(() => {
  const buffer = new SegmentBuffer({ maxBufferedRecords: 2, maxBufferedBytes: 100, batchWindowMs: 0 });
  const a = buffer.admit(record("a", 40), "a.log");
  const b = buffer.admit(record("b", 40), "b.log");
  assert.equal(a.kind, "accepted");
  assert.equal(b.kind, "accepted");
  if (a.kind !== "accepted" || b.kind !== "accepted") return;
  const first = buffer.next(a.segment);
  const second = buffer.next(b.segment);
  assert.equal(first.kind, "batch");
  assert.equal(second.kind, "batch");
  if (first.kind !== "batch" || second.kind !== "batch") return;
  assert.deepEqual(buffer.usage, { records: 2, bytes: 80 });
  const blocked = buffer.admit(record("c", 1), "c.log");
  assert.equal(blocked.kind, "wait");
  if (blocked.kind !== "wait") return;
  assert.equal(Deferred.isDoneUnsafe(blocked.changed), false);
  buffer.complete(second.batch);
  buffer.complete(second.batch);
  assert.equal(Deferred.isDoneUnsafe(blocked.changed), true);
  assert.deepEqual(buffer.usage, { records: 1, bytes: 40 });
  assert.equal(buffer.admit(record("too-many-bytes", 61), "c.log").kind, "wait");
  const c = buffer.admit(record("c", 60), "c.log");
  assert.equal(c.kind, "accepted");
  if (c.kind !== "accepted") return;
  const third = buffer.next(c.segment);
  assert.equal(third.kind, "batch");
  if (third.kind !== "batch") return;
  buffer.complete(first.batch);
  assert.deepEqual(buffer.usage, { records: 1, bytes: 60 });
  buffer.complete(third.batch);
  assert.deepEqual(buffer.usage, { records: 0, bytes: 0 });
}));

it.effect("admits a bounded oversized record alone and keeps its actual bytes charged", () => Effect.sync(() => {
  const buffer = new SegmentBuffer({ maxBufferedRecords: 8, maxBufferedBytes: 5, batchWindowMs: 1_000 });
  const a = buffer.admit(record("oversized", 10), "a.log");
  assert.equal(a.kind, "accepted");
  if (a.kind !== "accepted") return;
  assert.equal(a.flush, true);
  const batch = buffer.next(a.segment);
  assert.equal(batch.kind, "batch");
  if (batch.kind !== "batch") return;
  assert.deepEqual(buffer.usage, { records: 1, bytes: 10 });
  assert.equal(buffer.admit(record("b", 1), "b.log").kind, "wait");
  buffer.complete(batch.batch);
  assert.equal(buffer.admit(record("b", 1), "b.log").kind, "accepted");
}));

it.effect("preserves an enqueue racing detach and joins only the requested segment watermark", () => Effect.sync(() => {
  const buffer = new SegmentBuffer({ maxBufferedRecords: 8, maxBufferedBytes: 100, batchWindowMs: 0 });
  const first = record("native-first", 10);
  const a = buffer.admit(first, "a.log");
  assert.equal(a.kind, "accepted");
  if (a.kind !== "accepted") return;
  assert.equal(a.workers.length, 1);
  const detached = buffer.next(a.segment);
  assert.equal(detached.kind, "batch");
  if (detached.kind !== "batch") return;
  const second = record("canonical-second", 20, "canonical");
  const later = buffer.admit(second, "a.log");
  const b = buffer.admit(record("independent", 10), "b.log");
  assert.equal(later.kind, "accepted");
  assert.equal(b.kind, "accepted");
  if (later.kind !== "accepted" || b.kind !== "accepted") return;
  assert.equal(later.segment, a.segment);
  assert.deepEqual(later.workers, []);
  assert.deepEqual(detached.batch.records, [first]);
  const independent = buffer.next(b.segment);
  assert.equal(independent.kind, "batch");
  if (independent.kind !== "batch") return;
  buffer.complete(independent.batch);
  assert.equal(buffer.awaitWatermark(b.segment, b.watermark), undefined);
  assert.notEqual(buffer.awaitWatermark(a.segment, a.watermark), undefined);
  buffer.complete(detached.batch);
  assert.equal(buffer.awaitWatermark(a.segment, a.watermark), undefined);
  assert.notEqual(buffer.awaitWatermark(a.segment, later.watermark), undefined);
  const next = buffer.next(a.segment);
  assert.equal(next.kind, "batch");
  if (next.kind !== "batch") return;
  assert.deepEqual(next.batch.records, [second]);
  buffer.complete(next.batch);
  assert.equal(buffer.awaitWatermark(a.segment, later.watermark), undefined);
  assert.deepEqual(buffer.usage, { records: 0, bytes: 0 });
}));

it.effect("reserves rotating families and cannot evict an in-flight or successor segment", () => Effect.sync(() => {
  const buffer = new SegmentBuffer({ maxBufferedRecords: 8, maxBufferedBytes: 100, batchWindowMs: 0 });
  const a = buffer.admit(record("held", 10), "a.log");
  const b = buffer.admit(record("idle", 10), "b.log");
  assert.equal(a.kind, "accepted");
  assert.equal(b.kind, "accepted");
  if (a.kind !== "accepted" || b.kind !== "accepted") return;
  const held = buffer.next(a.segment);
  const idle = buffer.next(b.segment);
  assert.equal(held.kind, "batch");
  assert.equal(idle.kind, "batch");
  if (held.kind !== "batch" || idle.kind !== "batch") return;
  buffer.complete(idle.batch);
  assert.deepEqual(buffer.retireIdle(), [b.segment]);
  assert.equal(buffer.reserveRemoval("a.log"), undefined);
  assert.equal(buffer.reserveRemoval("a.log.1"), undefined);
  const release = buffer.reserveRemoval("b.log.1");
  assert.exists(release);
  if (!release) return;
  assert.equal(buffer.reserveRemoval("b.log.2"), undefined);
  const successor = buffer.admit(record("new-owner", 10), "b.log");
  assert.equal(successor.kind, "accepted");
  if (successor.kind !== "accepted") return;
  assert.notEqual(successor.segment, b.segment);
  assert.equal(buffer.next(b.segment).kind, "done");
  const waiting = buffer.next(successor.segment);
  assert.equal(waiting.kind, "wait");
  if (waiting.kind !== "wait") return;
  assert.equal(Deferred.isDoneUnsafe(waiting.changed), false);
  release();
  release();
  assert.equal(Deferred.isDoneUnsafe(waiting.changed), true);
  assert.deepEqual(buffer.retireIdle(), []);
  assert.equal(buffer.reserveRemoval("b.log"), undefined);
  assert.equal(buffer.next(successor.segment).kind, "batch");
}));

it.effect("closes admission before draining its captured watermarks and wakes capacity waiters", () => Effect.sync(() => {
  const buffer = new SegmentBuffer({ maxBufferedRecords: 2, maxBufferedBytes: 100, batchWindowMs: 1_000 });
  const a = buffer.admit(record("first", 10), "a.log");
  const b = buffer.admit(record("second", 10), "a.log");
  assert.equal(a.kind, "accepted");
  assert.equal(b.kind, "accepted");
  if (a.kind !== "accepted" || b.kind !== "accepted") return;
  const blocked = buffer.admit(record("not-accepted", 10), "b.log");
  assert.equal(blocked.kind, "wait");
  if (blocked.kind !== "wait") return;
  const closing = buffer.closeAdmission();
  assert.deepEqual(closing.watermarks, [{ segment: a.segment, watermark: b.watermark }]);
  assert.equal(Deferred.isDoneUnsafe(blocked.changed), true);
  assert.equal(buffer.admit(record("late", 10), "b.log").kind, "closed");
  assert.deepEqual(buffer.timerFired(), []);
  const final = buffer.next(a.segment);
  assert.equal(final.kind, "batch");
  if (final.kind !== "batch") return;
  assert.deepEqual(final.batch.records.map((entry) => entry.line), ["first", "second"]);
  buffer.complete(final.batch);
  assert.equal(buffer.awaitWatermark(a.segment, b.watermark), undefined);
  assert.equal(buffer.next(a.segment).kind, "done");
}));

const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

// The enclosing scope owns every directory and store; held I/O is released before scope finalizers join workers.
const logDirectory = Effect.acquireRelease(
  Effect.sync(() => NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-diagnostic-log-"))),
  (directory) => Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
);

it.layer(NodeServices.layer)("diagnostic log segment integration", (it) => {
  it.effect("preserves shared-stream FIFO through held writes, detach and rotation", () => Effect.scoped(Effect.gen(function* () {
    const directory = yield* logDirectory;
    const filePath = NodePath.join(directory, "events.log");
    const threadPath = NodePath.join(directory, "events.fifo.log");
    const fs = yield* FileSystem.FileSystem;
    const started = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    let calls = 0;
    let active = 0;
    let peak = 0;
    const store = yield* Effect.acquireRelease(makeEventNdjsonLogStore(filePath, {
      batchWindowMs: 0, maxBytes: 120, maxFiles: 3,
    }).pipe(Effect.provideService(FileSystem.FileSystem, {
      ...fs,
      writeFile: (path, content, options) => Effect.gen(function* () {
        active += 1;
        peak = Math.max(peak, active);
        if (++calls === 1) {
          yield* Deferred.succeed(started, undefined);
          yield* Deferred.await(release);
        }
        yield* fs.writeFile(path, content, options);
      }).pipe(Effect.ensuring(Effect.sync(() => { active -= 1; }))),
    })), (store) => store.close());
    yield* Effect.gen(function* () {
      const first = yield* store.logger("native").write({ id: "first", payload: "x".repeat(40) }, ThreadId.make("fifo"))
        .pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Deferred.await(started);
      const second = yield* store.logger("canonical").write({ id: "second", payload: "x".repeat(40) }, ThreadId.make("fifo"))
        .pipe(Effect.forkScoped({ startImmediately: true }));
      const third = yield* store.logger("native").write({ id: "third", payload: "x".repeat(40) }, ThreadId.make("fifo"))
        .pipe(Effect.forkScoped({ startImmediately: true }));
      assert.equal(calls, 1);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(first);
      yield* Fiber.join(second);
      yield* Fiber.join(third);
      yield* store.close();
      const lines = [`${threadPath}.2`, `${threadPath}.1`, threadPath].flatMap((path) => NodeFS.readFileSync(path, "utf8").trim().split("\n"));
      assert.deepEqual(lines.map((line) => decodeJson(line.slice(line.indexOf(": ") + 2))), [
        { id: "first", payload: "x".repeat(40) },
        { id: "second", payload: "x".repeat(40) },
        { id: "third", payload: "x".repeat(40) },
      ]);
      assert.deepEqual(lines.map((line) => line.slice(line.indexOf("] ") + 2, line.indexOf(": "))), ["NTIVE", "CANON", "NTIVE"]);
      assert.equal(peak, 1);
    }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
  })));

  it.effect("releases a failed in-flight reservation, reports it and admits a later segment", () => Effect.scoped(Effect.gen(function* () {
    const directory = yield* logDirectory;
    const filePath = NodePath.join(directory, "events.log");
    const fs = yield* FileSystem.FileSystem;
    const started = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const warnings: Array<unknown> = [];
    const attributed: Array<ResourceAttribution.ResourceAttributionRecord> = [];
    const capture = Logger.make<unknown, void>(({ message }) => { warnings.push(message); });
    let writes = 0;
    const store = yield* Effect.acquireRelease(makeEventNdjsonLogStore(filePath, {
      batchWindowMs: 0, maxBufferedRecords: 1,
      attribution: { record: (input) => Effect.sync(() => { attributed.push(input); }), snapshot: Effect.die("unused snapshot") },
    }).pipe(
      Effect.provideService(FileSystem.FileSystem, {
        ...fs,
        writeFile: (path, content, options) => Effect.gen(function* () {
          if (++writes === 1) {
            yield* Deferred.succeed(started, undefined);
            yield* Deferred.await(release);
            yield* fs.writeFile(NodePath.join(directory, "missing", "failure.log"), content, options);
            return;
          }
          yield* fs.writeFile(path, content, options);
        }),
      }),
    ), (store) => store.close());
    yield* Effect.gen(function* () {
      const first = yield* store.logger("native").write({ id: "failed" }, ThreadId.make("a"))
        .pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Deferred.await(started);
      const second = yield* store.logger("canonical").write({ id: "accepted-after-failure" }, ThreadId.make("b"))
        .pipe(Effect.forkScoped({ startImmediately: true }));
      assert.equal(writes, 1);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(first);
      yield* Fiber.join(second);
      yield* store.logger("native").write({ id: "new-event-after-failure" }, ThreadId.make("a"));
      yield* store.close();
      assert.equal(writes, 3);
      assert.equal(warnings.length, 1);
      assert.deepEqual(attributed.map(({ operation, count }) => ({ operation, count })), [
        { operation: "canonical.append", count: 1 }, { operation: "native.append", count: 1 },
      ]);
      assert.include(NodeFS.readFileSync(NodePath.join(directory, "events.b.log"), "utf8"), "accepted-after-failure");
      assert.notInclude(NodeFS.readFileSync(NodePath.join(directory, "events.a.log"), "utf8"), '"failed"');
    }).pipe(
      Effect.ensuring(Deferred.succeed(release, undefined)),
      Effect.provide(Logger.layer([capture], { mergeWithExisting: false })),
    );
  })));

  it.effect("keeps attribution I/O local to its segment and retains an interrupted caller's accepted batch", () => Effect.scoped(Effect.gen(function* () {
    const directory = yield* logDirectory;
    const filePath = NodePath.join(directory, "events.log");
    const started = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const secondDone = yield* Deferred.make<void>();
    const store = yield* Effect.acquireRelease(makeEventNdjsonLogStore(filePath, {
      batchWindowMs: 0,
      attribution: {
        record: (input) => input.operation === "canonical.append"
          ? Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release)))
          : Effect.void,
        snapshot: Effect.die("unused snapshot"),
      },
    }), (store) => store.close());
    yield* Effect.gen(function* () {
      const first = yield* store.logger("canonical").write({ id: "a" }, ThreadId.make("a"))
        .pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Deferred.await(started);
      const cancellation = yield* Fiber.interrupt(first).pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Fiber.join(cancellation);
      const second = yield* store.logger("native").write({ id: "b" }, ThreadId.make("b")).pipe(
        Effect.andThen(Deferred.succeed(secondDone, undefined)),
        Effect.forkScoped({ startImmediately: true }),
      );
      yield* Deferred.await(secondDone);
      yield* Fiber.join(second);
      assert.equal(yield* Deferred.isDone(release), false);
      assert.include(NodeFS.readFileSync(NodePath.join(directory, "events.b.log"), "utf8"), '"b"');
      yield* Deferred.succeed(release, undefined);
      yield* store.close();
      assert.include(NodeFS.readFileSync(NodePath.join(directory, "events.a.log"), "utf8"), '"a"');
    }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
  })));

  it.effect("retains an active rotation family and reopens an evicted idle sink without stale size state", () => Effect.scoped(Effect.gen(function* () {
    const directory = yield* logDirectory;
    const filePath = NodePath.join(directory, "events.log");
    const aPath = NodePath.join(directory, "events.a.log");
    const bPath = NodePath.join(directory, "events.b.log");
    const stalePath = NodePath.join(directory, "events.stale.log");
    const fs = yield* FileSystem.FileSystem;
    const started = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const removedB = yield* Deferred.make<void>();
    const attempts: Array<string> = [];
    const remove = NodeFS.promises.rm;
    yield* Effect.acquireRelease(Effect.sync(() => vi.spyOn(NodeFS.promises, "rm").mockImplementation(async (path, options) => {
      attempts.push(String(path));
      await remove(path, options);
      if (String(path) === bPath) Deferred.doneUnsafe(removedB, Effect.void);
    })), (spy) => Effect.sync(() => spy.mockRestore()));
    const store = yield* Effect.acquireRelease(makeEventNdjsonLogStore(filePath, {
      batchWindowMs: 0, maxBytes: 200, maxTotalBytes: 64, retentionCheckIntervalMs: 1,
    }).pipe(Effect.provideService(FileSystem.FileSystem, {
      ...fs,
      writeFile: (path, content, options) => Effect.gen(function* () {
        if (path === aPath) {
          yield* Deferred.succeed(started, undefined);
          yield* Deferred.await(release);
        }
        yield* fs.writeFile(path, content, options);
      }),
    })), (store) => store.close());
    yield* Effect.gen(function* () {
      NodeFS.writeFileSync(aPath, '[1970-01-01T00:00:00.000Z] NTIVE: {"id":"preexisting-a"}\n');
      NodeFS.writeFileSync(stalePath, '[1970-01-01T00:00:00.000Z] NTIVE: {"id":"stale"}\n');
      NodeFS.utimesSync(stalePath, 0, 0);
      NodeFS.utimesSync(aPath, 1, 1);
      const first = yield* store.logger("native").write({ id: "held-a" }, ThreadId.make("a"))
        .pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Deferred.await(started);
      yield* TestClock.adjust(1);
      const initialB = yield* store.logger("native")
        .write({ id: "initial", payload: "x".repeat(80) }, ThreadId.make("b"))
        .pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Deferred.await(removedB);
      yield* Fiber.join(initialB);
      assert.include(attempts, stalePath);
      assert.notInclude(attempts, aPath);
      assert.equal(NodeFS.existsSync(aPath), true);
      assert.equal(yield* Deferred.isDone(release), false);
      yield* store.logger("native").write({ id: "b1" }, ThreadId.make("b"));
      yield* store.logger("canonical").write({ id: "b2" }, ThreadId.make("b"));
      assert.equal(NodeFS.existsSync(`${bPath}.1`), false);
      assert.equal(NodeFS.readFileSync(bPath, "utf8").trim().split("\n").length, 2);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(first);
      yield* store.close();
      assert.deepEqual(NodeFS.readdirSync(directory), []);
    }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
  })));

  it.effect("closes admission while accepted I/O is held and never writes a blocked or later record", () => Effect.scoped(Effect.gen(function* () {
    const directory = yield* logDirectory;
    const filePath = NodePath.join(directory, "events.log");
    const fs = yield* FileSystem.FileSystem;
    const started = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const closed = yield* Deferred.make<void>();
    const closedAgain = yield* Deferred.make<void>();
    let writes = 0;
    const store = yield* Effect.acquireRelease(makeEventNdjsonLogStore(filePath, {
      batchWindowMs: 1_000, maxBufferedRecords: 1,
    }).pipe(Effect.provideService(FileSystem.FileSystem, {
      ...fs,
      writeFile: (path, content, options) => Effect.gen(function* () {
        writes += 1;
        yield* Deferred.succeed(started, undefined);
        yield* Deferred.await(release);
        yield* fs.writeFile(path, content, options);
      }),
    })), (store) => store.close());
    yield* Effect.gen(function* () {
      const first = yield* store.logger("native").write({ id: "accepted" }, ThreadId.make("a"))
        .pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Deferred.await(started);
      const blocked = yield* store.logger("native").write({ id: "blocked" }, ThreadId.make("b"))
        .pipe(Effect.forkScoped({ startImmediately: true }));
      const close = yield* store.close().pipe(Effect.andThen(Deferred.succeed(closed, undefined)), Effect.forkScoped({ startImmediately: true }));
      const closeAgain = yield* store.close().pipe(Effect.andThen(Deferred.succeed(closedAgain, undefined)), Effect.forkScoped({ startImmediately: true }));
      yield* Fiber.join(blocked);
      assert.equal(yield* Deferred.isDone(closed), false);
      assert.equal(yield* Deferred.isDone(closedAgain), false);
      yield* store.logger("native").write({ id: "after-close-intent" }, ThreadId.make("c"));
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(first);
      yield* Fiber.join(close);
      yield* Fiber.join(closeAgain);
      yield* store.close();
      yield* TestClock.adjust(2_000);
      assert.equal(writes, 1);
      assert.deepEqual(NodeFS.readdirSync(directory), ["events.a.log"]);
    }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
  })));

  it.effect("close drains a timer's accepted batch once and cancels the future timer", () => Effect.scoped(Effect.gen(function* () {
    const directory = yield* logDirectory;
    const fs = yield* FileSystem.FileSystem;
    let writes = 0;
    const store = yield* Effect.acquireRelease(makeEventNdjsonLogStore(NodePath.join(directory, "events.log"), {
      batchWindowMs: 1_000,
    }).pipe(Effect.provideService(FileSystem.FileSystem, {
      ...fs,
      writeFile: (path, content, options) => Effect.sync(() => { writes += 1; }).pipe(
        Effect.andThen(fs.writeFile(path, content, options)),
      ),
    })), (store) => store.close());
    yield* store.logger("native").write({ id: "buffered" }, ThreadId.make("a"));
    assert.equal(writes, 0);
    yield* store.close();
    assert.equal(writes, 1);
    yield* TestClock.adjust(1_000);
    yield* store.logger("native").write({ id: "late" }, ThreadId.make("a"));
    assert.equal(writes, 1);
    const contents = NodeFS.readFileSync(NodePath.join(directory, "events.a.log"), "utf8");
    assert.include(contents, "buffered");
    assert.notInclude(contents, "late");
  })));
});
