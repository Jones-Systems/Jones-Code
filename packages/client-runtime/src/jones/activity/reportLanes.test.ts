import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Scope from "effect/Scope";
import * as TestClock from "effect/testing/TestClock";

import { makeReportLanes } from "./reportLanes.ts";

describe("activity report lanes", () => {
  it.effect("sends only the latest pending observation with its original timestamp", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const latestSent = yield* Deferred.make<void>();
      const sent: Array<{ readonly observedAt: number; readonly visible: boolean }> = [];
      const lanes = yield* makeReportLanes((_key: string, report: (typeof sent)[number]) =>
        Effect.gen(function* () {
          sent.push(report);
          if (sent.length === 1) {
            yield* Deferred.succeed(started, undefined);
            yield* Deferred.await(release);
          } else {
            yield* Deferred.succeed(latestSent, undefined);
          }
        }),
      );
      yield* lanes.retain(["A"]);
      yield* lanes.submit("A", { observedAt: 0, visible: true });
      yield* Deferred.await(started);
      for (let observedAt = 1; observedAt <= 100; observedAt++) {
        yield* lanes.submit("A", { observedAt, visible: false });
      }
      expect(sent).toEqual([{ observedAt: 0, visible: true }]);
      yield* TestClock.adjust("1 second");
      yield* Deferred.succeed(release, undefined);
      yield* Deferred.await(latestSent);
      expect(sent).toEqual([
        { observedAt: 0, visible: true },
        { observedAt: 100, visible: false },
      ]);
    }).pipe(Effect.scoped),
  );

  it.effect("interrupts at the deadline, drops without retry, and accepts the next report", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const interrupted = yield* Deferred.make<void>();
      const nextSent = yield* Deferred.make<void>();
      const sent: number[] = [];
      let canceled = false;
      const lanes = yield* makeReportLanes((_key: string, report: number) =>
        Effect.gen(function* () {
          sent.push(report);
          if (report === 1) {
            yield* Deferred.succeed(started, undefined);
            return yield* Effect.never.pipe(
              Effect.onInterrupt(() =>
                Effect.sync(() => {
                  canceled = true;
                }).pipe(Effect.andThen(Deferred.succeed(interrupted, undefined))),
              ),
            );
          }
          yield* Deferred.succeed(nextSent, undefined);
        }),
      );
      yield* lanes.retain(["A"]);
      yield* lanes.submit("A", 1);
      yield* Deferred.await(started);
      yield* TestClock.adjust("9999 millis");
      expect(canceled).toBe(false);
      yield* TestClock.adjust("1 millis");
      yield* Deferred.await(interrupted);
      yield* TestClock.adjust("25 seconds");
      expect(sent).toEqual([1]);
      yield* lanes.submit("A", 2);
      yield* Deferred.await(nextSent);
      expect(sent).toEqual([1, 2]);
    }).pipe(Effect.scoped),
  );

  it.effect("drops failures and self-interruptions without losing subsequent observations", () =>
    Effect.gen(function* () {
      const starts = yield* Deferred.make<void>();
      const secondStarted = yield* Deferred.make<void>();
      const done = yield* Deferred.make<void>();
      const sent: number[] = [];
      const lanes = yield* makeReportLanes((_key: string, report: number) =>
        Effect.gen(function* () {
          sent.push(report);
          if (report === 1) {
            yield* Deferred.succeed(starts, undefined);
            return yield* Effect.fail("report failed");
          }
          if (report === 2) {
            yield* Deferred.succeed(secondStarted, undefined);
            return yield* Effect.interrupt;
          }
          yield* Deferred.succeed(done, undefined);
        }),
      );
      yield* lanes.retain(["A"]);
      yield* lanes.submit("A", 1);
      yield* Deferred.await(starts);
      yield* lanes.submit("A", 2);
      yield* Deferred.await(secondStarted);
      yield* lanes.submit("A", 3);
      yield* Deferred.await(done);
      expect(sent).toEqual([1, 2, 3]);
    }).pipe(Effect.scoped),
  );

  it.effect("removal interrupts in-flight work and discards pending work before re-addition", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const interrupted = yield* Deferred.make<void>();
      const readded = yield* Deferred.make<void>();
      const sent: number[] = [];
      const lanes = yield* makeReportLanes((_key: string, report: number) =>
        Effect.gen(function* () {
          sent.push(report);
          if (report === 1) {
            yield* Deferred.succeed(started, undefined);
            return yield* Effect.never.pipe(
              Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined)),
            );
          }
          yield* Deferred.succeed(readded, undefined);
        }),
      );
      yield* lanes.retain(["A"]);
      yield* lanes.submit("A", 1);
      yield* Deferred.await(started);
      yield* lanes.submit("A", 2);
      yield* lanes.retain([]);
      yield* Deferred.await(interrupted);
      yield* lanes.submit("A", 3);
      yield* TestClock.adjust("25 seconds");
      expect(sent).toEqual([1]);
      yield* lanes.retain(["A"]);
      yield* lanes.submit("A", 4);
      yield* Deferred.await(readded);
      expect(sent).toEqual([1, 4]);
    }).pipe(Effect.scoped),
  );

  it.effect(
    "reporter scope shutdown interrupts all environments and rejects later submissions",
    () =>
      Effect.gen(function* () {
        const reporterScope = yield* Effect.acquireRelease(Scope.make(), (scope) =>
          Scope.close(scope, Exit.void),
        );
        const started = yield* Deferred.make<void>();
        const canceled: string[] = [];
        let calls = 0;
        const lanes = yield* makeReportLanes((key: string, _report: number) =>
          Effect.gen(function* () {
            calls += 1;
            if (calls === 2) yield* Deferred.succeed(started, undefined);
            return yield* Effect.never.pipe(
              Effect.onInterrupt(() =>
                Effect.sync(() => {
                  canceled.push(key);
                }),
              ),
            );
          }),
        ).pipe(Scope.provide(reporterScope));
        yield* lanes.retain(["A", "B"]);
        yield* lanes.submit("A", 1);
        yield* lanes.submit("B", 1);
        yield* Deferred.await(started);
        yield* Scope.close(reporterScope, Exit.void);
        yield* lanes.retain(["A"]);
        yield* lanes.submit("A", 2);
        expect(canceled.sort()).toEqual(["A", "B"]);
        expect(calls).toBe(2);
      }).pipe(Effect.scoped),
  );
});
