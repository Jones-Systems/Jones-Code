import { describe, expect, it } from "@effect/vitest";
import * as Cache from "effect/Cache";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";

import { dependencies, makeFixture } from "./backgroundGitAdmission.fixture.ts";
import { asBackgroundGitWork } from "./backgroundGitAdmission.ts";

describe("background Git admission", () => {
  it.effect(
    "caps marked Git at four while retaining eight shared permits and timeout bypasses",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const backgroundLabels = Array.from({ length: 8 }, (_, index) => `background-${index}`);
          const foregroundLabels = Array.from({ length: 5 }, (_, index) => `foreground-${index}`);
          const fixture = yield* makeFixture([
            ...backgroundLabels,
            ...foregroundLabels,
            "unlimited-timeout",
            "long-timeout",
          ]);
          const background = yield* Effect.forEach(
            backgroundLabels,
            (label) => fixture.run(label),
            {
              concurrency: "unbounded",
            },
          ).pipe(asBackgroundGitWork, Effect.forkScoped);
          const firstBackground = yield* Effect.forEach(Array.from({ length: 4 }), () =>
            Queue.take(fixture.starts),
          );
          expect(firstBackground.every((label) => label.startsWith("background-"))).toBe(true);
          yield* Effect.yieldNow;
          expect(yield* Queue.size(fixture.starts)).toBe(0);

          const foreground = yield* Effect.forEach(
            foregroundLabels,
            (label) => fixture.run(label),
            {
              concurrency: "unbounded",
            },
          ).pipe(Effect.forkScoped);
          const firstForeground = yield* Effect.forEach(Array.from({ length: 4 }), () =>
            Queue.take(fixture.starts),
          );
          expect(firstForeground.every((label) => label.startsWith("foreground-"))).toBe(true);
          yield* Effect.yieldNow;
          expect(yield* Queue.size(fixture.starts)).toBe(0);

          const unlimited = yield* fixture
            .run("unlimited-timeout", null)
            .pipe(asBackgroundGitWork, Effect.forkScoped);
          expect(yield* Queue.take(fixture.starts)).toBe("unlimited-timeout");
          const long = yield* fixture
            .run("long-timeout", 30_001)
            .pipe(asBackgroundGitWork, Effect.forkScoped);
          expect(yield* Queue.take(fixture.starts)).toBe("long-timeout");
          yield* fixture.releaseAll;
          expect(yield* Fiber.join(background)).toHaveLength(8);
          expect(yield* Fiber.join(foreground)).toHaveLength(5);
          yield* Fiber.join(unlimited);
          yield* Fiber.join(long);
          expect(fixture.peakBackground()).toBe(4);
          expect(fixture.started.filter((label) => label.startsWith("background-"))).toHaveLength(
            8,
          );
        }),
      ).pipe(Effect.provide(dependencies)),
  );

  it.effect("inherits the mark through forked work and lookups in a cache created outside it", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const directLabels = Array.from({ length: 4 }, (_, index) => `background-direct-${index}`);
        const cacheLabels = ["background-cache-a", "background-cache-b"];
        const fixture = yield* makeFixture([...directLabels, ...cacheLabels]);
        const cache = yield* Cache.makeWith((label: string) => fixture.run(label), {
          capacity: 2,
          timeToLive: () => "1 minute",
        });
        const work = yield* Effect.all(
          [
            ...directLabels.map((label) => fixture.run(label)),
            ...cacheLabels.map((label) => Cache.get(cache, label)),
          ],
          { concurrency: "unbounded" },
        ).pipe(asBackgroundGitWork, Effect.forkScoped);
        yield* Effect.forEach(Array.from({ length: 4 }), () => Queue.take(fixture.starts));
        yield* Effect.yieldNow;
        expect(yield* Queue.size(fixture.starts)).toBe(0);
        yield* fixture.releaseAll;
        expect(yield* Fiber.join(work)).toHaveLength(6);
        expect(fixture.peakBackground()).toBe(4);
        expect(fixture.started).toHaveLength(6);
      }),
    ).pipe(Effect.provide(dependencies)),
  );

  it.effect("cancels queued work without spawning and returns running and shared permits", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const heldLabels = Array.from({ length: 4 }, (_, index) => `background-held-${index}`);
        const foregroundLabels = Array.from({ length: 4 }, (_, index) => `foreground-${index}`);
        const fixture = yield* makeFixture([
          ...heldLabels,
          ...foregroundLabels,
          "background-cancelled",
          "background-next",
        ]);
        const held = yield* Effect.forEach(heldLabels, (label) =>
          fixture.run(label).pipe(asBackgroundGitWork, Effect.forkScoped),
        );
        yield* Effect.forEach(Array.from({ length: 4 }), () => Queue.take(fixture.starts));
        const cancelled = yield* fixture
          .run("background-cancelled")
          .pipe(asBackgroundGitWork, Effect.forkScoped);
        yield* Effect.yieldNow;
        expect(yield* Queue.size(fixture.starts)).toBe(0);
        yield* Fiber.interrupt(cancelled);
        yield* Fiber.interrupt(held[0]!);
        const next = yield* fixture
          .run("background-next")
          .pipe(asBackgroundGitWork, Effect.forkScoped);
        expect(yield* Queue.take(fixture.starts)).toBe("background-next");
        const foreground = yield* Effect.forEach(foregroundLabels, (label) => fixture.run(label), {
          concurrency: "unbounded",
        }).pipe(Effect.forkScoped);
        const foregroundStarts = yield* Effect.forEach(Array.from({ length: 4 }), () =>
          Queue.take(fixture.starts),
        );
        expect(foregroundStarts.every((label) => label.startsWith("foreground-"))).toBe(true);
        expect(fixture.started).not.toContain("background-cancelled");
        yield* fixture.releaseAll;
        yield* Effect.forEach(held.slice(1), Fiber.join);
        yield* Fiber.join(next);
        yield* Fiber.join(foreground);
        expect(fixture.peakBackground()).toBe(4);
      }),
    ).pipe(Effect.provide(dependencies)),
  );
});
