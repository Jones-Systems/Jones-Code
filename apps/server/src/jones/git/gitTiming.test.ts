import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Tracer from "effect/Tracer";
import { TestClock } from "effect/testing";

import { dependencies, makeFixture } from "./backgroundGitAdmission.fixture.ts";
import { asBackgroundGitWork } from "./backgroundGitAdmission.ts";

const withTelemetry = <A, E, R>(
  test: (ended: ReadonlyArray<Tracer.NativeSpan>) => Effect.Effect<A, E, R>,
) => {
  const ended: Tracer.NativeSpan[] = [];
  const tracer = Tracer.make({
    span: (options) => {
      const span = new Tracer.NativeSpan(options);
      const end = span.end.bind(span);
      span.end = (time, exit) => {
        end(time, exit);
        ended.push(span);
      };
      return span;
    },
  });
  return test(ended).pipe(Effect.scoped, Effect.withTracer(tracer), Effect.provide(dependencies));
};

describe("Git timing on existing spans", () => {
  it.effect.each([
    { name: "background gate", count: 4, markHolders: true, backgroundWait: 11, sharedWait: 0 },
    { name: "shared permits", count: 8, markHolders: false, backgroundWait: 0, sharedWait: 11 },
  ])("separates $name wait from execution", ({ count, markHolders, backgroundWait, sharedWait }) =>
    withTelemetry((ended) =>
      Effect.gen(function* () {
        const labels = Array.from({ length: count }, (_, index) => `held-${index}`);
        const fixture = yield* makeFixture([...labels, "target"]);
        const holders = yield* Effect.forEach(labels, (label) => fixture.run(label), {
          concurrency: "unbounded",
        }).pipe(
          (effect) => (markHolders ? asBackgroundGitWork(effect) : effect),
          Effect.forkScoped,
        );
        yield* Effect.forEach(labels, () => Queue.take(fixture.starts));
        const target = yield* fixture
          .run("target", undefined, "test.timing.target")
          .pipe(asBackgroundGitWork, Effect.forkScoped);
        yield* TestClock.adjust(11);
        expect(fixture.started).not.toContain("target");
        yield* fixture.release(labels[0]!);
        expect(yield* Queue.take(fixture.starts)).toBe("target");
        yield* TestClock.adjust(7);
        yield* fixture.release("target");
        yield* Fiber.join(target);
        const span = ended.find((span) => span.name === "test.timing.target");
        expect(span).toBeDefined();
        expect(Object.fromEntries(span!.attributes)).toMatchObject({
          "git.background_gate_wait_ms": backgroundWait,
          "git.shared_permit_wait_ms": sharedWait,
          "git.execution_ms": 7,
        });
        expect(ended.filter((span) => span.name === "test.timing.target")).toHaveLength(1);
        yield* fixture.releaseAll;
        yield* Fiber.join(holders);
      }),
    ),
  );

  it.effect.each([
    { name: "background gate", count: 4, markHolders: true, backgroundWait: 17, sharedWait: 0 },
    { name: "shared permits", count: 8, markHolders: false, backgroundWait: 0, sharedWait: 17 },
  ])(
    "records cancellation while waiting for $name without executing",
    ({ count, markHolders, backgroundWait, sharedWait }) =>
      withTelemetry((ended) =>
        Effect.gen(function* () {
          const labels = Array.from({ length: count }, (_, index) => `held-${index}`);
          const fixture = yield* makeFixture([...labels, "cancelled"]);
          const holders = yield* Effect.forEach(labels, (label) => fixture.run(label), {
            concurrency: "unbounded",
          }).pipe(
            (effect) => (markHolders ? asBackgroundGitWork(effect) : effect),
            Effect.forkScoped,
          );
          yield* Effect.forEach(labels, () => Queue.take(fixture.starts));
          const cancelled = yield* fixture
            .run("cancelled", undefined, "test.timing.queued")
            .pipe(asBackgroundGitWork, Effect.forkScoped);
          yield* TestClock.adjust(17);
          yield* Fiber.interrupt(cancelled);
          const span = ended.find((span) => span.name === "test.timing.queued");
          expect(Object.fromEntries(span!.attributes)).toMatchObject({
            "git.background_gate_wait_ms": backgroundWait,
            "git.shared_permit_wait_ms": sharedWait,
          });
          expect(span!.attributes.has("git.execution_ms")).toBe(false);
          yield* fixture.releaseAll;
          yield* Fiber.join(holders);
          expect(fixture.started).not.toContain("cancelled");
        }),
      ),
  );

  it.effect("records worktree spawn and exit while retaining the long-timeout bypass", () =>
    withTelemetry((ended) =>
      Effect.gen(function* () {
        yield* TestClock.setTime(1_000);
        const fixture = yield* makeFixture(["worktree-add"]);
        const worktree = yield* fixture
          .run("worktree-add", 300_000, "GitVcsDriver.createWorktree")
          .pipe(asBackgroundGitWork, Effect.forkScoped);
        expect(yield* Queue.take(fixture.starts)).toBe("worktree-add");
        yield* TestClock.adjust(9);
        yield* fixture.release("worktree-add");
        yield* Fiber.join(worktree);
        const operation = ended.find((span) => span.name === "GitVcsDriver.createWorktree");
        expect(Object.fromEntries(operation!.attributes)).toMatchObject({
          "git.background_gate_wait_ms": 0,
          "git.shared_permit_wait_ms": 0,
          "git.execution_ms": 9,
        });
        const process = ended.find((span) => span.name === "runGitCommand");
        expect(Object.fromEntries(process!.attributes)).toMatchObject({
          "git.worktree_add.spawn_at_ms": 1_000,
          "git.worktree_add.exit_at_ms": 1_009,
        });
        expect(
          ended
            .map((span) => span.name)
            .filter((name) => name === "GitVcsDriver.createWorktree" || name === "runGitCommand")
            .sort(),
        ).toEqual(["GitVcsDriver.createWorktree", "runGitCommand"]);
      }),
    ),
  );

  it.effect("records execution time on interruption without changing permit return", () =>
    withTelemetry((ended) =>
      Effect.gen(function* () {
        const fixture = yield* makeFixture(["cancelled", "next"]);
        const cancelled = yield* fixture
          .run("cancelled", undefined, "test.timing.cancelled")
          .pipe(asBackgroundGitWork, Effect.forkScoped);
        expect(yield* Queue.take(fixture.starts)).toBe("cancelled");
        yield* TestClock.adjust(13);
        yield* Fiber.interrupt(cancelled);
        const span = ended.find((span) => span.name === "test.timing.cancelled");
        expect(span!.attributes.get("git.execution_ms")).toBe(13);
        const next = yield* fixture.run("next").pipe(Effect.forkScoped);
        expect(yield* Queue.take(fixture.starts)).toBe("next");
        yield* fixture.releaseAll;
        yield* Fiber.join(next);
      }),
    ),
  );
});
