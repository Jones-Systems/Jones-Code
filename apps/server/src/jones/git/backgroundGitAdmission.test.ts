import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Cache from "effect/Cache";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import * as ServerConfig from "../../config.ts";
import { makeGitVcsDriverCore } from "../../vcs/GitVcsDriverCore.ts";
import { asBackgroundGitWork } from "./backgroundGitAdmission.ts";

const dependencies = ServerConfig.layerTest(process.cwd(), {
  prefix: "background-git-admission-test-",
}).pipe(Layer.provideMerge(NodeServices.layer));

const makeFixture = (labels: readonly string[]) =>
  Effect.gen(function* () {
    const starts = yield* Queue.unbounded<string>();
    const gates = new Map(
      yield* Effect.forEach(labels, (label) =>
        Deferred.make<void>().pipe(Effect.map((gate) => [label, gate] as const)),
      ),
    );
    let activeBackground = 0;
    let peakBackground = 0;
    const started: string[] = [];
    const releaseAll = Effect.forEach(gates.values(), (gate) => Deferred.succeed(gate, undefined), {
      discard: true,
    });
    yield* Effect.addFinalizer(() => releaseAll);
    const spawner = ChildProcessSpawner.make((command) =>
      Effect.gen(function* () {
        if (!ChildProcess.isStandardCommand(command)) {
          return yield* Effect.die("expected a standard Git command");
        }
        const label = command.args[0]!;
        const gate = gates.get(label);
        if (gate === undefined) return yield* Effect.die(`unexpected Git command: ${label}`);
        started.push(label);
        const background = label.startsWith("background-");
        if (background) {
          activeBackground++;
          peakBackground = Math.max(peakBackground, activeBackground);
        }
        yield* Queue.offer(starts, label);
        return ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(1),
          exitCode: Deferred.await(gate).pipe(
            Effect.as(ChildProcessSpawner.ExitCode(0)),
            Effect.ensuring(
              Effect.sync(() => {
                if (background) activeBackground--;
              }),
            ),
          ),
          isRunning: Effect.succeed(true),
          kill: () => Deferred.succeed(gate, undefined).pipe(Effect.asVoid),
          unref: Effect.succeed(Effect.void),
          stdin: Sink.drain,
          stdout: Stream.empty,
          stderr: Stream.empty,
          all: Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
        });
      }),
    );
    const driver = yield* makeGitVcsDriverCore().pipe(
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    );
    return {
      starts,
      started,
      peakBackground: () => peakBackground,
      release: (label: string) => Deferred.succeed(gates.get(label)!, undefined),
      releaseAll,
      run: (label: string, timeoutMs?: number | null) =>
        driver.execute({
          cwd: "/workspace/background-git-admission",
          operation: "test.backgroundGitAdmission",
          args: [label],
          ...(timeoutMs === undefined ? {} : { timeoutMs }),
        }),
    };
  });

describe("background Git admission", () => {
  it.effect("caps marked Git at four while retaining eight shared permits and timeout bypasses", () =>
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
        const background = yield* Effect.forEach(backgroundLabels, (label) => fixture.run(label), {
          concurrency: "unbounded",
        }).pipe(asBackgroundGitWork, Effect.forkScoped);
        const firstBackground = yield* Effect.forEach(Array.from({ length: 4 }), () =>
          Queue.take(fixture.starts),
        );
        expect(firstBackground.every((label) => label.startsWith("background-"))).toBe(true);
        yield* Effect.yieldNow;
        expect(yield* Queue.size(fixture.starts)).toBe(0);

        const foreground = yield* Effect.forEach(foregroundLabels, (label) => fixture.run(label), {
          concurrency: "unbounded",
        }).pipe(Effect.forkScoped);
        const firstForeground = yield* Effect.forEach(Array.from({ length: 4 }), () =>
          Queue.take(fixture.starts),
        );
        expect(firstForeground.every((label) => label.startsWith("foreground-"))).toBe(true);
        yield* Effect.yieldNow;
        expect(yield* Queue.size(fixture.starts)).toBe(0);

        const unlimited = yield* fixture.run("unlimited-timeout", null).pipe(
          asBackgroundGitWork,
          Effect.forkScoped,
        );
        expect(yield* Queue.take(fixture.starts)).toBe("unlimited-timeout");
        const long = yield* fixture.run("long-timeout", 30_001).pipe(
          asBackgroundGitWork,
          Effect.forkScoped,
        );
        expect(yield* Queue.take(fixture.starts)).toBe("long-timeout");
        yield* fixture.releaseAll;
        expect(yield* Fiber.join(background)).toHaveLength(8);
        expect(yield* Fiber.join(foreground)).toHaveLength(5);
        yield* Fiber.join(unlimited);
        yield* Fiber.join(long);
        expect(fixture.peakBackground()).toBe(4);
        expect(fixture.started.filter((label) => label.startsWith("background-"))).toHaveLength(8);
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
          timeToLive: "1 minute",
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
        const cancelled = yield* fixture.run("background-cancelled").pipe(
          asBackgroundGitWork,
          Effect.forkScoped,
        );
        yield* Effect.yieldNow;
        expect(yield* Queue.size(fixture.starts)).toBe(0);
        yield* Fiber.interrupt(cancelled);
        yield* Fiber.interrupt(held[0]!);
        const next = yield* fixture.run("background-next").pipe(
          asBackgroundGitWork,
          Effect.forkScoped,
        );
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
