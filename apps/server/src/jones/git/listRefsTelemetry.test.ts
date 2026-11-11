import { describe, expect, it } from "@effect/vitest";
import * as Cache from "effect/Cache";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Queue from "effect/Queue";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as Tracer from "effect/Tracer";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { TestClock } from "effect/testing";

import { makeGitVcsDriverCore } from "../../vcs/GitVcsDriverCore.ts";
import { dependencies } from "./backgroundGitAdmission.fixture.ts";
import { asBackgroundGitWork } from "./backgroundGitAdmission.ts";
import {
  getObservedListRefsSnapshot,
  markListRefsSnapshotRead,
  observeListRefs,
} from "./listRefsTelemetry.ts";

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

const listRefsSpans = (ended: ReadonlyArray<Tracer.NativeSpan>) =>
  ended
    .filter((span) => span.name === "listRefs")
    .map((span) => Object.fromEntries(span.attributes));

// Real Git for the repository; `held-*` commands block until released, standing in for
// background PR discovery that occupies Git admission.
const makeDriver = Effect.fn("ListRefsTelemetryTest.makeDriver")(function* () {
  const delegate = yield* ChildProcessSpawner.ChildProcessSpawner;
  const heldStarts = yield* Queue.unbounded<string>();
  const releaseHeld = yield* Deferred.make<void>();
  yield* Effect.addFinalizer(() => Deferred.succeed(releaseHeld, undefined));
  const spawner = ChildProcessSpawner.make((command) =>
    Effect.gen(function* () {
      if (!ChildProcess.isStandardCommand(command)) {
        return yield* Effect.die("expected a standard Git command");
      }
      const label = command.args[0] ?? "";
      if (!label.startsWith("held-")) return yield* delegate.spawn(command);
      yield* Queue.offer(heldStarts, label);
      return ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(1),
        exitCode: Deferred.await(releaseHeld).pipe(Effect.as(ChildProcessSpawner.ExitCode(0))),
        isRunning: Effect.succeed(true),
        kill: () => Deferred.succeed(releaseHeld, undefined).pipe(Effect.asVoid),
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
  const cwd = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped({
    prefix: "list-refs-telemetry-test-",
  });
  const git = (args: readonly string[]) =>
    driver.execute({ operation: "test.listRefsTelemetry.setup", cwd, args, timeoutMs: 10_000 });
  yield* git(["init", "--quiet"]);
  yield* git([
    "-c",
    "user.email=test@test.com",
    "-c",
    "user.name=Test",
    "-c",
    "commit.gpgsign=false",
    "commit",
    "--allow-empty",
    "--quiet",
    "-m",
    "initial commit",
  ]);
  const hold = (count: number) =>
    Effect.forEach(
      Array.from({ length: count }, (_, index) => `held-${index}`),
      (label) => driver.execute({ operation: "test.listRefsTelemetry.held", cwd, args: [label] }),
      { concurrency: "unbounded" },
    );
  return {
    driver,
    cwd,
    heldStarts,
    hold,
    releaseHeld: Deferred.succeed(releaseHeld, undefined),
  };
});

describe("listRefs span telemetry", () => {
  it.effect("labels snapshot misses, hits and refreshes with this call's Git work", () =>
    withTelemetry((ended) =>
      Effect.gen(function* () {
        const { driver, cwd } = yield* makeDriver();
        yield* driver.listRefs({ cwd });
        yield* driver.listRefs({ cwd });
        // Pass the refresh coalescing window so the refresh reads Git again.
        yield* TestClock.adjust("6 seconds");
        yield* driver.listRefs({ cwd, refresh: true });

        const [miss, hit, refresh] = listRefsSpans(ended);
        expect(miss).toMatchObject({
          "vcs.list_refs.refresh": false,
          "vcs.list_refs.snapshot": "miss",
          "vcs.list_refs.git_background_gate_wait_ms_max": 0,
        });
        // for-each-ref, symbolic-ref, worktree list and remote at least.
        expect(miss?.["vcs.list_refs.git_commands"]).toBeGreaterThanOrEqual(4);
        expect(hit).toMatchObject({
          "vcs.list_refs.refresh": false,
          "vcs.list_refs.snapshot": "hit",
          "vcs.list_refs.git_commands": 0,
          "vcs.list_refs.git_execution_ms": 0,
        });
        expect(refresh).toMatchObject({
          "vcs.list_refs.refresh": true,
          "vcs.list_refs.snapshot": "miss",
        });
        expect(refresh?.["vcs.list_refs.git_commands"]).toBeGreaterThanOrEqual(4);
      }),
    ),
  );

  it.effect("completes foreground listRefs while background Git saturates its gate", () =>
    withTelemetry((ended) =>
      Effect.gen(function* () {
        const { driver, cwd, heldStarts, hold, releaseHeld } = yield* makeDriver();
        // Four hold the background gate; two more queue behind it.
        const background = yield* hold(6).pipe(asBackgroundGitWork, Effect.forkScoped);
        yield* Effect.forEach(Array.from({ length: 4 }), () => Queue.take(heldStarts));
        yield* Effect.yieldNow;
        expect(yield* Queue.size(heldStarts)).toBe(0);

        const refs = yield* driver.listRefs({ cwd, refresh: true });
        expect(refs.isRepo).toBe(true);
        expect(yield* Queue.size(heldStarts)).toBe(0);
        expect(listRefsSpans(ended)).toEqual([
          expect.objectContaining({
            "vcs.list_refs.snapshot": "miss",
            "vcs.list_refs.git_background_gate_wait_ms_max": 0,
            "vcs.list_refs.git_shared_permit_wait_ms_max": 0,
          }),
        ]);

        // The same call marked as background waits for the gate, and the span records it.
        yield* TestClock.adjust("6 seconds");
        const marked = yield* driver
          .listRefs({ cwd, refresh: true })
          .pipe(asBackgroundGitWork, Effect.forkChild({ startImmediately: true }));
        yield* Effect.forEach(Array.from({ length: 5 }), () => Effect.yieldNow);
        yield* TestClock.adjust(13);
        yield* releaseHeld;
        yield* Fiber.join(marked);
        yield* Fiber.join(background);
        const markedSpan = listRefsSpans(ended)[1];
        expect(
          markedSpan?.["vcs.list_refs.git_background_gate_wait_ms_max"],
        ).toBeGreaterThanOrEqual(13);
      }),
    ),
  );

  it.effect("labels a call that joins another caller's in-flight read", () =>
    withTelemetry((ended) =>
      Effect.gen(function* () {
        const gate = yield* Deferred.make<void>();
        const cache = yield* Cache.makeWith(
          (key: string) => markListRefsSnapshotRead(Deferred.await(gate).pipe(Effect.as(key))),
          { capacity: 4, timeToLive: () => "1 minute" },
        );
        const call = getObservedListRefsSnapshot(cache, "refs").pipe(
          observeListRefs(false),
          Effect.withSpan("listRefs"),
        );
        const first = yield* call.pipe(Effect.forkChild({ startImmediately: true }));
        const second = yield* call.pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.succeed(gate, undefined);
        yield* Fiber.join(first);
        yield* Fiber.join(second);
        yield* call;
        expect(listRefsSpans(ended).map((span) => span["vcs.list_refs.snapshot"])).toEqual([
          "miss",
          "joined",
          "hit",
        ]);
      }),
    ),
  );
});
