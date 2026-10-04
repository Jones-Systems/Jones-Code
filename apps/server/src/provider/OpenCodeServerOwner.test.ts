import { it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Ref from "effect/Ref";
import * as TestClock from "effect/testing/TestClock";
import { expect } from "vite-plus/test";

import * as OpenCodeRuntime from "./opencodeRuntime.ts";
import * as OpenCodeServerOwner from "./OpenCodeServerOwner.ts";

const unusedRuntimeMethod = () =>
  Effect.fail(
    new OpenCodeRuntime.OpenCodeRuntimeError({
      operation: "unused",
      detail: "unused test method",
    }),
  );

const makeRuntime = Effect.gen(function* () {
  const starts = yield* Ref.make(0);
  const closes = yield* Ref.make(0);
  const failNextStart = yield* Ref.make(false);
  const started = yield* Deferred.make<void>();
  const closed = yield* Deferred.make<void>();
  const runtime: OpenCodeRuntime.OpenCodeRuntimeShape = {
    startOpenCodeServerProcess: () =>
      Effect.gen(function* () {
        if (yield* Ref.getAndSet(failNextStart, false)) {
          return yield* new OpenCodeRuntime.OpenCodeRuntimeError({
            operation: "startOpenCodeServerProcess",
            detail: "start failed",
          });
        }
        const index = yield* Ref.updateAndGet(starts, (count) => count + 1);
        yield* Deferred.succeed(started, undefined).pipe(Effect.ignore);
        yield* Effect.addFinalizer(() =>
          Ref.update(closes, (count) => count + 1).pipe(
            Effect.andThen(Deferred.succeed(closed, undefined)),
            Effect.ignore,
          ),
        );
        return {
          url: `http://127.0.0.1:${index}`,
          version: "1.14.19",
          isRunning: Effect.succeed(true),
          exitCode: Effect.never,
        };
      }),
    connectToOpenCodeServer: unusedRuntimeMethod,
    runOpenCodeCommand: unusedRuntimeMethod,
    createOpenCodeSdkClient: () => ({}) as never,
    loadOpenCodeInventory: unusedRuntimeMethod,
    loadOpenCodeSkills: unusedRuntimeMethod,
    loadInventoryFromCli: unusedRuntimeMethod,
    loadSkillsFromCli: unusedRuntimeMethod,
  };
  return { runtime, starts, closes, failNextStart, started, closed };
});

it.effect("fences all registered borrowers before replacing the owned process", () =>
  Effect.gen(function* () {
    const testRuntime = yield* makeRuntime;
    const order: string[] = [];
    const running: Array<Ref.Ref<boolean>> = [];
    const runtime = {
      ...testRuntime.runtime,
      startOpenCodeServerProcess: () =>
        Effect.gen(function* () {
          order.push("native_start");
          const server = yield* testRuntime.runtime.startOpenCodeServerProcess({
            binaryPath: "synthetic",
            directory: "/synthetic",
          });
          const isRunning = yield* Ref.make(true);
          running.push(isRunning);
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              order.push("native_close");
            }),
          );
          return { ...server, isRunning: Ref.get(isRunning) };
        }),
    } satisfies OpenCodeRuntime.OpenCodeRuntimeShape;
    const owner = yield* OpenCodeServerOwner.make({
      binaryPath: "synthetic",
      directory: "/synthetic",
    }).pipe(Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, runtime));
    const firstEntered = yield* Deferred.make<void>();
    const secondEntered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    let firstGeneration: string | undefined;
    let secondGeneration: string | undefined;
    yield* (
      owner.subscribeBeforeRuntimeReplacement?.((generation) =>
        Effect.gen(function* () {
          yield* Effect.yieldNow;
          firstGeneration = generation;
          order.push("first_fenced");
        }),
      ) ?? Effect.void
    );
    yield* (
      owner.subscribeBeforeRuntimeReplacement?.((generation) =>
        Effect.gen(function* () {
          yield* Effect.yieldNow;
          secondGeneration = generation;
          order.push("second_fenced");
        }),
      ) ?? Effect.void
    );
    const first = yield* owner
      .withServer((server) =>
        Effect.gen(function* () {
          yield* Deferred.succeed(firstEntered, undefined);
          yield* Deferred.await(release);
          return server;
        }),
      )
      .pipe(Effect.forkScoped);
    yield* Deferred.await(firstEntered);
    const second = yield* owner
      .withServer((server) =>
        Effect.gen(function* () {
          yield* Deferred.succeed(secondEntered, undefined);
          yield* Deferred.await(release);
          return server;
        }),
      )
      .pipe(Effect.forkScoped);
    yield* Deferred.await(secondEntered);
    order.length = 0;
    yield* Ref.set(running[0]!, false);
    const replacement = yield* owner.withServer((server) => Effect.succeed(server));
    expect(order).toEqual(["first_fenced", "second_fenced", "native_close", "native_start"]);
    expect(replacement.runtimeGeneration).toBeTypeOf("string");
    expect(firstGeneration).toBe(replacement.runtimeGeneration);
    expect(secondGeneration).toBe(replacement.runtimeGeneration);
    yield* Deferred.succeed(release, undefined);
    const original = yield* Fiber.join(first);
    expect(yield* Fiber.join(second)).toBe(original);
    expect(original.runtimeGeneration).not.toBe(replacement.runtimeGeneration);
  }).pipe(Effect.scoped),
);

it.effect("awaits the current borrower creation guard with the actual daemon directory", () =>
  Effect.gen(function* () {
    const testRuntime = yield* makeRuntime;
    const order: string[] = [];
    const running: Array<Ref.Ref<boolean>> = [];
    const runtime = {
      ...testRuntime.runtime,
      startOpenCodeServerProcess: () =>
        Effect.gen(function* () {
          order.push("native_start");
          const server = yield* testRuntime.runtime.startOpenCodeServerProcess({
            binaryPath: "synthetic",
            directory: "/actual-daemon",
          });
          const isRunning = yield* Ref.make(true);
          running.push(isRunning);
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              order.push("native_close");
            }),
          );
          return { ...server, isRunning: Ref.get(isRunning) };
        }),
    } satisfies OpenCodeRuntime.OpenCodeRuntimeShape;
    const owner = yield* OpenCodeServerOwner.make({
      binaryPath: "synthetic",
      directory: "/actual-daemon",
    }).pipe(Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, runtime));
    yield* owner.withServer(() => Effect.void);
    yield* owner.subscribeBeforeRuntimeReplacement!(() =>
      Effect.sync(() => {
        order.push("fenced");
      }),
    );
    yield* Ref.set(running[0]!, false);
    order.length = 0;
    const failure = new OpenCodeRuntime.OpenCodeRuntimeError({
      operation: "synthetic-authority",
      detail: "Current native creation grant is unavailable.",
    });
    const rejected = yield* owner
      .withServer(
        () => Effect.void,
        (actualDirectory) =>
          Effect.gen(function* () {
            expect(actualDirectory).toBe("/actual-daemon");
            yield* Effect.yieldNow;
            order.push("authorization_rejected");
            return yield* failure;
          }),
      )
      .pipe(Effect.flip);
    expect(rejected.operation).toBe("beforeNativeCreation");
    expect(rejected.cause).toBe(failure);
    expect(order).toEqual(["fenced", "authorization_rejected"]);
    expect(yield* Ref.get(testRuntime.starts)).toBe(1);
    expect(yield* Ref.get(testRuntime.closes)).toBe(0);
    order.length = 0;
    yield* owner.withServer(
      () => Effect.void,
      (actualDirectory) =>
        Effect.gen(function* () {
          expect(actualDirectory).toBe("/actual-daemon");
          yield* Effect.yieldNow;
          order.push("authorized");
        }),
    );
    expect(order).toEqual(["fenced", "authorized", "native_close", "native_start"]);
    yield* owner.withServer(
      () => Effect.void,
      () => Effect.die("Cached native handle must not be recreated."),
    );
    expect(yield* Ref.get(testRuntime.starts)).toBe(2);
  }).pipe(Effect.scoped),
);

it.effect("shares concurrent borrowers and closes after the idle TTL", () =>
  Effect.gen(function* () {
    const testRuntime = yield* makeRuntime;
    const release = yield* Deferred.make<void>();
    yield* Effect.scoped(
      Effect.gen(function* () {
        const owner = yield* OpenCodeServerOwner.make({
          binaryPath: "opencode",
          directory: "/project",
        });
        const useServer = owner.withServer((server) =>
          Deferred.await(release).pipe(Effect.as(server.url)),
        );
        const fibers = yield* Effect.all([useServer, useServer], {
          concurrency: "unbounded",
        }).pipe(Effect.forkChild);
        yield* Deferred.await(testRuntime.started);
        expect(yield* Ref.get(testRuntime.starts)).toBe(1);
        yield* Deferred.succeed(release, undefined);
        expect(yield* Fiber.join(fibers)).toEqual(["http://127.0.0.1:1", "http://127.0.0.1:1"]);
        yield* TestClock.adjust(Duration.seconds(31));
        yield* Deferred.await(testRuntime.closed);
        expect(yield* Ref.get(testRuntime.closes)).toBe(1);
      }),
    ).pipe(Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, testRuntime.runtime));
  }).pipe(Effect.provide(TestClock.layer())),
);

it.effect("retries a failed start and closes on owner scope shutdown", () =>
  Effect.gen(function* () {
    const testRuntime = yield* makeRuntime;
    yield* Ref.set(testRuntime.failNextStart, true);
    yield* Effect.scoped(
      Effect.gen(function* () {
        const owner = yield* OpenCodeServerOwner.make({
          binaryPath: "opencode",
          directory: "/project",
        });
        expect(
          (yield* Effect.exit(owner.withServer((server) => Effect.succeed(server.url))))._tag,
        ).toBe("Failure");
        expect(yield* owner.withServer((server) => Effect.succeed(server.url))).toBe(
          "http://127.0.0.1:1",
        );
      }),
    ).pipe(Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, testRuntime.runtime));
    expect(yield* Ref.get(testRuntime.starts)).toBe(1);
    expect(yield* Ref.get(testRuntime.closes)).toBe(1);
  }),
);

it.effect("invalidates an exited process so the next borrower starts a new one", () =>
  Effect.gen(function* () {
    const starts = yield* Ref.make(0);
    const processExits: Array<Deferred.Deferred<number>> = [];
    const processClosed = yield* Deferred.make<void>();
    const runtime: OpenCodeRuntime.OpenCodeRuntimeShape = {
      startOpenCodeServerProcess: () =>
        Effect.gen(function* () {
          const index = yield* Ref.updateAndGet(starts, (count) => count + 1);
          const exitCode = yield* Deferred.make<number>();
          processExits.push(exitCode);
          yield* Effect.addFinalizer(() =>
            Deferred.succeed(processClosed, undefined).pipe(Effect.ignore),
          );
          return {
            url: `http://127.0.0.1:${index}`,
            version: "1.14.19",
            isRunning: Effect.succeed(true),
            exitCode: Deferred.await(exitCode),
          };
        }),
      connectToOpenCodeServer: unusedRuntimeMethod,
      runOpenCodeCommand: unusedRuntimeMethod,
      createOpenCodeSdkClient: () => ({}) as never,
      loadOpenCodeInventory: unusedRuntimeMethod,
      loadOpenCodeSkills: unusedRuntimeMethod,
      loadInventoryFromCli: unusedRuntimeMethod,
      loadSkillsFromCli: unusedRuntimeMethod,
    };

    yield* Effect.scoped(
      Effect.gen(function* () {
        const owner = yield* OpenCodeServerOwner.make({
          binaryPath: "opencode",
          directory: "/project",
        });
        expect(yield* owner.withServer((server) => Effect.succeed(server.url))).toBe(
          "http://127.0.0.1:1",
        );
        yield* Deferred.succeed(processExits[0]!, 1);
        yield* Deferred.await(processClosed);
        expect(yield* owner.withServer((server) => Effect.succeed(server.url))).toBe(
          "http://127.0.0.1:2",
        );
      }),
    ).pipe(Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, runtime));
    expect(yield* Ref.get(starts)).toBe(2);
  }),
);

it.effect("replaces a dead cached process before its exit watcher runs", () =>
  Effect.gen(function* () {
    const starts = yield* Ref.make(0);
    const closes = yield* Ref.make(0);
    const processRunning: Array<Ref.Ref<boolean>> = [];
    const runtime: OpenCodeRuntime.OpenCodeRuntimeShape = {
      startOpenCodeServerProcess: () =>
        Effect.gen(function* () {
          const index = yield* Ref.updateAndGet(starts, (count) => count + 1);
          const isRunning = yield* Ref.make(true);
          processRunning.push(isRunning);
          yield* Effect.addFinalizer(() => Ref.update(closes, (count) => count + 1));
          return {
            url: `http://127.0.0.1:${index}`,
            version: "1.14.19",
            isRunning: Ref.get(isRunning),
            exitCode: Effect.never,
          };
        }),
      connectToOpenCodeServer: unusedRuntimeMethod,
      runOpenCodeCommand: unusedRuntimeMethod,
      createOpenCodeSdkClient: () => ({}) as never,
      loadOpenCodeInventory: unusedRuntimeMethod,
      loadOpenCodeSkills: unusedRuntimeMethod,
      loadInventoryFromCli: unusedRuntimeMethod,
      loadSkillsFromCli: unusedRuntimeMethod,
    };

    yield* Effect.scoped(
      Effect.gen(function* () {
        const owner = yield* OpenCodeServerOwner.make({
          binaryPath: "opencode",
          directory: "/project",
        });
        expect(yield* owner.withServer((server) => Effect.succeed(server.url))).toBe(
          "http://127.0.0.1:1",
        );
        yield* Ref.set(processRunning[0]!, false);

        expect(yield* owner.withServer((server) => Effect.succeed(server.url))).toBe(
          "http://127.0.0.1:2",
        );
        expect(yield* Ref.get(starts)).toBe(2);
        expect(yield* Ref.get(closes)).toBe(1);
      }),
    ).pipe(Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, runtime));
  }),
);

it.effect("cleans up an interrupted startup and allows a retry", () =>
  Effect.gen(function* () {
    const starts = yield* Ref.make(0);
    const firstStartEntered = yield* Deferred.make<void>();
    const firstStartClosed = yield* Deferred.make<void>();
    const runtime: OpenCodeRuntime.OpenCodeRuntimeShape = {
      startOpenCodeServerProcess: () =>
        Effect.gen(function* () {
          const index = yield* Ref.updateAndGet(starts, (count) => count + 1);
          yield* Effect.addFinalizer(() =>
            index === 1
              ? Deferred.succeed(firstStartClosed, undefined).pipe(Effect.ignore)
              : Effect.void,
          );
          if (index === 1) {
            yield* Deferred.succeed(firstStartEntered, undefined);
            return yield* Effect.never;
          }
          return {
            url: `http://127.0.0.1:${index}`,
            version: "1.14.19",
            isRunning: Effect.succeed(true),
            exitCode: Effect.never,
          };
        }),
      connectToOpenCodeServer: unusedRuntimeMethod,
      runOpenCodeCommand: unusedRuntimeMethod,
      createOpenCodeSdkClient: () => ({}) as never,
      loadOpenCodeInventory: unusedRuntimeMethod,
      loadOpenCodeSkills: unusedRuntimeMethod,
      loadInventoryFromCli: unusedRuntimeMethod,
      loadSkillsFromCli: unusedRuntimeMethod,
    };

    yield* Effect.scoped(
      Effect.gen(function* () {
        const owner = yield* OpenCodeServerOwner.make({
          binaryPath: "opencode",
          directory: "/project",
        });
        const firstBorrower = yield* owner
          .withServer((server) => Effect.succeed(server.url))
          .pipe(Effect.forkChild);
        yield* Deferred.await(firstStartEntered);
        yield* Fiber.interrupt(firstBorrower);
        yield* Deferred.await(firstStartClosed);
        expect(yield* owner.withServer((server) => Effect.succeed(server.url))).toBe(
          "http://127.0.0.1:2",
        );
      }),
    ).pipe(Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, runtime));
  }),
);

it.effect("releases an interrupted borrower and closes after the idle TTL", () =>
  Effect.gen(function* () {
    const testRuntime = yield* makeRuntime;
    const borrowerEntered = yield* Deferred.make<void>();
    yield* Effect.scoped(
      Effect.gen(function* () {
        const owner = yield* OpenCodeServerOwner.make({
          binaryPath: "opencode",
          directory: "/project",
        });
        const borrower = yield* owner
          .withServer(() =>
            Deferred.succeed(borrowerEntered, undefined).pipe(Effect.andThen(Effect.never)),
          )
          .pipe(Effect.forkChild);
        yield* Deferred.await(borrowerEntered);
        yield* Fiber.interrupt(borrower);
        yield* TestClock.adjust(Duration.seconds(31));
        yield* Deferred.await(testRuntime.closed);
        expect(yield* Ref.get(testRuntime.closes)).toBe(1);
      }),
    ).pipe(Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, testRuntime.runtime));
  }).pipe(Effect.provide(TestClock.layer())),
);
