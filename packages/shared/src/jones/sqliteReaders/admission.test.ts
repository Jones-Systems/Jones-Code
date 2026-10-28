import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Scope from "effect/Scope";
import * as Exit from "effect/Exit";
import type { Connection } from "effect/sql/SqlConnection";
import { makeCheckpointReaderAdmission, makeReaderPool, type ReaderSlot } from "./admission.ts";
import type { SqliteReaderPoolOptions } from "../sqliteReaderContracts.ts";

const options = (size: 2 | 3): SqliteReaderPoolOptions => ({
  size,
  interactiveQueueCapacity: 64,
  backgroundQueueCapacity: 64,
  interactiveQueueTimeoutMs: 10_000,
  backgroundQueueTimeoutMs: 30_000,
  leaseWatchdogMs: 60_000,
});
// Admission tests use deliberate lifecycle callbacks; connection integration lives in nodeSqliteClient.test.ts.
const slot = (): ReaderSlot => ({
  acquire: Effect.succeed({} as Connection),
  generation: () => 1,
  retire: () => Effect.void,
  settled: Effect.succeed(true),
});
const acquire = (
  pool: Effect.Success<ReturnType<typeof makeReaderPool>>,
  priority: "interactive" | "background",
) => Effect.withFiber((fiber) => pool.acquire(priority, fiber));

it.effect.each([2, 3] as const)(
  "reserves interactive capacity and gives shared releases to background FIFO (size=%s)",
  (size) =>
    Effect.gen(function* () {
      const bridge = makeCheckpointReaderAdmission();
      const pool = yield* makeReaderPool(options(size), Array.from({ length: size }, slot), bridge);
      const releases = yield* Effect.forEach(Array.from({ length: size }), () =>
        Deferred.make<void>(),
      );
      const ready = yield* Effect.forEach(Array.from({ length: size }), () =>
        Deferred.make<void>(),
      );
      const order: Array<string> = [];
      const hold = (priority: "interactive" | "background", index: number) =>
        Effect.scoped(
          Effect.gen(function* () {
            const lease = yield* acquire(pool, priority);
            assert.equal(lease.slotId, index);
            yield* Deferred.succeed(ready[index]!, undefined);
            yield* Deferred.await(releases[index]!);
          }),
        );
      const holders = [];
      for (let index = 1; index < size; index++) {
        holders.push(
          yield* Effect.forkChild(hold("background", index), { startImmediately: true }),
        );
        yield* Deferred.await(ready[index]!);
      }
      holders.push(yield* Effect.forkChild(hold("interactive", 0), { startImmediately: true }));
      yield* Deferred.await(ready[0]!);
      const queued = (name: string, priority: "interactive" | "background") =>
        Effect.scoped(
          acquire(pool, priority).pipe(
            Effect.tap((lease) =>
              Effect.sync(() => {
                assert.notEqual(lease.slotId, 0);
                order.push(name);
              }),
            ),
          ),
        );
      const interactive = yield* Effect.forkChild(queued("interactive", "interactive"), {
        startImmediately: true,
      });
      const background1 = yield* Effect.forkChild(queued("background-1", "background"), {
        startImmediately: true,
      });
      const background2 = yield* Effect.forkChild(queued("background-2", "background"), {
        startImmediately: true,
      });
      yield* Deferred.succeed(releases[1]!, undefined);
      yield* Fiber.join(background1);
      yield* Fiber.join(background2);
      yield* Fiber.join(interactive);
      assert.deepEqual(order, ["background-1", "background-2", "interactive"]);
      for (const release of releases) yield* Deferred.succeed(release, undefined);
      for (const holder of holders) yield* Fiber.join(holder);
    }).pipe(Effect.scoped),
);

it.effect("unlinks one interrupted waiter and enforces the class queue bound", () =>
  Effect.gen(function* () {
    const bridge = makeCheckpointReaderAdmission();
    yield* bridge.update({ generation: 3, state: "pressure" });
    const pool = yield* makeReaderPool(
      { ...options(2), backgroundQueueCapacity: 1 },
      [slot(), slot()],
      bridge,
    );
    const cancelled = yield* Effect.forkChild(Effect.scoped(acquire(pool, "background")), {
      startImmediately: true,
    });
    const full = yield* Effect.scoped(acquire(pool, "background")).pipe(Effect.flip);
    assert.include(String(full.reason.cause), "queue-full");
    yield* Fiber.interrupt(cancelled);
    const next = yield* Effect.forkChild(Effect.scoped(acquire(pool, "background")), {
      startImmediately: true,
    });
    yield* bridge.update({ generation: 3, state: "open" });
    assert.equal((yield* Fiber.join(next)).slotId, 1);
  }).pipe(Effect.scoped),
);

it.effect("quarantines an unproved retirement without consuming the reserved reader", () =>
  Effect.gen(function* () {
    const bridge = makeCheckpointReaderAdmission();
    const pool = yield* makeReaderPool(
      { ...options(2), backgroundQueueCapacity: 1 },
      [slot(), { ...slot(), settled: Effect.succeed(false) }],
      bridge,
    );
    yield* Effect.scoped(acquire(pool, "background"));
    const waiting = yield* Effect.forkChild(Effect.scoped(acquire(pool, "background")), {
      startImmediately: true,
    });
    assert.isUndefined(waiting.pollUnsafe());
    assert.equal((yield* Effect.scoped(acquire(pool, "interactive"))).slotId, 0);
    yield* Fiber.interrupt(waiting);
  }).pipe(Effect.scoped),
);

it.effect("bounds checkpoint waiting by the original queue deadline", () =>
  Effect.gen(function* () {
    const bridge = makeCheckpointReaderAdmission();
    yield* bridge.update({ generation: 1, state: "pressure" });
    const pool = yield* makeReaderPool(
      { ...options(2), backgroundQueueTimeoutMs: 1 },
      [slot(), slot()],
      bridge,
    );
    const error = yield* Effect.scoped(acquire(pool, "background")).pipe(Effect.flip);
    assert.include(String(error.reason.cause), "checkpoint-held");
    yield* bridge.update({ generation: 1, state: "open" });
    assert.equal((yield* Effect.scoped(acquire(pool, "background"))).slotId, 1);
  }).pipe(Effect.scoped),
);

it.effect("lets pinned work drain while pressure blocks new leases", () =>
  Effect.gen(function* () {
    const bridge = makeCheckpointReaderAdmission();
    const pool = yield* makeReaderPool(options(2), [slot(), slot()], bridge);
    const held = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const owner = yield* Effect.forkChild(
      Effect.scoped(
        Effect.gen(function* () {
          yield* acquire(pool, "background");
          yield* Deferred.succeed(held, undefined);
          yield* Deferred.await(release);
        }),
      ),
      { startImmediately: true },
    );
    yield* Deferred.await(held);
    yield* bridge.update({ generation: 2, state: "truncate" });
    const next = yield* Effect.forkChild(Effect.scoped(acquire(pool, "interactive")), {
      startImmediately: true,
    });
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(owner);
    assert.isUndefined(next.pollUnsafe());
    yield* bridge.update({ generation: 1, state: "open" });
    assert.isUndefined(next.pollUnsafe());
    yield* bridge.update({ generation: 2, state: "open" });
    assert.equal((yield* Fiber.join(next)).slotId, 0);
  }).pipe(Effect.scoped),
);

it.effect("keeps a watchdog's slot occupied until retirement proof completes", () =>
  Effect.gen(function* () {
    const bridge = makeCheckpointReaderAdmission();
    const retiring = yield* Deferred.make<void>();
    const exited = yield* Deferred.make<void>();
    let proved = false;
    const shared: ReaderSlot = {
      ...slot(),
      retire: () =>
        Deferred.succeed(retiring, undefined).pipe(
          Effect.andThen(Deferred.await(exited)),
          Effect.andThen(
            Effect.sync(() => {
              proved = true;
            }),
          ),
        ),
      settled: Effect.sync(() => proved),
    };
    const pool = yield* makeReaderPool(
      { ...options(2), leaseWatchdogMs: 1 },
      [slot(), shared],
      bridge,
    );
    const owner = yield* Effect.forkChild(
      Effect.scoped(acquire(pool, "background").pipe(Effect.andThen(Effect.never))),
      { startImmediately: true },
    );
    yield* Deferred.await(retiring);
    const next = yield* Effect.forkChild(Effect.scoped(acquire(pool, "background")), {
      startImmediately: true,
    });
    assert.isUndefined(owner.pollUnsafe());
    assert.isUndefined(next.pollUnsafe());
    yield* Deferred.succeed(exited, undefined);
    yield* Fiber.await(owner);
    assert.equal((yield* Fiber.join(next)).slotId, 1);
  }).pipe(Effect.scoped),
);

it.effect("closes admission and fails queued leases on database scope shutdown", () =>
  Effect.gen(function* () {
    const databaseScope = yield* Scope.make();
    const bridge = makeCheckpointReaderAdmission();
    yield* bridge.update({ generation: 1, state: "uncertain" });
    const pool = yield* Scope.provide(
      makeReaderPool(options(2), [slot(), slot()], bridge),
      databaseScope,
    );
    const queued = yield* Effect.forkChild(
      Effect.scoped(acquire(pool, "interactive").pipe(Effect.flip)),
      { startImmediately: true },
    );
    yield* Scope.close(databaseScope, Exit.void);
    assert.include(String((yield* Fiber.join(queued)).reason.cause), "reader-unavailable");
    const closed = yield* Effect.scoped(acquire(pool, "background")).pipe(Effect.flip);
    assert.include(String(closed.reason.cause), "reader-unavailable");
  }),
);

it.effect("allows opportunistic shared borrowing while the reserved slot is cooling down", () =>
  Effect.gen(function* () {
    const bridge = makeCheckpointReaderAdmission();
    const pool = yield* makeReaderPool(
      options(2),
      [{ ...slot(), available: () => false }, slot()],
      bridge,
    );
    assert.equal((yield* Effect.scoped(acquire(pool, "interactive"))).slotId, 1);
    assert.equal((yield* Effect.scoped(acquire(pool, "background"))).slotId, 1);
  }).pipe(Effect.scoped),
);

it.effect.each(["undefined", "defect", "interruption"] as const)(
  "releases a %s startup failure before its reservation scope closes",
  (failure) =>
    Effect.gen(function* () {
      const leaseScope = yield* Scope.make();
      yield* Effect.addFinalizer((exit) => Scope.close(leaseScope, exit));
      let scopeClosed = false;
      yield* Scope.addFinalizer(
        leaseScope,
        Effect.sync(() => {
          scopeClosed = true;
        }),
      );
      let attempts = 0;
      let drains = 0;
      const shared: ReaderSlot = {
        ...slot(),
        acquire: Effect.suspend(() => {
          if (++attempts !== 1) return slot().acquire;
          if (failure === "defect") return Effect.die("startup failed");
          if (failure === "interruption") return Effect.interrupt;
          // @effect-diagnostics-next-line effectSucceedWithVoid:off -- An unavailable reader returns undefined; Effect.void is not assignable to the slot's Connection | undefined result.
          return Effect.succeed(undefined);
        }),
        settled: Effect.sync(() => {
          drains++;
          return true;
        }),
      };
      const pool = yield* makeReaderPool(
        { ...options(2), backgroundQueueTimeoutMs: 1_000 },
        [slot(), shared],
        makeCheckpointReaderAdmission(),
      );
      const first = yield* Effect.forkChild(
        Scope.provide(acquire(pool, "background"), leaseScope),
        { startImmediately: true },
      );
      assert.isTrue(Exit.isFailure(yield* Fiber.await(first)));
      assert.equal(drains, 1);
      assert.isFalse(scopeClosed);
      assert.equal((yield* Scope.provide(acquire(pool, "background"), leaseScope)).slotId, 1);
      assert.equal(attempts, 2);
      assert.isFalse(scopeClosed);
      yield* Scope.close(leaseScope, Exit.void);
      assert.equal(drains, 2, "the failed acquisition finalizer must not release twice");
    }).pipe(Effect.scoped),
);

it.effect.each(["checkpoint", "slot"] as const)(
  "closing a queued caller scope removes its waiter before %s admission reopens",
  (blocker) =>
    Effect.gen(function* () {
      const leaseScope = yield* Scope.make();
      const holderScope = yield* Scope.make();
      yield* Effect.addFinalizer((exit) => Scope.close(leaseScope, exit));
      yield* Effect.addFinalizer((exit) => Scope.close(holderScope, exit));
      const bridge = makeCheckpointReaderAdmission();
      if (blocker === "checkpoint") yield* bridge.update({ generation: 1, state: "pressure" });
      let starts = 0;
      const shared = {
        ...slot(),
        acquire: Effect.sync(() => {
          starts++;
          return {} as Connection;
        }),
      };
      const pool = yield* makeReaderPool(
        { ...options(2), backgroundQueueCapacity: 1 },
        [slot(), shared],
        bridge,
      );
      if (blocker === "slot") yield* Scope.provide(acquire(pool, "background"), holderScope);
      const before = starts;
      const queued = yield* Effect.forkChild(
        Scope.provide(acquire(pool, "background"), leaseScope),
        { startImmediately: true },
      );
      yield* Scope.close(leaseScope, Exit.void);
      yield* Scope.close(holderScope, Exit.void);
      yield* bridge.update({ generation: 1, state: "open" });
      assert.isTrue(Exit.isFailure(yield* Fiber.await(queued)));
      assert.equal(starts, before, "a closed lease scope must never start its queued worker");
      assert.equal((yield* Effect.scoped(acquire(pool, "background"))).slotId, 1);
      assert.equal(starts, before + 1);
      assert.isTrue(
        Exit.isFailure(
          yield* Scope.provide(acquire(pool, "background"), leaseScope).pipe(Effect.exit),
        ),
      );
      assert.equal(starts, before + 1, "an already closed scope must not start a worker either");
    }).pipe(Effect.scoped),
);

it.effect("caller scope closure at election cannot orphan capacity or start its worker", () =>
  Effect.gen(function* () {
    const leaseScope = yield* Scope.make();
    yield* Effect.addFinalizer((exit) => Scope.close(leaseScope, exit));
    let closing: Fiber.Fiber<void> | undefined;
    let starts = 0;
    const shared = {
      ...slot(),
      acquire: Effect.sync(() => {
        starts++;
        return {} as Connection;
      }),
    };
    const pool = yield* makeReaderPool(
      {
        ...options(2),
        backgroundQueueTimeoutMs: 1_000,
        observe: (event) => {
          if (event.phase === "queue" && event.outcome === "ok" && closing === undefined) {
            closing = Effect.runFork(Scope.close(leaseScope, Exit.void));
          }
        },
      },
      [slot(), shared],
      makeCheckpointReaderAdmission(),
    );
    yield* Scope.provide(acquire(pool, "background"), leaseScope).pipe(Effect.flip);
    assert.isDefined(closing);
    yield* Fiber.join(closing!);
    assert.equal(starts, 0);
    assert.equal((yield* Effect.scoped(acquire(pool, "background"))).slotId, 1);
    assert.equal(starts, 1);
  }).pipe(Effect.scoped),
);

it.effect.each([true, false])(
  "caller scope closure drains elected startup and native cleanup before re-election (masked=%s)",
  (masked) =>
    Effect.gen(function* () {
      const leaseScope = yield* Scope.make();
      yield* Effect.addFinalizer((exit) => Scope.close(leaseScope, exit));
      const started = yield* Deferred.make<void>();
      const startupDone = yield* Deferred.make<void>();
      const draining = yield* Deferred.make<void>();
      const nativeDone = yield* Deferred.make<void>();
      yield* Effect.addFinalizer(() =>
        Deferred.succeed(startupDone, undefined).pipe(
          Effect.andThen(Deferred.succeed(nativeDone, undefined)),
        ),
      );
      let starts = 0;
      const shared: ReaderSlot = {
        ...slot(),
        acquire: Effect.suspend(() => {
          if (++starts !== 1) return slot().acquire;
          const startup = Deferred.succeed(started, undefined).pipe(
            Effect.andThen(Deferred.await(startupDone)),
            Effect.as({} as Connection),
          );
          return (masked ? Effect.uninterruptible(startup) : startup).pipe(
            Effect.ensuring(
              Deferred.succeed(draining, undefined).pipe(
                Effect.andThen(Deferred.await(nativeDone)),
              ),
            ),
          );
        }),
      };
      const pool = yield* makeReaderPool(
        options(2),
        [slot(), shared],
        makeCheckpointReaderAdmission(),
      );
      const first = yield* Effect.forkChild(
        Scope.provide(acquire(pool, "background"), leaseScope),
        { startImmediately: true },
      );
      yield* Deferred.await(started);
      const closing = yield* Effect.forkChild(Scope.close(leaseScope, Exit.void), {
        startImmediately: true,
      });
      const next = yield* Effect.forkChild(Effect.scoped(acquire(pool, "background")), {
        startImmediately: true,
      });
      assert.isUndefined(closing.pollUnsafe());
      assert.isUndefined(next.pollUnsafe());
      assert.equal(starts, 1);
      yield* Deferred.succeed(startupDone, undefined);
      yield* Deferred.await(draining);
      assert.isUndefined(closing.pollUnsafe());
      assert.isUndefined(next.pollUnsafe());
      assert.equal(starts, 1);
      yield* Deferred.succeed(nativeDone, undefined);
      yield* Fiber.join(closing);
      assert.isTrue(Exit.isFailure(yield* Fiber.await(first)));
      assert.equal((yield* Fiber.join(next)).slotId, 1);
      assert.equal(starts, 2);
    }).pipe(Effect.scoped),
);

it.effect(
  "settles immediate startup cancellation once even when acquisition failure re-enters cleanup",
  () =>
    Effect.gen(function* () {
      const leaseScope = yield* Scope.make();
      yield* Effect.addFinalizer((exit) => Scope.close(leaseScope, exit));
      const started = yield* Deferred.make<void>();
      let starts = 0;
      let drains = 0;
      const shared: ReaderSlot = {
        ...slot(),
        acquire: Effect.suspend(() =>
          ++starts === 1
            ? Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never))
            : slot().acquire,
        ),
        settled: Effect.sync(() => {
          drains++;
          return true;
        }),
      };
      const pool = yield* makeReaderPool(
        options(2),
        [slot(), shared],
        makeCheckpointReaderAdmission(),
      );
      const first = yield* Effect.forkChild(
        Scope.provide(acquire(pool, "background"), leaseScope),
        { startImmediately: true },
      );
      yield* Deferred.await(started);
      yield* Scope.close(leaseScope, Exit.void);
      assert.isTrue(Exit.isFailure(yield* Fiber.await(first)));
      assert.equal(drains, 1);
      assert.equal((yield* Effect.scoped(acquire(pool, "background"))).slotId, 1);
      assert.equal(starts, 2);
      assert.equal(drains, 2);
    }).pipe(Effect.scoped),
);

it.effect("a failed acquisition's stale finalizer cannot release a newer lease", () =>
  Effect.gen(function* () {
    const failedScope = yield* Scope.make();
    const newerScope = yield* Scope.make();
    yield* Effect.addFinalizer((exit) => Scope.close(failedScope, exit));
    yield* Effect.addFinalizer((exit) => Scope.close(newerScope, exit));
    let starts = 0;
    let drains = 0;
    const shared: ReaderSlot = {
      ...slot(),
      acquire: Effect.sync(() => (++starts === 1 ? undefined : ({} as Connection))),
      settled: Effect.sync(() => {
        drains++;
        return true;
      }),
    };
    const pool = yield* makeReaderPool(
      { ...options(2), backgroundQueueTimeoutMs: 1_000 },
      [slot(), shared],
      makeCheckpointReaderAdmission(),
    );
    yield* Scope.provide(acquire(pool, "background"), failedScope).pipe(Effect.flip);
    yield* Scope.provide(acquire(pool, "background"), newerScope);
    yield* Scope.close(failedScope, Exit.void);
    assert.equal(drains, 1);
    const next = yield* Effect.forkChild(Effect.scoped(acquire(pool, "background")), {
      startImmediately: true,
    });
    assert.isUndefined(next.pollUnsafe());
    assert.equal(starts, 2);
    yield* Scope.close(newerScope, Exit.void);
    assert.equal((yield* Fiber.join(next)).slotId, 1);
    assert.equal(starts, 3);
  }).pipe(Effect.scoped),
);

it.effect("retains an unsuccessful acquisition's slot when native retirement is unproved", () =>
  Effect.gen(function* () {
    let starts = 0;
    const shared: ReaderSlot = {
      ...slot(),
      acquire: Effect.sync(() => {
        starts++;
        return undefined;
      }),
      settled: Effect.succeed(false),
    };
    const pool = yield* makeReaderPool(
      options(2),
      [slot(), shared],
      makeCheckpointReaderAdmission(),
    );
    yield* Effect.scoped(acquire(pool, "background")).pipe(Effect.flip);
    const next = yield* Effect.forkChild(Effect.scoped(acquire(pool, "background")), {
      startImmediately: true,
    });
    assert.isUndefined(next.pollUnsafe());
    assert.equal(starts, 1);
    assert.equal((yield* Effect.scoped(acquire(pool, "interactive"))).slotId, 0);
    yield* Fiber.interrupt(next);
  }).pipe(Effect.scoped),
);
