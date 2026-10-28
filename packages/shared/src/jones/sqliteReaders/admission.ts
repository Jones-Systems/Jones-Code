// @effect-diagnostics-next-line nodeBuiltinImport:off -- The callback boundary owns real-time admission and retirement deadlines independently of caller interruption and the Effect test clock.
import * as NodeTimers from "node:timers";

import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Scheduler from "effect/Scheduler";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import type { Connection } from "effect/sql/SqlConnection";
import { SqlError, classifySqliteError } from "effect/sql/SqlError";

import type { SqliteReadPriority } from "../sqliteReadPriority.ts";
import type { SqliteReaderObservation, SqliteReaderPoolOptions } from "../sqliteReaderContracts.ts";

type AdmissionFailure =
  | "queue-full"
  | "queue-timeout"
  | "reader-unavailable"
  | "checkpoint-held"
  | "generation-retired";

const admissionError = (reason: AdmissionFailure) =>
  new SqlError({
    reason: classifySqliteError(new Error(reason), {
      message: "SQLite reader admission failed",
      operation: "acquire",
    }),
  });

export interface ReaderSlot {
  readonly acquire: Effect.Effect<Connection | undefined>;
  readonly generation: () => number;
  readonly available?: () => boolean;
  readonly retire: (reason: string) => Effect.Effect<void>;
  /** False keeps the slot quarantined: a failed termination is not an exit. */
  readonly settled: Effect.Effect<boolean>;
}

export interface ReaderLease {
  readonly slotId: number;
  readonly generation: number;
  readonly connection: Connection;
}

type CheckpointState = {
  readonly generation: number;
  readonly state: "open" | "pressure" | "truncate" | "uncertain" | "restored";
};

/** Synchronous publication prevents admission between checkpoint evidence and its effect. */
export const makeCheckpointReaderAdmission = () => {
  let generation = -1;
  let open = true;
  const listeners = new Set<() => void>();
  let released = () => {};
  const updateUnsafe = (next: CheckpointState) => {
    if (next.generation < generation) return;
    generation = next.generation;
    open = next.state === "open" || next.state === "restored";
    for (const listener of listeners) listener();
  };
  return {
    isOpen: () => open,
    updateUnsafe,
    update: (next: CheckpointState) => Effect.sync(() => updateUnsafe(next)),
    subscribe: (listener: () => void) => {
      listeners.add(listener);
      return () => { listeners.delete(listener); };
    },
    onReaderReleased: (callback: () => void) => { released = callback; },
    readerReleased: () => Effect.sync(() => released()),
  };
};

export type CheckpointReaderAdmission = ReturnType<typeof makeCheckpointReaderAdmission>;

export const makeReaderPool = Effect.fnUntraced(function* (
  options: SqliteReaderPoolOptions,
  slots: ReadonlyArray<ReaderSlot>,
  checkpoint: CheckpointReaderAdmission,
) {
  if (slots.length !== options.size || (options.size !== 2 && options.size !== 3)) {
    return yield* Effect.die(new Error("SQLite reader pool requires exactly two or three slots"));
  }
  const limits = [
    options.interactiveQueueCapacity, options.backgroundQueueCapacity,
    options.interactiveQueueTimeoutMs, options.backgroundQueueTimeoutMs, options.leaseWatchdogMs,
  ];
  if (limits.some((limit) => !Number.isSafeInteger(limit) || limit <= 0)) {
    return yield* Effect.die(new Error("SQLite reader admission limits must be positive integers"));
  }
  const clock = yield* Clock.Clock;
  const now = () => Number(clock.monotonicTimeNanosUnsafe()) / 1e6;
  const observe = (event: SqliteReaderObservation) => {
    try { options.observe?.(event); } catch {}
  };
  type Waiter = {
    readonly priority: SqliteReadPriority;
    readonly start: number;
    readonly deadline: number;
    checkpointStart: number | undefined;
    checkpointMs: number;
    readonly resume: (slot: number | SqlError) => void;
    timer: ReturnType<typeof NodeTimers.setTimeout> | undefined;
    done: boolean;
  };
  const queues: Record<SqliteReadPriority, Array<Waiter>> = { interactive: [], background: [] };
  const occupied = slots.map(() => false);
  let closed = false;
  const depth = () => queues.interactive.length + queues.background.length;
  const unlink = (waiter: Waiter) => {
    const queue = queues[waiter.priority];
    const index = queue.indexOf(waiter);
    if (index !== -1) queue.splice(index, 1);
    NodeTimers.clearTimeout(waiter.timer);
  };
  const expire = (waiter: Waiter) => {
    if (waiter.done) return;
    waiter.done = true;
    unlink(waiter);
    observe({
      phase: "queue", priority: waiter.priority, slotId: -1, generation: 0,
      durationMs: now() - waiter.start, outcome: "timeout", queueDepth: depth(),
    });
    waiter.resume(admissionError(checkpoint.isOpen() ? "queue-timeout" : "checkpoint-held"));
  };
  const elect = (queue: Array<Waiter>) => {
    let waiter: Waiter | undefined;
    while ((waiter = queue.shift()) !== undefined) {
      if (now() >= waiter.deadline) expire(waiter);
      else return waiter;
    }
    return undefined;
  };
  const dispatch = () => {
    if (closed || !checkpoint.isOpen()) return;
    // Reserve slot zero exclusively; shared slots always elect background first.
    for (let slot = 0; slot < slots.length; slot++) {
      if (occupied[slot] || slots[slot]!.available?.() === false) continue;
      const waiter = slot === 0
        ? elect(queues.interactive)
        : elect(queues.background) ?? elect(queues.interactive);
      if (waiter === undefined) continue;
      occupied[slot] = true;
      waiter.done = true;
      NodeTimers.clearTimeout(waiter.timer);
      const labels = {
        priority: waiter.priority, slotId: slot, generation: slots[slot]!.generation(),
        outcome: "ok" as const, queueDepth: depth(),
      };
      // Publish ownership before an observer can synchronously close the caller scope.
      waiter.resume(slot);
      if (waiter.checkpointMs > 0) {
        observe({ ...labels, phase: "checkpoint", durationMs: waiter.checkpointMs });
      }
      observe({ ...labels, phase: "queue", durationMs: now() - waiter.start });
    }
    for (const priority of ["interactive", "background"] as const) {
      const eligible = priority === "interactive" ? slots : slots.slice(1);
      const offset = priority === "interactive" ? 0 : 1;
      if (!eligible.every((slot, index) => !occupied[index + offset] && slot.available?.() === false)) continue;
      for (const waiter of queues[priority].splice(0)) {
        waiter.done = true;
        NodeTimers.clearTimeout(waiter.timer);
        waiter.resume(admissionError("reader-unavailable"));
      }
    }
  };
  const unsubscribe = checkpoint.subscribe(() => {
    const time = now();
    for (const queue of Object.values(queues)) {
      for (const waiter of queue) {
        if (checkpoint.isOpen() && waiter.checkpointStart !== undefined) {
          waiter.checkpointMs += time - waiter.checkpointStart;
          waiter.checkpointStart = undefined;
        } else if (!checkpoint.isOpen() && waiter.checkpointStart === undefined) {
          waiter.checkpointStart = time;
        }
      }
    }
    dispatch();
  });
  yield* Effect.addFinalizer(() => Effect.sync(() => {
    closed = true;
    unsubscribe();
    for (const queue of Object.values(queues)) {
      for (const waiter of queue.splice(0)) {
        waiter.done = true;
        NodeTimers.clearTimeout(waiter.timer);
        waiter.resume(admissionError("reader-unavailable"));
      }
    }
  }));

  const acquire = (priority: SqliteReadPriority, owner: Fiber.Fiber<unknown, unknown>) =>
    Effect.uninterruptibleMask((restore) => Effect.gen(function* () {
      const start = now();
      let reserved: number | undefined;
      let active = true;
      let waiting: Waiter | undefined;
      const cancelWait = () => {
        if (waiting === undefined || waiting.done) return;
        waiting.done = true;
        unlink(waiting);
        observe({
          phase: "queue", priority, slotId: -1, generation: 0,
          durationMs: now() - start, outcome: "cancelled", queueDepth: depth(),
        });
        waiting.resume(admissionError("generation-retired"));
      };
      const wait = Effect.callback<number, SqlError>((resume) => {
        if (closed || !active) {
          resume(Effect.fail(admissionError("reader-unavailable")));
          return;
        }
        const capacity = priority === "interactive"
          ? options.interactiveQueueCapacity : options.backgroundQueueCapacity;
        if (queues[priority].length >= Math.min(64, capacity)) {
          resume(Effect.fail(admissionError("queue-full")));
          return;
        }
        const timeout = priority === "interactive"
          ? Math.min(10_000, options.interactiveQueueTimeoutMs)
          : Math.min(30_000, options.backgroundQueueTimeoutMs);
        const waiter: Waiter = {
          priority, start, deadline: start + timeout,
          checkpointStart: checkpoint.isOpen() ? undefined : now(),
          checkpointMs: 0, timer: undefined, done: false,
          resume: (result) => {
            if (typeof result === "number") {
              reserved = result;
              resume(Effect.succeed(result));
            } else resume(Effect.fail(result));
          },
        };
        waiting = waiter;
        queues[priority].push(waiter);
        // @effect-diagnostics-next-line globalTimersInEffect:off -- This queue-owned real-time deadline outlives caller masking and is cleared on election, cancellation or shutdown.
        waiter.timer = NodeTimers.setTimeout(() => expire(waiter), timeout);
        dispatch();
        return Effect.sync(cancelWait);
      });
      // Register ownership before restoring cancellation: an elected callback can be interrupted.
      const scope = yield* Scope.Scope;
      let watchdog: ReturnType<typeof NodeTimers.setTimeout> | undefined;
      let timedOut = false;
      let retirement: Fiber.Fiber<void> | undefined;
      let starting: Fiber.Fiber<Connection | undefined> | undefined;
      const released = yield* Deferred.make<void>();
      let releasing = false;
      const runFork = Effect.runForkWith(yield* Effect.context<never>());
      const release = (exit: Exit.Exit<unknown, unknown>) => Effect.uninterruptible(Effect.suspend(() => {
        if (releasing) return Deferred.await(released);
        // Cancellation can synchronously resume acquisition and re-enter this finalizer.
        releasing = true;
        active = false;
        cancelWait();
        return Deferred.complete(released, Effect.gen(function* () {
          // Startup cancellation must drain before this slot can be elected again.
          if (starting !== undefined) yield* Fiber.interrupt(starting);
          if (reserved === undefined) return;
          const slotId = reserved;
          if (retirement !== undefined) yield* Fiber.join(retirement);
          const safe = yield* slots[slotId]!.settled;
          NodeTimers.clearTimeout(watchdog);
          observe({
            phase: "lease", priority, slotId, generation: slots[slotId]!.generation(),
            durationMs: now() - start, queueDepth: depth(),
            outcome: timedOut ? "timeout" : !safe ? "unavailable"
              : Exit.hasInterrupts(exit) ? "cancelled" : Exit.isFailure(exit) ? "failed" : "ok",
          });
          if (safe) occupied[slotId] = false;
          yield* checkpoint.readerReleased();
          dispatch();
        }).pipe(Effect.ensuring(Effect.sync(() => NodeTimers.clearTimeout(watchdog)))))
          .pipe(Effect.andThen(Deferred.await(released)));
      }));
      yield* Scope.addFinalizerExit(scope, release);
      return yield* Effect.gen(function* () {
        const slotId = yield* restore(wait);
        const slot = slots[slotId]!;
        if (!active) return yield* admissionError("generation-retired");
        const startup = now();
        // Fork while masked and publish before joining so scope closure can cancel and drain it.
        starting = yield* Effect.forkChild(restore(slot.acquire));
        const connection = yield* restore(Fiber.join(starting));
        observe({
          phase: "startup", priority, slotId, generation: slot.generation(),
          durationMs: now() - startup, outcome: connection === undefined ? "unavailable" : "ok",
          queueDepth: depth(),
        });
        if (connection === undefined) return yield* admissionError("reader-unavailable");
        if (!active) return yield* admissionError("generation-retired");
        // @effect-diagnostics-next-line globalTimersInEffect:off -- A real-time watchdog must initiate native worker retirement even when its owner fiber is uninterruptible; release clears it.
        watchdog = NodeTimers.setTimeout(() => {
          timedOut = true;
          active = false;
          retirement = runFork(slot.retire("SQLite reader lease watchdog expired"));
          owner.interruptUnsafe();
        }, Math.min(60_000, options.leaseWatchdogMs));
        const generation = slot.generation();
        const guard = <A, E>(execute: () => Effect.Effect<A, E>): Effect.Effect<A, E | SqlError> =>
          Effect.suspend((): Effect.Effect<A, E | SqlError> =>
            active && slot.generation() === generation
              ? execute()
              : Effect.fail(admissionError("generation-retired")),
          ).pipe(Effect.provideService(Scheduler.PreventSchedulerYield, true));
        const scopedConnection: Connection = {
          execute: (...args) => guard(() => connection.execute(...args)),
          executeRaw: (...args) => guard(() => connection.executeRaw(...args)),
          executeValues: (...args) => guard(() => connection.executeValues(...args)),
          executeValuesUnprepared: (...args) => guard(() => connection.executeValuesUnprepared(...args)),
          executeUnprepared: (...args) => guard(() => connection.executeUnprepared(...args)),
          executeStream: (...args) => Stream.unwrap(guard(() => Effect.succeed(connection.executeStream(...args)))),
        };
        return { slotId, generation, connection: scopedConnection } satisfies ReaderLease;
      }).pipe(Effect.onExit((exit) => Exit.isFailure(exit) ? release(exit) : Effect.void));
    }).pipe(Effect.provideService(Scheduler.PreventSchedulerYield, true)));
  return { acquire };
});
