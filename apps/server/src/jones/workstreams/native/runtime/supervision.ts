import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";

/** Cadence of every native registry background loop, measured from one iteration's end to the next start. */
const LOOP_INTERVAL_MS = 30_000;
/** Repeated failure never waits longer than this between iterations. */
const MAX_BACKOFF_MS = 300_000;

// The first failure keeps the base cadence; each further consecutive failure doubles the wait up to the cap.
const backoffMs = (consecutiveFailures: number) =>
  Math.min(MAX_BACKOFF_MS, LOOP_INTERVAL_MS * 2 ** (consecutiveFailures - 1));

interface SingleFlightOptions<A> {
  /**
   * What a caller gets when a run is already in flight. `"join"` waits for that run and shares its
   * outcome. `{ skip }` returns the value at once without waiting or starting another run.
   */
  readonly whenBusy: "join" | { readonly skip: A };
  /**
   * Whether interrupting the caller that started a run also interrupts the run. When false the run
   * belongs to the owner scope alone, so a departing caller never cancels recovery work it started.
   */
  readonly starterCancels: boolean;
}

/**
 * At most one run at a time. The owner scope (the one current when this is made) executes each run,
 * so closing it interrupts the run and no caller's departure can orphan it. The flight is released
 * by the run's own completion, never by a caller, so it cannot stay held after an interruption.
 */
export const makeSingleFlight = Effect.fnUntraced(function* <A, E>(
  options: SingleFlightOptions<A>,
) {
  const owner = yield* Effect.scope;
  let inFlight: Deferred.Deferred<A, E> | undefined;
  const run = (work: Effect.Effect<A, E>): Effect.Effect<A, E> =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        if (inFlight !== undefined)
          return options.whenBusy === "join"
            ? yield* restore(Deferred.await(inFlight))
            : options.whenBusy.skip;
        const flight = yield* Deferred.make<A, E>();
        inFlight = flight;
        const fiber = yield* work.pipe(Effect.interruptible, Effect.forkIn(owner));
        // Completion must be observed even if scope disposal interrupts before the run starts.
        fiber.addObserver((exit) => {
          if (inFlight === flight) inFlight = undefined;
          Deferred.doneUnsafe(flight, exit);
        });
        const outcome = restore(Deferred.await(flight));
        return yield* options.starterCancels
          ? outcome.pipe(Effect.onInterrupt(() => Fiber.interrupt(fiber)))
          : outcome;
      }),
    );
  return { run };
});

interface SuperviseLoopOptions<A, E> {
  /** Stable name of the loop, used only to label its log records. */
  readonly loop: string;
  readonly iteration: Effect.Effect<A, E>;
  /** Content-free code for a typed failure, such as its error code. */
  readonly code: (error: E) => string;
  /** Observes the result of each successful iteration. */
  readonly onSuccess?: (result: A) => Effect.Effect<void>;
}

/**
 * Runs `iteration` now and once per cadence, waiting for each to finish, so missed ticks never queue.
 * A failure or defect is logged (cause on the record, bounded annotations) and backs off to a cap;
 * it never ends the loop. Interruption does. Untraced: a span around a loop-long fiber never ends.
 */
export const superviseLoop = Effect.fnUntraced(function* <A, E>(
  options: SuperviseLoopOptions<A, E>,
) {
  let consecutiveFailures = 0;
  for (;;) {
    const exit = yield* Effect.exit(options.iteration);
    if (Exit.isSuccess(exit)) {
      if (consecutiveFailures > 0)
        yield* Effect.logInfo("Native workstreams background sweep recovered").pipe(
          Effect.annotateLogs({ loop: options.loop, afterFailures: consecutiveFailures }),
        );
      consecutiveFailures = 0;
      if (options.onSuccess) yield* options.onSuccess(exit.value);
      yield* Effect.sleep(LOOP_INTERVAL_MS);
      continue;
    }
    if (Cause.hasInterrupts(exit.cause)) return yield* Effect.interrupt;
    consecutiveFailures++;
    const retryInMs = backoffMs(consecutiveFailures);
    const typed = Cause.findErrorOption(exit.cause);
    yield* Effect.logWarning("Native workstreams background sweep failed", exit.cause).pipe(
      Effect.annotateLogs({
        loop: options.loop,
        failure: Option.isSome(typed) ? "typed" : "defect",
        ...(Option.isSome(typed) ? { code: options.code(typed.value) } : {}),
        consecutiveFailures,
        retryInMs,
      }),
    );
    yield* Effect.sleep(retryInMs);
  }
});
