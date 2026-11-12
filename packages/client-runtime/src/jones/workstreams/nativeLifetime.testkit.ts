import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";

/** Runs Promise-facing client ports on children owned by the enclosing Effect test. */
export const makeNativeWorkstreamsTestRunner = Effect.gen(function* () {
  const jobs = yield* Queue.unbounded<Effect.Effect<void>>();
  yield* Queue.take(jobs).pipe(
    Effect.flatMap((job) => Effect.forkChild(job)),
    Effect.forever,
    Effect.forkChild,
  );
  return <A, E>(effect: Effect.Effect<A, E>, signal: AbortSignal) =>
    new Promise<A>((resolve, reject) => {
      const cancelled = Effect.callback<never>((resume) => {
        const abort = () => resume(Effect.interrupt);
        if (signal.aborted) abort();
        else signal.addEventListener("abort", abort, { once: true });
        return Effect.sync(() => signal.removeEventListener("abort", abort));
      });
      const job = Effect.raceFirst(effect, cancelled).pipe(
        Effect.matchCauseEffect({
          onSuccess: (value) => Effect.sync(() => resolve(value)),
          onFailure: (cause) => Effect.sync(() => reject(Cause.squash(cause))),
        }),
      );
      if (!Queue.offerUnsafe(jobs, job)) reject(new Error("The test lifetime closed."));
    });
});
