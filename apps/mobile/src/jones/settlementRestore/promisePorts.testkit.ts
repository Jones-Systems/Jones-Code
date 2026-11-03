import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";

// Promise-based mobile ports execute in the enclosing Effect test's scoped services.
export const makePromisePorts = <R = never>() =>
  Effect.gen(function* () {
    const queue = yield* Queue.unbounded<Effect.Effect<void, never, R>>();
    yield* Effect.forever(Queue.take(queue).pipe(Effect.flatMap((job) => job))).pipe(
      Effect.forkScoped,
    );
    return <A, E>(effect: Effect.Effect<A, E, R>): Promise<A> =>
      new Promise<A>((resolve, reject) => {
        Queue.offerUnsafe(
          queue,
          effect.pipe(
            Effect.matchCauseEffect({
              onFailure: (cause) => Effect.sync(() => reject(Cause.squash(cause))),
              onSuccess: (value) => Effect.sync(() => resolve(value)),
            }),
          ),
        );
      });
  });
