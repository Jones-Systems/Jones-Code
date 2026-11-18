import * as Effect from "effect/Effect";

import { ConnectionTransientError } from "../../connection/model.ts";

// The driver owns the surrounding span and route scope; expiry is a route
// failure, leaving scope disposal and retry policy with their existing owners.
export const withRouteAttemptBudget = Effect.fnUntraced(function* <A, E, R, B, E2, R2>(
  hasFallback: boolean,
  establish: Effect.Effect<A, E, R>,
  synchronize: (established: A) => Effect.Effect<B, E2, R2>,
  hasFallbackOnTimeout: Effect.Effect<boolean> = Effect.succeed(hasFallback),
) {
  // The last attempt retains the supervisor's aggregate deadline, including
  // unused allowances from routes that failed early or were deferred as silent.
  if (!hasFallback) return yield* Effect.flatMap(establish, synchronize);

  // Do not interrupt an in-flight operation just to discover that its timeout
  // would exclude every fallback. Keep it running under the aggregate deadline.
  const timeout = (duration: "15 seconds" | "75 seconds", detail: string) =>
    Effect.sleep(duration).pipe(
      Effect.andThen(hasFallbackOnTimeout),
      Effect.flatMap((eligible) =>
        eligible
          ? Effect.fail(
              new ConnectionTransientError({
                reason: "timeout",
                detail,
              }),
            )
          : Effect.never,
      ),
    );

  const established = yield* Effect.raceFirst(
    establish,
    timeout("15 seconds", "The route did not respond during connection setup."),
  );
  return yield* Effect.raceFirst(
    synchronize(established),
    timeout("75 seconds", "The route did not finish connection synchronization."),
  );
});
