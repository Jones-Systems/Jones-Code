import * as Effect from "effect/Effect";

import { ConnectionTransientError } from "../../connection/model.ts";

// The driver owns the surrounding span and route scope; expiry is a route
// failure, leaving scope disposal and retry policy with their existing owners.
export const withRouteAttemptBudget = Effect.fnUntraced(function* <A, E, R, B, E2, R2>(
  hasFallback: boolean,
  establish: Effect.Effect<A, E, R>,
  synchronize: (established: A) => Effect.Effect<B, E2, R2>,
) {
  // The last attempt retains the supervisor's aggregate deadline, including
  // unused allowances from routes that failed early or were deferred as silent.
  if (!hasFallback) return yield* Effect.flatMap(establish, synchronize);

  const established = yield* establish.pipe(
    Effect.timeoutOrElse({
      duration: "15 seconds",
      orElse: () =>
        Effect.fail(
          new ConnectionTransientError({
            reason: "timeout",
            detail: "The route did not respond during connection setup.",
          }),
        ),
    }),
  );
  return yield* synchronize(established).pipe(
    Effect.timeoutOrElse({
      duration: "75 seconds",
      orElse: () =>
        Effect.fail(
          new ConnectionTransientError({
            reason: "timeout",
            detail: "The route did not finish connection synchronization.",
          }),
        ),
    }),
  );
});
