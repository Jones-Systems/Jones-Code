import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";

/** Shares only pending work. Waiter interruption never owns the service's opener. */
export const make = Effect.fn("InFlight.make")(function* <A, E>() {
  const scope = yield* Effect.scope;
  const pending = new Map<string, Deferred.Deferred<A, E>>();

  const run = <R>(key: string, open: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        if (scope.state._tag === "Closed") return yield* Effect.interrupt;
        let ready = pending.get(key);
        if (ready === undefined) {
          ready = yield* Deferred.make<A, E>();
          const own = ready;
          pending.set(key, own);
          const opener = yield* open.pipe(Effect.interruptible, Effect.forkIn(scope));
          // Observe even a fiber interrupted before its effect started during scope closure.
          opener.addObserver((exit) => {
            if (pending.get(key) === own) pending.delete(key);
            Deferred.doneUnsafe(own, exit);
          });
        }
        return yield* restore(Deferred.await(ready));
      }),
    );

  // A retired opener may still finish within its deadline. Its cleanup is identity guarded.
  const forget = (key: string) => pending.delete(key);
  return { run, forget };
});
