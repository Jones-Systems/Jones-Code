import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";

/** Shares only pending work. Uncertain openers remain service-owned; read-only probes may cancel when unused. */
export const make = Effect.fn("InFlight.make")(function* <A, E>(
  options: { readonly cancelWhenUnused?: boolean } = {},
) {
  const scope = yield* Effect.scope;
  interface Pending {
    readonly ready: Deferred.Deferred<A, E>;
    waiters: number;
    interrupt: Effect.Effect<void>;
  }
  const pending = new Map<string, Pending>();

  const run = <R>(key: string, open: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        if (scope.state._tag === "Closed") return yield* Effect.interrupt;
        let entry = pending.get(key);
        if (entry === undefined) {
          const own: Pending = {
            ready: yield* Deferred.make<A, E>(),
            waiters: 0,
            interrupt: Effect.void,
          };
          pending.set(key, own);
          entry = own;
          const opener = yield* open.pipe(Effect.interruptible, Effect.forkIn(scope));
          own.interrupt = Fiber.interrupt(opener).pipe(Effect.asVoid);
          // Observe even a fiber interrupted before its effect started during scope closure.
          opener.addObserver((exit) => {
            if (pending.get(key) === own) pending.delete(key);
            Deferred.doneUnsafe(own.ready, exit);
          });
        }
        const own = entry;
        own.waiters++;
        return yield* restore(Deferred.await(own.ready)).pipe(
          Effect.ensuring(
            Effect.suspend(() => {
              own.waiters--;
              if (!options.cancelWhenUnused || own.waiters !== 0 || pending.get(key) !== own)
                return Effect.void;
              // Retire before awaiting cancellation: a later independent read gets a fresh opener.
              pending.delete(key);
              return own.interrupt;
            }),
          ),
        );
      }),
    );

  // A retired opener may still finish within its deadline. Its cleanup is identity guarded.
  const forget = (key: string) => pending.delete(key);
  return { run, forget };
});
