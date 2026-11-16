import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";

interface Waiter {
  readonly exclusive: boolean;
  readonly ready: Deferred.Deferred<void>;
  granted: boolean;
}

interface ScopeLock {
  holders: number;
  exclusive: boolean;
  readonly waiters: Set<Waiter>;
}

export const makePullRequestScopeLocks = Effect.sync(() => {
  const scopes = new Map<string, ScopeLock>();

  const grantWaiting = (lock: ScopeLock) => {
    if (lock.exclusive) return;
    // Admit adjacent readers together, but never let them bypass a queued writer.
    for (const waiter of lock.waiters) {
      if (waiter.exclusive && lock.holders > 0) break;
      lock.waiters.delete(waiter);
      lock.holders++;
      lock.exclusive = waiter.exclusive;
      waiter.granted = true;
      Deferred.doneUnsafe(waiter.ready, Effect.void);
      if (waiter.exclusive) break;
    }
  };

  const withScope =
    (scope: string, exclusive: boolean) =>
    <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
      Effect.acquireUseRelease(
        Effect.sync(() => {
          let lock = scopes.get(scope);
          if (lock === undefined) {
            lock = { holders: 0, exclusive: false, waiters: new Set() };
            scopes.set(scope, lock);
          }
          const waiter: Waiter = {
            exclusive,
            ready: Deferred.makeUnsafe<void>(),
            granted: false,
          };
          lock.waiters.add(waiter);
          grantWaiting(lock);
          return { lock, waiter };
        }),
        // Register cleanup before the interruptible wait, including a grant that
        // races with cancellation before this fiber resumes.
        ({ waiter }) => Deferred.await(waiter.ready).pipe(Effect.andThen(effect)),
        ({ lock, waiter }) =>
          Effect.sync(() => {
            if (waiter.granted) {
              lock.holders--;
              if (waiter.exclusive) lock.exclusive = false;
            } else {
              lock.waiters.delete(waiter);
            }
            grantWaiting(lock);
            if (lock.holders === 0 && lock.waiters.size === 0 && scopes.get(scope) === lock) {
              scopes.delete(scope);
            }
          }),
      );

  return {
    withReadScopes:
      (readScopes: ReadonlyArray<string>) =>
      <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
        [...new Set(readScopes)]
          .toSorted()
          .reduceRight((read, scope) => withScope(scope, false)(read), effect),
    withWriteScope: (scope: string) => withScope(scope, true),
    get size() {
      return scopes.size;
    },
  };
});
