import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Semaphore from "effect/Semaphore";

export interface KeyedLock<Key> {
  /** Runs `effect` once no other holder of `key` is running. Queued waiters go first-in, first-out. */
  readonly withLock: <A, E, R>(key: Key, effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
  /** Runs only if the key has no holder or waiter; otherwise returns None without running `effect`. */
  readonly tryWithLock: <A, E, R>(
    key: Key,
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<Option.Option<A>, E, R>;
  /** The keys someone holds or waits on right now. */
  readonly activeKeys: Effect.Effect<ReadonlyArray<Key>>;
}

interface LockEntry {
  readonly semaphore: Semaphore.Semaphore;
  readonly users: number;
}

/**
 * Mutual exclusion per key. A key's lock exists only while someone holds or
 * waits on it, so locks for keys that come and go never accumulate. `withLock`
 * is not reentrant: waiting for a key while already holding it deadlocks.
 *
 * Keys compare like `Map` keys (by value for strings and numbers). The lock is
 * not tied to a scope, so it keeps working for as long as anyone references it.
 */
export const make = <Key>(): Effect.Effect<KeyedLock<Key>> =>
  Effect.gen(function* () {
    const locks = yield* Ref.make<ReadonlyMap<Key, LockEntry>>(new Map());

    const acquire = (key: Key) =>
      Effect.flatMap(Semaphore.make(1), (candidate) =>
        Ref.modify(locks, (current) => {
          const existing = current.get(key);
          const semaphore = existing?.semaphore ?? candidate;
          const next = new Map(current);
          next.set(key, { semaphore, users: (existing?.users ?? 0) + 1 });
          return [semaphore, next] as const;
        }),
      );

    const release = (key: Key) =>
      Ref.update(locks, (current) => {
        const existing = current.get(key);
        if (existing === undefined) return current;
        const next = new Map(current);
        if (existing.users === 1) {
          next.delete(key);
        } else {
          next.set(key, { ...existing, users: existing.users - 1 });
        }
        return next;
      });

    return {
      withLock: (key, effect) =>
        Effect.acquireUseRelease(
          acquire(key),
          (semaphore) => semaphore.withPermit(effect),
          () => release(key),
        ),
      tryWithLock: <A, E, R>(
        key: Key,
        effect: Effect.Effect<A, E, R>,
      ): Effect.Effect<Option.Option<A>, E, R> =>
        Effect.acquireUseRelease(
          Effect.flatMap(Semaphore.make(1), (semaphore) =>
            Ref.modify(locks, (current) => {
              // Reserving the key also respects waiters whose permit has not been handed over yet.
              if (current.has(key)) return [Option.none<Semaphore.Semaphore>(), current] as const;
              const next = new Map(current);
              next.set(key, { semaphore, users: 1 });
              return [Option.some(semaphore), next] as const;
            }),
          ),
          (reserved): Effect.Effect<Option.Option<A>, E, R> =>
            Option.isSome(reserved)
              ? reserved.value.withPermitsIfAvailable(1)(effect)
              : Effect.succeed(Option.none<A>()),
          (reserved) => (Option.isSome(reserved) ? release(key) : Effect.void),
        ),
      activeKeys: Effect.map(Ref.get(locks), (current) => Array.from(current.keys())),
    } satisfies KeyedLock<Key>;
  });
