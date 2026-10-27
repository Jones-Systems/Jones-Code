import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";

const CWD_CAPACITY = 2048;
const KEYS_PER_CWD = 64;
const TTL_MS = 5 * 60 * 1000;

interface Entry {
  value?: {
    readonly value: string | null;
    readonly readAtMs: number;
    readonly epoch: number;
    readonly sequence: number;
  };
  pending?: { readonly epoch: number; readonly done: Deferred.Deferred<void> };
}

export const makePrLookupGitStateCache = (epoch: (cwd: string) => number) => {
  const directories = new Map<string, Map<string, Entry>>();
  let sequence = 0;

  const entryFor = (cwd: string, key: string) => {
    const entries = directories.get(cwd) ?? new Map<string, Entry>();
    directories.delete(cwd);
    directories.set(cwd, entries);
    if (directories.size > CWD_CAPACITY) directories.delete(directories.keys().next().value!);
    const entry = entries.get(key) ?? {};
    entries.delete(key);
    entries.set(key, entry);
    if (entries.size > KEYS_PER_CWD) entries.delete(entries.keys().next().value!);
    return { entries, entry };
  };

  const remember = <E, R>(
    cwd: string,
    key: string,
    fresh: boolean,
    read: Effect.Effect<string | null, E, R>,
  ): Effect.Effect<string | null, E, R> =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const readAtMs = yield* Clock.currentTimeMillis;
        const { entries, entry } = entryFor(cwd, key);
        if (
          !fresh &&
          entry.value !== undefined &&
          entry.value.epoch === epoch(cwd) &&
          readAtMs - entry.value.readAtMs < TTL_MS
        ) {
          return entry.value.value;
        }
        const startedEpoch = epoch(cwd);
        if (!fresh && entry.pending?.epoch === startedEpoch) {
          yield* restore(Deferred.await(entry.pending.done));
          return yield* restore(remember(cwd, key, false, read));
        }
        const readSequence = ++sequence;
        const pending = { epoch: startedEpoch, done: yield* Deferred.make<void>() };
        if (!fresh) entry.pending = pending;
        return yield* restore(read).pipe(
          Effect.tap((value) =>
            Effect.sync(() => {
              // Invalidated or evicted reads cannot resurrect state. Sequence numbers
              // also order reads that start in the same clock millisecond.
              if (
                epoch(cwd) === startedEpoch &&
                directories.get(cwd) === entries &&
                entries.get(key) === entry &&
                (entry.value === undefined || entry.value.sequence < readSequence)
              ) {
                entry.value = { value, readAtMs, epoch: startedEpoch, sequence: readSequence };
              }
            }),
          ),
          Effect.ensuring(
            Effect.gen(function* () {
              if (entry.pending === pending) delete entry.pending;
              // Notify, rather than share the owner's exit: cancellation belongs
              // to its caller, and waiters retry after interruption or failure.
              yield* Deferred.succeed(pending.done, undefined);
            }),
          ),
        );
      }),
    );

  return {
    remember,
    forget: (cwd: string) => {
      directories.delete(cwd);
    },
  };
};
