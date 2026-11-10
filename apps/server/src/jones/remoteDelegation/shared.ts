import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";
import * as Semaphore from "effect/Semaphore";

/**
 * Helpers shared by the remote delegation, peer monitor and remote admission
 * modules, which each used to carry their own copy.
 */

/**
 * How long a finished remote delegation row outlives its end before startup
 * recovery may delete it. The rows only serve replays of an already finished
 * request, so a month is far longer than any retry or reconciliation window
 * while still bounding what a long-lived server accumulates.
 */
export const TERMINAL_RETENTION = Duration.days(30);

/** The newest time, in epoch milliseconds, a finished row may carry and still be pruned. */
export const terminalRetentionCutoffMillis = Effect.map(
  Clock.currentTimeMillis,
  (now) => now - Duration.toMillis(TERMINAL_RETENTION),
);

/** The same cutoff as an ISO UTC string, for comparison with stored timestamps. */
export const terminalRetentionCutoff = Effect.map(terminalRetentionCutoffMillis, (millis) =>
  DateTime.formatIso(DateTime.makeUnsafe(millis)),
);

/** Rows removed by one transaction while pruning, so a large backlog never holds the writer. */
export const PRUNE_BATCH_SIZE = 200;

/** Statuses after which a run or task never moves again. */
export const TERMINAL_STATUSES = [
  "completed",
  "failed",
  "cancelled",
  "interrupted",
  "rolled_back",
] as const;

export const isTerminalStatus = (status: string): boolean =>
  (TERMINAL_STATUSES as ReadonlyArray<string>).includes(status);

const BACKOFF_BASE = Duration.seconds(2);
const BACKOFF_CAP = Duration.minutes(5);

/** Retry delays from two seconds, doubling to a five minute cap, each jittered by 20%. */
export const backoffSchedule = Schedule.exponential(BACKOFF_BASE).pipe(
  Schedule.modifyDelay(({ duration }) => Effect.succeed(Duration.min(duration, BACKOFF_CAP))),
  Schedule.jittered,
);

/** The same delays for a caller that tracks failures itself. `failures` counts from one. */
export const backoffDelayMillis = (failures: number, jitter: number): number =>
  Math.min(
    Duration.toMillis(BACKOFF_CAP),
    Duration.toMillis(BACKOFF_BASE) * 2 ** (failures - 1) * jitter,
  );

/**
 * Canonical JSON: object keys sorted by UTF-16 code unit and undefined members
 * dropped. Persisted request fingerprints and digests depend on these exact bytes.
 */
export const canonicalJson = (value: unknown): string => {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value !== null && typeof value === "object")
    return `{${Object.entries(value)
      .filter(([, child]) => child !== undefined)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([key, child]) => `${JSON.stringify(key)}:${canonicalJson(child)}`)
      .join(",")}}`;
  return JSON.stringify(value);
};

const assertJson = (value: unknown): void => {
  if (value === null || typeof value === "string" || typeof value === "boolean") return;
  if (typeof value === "number" && Number.isFinite(value)) return;
  if (Array.isArray(value)) return value.forEach(assertJson);
  if (typeof value === "object") {
    for (const key of Object.keys(value)) assertJson((value as Record<string, unknown>)[key]);
    return;
  }
  throw new Error("Input must be JSON");
};

/** Canonical JSON for hashed bindings: rejects anything JSON cannot represent instead of dropping it. */
export const canonicalJsonStrict = (value: unknown): string => {
  assertJson(value);
  return canonicalJson(value);
};

/**
 * One mutual-exclusion lock per key. A key's lock exists only while someone
 * holds or awaits it, so a long-lived server does not accumulate one per key
 * it has ever seen.
 */
export const makeKeyedLock = <Key>() => {
  const entries = new Map<Key, { readonly lock: Semaphore.Semaphore; users: number }>();
  const locked = <A, E, R>(key: Key, effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    Effect.acquireUseRelease(
      Effect.sync(() => {
        let entry = entries.get(key);
        if (entry === undefined) {
          entry = { lock: Semaphore.makeUnsafe(1), users: 0 };
          entries.set(key, entry);
        }
        entry.users += 1;
        return entry;
      }),
      (entry) => entry.lock.withPermits(1)(effect),
      (entry) =>
        Effect.sync(() => {
          entry.users -= 1;
          if (entry.users === 0 && entries.get(key) === entry) entries.delete(key);
        }),
    );
  return { locked, size: () => entries.size };
};
