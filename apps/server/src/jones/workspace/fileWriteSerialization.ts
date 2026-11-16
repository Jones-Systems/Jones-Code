import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";

class FileWriteBusyError extends Schema.TaggedError<FileWriteBusyError>()(
  "FileWriteBusyError",
  {},
) {}

/** Scoped same-canonical-file admission. Waiting/cancelled users retain their lock until release. */
export const makeFileWriteSerialization = () => {
  const entries = new Map<string, { readonly lock: Semaphore.Semaphore; users: number }>();
  return <A, E, R>(path: string, effect: Effect.Effect<A, E, R>) =>
    Effect.acquireUseRelease(
      Effect.suspend(() => {
        let entry = entries.get(path);
        if ((entry?.users ?? 0) >= 8 || (entry === undefined && entries.size >= 256))
          return Effect.fail(new FileWriteBusyError({}));
        if (entry === undefined) {
          entry = { lock: Semaphore.makeUnsafe(1), users: 0 };
          entries.set(path, entry);
        }
        entry.users++;
        return Effect.succeed(entry);
      }),
      (entry) => entry.lock.withPermits(1)(effect),
      (entry) =>
        Effect.sync(() => {
          if (--entry.users === 0 && entries.get(path) === entry) entries.delete(path);
        }),
    );
};
