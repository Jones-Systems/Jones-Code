import type { ProviderInstanceId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Semaphore from "effect/Semaphore";

/** Claims are made with the metadata mutation; only disk work waits on the instance lane. */
export const makeCatalogCachePublication = () => {
  const lanes = new Map<
    ProviderInstanceId,
    { readonly semaphore: Semaphore.Semaphore; claim: object }
  >();

  return (instanceId: ProviderInstanceId, sourceIsCurrent: () => boolean) => {
    let lane = lanes.get(instanceId);
    if (!lane) {
      lane = { semaphore: Semaphore.makeUnsafe(1), claim: {} };
      lanes.set(instanceId, lane);
    }
    const currentLane = lane;
    const claim = {};
    currentLane.claim = claim;
    const isCurrent = () => currentLane.claim === claim && sourceIsCurrent();

    return {
      isCurrent,
      run: <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        currentLane.semaphore.withPermits(1)(
          Effect.suspend(() =>
            // Cancellation cannot release the lane while an issued filesystem operation can still land.
            isCurrent() ? effect.pipe(Effect.asVoid, Effect.uninterruptible) : Effect.void,
          ),
        ),
    };
  };
};

export const withCurrentCatalogCacheWrite = Effect.fn("CatalogCache.withCurrentWrite")(
  function* <A, E, R>(write: Effect.Effect<A, E, R>, isCurrent: () => boolean) {
    const fs = yield* FileSystem.FileSystem;
    return yield* write.pipe(
      Effect.provideService(FileSystem.FileSystem, {
        ...fs,
        rename: (fromPath, toPath) =>
          Effect.suspend(() => {
            if (!isCurrent()) return Effect.void;
            // The caller holds the instance's cache lane through rename and stale-write removal.
            return fs.rename(fromPath, toPath).pipe(
              Effect.andThen(
                Effect.suspend(() =>
                  isCurrent() ? Effect.void : fs.remove(toPath, { force: true }),
                ),
              ),
            );
          }),
      }),
    );
  },
);
