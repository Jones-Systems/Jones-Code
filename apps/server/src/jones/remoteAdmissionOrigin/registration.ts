import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as PeerLinks from "../../peer/PeerLinks.ts";
import * as Bindings from "./OriginPeerBindings.ts";
import * as Delivery from "./OriginDelivery.ts";
import * as Coordinator from "./OriginCoordinator.ts";
import * as Signals from "./OriginSignals.ts";

/** Reuse this exact layer reference for Forwarding and the coordinator. */
export const servicesLayer = Delivery.layer.pipe(
  Layer.provideMerge(Bindings.layer),
  Layer.provideMerge(Signals.layer),
);

// PeerLinks has no change stream. This watcher samples only local public metadata;
// it does not resolve tokens, probe routes, retry transport or open another client.
const encodeLinkSnapshot = Schema.encodeSync(
  Schema.fromJsonString(
    Schema.Array(
      Schema.Struct({
        environmentId: Schema.String,
        generation: Schema.NullOr(Schema.String),
        status: Schema.String,
      }),
    ),
  ),
);
const linkSnapshot = Effect.gen(function* () {
  const links = yield* PeerLinks.PeerLinks;
  const listed = yield* links.list;
  const generations = yield* Effect.forEach(listed, (link) =>
    links
      .currentGeneration(link.environmentId)
      .pipe(
        Effect.map((generation) => ({
          environmentId: link.environmentId,
          generation: Option.getOrNull(generation),
          status: link.status,
        })),
      ),
  );
  return encodeLinkSnapshot(
    generations.toSorted((a, b) => a.environmentId.localeCompare(b.environmentId)),
  );
});

export const start = Effect.gen(function* () {
  const coordinator = yield* Coordinator.OriginCoordinator;
  const signals = yield* Signals.OriginSignals;
  let previous: string | undefined;
  let lastReconcile = -Infinity;
  // This effect and every coordinator child inherit the server layer's Scope,
  // independently of the request that prepared an observation.
  yield* Effect.gen(function* () {
    while (true) {
      const snapshot = yield* linkSnapshot.pipe(Effect.orElseSucceed(() => undefined));
      const clock = DateTime.toEpochMillis(yield* DateTime.now);
      if (snapshot !== previous || clock - lastReconcile >= Coordinator.FALLBACK_MS) {
        previous = snapshot;
        lastReconcile = clock;
        yield* coordinator
          .reconcile()
          .pipe(
            Effect.catch(() =>
              Effect.logWarning(
                "Origin launch reconciliation is unavailable; retained intent will be revisited.",
              ),
            ),
          );
      }
      const intentChanged = yield* Effect.race(
        signals.take.pipe(Effect.as(true)),
        Effect.sleep(1_000).pipe(Effect.as(false)),
      );
      if (intentChanged) lastReconcile = -Infinity;
    }
  }).pipe(Effect.forkScoped);
});
export const workerLayer = Layer.effectDiscard(start).pipe(Layer.provide(Coordinator.layer));
