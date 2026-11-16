import { expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as TestClock from "effect/testing/TestClock";
import * as PeerLinks from "../../peer/PeerLinks.ts";
import * as Coordinator from "./OriginCoordinator.ts";
import * as Signals from "./OriginSignals.ts";
import { start } from "./registration.ts";

const unused = () => Effect.die("Unexpected peer mutation or token resolution");
const links = Layer.succeed(PeerLinks.PeerLinks, {
  enabled: Effect.succeed(true),
  list: Effect.succeed([]),
  currentGeneration: unused,
  link: unused,
  describe: unused,
  get: unused,
  unlink: unused,
  resolve: unused,
  recordOutcome: unused,
});

it.effect(
  "server-owned worker starts once, survives a request Scope and reconciles durable intent plus ten-minute cadence",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const count = yield* Ref.make(0);
        const first = yield* Deferred.make<void>();
        const second = yield* Deferred.make<void>();
        const third = yield* Deferred.make<void>();
        const coordinator = Coordinator.OriginCoordinator.of({
          reconcile: () =>
            Effect.gen(function* () {
              const value = yield* Ref.updateAndGet(count, (n) => n + 1);
              yield* Deferred.succeed(
                value === 1 ? first : value === 2 ? second : third,
                undefined,
              );
            }),
        });
        yield* start.pipe(Effect.provideService(Coordinator.OriginCoordinator, coordinator));
        yield* Deferred.await(first);
        // A request can end without cancelling the server-owned follower.
        yield* Effect.scoped(Effect.void);
        const signals = yield* Signals.OriginSignals;
        yield* signals.changed;
        yield* Deferred.await(second);
        expect(yield* Ref.get(count)).toBe(2);
        yield* TestClock.adjust(Coordinator.FALLBACK_MS);
        yield* Deferred.await(third);
        expect(yield* Ref.get(count)).toBe(3);
      }),
    ).pipe(Effect.provide(Signals.layer), Effect.provide(links)),
);
