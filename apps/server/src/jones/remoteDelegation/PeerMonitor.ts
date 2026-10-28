import type { EnvironmentId, OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Random from "effect/Random";
import * as Semaphore from "effect/Semaphore";

/** Shared peer capacity and reachability delay; foreground cancellation does not acquire these permits. */
export const make = Effect.sync(() => {
  const peers = new Map<
    EnvironmentId,
    { gate: Semaphore.Semaphore; retryAt: number; failures: number }
  >();
  return <A>(environmentId: EnvironmentId, operation: Effect.Effect<A, OrchestratorMcpFailure>) =>
    Effect.suspend(() => {
      let state = peers.get(environmentId);
      if (state === undefined) {
        state = { gate: Semaphore.makeUnsafe(4), retryAt: 0, failures: 0 };
        peers.set(environmentId, state);
      }
      const peer = state;
      return peer.gate.withPermits(1)(
        Effect.gen(function* () {
          const now = yield* Clock.currentTimeMillis;
          if (peer.retryAt > now) yield* Effect.sleep(peer.retryAt - now);
          return yield* operation.pipe(
            Effect.tap(() =>
              Effect.sync(() => {
                peer.failures = 0;
                peer.retryAt = 0;
              }),
            ),
            Effect.tapError((error) =>
              Effect.gen(function* () {
                if (error.code !== "orchestration_error") return;
                peer.failures = Math.min(peer.failures + 1, 9);
                const jitter = yield* Random.nextBetween(0.8, 1.2);
                peer.retryAt =
                  (yield* Clock.currentTimeMillis) +
                  Math.min(300_000, 1_000 * 2 ** peer.failures * jitter);
              }),
            ),
          );
        }),
      );
    });
});
