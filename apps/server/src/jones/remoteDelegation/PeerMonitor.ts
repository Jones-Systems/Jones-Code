import type { EnvironmentId, OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Random from "effect/Random";
import * as Semaphore from "effect/Semaphore";
import { backoffDelayMillis } from "./shared.ts";

/** Requests to one peer that may hold a permit at the same time. */
const PEER_PERMITS = 4;
/**
 * How long a request holds its permit. The permits bound bursts of ordinary calls. A request
 * that outlives this is a long poll, which a peer parks without working on, so it lets go and
 * the next caller proceeds. Holding a permit across a poll of roughly 50 seconds would stall
 * every task behind the first few by that long.
 */
export const PERMIT_LEASE = Duration.seconds(2);

interface PeerState {
  readonly gate: Semaphore.Semaphore;
  retryAt: number;
  failures: number;
}

/** Runs `operation` holding one of the peer's permits for at most the lease. */
const leased = <A, E>(gate: Semaphore.Semaphore, operation: Effect.Effect<A, E>) =>
  Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      // Waiting for a permit stays interruptible; once one is taken, its release is guaranteed.
      yield* restore(gate.take(1));
      let held = true;
      const release = Effect.suspend(() => {
        if (!held) return Effect.void;
        held = false;
        return gate.release(1).pipe(Effect.asVoid);
      });
      const timer = yield* restore(Effect.sleep(PERMIT_LEASE).pipe(Effect.andThen(release))).pipe(
        Effect.forkChild,
      );
      return yield* restore(operation).pipe(
        Effect.ensuring(Fiber.interrupt(timer).pipe(Effect.andThen(release))),
      );
    }),
  );

/**
 * Shared peer capacity and reachability delay; foreground cancellation does not acquire these
 * permits. A call waits out the peer's reachability delay without holding a permit.
 */
export const make = Effect.sync(() => {
  const peers = new Map<EnvironmentId, PeerState>();
  return <A>(environmentId: EnvironmentId, operation: Effect.Effect<A, OrchestratorMcpFailure>) =>
    Effect.suspend(() => {
      let state = peers.get(environmentId);
      if (state === undefined) {
        state = { gate: Semaphore.makeUnsafe(PEER_PERMITS), retryAt: 0, failures: 0 };
        peers.set(environmentId, state);
      }
      const peer = state;
      const delay = Effect.map(Clock.currentTimeMillis, (now) => peer.retryAt - now);
      const attempt = Effect.gen(function* () {
        // Another call may have failed while this one queued for its permit.
        if ((yield* delay) > 0) return { delayed: true } as const;
        const value = yield* operation.pipe(
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
                (yield* Clock.currentTimeMillis) + backoffDelayMillis(peer.failures, jitter);
            }),
          ),
        );
        return { delayed: false, value } as const;
      });
      return Effect.gen(function* () {
        while (true) {
          const wait = yield* delay;
          if (wait > 0) {
            yield* Effect.sleep(wait);
            continue;
          }
          const outcome = yield* leased(peer.gate, attempt);
          if (!outcome.delayed) return outcome.value;
        }
      });
    });
});
