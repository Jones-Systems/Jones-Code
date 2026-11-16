import type { NodeId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";

import { FollowRetrySchedule } from "./retrySchedule.ts";

/** Keep recovery in the owning fiber so its single-flight entry survives every retry. */
export const retry = Effect.fn("RemoteFollowerSupervision.retry")(function* <A, E, R>(
  operation: Effect.Effect<A, E, R>,
  context: {
    readonly operation: "follow" | "startup" | "questions" | "subscribe" | "scan";
    readonly taskId?: NodeId;
  },
): Effect.fn.Return<A, never, R> {
  const backoff = yield* FollowRetrySchedule;
  return yield* operation.pipe(
    Effect.catchCause((cause) => {
      // Shutdown wins even when an interrupted operation's finalizer also defects.
      if (Cause.hasInterrupts(cause)) return Effect.interrupt;
      return (
        Cause.hasDies(cause)
          ? Effect.logWarning("remote-delegation.follower-defect", context)
          : Effect.void
      ).pipe(Effect.andThen(Effect.fail(cause)));
    }),
    Effect.retry(backoff),
    // The schedule never exhausts; interruption stays in the cause channel.
    Effect.orDie,
  );
});

/** Rescan after subscribing so durable Stop debt recorded during backoff is not missed. */
export const subscribe = Effect.fn("RemoteFollowerSupervision.subscribe")(function* <E, R, R2>(
  subscription: Effect.Effect<void, E, R>,
  reconcile: Effect.Effect<void, never, R2>,
): Effect.fn.Return<void, never, R | R2> {
  yield* retry(
    Effect.scoped(
      Effect.gen(function* () {
        const fiber = yield* subscription.pipe(Effect.forkScoped({ startImmediately: true }));
        yield* reconcile;
        yield* Fiber.join(fiber);
      }),
    ),
    { operation: "subscribe" },
  );
});
