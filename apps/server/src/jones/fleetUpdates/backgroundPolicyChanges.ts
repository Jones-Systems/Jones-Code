import type { BackgroundPolicySnapshot } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as PubSub from "effect/PubSub";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { subscribeBeforeSnapshot } from "../../utils/subscribeBeforeSnapshot.ts";

// Publication and subscription share the mutation lock so a quiet follower cannot miss recent input.
export const makeBackgroundPolicyChanges = Effect.fn("JonesBackgroundPolicyChanges.make")(
  function* (snapshot: Effect.Effect<BackgroundPolicySnapshot>) {
    const changes = yield* PubSub.sliding<BackgroundPolicySnapshot>(1);
    const mutex = yield* Semaphore.make(1);
    const publishUnlocked = snapshot.pipe(
      Effect.flatMap((next) => PubSub.publish(changes, next)),
      Effect.asVoid,
    );
    return {
      mutate: <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        mutex.withPermits(1)(effect.pipe(Effect.tap(() => publishUnlocked))),
      publish: mutex.withPermits(1)(publishUnlocked),
      subscribe: subscribeBeforeSnapshot(changes, snapshot, mutex),
      streamChanges: Stream.fromPubSub(changes),
    };
  },
);
