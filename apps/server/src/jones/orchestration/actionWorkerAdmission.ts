import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";

export const makeActionWorkerAdmission = Effect.fnUntraced(function* (
  awaitWork: Effect.Effect<void>,
) {
  const mixedAvailable = yield* Queue.dropping<void>(64);
  const actionAvailable = yield* Queue.sliding<void>(1);

  // The daemon is the only production consumer of outbox wakes. Relay each
  // wake to both lanes so the reserved worker cannot steal a title's wake.
  yield* Effect.gen(function* () {
    while (true) {
      yield* awaitWork;
      yield* Queue.offer(mixedAvailable, undefined);
      yield* Queue.offer(actionAvailable, undefined);
    }
  }).pipe(Effect.forkScoped);

  return {
    awaitMixedWork: Queue.take(mixedAvailable),
    awaitActionWork: Queue.take(actionAvailable),
  };
});
