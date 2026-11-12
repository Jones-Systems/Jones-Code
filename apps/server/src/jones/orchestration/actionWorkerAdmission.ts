import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";

// Steer retains its existing ordering and does not use the urgent reservation.
export const URGENT_ACTION_EFFECT_TYPES = [
  "provider-turn.interrupt",
  "delegated-tasks.stop",
  "runtime-request.respond",
] as const;

export const actionWorkerLane = (index: number, concurrency: number) =>
  concurrency >= 3 && index === 0
    ? "urgent"
    : concurrency >= 2 && index === (concurrency >= 3 ? 1 : 0)
      ? "action"
      : "mixed";

export const makeActionWorkerAdmission = Effect.fnUntraced(function* (
  awaitWork: Effect.Effect<void>,
) {
  const mixedAvailable = yield* Queue.dropping<void>(64);
  const actionAvailable = yield* Queue.sliding<void>(1);
  const urgentAvailable = yield* Queue.sliding<void>(1);

  // The daemon is the only production consumer of outbox wakes. Relay each
  // wake independently so a reserved worker cannot steal another lane's wake.
  yield* Effect.gen(function* () {
    while (true) {
      yield* awaitWork;
      yield* Queue.offer(mixedAvailable, undefined);
      yield* Queue.offer(actionAvailable, undefined);
      yield* Queue.offer(urgentAvailable, undefined);
    }
  }).pipe(Effect.forkScoped);

  return {
    awaitMixedWork: Queue.take(mixedAvailable),
    awaitActionWork: Queue.take(actionAvailable),
    awaitUrgentWork: Queue.take(urgentAvailable),
  };
});
