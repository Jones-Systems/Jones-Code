import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Semaphore from "effect/Semaphore";

const REPORT_DEADLINE_MS = 10_000;

/** Reporter-scoped lanes for replaceable observations, never transport retries. */
export const makeReportLanes = Effect.fn("ActivityReportLanes.make")(function* <Key, Report, E>(
  send: (key: Key, report: Report) => Effect.Effect<unknown, E>,
) {
  const scope = yield* Effect.scope;
  const bookkeeping = yield* Semaphore.make(1);
  const lanes = new Map<
    Key,
    { readonly pending: Queue.Queue<Report>; readonly fiber: Fiber.Fiber<never> }
  >();
  let retained = new Set<Key>();
  let closed = false;
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      closed = true;
      retained.clear();
      lanes.clear();
    }),
  );

  const retain = (keys: Iterable<Key>) =>
    bookkeeping
      .withPermits(1)(
        Effect.sync(() => {
          retained = new Set(keys);
          const removed = [];
          for (const [key, lane] of lanes) {
            if (retained.has(key)) continue;
            lanes.delete(key);
            Queue.shutdownUnsafe(lane.pending);
            removed.push(lane.fiber);
          }
          return removed;
        }),
      )
      .pipe(Effect.flatMap(Fiber.interruptAll));

  const submit = Effect.fn("ActivityReportLanes.submit")(
    function* (key: Key, report: Report) {
      if (closed || !retained.has(key)) return;
      let lane = lanes.get(key);
      if (lane === undefined) {
        const pending = yield* Queue.sliding<Report>(1);
        const fiber = yield* Queue.take(pending).pipe(
          Effect.flatMap((next) =>
            Effect.suspend(() => send(key, next)).pipe(
              Effect.timeoutOrElse({
                duration: REPORT_DEADLINE_MS,
                orElse: () => Effect.void,
              }),
              // A report can itself be interrupted by registry removal. Drop it
              // without ending future reporting or starting a reconnect/retry.
              Effect.ignoreCause,
            ),
          ),
          Effect.forever,
          Effect.ensuring(Queue.shutdown(pending)),
          Effect.forkIn(scope, { uninterruptible: false }),
        );
        lane = { pending, fiber };
        lanes.set(key, lane);
      }
      // Reports (including observedAt) are already built; queued data is never re-stamped.
      yield* Queue.offer(lane.pending, report);
    },
    Effect.uninterruptible,
    bookkeeping.withPermits(1),
  );

  return { retain, submit };
});
