import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import type * as Scope from "effect/Scope";
import * as SqlClient from "effect/sql/SqlClient";

export class ReviewChanges extends Context.Service<
  ReviewChanges,
  {
    readonly invalidate: Effect.Effect<void>;
    readonly subscribe: Effect.Effect<{ readonly wait: Effect.Effect<void> }, never, Scope.Scope>;
    readonly waitFor: <A, E, R>(input: {
      readonly read: Effect.Effect<A, E, R>;
      readonly isReady: (snapshot: A) => boolean;
      readonly timeoutMs: number;
    }) => Effect.Effect<A, E, R>;
  }
>()("t3/jones/remoteAdmission/ReviewChanges") {}

// Construct once outside a SQL transaction. The worker inherits that clean SQL
// context, not the context of a caller invalidating inside a nested transaction.
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const pending = yield* Effect.acquireRelease(Queue.sliding<void>(1), Queue.shutdown);
  const signals = yield* Effect.acquireRelease(PubSub.sliding<void>(1), PubSub.shutdown);
  yield* Effect.forever(Effect.gen(function* () {
    yield* Queue.take(pending);
    // BEGIN IMMEDIATE serializes behind the transaction that requested the wake.
    // Nested transactions in callers therefore cannot publish before outer commit.
    // A rollback (or failed barrier) still only invalidates; reads must revalidate.
    yield* sql.withTransaction(Effect.void).pipe(
      Effect.catchTags({ SqlError: () => Effect.void }),
    );
    yield* PubSub.publish(signals, undefined);
  })).pipe(Effect.forkScoped);

  const subscribe = PubSub.subscribe(signals).pipe(
    Effect.map((subscription) => ({ wait: PubSub.take(subscription) })),
  );
  return ReviewChanges.of({
    invalidate: Queue.offer(pending, undefined).pipe(Effect.asVoid),
    subscribe,
    waitFor: Effect.fn("ReviewChanges.waitFor")(function* <A, E, R>(input: {
      readonly read: Effect.Effect<A, E, R>;
      readonly isReady: (snapshot: A) => boolean;
      readonly timeoutMs: number;
    }) {
      return yield* Effect.scoped(Effect.gen(function* () {
        const subscription = yield* subscribe;
        const duration = Number.isFinite(input.timeoutMs) ? Math.max(0, Math.min(50000, input.timeoutMs)) : 0;
        const deadline = (yield* Clock.currentTimeMillis) + duration;
        for (;;) {
          const snapshot = yield* input.read;
          if (input.isReady(snapshot)) return snapshot;
          const remaining = deadline - (yield* Clock.currentTimeMillis);
          if (remaining <= 0) return snapshot;
          // Take consumes only an invalidation, never an authorization or result.
          yield* subscription.wait.pipe(Effect.timeoutOption(remaining));
        }
      }));
    }),
  });
});

export const layer = Layer.effect(ReviewChanges, make);
