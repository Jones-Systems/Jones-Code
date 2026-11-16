import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SubscriptionRef from "effect/SubscriptionRef";

import type { RpcSession } from "../../rpc/session.ts";

export interface SubscriptionProducer {
  readonly session: RpcSession;
  readonly subscriptionEpoch: number;
}

export function makeProducerGuard(
  session: SubscriptionRef.SubscriptionRef<Option.Option<RpcSession>>,
) {
  let active: SubscriptionProducer | undefined;
  const isCurrent = (producer: SubscriptionProducer) =>
    active?.session === producer.session &&
    active.subscriptionEpoch === producer.subscriptionEpoch &&
    Option.getOrNull(SubscriptionRef.getUnsafe(session)) === producer.session;
  return {
    begin: (producer: SubscriptionProducer) =>
      Effect.sync(() => {
        active = producer;
      }),
    isCurrent,
    // The state semaphore may have waited after the caller acquired its apply lock.
    // Check here so a replacement during that wait cannot commit old data or freshness.
    commit: <A>(
      producer: SubscriptionProducer,
      state: SubscriptionRef.SubscriptionRef<A>,
      update: (current: A) => A,
    ) =>
      SubscriptionRef.modify(state, (current) =>
        isCurrent(producer) ? ([true, update(current)] as const) : ([false, current] as const),
      ),
  };
}

// Chunk delivery is already traced by the state apply operation; keep its batching loop untraced.
export const applyProducerBatches = Effect.fnUntraced(function* <A, E, R>(
  items: ReadonlyArray<readonly [SubscriptionProducer, A]>,
  apply: (producer: SubscriptionProducer, values: ReadonlyArray<A>) => Effect.Effect<void, E, R>,
): Effect.fn.Return<void, E, R> {
  let index = 0;
  while (index < items.length) {
    const producer = items[index]![0];
    const values: A[] = [];
    while (index < items.length && items[index]![0] === producer) {
      values.push(items[index++]![1]);
    }
    yield* apply(producer, values);
  }
});
