import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";

/** One server-owned consumer; request completions only enqueue coalesced invalidation. */
export class OriginSignals extends Context.Service<OriginSignals, {
  readonly changed: Effect.Effect<void>;
  readonly take: Effect.Effect<void>;
}>()("jones/remoteAdmissionOrigin/OriginSignals") {}
export const layer = Layer.effect(OriginSignals, Effect.gen(function* () {
  const queue = yield* Queue.sliding<void>(1);
  return OriginSignals.of({changed: Queue.offer(queue, undefined).pipe(Effect.asVoid), take: Queue.take(queue)});
}));
