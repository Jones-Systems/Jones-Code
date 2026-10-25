import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Targets from "../ordinaryCheckout/CheckoutTarget.ts";
import { canonical, CheckoutError, type Target } from "../ordinaryCheckout/CheckoutProtocol.ts";

export interface CheckoutSource {
  readonly target: Target;
  readonly head: string;
}

export class CheckoutSourceObservation extends Context.Service<
  CheckoutSourceObservation,
  {
    readonly capture: (input: Targets.TargetInput) => Effect.Effect<CheckoutSource, CheckoutError>;
    readonly revalidate: (source: CheckoutSource) => Effect.Effect<void, CheckoutError>;
  }
>()("t3/jones/workspaceLifecycle/CheckoutSource/CheckoutSourceObservation") {}

const make = Effect.gen(function* () {
  const targets = yield* Targets.CheckoutTarget;
  const observation = yield* Targets.CheckoutTargetObservation;
  const capture = Effect.fn("CheckoutSourceObservation.capture")(function* (
    input: Targets.TargetInput,
  ) {
    const target = yield* targets.capture(input);
    const before = yield* observation.git(target.checkoutKey);
    if (
      before?.head === undefined ||
      !/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/.test(before.head) ||
      before.root !== target.checkoutKey ||
      before.commonDirectory !== target.gitCommonDirectory ||
      before.gitDirectory !== target.gitDirectory ||
      before.branch !== target.branch ||
      canonical(before.commonIdentity) !== canonical(target.gitCommonIdentity) ||
      canonical(before.directoryIdentity) !== canonical(target.gitDirectoryIdentity)
    )
      return yield* new CheckoutError({ reason: "identity" });
    yield* targets.revalidate(target);
    const after = yield* observation.git(target.checkoutKey);
    if (canonical(before) !== canonical(after))
      return yield* new CheckoutError({ reason: "stale" });
    return { target, head: before.head };
  });
  return CheckoutSourceObservation.of({
    capture,
    revalidate: Effect.fn("CheckoutSourceObservation.revalidate")(function* (source) {
      if (canonical(yield* capture(source.target)) !== canonical(source))
        return yield* new CheckoutError({ reason: "stale" });
    }),
  });
});

export const layer = Layer.effect(CheckoutSourceObservation, make);
