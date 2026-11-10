import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Forwarder from "../forwarding/forwarder.ts";
import { RegistryError } from "../registry/session.ts";
import * as Application from "./memberApplication.ts";
import { makeSingleFlight, superviseLoop } from "./supervision.ts";

export class MemberRecovery extends Context.Service<
  MemberRecovery,
  {
    readonly sweep: () => Effect.Effect<readonly Forwarder.ForwardView[], RegistryError>;
  }
>()("t3/jones/workstreams/native/runtime/memberRecovery") {}

const make = Effect.gen(function* () {
  const application = yield* Application.MemberApplication;
  const scope = yield* Effect.scope;
  // The owner scope runs the sweep. Waiting callers can leave without cancelling recovery.
  const flight = yield* makeSingleFlight<readonly Forwarder.ForwardView[], RegistryError>({
    whenBusy: "join",
    starterCancels: false,
  });
  const sweep = Effect.fn("MemberRecovery.sweep")(() =>
    Effect.suspend(() =>
      scope.state._tag === "Closed"
        ? Effect.fail(new RegistryError({ code: "authority_fenced" }))
        : flight.run(application.recoverOpen({ dueOnly: true })),
    ),
  );
  // Completion-based cadence never queues missed ticks or grants unknown-outcome redispatch.
  yield* superviseLoop({
    loop: "member-recovery",
    iteration: sweep(),
    code: (error) => error.code,
  }).pipe(Effect.forkScoped);
  return MemberRecovery.of({ sweep });
});

export const layer = Layer.effect(MemberRecovery, make);
