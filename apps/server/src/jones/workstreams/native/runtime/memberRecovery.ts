import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Forwarder from "../forwarding/forwarder.ts";
import { RegistryError } from "../registry/session.ts";
import * as Application from "./memberApplication.ts";

export class MemberRecovery extends Context.Service<
  MemberRecovery,
  {
    readonly sweep: () => Effect.Effect<readonly Forwarder.ForwardView[], RegistryError>;
  }
>()("t3/jones/workstreams/native/runtime/memberRecovery") {}

const make = Effect.gen(function* () {
  const application = yield* Application.MemberApplication;
  const scope = yield* Effect.scope;
  let pending: Deferred.Deferred<readonly Forwarder.ForwardView[], RegistryError> | undefined;
  const sweep = Effect.fn("MemberRecovery.sweep")(() =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        if (scope.state._tag === "Closed")
          return yield* new RegistryError({ code: "authority_fenced" });
        let ready = pending;
        if (ready === undefined) {
          ready = yield* Deferred.make<readonly Forwarder.ForwardView[], RegistryError>();
          const own = ready;
          pending = own;
          // The application owns the sweep. Waiting callers can leave without cancelling recovery.
          const opener = yield* application
            .recoverOpen({ dueOnly: true })
            .pipe(Effect.interruptible, Effect.forkIn(scope));
          // Completion must be observed even if scope disposal interrupts before the effect starts.
          opener.addObserver((exit) => {
            if (pending === own) pending = undefined;
            Deferred.doneUnsafe(own, exit);
          });
        }
        return yield* restore(Deferred.await(ready));
      }),
    ),
  );
  yield* Effect.gen(function* () {
    for (;;) {
      yield* sweep().pipe(Effect.catchTags({ NativeRegistryError: () => Effect.void }));
      // Completion-based cadence never queues missed ticks or grants unknown-outcome redispatch.
      yield* Effect.sleep(30_000);
    }
  }).pipe(Effect.forkScoped);
  return MemberRecovery.of({ sweep });
});

export const layer = Layer.effect(MemberRecovery, make);
