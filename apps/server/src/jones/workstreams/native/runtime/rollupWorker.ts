import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Application from "../rollups/application.ts";
import type * as Evidence from "../rollups/evidence.ts";

export class NativeRollupWorker extends Context.Service<
  NativeRollupWorker,
  {
    readonly sweep: () => Effect.Effect<Application.RunOutcome | null, Evidence.RollupError>;
  }
>()("t3/jones/workstreams/native/runtime/rollupWorker/NativeRollupWorker") {}

const make = Effect.gen(function* () {
  const application = yield* Application.RollupApplication;
  const scope = yield* Effect.scope;
  let sweeping = false;
  let stopped = false;
  const sweep = Effect.fn("NativeRollupWorker.sweep")(() =>
    Effect.uninterruptibleMask((restore) =>
      Effect.suspend(() => {
        if (stopped || sweeping) return Effect.succeed(null);
        sweeping = true;
        return Effect.gen(function* () {
          // Manual and background work share the owner's scope; caller cancellation also interrupts its sweep.
          const fiber = yield* restore(application.processOnce()).pipe(Effect.forkIn(scope));
          return yield* restore(Fiber.join(fiber)).pipe(
            Effect.onInterrupt(() => Fiber.interrupt(fiber)),
          );
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              sweeping = false;
            }),
          ),
        );
      }),
    ),
  );
  yield* Effect.gen(function* () {
    for (;;) {
      yield* sweep().pipe(Effect.catchTags({ WorkstreamsRollupError: () => Effect.void }));
      // Completion-based cadence never queues missed ticks or retries an application attempt.
      yield* Effect.sleep(30_000);
    }
  }).pipe(Effect.forkScoped);
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      stopped = true;
    }),
  );
  return NativeRollupWorker.of({ sweep });
});

export const layer = Layer.effect(NativeRollupWorker, make);
