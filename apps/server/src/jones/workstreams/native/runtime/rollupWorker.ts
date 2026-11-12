import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Application from "../rollups/application.ts";
import type * as Evidence from "../rollups/evidence.ts";
import { makeSingleFlight, superviseLoop } from "./supervision.ts";

export class NativeRollupWorker extends Context.Service<
  NativeRollupWorker,
  {
    readonly sweep: () => Effect.Effect<Application.RunOutcome | null, Evidence.RollupError>;
  }
>()("t3/jones/workstreams/native/runtime/rollupWorker/NativeRollupWorker") {}

const make = Effect.gen(function* () {
  const application = yield* Application.RollupApplication;
  let stopped = false;
  // Manual and background work share the owner's scope; caller cancellation also interrupts its sweep.
  const flight = yield* makeSingleFlight<Application.RunOutcome | null, Evidence.RollupError>({
    whenBusy: { skip: null },
    starterCancels: true,
  });
  const sweep = Effect.fn("NativeRollupWorker.sweep")(() =>
    Effect.suspend(() => (stopped ? Effect.succeed(null) : flight.run(application.processOnce()))),
  );
  // Completion-based cadence never queues missed ticks or retries an application attempt.
  yield* superviseLoop({
    loop: "rollup",
    iteration: sweep(),
    code: (error) => error.code,
  }).pipe(Effect.forkScoped);
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      stopped = true;
    }),
  );
  return NativeRollupWorker.of({ sweep });
});

export const layer = Layer.effect(NativeRollupWorker, make);
