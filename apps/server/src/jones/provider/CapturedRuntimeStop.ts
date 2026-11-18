import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as EffectWorker from "../../orchestration-v2/EffectWorker.ts";
import * as RuntimeStop from "../runtime/RuntimeStop.ts";

/** Retain the graph's Stop owner, while inheriting each actual worker invocation. */
export const layer = Layer.effect(
  EffectWorker.OrchestrationEffectExecutorV2,
  Effect.gen(function* () {
    const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
    const stop = yield* RuntimeStop.CurrentRuntimeStop;
    return EffectWorker.OrchestrationEffectExecutorV2.of({
      ...executor,
      execute: Effect.fn("CapturedRuntimeStop.execute")((effect, options) =>
        executor
          .execute(effect, options)
          .pipe(Effect.provideService(RuntimeStop.CurrentRuntimeStop, stop)),
      ),
    });
  }),
);
