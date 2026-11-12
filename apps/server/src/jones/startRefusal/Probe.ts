import type { ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { probeStartAdmission } from "../../orchestration-v2/ProviderTurnStartService.ts";

export class Probe extends Context.Service<
  Probe,
  {
    readonly open: (threadId: ThreadId) => Effect.Effect<boolean>;
  }
>()("t3/jones/startRefusal/Probe") {}

export const layer = Layer.effect(
  Probe,
  Effect.gen(function* () {
    const services =
      yield* Effect.context<Effect.Services<ReturnType<typeof probeStartAdmission>>>();
    return Probe.of({
      open: Effect.fn("Probe.open")((threadId: ThreadId) =>
        probeStartAdmission(threadId).pipe(Effect.provide(services)),
      ),
    });
  }),
);
