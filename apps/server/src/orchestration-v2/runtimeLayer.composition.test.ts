import { expect, it } from "@effect/vitest";
import { RunId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as StartAdmission from "../jones/provider/StartAdmission.ts";
import * as Goals from "../jones/goals/continuity/GoalContinuity.ts";
import * as Control from "../jones/orchestration/ControlAdmission.ts";
import * as Refusals from "../jones/startRefusal/Store.ts";
import * as Restart from "../jones/orchestration/RestartPreparation.ts";
import * as Environment from "../environment/ServerEnvironment.ts";
import { CurrentEffectClaim } from "../jones/workspaceLifecycle/ActiveEffectClaim.ts";
import * as Starts from "./ProviderTurnStartService.ts";
import { layerCapturedProviderTurnStart } from "./runtimeLayer.ts";

// Exercise the production composition boundary after its build scope has ended.
// These explicit external service fakes prove registration/context plumbing only;
// recorded admission, native guards and actual boot still need composed proofs.
it.effect("captures static start ports once while retaining each invocation's original worker claim", () =>
  Effect.scoped(Effect.gen(function* () {
    const registrations = yield* Layer.build(Layer.mergeAll(
      Layer.mock(StartAdmission.StartAdmission)({}),
      Layer.mock(Goals.GoalContinuity)({}),
      Layer.mock(Control.ControlAdmission)({}),
      Layer.mock(Refusals.StartRefusals)({}),
      Layer.mock(Restart.RestartPreparation)({}),
      Layer.mock(Environment.ServerEnvironmentIdentity)({}),
    ));
    const observed: unknown[][] = [];
    const invoke = Effect.gen(function* () {
      observed.push([
        Option.getOrThrow(yield* Effect.serviceOption(StartAdmission.StartAdmission)),
        Option.getOrThrow(yield* Effect.serviceOption(Goals.GoalContinuity)),
        Option.getOrThrow(yield* Effect.serviceOption(Control.ControlAdmission)),
        Option.getOrThrow(yield* Effect.serviceOption(Refusals.StartRefusals)),
        Option.getOrThrow(yield* Effect.serviceOption(Restart.RestartPreparation)),
        Option.getOrThrow(yield* Effect.serviceOption(Environment.ServerEnvironmentIdentity)),
        Option.getOrThrow(yield* Effect.serviceOption(CurrentEffectClaim)),
      ]);
    });
    const source = Starts.ProviderTurnStartServiceV2.of({
      start: () => invoke,
      failStartingRun: () => Effect.void,
    });
    const wrapped = Context.get(yield* Layer.build(layerCapturedProviderTurnStart.pipe(
      Layer.provide(Layer.succeed(Starts.ProviderTurnStartServiceV2, source)),
      Layer.provide(Layer.succeedContext(registrations)),
    )), Starts.ProviderTurnStartServiceV2);
    const claims = yield* Effect.forEach([1, 2], () => Layer.build(Layer.mock(CurrentEffectClaim)({})));
    for (const claim of claims) {
      yield* wrapped.start({ threadId: ThreadId.make("thread"), runId: RunId.make("run") }).pipe(
        Effect.provideService(CurrentEffectClaim, Context.get(claim, CurrentEffectClaim)),
        // A caller cannot substitute another static registration after capture.
        Effect.provideService(StartAdmission.StartAdmission, {
          capture: () => Effect.die("caller substituted the recorded owner"),
        }),
      );
    }
    const expected = [
      Context.get(registrations, StartAdmission.StartAdmission), Context.get(registrations, Goals.GoalContinuity),
      Context.get(registrations, Control.ControlAdmission), Context.get(registrations, Refusals.StartRefusals),
      Context.get(registrations, Restart.RestartPreparation), Context.get(registrations, Environment.ServerEnvironmentIdentity),
    ];
    expect(observed).toHaveLength(2);
    for (let i = 0; i < observed.length; i++) {
      expected.forEach((service, index) => expect(observed[i]![index]).toBe(service));
      expect(observed[i]![6]).toBe(Context.get(claims[i]!, CurrentEffectClaim));
    }
  })),
);
