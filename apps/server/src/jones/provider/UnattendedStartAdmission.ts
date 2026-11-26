import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { ServerEnvironmentIdentity } from "../../environment/ServerEnvironment.ts";
import { GoalContinuity } from "../goals/continuity/GoalContinuity.ts";
import { ControlAdmission } from "../orchestration/ControlAdmission.ts";
import { RestartPreparation } from "../orchestration/RestartPreparation.ts";
import { StartRefusals } from "../startRefusal/Store.ts";

const make = Effect.gen(function* () {
  return {
    restart: yield* RestartPreparation,
    control: yield* ControlAdmission,
    goals: yield* GoalContinuity,
    environment: yield* ServerEnvironmentIdentity,
    refusals: yield* StartRefusals,
  };
});

export class UnattendedStartAdmission extends Context.Service<
  UnattendedStartAdmission,
  Effect.Success<typeof make>
>()("t3/jones/provider/UnattendedStartAdmission") {}

// Keep these dependencies scoped to unattended resend. Supplying them directly
// to reconciliation also enables the existing interactive and usage-limit paths.
export const layer = Layer.effect(UnattendedStartAdmission, make);

export function readStartAdmission(context: Context.Context<never>) {
  return Option.all({
    restart: Context.getOption(context, RestartPreparation),
    control: Context.getOption(context, ControlAdmission),
    goals: Context.getOption(context, GoalContinuity),
    environment: Context.getOption(context, ServerEnvironmentIdentity),
    refusals: Context.getOption(context, StartRefusals),
  });
}
