import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import {
  JonesInstallPlanRequest,
  JonesInstallPlanResult,
} from "@t3tools/contracts/jones/installPlan";
import * as DesktopIpc from "../../ipc/DesktopIpc.ts";
import { DesktopUpdates } from "../../updates/DesktopUpdates.ts";

const INSTALL_PLAN_CHANNEL = "desktop:jones-install-plan";
export const installPlan = DesktopIpc.makeIpcMethod({
  channel: INSTALL_PLAN_CHANNEL,
  payload: JonesInstallPlanRequest,
  result: JonesInstallPlanResult,
  handler: Effect.fn("desktop.ipc.jonesInstallPlan")(function* (request) {
    const updates = yield* DesktopUpdates;
    if (updates.installPlan === undefined)
      return yield* Effect.die(new Error("Install plans are unavailable in this desktop build."));
    return yield* updates.installPlan(request);
  }),
});

export const installPlanAvailable = DesktopIpc.makeSyncIpcMethod({
  channel: "desktop:jones-install-plan-available",
  result: Schema.Boolean,
  handler: Effect.fn("desktop.ipc.jonesInstallPlanAvailable")(function* () {
    const updates = yield* DesktopUpdates;
    return updates.installPlan !== undefined;
  }),
});
