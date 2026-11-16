import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import {
  JonesInstallPlanRequest,
  JonesInstallPlanResult,
} from "@t3tools/contracts/jones/installPlan";
import * as DesktopIpc from "../../ipc/DesktopIpc.ts";
import { DesktopUpdates } from "../../updates/DesktopUpdatesService.ts";

import * as JonesUpdateChannels from "../updates/ipcChannels.ts";
export const installPlan = DesktopIpc.makeIpcMethod({
  channel: JonesUpdateChannels.INSTALL_PLAN,
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
  channel: JonesUpdateChannels.INSTALL_PLAN_AVAILABLE,
  result: Schema.Boolean,
  handler: Effect.fn("desktop.ipc.jonesInstallPlanAvailable")(function* () {
    const updates = yield* DesktopUpdates;
    return updates.installPlan !== undefined;
  }),
});
