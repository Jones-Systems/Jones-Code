import { DesktopUpdateStateSchema } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import * as DesktopIpc from "../../ipc/DesktopIpc.ts";
import * as DesktopUpdates from "../../updates/DesktopUpdates.ts";
import * as JonesUpdateChannels from "./ipcChannels.ts";

export const setBackgroundPreparation = DesktopIpc.makeIpcMethod({
  channel: JonesUpdateChannels.SET_BACKGROUND_PREPARATION,
  payload: Schema.Boolean,
  result: DesktopUpdateStateSchema,
  handler: Effect.fn("desktop.ipc.jonesUpdates.setBackgroundPreparation")(function* (enabled) {
    const updates = yield* DesktopUpdates.DesktopUpdates;
    if (updates.setBackgroundPreparation === undefined)
      return yield* Effect.die(
        new Error("Background preparation is unavailable in this desktop build."),
      );
    return yield* updates.setBackgroundPreparation(enabled);
  }),
});
