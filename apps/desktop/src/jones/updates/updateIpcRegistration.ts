import * as Effect from "effect/Effect";
import type { DesktopIpc } from "../../ipc/DesktopIpc.ts";
import * as InstallPlanIpc from "../installPlan/ipc.ts";
import * as JonesUpdatesIpc from "./ipcMethods.ts";
import {
  checkForUpdate,
  downloadUpdate,
  discardUpdate,
  fleetUpdates,
  getUpdateState,
  installUpdate,
  listJonesUpdateBuilds,
  selectJonesUpdateBuild,
  setUpdateChannel,
} from "../../ipc/methods/updates.ts";

export const registerDesktopUpdateIpc = Effect.fn("desktop.ipc.registerUpdates")(function* (
  ipc: DesktopIpc["Service"],
) {
  yield* ipc.handle(getUpdateState);
  yield* ipc.handle(setUpdateChannel);
  yield* ipc.handle(downloadUpdate);
  yield* ipc.handle(listJonesUpdateBuilds);
  yield* ipc.handle(selectJonesUpdateBuild);
  yield* ipc.handle(discardUpdate);
  yield* ipc.handle(JonesUpdatesIpc.setBackgroundPreparation);
  yield* ipc.handle(InstallPlanIpc.installPlan);
  yield* ipc.handleSync(InstallPlanIpc.installPlanAvailable);
  yield* ipc.handle(fleetUpdates);
  yield* ipc.handle(installUpdate);
  yield* ipc.handle(checkForUpdate);
});
