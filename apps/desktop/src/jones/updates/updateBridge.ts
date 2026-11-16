import type { DesktopBridge } from "@t3tools/contracts";
import * as IpcChannels from "../../ipc/channels.ts";
import * as JonesUpdateChannels from "./ipcChannels.ts";

export interface DesktopUpdateRenderer {
  readonly invoke: (channel: string, ...args: unknown[]) => Promise<unknown>;
  readonly sendSync: (channel: string) => unknown;
  readonly on: (channel: string, listener: (event: unknown, value: unknown) => void) => unknown;
  readonly removeListener: (
    channel: string,
    listener: (event: unknown, value: unknown) => void,
  ) => unknown;
}
type UpdateBridge = Pick<
  DesktopBridge,
  | "getUpdateState"
  | "setUpdateChannel"
  | "checkForUpdate"
  | "listJonesUpdateBuilds"
  | "selectJonesUpdateBuild"
  | "downloadUpdate"
  | "discardUpdate"
  | "setJonesBackgroundPreparation"
  | "installPlan"
  | "onInstallPlan"
  | "fleetUpdates"
  | "installUpdate"
  | "onUpdateState"
>;
export function createDesktopUpdateBridge(ipcRenderer: DesktopUpdateRenderer): UpdateBridge {
  return {
    getUpdateState: () => ipcRenderer.invoke(IpcChannels.UPDATE_GET_STATE_CHANNEL),
    setUpdateChannel: (channel, minimumVersion) =>
      ipcRenderer.invoke(IpcChannels.UPDATE_SET_CHANNEL_CHANNEL, { channel, minimumVersion }),
    checkForUpdate: () => ipcRenderer.invoke(IpcChannels.UPDATE_CHECK_CHANNEL),
    listJonesUpdateBuilds: () => ipcRenderer.invoke(IpcChannels.JONES_UPDATE_LIST_BUILDS_CHANNEL),
    selectJonesUpdateBuild: (build) =>
      ipcRenderer.invoke(IpcChannels.JONES_UPDATE_SELECT_BUILD_CHANNEL, build),
    downloadUpdate: (selection) =>
      selection === undefined
        ? ipcRenderer.invoke(IpcChannels.UPDATE_DOWNLOAD_CHANNEL)
        : ipcRenderer.invoke(IpcChannels.UPDATE_DOWNLOAD_CHANNEL, selection),
    discardUpdate: (stagedHandle) =>
      ipcRenderer.invoke(IpcChannels.UPDATE_DISCARD_CHANNEL, stagedHandle),
    setJonesBackgroundPreparation: (enabled) =>
      ipcRenderer.invoke(JonesUpdateChannels.SET_BACKGROUND_PREPARATION, enabled),
    ...(ipcRenderer.sendSync(JonesUpdateChannels.INSTALL_PLAN_AVAILABLE) === true
      ? {
          installPlan: (request: Parameters<NonNullable<DesktopBridge["installPlan"]>>[0]) =>
            ipcRenderer.invoke(JonesUpdateChannels.INSTALL_PLAN, request),
          onInstallPlan: (listener: Parameters<NonNullable<DesktopBridge["onInstallPlan"]>>[0]) => {
            const wrapped = (_event: unknown, state: unknown) =>
              listener(state as Parameters<typeof listener>[0]);
            ipcRenderer.on(JonesUpdateChannels.INSTALL_PLAN_STATE, wrapped);
            return () =>
              ipcRenderer.removeListener(JonesUpdateChannels.INSTALL_PLAN_STATE, wrapped);
          },
        }
      : {}),
    fleetUpdates: (request) => ipcRenderer.invoke(IpcChannels.FLEET_UPDATES_CHANNEL, request),
    installUpdate: (stagedHandle, campaignId) =>
      campaignId !== undefined
        ? ipcRenderer.invoke(IpcChannels.UPDATE_INSTALL_CHANNEL, { stagedHandle, campaignId })
        : stagedHandle === undefined
          ? ipcRenderer.invoke(IpcChannels.UPDATE_INSTALL_CHANNEL)
          : ipcRenderer.invoke(IpcChannels.UPDATE_INSTALL_CHANNEL, stagedHandle),
    onUpdateState: (listener) => {
      const wrappedListener = (_event: unknown, state: unknown) => {
        if (typeof state !== "object" || state === null) return;
        listener(state as Parameters<typeof listener>[0]);
      };

      ipcRenderer.on(IpcChannels.UPDATE_STATE_CHANNEL, wrappedListener);
      return () => {
        ipcRenderer.removeListener(IpcChannels.UPDATE_STATE_CHANNEL, wrappedListener);
      };
    },
  } as UpdateBridge;
}
