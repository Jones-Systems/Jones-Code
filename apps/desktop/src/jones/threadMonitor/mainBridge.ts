import { ipcRenderer } from "electron";
import type { DesktopThreadMonitorMainBridge } from "@t3tools/contracts/jones/desktopThreadMonitor";
import { MONITOR_CHANNELS as channels } from "./channels.ts";
import { envelope, peerPayload, publisherPayload, statePayload } from "./transport.ts";

export function createThreadMonitorMainBridge(): DesktopThreadMonitorMainBridge {
  return {
    getState: async () => statePayload(await ipcRenderer.invoke(channels.getState, envelope(null))),
    setEnabled: async (enabled) =>
      statePayload(await ipcRenderer.invoke(channels.setEnabled, envelope(enabled))),
    publish: async (frame) => {
      await ipcRenderer.invoke(channels.publish, envelope(publisherPayload(envelope(frame))));
    },
    invalidateEnvironment: async (frame) => {
      const result = peerPayload(
        await ipcRenderer.invoke(channels.invalidate, envelope(publisherPayload(envelope(frame)))),
      );
      if (result.type !== "environmentInvalidated")
        throw new Error("Invalid monitor retirement acknowledgment");
      return result;
    },
    onIntent: (listener) => {
      const handler = (_event: Electron.IpcRendererEvent, value: unknown) => {
        try {
          listener(peerPayload(value));
        } catch {
          /* Malformed transport never reaches application handlers. */
        }
      };
      ipcRenderer.on(channels.intent, handler);
      return () => ipcRenderer.removeListener(channels.intent, handler);
    },
    onState: (listener) => {
      const handler = (_event: Electron.IpcRendererEvent, value: unknown) => {
        try {
          listener(statePayload(value));
        } catch {
          /* Ignore invalid native state. */
        }
      };
      ipcRenderer.on(channels.state, handler);
      return () => ipcRenderer.removeListener(channels.state, handler);
    },
  };
}
