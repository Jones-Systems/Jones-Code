import { contextBridge, ipcRenderer } from "electron";
import type { DesktopThreadMonitorPeerBridge } from "@t3tools/contracts/jones/desktopThreadMonitor";
import { MONITOR_CHANNELS as channels } from "./channels.ts";
import { envelope, peerPayload, publisherPayload } from "./transport.ts";

const bridge: DesktopThreadMonitorPeerBridge = {
  ready: async () => {
    await ipcRenderer.invoke(channels.dispatch, envelope({ type: "ready" }));
  },
  dispatch: async (intent) => {
    await ipcRenderer.invoke(channels.dispatch, envelope(peerPayload(envelope(intent))));
  },
  onFrame: (listener) => {
    const handler = (_event: Electron.IpcRendererEvent, value: unknown) => {
      try {
        listener(publisherPayload(value));
      } catch {
        /* Reject malformed or wrong-direction native delivery. */
      }
    };
    ipcRenderer.on(channels.frame, handler);
    return () => ipcRenderer.removeListener(channels.frame, handler);
  },
};
contextBridge.exposeInMainWorld("threadMonitor", bridge);
