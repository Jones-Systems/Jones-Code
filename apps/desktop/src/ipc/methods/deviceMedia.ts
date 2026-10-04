import {
  DesktopDeviceMediaTunnelInputSchema,
  DesktopDeviceMediaTunnelSchema,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { webContents } from "electron";

import * as DesktopDeviceMediaTunnel from "../../ssh/DesktopDeviceMediaTunnel.ts";
import * as DesktopIpc from "../DesktopIpc.ts";
import * as IpcChannels from "../channels.ts";

export const openDeviceMediaTunnel = DesktopIpc.makeIpcMethod({
  channel: IpcChannels.OPEN_DEVICE_MEDIA_TUNNEL_CHANNEL,
  payload: DesktopDeviceMediaTunnelInputSchema,
  result: DesktopDeviceMediaTunnelSchema,
  handler: Effect.fn("desktop.ipc.deviceMedia.open")(function* (input, event) {
    const renderer = event === undefined ? undefined : webContents.fromId(event.sender.id);
    if (renderer === undefined || renderer.isDestroyed()) {
      return yield* new DesktopDeviceMediaTunnel.DesktopDeviceMediaTunnelError({
        message: "Device media renderer is unavailable.",
      });
    }
    const tunnels = yield* DesktopDeviceMediaTunnel.DesktopDeviceMediaTunnel;
    let disposed = false;
    return yield* tunnels.open(input, renderer.id, {
      isDisposed: () => disposed || renderer.isDestroyed(),
      subscribe: (dispose) => {
        const stop = () => {
          disposed = true;
          dispose();
        };
        const navigate = (
          _event: Electron.Event,
          _url: string,
          _isInPlace: boolean,
          isMainFrame: boolean,
        ) => {
          if (isMainFrame) stop();
        };
        renderer.once("destroyed", stop);
        renderer.once("render-process-gone", stop);
        renderer.on("did-start-navigation", navigate);
        return () => {
          renderer.removeListener("destroyed", stop);
          renderer.removeListener("render-process-gone", stop);
          renderer.removeListener("did-start-navigation", navigate);
        };
      },
    });
  }),
});

export const closeDeviceMediaTunnel = DesktopIpc.makeIpcMethod({
  channel: IpcChannels.CLOSE_DEVICE_MEDIA_TUNNEL_CHANNEL,
  payload: Schema.String,
  result: Schema.Void,
  handler: Effect.fn("desktop.ipc.deviceMedia.close")(function* (id, event) {
    if (event === undefined) return;
    const tunnels = yield* DesktopDeviceMediaTunnel.DesktopDeviceMediaTunnel;
    yield* tunnels.close(id, event.sender.id);
  }),
});
