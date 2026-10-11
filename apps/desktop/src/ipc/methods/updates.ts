import { FleetDesktopRequest, FleetDesktopState, FleetOperationId } from "@t3tools/contracts/jones/fleet-updates";
import { JonesUpdateDownloadInput } from "@t3tools/contracts/jones/jonesUpdates";
import {
  DesktopUpdateActionResultSchema,
  DesktopUpdateChannelSchema,
  DesktopUpdateCheckResultSchema,
  DesktopUpdateStateSchema,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import * as DesktopUpdates from "../../updates/DesktopUpdates.ts";
import * as IpcChannels from "../channels.ts";
import * as DesktopIpc from "../DesktopIpc.ts";

export const getUpdateState = DesktopIpc.makeIpcMethod({
  channel: IpcChannels.UPDATE_GET_STATE_CHANNEL,
  payload: Schema.Void,
  result: DesktopUpdateStateSchema,
  handler: Effect.fn("desktop.ipc.updates.getState")(function* () {
    const updates = yield* DesktopUpdates.DesktopUpdates;
    return yield* updates.getState;
  }),
});

export const setUpdateChannel = DesktopIpc.makeIpcMethod({
  channel: IpcChannels.UPDATE_SET_CHANNEL_CHANNEL,
  payload: DesktopUpdateChannelSchema,
  result: DesktopUpdateStateSchema,
  handler: Effect.fn("desktop.ipc.updates.setChannel")(function* (channel) {
    const updates = yield* DesktopUpdates.DesktopUpdates;
    return yield* updates.setChannel(channel);
  }),
});

export const downloadUpdate = DesktopIpc.makeIpcMethod({
  channel: IpcChannels.UPDATE_DOWNLOAD_CHANNEL,
  payload: Schema.Union([Schema.Undefined, JonesUpdateDownloadInput]),
  result: DesktopUpdateActionResultSchema,
  handler: Effect.fn("desktop.ipc.updates.download")(function* (selection) {
    const updates = yield* DesktopUpdates.DesktopUpdates;
    if (selection !== undefined) {
      if (updates.downloadSelected !== undefined) return yield* updates.downloadSelected(selection);
      return { accepted: false, completed: false, state: yield* updates.getState };
    }
    const state = yield* updates.getState;
    if (state.jones !== undefined) return { accepted: false, completed: false, state };
    return yield* updates.download;
  }),
});

export const installUpdate = DesktopIpc.makeIpcMethod({
  channel: IpcChannels.UPDATE_INSTALL_CHANNEL,
  payload: Schema.Union([Schema.Undefined, Schema.String, Schema.Struct({ stagedHandle: Schema.String, campaignId: FleetOperationId })]),
  result: DesktopUpdateActionResultSchema,
  handler: Effect.fn("desktop.ipc.updates.install")(function* (request) {
    const updates = yield* DesktopUpdates.DesktopUpdates;
    const stagedHandle = typeof request === "object" ? request.stagedHandle : request;
    const campaignId = typeof request === "object" ? request.campaignId : undefined;
    if (stagedHandle !== undefined) {
      if (updates.installStaged !== undefined) return yield* (campaignId === undefined
        ? updates.installStaged(stagedHandle) : updates.installStaged(stagedHandle, campaignId));
      return { accepted: false, completed: false, state: yield* updates.getState };
    }
    return yield* updates.install;
  }),
});

export const checkForUpdate = DesktopIpc.makeIpcMethod({
  channel: IpcChannels.UPDATE_CHECK_CHANNEL,
  payload: Schema.Void,
  result: DesktopUpdateCheckResultSchema,
  handler: Effect.fn("desktop.ipc.updates.check")(function* () {
    const updates = yield* DesktopUpdates.DesktopUpdates;
    return yield* updates.check("web-ui");
  }),
});

export const discardUpdate = DesktopIpc.makeIpcMethod({
  channel: IpcChannels.UPDATE_DISCARD_CHANNEL,
  payload: Schema.String,
  result: DesktopUpdateActionResultSchema,
  handler: Effect.fn("desktop.ipc.updates.discard")(function* (handle) {
    const updates = yield* DesktopUpdates.DesktopUpdates;
    if (updates.discardStaged !== undefined) return yield* updates.discardStaged(handle);
    return { accepted: false, completed: false, state: yield* updates.getState };
  }),
});

export const fleetUpdates = DesktopIpc.makeIpcMethod({
  channel: IpcChannels.FLEET_UPDATES_CHANNEL,
  payload: FleetDesktopRequest,
  result: FleetDesktopState,
  handler: Effect.fn("desktop.ipc.updates.fleet")(function* (request) {
    const updates = yield* DesktopUpdates.DesktopUpdates;
    if (updates.fleetUpdates === undefined) return yield* Effect.die(new Error("Fleet updates are unavailable in this desktop build."));
    return yield* updates.fleetUpdates(request);
  }),
});
