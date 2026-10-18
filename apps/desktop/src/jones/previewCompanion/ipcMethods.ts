import {
  DesktopCompanionConfigureInput,
  DesktopCompanionState,
  DesktopCompanionTicketResponse,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as DesktopIpc from "../../ipc/DesktopIpc.ts";
import * as ElectronWindow from "../../electron/ElectronWindow.ts";
import * as DesktopAppSettings from "../../settings/DesktopAppSettings.ts";
import * as DesktopLifecycle from "../../app/DesktopLifecycle.ts";
import * as CompanionConfig from "./CompanionConfig.ts";
import * as CompanionUplink from "./CompanionUplink.ts";
import * as Channels from "./channels.ts";

export class CompanionIpcSenderError extends Schema.TaggedError<CompanionIpcSenderError>()(
  "CompanionIpcSenderError",
  {},
) {
  override get message() {
    return "Preview companion IPC requires the main application renderer.";
  }
}

export class CompanionIpcPayloadError extends Schema.TaggedError<CompanionIpcPayloadError>()(
  "CompanionIpcPayloadError",
  {},
) {
  override get message() {
    return "Invalid preview companion ticket response.";
  }
}

const mainRenderer = (event?: DesktopIpc.DesktopIpcInvokeEvent) =>
  Effect.gen(function* () {
    const windows = yield* ElectronWindow.ElectronWindow;
    const main = yield* windows.main;
    if (
      event === undefined ||
      Option.isNone(main) ||
      main.value.webContents.isDestroyed() ||
      main.value.webContents.id !== event.sender.id
    )
      return yield* new CompanionIpcSenderError();
    return main.value.webContents;
  });

export const getState = DesktopIpc.makeIpcMethod({
  channel: Channels.GET_STATE,
  payload: Schema.Void,
  result: DesktopCompanionState,
  handler: (_, event) =>
    Effect.gen(function* () {
      yield* mainRenderer(event);
      return yield* (yield* CompanionUplink.CompanionUplink).getState;
    }),
});

const configure = DesktopIpc.makeIpcMethod({
  channel: Channels.CONFIGURE,
  payload: DesktopCompanionConfigureInput,
  result: DesktopCompanionState,
  handler: (input, event) =>
    Effect.gen(function* () {
      yield* mainRenderer(event);
      const store = yield* CompanionConfig.CompanionConfig;
      const uplink = yield* CompanionUplink.CompanionUplink;
      const settings = yield* DesktopAppSettings.DesktopAppSettings;
      const config = yield* store.set(input);
      const restartRequired =
        CompanionConfig.browserOnlyStartup(config) && (yield* settings.get).localEnvironmentEnabled;
      yield* uplink.configure(config, restartRequired);
      if (restartRequired) {
        // The next bootstrap disables the backend before its first spawn; never relabel a running server as browser-only.
        yield* (yield* DesktopLifecycle.DesktopLifecycle).relaunch(
          "previewCompanion.browserOnly=true",
        );
      }
      return yield* uplink.getState;
    }),
});

export const setTicketProviderReady = DesktopIpc.makeIpcMethod({
  channel: Channels.TICKET_PROVIDER_READY,
  payload: Schema.Boolean,
  result: Schema.Void,
  handler: (ready, event) =>
    Effect.gen(function* () {
      const renderer = yield* mainRenderer(event);
      yield* (yield* CompanionUplink.CompanionUplink).setReady(ready, renderer);
    }),
});

export const completeTicket = DesktopIpc.makeIpcMethod({
  channel: Channels.COMPLETE_TICKET,
  // Decode after sender validation, replacing schema errors that could contain a ticket URL.
  payload: Schema.Unknown,
  result: Schema.Void,
  handler: (raw, event) =>
    Effect.gen(function* () {
      yield* mainRenderer(event);
      const response = yield* Schema.decodeUnknownEffect(DesktopCompanionTicketResponse)(raw).pipe(
        Effect.mapError(() => new CompanionIpcPayloadError()),
      );
      yield* (yield* CompanionUplink.CompanionUplink).completeTicket(response);
    }),
});

export const retry = DesktopIpc.makeIpcMethod({
  channel: Channels.RETRY,
  payload: Schema.Void,
  result: Schema.Void,
  handler: (_, event) =>
    Effect.gen(function* () {
      yield* mainRenderer(event);
      yield* (yield* CompanionUplink.CompanionUplink).retry;
    }),
});

export const install = Effect.gen(function* () {
  const ipc = yield* DesktopIpc.DesktopIpc;
  yield* ipc.handle(getState);
  yield* ipc.handle(configure);
  yield* ipc.handle(setTicketProviderReady);
  yield* ipc.handle(completeTicket);
  yield* ipc.handle(retry);
});
