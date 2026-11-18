// @effect-diagnostics nodeBuiltinImport:off - Narrow packaged Electron qualification host; never imports application startup.
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { app, BrowserWindow, ipcMain, session } from "electron";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import {
  createPickerVerificationCore,
  type PickerVerificationInput,
} from "./pickerVerificationCore.ts";
import * as Channels from "../../ipc/channels.ts";
import * as DesktopIpc from "../../ipc/DesktopIpc.ts";
import * as JonesChannels from "./ipcChannels.ts";
import { registerDesktopUpdateIpc } from "./updateIpcRegistration.ts";

export function prepare(input: { readonly root: string; readonly nonce: string }) {
  if (app.isReady()) throw new Error("Picker paths must be configured before Electron ready.");
  const paths = {
    userData: "electron-user",
    sessionData: "electron-session",
    logs: "electron-logs",
    crashDumps: "electron-crashes",
    temp: "electron-temp",
  } as const;
  for (const [key, value] of Object.entries(paths))
    app.setPath(key as keyof typeof paths, NodePath.join(input.root, value));
  app.commandLine.appendSwitch("host-resolver-rules", "MAP * ~NOTFOUND");
  app.disableHardwareAcceleration();
  app.dock?.hide();
  const observed = Object.fromEntries(
    Object.keys(paths).map((key) => [key, app.getPath(key as keyof typeof paths)]),
  );
  for (const [key, value] of Object.entries(paths)) {
    if (observed[key] !== NodePath.join(input.root, value))
      throw new Error(`Picker path readback failed: ${key}`);
  }
  return observed;
}

export async function createHost(
  input: Omit<PickerVerificationInput, "broadcastState" | "broadcastPlan"> & {
    readonly preload: string;
    readonly content: string;
    readonly onPlan: (
      state: import("@t3tools/contracts/jones/installPlan").JonesInstallPlanState,
    ) => void;
  },
) {
  if (!app.isReady()) throw new Error("Picker host requires Electron ready.");
  const network: string[] = [];
  const partition = session.fromPartition(`jones-picker-${input.nonce}`);
  const allowed = NodeURL.pathToFileURL(input.content).href;
  partition.webRequest.onBeforeRequest((details, callback) => {
    network.push(details.url);
    callback({ cancel: details.url !== allowed });
  });
  partition.setPermissionRequestHandler((_contents, _permission, callback) => callback(false));
  partition.setPermissionCheckHandler(() => false);
  let window: BrowserWindow | undefined;
  const core = await createPickerVerificationCore({
    ...input,
    broadcastState: async (state) => {
      window?.webContents.send(Channels.UPDATE_STATE_CHANNEL, state);
    },
    broadcastPlan: async (state) => {
      input.onPlan(state);
      window?.webContents.send(JonesChannels.INSTALL_PLAN_STATE, state);
    },
  });
  let installs = 0;
  await core.register(
    registerDesktopUpdateIpc(
      DesktopIpc.make(
        {
          removeHandler: (channel) => ipcMain.removeHandler(channel),
          handle: (channel, listener) =>
            ipcMain.handle(channel, (event, raw) => {
              if (channel === Channels.UPDATE_INSTALL_CHANNEL) installs++;
              return listener(event, raw);
            }),
          removeAllListeners: (channel) => {
            ipcMain.removeAllListeners(channel);
          },
          on: (channel, listener) => {
            ipcMain.on(channel, listener);
          },
        },
        DesktopIpc.windowSenderGuard(
          Effect.sync(() =>
            window !== undefined && !window.isDestroyed() ? Option.some(window) : Option.none(),
          ),
        ),
      ),
    ),
  );
  window = new BrowserWindow({
    show: false,
    webPreferences: {
      session: partition,
      preload: input.preload,
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
    },
  });
  // Electron 44.10.1 exposes this native method but omits it from public typings.
  const contents = window.webContents as typeof window.webContents & {
    getLastWebPreferences: () => Electron.WebPreferences;
  };
  if (typeof contents.getLastWebPreferences !== "function")
    throw new Error("Electron preference readback unavailable.");
  const preferences = contents.getLastWebPreferences();
  if (!preferences.contextIsolation || preferences.nodeIntegration || !preferences.sandbox)
    throw new Error("Picker security settings differ from production.");
  window.webContents.on("will-navigate", (event) => event.preventDefault());
  window.webContents.setWindowOpenHandler(() => ({ action: "deny" }));
  await window.loadFile(input.content);
  await window.webContents.executeJavaScript(
    `globalThis.pickerEvents = []; desktopBridge.onUpdateState(value => pickerEvents.push({kind:"state", value})); desktopBridge.onInstallPlan(value => pickerEvents.push({kind:"plan", value})); void 0;`,
  );
  return {
    ...core,
    network,
    preferences,
    installCalls: () => installs,
    bridgeCall: (member: string, args: readonly unknown[] = []) => {
      if (!/^[a-zA-Z]+$/.test(member)) throw new Error("Invalid bridge member.");
      return window!.webContents.executeJavaScript(
        `window.desktopBridge[${JSON.stringify(member)}](...${JSON.stringify(args)})`,
      );
    },
    inspect: () =>
      window!.webContents.executeJavaScript(
        `({members:Object.keys(desktopBridge).sort(), require:typeof require, process:typeof process, events:pickerEvents.splice(0)})`,
      ),
    close: async () => {
      window?.destroy();
      await core.close();
    },
  };
}
