// @effect-diagnostics nodeBuiltinImport:off - Packaged bootstrap must bind Electron storage before any asynchronous initialization.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import {
  configureCompanionProduct,
  readDesktopProductMetadata,
} from "./jones/previewCompanion/CompanionProduct.ts";
import { configureDesktopUserDataOverride } from "./app/DesktopUserDataOverride.ts";
import {
  holdJonesDesktopNativeWriterFence,
  reportJonesNativeStartupRefusal,
} from "./jones/updates/jonesNativeStartup.ts";

const electron = require("electron") as typeof import("electron");
function startDesktop(): void {
  const metadata = readDesktopProductMetadata({
    isPackaged: electron.app.isPackaged,
    readPackage: () =>
      NodeFS.readFileSync(NodePath.join(electron.app.getAppPath(), "package.json"), "utf8"),
  });
  try {
    holdJonesDesktopNativeWriterFence({
      // oxlint-disable-next-line t3code/no-global-process-runtime -- Synchronous admission must precede profile and Effect runtime initialization.
      platform: process.platform,
      env: process.env,
      homeDirectory: electron.app.getPath("home"),
      appDataDirectory: electron.app.getPath("appData"),
      version: electron.app.getVersion(),
      metadata,
    });
  } catch (cause) {
    // Electron handles uncaught exceptions without exiting; denied writers must release their leases.
    reportJonesNativeStartupRefusal(cause, {
      showErrorBox: (title, content) => electron.dialog.showErrorBox(title, content),
      exit: (code) => electron.app.exit(code),
    });
    return;
  }
  configureCompanionProduct({
    metadata,
    appDataDirectory: electron.app.getPath("appData"),
    homeDirectory: electron.app.getPath("home"),
    join: NodePath.join,
    env: process.env,
    createDirectory: (directory) => NodeFS.mkdirSync(directory, { recursive: true }),
    setPath: (name, directory) => electron.app.setPath(name, directory),
  });

  configureDesktopUserDataOverride({
    directory:
      process.env.JONES_PREVIEW_COMPANION_PRODUCT === "true"
        ? undefined
        : process.env.T3CODE_DESKTOP_USER_DATA_DIR,
    path: NodePath,
    createDirectory: (directory) => NodeFS.mkdirSync(directory, { recursive: true }),
    setPath: (name, directory) => {
      const electron = require("electron") as typeof import("electron");
      electron.app.setPath(name, directory);
    },
  });

  // Packaged app entry. Enables the compile cache before the main bundle loads,
  // so the cache also covers main.cjs itself.
  require("./compileCache.cjs");
  require("./main.cjs");
}

startDesktop();
