// @effect-diagnostics nodeBuiltinImport:off - Packaged bootstrap must bind Electron storage before any asynchronous initialization.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import {
  configureCompanionProduct,
  readDesktopProductMetadata,
} from "./jones/previewCompanion/CompanionProduct.ts";
import { configureDesktopUserDataOverride } from "./app/DesktopUserDataOverride.ts";
import { holdJonesDesktopNativeWriterFence } from "./jones/updates/jonesNativeStartup.ts";

const electron = require("electron") as typeof import("electron");
const metadata = readDesktopProductMetadata({
  isPackaged: electron.app.isPackaged,
  readPackage: () =>
    NodeFS.readFileSync(NodePath.join(electron.app.getAppPath(), "package.json"), "utf8"),
});
holdJonesDesktopNativeWriterFence({
  platform: process.platform,
  env: process.env,
  homeDirectory: electron.app.getPath("home"),
  appDataDirectory: electron.app.getPath("appData"),
  version: electron.app.getVersion(),
  metadata,
});
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
