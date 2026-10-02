// @effect-diagnostics nodeBuiltinImport:off - Packaged bootstrap must bind Electron storage before any asynchronous initialization.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as Electron from "electron";
import { configureDesktopUserDataOverride } from "./app/DesktopUserDataOverride.ts";

configureDesktopUserDataOverride({
  directory: process.env.T3CODE_DESKTOP_USER_DATA_DIR,
  path: NodePath,
  createDirectory: (directory) => NodeFS.mkdirSync(directory, { recursive: true }),
  setPath: (name, directory) => Electron.app.setPath(name, directory),
});

// Packaged app entry. Enables the compile cache before the main bundle loads,
// so the cache also covers main.cjs itself.
require("./compileCache.cjs");
require("./main.cjs");
