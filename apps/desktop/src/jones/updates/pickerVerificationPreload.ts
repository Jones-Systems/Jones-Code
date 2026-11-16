import { contextBridge, ipcRenderer } from "electron";
import { createDesktopUpdateBridge } from "./updateBridge.ts";

contextBridge.exposeInMainWorld("desktopBridge", createDesktopUpdateBridge(ipcRenderer));
