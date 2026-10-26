// @effect-diagnostics nodeBuiltinImport:off - In-memory Electron lifecycle doubles; no host windows or files are created.
import type { BrowserWindow, IpcMainInvokeEvent } from "electron";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { ThreadMonitorHost } from "./host.ts";
import { MONITOR_CHANNELS as channels } from "./channels.ts";
import { envelope } from "./transport.ts";

interface FakeContents {
  session: object;
  mainFrame: { url: string };
  url: string;
  throttled: boolean;
  send: ReturnType<typeof vi.fn>;
  emit: (event: string, ...args: unknown[]) => boolean;
  listenerCount: (event: string) => number;
}
interface FakeWindow {
  webContents: FakeContents;
  options: Record<string, unknown>;
  visible: boolean;
  minimized: boolean;
  focused: boolean;
  destroyed: boolean;
  showInactive: ReturnType<typeof vi.fn>;
  emit: (event: string, ...args: unknown[]) => boolean;
  listenerCount: (event: string) => number;
}
const native = vi.hoisted(() => ({
  windows: [] as FakeWindow[],
  handlers: new Map<string, (event: IpcMainInvokeEvent, raw: unknown) => unknown>(),
  preferences: null as unknown,
  writes: [] as unknown[],
  protocols: [] as { handle: ReturnType<typeof vi.fn>; unhandle: ReturnType<typeof vi.fn> }[],
  app: null as unknown as {
    emit: (event: string) => boolean;
    listenerCount: (event: string) => number;
  },
  screen: null as unknown as { listenerCount: (event: string) => number },
}));
vi.mock("node:fs/promises", () => ({
  lstat: async () => {
    if (native.preferences === null) throw new Error("ENOENT");
    return { size: 100, isFile: () => true };
  },
  readFile: async () => JSON.stringify(native.preferences),
  mkdir: async () => undefined,
  open: async () => ({
    writeFile: async (data: string) => {
      native.writes.push(JSON.parse(data));
    },
    close: async () => undefined,
  }),
  rename: async () => undefined,
  rm: async () => undefined,
}));
vi.mock("electron", async () => {
  const { EventEmitter } = await import("node:events");
  const display = { id: 1, workArea: { x: 0, y: 24, width: 1920, height: 1056 } };
  class Contents extends EventEmitter {
    session: object;
    url = "t3code://app/";
    mainFrame = { url: this.url };
    throttled = false;
    send = vi.fn();
    constructor(session: object) {
      super();
      this.session = session;
    }
    isDestroyed() {
      return false;
    }
    getURL() {
      return this.url;
    }
    getBackgroundThrottling() {
      return this.throttled;
    }
    setBackgroundThrottling(value: boolean) {
      this.throttled = value;
    }
    setWindowOpenHandler = vi.fn();
  }
  class Window extends EventEmitter {
    webContents: Contents;
    visible = false;
    minimized = false;
    focused = false;
    destroyed = false;
    options: Record<string, unknown>;
    bounds = { x: 0, y: 0, width: 46, height: 128 };
    constructor(options: Record<string, unknown> = {}) {
      super();
      this.options = options;
      const preferences = options.webPreferences as { session?: object } | undefined;
      this.webContents = new Contents(preferences?.session ?? {});
      native.windows.push(this);
    }
    isDestroyed() {
      return this.destroyed;
    }
    isVisible() {
      return this.visible;
    }
    isMinimized() {
      return this.minimized;
    }
    isFocused() {
      return this.focused;
    }
    setMenu = vi.fn();
    setVisibleOnAllWorkspaces = vi.fn();
    loadURL = async (url: string) => {
      this.webContents.url = url;
      this.webContents.mainFrame.url = url;
    };
    showInactive = vi.fn(() => {
      this.visible = true;
      this.emit("show");
    });
    hide() {
      this.visible = false;
      this.emit("hide");
    }
    destroy() {
      this.destroyed = true;
      this.emit("closed");
    }
    setBounds(bounds: typeof this.bounds) {
      this.bounds = bounds;
    }
    getBounds() {
      return this.bounds;
    }
  }
  const app = new EventEmitter();
  const screen = Object.assign(new EventEmitter(), {
    getAllDisplays: () => [display],
    getPrimaryDisplay: () => display,
    getDisplayMatching: () => display,
  });
  native.app = app;
  native.screen = screen;
  return {
    BrowserWindow: Window,
    app,
    screen,
    ipcMain: {
      handle: (channel: string, callback: (event: IpcMainInvokeEvent, raw: unknown) => unknown) => {
        if (native.handlers.has(channel)) throw new Error("Duplicate IPC handler");
        native.handlers.set(channel, callback);
      },
      removeHandler: (channel: string) => native.handlers.delete(channel),
    },
    session: {
      fromPartition: (partition: string) => {
        const protocol = { handle: vi.fn(), unhandle: vi.fn() };
        native.protocols.push(protocol);
        return Object.assign(new EventEmitter(), {
          partition,
          protocol,
          setPermissionRequestHandler: vi.fn(),
          setPermissionCheckHandler: vi.fn(),
          setDevicePermissionHandler: vi.fn(),
          webRequest: { onBeforeRequest: vi.fn() },
        });
      },
    },
    net: { fetch: vi.fn() },
  };
});

const hosts: ThreadMonitorHost[] = [];
async function setup(preferences: unknown = null) {
  native.preferences = preferences;
  native.windows.length = 0;
  native.writes.length = 0;
  native.protocols.length = 0;
  const { BrowserWindow } = await import("electron");
  const main = new BrowserWindow({ show: false });
  const revealMain = vi.fn();
  const host = new ThreadMonitorHost({
    main,
    bundleDirectory: "/isolated/bundle",
    stateDirectory: "/isolated/state",
    platform: "linux",
    scheme: "t3code",
    assetDirectory: "/isolated/assets",
    developmentOrigin: null,
    backend: { supported: true, reason: null },
    revealMain,
  });
  hosts.push(host);
  // Drain only the bounded preference-read promise chain; no polling, files, processes or real timers.
  for (let i = 0; i < 5; i++) await Promise.resolve();
  const mainWindow = native.windows[0]!;
  const peer = native.windows[1]!;
  const event = (window: FakeWindow): IpcMainInvokeEvent =>
    ({
      sender: window.webContents,
      senderFrame: window.webContents.mainFrame,
    }) as unknown as IpcMainInvokeEvent;
  const invoke = (channel: string, payload: unknown, window = mainWindow) => {
    const callback = native.handlers.get(channel);
    if (!callback) throw new Error(`Missing ${channel}`);
    return callback(event(window), envelope(payload));
  };
  await invoke(channels.publish, { type: "reset", publisherGeneration: "generation-1" });
  return { host, mainWindow, peer, invoke, revealMain, main: main as BrowserWindow };
}
afterEach(() => {
  for (const host of hosts.splice(0)) host.dispose();
  vi.useRealTimers();
});

describe("monitor host lifecycle", () => {
  it("shows the independent sandbox peer without revealing main and keeps real activity during main minimize", async () => {
    const h = await setup();
    expect(h.peer.options.parent).toBeUndefined();
    expect(h.peer.options).toMatchObject({
      frame: false,
      width: 46,
      height: 128,
      skipTaskbar: true,
      alwaysOnTop: true,
    });
    expect(h.peer.options.webPreferences).toMatchObject({
      sandbox: true,
      contextIsolation: true,
      nodeIntegration: false,
      webviewTag: false,
    });
    expect((h.peer.options.webPreferences as { session: object }).session).not.toBe(
      h.mainWindow.webContents.session,
    );
    await h.invoke(channels.dispatch, { type: "ready" }, h.peer);
    expect(h.peer.showInactive).toHaveBeenCalledTimes(1);
    expect(h.revealMain).not.toHaveBeenCalled();
    h.host.setMainBackgroundThrottling(true);
    expect(h.mainWindow.webContents.throttled).toBe(false);
    h.mainWindow.visible = true;
    h.mainWindow.focused = true;
    h.mainWindow.emit("focus");
    expect(h.host.state()).toMatchObject({ mainVisible: true, mainFocused: true });
    h.mainWindow.minimized = true;
    h.mainWindow.emit("minimize");
    expect(h.host.state()).toMatchObject({ visible: true, mainVisible: false, mainFocused: false });
    expect(h.mainWindow.webContents.send).toHaveBeenLastCalledWith(
      channels.state,
      expect.objectContaining({
        payload: expect.objectContaining({ mainVisible: false, mainFocused: false }),
      }),
    );
    await h.invoke(channels.dispatch, { type: "setExpanded", expanded: true }, h.peer);
    expect(h.revealMain).not.toHaveBeenCalled();
  });

  it("honors saved hidden preference, persists only safe show/hide geometry and never sends a run-control intent", async () => {
    const h = await setup({ enabled: false, displayId: 1, vertical: 0.2 });
    await h.invoke(channels.dispatch, { type: "ready" }, h.peer);
    expect(h.host.state().visible).toBe(false);
    await h.invoke(channels.setEnabled, true);
    expect(h.host.state().visible).toBe(true);
    h.host.setMainBackgroundThrottling(true);
    h.mainWindow.webContents.send.mockClear();
    await h.invoke(channels.dispatch, { type: "hide" }, h.peer);
    expect(h.host.state()).toMatchObject({ enabled: false, visible: false, expanded: false });
    expect(h.mainWindow.webContents.throttled).toBe(true);
    expect(native.writes.at(-1)).toEqual({ enabled: false, displayId: 1, vertical: 0.2 });
    expect(
      h.mainWindow.webContents.send.mock.calls.some(([channel]) => channel === channels.intent),
    ).toBe(false);
  });

  it("rejects peer use of publisher IPC and wrong document/frame/session before any effect", async () => {
    const h = await setup();
    expect(() => h.invoke(channels.setEnabled, false, h.peer)).toThrow();
    expect(() =>
      h.invoke(channels.publish, { type: "reset", publisherGeneration: "wrong" }, h.peer),
    ).toThrow();
    const handler = native.handlers.get(channels.dispatch)!;
    for (const event of [
      { sender: h.peer.webContents, senderFrame: { url: h.peer.webContents.url } },
      {
        sender: { ...h.peer.webContents, session: h.mainWindow.webContents.session },
        senderFrame: h.peer.webContents.mainFrame,
      },
    ])
      expect(() =>
        handler(event as unknown as IpcMainInvokeEvent, envelope({ type: "hide" })),
      ).toThrow();
    expect(h.host.state().enabled).toBe(true);
  });

  it("removes peer, IPC and owned listeners when main closes; main reload binds a fresh peer generation", async () => {
    const h = await setup();
    await h.invoke(channels.dispatch, { type: "ready" }, h.peer);
    h.mainWindow.webContents.emit("did-start-navigation", {}, "t3code://app/", false, true, 1, 1);
    expect(h.peer.destroyed).toBe(true);
    expect(native.protocols[0]!.unhandle).toHaveBeenCalledWith("t3code");
    await h.invoke(channels.publish, { type: "reset", publisherGeneration: "generation-2" });
    expect(native.windows).toHaveLength(3);
    const replacement = native.windows[2]!;
    expect(() => h.invoke(channels.dispatch, { type: "ready" }, h.peer)).toThrow();
    await h.invoke(channels.dispatch, { type: "ready" }, replacement);
    h.mainWindow.emit("closed");
    expect(replacement.destroyed).toBe(true);
    expect(native.handlers.size).toBe(0);
    expect(native.app.listenerCount("before-quit")).toBe(0);
    expect(native.screen.listenerCount("display-metrics-changed")).toBe(0);
    expect(h.mainWindow.listenerCount("focus")).toBe(0);
  });
});
