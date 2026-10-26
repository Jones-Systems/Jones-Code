// @effect-diagnostics nodeBuiltinImport:off - In-memory Electron lifecycle doubles; no host windows or files are created.
import type { BrowserWindow, IpcMainInvokeEvent } from "electron";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { ThreadMonitorHost } from "./host.ts";
import { MONITOR_CHANNELS as channels } from "./channels.ts";
import { envelope } from "./transport.ts";
import { monitorBackend } from "./policy.ts";

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
  loadURL: ReturnType<typeof vi.fn>;
  getBounds: () => { x: number; y: number; width: number; height: number };
  emit: (event: string, ...args: unknown[]) => boolean;
  listenerCount: (event: string) => number;
}
const native = vi.hoisted(() => ({
  windows: [] as FakeWindow[],
  handlers: new Map<string, (event: IpcMainInvokeEvent, raw: unknown) => unknown>(),
  preferences: null as unknown,
  loadFailures: 0,
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
    loadURL = vi.fn(async (url: string) => {
      this.webContents.url = url;
      this.webContents.mainFrame.url = url;
      if (native.loadFailures > 0) {
        native.loadFailures--;
        throw new Error("Transient monitor asset failure");
      }
    });
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
async function setup(
  preferences: unknown = null,
  options: { loadFailures?: number; backend?: ReturnType<typeof monitorBackend> } = {},
) {
  native.preferences = preferences;
  native.loadFailures = options.loadFailures ?? 0;
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
    backend: options.backend ?? { supported: true, reason: null },
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

const lifetime = {
  publisherGeneration: "generation-1",
  environmentId: "environment-1",
  environmentEpoch: 0,
};
const ref = { ...lifetime, threadId: "thread-1" };
const available = { available: true, reason: null };
const snapshot = {
  type: "view",
  publisherGeneration: ref.publisherGeneration,
  snapshotId: "snapshot-1",
  revision: 1,
  chunkIndex: 0,
  chunkCount: 1,
  complete: true,
  replace: true,
  environments: [
    { ...lifetime, label: "Synthetic", transport: "connected", freshness: "live", reason: null },
  ],
  upserts: [
    {
      ...ref,
      title: "Draft target",
      projectLabel: "Synthetic",
      group: "waiting",
      status: "idle",
      statusLabel: "Idle",
      updatedAt: "2026-10-26T07:00:00Z",
      completedAt: null,
      completionTimeUnavailable: false,
      pendingChildAttention: null,
      actions: {
        reply: available,
        settle: available,
        respondToApproval: available,
        respondToUserInput: available,
        usePlan: available,
        restore: available,
      },
    },
  ],
  removed: [],
  counts: { needsYou: 0, working: 0, waiting: 1, done: 0 },
};
const draftFrame = {
  type: "draft",
  draft: { ...ref, kind: "reply", requestId: null, revision: 2, text: "kept draft", answers: {} },
};
const viewFrame = {
  type: "viewState",
  state: {
    publisherGeneration: ref.publisherGeneration,
    revision: 1,
    collapsed: { needsYou: true, working: false, waiting: false, done: true },
    list: { scrollTop: 120, anchors: [{ key: "section:waiting", offset: -12 }] },
    detail: { scrollTop: 0, anchors: [] },
    editor: { ref, quick: true, focused: true, selectionStart: 2, selectionEnd: 5 },
  },
};
const send = {
  ...ref,
  type: "send",
  actionId: "original-action",
  commandId: "original-command",
  messageId: "original-message",
  draftRevision: 1,
  text: "submitted before peer failure",
};
const pendingResult = {
  ...ref,
  type: "actionResult",
  actionId: send.actionId,
  commandId: send.commandId,
  status: "pending",
  reason: "Awaiting command acceptance",
  submittedDraftRevision: send.draftRevision,
};

describe("monitor host lifecycle", () => {
  it.each(["peer", "toolbar"] as const)(
    "retains the peer's expanded bounds and focused-draft view contract through %s hide and toolbar show",
    async (control) => {
      const h = await setup();
      await h.invoke(channels.dispatch, { type: "ready" }, h.peer);
      for (const frame of [snapshot, draftFrame, viewFrame])
        await h.invoke(channels.publish, frame);
      const peerExpansion = { type: "setExpanded", expanded: true };
      await h.invoke(channels.dispatch, peerExpansion, h.peer);
      const bounds = h.peer.getBounds();
      expect(bounds).toMatchObject({ width: 384, height: 704 });
      const peerFrames = h.peer.webContents.send.mock.calls.slice();
      h.mainWindow.webContents.send.mockClear();
      h.mainWindow.minimized = true;
      if (control === "peer") await h.invoke(channels.dispatch, { type: "hide" }, h.peer);
      else await h.invoke(channels.setEnabled, false);
      expect(h.host.state()).toMatchObject({ enabled: false, visible: false, expanded: true });
      expect(h.peer.getBounds()).toEqual(bounds);
      await h.invoke(channels.setEnabled, true);
      expect(h.host.state()).toMatchObject({
        enabled: true,
        visible: true,
        expanded: peerExpansion.expanded,
        mainVisible: false,
        mainFocused: false,
      });
      expect(h.peer.getBounds()).toEqual(bounds);
      expect(native.windows).toHaveLength(2);
      expect(h.peer.webContents.send.mock.calls).toEqual(peerFrames);
      expect(peerFrames).toContainEqual([channels.frame, envelope(draftFrame)]);
      expect(peerFrames).toContainEqual([channels.frame, envelope(viewFrame)]);
      expect(h.revealMain).not.toHaveBeenCalled();
      expect(h.mainWindow.webContents.send.mock.calls.some(([c]) => c === channels.intent)).toBe(
        false,
      );
      await h.invoke(channels.dispatch, { type: "setExpanded", expanded: false }, h.peer);
      expect(h.peer.getBounds()).toMatchObject({ width: 46, height: 128 });
      await h.invoke(channels.dispatch, peerExpansion, h.peer);
      expect(h.peer.getBounds()).toEqual(bounds);
    },
  );

  it("keeps crash recovery supported, recreates only the peer and replays the same draft/view/action lifetime", async () => {
    vi.useFakeTimers();
    const h = await setup();
    await h.invoke(channels.dispatch, { type: "ready" }, h.peer);
    await h.invoke(channels.publish, snapshot);
    await h.invoke(channels.dispatch, send, h.peer);
    for (const frame of [draftFrame, viewFrame, pendingResult])
      await h.invoke(channels.publish, frame);
    h.mainWindow.minimized = true;
    h.peer.webContents.emit("render-process-gone", {}, { reason: "crashed", exitCode: 1 });
    expect(h.peer.destroyed).toBe(true);
    expect(h.host.state()).toMatchObject({ supported: true, enabled: true, visible: false });
    expect(h.host.state().reason).toContain("recover");
    expect(h.mainWindow.destroyed).toBe(false);
    await h.invoke(channels.publish, {
      type: "reset",
      publisherGeneration: ref.publisherGeneration,
    });
    expect(native.windows).toHaveLength(2);
    const recovered = await h.invoke(channels.setEnabled, true);
    expect(recovered).toEqual(envelope(h.host.state()));
    expect(h.host.state()).toMatchObject({ supported: true, reason: null, visible: false });
    expect(native.windows).toHaveLength(3);
    const replacement = native.windows[2]!;
    expect(replacement.webContents.session).not.toBe(h.peer.webContents.session);
    for (const intent of [{ type: "ready" }, { type: "hide" }, send])
      expect(() => h.invoke(channels.dispatch, intent, h.peer)).toThrow();
    await h.invoke(channels.dispatch, { type: "ready" }, replacement);
    expect(h.mainWindow.webContents.send).toHaveBeenLastCalledWith(
      channels.state,
      expect.objectContaining({ payload: expect.objectContaining({ visible: true }) }),
    );
    expect(h.mainWindow.webContents.send.mock.calls).toContainEqual([
      channels.intent,
      envelope({ type: "ready" }),
    ]);
    for (const frame of [snapshot, draftFrame, viewFrame, pendingResult])
      await h.invoke(channels.publish, frame);
    for (const frame of [draftFrame, viewFrame, pendingResult])
      expect(replacement.webContents.send.mock.calls).toContainEqual([
        channels.frame,
        envelope(frame),
      ]);
    await h.invoke(
      channels.dispatch,
      { type: "setExpanded", expanded: viewFrame.state.editor.focused },
      replacement,
    );
    expect(replacement.getBounds()).toMatchObject({ width: 384, height: 704 });
    const sends = () =>
      h.mainWindow.webContents.send.mock.calls.filter(
        ([channel, frame]) => channel === channels.intent && frame.payload.type === "send",
      );
    await h.invoke(channels.dispatch, send, replacement);
    expect(sends()).toHaveLength(1);
    const accepted = { ...pendingResult, status: "accepted", reason: null };
    await h.invoke(channels.publish, accepted);
    expect(replacement.webContents.send).toHaveBeenLastCalledWith(
      channels.frame,
      envelope(accepted),
    );
    await expect(
      h.invoke(channels.dispatch, { ...send, environmentEpoch: 1, actionId: "late" }, replacement),
    ).rejects.toThrow("Monitor request was rejected");
    expect(h.host.state()).toMatchObject({ mainVisible: false, mainFocused: false });
    expect(h.revealMain).not.toHaveBeenCalled();
  });

  it("allows a bounded explicit retry after a transient initial load failure and fences the failed sender", async () => {
    const h = await setup(null, { loadFailures: 1 });
    await expect(h.peer.loadURL.mock.results[0]!.value).rejects.toThrow(
      "Transient monitor asset failure",
    );
    expect(h.peer.destroyed).toBe(true);
    expect(h.host.state()).toMatchObject({ supported: true, enabled: true, visible: false });
    expect(h.host.state().reason).toContain("could not load");
    expect(native.windows).toHaveLength(2);
    for (const frame of [snapshot, draftFrame, viewFrame]) await h.invoke(channels.publish, frame);
    expect(native.windows).toHaveLength(2);
    await h.invoke(channels.setEnabled, true);
    const replacement = native.windows[2]!;
    expect(h.host.state()).toMatchObject({ supported: true, reason: null, visible: false });
    for (const intent of [{ type: "ready" }, { type: "hide" }, send])
      expect(() => h.invoke(channels.dispatch, intent, h.peer)).toThrow();
    await h.invoke(channels.dispatch, { type: "ready" }, replacement);
    for (const frame of [snapshot, draftFrame, viewFrame]) await h.invoke(channels.publish, frame);
    expect(replacement.webContents.send.mock.calls).toContainEqual([
      channels.frame,
      envelope(draftFrame),
    ]);
    expect(replacement.webContents.send.mock.calls).toContainEqual([
      channels.frame,
      envelope(viewFrame),
    ]);
    expect(h.host.state().visible).toBe(true);
    expect(native.windows).toHaveLength(3);
    expect(h.mainWindow.destroyed).toBe(false);
    expect(h.revealMain).not.toHaveBeenCalled();
  });

  it("keeps native Wayland unavailable even when enable is requested", async () => {
    const backend = monitorBackend({
      platform: "linux",
      ozonePlatform: "wayland",
      display: ":1",
      waylandDisplay: "wayland-0",
      sessionType: "wayland",
    });
    const h = await setup(null, { backend });
    await h.invoke(channels.setEnabled, true);
    expect(h.host.state()).toMatchObject({
      supported: false,
      visible: false,
      reason: backend.reason,
    });
    expect(native.windows).toHaveLength(1);
  });

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

  it("disposes safely when Electron invalidates the main webContents getter before closed", async () => {
    const h = await setup();
    await h.invoke(channels.dispatch, { type: "ready" }, h.peer);
    h.mainWindow.destroyed = true;
    Object.defineProperty(h.mainWindow, "webContents", {
      configurable: true,
      get() {
        throw new Error("Object has been destroyed");
      },
    });
    expect(() => h.mainWindow.emit("closed")).not.toThrow();
    expect(h.peer.destroyed).toBe(true);
    expect(native.handlers.size).toBe(0);
    expect(native.app.listenerCount("before-quit")).toBe(0);
    expect(() => h.host.setMainBackgroundThrottling(true)).not.toThrow();
    expect(() => h.host.dispose()).not.toThrow();
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
