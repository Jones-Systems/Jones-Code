// @effect-diagnostics nodeBuiltinImport:off - Electron lifecycle adapter owns its bounded geometry preference file.
// @effect-diagnostics globalTimers:off - Task-owned timers are cancelled by dispose and broker retirement.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import * as Electron from "electron";
import type { ThreadMonitorNativeState } from "@t3tools/contracts/jones/desktopThreadMonitor";
import { ThreadMonitorBroker } from "./broker.ts";
import { MONITOR_CHANNELS as channels, MONITOR_ENTRY } from "./channels.ts";
import {
  authorizeMonitorSender,
  monitorBackend,
  monitorBounds,
  monitorVertical,
  readMonitorPreferences,
  type MonitorPreferences,
} from "./policy.ts";
import { registerMonitorProtocol, type MonitorAssetSource } from "./protocol.ts";
import { markThreadMonitorWindow } from "./roles.ts";
import {
  emptyPayload,
  enabledPayload,
  envelope,
  peerPayload,
  publisherPayload,
} from "./transport.ts";
import { monitorThrottleLease } from "./throttle.ts";

let peerSequence = 0;
const preferenceWrites = new Map<string, Promise<void>>();
export interface ThreadMonitorHostOptions extends MonitorAssetSource {
  readonly main: Electron.BrowserWindow;
  readonly bundleDirectory: string;
  readonly stateDirectory: string;
  readonly platform: NodeJS.Platform;
  readonly backend: ReturnType<typeof monitorBackend>;
  readonly revealMain: () => void;
}

/** One host belongs to one main BrowserWindow; its peer is never a child window. */
export class ThreadMonitorHost {
  private readonly options: ThreadMonitorHostOptions;
  private readonly broker: ThreadMonitorBroker;
  private readonly origin: string;
  private readonly entry: string;
  private readonly preferencePath: string;
  private preferences: MonitorPreferences = readMonitorPreferences(null);
  private readonly initialize: Promise<void>;
  private writes: Promise<void> = Promise.resolve();
  private peer: Electron.BrowserWindow | null = null;
  private peerSession: Electron.Session | null = null;
  private unregisterProtocol: (() => void) | null = null;
  private readonly cleanups: Array<() => void> = [];
  private readonly peerCleanups: Array<() => void> = [];
  private disposed = false;
  private preferencesLoaded = false;
  private documentReady = false;
  private peerReady = false;
  private expanded = false;
  private placing = false;
  private fitReason: string | null = null;
  private loadReason: string | null = null;
  private readonly throttle: ReturnType<typeof monitorThrottleLease>;
  private moveTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(options: ThreadMonitorHostOptions) {
    this.options = options;
    this.origin = `${options.scheme}://app`;
    this.entry = `${this.origin}${MONITOR_ENTRY}`;
    this.preferencePath = NodePath.join(options.stateDirectory, "jones-thread-monitor.json");
    this.throttle = monitorThrottleLease(options.main.webContents);
    this.broker = new ThreadMonitorBroker({
      peer: (frame) => {
        if (this.peerReady && this.peer && !this.peer.isDestroyed())
          this.peer.webContents.send(channels.frame, envelope(frame));
      },
      publisher: (intent) => {
        if (
          this.documentReady &&
          !this.disposed &&
          !options.main.isDestroyed() &&
          !options.main.webContents.isDestroyed()
        )
          options.main.webContents.send(channels.intent, envelope(intent));
      },
      recreatePeer: () => {
        this.destroyPeer();
        if (!this.disposed && this.documentReady) this.createPeer();
      },
      revealMain: options.revealMain,
      timer: (callback, milliseconds) => {
        const timer = setTimeout(callback, milliseconds);
        return () => clearTimeout(timer);
      },
    });
    this.installIpc();
    for (const event of ["show", "hide", "minimize", "restore", "focus", "blur"] as const)
      this.listen(options.main, event, () => this.emitState());
    this.listen(options.main, "closed", () => this.dispose());
    this.listen(
      options.main.webContents,
      "did-start-navigation",
      (_event, _url, _inPlace, isMainFrame, _processId, _routingId) => {
        if (isMainFrame && !_inPlace) this.publisherLost();
      },
    );
    this.listen(options.main.webContents, "render-process-gone", () => this.publisherLost());
    for (const event of ["display-added", "display-removed", "display-metrics-changed"] as const)
      this.listen(Electron.screen, event, () => {
        this.createPeer();
        this.place();
        this.showIfReady();
        this.emitState();
      });
    this.listen(Electron.app, "before-quit", () => this.dispose());
    this.initialize = this.loadPreferences();
  }

  private listen(
    target: NodeJS.EventEmitter,
    event: string,
    listener: (...args: unknown[]) => void,
  ): void {
    target.on(event, listener);
    this.cleanups.push(() => target.removeListener(event, listener));
  }

  private async loadPreferences(): Promise<void> {
    try {
      await preferenceWrites.get(this.preferencePath)?.catch(() => undefined);
      const stat = await NodeFSP.lstat(this.preferencePath);
      if (stat.isFile() && stat.size <= 4_096)
        this.preferences = readMonitorPreferences(
          JSON.parse(await NodeFSP.readFile(this.preferencePath, "utf8")),
        );
    } catch {
      /* Absent or corrupt geometry never changes the default-visible preference. */
    }
    this.preferencesLoaded = true;
    if (!this.disposed) {
      this.createPeer();
      this.emitState();
    }
  }

  private persist(): Promise<void> {
    const preferences = this.preferences;
    const write = (preferenceWrites.get(this.preferencePath) ?? this.writes)
      .catch(() => undefined)
      .then(async () => {
        await NodeFSP.mkdir(this.options.stateDirectory, { recursive: true, mode: 0o700 });
        const temporary = `${this.preferencePath}.${NodeCrypto.randomBytes(8).toString("hex")}.tmp`;
        try {
          const file = await NodeFSP.open(temporary, "wx", 0o600);
          try {
            await file.writeFile(JSON.stringify(preferences), "utf8");
          } finally {
            await file.close();
          }
          await NodeFSP.rename(temporary, this.preferencePath);
        } finally {
          await NodeFSP.rm(temporary, { force: true });
        }
      });
    preferenceWrites.set(this.preferencePath, write);
    void write
      .finally(() => {
        if (preferenceWrites.get(this.preferencePath) === write)
          preferenceWrites.delete(this.preferencePath);
      })
      .catch(() => undefined);
    this.writes = write;
    return write;
  }

  state(): ThreadMonitorNativeState {
    const main = this.options.main;
    const mainVisible = !main.isDestroyed() && main.isVisible() && !main.isMinimized();
    return {
      // Peer failures remain retryable through the toolbar on a supported backend/work area.
      supported: this.options.backend.supported && this.fitReason === null,
      enabled: this.preferences.enabled,
      visible: !!this.peer && !this.peer.isDestroyed() && this.peer.isVisible(),
      mainVisible,
      mainFocused: mainVisible && main.isFocused(),
      expanded: this.expanded,
      reason: this.options.backend.reason ?? this.fitReason ?? this.loadReason,
    };
  }

  private emitState(): void {
    if (
      this.disposed ||
      this.options.main.isDestroyed() ||
      this.options.main.webContents.isDestroyed()
    )
      return;
    this.options.main.webContents.send(channels.state, envelope(this.state()));
  }

  /** First reveal changes the normal policy, without overriding a visible monitor's lease. */
  setMainBackgroundThrottling(enabled: boolean): void {
    if (!this.options.main.isDestroyed() && !this.options.main.webContents.isDestroyed())
      this.throttle.normal(enabled);
  }

  private applyThrottle(): void {
    if (this.options.main.isDestroyed() || this.options.main.webContents.isDestroyed()) return;
    const visible = !!this.peer && !this.peer.isDestroyed() && this.peer.isVisible();
    this.throttle.visible(visible);
  }

  private authorize(event: Electron.IpcMainInvokeEvent, role: "main" | "peer"): void {
    const window = role === "main" ? this.options.main : this.peer;
    if (this.disposed || !window || window.isDestroyed() || (role === "peer" && !this.peerSession))
      throw new Error("Unavailable monitor sender");
    const contents = window.webContents;
    if (
      !authorizeMonitorSender({
        sender: event.sender,
        expectedSender: contents,
        frame: event.senderFrame,
        mainFrame: contents.mainFrame,
        session: event.sender.session,
        expectedSession: role === "main" ? this.options.main.webContents.session : this.peerSession,
        frameUrl: event.senderFrame?.url ?? "",
        contentsUrl: contents.getURL(),
        expectedOrigin: this.origin,
        expectedEntry: role === "peer" ? MONITOR_ENTRY : null,
        destroyed: contents.isDestroyed(),
      }) ||
      (role === "main" && new URL(contents.getURL()).pathname === MONITOR_ENTRY)
    )
      throw new Error("Unauthorized monitor sender");
  }

  private installIpc(): void {
    const handle = (
      channel: string,
      role: "main" | "peer",
      listener: (raw: unknown, authorize: () => void) => unknown,
    ) => {
      Electron.ipcMain.handle(channel, (event, raw: unknown) => {
        const senderFrame = event.senderFrame;
        const authorize = () => {
          if (event.senderFrame !== senderFrame) throw new Error("Monitor sender document changed");
          this.authorize(event, role);
        };
        authorize();
        // Errors deliberately omit payloads, drafts and schema diagnostics from Electron logs.
        try {
          const result = listener(raw, authorize);
          if (result instanceof Promise)
            return result.catch(() => {
              throw new Error("Monitor request was rejected");
            });
          return result;
        } catch {
          throw new Error("Monitor request was rejected");
        }
      });
      this.cleanups.push(() => Electron.ipcMain.removeHandler(channel));
    };
    handle(channels.getState, "main", (raw) => {
      emptyPayload(raw);
      return envelope(this.state());
    });
    handle(channels.setEnabled, "main", async (raw, authorize) => {
      const enabled = enabledPayload(raw);
      await this.initialize;
      authorize();
      if (this.disposed) throw new Error("Monitor is closed");
      this.preferences = { ...this.preferences, enabled };
      if (enabled) {
        this.loadReason = null;
        this.createPeer();
        this.showIfReady();
      } else this.hide();
      await this.persist();
      this.emitState();
      return envelope(this.state());
    });
    handle(channels.publish, "main", (raw) => {
      const frame = publisherPayload(raw);
      if (frame.type === "reset") this.documentReady = true;
      this.broker.publish(frame);
      if (frame.type === "reset") this.createPeer();
    });
    handle(channels.invalidate, "main", async (raw, authorize) => {
      const frame = publisherPayload(raw);
      if (frame.type !== "invalidateEnvironment") throw new Error("Invalid retirement control");
      const acknowledgment = await this.broker.invalidate(frame);
      authorize();
      return envelope(acknowledgment);
    });
    handle(channels.dispatch, "peer", async (raw) => {
      const intent = peerPayload(raw);
      if (intent.type === "hide") {
        this.preferences = { ...this.preferences, enabled: false };
        this.hide();
        await this.persist();
        this.emitState();
        return;
      }
      if (intent.type === "setExpanded") {
        this.expanded = intent.expanded;
        this.place();
        this.emitState();
        return;
      }
      if (intent.type === "ready") {
        this.peerReady = true;
        this.broker.ready();
        this.showIfReady();
        this.emitState();
        return;
      }
      this.broker.intent(intent);
    });
  }

  private createPeer(): void {
    try {
      this.createPeerWindow();
    } catch {
      this.destroyPeer();
      this.loadReason = "Thread monitor window is unavailable. Toggle the monitor to retry.";
      this.emitState();
    }
  }

  private createPeerWindow(): void {
    if (
      this.disposed ||
      !this.preferencesLoaded ||
      !this.options.backend.supported ||
      this.loadReason !== null ||
      this.peer
    )
      return;
    const display = this.display();
    const bounds = monitorBounds(display.workArea, false, this.preferences.vertical);
    if (!bounds) {
      this.fitReason = "Thread monitor needs a display work area at least 320 × 128 DIP.";
      return;
    }
    this.fitReason = null;
    const partition = `jones-thread-monitor-${++peerSequence}`;
    const session = Electron.session.fromPartition(partition, { cache: false });
    this.unregisterProtocol = registerMonitorProtocol(session, this.options);
    this.peerSession = session;
    this.expanded = false;
    const peer = new Electron.BrowserWindow({
      ...bounds,
      show: false,
      frame: false,
      resizable: false,
      maximizable: false,
      minimizable: false,
      fullscreenable: false,
      skipTaskbar: true,
      alwaysOnTop: true,
      hasShadow: true,
      title: "Jones Code Thread Monitor",
      backgroundColor: "#17171c",
      autoHideMenuBar: true,
      webPreferences: {
        session,
        preload: NodePath.join(this.options.bundleDirectory, "jones/threadMonitor/preload.cjs"),
        sandbox: true,
        contextIsolation: true,
        nodeIntegration: false,
        nodeIntegrationInWorker: false,
        nodeIntegrationInSubFrames: false,
        webviewTag: false,
        webSecurity: true,
        allowRunningInsecureContent: false,
        backgroundThrottling: false,
        spellcheck: false,
        safeDialogs: true,
        disableDialogs: true,
      },
    });
    const bind = <Args extends unknown[]>(
      target: NodeJS.EventEmitter,
      event: string,
      listener: (...args: Args) => void,
    ) => {
      const callback = listener as (...args: unknown[]) => void;
      target.on(event, callback);
      this.peerCleanups.push(() => target.removeListener(event, callback));
    };
    this.peer = peer;
    markThreadMonitorWindow(peer);
    peer.setMenu(null);
    if (this.options.platform === "darwin")
      peer.setVisibleOnAllWorkspaces(true, { visibleOnFullScreen: true });
    peer.webContents.setWindowOpenHandler(() => ({ action: "deny" }));
    bind(peer.webContents, "will-navigate", (event: Electron.Event) => event.preventDefault());
    bind(peer.webContents, "will-redirect", (event: Electron.Event) => event.preventDefault());
    bind(peer.webContents, "will-attach-webview", (event: Electron.Event) =>
      event.preventDefault(),
    );
    bind(
      peer.webContents,
      "will-frame-navigate",
      (event: Electron.Event & { isMainFrame: boolean; url: string }) => {
        if (this.peerReady || !event.isMainFrame || event.url !== this.entry)
          event.preventDefault();
      },
    );
    bind(
      peer.webContents,
      "did-start-navigation",
      (_event: Electron.Event, _url: string, inPlace: boolean, mainFrame: boolean) => {
        if (!inPlace && mainFrame && this.peerReady) {
          this.destroyPeer();
          this.createPeer();
          this.emitState();
        }
      },
    );
    bind(peer.webContents, "render-process-gone", () => {
      if (!this.disposed && this.peer === peer) {
        this.destroyPeer();
        this.loadReason = "Thread monitor renderer stopped. Toggle the monitor to recover.";
        this.emitState();
      }
    });
    bind(peer, "move", () => this.moved());
    bind(peer, "moved", () => this.moved());
    bind(peer, "show", () => {
      this.applyThrottle();
      this.emitState();
    });
    bind(peer, "hide", () => {
      this.applyThrottle();
      this.emitState();
    });
    bind(peer, "closed", () => {
      if (this.peer === peer) {
        this.destroyPeer();
        this.emitState();
      }
    });
    void peer.loadURL(this.entry).catch(() => {
      if (this.peer !== peer || this.disposed) return;
      this.loadReason = "Thread monitor assets could not load. Toggle the monitor to retry.";
      this.destroyPeer();
      this.emitState();
    });
  }

  private display(): Electron.Display {
    return (
      Electron.screen
        .getAllDisplays()
        .find((display) => display.id === this.preferences.displayId) ??
      Electron.screen.getPrimaryDisplay()
    );
  }

  private place(): void {
    const display = this.display();
    const bounds = monitorBounds(display.workArea, this.expanded, this.preferences.vertical);
    this.fitReason = bounds
      ? null
      : "Thread monitor needs a display work area at least 320 × 128 DIP.";
    if (!this.peer || this.peer.isDestroyed()) return;
    if (!bounds) {
      this.peer.hide();
      return;
    }
    const current = this.peer.getBounds();
    if (
      current.x === bounds.x &&
      current.y === bounds.y &&
      current.width === bounds.width &&
      current.height === bounds.height
    )
      return;
    this.placing = true;
    try {
      this.peer.setBounds(bounds, false);
    } finally {
      this.placing = false;
    }
  }

  private moved(): void {
    if (this.placing || !this.peer || this.peer.isDestroyed()) return;
    if (this.moveTimer) clearTimeout(this.moveTimer);
    this.moveTimer = setTimeout(() => {
      this.moveTimer = null;
      if (!this.peer || this.peer.isDestroyed()) return;
      const bounds = this.peer.getBounds();
      const display = Electron.screen.getDisplayMatching(bounds);
      this.preferences = {
        ...this.preferences,
        displayId: display.id,
        vertical: monitorVertical(bounds, display.workArea),
      };
      this.place();
      void this.persist().catch(() => {
        this.loadReason = "Thread monitor placement could not be saved.";
        this.emitState();
      });
    }, 180);
  }

  private showIfReady(): void {
    if (
      !this.disposed &&
      this.preferences.enabled &&
      this.peerReady &&
      this.peer &&
      !this.peer.isDestroyed() &&
      this.fitReason === null &&
      this.loadReason === null
    ) {
      // Passive creation and hover never restore, show, or focus the main window.
      if (!this.peer.isVisible()) this.peer.showInactive();
      this.applyThrottle();
    }
  }

  private hide(): void {
    // The surviving renderer keeps its expansion and editor state while hidden.
    if (this.peer && !this.peer.isDestroyed()) this.peer.hide();
    this.applyThrottle();
  }

  private publisherLost(): void {
    this.documentReady = false;
    this.destroyPeer();
    this.broker.publisherLost();
    // Keep the ambient surface available, with an empty unactionable peer until replay.
    this.createPeer();
    this.emitState();
  }

  private destroyPeer(): void {
    const peer = this.peer;
    this.peer = null;
    this.peerReady = false;
    if (this.moveTimer) {
      clearTimeout(this.moveTimer);
      this.moveTimer = null;
    }
    for (const cleanup of this.peerCleanups.splice(0).toReversed()) cleanup();
    if (peer && !peer.isDestroyed()) peer.destroy();
    this.unregisterProtocol?.();
    this.unregisterProtocol = null;
    this.peerSession = null;
    this.broker.peerLost();
    this.applyThrottle();
  }

  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.destroyPeer();
    this.broker.dispose();
    for (const cleanup of this.cleanups.splice(0).toReversed()) cleanup();
    if (!this.options.main.isDestroyed() && !this.options.main.webContents.isDestroyed())
      this.throttle.dispose();
  }
}

export function attachThreadMonitor(
  options: Omit<ThreadMonitorHostOptions, "backend"> & {
    readonly hostEnvironment: NodeJS.ProcessEnv;
  },
): ThreadMonitorHost {
  // Environment and explicit Ozone evidence are conservative intent signals, not an OS-proof claim.
  const backend = monitorBackend({
    platform: options.platform,
    ozonePlatform: Electron.app.commandLine.getSwitchValue("ozone-platform"),
    display: options.hostEnvironment.DISPLAY,
    waylandDisplay: options.hostEnvironment.WAYLAND_DISPLAY,
    sessionType: options.hostEnvironment.XDG_SESSION_TYPE,
  });
  return new ThreadMonitorHost({ ...options, backend });
}
