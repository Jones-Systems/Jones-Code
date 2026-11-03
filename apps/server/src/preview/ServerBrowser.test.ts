import * as NodeEvents from "node:events";
import * as NodeVM from "node:vm";
// @effect-diagnostics-next-line nodeBuiltinImport:off - Playwright download mocks perform real file operations outside the Effect runtime.
import * as NodeFSP from "node:fs/promises";

import * as NodeServices from "@effect/platform-node/NodeServices";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { it } from "@effect/vitest";
import {
  type BrowserRecordingFrameRate,
  type CompanionDown,
  type CompanionUp,
  EnvironmentId,
  PreviewTabId,
  ProviderInstanceId,
  ThreadId,
  type PreviewAutomationSnapshot,
  type PreviewAutomationStatus,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import type { BrowserContext, CDPSession, Page } from "playwright-core";
import { beforeEach, expect, vi } from "vite-plus/test";

import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as Broker from "../mcp/PreviewAutomationBroker.ts";
import * as DesktopChannel from "./DesktopBrowserChannel.ts";
import * as Manager from "./Manager.ts";
import * as PreviewAgentActivity from "./PreviewAgentActivity.ts";
import * as ServerBrowser from "./ServerBrowser.ts";
import * as ServerBrowserPage from "./ServerBrowserPage.ts";
import * as PreviewBrowser from "./PreviewBrowser.ts";
import * as Placement from "../jones/previewCompanion/RenderPlacement.ts";
import * as Selection from "../jones/previewCompanion/RenderHostSelection.ts";
import * as CompanionRegistration from "../jones/previewCompanion/registration.ts";
import * as CompanionRegistry from "../jones/previewCompanion/CompanionHostRegistry.ts";

vi.mock("./ServerBrowserContexts.ts", () => ({
  presentAsChrome: async () => {},
  ServerBrowserContexts: class {
    private readonly onClose: ((context: BrowserContext) => void) | undefined;
    constructor(options: { onContextClose?: (context: BrowserContext) => void }) {
      this.onClose = options.onContextClose;
    }
    async contextFor(profileId: string, isolationKey?: string) {
      contextRequests.push({ profileId, isolated: isolationKey !== undefined });
      if (contextFailure) throw contextFailure;
      await contextGate?.promise;
      const context = makeContext(this.onClose);
      contexts.push(context);
      return context as unknown as BrowserContext;
    }
    async scratchPage() {
      return makeEncoderPage() as unknown as Page;
    }
    async connectDesktopPage(endpoint: string) {
      const context = makeContext(undefined, desktopPageUrl);
      desktopConnections.push({ endpoint, context });
      return { browser: { close: async () => {} }, page: context.page as unknown as Page };
    }
    async close() {
      for (const context of contexts) await context.close();
    }
  },
}));

function makeFrame() {
  const frame = {
    copy: "",
    detached: false,
    isDetached: () => frame.detached,
    recording: Promise.withResolvers<void>(),
  };
  return frame;
}

const PNG_HEADER = (() => {
  const header = Buffer.alloc(24);
  header.write("IHDR", 12);
  header.writeUInt32BE(1280, 16);
  header.writeUInt32BE(800, 20);
  return header.toString("base64");
})();

function makeSession(frame?: ReturnType<typeof makeFrame>) {
  const events = new NodeEvents.EventEmitter();
  return {
    on: vi.fn((name: string, listener: () => void) => events.on(name, listener)),
    emit: (name: string) => events.emit(name),
    detach: vi.fn(async () => {}),
    send: vi.fn(async (method: string, input?: unknown): Promise<Record<string, unknown>> => {
      if (method === "Page.getNavigationHistory") return { currentIndex: 0, entries: [{}] };
      if (method === "Page.getLayoutMetrics") return { cssVisualViewport: { pageX: 0, pageY: 0 } };
      if (method === "Page.captureScreenshot") {
        // A snapshot sizes its screenshot from the PNG header; viewers get JPEG frames as-is.
        return { data: (input as { format: string }).format === "png" ? PNG_HEADER : "ZnJhbWU=" };
      }
      if (method === "Page.getFrameTree") return { frameTree: { frame: { id: "main" } } };
      if (method === "Page.createIsolatedWorld") {
        return {
          executionContextId: (input as { worldName?: string }).worldName === "t3-preview" ? 1 : 7,
        };
      }
      if (method === "Runtime.evaluate" && (input as { contextId?: number }).contextId === 1) {
        return { result: { value: [] } };
      }
      if (method === "Page.addScriptToEvaluateOnNewDocument") frame?.recording.resolve();
      if (method === "Input.dispatchKeyEvent" && (input as { commands?: unknown }).commands) {
        if (focusedFrame) focusedFrame.copy = keyCopy || focusedFrame.copy;
        else pageCopy = keyCopy || pageCopy;
      }
      if (method === "Runtime.evaluate" && (input as { contextId?: number }).contextId === 7) {
        const value = frame ? frame.copy : pageCopy;
        if (frame) frame.copy = "";
        else pageCopy = "";
        return { result: { value } };
      }
      return { result: { value: "evaluated" } };
    }),
  };
}

function makeContext(onClose?: (context: BrowserContext) => void, initialUrl = "about:blank") {
  const events = new NodeEvents.EventEmitter();
  const nativeFileChooser = vi.fn();
  const setChooserFiles = vi.fn(async (_files: unknown) => {});
  const sessions: ReturnType<typeof makeSession>[] = [];
  let url = initialUrl;
  let viewport = { width: 1280, height: 800 };
  let closed = false;
  let contextClosed = false;
  const page = {
    on: (name: string, callback: (...args: unknown[]) => void) => events.on(name, callback),
    once: (name: string, callback: (...args: unknown[]) => void) => events.once(name, callback),
    off: (name: string, callback: (...args: unknown[]) => void) => events.off(name, callback),
    emit: (name: string, ...args: unknown[]) => events.emit(name, ...args),
    nativeFileChooser,
    setChooserFiles,
    openFileChooser: () => {
      if (
        !events.emit("filechooser", {
          isMultiple: () => false,
          element: () => ({ getAttribute: async () => ".csv" }),
          setFiles: setChooserFiles,
        })
      ) {
        nativeFileChooser();
      }
    },
    emitAsync: (name: string, ...args: unknown[]) =>
      Promise.all(events.listeners(name).map((listener) => listener(...args))),
    setDefaultTimeout: vi.fn(),
    setDefaultNavigationTimeout: vi.fn(),
    context: () => context,
    mainFrame: () => page,
    frames: () => [page],
    url: () => url,
    title: vi.fn(async () => "test page"),
    viewportSize: () => viewport,
    setViewportSize: vi.fn(async (size: typeof viewport) => {
      viewport = size;
    }),
    goto: vi.fn(async (next: string) => {
      url = next;
      events.emit("load");
      const pending = initialNavigation;
      initialNavigation = null;
      await pending?.();
    }),
    goBack: vi.fn(async () => {}),
    goForward: vi.fn(async () => {}),
    reload: vi.fn(async () => {}),
    emulateMedia: vi.fn(async () => {}),
    waitForLoadState: vi.fn(async () => {}),
    evaluate: vi.fn(async () => ({
      url,
      title: "test page",
      loading: false,
      visibleText: "delete",
      interactiveElements: [],
    })),
    ariaSnapshot: vi.fn(async () => '- button "delete" [ref=e1]'),
    locator: vi.fn(() => {
      throw new Error("Unexpected locator action");
    }),
    isClosed: () => closed,
    close: vi.fn(async () => {
      if (!closed) {
        closed = true;
        events.emit("close");
      }
    }),
  };
  const context = {
    page,
    sessions,
    newPage: async () => page as unknown as Page,
    grantPermissions: vi.fn(async () => {}),
    newCDPSession: async (target: unknown) => {
      const session = makeSession(
        target === page || (typeof target === "object" && target !== null && "context" in target)
          ? undefined
          : (target as ReturnType<typeof makeFrame>),
      );
      sessions.push(session);
      return session;
    },
    close: vi.fn(async () => {
      if (contextClosed) return;
      contextClosed = true;
      await page.close();
      onClose?.(context as unknown as BrowserContext);
    }),
  };
  return context;
}

// Runs the real encoder script against fake canvas and MediaRecorder globals.
function makeEncoderPage() {
  const canvas = {
    width: 0,
    height: 0,
    getContext: () => ({ drawImage() {}, fillRect() {} }),
    captureStream: (frameRate?: number) => ({ frameRate, getTracks: () => [] }),
  };
  class FakeMediaRecorder {
    static isTypeSupported = (type: string) => type === "video/webm";
    onstart: (() => void) | null = null;
    constructor(stream: { frameRate?: number }, options: { videoBitsPerSecond: number }) {
      encoderStreams.push({
        frameRate: stream.frameRate,
        bitsPerSecond: options.videoBitsPerSecond,
      });
    }
    start() {
      this.onstart?.();
    }
  }
  const sandbox: Record<string, unknown> = {
    document: { createElement: () => canvas },
    Path2D: function Path2D() {},
    MediaRecorder: FakeMediaRecorder,
    createImageBitmap: async () => ({ width: 1280, height: 800, close() {} }),
    atob,
    Blob,
    performance,
  };
  sandbox.window = sandbox;
  const context = NodeVM.createContext(sandbox);
  return {
    ...makeContext().page,
    evaluate: vi.fn(async (script: string | ((arg: unknown) => unknown), arg?: unknown) =>
      typeof script === "string"
        ? NodeVM.runInContext(script, context)
        : NodeVM.runInContext(`(${script.toString()})`, context)(arg),
    ),
  };
}

const contexts: ReturnType<typeof makeContext>[] = [];
const encoderStreams: Array<{ frameRate: number | undefined; bitsPerSecond: number }> = [];
const contextRequests: Array<{ profileId: string; isolated: boolean }> = [];
let contextGate: PromiseWithResolvers<void> | null = null;
let pageCopy = "";
let keyCopy = "";
let focusedFrame: ReturnType<typeof makeFrame> | null = null;
let contextFailure: Error | null = null;
let initialNavigation: (() => Promise<void>) | null = null;
let desktopRendersNext = false;
let desktopPageUrl = "about:blank";
const desktopDetaches = new NodeEvents.EventEmitter();
const desktopTabs = new Set<string>();
const desktopRenders = (tabId: string) => {
  if (desktopRendersNext) {
    desktopRendersNext = false;
    desktopTabs.add(tabId);
  }
  return desktopTabs.has(tabId);
};
const releasedDesktopTabs: Array<string> = [];
let desktopReleased = Promise.withResolvers<void>();
let desktopDetachSubscribed = Promise.withResolvers<void>();
const desktopTurns = new NodeEvents.EventEmitter();
const desktopConnections: Array<{ endpoint: string; context: ReturnType<typeof makeContext> }> = [];
const testThread = {
  threadId: ThreadId.make("browser-test-thread"),
  providerSessionId: "agent-a",
  providerInstanceId: ProviderInstanceId.make("codex"),
};
const scope = {
  environmentId: EnvironmentId.make("browser-test-environment"),
  thread: testThread,
  client: undefined,
  requestNamespace: "browser-test",
  capabilities: new Set(["preview"] as const),
  issuedAt: 1,
};
const asSession = (providerSessionId: string) => ({
  ...scope,
  thread: { ...testThread, providerSessionId },
});
const desktopRuntimeIdentity = {
  schemaVersion: 1,
  runtimeKind: "electron",
  runtimeInstanceId: "desktop-fixture",
  appVersion: "0.1.0",
  buildCommit: null,
} as const;
const dependencies = Layer.mergeAll(
  Broker.layer,
  Manager.layer,
  Layer.succeed(ServerEnvironment.ServerEnvironment, {
    getEnvironmentId: Effect.succeed(scope.environmentId),
    getDescriptor: Effect.die("unused descriptor"),
  }),
  Layer.succeed(PreviewBrowser.PreviewBrowser, {
    executable: Effect.die("mock Chromium does not need an executable"),
    installed: Effect.die("mock Chromium does not need an executable"),
  }),
  Layer.succeed(DesktopChannel.DesktopBrowserChannel, {
    available: true,
    awaitAttached: (key) => Effect.sync(() => desktopRenders(key.tabId)),
    detached: Stream.callback<{ threadId: string; tabId: string }>((queue) =>
      Effect.acquireRelease(
        Effect.sync(() => {
          const onDetach = (key: { threadId: string; tabId: string }) =>
            Queue.offerUnsafe(queue, key);
          desktopDetaches.on("detach", onDetach);
          desktopDetachSubscribed.resolve();
          return onDetach;
        }),
        (onDetach) => Effect.sync(() => desktopDetaches.off("detach", onDetach)),
      ),
    ),
    attached: Stream.callback<{ threadId: string; tabId: string }>((queue) =>
      Effect.acquireRelease(
        Effect.sync(() => {
          const onAttach = (key: { threadId: string; tabId: string }) =>
            Queue.offerUnsafe(queue, key);
          desktopDetaches.on("attach", onAttach);
          return onAttach;
        }),
        (onAttach) => Effect.sync(() => desktopDetaches.off("attach", onAttach)),
      ),
    ),
    isAttached: (key) => Effect.sync(() => desktopRenders(key.tabId)),
    runtimeEvidence: (key) =>
      Effect.sync(() =>
        desktopRenders(key.tabId)
          ? { runtimeIdentity: desktopRuntimeIdentity, attachmentGeneration: 1 }
          : null,
      ),
    endpoint: (key) =>
      Effect.acquireRelease(Effect.succeed(`ws://desktop/${key.tabId}`), () =>
        Effect.sync(() => {
          releasedDesktopTabs.push(key.tabId);
          desktopReleased.resolve();
        }),
      ),
    pointer: () => Effect.void,
    turn: (threadId, active) => Effect.sync(() => desktopTurns.emit("turn", threadId, active)),
  }),
  PreviewAgentActivity.layer,
).pipe(
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-server-browser-" })),
  Layer.provideMerge(NodeServices.layer),
);
const layer = ServerBrowser.layer.pipe(Layer.provideMerge(dependencies));
const placementLayer = ServerBrowser.layer.pipe(
  Layer.provideMerge(CompanionRegistration.servicesLayer.pipe(Layer.provideMerge(dependencies))),
);
const ready = Effect.gen(function* () {
  const browser = yield* ServerBrowser.ServerBrowser;
  const broker = yield* Broker.PreviewAutomationBroker;
  yield* Effect.yieldNow;
  const opened = yield* broker.invoke<PreviewAutomationStatus>({
    scope,
    operation: "open",
    input: { reuseExistingTab: false, show: false },
  });
  const tabId = PreviewTabId.make(opened.tabId!);
  return { browser, broker, tabId };
});
const stallViewer = (
  viewer: ServerBrowser.ServerBrowserViewer,
  item: ServerBrowser.ServerBrowserViewerOutput,
) => {
  // The real attachViewer owner returns this queue's read handle. Check its runtime
  // write capability before filling the synthetic backlog; do not cast a dequeue.
  const output = viewer.output;
  if (!Queue.isQueue<ServerBrowser.ServerBrowserViewerOutput, never>(output)) {
    throw new Error("Expected the server-owned viewer output queue");
  }
  while (Queue.offerUnsafe(output, item));
};

const viewerInput = (tabId: string, canOperate: boolean) => ({
  threadId: scope.thread.threadId,
  tabId,
  canOperate,
  maxWidth: 1280,
  maxHeight: 800,
  quality: 70,
});

beforeEach(() => {
  contexts.length = 0;
  encoderStreams.length = 0;
  contextRequests.length = 0;
  contextGate = null;
  contextFailure = null;
  initialNavigation = null;
  pageCopy = "";
  keyCopy = "";
  focusedFrame = null;
  desktopTabs.clear();
  desktopRendersNext = false;
  desktopPageUrl = "about:blank";
  releasedDesktopTabs.length = 0;
  desktopReleased = Promise.withResolvers<void>();
  desktopDetachSubscribed = Promise.withResolvers<void>();
  desktopConnections.length = 0;
});

it.live("a selected companion attachment failure never becomes a headless fallback", () =>
  Effect.gen(function* () {
    yield* ServerBrowser.ServerBrowser;
    const broker = yield* Broker.PreviewAutomationBroker;
    const manager = yield* Manager.PreviewManager;
    yield* Effect.yieldNow;
    const error = yield* broker
      .invoke<PreviewAutomationStatus>({
        scope,
        operation: "open",
        input: { reuseExistingTab: false, show: false },
      })
      .pipe(Effect.flip);
    expect(error).toMatchObject({
      _tag: "PreviewAutomationExecutionError",
      outcome: "not_started",
    });
    expect((yield* manager.list({ threadId: scope.thread.threadId })).sessions).toEqual([]);
    expect(contextRequests).toEqual([]);
    expect(desktopConnections).toEqual([]);
  }).pipe(
    Effect.scoped,
    Effect.provide(
      ServerBrowser.layer.pipe(
        Layer.provide(
          Layer.mock(Placement.RenderPlacement, {
            channel: {
              availableFor: () => Effect.die("unexpected companion channel operation"),
              mount: () => Effect.die("unexpected companion channel operation"),
              unmount: () => Effect.die("unexpected companion channel operation"),
              awaitAttached: () => Effect.die("unexpected companion channel operation"),
              isAttached: () => Effect.die("unexpected companion channel operation"),
              detached: Stream.never,
              runtimeEvidence: () => Effect.die("unexpected companion channel operation"),
              endpoint: () => Effect.die("unexpected companion channel operation"),
              turn: () => Effect.void,
              pointer: () => Effect.die("unexpected companion channel operation"),
            },
            bindAtOpen: () => ({ _tag: "companion", hostId: "selected-mini" }),
            place: () =>
              Effect.succeed({
                _tag: "companion",
                hostId: "selected-mini",
                label: "Mini",
                capabilities: {
                  cdp: true,
                  clipboardText: true,
                  uploads: false,
                  downloads: false,
                  recording: false,
                },
              }),
            awaitCompanion: () =>
              Effect.fail(
                new CompanionRegistry.CompanionHostUnavailable({
                  hostId: "selected-mini",
                  label: "Mini",
                  state: "timeout",
                }),
              ),
            release: () => Effect.void,
          }),
        ),
        Layer.provideMerge(dependencies),
      ),
    ),
  ),
);

it.live("tells the desktop once per thread when its agent turns start and end", () =>
  Effect.gen(function* () {
    yield* ServerBrowser.ServerBrowser;
    const activity = yield* PreviewAgentActivity.PreviewAgentActivity;
    const threadId = scope.thread.threadId;
    const turns = yield* Queue.unbounded<readonly [string, boolean]>();
    const record = (id: string, active: boolean) => Queue.offerUnsafe(turns, [id, active]);
    desktopTurns.on("turn", record);
    yield* Effect.addFinalizer(() => Effect.sync(() => desktopTurns.off("turn", record)));
    const endFirst = yield* activity.begin(threadId);
    // An overlapping turn on the same thread neither starts nor ends focus on its own.
    const endSecond = yield* activity.begin(threadId);
    expect(yield* Queue.take(turns)).toEqual([threadId, true]);
    yield* endFirst;
    yield* endFirst;
    expect(yield* activity.active(threadId)).toBe(true);
    yield* endSecond;
    expect(yield* Queue.take(turns)).toEqual([threadId, false]);
    expect(yield* Queue.size(turns)).toBe(0);
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.live("preview_open reports its navigation timeout instead of the request budget", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { broker } = yield* ready;
      initialNavigation = async () => {
        throw Object.assign(new Error("page.goto: Timeout 15000ms exceeded."), {
          name: "TimeoutError",
        });
      };

      const error = yield* broker
        .invoke<PreviewAutomationStatus>({
          scope,
          operation: "open",
          input: {
            url: "http://localhost:1337/",
            reuseExistingTab: false,
            show: false,
          },
        })
        .pipe(Effect.flip);

      expect(error).toMatchObject({
        _tag: "PreviewAutomationTimeoutError",
        timeoutMs: 15_000,
      });
      expect(error.message).toBe("Preview automation open timed out after 15000ms.");
    }),
  ).pipe(Effect.provide(layer)),
);

it.live("preview_snapshot reports the page budget after an ARIA timeout", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { broker, tabId } = yield* ready;
      contexts[0]!.page.ariaSnapshot.mockRejectedValue(
        Object.assign(new Error("locator.ariaSnapshot: Timeout 8000ms exceeded."), {
          name: "TimeoutError",
        }),
      );

      const error = yield* broker
        .invoke<PreviewAutomationSnapshot>({
          scope,
          operation: "snapshot",
          tabId,
          input: {},
        })
        .pipe(Effect.flip);

      expect(error).toMatchObject({
        _tag: "PreviewAutomationTimeoutError",
        operation: "snapshot",
        timeoutMs: 10_000,
        outcome: "reported",
      });
      expect(error.message).toBe("Preview automation snapshot timed out after 10000ms.");
    }),
  ).pipe(Effect.provide(layer)),
);

it("includes the page budget when the snapshot timer expires", async () => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  try {
    const failure = ServerBrowserPage.snapshot({
      page: {} as Page,
      cdp: { send: () => new Promise<never>(() => {}) } as unknown as CDPSession,
      renderScale: "display",
      consoleEntries: [],
      networkEntries: [],
      actionTimeline: [],
    }).catch((error: unknown) => error);

    await vi.advanceTimersByTimeAsync(10_000);
    expect(await failure).toMatchObject({
      tag: "PreviewAutomationTimeoutError",
      detail: { timeoutMs: 10_000 },
    });
  } finally {
    vi.useRealTimers();
  }
});

it.live.each(["desktop" as const, undefined])(
  "explicit desktop targets ($0 runtime) fail without creating a server tab",
  (runtime) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { broker, tabId } = yield* ready;
        const manager = yield* Manager.PreviewManager;
        const desktop = yield* manager.open({
          threadId: scope.thread.threadId,
          url: "https://example.com/original",
          ...(runtime === undefined ? {} : { runtime }),
        });
        const before = yield* manager.list({ threadId: scope.thread.threadId });
        const error = yield* broker
          .invoke<PreviewAutomationStatus>({
            scope,
            tabId: PreviewTabId.make(desktop.tabId),
            operation: "open",
            input: { url: "https://example.com/replacement", open: true },
          })
          .pipe(Effect.flip);
        expect(error).toMatchObject({ _tag: "PreviewAutomationExecutionError" });
        expect(error.message).toContain("desktop runtime");
        expect(error.message).toContain("cannot control");
        expect(yield* manager.list({ threadId: scope.thread.threadId })).toEqual(before);
        expect(contexts).toHaveLength(1);
        const reused = yield* broker.invoke<PreviewAutomationStatus>({
          scope,
          operation: "open",
          input: { open: false },
        });
        expect(reused.tabId).toBe(tabId);
      }),
    ).pipe(Effect.provide(layer)),
);

it.live.each([
  { method: "goto" as const, message: { type: "navigate", url: "http://10.255.255.1/" } },
  { method: "goBack" as const, message: { type: "history", delta: -1 } },
  { method: "goForward" as const, message: { type: "history", delta: 1 } },
  { method: "reload" as const, message: { type: "reload" } },
])("a pending viewer $method allows a corrected URL and text input", ({ method, message }) =>
  Effect.scoped(
    Effect.gen(function* () {
      const { browser, tabId } = yield* ready;
      const viewer = yield* browser.attachViewer(viewerInput(tabId, true));
      yield* viewer.input({ type: "takeControl" });
      const started = Promise.withResolvers<void>();
      const committed = Promise.withResolvers<void>();
      const page = contexts[0]!.page;
      page[method].mockImplementationOnce(() => {
        started.resolve();
        return committed.promise;
      });
      const navigation = yield* viewer.input(message).pipe(Effect.forkScoped);
      yield* Effect.promise(() => started.promise);
      const cdp = contexts[0]!.sessions.at(-1)!;
      const send = cdp.send.getMockImplementation()!;
      const typed = Promise.withResolvers<void>();
      cdp.send.mockImplementation(async (operation, input) => {
        if (operation === "Input.insertText") typed.resolve();
        return send(operation, input);
      });
      const correction = yield* Effect.gen(function* () {
        yield* viewer.input({ type: "navigate", url: "http://localhost:5173/fixed" });
        yield* viewer.input({ type: "text", text: "hello" });
      }).pipe(Effect.forkScoped);
      yield* Effect.addFinalizer(() => Effect.sync(() => committed.resolve()));
      // The text reaches the page while the first navigation is still uncommitted.
      yield* Effect.promise(() => typed.promise);
      expect(page.goto).toHaveBeenLastCalledWith(
        "http://localhost:5173/fixed",
        expect.objectContaining({ waitUntil: "commit" }),
      );
      expect(contexts[0]!.sessions.at(-1)!.send).toHaveBeenCalledWith("Input.insertText", {
        text: "hello",
      });
      committed.resolve();
      yield* Fiber.join(navigation);
      yield* Fiber.join(correction);
    }),
  ).pipe(Effect.provide(layer)),
);

// The fake encoder decodes every frame at 1280x800, so 30 fps hits the bitrate floor.
it.live.each([
  { reports: [], frameRate: 30, bitsPerSecond: 2.5e6 },
  { reports: [30], frameRate: 30, bitsPerSecond: 2.5e6 },
  { reports: [60], frameRate: 60, bitsPerSecond: 1280 * 800 * 60 * 0.05 },
  { reports: [60, undefined], frameRate: 60, bitsPerSecond: 1280 * 800 * 60 * 0.05 },
  { reports: [60, 30], frameRate: 30, bitsPerSecond: 2.5e6 },
] satisfies ReadonlyArray<{
  reports: ReadonlyArray<BrowserRecordingFrameRate | undefined>;
  frameRate: number;
  bitsPerSecond: number;
}>)(
  "agent recordings encode at the last reported frame rate ($reports)",
  ({ reports, frameRate, bitsPerSecond }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { browser, broker, tabId } = yield* ready;
        for (const recordingFrameRate of reports) {
          yield* browser.reportProfiles({
            profiles: [],
            defaultProfileId: "default",
            ...(recordingFrameRate === undefined ? {} : { recordingFrameRate }),
          });
        }
        yield* broker.invoke({ scope, tabId, operation: "recordingStart", input: {} });
        expect(encoderStreams).toEqual([{ frameRate, bitsPerSecond }]);
      }),
    ).pipe(Effect.provide(layer)),
);

it.live("enforces provider ownership and explicit targets when a session has multiple tabs", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { broker, tabId } = yield* ready;
      const foreign = yield* broker
        .invoke<void>({
          scope: asSession("agent-b"),
          tabId,
          operation: "navigate",
          input: { url: "http://localhost:5173/foreign" },
        })
        .pipe(Effect.flip);
      expect(foreign).toMatchObject({
        _tag: "PreviewAutomationControlInterruptedError",
        reason: "agentMismatch",
      });
      expect(contexts[0]!.page.goto).not.toHaveBeenCalledWith(
        "http://localhost:5173/foreign",
        expect.anything(),
      );
      yield* broker.invoke<PreviewAutomationSnapshot>({
        scope: asSession("agent-b"),
        tabId,
        operation: "snapshot",
        input: {},
      });
      const foreignEvaluate = yield* broker
        .invoke<void>({
          scope: asSession("agent-b"),
          tabId,
          operation: "evaluate",
          input: { expression: "read()" },
        })
        .pipe(Effect.flip);
      expect(foreignEvaluate).toMatchObject({
        _tag: "PreviewAutomationControlInterruptedError",
        reason: "agentMismatch",
      });
      yield* broker.invoke({
        scope,
        operation: "open",
        input: { reuseExistingTab: false, show: false },
      });
      const ambiguous = yield* broker
        .invoke<void>({ scope, operation: "evaluate", input: { expression: "ambiguous()" } })
        .pipe(Effect.flip);
      expect(ambiguous).toMatchObject({
        _tag: "PreviewAutomationControlInterruptedError",
        reason: "tabRequired",
      });
      const ambiguousStop = yield* broker
        .invoke<void>({ scope, operation: "recordingStop", input: {} })
        .pipe(Effect.flip);
      expect(ambiguousStop).toMatchObject({
        _tag: "PreviewAutomationControlInterruptedError",
        reason: "tabRequired",
      });
      const explicitStop = yield* broker
        .invoke<void>({ scope, tabId, operation: "recordingStop", input: {} })
        .pipe(Effect.flip);
      expect(explicitStop).toMatchObject({
        _tag: "PreviewAutomationExecutionError",
        cause: { _tag: "PreviewAutomationRecordingNotActiveError" },
      });
      const result = yield* broker.invoke({
        scope,
        tabId,
        operation: "evaluate",
        input: { expression: "owned()" },
      });
      expect(result).toBe("evaluated");
      expect(contexts).toHaveLength(2);
    }),
  ).pipe(Effect.provide(layer)),
);

it.live("streams to a read-only viewer without allowing takeover, input, or viewport changes", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { browser, tabId } = yield* ready;
      const viewer = yield* browser.attachViewer(viewerInput(tabId, false));
      const page = contexts[0]!.page;
      page.setViewportSize.mockClear();
      const session = contexts[0]!.sessions.at(-1)!;
      session.send.mockClear();
      for (const message of [
        { type: "takeControl" },
        { type: "key", action: "down", key: "a", text: "a" },
        { type: "resize", width: 390, height: 844 },
        { type: "viewport", setting: { _tag: "freeform", width: 390, height: 844 } },
      ])
        yield* viewer.input(message);
      expect(page.setViewportSize).not.toHaveBeenCalled();
      expect(session.send.mock.calls.some(([method]) => method.startsWith("Input."))).toBe(false);
      const outputs = yield* Queue.takeAll(viewer.output);
      expect(outputs).toContainEqual(
        expect.objectContaining({ _tag: "frame", data: Buffer.from("frame") }),
      );
      expect(outputs).toContainEqual(expect.objectContaining({ _tag: "viewport" }));
      expect(outputs).toContainEqual(
        expect.objectContaining({ _tag: "control", canOperate: false, controller: "agent" }),
      );
      const operator = yield* browser.attachViewer(viewerInput(tabId, true));
      yield* operator.input({ type: "takeControl" });
      yield* operator.input({ type: "text", text: "typed" });
      yield* operator.input({ type: "resize", width: 390, height: 844 });
      expect(contexts[0]!.sessions.at(-1)!.send).toHaveBeenCalledWith("Input.insertText", {
        text: "typed",
      });
      expect(page.setViewportSize).toHaveBeenCalledWith({ width: 390, height: 844 });
      yield* operator.input({ type: "releaseControl" });
    }),
  ).pipe(Effect.provide(layer)),
);

it.live(
  "takeover waits for the running agent and revokes snapshot refs before returning control",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { browser, broker, tabId } = yield* ready;
        const snapshot = yield* broker.invoke<PreviewAutomationSnapshot>({
          scope,
          tabId,
          operation: "snapshot",
          input: {},
        });
        const ref = /\[ref=([^\]]+)\]/.exec(String(snapshot.accessibilityTree))![1]!;
        const viewer = yield* browser.attachViewer(viewerInput(tabId, true));
        const started = Promise.withResolvers<void>();
        const finish = Promise.withResolvers<Record<string, unknown>>();
        const session = contexts[0]!.sessions[0]!;
        session.send.mockImplementationOnce(async () => {
          started.resolve();
          return finish.promise;
        });
        const running = yield* broker
          .invoke({ scope, tabId, operation: "evaluate", input: { expression: "pending()" } })
          .pipe(Effect.forkScoped);
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => finish.resolve({ result: { value: "finished" } })),
        );
        yield* Effect.promise(() => started.promise);
        const takeover = yield* viewer.input({ type: "takeControl" }).pipe(Effect.forkScoped);
        let control = yield* Queue.take(viewer.output);
        while (control._tag !== "control" || control.controller !== "you") {
          control = yield* Queue.take(viewer.output);
        }
        const rejected = yield* broker
          .invoke<void>({ scope, tabId, operation: "evaluate", input: { expression: "racing()" } })
          .pipe(Effect.flip);
        expect(rejected).toMatchObject({
          _tag: "PreviewAutomationControlInterruptedError",
          reason: "humanControl",
        });
        finish.resolve({ result: { value: "finished" } });
        expect(yield* Fiber.join(running)).toBe("finished");
        yield* Fiber.join(takeover);
        yield* viewer.input({ type: "releaseControl" });
        const stale = yield* broker
          .invoke<void>({ scope, tabId, operation: "click", input: { locator: `aria-ref=${ref}` } })
          .pipe(Effect.flip);
        expect(stale._tag).toBe("PreviewAutomationInvalidSelectorError");
        expect(contexts[0]!.page.locator).not.toHaveBeenCalled();
        expect(
          yield* broker.invoke({
            scope,
            tabId,
            operation: "evaluate",
            input: { expression: "resumed()" },
          }),
        ).toBe("evaluated");
      }),
    ).pipe(Effect.provide(layer)),
);

it.live("reports a pending dialog without evaluating the page and resolves it explicitly", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { broker, tabId } = yield* ready;
      const page = contexts[0]!.page;
      yield* broker.invoke({
        scope,
        tabId,
        operation: "navigate",
        input: { url: "http://localhost:5173" },
      });
      const started = Promise.withResolvers<void>();
      const resolvedEvaluation = Promise.withResolvers<Record<string, unknown>>();
      const session = contexts[0]!.sessions[0]!;
      const send = session.send.getMockImplementation()!;
      session.send.mockImplementation(async (method, input) => {
        if (method !== "Runtime.evaluate") return send(method, input);
        started.resolve();
        return resolvedEvaluation.promise;
      });
      const blockedAction = yield* broker
        .invoke({
          scope,
          tabId,
          operation: "evaluate",
          input: { expression: 'confirm("Delete row?")' },
        })
        .pipe(Effect.forkScoped);
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => resolvedEvaluation.resolve({ result: { value: true } })),
      );
      yield* Effect.promise(() => started.promise);
      const dialog = {
        type: () => "confirm",
        message: () => "Delete row?",
        defaultValue: () => "",
        accept: vi.fn(async () => {
          resolvedEvaluation.resolve({ result: { value: true } });
        }),
        dismiss: vi.fn(async () => {}),
      };
      page.emit("dialog", dialog);
      page.title.mockClear();
      const status = yield* broker.invoke<PreviewAutomationStatus>({
        scope,
        tabId,
        operation: "status",
        input: {},
      });
      expect(status.dialog).toMatchObject({ type: "confirm", message: "Delete row?" });
      expect(page.title).not.toHaveBeenCalled();
      expect(dialog.dismiss).not.toHaveBeenCalled();
      const reuse = yield* broker
        .invoke<void>({
          scope,
          tabId,
          operation: "open",
          input: { url: "http://localhost:5173/other" },
        })
        .pipe(Effect.flip);
      expect(reuse).toMatchObject({
        _tag: "PreviewAutomationControlInterruptedError",
        reason: "dialogPending",
      });
      expect(page.goto).toHaveBeenCalledTimes(1);
      yield* broker.invoke({ scope, tabId, operation: "dialog", input: { accept: true } });
      expect(yield* Fiber.join(blockedAction)).toBe(true);
      expect(dialog.accept).toHaveBeenCalledTimes(1);
      const resolved = yield* broker.invoke<PreviewAutomationStatus>({
        scope,
        tabId,
        operation: "status",
        input: {},
      });
      expect(resolved.dialog).toBeNull();
    }),
  ).pipe(Effect.provide(layer)),
);

it.live("the owner can close a tab while an agent action waits on its dialog", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { broker, tabId } = yield* ready;
      const page = contexts[0]!.page;
      const started = Promise.withResolvers<void>();
      const finish = Promise.withResolvers<Record<string, unknown>>();
      contexts[0]!.sessions[0]!.send.mockImplementationOnce(async () => {
        started.resolve();
        return finish.promise;
      });
      page.on("close", () => finish.resolve({ result: { value: "closed" } }));
      const running = yield* broker
        .invoke({
          scope,
          tabId,
          operation: "evaluate",
          input: { expression: 'confirm("Delete?")' },
        })
        .pipe(Effect.forkScoped);
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => finish.resolve({ result: { value: "cleanup" } })),
      );
      yield* Effect.promise(() => started.promise);
      page.emit("dialog", {
        type: () => "confirm",
        message: () => "Delete?",
        defaultValue: () => "",
        accept: vi.fn(),
        dismiss: vi.fn(),
      });
      const foreign = yield* broker
        .invoke<void>({
          scope: asSession("agent-b"),
          tabId,
          operation: "close",
          input: {},
        })
        .pipe(Effect.flip);
      expect(foreign).toMatchObject({
        _tag: "PreviewAutomationControlInterruptedError",
        reason: "agentMismatch",
      });
      expect(page.close).not.toHaveBeenCalled();
      yield* broker.invoke({ scope, tabId, operation: "close", input: {} });
      expect(yield* Fiber.join(running)).toBe("closed");
      expect(page.close).toHaveBeenCalled();
      const manager = yield* Manager.PreviewManager;
      expect((yield* manager.list({ threadId: scope.thread.threadId })).sessions).toHaveLength(0);
      const afterClose = yield* broker
        .invoke<void>({ scope, tabId, operation: "evaluate", input: { expression: "late()" } })
        .pipe(Effect.flip);
      expect(afterClose._tag).toBe("PreviewAutomationTabNotFoundError");
    }),
  ).pipe(Effect.provide(layer)),
);

it.live("a popup becomes the agent's own tab and keeps its opener page", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { broker, tabId } = yield* ready;
      const opener = contexts[0]!.page;
      const popup = makeContext();
      const manager = yield* Manager.PreviewManager;
      const events = yield* manager.subscribeEvents;
      opener.emit("popup", popup.page);
      let event = yield* PubSub.take(events);
      while (event.type !== "opened" || event.tabId === tabId) event = yield* PubSub.take(events);
      const sessions = (yield* manager.list({ threadId: scope.thread.threadId })).sessions;
      const popupTab = sessions.find((session) => session.tabId !== tabId)!;
      const opened = sessions.find((session) => session.tabId === tabId)!;
      expect(popupTab).toMatchObject({ automationOwner: opened.automationOwner, reveal: false });
      const status = yield* broker.invoke<PreviewAutomationStatus>({
        scope,
        tabId,
        operation: "status",
        input: {},
      });
      expect(status.selectedClient?.runtimeIdentity).toBeNull();
      expect(status.tabs).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ tabId }),
          expect.objectContaining({ tabId: popupTab.tabId, openerTabId: tabId }),
        ]),
      );
      expect(opener.goto).not.toHaveBeenCalled();
      expect(opener.close).not.toHaveBeenCalled();
      yield* Effect.promise(() => popup.page.close());
      let closed = yield* PubSub.take(events);
      while (closed.type !== "closed" || closed.tabId !== popupTab.tabId) {
        closed = yield* PubSub.take(events);
      }
    }),
  ).pipe(Effect.provide(layer)),
);

it.live("a tab opened to a file the browser cannot show reports the file to download", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const browser = yield* ServerBrowser.ServerBrowser;
      const manager = yield* Manager.PreviewManager;
      const events = yield* manager.subscribeEvents;
      yield* Effect.yieldNow;
      const opened = yield* manager.open({ threadId: scope.thread.threadId, runtime: "server" });
      const viewer = yield* browser.attachViewer(viewerInput(opened.tabId, true));
      const page = contexts[0]!.page;
      const pdf = "https://example.com/paper.pdf";
      const request = { url: () => pdf, method: () => "GET", isNavigationRequest: () => true };
      page.emit("requestfailed", {
        ...request,
        frame: () => page,
        failure: () => ({ errorText: "net::ERR_ABORTED" }),
      });
      const saved = yield* Queue.unbounded<string>();
      const written = Promise.withResolvers<void>();
      page.emit("download", {
        failure: async () => null,
        delete: async () => {
          // A new navigation during cleanup must not erase the settled download status.
          page.emit("request", {
            url: () => "https://example.com/next",
            method: () => "GET",
            isNavigationRequest: () => true,
            frame: () => page,
          });
        },
        saveAs: (path: string) => {
          Queue.offerUnsafe(saved, path);
          return written.promise;
        },
        suggestedFilename: () => "paper.pdf",
        url: () => pdf,
      });
      const fs = yield* FileSystem.FileSystem;
      yield* fs.writeFileString(yield* Queue.take(saved), "%PDF");
      written.resolve();
      const received = [];
      let status;
      do {
        status = yield* PubSub.take(events);
        received.push(status);
      } while (
        status.type !== "navigated" ||
        status.snapshot.navStatus._tag !== "Loading" ||
        status.snapshot.navStatus.url !== "https://example.com/next"
      );
      expect(received).toContainEqual(
        expect.objectContaining({
          type: "failed",
          url: pdf,
          download: { id: expect.any(String), fileName: "paper.pdf" },
        }),
      );
      expect((yield* Queue.clear(viewer.output)).some((item) => item._tag === "download")).toBe(
        false,
      );
    }),
  ).pipe(Effect.provide(layer)),
);

it.live(
  "a late failure from a request that predates tracking leaves a newer navigation loading",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { broker, tabId } = yield* ready;
        const page = contexts[0]!.page;
        const navigation = (url: string) => ({
          url: () => url,
          method: () => "GET",
          isNavigationRequest: () => true,
          frame: () => page,
          failure: () => ({ errorText: "net::ERR_CONNECTION_REFUSED" }),
        });
        page.emit("request", navigation("https://example.com/next"));
        // A popup's first request can start before the tab listens for requests.
        page.emit("requestfailed", navigation("https://example.com/first"));
        const status = yield* broker.invoke<PreviewAutomationStatus>({
          scope,
          tabId,
          operation: "status",
          input: {},
        });
        expect(status.loading).toBe(true);
      }),
    ).pipe(Effect.provide(layer)),
);

it.live("only the controlling viewer's copy and cut keys reach its clipboard", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { browser, tabId } = yield* ready;
      const watcher = yield* browser.attachViewer(viewerInput(tabId, false));
      const viewer = yield* browser.attachViewer(viewerInput(tabId, true));
      yield* viewer.input({ type: "takeControl" });
      const clipboard = (queue: typeof viewer.output) =>
        Queue.clear(queue).pipe(
          Effect.map((items) => items.filter((item) => item._tag === "clipboard")),
        );
      pageCopy = "unprompted";
      yield* viewer.input({ type: "mouse", action: "move", x: 10, y: 10 });
      yield* viewer.input({ type: "wheel", x: 10, y: 10, deltaY: 40 });
      yield* viewer.input({ type: "key", action: "down", key: "v", code: "KeyV", modifiers: 4 });
      yield* viewer.input({ type: "unknown" });
      expect(yield* clipboard(viewer.output)).toEqual([]);
      keyCopy = "copied";
      yield* viewer.input({ type: "key", action: "down", key: "c", code: "KeyC", modifiers: 4 });
      const cdp = contexts[0]!.sessions.at(-1)!;
      expect(cdp.send).toHaveBeenCalledWith(
        "Input.dispatchKeyEvent",
        expect.objectContaining({ key: "c", commands: ["copy"] }),
      );
      expect(yield* clipboard(viewer.output)).toEqual([{ _tag: "clipboard", text: "copied" }]);
      keyCopy = "cut";
      yield* viewer.input({ type: "key", action: "down", key: "x", code: "KeyX", modifiers: 2 });
      expect(yield* clipboard(viewer.output)).toEqual([{ _tag: "clipboard", text: "cut" }]);
      pageCopy = "left over";
      keyCopy = "";
      yield* viewer.input({ type: "key", action: "down", key: "c", code: "KeyC", modifiers: 4 });
      expect(yield* clipboard(viewer.output)).toEqual([]);
      expect(yield* clipboard(watcher.output)).toEqual([]);
    }),
  ).pipe(Effect.provide(layer)),
);

it.live.each([false, true])(
  "a page download is saved, offered to the controller, and listed for the agent (delete fails: %s)",
  (deleteFails) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { browser, broker, tabId } = yield* ready;
        const page = contexts[0]!.page;
        const viewer = yield* browser.attachViewer(viewerInput(tabId, true));
        yield* viewer.input({ type: "takeControl" });
        yield* Queue.clear(viewer.output);
        const fs = yield* FileSystem.FileSystem;
        const original = `${yield* fs.makeTempDirectoryScoped()}/chromium-download`;
        yield* fs.writeFileString(original, "a,b");
        const released = Promise.withResolvers<void>();
        page.emit("download", {
          failure: async () => null,
          delete: async () => {
            try {
              if (deleteFails) throw new Error("context closed");
              await NodeFSP.rm(original);
            } finally {
              released.resolve();
            }
          },
          saveAs: (path: string) => NodeFSP.copyFile(original, path),
          suggestedFilename: () => "report.csv",
          url: () => 'data:text/csv;charset="a,b",a b\'c',
        });
        let offered = yield* Queue.take(viewer.output);
        while (offered._tag !== "download") offered = yield* Queue.take(viewer.output);
        expect(offered).toMatchObject({ _tag: "download", fileName: "report.csv", sizeBytes: 3 });
        yield* Effect.promise(() => released.promise);
        expect(yield* fs.exists(original)).toBe(deleteFails);
        const file = yield* browser.openDownload({
          threadId: scope.thread.threadId,
          tabId,
          downloadId: offered.id,
        });
        expect(Option.isSome(file) && file.value.fileName).toBe("report.csv");
        expect(yield* fs.readFileString(Option.getOrThrow(file).path)).toBe("a,b");
        if ((yield* HostProcess.Platform) !== "win32") {
          expect((yield* fs.stat(Option.getOrThrow(file).path)).mode & 0o777).toBe(0o600);
        }
        const status = yield* broker.invoke<PreviewAutomationStatus>({
          scope,
          tabId,
          operation: "status",
          input: {},
        });
        expect(status.downloads).toEqual([
          expect.objectContaining({
            fileName: "report.csv",
            sizeBytes: 3,
            url: "data:text/csv,…",
          }),
        ]);
        expect(
          Option.isNone(
            yield* browser.openDownload({
              threadId: scope.thread.threadId,
              tabId,
              downloadId: "guess",
            }),
          ),
        ).toBe(true);
        if ((yield* HostProcess.Platform) !== "win32") {
          const folder = (yield* Path.Path).dirname(Option.getOrThrow(file).path);
          yield* fs.chmod(folder, 0o755);
          page.emit("download", {
            failure: async () => null,
            delete: async () => {},
            saveAs: (path: string) => NodeFSP.writeFile(path, "c"),
            suggestedFilename: () => "again.csv",
            url: () => "https://example.test/again.csv",
          });
          let again = yield* Queue.take(viewer.output);
          while (again._tag !== "download") again = yield* Queue.take(viewer.output);
          expect((yield* fs.stat(folder)).mode & 0o777).toBe(0o700);
        }
      }),
    ).pipe(Effect.provide(layer)),
);

it.live.each(["save failed", "oversized"])(
  "a rejected download releases the original and keeps no saved copy (%s)",
  (outcome) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { broker, tabId } = yield* ready;
        const fs = yield* FileSystem.FileSystem;
        const original = `${yield* fs.makeTempDirectoryScoped()}/chromium-download`;
        yield* fs.writeFileString(original, "a,b");
        let savedPath: string | undefined;
        const released = Promise.withResolvers<void>();
        contexts[0]!.page.emit("download", {
          failure: async () => null,
          delete: async () => {
            await NodeFSP.rm(original);
            released.resolve();
          },
          saveAs: async (path: string) => {
            savedPath = path;
            await NodeFSP.copyFile(original, path);
            if (outcome === "save failed") throw new Error("save interrupted");
            // A sparse file exercises the size limit without writing a GiB of data.
            await NodeFSP.truncate(path, 1024 ** 3 + 1);
          },
          suggestedFilename: () => "report.csv",
          url: () => "blob:http://localhost:5173/1",
        });
        yield* Effect.promise(() => released.promise);
        expect(yield* fs.exists(original)).toBe(false);
        if (savedPath !== undefined) expect(yield* fs.exists(savedPath)).toBe(false);
        const status = yield* broker.invoke<PreviewAutomationStatus>({
          scope,
          tabId,
          operation: "status",
          input: {},
        });
        expect(status.downloads).toEqual([]);
      }),
    ).pipe(Effect.provide(layer)),
);

it.live("a page's file picker goes to the controller and takes its uploaded files", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { browser, tabId } = yield* ready;
      const page = contexts[0]!.page;
      const viewer = yield* browser.attachViewer(viewerInput(tabId, true));
      yield* viewer.input({ type: "takeControl" });
      const setFiles = vi.fn(async (_files: unknown) => {});
      page.emit("filechooser", {
        isMultiple: () => false,
        element: () => ({ getAttribute: async () => ".csv" }),
        setFiles,
      });
      let offered = yield* Queue.take(viewer.output);
      while (offered._tag !== "fileChooser") offered = yield* Queue.take(viewer.output);
      expect(offered).toMatchObject({ multiple: false, accept: ".csv" });
      const file = (name: string) => ({ name, mimeType: "text/csv", buffer: Buffer.from(name) });
      const answer = (chooserId: string) =>
        browser.answerFileChooser({
          threadId: scope.thread.threadId,
          tabId,
          chooserId,
          files: [file("a.csv"), file("b.csv")],
        });
      expect(yield* answer("other")).toBe(false);
      expect(yield* answer(offered.id)).toBe(true);
      expect(setFiles).toHaveBeenCalledExactlyOnceWith([file("a.csv")]);
      let closed = yield* Queue.take(viewer.output);
      while (closed._tag !== "fileChooserClosed") closed = yield* Queue.take(viewer.output);
      expect(closed.id).toBe(offered.id);
      expect(yield* answer(offered.id)).toBe(false);
    }),
  ).pipe(Effect.provide(layer)),
);

it.live("only the current controller can answer a file picker", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { browser, tabId } = yield* ready;
      const first = yield* browser.attachViewer(viewerInput(tabId, true));
      yield* first.input({ type: "takeControl" });
      const setFiles = vi.fn(async (_files: unknown) => {});
      contexts[0]!.page.emit("filechooser", {
        isMultiple: () => false,
        element: () => ({ getAttribute: async () => "" }),
        setFiles,
      });
      let offered = yield* Queue.take(first.output);
      while (offered._tag !== "fileChooser") offered = yield* Queue.take(first.output);
      const second = yield* browser.attachViewer(viewerInput(tabId, true));
      yield* first.input({ type: "releaseControl" });
      yield* second.input({ type: "takeControl" });
      const answered = yield* browser.answerFileChooser({
        threadId: scope.thread.threadId,
        tabId,
        chooserId: offered.id,
        files: [{ name: "a.txt", mimeType: "text/plain", buffer: Buffer.from("a") }],
      });
      expect(answered).toBe(false);
      expect(setFiles).not.toHaveBeenCalled();
    }),
  ).pipe(Effect.provide(layer)),
);

it.live("a file picker replaces a stalled viewer backlog instead of being dropped", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { browser, tabId } = yield* ready;
      const page = contexts[0]!.page;
      const viewer = yield* browser.attachViewer(viewerInput(tabId, true));
      yield* viewer.input({ type: "takeControl" });
      const accept = Promise.withResolvers<string>();
      const offered = page.emitAsync("filechooser", {
        isMultiple: () => false,
        element: () => ({ getAttribute: () => accept.promise }),
        setFiles: async () => {},
      });
      yield* Queue.clear(viewer.output);
      stallViewer(viewer, { _tag: "viewport", width: 1, height: 1 });
      accept.resolve(".csv");
      yield* Effect.promise(() => offered);
      expect(yield* Queue.clear(viewer.output)).toEqual([
        expect.objectContaining({ _tag: "fileChooser", accept: ".csv" }),
      ]);
    }),
  ).pipe(Effect.provide(layer)),
);

it.live("control updates replace a stalled viewer backlog in the order they happen", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { browser, tabId } = yield* ready;
      const viewer = yield* browser.attachViewer(viewerInput(tabId, true));
      yield* Queue.clear(viewer.output);
      const acked = Promise.withResolvers<void>();
      yield* Effect.addFinalizer(() => Effect.sync(() => acked.resolve()));
      stallViewer(viewer, {
        _tag: "frame",
        data: Buffer.alloc(0),
        ack: Effect.promise(() => acked.promise),
      });
      yield* viewer.input({ type: "takeControl" });
      yield* viewer.input({ type: "releaseControl" });
      const controls = (yield* Queue.clear(viewer.output)).flatMap((item) =>
        item._tag === "control" ? [item.controller] : [],
      );
      expect(controls[0]).toBe("you");
      expect(controls.at(-1)).not.toBe("you");
    }),
  ).pipe(Effect.provide(layer)),
);

it.live("an agent's tabs stop at the limit until an unwatched idle tab closes", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { broker, tabId } = yield* ready;
      const open = broker.invoke<PreviewAutomationStatus>({
        scope,
        operation: "open",
        input: { reuseExistingTab: false, show: false },
      });
      for (let count = 1; count < 8; count += 1) yield* open;
      expect(yield* Effect.flip(open)).toMatchObject({
        _tag: "PreviewAutomationControlInterruptedError",
        reason: "tabLimit",
      });
      yield* broker.invoke({
        scope: asSession("agent-b"),
        operation: "open",
        input: { reuseExistingTab: false, show: false },
      });
      const later = (yield* Clock.currentTimeMillis) + 31 * 60 * 1000;
      const clock = vi.spyOn(Date, "now").mockReturnValue(later);
      yield* Effect.addFinalizer(() => Effect.sync(() => clock.mockRestore()));
      const browser = yield* ServerBrowser.ServerBrowser;
      yield* browser.attachViewer(viewerInput(tabId, false));
      const reopened = yield* open;
      const manager = yield* Manager.PreviewManager;
      const { sessions } = yield* manager.list({ threadId: scope.thread.threadId });
      expect(sessions.map((session) => session.tabId).toSorted()).toEqual(
        [tabId, reopened.tabId].toSorted(),
      );
    }),
  ).pipe(Effect.provide(layer)),
);

it.live("an agent answers the page's file picker or sets files on a file input", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { broker, tabId } = yield* ready;
      const page = contexts[0]!.page;
      const upload = (input: Record<string, unknown>) =>
        broker.invoke<void>({ scope, tabId, operation: "upload", input });
      expect(yield* Effect.flip(upload({ paths: ["/tmp/a.csv"] }))).toMatchObject({
        _tag: "PreviewAutomationExecutionError",
      });
      const setFiles = vi.fn(async (_files: unknown) => {});
      yield* Effect.promise(() =>
        page.emitAsync("filechooser", {
          isMultiple: () => false,
          element: () => ({ getAttribute: async () => ".csv" }),
          setFiles,
        }),
      );
      const status = broker.invoke<PreviewAutomationStatus>({
        scope,
        tabId,
        operation: "status",
        input: {},
      });
      const opened = yield* status;
      expect(opened.fileChooser).toEqual({ multiple: false, accept: ".csv" });
      yield* Effect.flip(upload({ paths: ["/tmp/a.csv", "/tmp/b.csv"] }));
      yield* upload({ paths: ["/tmp/a.csv"] });
      expect(setFiles).toHaveBeenCalledExactlyOnceWith(["/tmp/a.csv"], expect.anything());
      expect((yield* status).fileChooser).toBeNull();

      const setInputFiles = vi.fn(async () => {});
      page.locator.mockReturnValue({ setInputFiles } as never);
      yield* upload({ paths: ["/tmp/b.csv"], locator: "input[type=file]" });
      expect(page.locator).toHaveBeenLastCalledWith("input[type=file]");
      expect(setInputFiles).toHaveBeenCalledWith(["/tmp/b.csv"], expect.anything());
    }),
  ).pipe(Effect.provide(layer)),
);

it.live("viewers see the agent's pointer move to its target and click there", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { browser, broker, tabId } = yield* ready;
      const page = contexts[0]!.page;
      const viewer = yield* browser.attachViewer(viewerInput(tabId, false));
      const click = vi.fn(async () => {});
      page.locator.mockReturnValue({
        scrollIntoViewIfNeeded: async () => {},
        boundingBox: async () => ({ x: 100, y: 40, width: 80, height: 20 }),
        click,
      } as never);
      yield* broker.invoke({ scope, tabId, operation: "click", input: { locator: "#go" } });
      const pointers = (yield* Queue.clear(viewer.output)).filter(
        (item) => item._tag === "pointer",
      );
      expect(pointers).toEqual([
        expect.objectContaining({ phase: "move", x: 140, y: 50 }),
        expect.objectContaining({ phase: "click", x: 140, y: 50 }),
      ]);
      expect(click).toHaveBeenCalledOnce();
    }),
  ).pipe(Effect.provide(layer)),
);

it.live("drives the desktop's own page for a tab the desktop renders", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const browser = yield* ServerBrowser.ServerBrowser;
      const broker = yield* Broker.PreviewAutomationBroker;
      const manager = yield* Manager.PreviewManager;
      yield* Effect.yieldNow;
      desktopRendersNext = true;
      const opened = yield* manager.open({ threadId: scope.thread.threadId, runtime: "server" });
      const viewer = yield* browser.attachViewer(viewerInput(opened.tabId, true));
      expect(desktopConnections.map((connection) => connection.endpoint)).toEqual([
        `ws://desktop/${opened.tabId}`,
      ]);
      expect(contexts).toEqual([]);
      const page = desktopConnections[0]!.context.page;
      yield* viewer.input({ type: "takeControl" });
      yield* viewer.input({ type: "resize", width: 390, height: 844 });
      expect(page.setViewportSize).not.toHaveBeenCalled();
      yield* viewer.input({ type: "releaseControl" });
      const evaluated = yield* broker.invoke({
        scope: asSession("agent-desktop"),
        operation: "status",
        input: {},
        tabId: PreviewTabId.make(opened.tabId),
      });
      expect(evaluated).toMatchObject({
        tabId: opened.tabId,
        selectedClient: { runtimeIdentity: desktopRuntimeIdentity },
      });
      yield* manager.close({ threadId: scope.thread.threadId, tabId: opened.tabId });
      yield* Effect.promise(() => desktopReleased.promise);
      expect(releasedDesktopTabs).toEqual([opened.tabId]);
      expect(page.close).not.toHaveBeenCalled();
    }),
  ).pipe(Effect.provide(layer)),
);

it.live("a desktop page the desktop takes back reconnects instead of closing", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const browser = yield* ServerBrowser.ServerBrowser;
      const manager = yield* Manager.PreviewManager;
      yield* Effect.yieldNow;
      desktopRendersNext = true;
      const opened = yield* manager.open({ threadId: scope.thread.threadId, runtime: "server" });
      const viewer = yield* browser.attachViewer(viewerInput(opened.tabId, false));
      // The server's listener subscribes in its own fiber; detach once it is there.
      yield* Effect.promise(() => desktopDetachSubscribed.promise);
      desktopDetaches.emit("detach", { threadId: scope.thread.threadId, tabId: opened.tabId });
      let end = yield* Queue.take(viewer.output);
      while (end._tag !== "reconnect" && end._tag !== "gone")
        end = yield* Queue.take(viewer.output);
      expect(end._tag).toBe("reconnect");
      expect(releasedDesktopTabs).toEqual([opened.tabId]);
      const { sessions } = yield* manager.list({ threadId: scope.thread.threadId });
      expect(sessions.map((session) => session.tabId)).toContain(opened.tabId);
      yield* browser.attachViewer(viewerInput(opened.tabId, false));
      expect(desktopConnections).toHaveLength(2);
    }),
  ).pipe(Effect.provide(layer)),
);

it.live("agents read a human's tab with no arguments and act on it only while nobody drives", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const browser = yield* ServerBrowser.ServerBrowser;
      const broker = yield* Broker.PreviewAutomationBroker;
      const manager = yield* Manager.PreviewManager;
      yield* Effect.yieldNow;
      const opened = yield* manager.open({
        threadId: scope.thread.threadId,
        url: "http://localhost:5173/mine",
        runtime: "server",
      });
      const tabId = PreviewTabId.make(opened.tabId);
      const viewer = yield* browser.attachViewer(viewerInput(tabId, true));
      const status = yield* broker.invoke<PreviewAutomationStatus>({
        scope,
        operation: "status",
        input: {},
      });
      expect(status).toMatchObject({
        tabId,
        control: { owner: "human", ownedByCaller: false },
      });
      expect(status.tabs).toEqual([
        expect.objectContaining({ tabId, owner: "human", ownedByCaller: false, visible: true }),
      ]);
      const evaluated = yield* broker
        .invoke<void>({ scope, operation: "evaluate", input: { expression: "read()" } })
        .pipe(Effect.flip);
      expect(evaluated).toMatchObject({
        _tag: "PreviewAutomationControlInterruptedError",
        reason: "humanControl",
      });
      yield* broker.invoke<PreviewAutomationSnapshot>({
        scope,
        tabId,
        operation: "snapshot",
        input: {},
      });
      const refused = yield* broker
        .invoke<void>({
          scope,
          tabId,
          operation: "navigate",
          input: { url: "http://localhost:5173/agent" },
        })
        .pipe(Effect.flip);
      expect(refused).toMatchObject({
        _tag: "PreviewAutomationControlInterruptedError",
        reason: "humanControl",
      });
      yield* viewer.input({ type: "releaseControl" });
      const released = yield* broker.invoke<PreviewAutomationStatus>({
        scope,
        tabId,
        operation: "status",
        input: {},
      });
      expect(released.control).toMatchObject({ owner: "unclaimed", ownedByCaller: false });
      yield* broker.invoke({
        scope,
        tabId,
        operation: "navigate",
        input: { url: "http://localhost:5173/agent" },
      });
      expect(contexts[0]!.page.goto).toHaveBeenCalledWith(
        "http://localhost:5173/agent",
        expect.anything(),
      );
      yield* viewer.input({ type: "takeControl" });
      const retaken = yield* broker
        .invoke<void>({ scope, tabId, operation: "press", input: { key: "Enter" } })
        .pipe(Effect.flip);
      expect(retaken).toMatchObject({ reason: "humanControl" });
    }),
  ).pipe(Effect.provide(layer)),
);

it.live("a session's own tab stays its default while other sessions' tabs are listed", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { broker, tabId } = yield* ready;
      const other = yield* broker.invoke<PreviewAutomationStatus>({
        scope: asSession("agent-b"),
        operation: "open",
        input: { reuseExistingTab: false, show: false },
      });
      const status = yield* broker.invoke<PreviewAutomationStatus>({
        scope,
        operation: "status",
        input: {},
      });
      expect(status.tabId).toBe(tabId);
      expect(status.tabs).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ tabId, owner: "agent", ownedByCaller: true }),
          expect.objectContaining({ tabId: other.tabId, owner: "agent", ownedByCaller: false }),
        ]),
      );
    }),
  ).pipe(Effect.provide(layer)),
);

it.live(
  "preview_open picks a reported profile by id or name and defaults to the reported one",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const browser = yield* ServerBrowser.ServerBrowser;
        const broker = yield* Broker.PreviewAutomationBroker;
        const manager = yield* Manager.PreviewManager;
        yield* Effect.yieldNow;
        yield* browser.reportProfiles({
          profiles: [{ id: "profile-work", name: "Work", kind: "persistent" }],
          defaultProfileId: "profile-work",
        });
        const openWith = (profileId?: string) =>
          broker.invoke<PreviewAutomationStatus>({
            scope,
            operation: "open",
            input: {
              reuseExistingTab: false,
              show: false,
              ...(profileId === undefined ? {} : { profileId }),
            },
          });
        const byDefault = yield* openWith();
        const byName = yield* openWith("WORK");
        const byId = yield* openWith("incognito");
        const { sessions } = yield* manager.list({ threadId: scope.thread.threadId });
        const profileOf = (tabId: string | null) =>
          sessions.find((session) => session.tabId === tabId)?.profileId;
        expect(profileOf(byDefault.tabId)).toBe("profile-work");
        expect(profileOf(byName.tabId)).toBe("profile-work");
        expect(profileOf(byId.tabId)).toBe("incognito");
        expect(contextRequests).toEqual([
          { profileId: "profile-work", isolated: false },
          { profileId: "profile-work", isolated: false },
          { profileId: "incognito", isolated: true },
        ]);
        expect(byDefault).toMatchObject({
          defaultProfileId: "profile-work",
          profiles: [
            { id: "default", name: "Default" },
            { id: "incognito", name: "Incognito", incognito: true },
            { id: "profile-work", name: "Work" },
          ],
        });
        const unknown = yield* openWith("Personal").pipe(Effect.flip);
        expect(unknown).toMatchObject({
          _tag: "PreviewAutomationExecutionError",
          reason:
            'No browser profile is named "Personal". Use one of: Default (id default), Incognito (id incognito), Work (id profile-work).',
        });
      }),
    ).pipe(Effect.provide(layer)),
);

it.effect("a tab the desktop does not draw is marked headless, so the desktop streams it", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { broker, tabId } = yield* ready;
      const manager = yield* Manager.PreviewManager;
      desktopRendersNext = true;
      const desktopTab = yield* broker.invoke<PreviewAutomationStatus>({
        scope,
        operation: "open",
        input: { reuseExistingTab: false, show: false },
      });
      const { sessions } = yield* manager.list({ threadId: scope.thread.threadId });
      const headlessOf = (id: string | null) =>
        sessions.find((session) => session.tabId === id)?.headless;

      expect(headlessOf(tabId)).toBe(true);
      expect(headlessOf(desktopTab.tabId)).toBeUndefined();
    }),
  ).pipe(Effect.provide(layer)),
);

it.live.each([
  { budget: "agent", limit: 8 },
  { budget: "server", limit: 32 },
])("opens and popups reserve the $budget budget before session creation", ({ budget, limit }) =>
  Effect.scoped(
    Effect.gen(function* () {
      const { broker, browser, tabId } = yield* ready;
      const manager = yield* Manager.PreviewManager;
      const open = (target = scope) =>
        broker.invoke<PreviewAutomationStatus>({
          scope: target,
          operation: "open",
          input: { reuseExistingTab: false, show: false },
        });
      for (let index = 1; index < limit - 3; index++) {
        yield* open(budget === "agent" ? scope : asSession(`other-${Math.floor(index / 8)}`));
      }

      const sessionGate = Promise.withResolvers<void>();
      const launchGate = (contextGate = Promise.withResolvers<void>());
      const sessionStarted = Promise.withResolvers<void>();
      const realOpen = manager.open;
      const delayedOpen = vi.spyOn(manager, "open").mockImplementation((input) =>
        Effect.promise(() => {
          sessionStarted.resolve();
          return sessionGate.promise;
        }).pipe(Effect.andThen(realOpen(input))),
      );
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          sessionGate.resolve();
          launchGate.resolve();
          delayedOpen.mockRestore();
        }),
      );
      const events = yield* manager.subscribeEvents;
      const first = yield* open().pipe(Effect.result, Effect.forkScoped);
      yield* Effect.promise(() => sessionStarted.promise);
      const popup = makeContext();
      const resized = Promise.withResolvers<void>();
      const setViewportSize = popup.page.setViewportSize;
      popup.page.setViewportSize = vi.fn(async (size) => {
        await setViewportSize(size);
        if (size?.width === 900) resized.resolve();
      });
      const newCDPSession = popup.newCDPSession;
      popup.newCDPSession = async (target) => {
        await launchGate.promise;
        return newCDPSession(target);
      };
      contexts[0]!.page.emit("popup", popup.page);
      const last = yield* open().pipe(Effect.result, Effect.forkScoped);
      const overflow = yield* open().pipe(Effect.result, Effect.forkScoped);
      // The host processes this after the opens, while manager creation is blocked.
      const reused = yield* broker.invoke<PreviewAutomationStatus>({
        scope,
        tabId,
        operation: "open",
        input: { show: false },
      });
      expect(reused.tabId).toBe(tabId);
      expect((yield* manager.list({ threadId: scope.thread.threadId })).sessions).toHaveLength(
        limit - 3,
      );

      sessionGate.resolve();
      for (let count = 0; count < 3; count++) {
        let event = yield* PubSub.take(events);
        while (event.type !== "opened") event = yield* PubSub.take(events);
        yield* manager.resize({
          threadId: scope.thread.threadId,
          tabId: event.tabId,
          viewport: { _tag: "freeform", width: 900, height: 700 },
        });
      }
      // The same slots must stay reserved after publication, while pages are still opening.
      const duringLaunch = yield* open().pipe(Effect.result, Effect.forkScoped);
      const rejectedPopup = makeContext();
      contexts[0]!.page.emit("popup", rejectedPopup.page);
      yield* broker.invoke({ scope, tabId, operation: "status", input: {} });
      launchGate.resolve();

      expect((yield* Fiber.join(first))._tag).toBe("Success");
      expect((yield* Fiber.join(last))._tag).toBe("Success");
      for (const fiber of [overflow, duringLaunch]) {
        expect(yield* Fiber.join(fiber)).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "PreviewAutomationControlInterruptedError", reason: "tabLimit" },
        });
      }
      yield* Effect.promise(() => resized.promise);
      const { sessions } = yield* manager.list({ threadId: scope.thread.threadId });
      for (const session of sessions)
        yield* browser.attachViewer(viewerInput(session.tabId, false));
      expect(sessions).toHaveLength(limit);
      expect(popup.page.isClosed()).toBe(false);
      expect(rejectedPopup.page.isClosed()).toBe(true);
      const status = yield* broker.invoke<PreviewAutomationStatus>({
        scope,
        tabId,
        operation: "status",
        input: {},
      });
      expect(status.tabs).toHaveLength(limit);
    }),
  ).pipe(Effect.provide(placementLayer)),
);

it.live("failed session and browser launches release capacity, as does closing a tab", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { broker, tabId } = yield* ready;
      const manager = yield* Manager.PreviewManager;
      const open = broker.invoke<PreviewAutomationStatus>({
        scope,
        operation: "open",
        input: { reuseExistingTab: false, show: false },
      });
      for (let count = 1; count < 7; count++) yield* open;
      const failSession = vi
        .spyOn(manager, "open")
        .mockImplementationOnce(() => Effect.die(new Error("session creation failed")));
      yield* Effect.addFinalizer(() => Effect.sync(() => failSession.mockRestore()));
      expect(yield* open.pipe(Effect.result)).toMatchObject({ _tag: "Failure" });
      failSession.mockRestore();
      const events = yield* manager.subscribeEvents;
      contextFailure = new Error("Chromium launch failed");
      expect(yield* open.pipe(Effect.result)).toMatchObject({ _tag: "Failure" });
      let failedSession = yield* PubSub.take(events);
      while (failedSession.type !== "opened") failedSession = yield* PubSub.take(events);
      // A failed launch must not leave a session a viewer can relaunch past the budget.
      expect((yield* manager.list({ threadId: scope.thread.threadId })).sessions).toHaveLength(7);
      contextFailure = null;
      expect((yield* open).available).toBe(true);
      expect(yield* open.pipe(Effect.flip)).toMatchObject({ reason: "tabLimit" });
      expect(
        yield* broker
          .invoke<PreviewAutomationStatus>({
            scope,
            tabId: PreviewTabId.make(failedSession.tabId),
            operation: "open",
            input: { show: false },
          })
          .pipe(Effect.flip),
      ).toMatchObject({ _tag: "PreviewAutomationTabNotFoundError" });
      const browser = yield* ServerBrowser.ServerBrowser;
      expect(
        yield* browser.attachViewer(viewerInput(failedSession.tabId, false)).pipe(Effect.flip),
      ).toMatchObject({ _tag: "ServerBrowserTabNotFoundError" });
      yield* broker.invoke({ scope, tabId, operation: "close", input: {} });
      expect((yield* open).available).toBe(true);
      const status = yield* broker.invoke<PreviewAutomationStatus>({
        scope,
        operation: "status",
        input: {},
      });
      expect(status.tabs).toHaveLength(8);
    }),
  ).pipe(Effect.provide(placementLayer)),
);

it.effect("a desktop page that loaded before the server attached reports its loaded title", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { broker } = yield* ready;
      const manager = yield* Manager.PreviewManager;
      const events = yield* manager.subscribeEvents;
      desktopRendersNext = true;
      desktopPageUrl = "https://example.com/loaded";
      const opened = yield* broker.invoke<PreviewAutomationStatus>({
        scope,
        operation: "open",
        input: { url: desktopPageUrl, reuseExistingTab: false, show: false },
      });
      let event = yield* PubSub.take(events);
      while (event.type !== "navigated" || event.tabId !== opened.tabId) {
        event = yield* PubSub.take(events);
      }

      expect(event.snapshot.navStatus).toEqual({
        _tag: "Success",
        url: "https://example.com/loaded",
        title: "test page",
      });
    }),
  ).pipe(Effect.provide(layer)),
);

it.effect("a navigation that fails while a load reads its title keeps its failure", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { tabId } = yield* ready;
      const manager = yield* Manager.PreviewManager;
      const events = yield* manager.subscribeEvents;
      const page = contexts[0]!.page;
      const heldTitle = Promise.withResolvers<string>();
      page.title.mockImplementationOnce(() => heldTitle.promise);
      yield* Effect.promise(() => page.goto("https://example.com/first"));
      const failing = {
        url: () => "https://example.com/missing",
        method: () => "GET",
        isNavigationRequest: () => true,
        frame: () => page,
        failure: () => ({ errorText: "net::ERR_NAME_NOT_RESOLVED" }),
      };
      page.emit("request", failing);
      page.emit("requestfailed", failing);
      heldTitle.resolve("Held");
      // A load at the same await depth reports after the held one, so its Success marks the end.
      page.emit("load");

      const statuses: Array<string> = [];
      while (!statuses.at(-1)?.startsWith("Success")) {
        const event = yield* PubSub.take(events);
        if (event.type === "failed" && event.tabId === tabId) {
          statuses.push(`LoadFailed ${event.url}`);
        } else if (event.type === "navigated" && event.tabId === tabId) {
          const status = event.snapshot.navStatus;
          if (status._tag === "Success") statuses.push(`Success ${status.title}`);
          else if (status._tag === "Loading") statuses.push(`Loading ${status.url}`);
        }
      }
      expect(statuses).toEqual([
        "Loading https://example.com/missing",
        "LoadFailed https://example.com/missing",
        "Success test page",
      ]);
    }),
  ).pipe(Effect.provide(layer)),
);

it.live("agent admission freezes selection before opened publication and delayed placement", () =>
  Effect.gen(function* () {
    yield* ServerBrowser.ServerBrowser;
    const broker = yield* Broker.PreviewAutomationBroker;
    const manager = yield* Manager.PreviewManager;
    const selection = yield* Selection.RenderHostSelection;
    const placement = yield* Placement.RenderPlacement;
    const admitted = Promise.withResolvers<void>();
    const resume = Promise.withResolvers<void>();
    const realPlace = placement.place;
    const delayed = vi.spyOn(placement, "place").mockImplementation((snapshot) =>
      Effect.promise(() => {
        admitted.resolve();
        return resume.promise;
      }).pipe(Effect.andThen(realPlace(snapshot))),
    );
    const bindings: unknown[] = [];
    const boundSelections = new Map<string, unknown>();
    const realSelectionBind = selection.bind;
    const recordSelection = vi.spyOn(selection, "bind").mockImplementation((key, inherited) =>
      realSelectionBind(key, inherited).pipe(
        Effect.tap((bound) =>
          Effect.sync(() => {
            boundSelections.set(key.tabId, bound);
          }),
        ),
      ),
    );
    yield* Effect.addFinalizer(() => Effect.sync(() => recordSelection.mockRestore()));
    const realOpen = manager.open;
    const observed = vi.spyOn(manager, "open").mockImplementation((input) =>
      realOpen({
        ...input,
        beforePublish: (snapshot) => {
          input.beforePublish?.(snapshot);
          bindings.push(boundSelections.get(snapshot.tabId));
        },
      }),
    );
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        resume.resolve();
        delayed.mockRestore();
        observed.mockRestore();
      }),
    );
    yield* Effect.yieldNow;
    const events = yield* manager.subscribeEvents;
    const opening = yield* broker
      .invoke<PreviewAutomationStatus>({
        scope,
        operation: "open",
        input: { reuseExistingTab: false, show: false },
      })
      .pipe(Effect.result, Effect.forkScoped);
    yield* Effect.promise(() => admitted.promise);
    let event = yield* PubSub.take(events);
    while (event.type !== "opened") event = yield* PubSub.take(events);
    const admittedBinding = yield* selection.binding(event.snapshot);
    yield* selection.setDefault({ _tag: "companion", hostId: "offline" });
    resume.resolve();
    const firstResult = yield* Fiber.join(opening);
    expect(bindings).toEqual([{ _tag: "server" }]);
    expect(admittedBinding).toEqual({ _tag: "server" });
    expect(firstResult).toMatchObject({ _tag: "Success" });
    expect(contextRequests).toHaveLength(1);
    expect(
      yield* broker
        .invoke<PreviewAutomationStatus>({
          scope,
          operation: "open",
          input: { reuseExistingTab: false, show: false },
        })
        .pipe(Effect.flip),
    ).toMatchObject({ outcome: "not_started" });
    expect((yield* manager.list({ threadId: scope.thread.threadId })).sessions).toHaveLength(1);
    yield* broker.invoke({
      scope,
      tabId: PreviewTabId.make(event.tabId),
      operation: "close",
      input: {},
    });
    // The manager mirror releases bindings before processing the following opened event.
    yield* selection.setDefault({ _tag: "server" });
    yield* broker.invoke({
      scope,
      operation: "open",
      input: { reuseExistingTab: false, show: false },
    });
    expect(yield* selection.binding(event.snapshot)).toBeUndefined();
    expect((yield* selection.thread(scope.thread.threadId)).tabs).toHaveLength(1);
  }).pipe(Effect.scoped, Effect.provide(placementLayer)),
);

it.live.each([true, false])(
  "server popup keeps its adopted page after default changes (opener binding %s)",
  (recorded) =>
    Effect.gen(function* () {
      const { broker, browser, tabId } = yield* ready;
      const manager = yield* Manager.PreviewManager;
      const selection = yield* Selection.RenderHostSelection;
      const placement = yield* Placement.RenderPlacement;
      const openerKey = { threadId: scope.thread.threadId, tabId };
      if (recorded) yield* selection.bind(openerKey);
      else yield* selection.release(openerKey);
      yield* selection.setDefault({ _tag: "companion", hostId: "other-browser" });
      const mounts = vi.spyOn(placement, "awaitCompanion");
      yield* Effect.addFinalizer(() => Effect.sync(() => mounts.mockRestore()));
      const popup = makeContext();
      const openerContext = contexts[0]!;
      popup.page.context = () => openerContext;
      const cdpTargets: unknown[] = [];
      const realCDP = openerContext.newCDPSession;
      openerContext.newCDPSession = async (target) => {
        cdpTargets.push(target);
        return realCDP(target);
      };
      const events = yield* manager.subscribeEvents;
      contexts[0]!.page.emit("popup", popup.page);
      let event = yield* PubSub.take(events);
      while (event.type !== "opened" || event.tabId === tabId) event = yield* PubSub.take(events);
      expect(yield* selection.binding(event.snapshot)).toEqual({ _tag: "server" });
      yield* browser.attachViewer(viewerInput(event.tabId, false));
      expect(popup.page.context()).toBe(openerContext);
      expect(cdpTargets).toContain(popup.page);
      expect(popup.sessions).toEqual([]);
      expect(popup.page.isClosed()).toBe(false);
      expect(mounts).not.toHaveBeenCalled();
      expect(desktopConnections).toEqual([]);
      expect(contextRequests).toHaveLength(1);
      const status = yield* broker.invoke<PreviewAutomationStatus>({
        scope,
        operation: "status",
        input: {},
      });
      expect(status.tabs).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ tabId: event.tabId, openerTabId: tabId }),
        ]),
      );
    }).pipe(Effect.scoped, Effect.provide(placementLayer)),
);

it.live("popup adoption rejects a conflicting prebind before publication and releases it", () =>
  Effect.gen(function* () {
    const { tabId } = yield* ready;
    const manager = yield* Manager.PreviewManager;
    const selection = yield* Selection.RenderHostSelection;
    yield* selection.setDefault({ _tag: "companion", hostId: "conflicting" });
    const placement = yield* Placement.RenderPlacement;
    let popupKey: { threadId: string; tabId: string } | undefined;
    const realOpen = manager.open;
    const conflict = vi.spyOn(manager, "open").mockImplementation((input) =>
      realOpen({
        ...input,
        beforePublish: (snapshot) => {
          popupKey = snapshot;
          placement.bindAtOpen(snapshot);
          input.beforePublish?.(snapshot);
        },
      }),
    );
    yield* Effect.addFinalizer(() => Effect.sync(() => conflict.mockRestore()));
    const popup = makeContext();
    const closed = Promise.withResolvers<void>();
    popup.page.on("close", () => closed.resolve());
    contexts[0]!.page.emit("popup", popup.page);
    yield* Effect.promise(() => closed.promise);
    expect(popupKey).toBeDefined();
    expect(yield* selection.binding(popupKey!)).toBeUndefined();
    expect(
      (yield* manager.list({ threadId: scope.thread.threadId })).sessions.map((s) => s.tabId),
    ).toEqual([tabId]);
    expect(desktopConnections).toEqual([]);
    expect(popup.sessions).toEqual([]);
  }).pipe(Effect.scoped, Effect.provide(placementLayer)),
);

it.live("companion popup is explicitly refused without a second mount or mixed CDP adoption", () =>
  Effect.gen(function* () {
    yield* ServerBrowser.ServerBrowser;
    const selection = yield* Selection.RenderHostSelection;
    const registry = yield* CompanionRegistry.CompanionHostRegistry;
    const placement = yield* Placement.RenderPlacement;
    const manager = yield* Manager.PreviewManager;
    const online = vi.spyOn(registry, "status").mockImplementation((hostId) =>
      Effect.succeed({
        hostId,
        label: "Mini",
        platform: "synthetic",
        online: true,
        lastSeenAt: 0,
        runtimeIdentity: desktopRuntimeIdentity,
        connectionGeneration: 1,
        capabilities: {
          cdp: true,
          clipboardText: true,
          uploads: false,
          downloads: false,
          recording: false,
        },
      }),
    );
    const mount = vi
      .spyOn(placement, "awaitCompanion")
      .mockImplementationOnce(() => Effect.void)
      .mockImplementation(() =>
        Effect.fail(
          new CompanionRegistry.CompanionHostUnavailable({
            hostId: "mini",
            label: "Mini",
            state: "timeout",
          }),
        ),
      );
    const endpoint = vi
      .spyOn(placement.channel, "endpoint")
      .mockImplementation((key) => Effect.succeed(`ws://companion/${key.tabId}`));
    const evidence = vi
      .spyOn(placement.channel, "runtimeEvidence")
      .mockReturnValue(
        Effect.succeed({ runtimeIdentity: desktopRuntimeIdentity, attachmentGeneration: 1 }),
      );
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        online.mockRestore();
        mount.mockRestore();
        endpoint.mockRestore();
        evidence.mockRestore();
      }),
    );
    yield* selection.setDefault({ _tag: "companion", hostId: "mini" });
    const { tabId } = yield* ready;
    expect(desktopConnections).toHaveLength(1);
    expect(mount).toHaveBeenCalledTimes(1);
    const popup = makeContext();
    const closed = Promise.withResolvers<void>();
    popup.page.on("close", () => closed.resolve());
    const order: unknown[] = [];
    const inheritedSelections: unknown[] = [];
    const realSelectionBind = selection.bind;
    const recordSelection = vi.spyOn(selection, "bind").mockImplementation((key, inherited) => {
      inheritedSelections.push(inherited);
      return realSelectionBind(key, inherited);
    });
    yield* Effect.addFinalizer(() => Effect.sync(() => recordSelection.mockRestore()));
    const realBind = placement.bindAtOpen;
    const bind = vi.spyOn(placement, "bindAtOpen").mockImplementation((key, opener) => {
      order.push({ opener });
      return realBind(key, opener);
    });
    yield* Effect.addFinalizer(() => Effect.sync(() => bind.mockRestore()));
    desktopConnections[0]!.context.page.emit("popup", popup.page);
    yield* Effect.promise(() => closed.promise);
    expect(order).toEqual([{ opener: { threadId: scope.thread.threadId, tabId } }]);
    expect(inheritedSelections[0]).toEqual({ _tag: "companion", hostId: "mini" });
    expect(mount).toHaveBeenCalledTimes(1);
    expect(desktopConnections).toHaveLength(1);
    expect(contextRequests).toEqual([]);
    expect(popup.sessions).toEqual([]);
    expect(
      (yield* manager.list({ threadId: scope.thread.threadId })).sessions.map((s) => s.tabId),
    ).toEqual([tabId]);
    expect((yield* selection.thread(scope.thread.threadId)).tabs.map((s) => s.tabId)).toEqual([
      tabId,
    ]);
  }).pipe(Effect.scoped, Effect.provide(placementLayer)),
);

it.live("failed client placement releases its binding and cannot retry on a changed default", () =>
  Effect.gen(function* () {
    yield* ServerBrowser.ServerBrowser;
    const manager = yield* Manager.PreviewManager;
    const selection = yield* Selection.RenderHostSelection;
    const placement = yield* Placement.RenderPlacement;
    yield* selection.setDefault({ _tag: "companion", hostId: "offline-client-host" });
    const events = yield* manager.subscribeEvents;
    const opened = yield* manager.open({
      threadId: scope.thread.threadId,
      runtime: "server",
      beforePublish: (snapshot) => {
        placement.bindAtOpen(snapshot);
      },
    });
    let event = yield* PubSub.take(events);
    while (event.type !== "closed" || event.tabId !== opened.tabId)
      event = yield* PubSub.take(events);
    expect(yield* selection.binding(opened)).toBeUndefined();
    yield* selection.setDefault({ _tag: "server" });
    const browser = yield* ServerBrowser.ServerBrowser;
    expect(
      yield* browser.attachViewer(viewerInput(opened.tabId, false)).pipe(Effect.flip),
    ).toMatchObject({ _tag: "ServerBrowserTabNotFoundError" });
    expect(contextRequests).toEqual([]);
    expect(desktopConnections).toEqual([]);
    expect((yield* manager.list({ threadId: scope.thread.threadId })).sessions).toEqual([]);
  }).pipe(Effect.scoped, Effect.provide(placementLayer)),
);

// Keep placement/channel seams synthetic while exercising the real manager mirror and tab lifecycle.
const companionFixture = Effect.gen(function* () {
  const registry = yield* CompanionRegistry.CompanionHostRegistry;
  const placement = yield* Placement.RenderPlacement;
  let online = true;
  const status = vi.spyOn(registry, "status").mockImplementation((hostId) =>
    Effect.sync(() => ({
      hostId,
      label: "Mini",
      platform: "synthetic",
      online,
      lastSeenAt: 0,
      runtimeIdentity: desktopRuntimeIdentity,
      connectionGeneration: 1,
      capabilities: {
        cdp: true,
        clipboardText: true,
        uploads: false,
        downloads: false,
        recording: false,
      },
    })),
  );
  const mount = vi.spyOn(placement, "awaitCompanion").mockReturnValue(Effect.void);
  const endpoint = vi
    .spyOn(placement.channel, "endpoint")
    .mockImplementation((key) => Effect.succeed(`ws://companion/${key.tabId}`));
  const evidence = vi
    .spyOn(placement.channel, "runtimeEvidence")
    .mockReturnValue(
      Effect.succeed({ runtimeIdentity: desktopRuntimeIdentity, attachmentGeneration: 1 }),
    );
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      status.mockRestore();
      mount.mockRestore();
      endpoint.mockRestore();
      evidence.mockRestore();
    }),
  );
  return {
    mount,
    setOnline: (value: boolean) => {
      online = value;
    },
  };
});

it.live.each(["client", "human popup"] as const)(
  "failed initial %s admission stays inert behind a held production mirror",
  (kind) =>
    Effect.gen(function* () {
      const browser = yield* ServerBrowser.ServerBrowser;
      const manager = yield* Manager.PreviewManager;
      const selection = yield* Selection.RenderHostSelection;
      const placement = yield* Placement.RenderPlacement;
      const companion = yield* companionFixture;
      const openClient = () =>
        manager.open({
          threadId: scope.thread.threadId,
          runtime: "server",
          beforePublish: (snapshot) => {
            placement.bindAtOpen(snapshot);
          },
        });
      let opener: Awaited<Effect.Success<ReturnType<typeof openClient>>> | undefined;
      if (kind === "human popup") {
        yield* selection.setDefault({ _tag: "companion", hostId: "mini" });
        opener = yield* openClient();
        yield* browser.attachViewer(viewerInput(opener.tabId, false));
      }
      yield* selection.setDefault({ _tag: "server" });
      const held = Promise.withResolvers<void>();
      const resume = Promise.withResolvers<void>();
      const drained = Promise.withResolvers<void>();
      let first = true;
      let markerId: string | undefined;
      const realPlace = placement.place;
      const places = vi.spyOn(placement, "place").mockImplementation((snapshot) => {
        if (first) {
          first = false;
          return Effect.promise(() => {
            held.resolve();
            return resume.promise;
          }).pipe(Effect.andThen(realPlace(snapshot)));
        }
        if (snapshot.tabId === markerId) drained.resolve();
        return realPlace(snapshot);
      });
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          resume.resolve();
          places.mockRestore();
        }),
      );
      const releases = vi.spyOn(placement, "release");
      const closes = vi.spyOn(manager, "close");
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          releases.mockRestore();
          closes.mockRestore();
        }),
      );
      yield* openClient();
      yield* Effect.promise(() => held.promise);
      yield* selection.setDefault({ _tag: "companion", hostId: "mini" });
      const events = yield* manager.subscribeEvents;
      let failedId: string;
      if (kind === "client") {
        companion.setOnline(false);
        const failed = yield* openClient();
        failedId = failed.tabId;
        const errors = yield* Effect.all(
          [
            browser.attachViewer(viewerInput(failedId, false)).pipe(Effect.flip),
            browser.attachViewer(viewerInput(failedId, false)).pipe(Effect.flip),
          ],
          { concurrency: "unbounded" },
        );
        expect(errors).toEqual([
          expect.objectContaining({ _tag: "ServerBrowserLaunchError" }),
          expect.objectContaining({ _tag: "ServerBrowserLaunchError" }),
        ]);
      } else {
        const popup = makeContext();
        const closed = Promise.withResolvers<void>();
        popup.page.on("close", () => closed.resolve());
        desktopConnections[0]!.context.page.emit("popup", popup.page);
        let event = yield* PubSub.take(events);
        while (event.type !== "opened") event = yield* PubSub.take(events);
        failedId = event.tabId;
        yield* Effect.promise(() => closed.promise);
        expect(popup.sessions).toEqual([]);
      }
      let event = yield* PubSub.take(events);
      while (event.type !== "closed" || event.tabId !== failedId)
        event = yield* PubSub.take(events);
      expect(
        yield* selection.binding({ threadId: scope.thread.threadId, tabId: failedId }),
      ).toBeUndefined();
      yield* selection.setDefault({ _tag: "server" });
      const marker = yield* openClient();
      markerId = marker.tabId;
      resume.resolve();
      yield* Effect.promise(() => drained.promise);
      yield* browser.attachViewer(viewerInput(markerId, false));
      expect(places.mock.calls.filter(([s]) => s.tabId === failedId)).toHaveLength(1);
      expect(contextRequests).toHaveLength(2);
      expect(desktopConnections).toHaveLength(opener ? 1 : 0);
      expect(companion.mount).toHaveBeenCalledTimes(opener ? 1 : 0);
      expect(closes.mock.calls.filter(([s]) => s.tabId === failedId)).toHaveLength(1);
      expect(releases.mock.calls.filter(([s]) => s.tabId === failedId)).toHaveLength(1);
      expect(
        (yield* manager.list({ threadId: scope.thread.threadId })).sessions.some(
          (s) => s.tabId === failedId,
        ),
      ).toBe(false);
      expect(
        (yield* selection.thread(scope.thread.threadId)).tabs.some((s) => s.tabId === failedId),
      ).toBe(false);
    }).pipe(Effect.scoped, Effect.provide(placementLayer)),
);

const companionTransport = Effect.gen(function* () {
  const registry = yield* CompanionRegistry.CompanionHostRegistry;
  const input = yield* Queue.unbounded<ReadonlyArray<string>>();
  const output = yield* Queue.unbounded<CompanionDown>();
  const connection = yield* registry
    .connect({
      read: Queue.take(input).pipe(
        Effect.mapError((cause) => new CompanionRegistry.CompanionTransportError({ cause })),
      ),
      write: (text) => Queue.offer(output, JSON.parse(text) as CompanionDown).pipe(Effect.asVoid),
      close: () => Effect.void,
      writableLength: () => 0,
    })
    .pipe(Effect.scoped, Effect.forkScoped);
  const send = (message: CompanionUp) => Queue.offer(input, [JSON.stringify(message)]);
  yield* send({
    type: "hello",
    protocol: 1,
    hostId: "mini",
    label: "Mini",
    platform: "synthetic",
    runtimeIdentity: desktopRuntimeIdentity,
    capabilities: {
      cdp: true,
      clipboardText: true,
      uploads: false,
      downloads: false,
      recording: false,
    },
  });
  expect((yield* Queue.take(output)).type).toBe("welcome");
  const assignments = yield* Queue.take(output);
  if (assignments.type !== "assignments") throw new Error("Expected reconnect assignments");
  const attach = (key: { threadId: string; tabId: string }) =>
    send({
      type: "browser",
      event: {
        ...key,
        type: "attached",
        runtimeIdentity: desktopRuntimeIdentity,
      },
    });
  yield* Effect.forEach(assignments.tabs, attach);
  yield* Stream.fromQueue(output).pipe(
    Stream.runForEach((message) =>
      message.type === "mount" ? attach(message).pipe(Effect.asVoid) : Effect.void,
    ),
    Effect.forkScoped,
  );
  return { disconnect: Fiber.interrupt(connection), assignments: assignments.tabs };
});

it.live(
  "established companion tab survives an offline viewer retry and reconnects on its pinned host",
  () =>
    Effect.gen(function* () {
      const browser = yield* ServerBrowser.ServerBrowser;
      const manager = yield* Manager.PreviewManager;
      const registry = yield* CompanionRegistry.CompanionHostRegistry;
      const selection = yield* Selection.RenderHostSelection;
      const placement = yield* Placement.RenderPlacement;
      const endpoint = vi
        .spyOn(placement.channel, "endpoint")
        .mockImplementation((key) => Effect.succeed(`ws://companion/${key.tabId}`));
      yield* Effect.addFinalizer(() => Effect.sync(() => endpoint.mockRestore()));
      const companion = yield* companionTransport;
      yield* selection.setDefault({ _tag: "companion", hostId: "mini" });
      const opened = yield* manager.open({
        threadId: scope.thread.threadId,
        runtime: "server",
        beforePublish: (snapshot) => {
          placement.bindAtOpen(snapshot);
        },
      });
      const viewer = yield* browser.attachViewer(viewerInput(opened.tabId, false));
      const release = vi.spyOn(placement, "release");
      const unassign = vi.spyOn(registry, "unassign");
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          release.mockRestore();
          unassign.mockRestore();
        }),
      );
      yield* companion.disconnect;
      let output = yield* Queue.take(viewer.output);
      while (output._tag !== "reconnect") output = yield* Queue.take(viewer.output);
      yield* selection.setDefault({ _tag: "server" });
      const retries = yield* Effect.all(
        [
          browser.attachViewer(viewerInput(opened.tabId, false)).pipe(Effect.flip),
          browser.attachViewer(viewerInput(opened.tabId, false)).pipe(Effect.flip),
        ],
        { concurrency: "unbounded" },
      );
      for (const error of retries)
        expect(error).toMatchObject({
          _tag: "ServerBrowserLaunchError",
          cause: { _tag: "CompanionHostUnavailable", state: "offline", outcome: "not_started" },
        });
      expect(
        (yield* manager.list({ threadId: scope.thread.threadId })).sessions.map((s) => s.tabId),
      ).toEqual([opened.tabId]);
      expect(yield* selection.binding(opened)).toEqual({ _tag: "companion", hostId: "mini" });
      expect((yield* selection.thread(scope.thread.threadId)).tabs.map((s) => s.tabId)).toEqual([
        opened.tabId,
      ]);
      expect(release).not.toHaveBeenCalled();
      expect(unassign).not.toHaveBeenCalled();
      const returned = yield* companionTransport;
      expect(returned.assignments).toEqual([{ threadId: opened.threadId, tabId: opened.tabId }]);
      yield* browser.attachViewer(viewerInput(opened.tabId, false));
      expect(desktopConnections.map((c) => c.endpoint)).toEqual([
        `ws://companion/${opened.tabId}`,
        `ws://companion/${opened.tabId}`,
      ]);
      expect(contextRequests).toEqual([]);
      expect(
        (yield* manager.list({ threadId: scope.thread.threadId })).sessions.map((s) => s.tabId),
      ).toEqual([opened.tabId]);
    }).pipe(Effect.scoped, Effect.provide(placementLayer)),
);

it.live.each([true, false])(
  "closing a pending reconnect releases once after settlement (host online %s)",
  (online) =>
    Effect.gen(function* () {
      const browser = yield* ServerBrowser.ServerBrowser;
      const manager = yield* Manager.PreviewManager;
      const selection = yield* Selection.RenderHostSelection;
      const placement = yield* Placement.RenderPlacement;
      const companion = yield* companionFixture;
      yield* selection.setDefault({ _tag: "companion", hostId: "mini" });
      const opened = yield* manager.open({
        threadId: scope.thread.threadId,
        runtime: "server",
        beforePublish: (snapshot) => {
          placement.bindAtOpen(snapshot);
        },
      });
      const viewer = yield* browser.attachViewer(viewerInput(opened.tabId, false));
      yield* Effect.promise(() => desktopDetachSubscribed.promise);
      desktopDetaches.emit("detach", opened);
      let output = yield* Queue.take(viewer.output);
      while (output._tag !== "reconnect") output = yield* Queue.take(viewer.output);
      companion.setOnline(online);
      const held = Promise.withResolvers<void>();
      const resume = Promise.withResolvers<void>();
      const drained = Promise.withResolvers<void>();
      const realPlace = placement.place;
      const places = vi.spyOn(placement, "place").mockImplementation((snapshot) =>
        snapshot.tabId === opened.tabId
          ? Effect.promise(() => {
              held.resolve();
              return resume.promise;
            }).pipe(Effect.andThen(realPlace(snapshot)))
          : realPlace(snapshot).pipe(Effect.tap(() => Effect.sync(() => drained.resolve()))),
      );
      const releases = vi.spyOn(placement, "release");
      const closes = vi.spyOn(manager, "close");
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          resume.resolve();
          places.mockRestore();
          releases.mockRestore();
          closes.mockRestore();
        }),
      );
      const retry = yield* browser
        .attachViewer(viewerInput(opened.tabId, false))
        .pipe(Effect.result, Effect.forkScoped);
      yield* Effect.promise(() => held.promise);
      yield* manager.close({
        threadId: scope.thread.threadId,
        tabId: PreviewTabId.make(opened.tabId),
      });
      yield* selection.setDefault({ _tag: "server" });
      const replacement = yield* manager.open({
        threadId: scope.thread.threadId,
        runtime: "server",
        beforePublish: (snapshot) => {
          placement.bindAtOpen(snapshot);
        },
      });
      yield* Effect.promise(() => drained.promise);
      expect(releases.mock.calls.filter(([s]) => s.tabId === opened.tabId)).toHaveLength(0);
      resume.resolve();
      expect(yield* Fiber.join(retry)).toMatchObject({ _tag: "Failure" });
      yield* browser.attachViewer(viewerInput(replacement.tabId, false));
      expect(releases.mock.calls.filter(([s]) => s.tabId === opened.tabId)).toHaveLength(1);
      expect(closes.mock.calls.filter(([s]) => s.tabId === opened.tabId)).toHaveLength(1);
      expect(yield* selection.binding(opened)).toBeUndefined();
      expect(yield* selection.binding(replacement)).toEqual({ _tag: "server" });
      expect(
        (yield* manager.list({ threadId: scope.thread.threadId })).sessions.map((s) => s.tabId),
      ).toEqual([replacement.tabId]);
      expect((yield* selection.thread(scope.thread.threadId)).tabs.map((s) => s.tabId)).toEqual([
        replacement.tabId,
      ]);
      expect(contextRequests).toHaveLength(1);
      if (online) expect(desktopConnections[1]!.context.page.goto).not.toHaveBeenCalled();
    }).pipe(Effect.scoped, Effect.provide(placementLayer)),
);

it.live(
  "REC-R registered logical retirement releases a desktop pin without claiming page cessation",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const browser = yield* ServerBrowser.ServerBrowser;
        const broker = yield* Broker.PreviewAutomationBroker;
        const manager = yield* Manager.PreviewManager;
        yield* Effect.yieldNow;
        desktopRendersNext = true;
        const opened = yield* manager.open({ threadId: scope.thread.threadId, runtime: "server" });
        yield* browser.attachViewer(viewerInput(opened.tabId, true));
        const tabId = PreviewTabId.make(opened.tabId);
        const status = yield* broker.invoke({ scope, operation: "status", tabId, input: {} });
        expect(status).toMatchObject({
          selectedClient: { runtimeIdentity: desktopRuntimeIdentity },
        });
        const page = desktopConnections[0]!.context.page;
        yield* manager.close({ threadId: scope.thread.threadId, tabId });
        yield* Effect.promise(() => desktopReleased.promise);
        const missing = yield* broker
          .invoke({ scope, operation: "snapshot", tabId, input: {} })
          .pipe(Effect.flip, Effect.orDie);
        expect(missing).toMatchObject({ _tag: "PreviewAutomationTabNotFoundError", tabId });
        expect(page.close).not.toHaveBeenCalled();
      }),
    ).pipe(Effect.provide(layer)),
);
