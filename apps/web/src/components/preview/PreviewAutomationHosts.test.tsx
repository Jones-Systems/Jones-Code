import {
  DEFAULT_CLIENT_SETTINGS,
  EnvironmentId,
  ThreadId,
  type ClientSettings,
  type PreviewAutomationResponse,
  type PreviewAutomationRuntimeIdentity,
  type PreviewAutomationHost,
  type PreviewAutomationRequest,
  type PreviewAutomationStreamEvent,
  type PreviewListResult,
  type PreviewOpenInput,
  type PreviewSessionSnapshot,
} from "@t3tools/contracts";
import type { AtomCommandResult } from "@t3tools/client-runtime/state/runtime";
import * as Cause from "effect/Cause";
import { AsyncResult, Atom } from "effect/unstable/reactivity";
import { act } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

import { useBrowserSurfaceStore } from "~/browser/browserSurfaceStore";
import { previewRuntimeTabId } from "~/browser/previewRuntimeTabId";
import { __resetClientSettingsPersistenceForTests } from "~/hooks/useSettings";
import {
  applyPreviewServerSnapshot,
  readThreadPreviewState,
  resetPreviewStateForTests,
  applyPreviewDesktopState,
  reconcilePreviewServerSessions,
} from "~/previewStateStore";
import { appAtomRegistry, AppAtomRegistryProvider } from "~/rpc/atomRegistry";

import { PreviewAutomationHosts } from "./PreviewAutomationHosts";

const mocks = vi.hoisted(() => ({
  environments: [] as Array<{ environmentId: EnvironmentId }>,
  automationRequests:
    vi.fn<
      (target: {
        environmentId: EnvironmentId;
        input: PreviewAutomationHost;
      }) => typeof requestsAtom
    >(),
  getClientSettings: vi.fn<() => Promise<ClientSettings | null>>(),
  setClientSettings: vi.fn(),
  open: vi.fn(async (_target: { environmentId: EnvironmentId; input: PreviewOpenInput }) =>
    AsyncResult.success(snapshot),
  ),
  list: vi.fn<() => Promise<AtomCommandResult<PreviewListResult, Error>>>(),
  status: vi.fn(async () => ({ available: true, tabId: snapshot.tabId, loading: false })),
  type: vi.fn(async () => ({ typed: true })),
  activeRecordings: vi.fn<() => Array<{ runtimeTabId: string; serverTabId: string }>>(),
  stopRecording: vi.fn(async () => ({ path: "synthetic-recording.webm" })),
  resize: vi.fn(),
  respond:
    vi.fn<
      (target: {
        environmentId: EnvironmentId;
        input: PreviewAutomationResponse;
      }) => Promise<void | AtomCommandResult<void, Error>>
    >(),
  focus: vi.fn<() => Promise<AtomCommandResult<void, Error>>>(),
}));

vi.mock("~/localApi", () => ({
  ensureLocalApi: () => ({ persistence: mocks }),
}));
vi.mock("~/env", () => ({ isElectron: true }));
vi.mock("~/state/environments", () => ({
  useEnvironments: () => ({ environments: mocks.environments }),
}));
vi.mock("~/state/preview", () => ({
  previewEnvironment: {
    automationRequests: mocks.automationRequests,
    list: () => listAtom,
    open: mocks.open,
    resize: mocks.resize,
    respondToAutomation: mocks.respond,
    focusAutomationHost: mocks.focus,
  },
}));
vi.mock("~/state/use-atom-command", () => ({
  useAtomCommand: (command: unknown) => command,
}));
vi.mock("~/state/use-atom-query-runner", () => ({
  useAtomQueryRunner: () => mocks.list,
}));
vi.mock("./previewBridge", () => ({
  previewBridge: { automation: { status: mocks.status, type: mocks.type } },
}));
vi.mock("~/browser/browserRecording", () => ({
  readActiveBrowserRecordingTargets: mocks.activeRecordings,
  startBrowserRecording: vi.fn(),
  stopBrowserRecording: mocks.stopRecording,
  stopBrowserRecordingForUpload: vi.fn(),
}));

const environmentId = EnvironmentId.make("automation-environment");
const threadId = ThreadId.make("automation-thread");
const threadRef = { environmentId, threadId };
const viewport = { _tag: "freeform", width: 1440, height: 900 } as const;
const savedSettings: ClientSettings = {
  ...DEFAULT_CLIENT_SETTINGS,
  browserDefaultViewport: viewport,
  browserDefaultProfileId: "work",
  browserProfiles: [{ id: "work", name: "Work", kind: "persistent" }],
  browserAutoShowFloatingPreview: false,
};
const snapshot: PreviewSessionSnapshot = {
  threadId,
  tabId: "automation-tab",
  navStatus: { _tag: "Idle" },
  canGoBack: false,
  canGoForward: false,
  viewport,
  profileId: "work",
  updatedAt: "2026-09-05T00:00:00.000Z",
};
const emptyList = { sessions: [], serverEpoch: "test-server", revision: 0 };
const listAtom = Atom.make(AsyncResult.success(emptyList));
let requestsAtom = Atom.make<AsyncResult.AsyncResult<PreviewAutomationStreamEvent, Error>>(
  AsyncResult.initial(false),
);
const requestEvent: PreviewAutomationStreamEvent = {
  type: "request",
  connectionId: "automation-connection",
  request: {
    requestId: "open-request",
    threadId,
    operation: "open",
    input: { open: false, reuseExistingTab: false },
    timeoutMs: 15_000,
  },
};

function deferred<A>() {
  let resolve!: (value: A) => void;
  const promise = new Promise<A>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}

let renderer: ReactTestRenderer | null = null;

beforeEach(async () => {
  vi.clearAllMocks();
  requestsAtom = Atom.make<AsyncResult.AsyncResult<PreviewAutomationStreamEvent, Error>>(
    AsyncResult.initial(false),
  );
  mocks.environments = [{ environmentId }];
  mocks.automationRequests.mockReset().mockReturnValue(requestsAtom);
  mocks.getClientSettings.mockReset().mockResolvedValue(savedSettings);
  mocks.open.mockReset().mockResolvedValue(AsyncResult.success(snapshot));
  mocks.list.mockReset().mockResolvedValue(AsyncResult.success(emptyList));
  mocks.status.mockReset().mockResolvedValue({
    available: true,
    tabId: snapshot.tabId,
    loading: false,
  });
  mocks.type.mockReset().mockResolvedValue({ typed: true });
  mocks.activeRecordings.mockReset().mockReturnValue([]);
  mocks.stopRecording.mockReset().mockResolvedValue({ path: "synthetic-recording.webm" });
  mocks.respond.mockReset();
  mocks.focus.mockReset().mockResolvedValue(AsyncResult.success(undefined));
  __resetClientSettingsPersistenceForTests();
  resetPreviewStateForTests();
  useBrowserSurfaceStore.setState({ byTabId: {}, activityByTabId: {} });
  appAtomRegistry.set(requestsAtom, AsyncResult.initial(false));
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.stubGlobal("window", { addEventListener: vi.fn(), removeEventListener: vi.fn() });
  vi.stubGlobal(
    "document",
    Object.assign(new EventTarget(), {
      hasFocus: () => false,
      visibilityState: "visible",
      querySelectorAll: () => [],
    }),
  );
  await act(() => {
    renderer = create(
      <AppAtomRegistryProvider>
        <PreviewAutomationHosts />
      </AppAtomRegistryProvider>,
    );
  });
});

afterEach(async () => {
  await act(async () => {
    renderer?.unmount();
    if (vi.isFakeTimers()) await vi.runOnlyPendingTimersAsync();
  });
  renderer = null;
  resetPreviewStateForTests();
  __resetClientSettingsPersistenceForTests();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

const liveList: PreviewListResult = {
  sessions: [snapshot],
  serverEpoch: "test-server",
  revision: 1,
};

function retainBackgroundPreview() {
  reconcilePreviewServerSessions(threadRef, liveList);
  applyPreviewDesktopState(threadRef, snapshot.tabId, {
    hasWebContents: true,
    canGoBack: false,
    canGoForward: false,
    loading: false,
    zoomFactor: 1,
    pictureInPicture: false,
    colorScheme: "system",
    audioMuted: false,
    audible: false,
    controller: "none",
    favicon: null,
  });
  const runtimeTabId = previewRuntimeTabId(threadRef, liveList.serverEpoch, snapshot.tabId);
  Object.assign(document, {
    querySelectorAll: () => [
      {
        getAttribute: (name: string) => (name === "data-preview-tab" ? runtimeTabId : null),
        closest: () => ({ getAttribute: () => "active" }),
        executeJavaScript: async () => ({ width: viewport.width, height: viewport.height }),
      },
    ],
  });
  return runtimeTabId;
}

const targetedRequest = (
  overrides: Partial<PreviewAutomationRequest> = {},
): PreviewAutomationStreamEvent => ({
  ...requestEvent,
  request: {
    ...requestEvent.request,
    requestId: "background-request",
    operation: "type",
    input: { text: "synthetic input" },
    tabId: snapshot.tabId,
    tabIdExplicit: true,
    ...overrides,
  },
});

async function sendRequest(event: PreviewAutomationStreamEvent) {
  const response = deferred<PreviewAutomationResponse>();
  mocks.respond.mockImplementationOnce(async ({ input }) => response.resolve(input));
  await act(async () => {
    appAtomRegistry.set(requestsAtom, AsyncResult.success(event));
    await response.promise;
  });
  return response.promise;
}

describe("PreviewAutomationHosts background target freshness", () => {
  it.each([true, false])(
    "rejects a cached closed tab before dispatch (explicit target: %s)",
    async (tabIdExplicit) => {
      await act(() => retainBackgroundPreview());
      const response = await sendRequest(targetedRequest({ tabIdExplicit }));

      expect(response).toMatchObject({
        ok: false,
        error: { _tag: "PreviewAutomationTabNotFoundError", outcome: "not_started" },
      });
      expect(mocks.list).toHaveBeenCalledExactlyOnceWith({
        environmentId,
        input: { threadId },
      });
      expect(mocks.status).not.toHaveBeenCalled();
      expect(mocks.type).not.toHaveBeenCalled();
      expect(mocks.open).not.toHaveBeenCalled();
    },
  );

  it("rejects the same textual tab id from a new server epoch before dispatch", async () => {
    await act(() => retainBackgroundPreview());
    mocks.list.mockResolvedValueOnce(
      AsyncResult.success({ ...liveList, serverEpoch: "replacement-server" }),
    );
    const response = await sendRequest(targetedRequest());

    expect(response).toMatchObject({
      ok: false,
      error: { _tag: "PreviewAutomationTabNotFoundError", outcome: "not_started" },
    });
    expect(mocks.status).not.toHaveBeenCalled();
    expect(mocks.type).not.toHaveBeenCalled();
    expect(mocks.open).not.toHaveBeenCalled();
  });

  it("dispatches once to a fresh valid background target", async () => {
    vi.useFakeTimers();
    let runtimeTabId = "";
    await act(() => {
      runtimeTabId = retainBackgroundPreview();
    });
    mocks.list.mockResolvedValueOnce(AsyncResult.success(liveList));
    const response = await sendRequest(targetedRequest());

    expect(response).toMatchObject({ ok: true, result: { typed: true } });
    expect(mocks.list).toHaveBeenCalledOnce();
    expect(mocks.type).toHaveBeenCalledExactlyOnceWith(
      runtimeTabId,
      { text: "synthetic input" },
      expect.any(Number),
    );
    expect(useBrowserSurfaceStore.getState().activityByTabId).toEqual({});
    expect(vi.getTimerCount()).toBe(0);
  });

  it("reports a rejected freshness read before any browser call", async () => {
    vi.useFakeTimers();
    await act(() => retainBackgroundPreview());
    mocks.list.mockRejectedValueOnce(new Error("Synthetic freshness read failed"));
    const response = await sendRequest(targetedRequest());

    expect(response).toMatchObject({
      ok: false,
      error: { _tag: "PreviewAutomationExecutionError", outcome: "not_started" },
    });
    expect(mocks.status).not.toHaveBeenCalled();
    expect(mocks.type).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each([undefined, true])(
    "does not create a replacement for an uncached requested tab (reuse: %s)",
    async (reuseExistingTab) => {
      mocks.list.mockResolvedValueOnce(AsyncResult.success(liveList));
      const response = await sendRequest(
        targetedRequest({
          operation: "open",
          tabId: "missing-tab",
          input: { open: false, ...(reuseExistingTab === undefined ? {} : { reuseExistingTab }) },
        }),
      );
      expect(response).toMatchObject({
        ok: false,
        error: { _tag: "PreviewAutomationTabNotFoundError", outcome: "not_started" },
      });
      expect(mocks.open).not.toHaveBeenCalled();
      expect(mocks.status).not.toHaveBeenCalled();
    },
  );

  it("reports unavailable status when there is no previous or requested target", async () => {
    const response = await sendRequest(
      targetedRequest({ operation: "status", input: {}, tabId: undefined }),
    );
    expect(response).toMatchObject({ ok: true, result: { available: false, tabId: null } });
    expect(mocks.list).toHaveBeenCalledOnce();
    expect(mocks.status).not.toHaveBeenCalled();
  });

  it.each(["closed", "replaced"])(
    "preserves newer %s state that arrives while freshness is in flight",
    async (change) => {
      await act(() => retainBackgroundPreview());
      const pending = deferred<AtomCommandResult<PreviewListResult, Error>>();
      mocks.list.mockReturnValueOnce(pending.promise);
      const response = deferred<PreviewAutomationResponse>();
      mocks.respond.mockImplementationOnce(async ({ input }) => response.resolve(input));
      await act(async () => {
        appAtomRegistry.set(requestsAtom, AsyncResult.success(targetedRequest()));
      });
      expect(mocks.list).toHaveBeenCalledOnce();
      const currentList: PreviewListResult =
        change === "closed"
          ? { ...liveList, sessions: [], revision: 2 }
          : { ...liveList, serverEpoch: "replacement-server" };
      await act(async () => {
        reconcilePreviewServerSessions(threadRef, currentList);
        pending.resolve(AsyncResult.success(liveList));
        await response.promise;
      });
      await expect(response.promise).resolves.toMatchObject({
        ok: false,
        error: { _tag: "PreviewAutomationTabNotFoundError", outcome: "not_started" },
      });
      expect(readThreadPreviewState(threadRef).serverEpoch).toBe(currentList.serverEpoch);
      expect(readThreadPreviewState(threadRef).serverRevision).toBe(currentList.revision);
      expect(mocks.type).not.toHaveBeenCalled();
      expect(mocks.status).not.toHaveBeenCalled();
    },
  );

  it("bounds a delayed freshness read and never dispatches when it later resolves", async () => {
    vi.useFakeTimers();
    await act(() => retainBackgroundPreview());
    const pending = deferred<AtomCommandResult<PreviewListResult, Error>>();
    mocks.list.mockReturnValueOnce(pending.promise);
    const response = deferred<PreviewAutomationResponse>();
    mocks.respond.mockImplementationOnce(async ({ input }) => response.resolve(input));
    await act(async () => {
      appAtomRegistry.set(requestsAtom, AsyncResult.success(targetedRequest({ timeoutMs: 100 })));
    });
    expect(mocks.list).toHaveBeenCalledOnce();
    expect(mocks.type).not.toHaveBeenCalled();

    await act(async () => {
      await vi.advanceTimersByTimeAsync(80);
      await response.promise;
    });
    await expect(response.promise).resolves.toMatchObject({
      ok: false,
      error: { _tag: "PreviewAutomationTimeoutError", outcome: "not_started" },
    });
    await act(async () => {
      pending.resolve(AsyncResult.success({ ...liveList, serverEpoch: "late-server" }));
      await pending.promise;
    });
    expect(mocks.type).not.toHaveBeenCalled();
    expect(mocks.status).not.toHaveBeenCalled();
    expect(readThreadPreviewState(threadRef).serverEpoch).toBe(liveList.serverEpoch);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("does not dispatch a freshness read completed after a connection change", async () => {
    await act(() => retainBackgroundPreview());
    const pending = deferred<AtomCommandResult<PreviewListResult, Error>>();
    mocks.list.mockReturnValueOnce(pending.promise);
    await act(async () => {
      appAtomRegistry.set(requestsAtom, AsyncResult.success(targetedRequest()));
    });
    expect(mocks.list).toHaveBeenCalledOnce();
    await act(async () => {
      appAtomRegistry.set(
        requestsAtom,
        AsyncResult.success({ type: "connected", connectionId: "replacement-connection" }),
      );
      pending.resolve(AsyncResult.success(liveList));
      await pending.promise;
    });
    expect(mocks.type).not.toHaveBeenCalled();
    expect(mocks.status).not.toHaveBeenCalled();
    for (const [target] of mocks.respond.mock.calls) {
      expect(target.input).toMatchObject({
        ok: false,
        error: { outcome: "not_started" },
      });
    }
  });

  it("answers ping without depending on the session list", async () => {
    mocks.list.mockImplementationOnce(() => new Promise(() => {}));
    const response = await sendRequest(targetedRequest({ operation: "ping", input: {} }));
    expect(response).toMatchObject({ ok: true, result: { alive: true } });
    expect(mocks.list).not.toHaveBeenCalled();
  });

  it("creates a deliberate new tab despite a stale remembered target", async () => {
    await act(() => retainBackgroundPreview());
    const response = await sendRequest(
      targetedRequest({
        operation: "open",
        input: { open: false, reuseExistingTab: false },
        tabIdExplicit: false,
      }),
    );
    expect(response).toMatchObject({ ok: true, result: { tabId: snapshot.tabId } });
    expect(mocks.open).toHaveBeenCalledOnce();
    expect(mocks.type).not.toHaveBeenCalled();
  });

  it("finalizes a recorded target without depending on its server session", async () => {
    const runtimeTabId = previewRuntimeTabId(threadRef, "closed-server", snapshot.tabId);
    mocks.activeRecordings.mockReturnValueOnce([{ runtimeTabId, serverTabId: snapshot.tabId }]);
    mocks.list.mockRejectedValueOnce(new Error("Synthetic unavailable session list"));
    const response = await sendRequest(
      targetedRequest({ operation: "recordingStop", input: {}, tabIdExplicit: false }),
    );
    expect(response).toMatchObject({
      ok: true,
      result: { path: "synthetic-recording.webm", tabId: snapshot.tabId },
    });
    expect(mocks.stopRecording).toHaveBeenCalledExactlyOnceWith(runtimeTabId);
    expect(mocks.list).not.toHaveBeenCalled();
  });
});

describe("PreviewAutomationHosts open", () => {
  it("retries a settled RPC failure without executing the browser operation again", async () => {
    const response = deferred<PreviewAutomationResponse>();
    mocks.respond
      .mockResolvedValueOnce(AsyncResult.failure(Cause.fail(new Error("Response RPC failed"))))
      .mockImplementationOnce(async ({ input }) => response.resolve(input));
    await act(async () => {
      appAtomRegistry.set(requestsAtom, AsyncResult.success(requestEvent));
      await response.promise;
    });
    expect(mocks.respond).toHaveBeenCalledTimes(2);
    expect(mocks.respond.mock.calls[0]![0].input).toEqual(mocks.respond.mock.calls[1]![0].input);
    expect(mocks.open).toHaveBeenCalledOnce();
    await expect(response.promise).resolves.toMatchObject({ ok: true, requestId: "open-request" });
  });
  it("waits for saved settings before opening a tab with the configured profile and viewport", async () => {
    const readStarted = deferred<void>();
    const read = deferred<ClientSettings>();
    const response = deferred<PreviewAutomationResponse>();
    mocks.getClientSettings.mockImplementationOnce(() => {
      readStarted.resolve();
      return read.promise;
    });
    mocks.respond.mockImplementationOnce(async ({ input }) => response.resolve(input));

    await act(async () => {
      appAtomRegistry.set(requestsAtom, AsyncResult.success(requestEvent));
      await readStarted.promise;
    });
    expect(mocks.open).not.toHaveBeenCalled();

    await act(async () => {
      read.resolve(savedSettings);
      await response.promise;
    });

    expect(mocks.open).toHaveBeenCalledExactlyOnceWith({
      environmentId,
      input: { threadId, viewport, profileId: "work" },
    });
    expect(mocks.getClientSettings).toHaveBeenCalledOnce();
    await expect(response.promise).resolves.toMatchObject({
      requestId: "open-request",
      ok: true,
      result: { available: false, tabId: snapshot.tabId },
    });
    expect(readThreadPreviewState(threadRef).snapshot).toEqual(snapshot);
    expect(mocks.setClientSettings).not.toHaveBeenCalled();
  });

  it("reports a settings read failure without opening a tab", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    mocks.getClientSettings.mockRejectedValueOnce(new Error("Settings read failed"));
    const response = deferred<PreviewAutomationResponse>();
    mocks.respond.mockImplementationOnce(async ({ input }) => response.resolve(input));

    await act(async () => {
      appAtomRegistry.set(requestsAtom, AsyncResult.success(requestEvent));
      await response.promise;
    });

    await expect(response.promise).resolves.toMatchObject({
      requestId: "open-request",
      ok: false,
      error: { _tag: "PreviewAutomationExecutionError" },
    });
    expect(mocks.getClientSettings).toHaveBeenCalledOnce();
    expect(mocks.open).not.toHaveBeenCalled();
    expect(readThreadPreviewState(threadRef).snapshot).toBeNull();
    expect(mocks.setClientSettings).not.toHaveBeenCalled();
  });
});

describe("PreviewAutomationHosts ownership", () => {
  it("reports only local live tabs and removes ownership when their web contents close", async () => {
    await act(() => {
      appAtomRegistry.set(
        requestsAtom,
        AsyncResult.success({ type: "connected", connectionId: "automation-connection" }),
      );
      applyPreviewServerSnapshot(threadRef, snapshot);
    });
    expect(mocks.focus).toHaveBeenLastCalledWith(
      expect.objectContaining({ input: expect.objectContaining({ liveTabs: [] }) }),
    );
    const overlay = {
      hasWebContents: true,
      canGoBack: false,
      canGoForward: false,
      loading: false,
      zoomFactor: 1,
      pictureInPicture: false,
      colorScheme: "system" as const,
      audioMuted: false,
      audible: false,
      controller: "none" as const,
      favicon: null,
    };
    await act(() => applyPreviewDesktopState(threadRef, snapshot.tabId, overlay));
    expect(mocks.focus).toHaveBeenLastCalledWith(
      expect.objectContaining({
        input: expect.objectContaining({
          liveTabs: [{ threadId, tabId: snapshot.tabId, visible: false }],
        }),
      }),
    );
    const reportCount = mocks.focus.mock.calls.length;
    await act(() =>
      applyPreviewDesktopState(threadRef, snapshot.tabId, { ...overlay, loading: true }),
    );
    expect(mocks.focus).toHaveBeenCalledTimes(reportCount);
    const runtimeTabId = previewRuntimeTabId(threadRef, null, snapshot.tabId);
    const owner = Symbol();
    await act(() => {
      useBrowserSurfaceStore.getState().claim(runtimeTabId, owner, false);
      useBrowserSurfaceStore
        .getState()
        .present(runtimeTabId, owner, { x: 0, y: 0, width: 800, height: 600 }, true, 0, 1);
    });
    expect(mocks.focus).toHaveBeenLastCalledWith(
      expect.objectContaining({
        input: expect.objectContaining({
          liveTabs: [{ threadId, tabId: snapshot.tabId, visible: true }],
        }),
      }),
    );
    for (const visibilityState of ["hidden", "visible"]) {
      await act(() => {
        Object.assign(document, { visibilityState });
        document.dispatchEvent(new Event("visibilitychange"));
      });
      expect(mocks.focus).toHaveBeenLastCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            focused: false,
            liveTabs: [{ threadId, tabId: snapshot.tabId, visible: visibilityState === "visible" }],
          }),
        }),
      );
    }
    await act(() => {
      appAtomRegistry.set(
        requestsAtom,
        AsyncResult.success({ type: "connected", connectionId: "reconnected" }),
      );
    });
    expect(mocks.focus).toHaveBeenLastCalledWith(
      expect.objectContaining({
        input: expect.objectContaining({
          connectionId: "reconnected",
          liveTabs: [{ threadId, tabId: snapshot.tabId, visible: true }],
        }),
      }),
    );
    await act(() =>
      applyPreviewDesktopState(threadRef, snapshot.tabId, { ...overlay, hasWebContents: false }),
    );
    expect(mocks.focus).toHaveBeenLastCalledWith(
      expect.objectContaining({ input: expect.objectContaining({ liveTabs: [] }) }),
    );
  });

  it.each([false, true])(
    "retries failed reports without clearing newer connection reports (reconnect: %s)",
    async (reconnect) => {
      const report = deferred<Awaited<ReturnType<typeof mocks.focus>>>();
      mocks.focus.mockReturnValueOnce(report.promise);
      await act(() => {
        appAtomRegistry.set(
          requestsAtom,
          AsyncResult.success({ type: "connected", connectionId: "first" }),
        );
      });
      if (reconnect) {
        await act(() => {
          appAtomRegistry.set(
            requestsAtom,
            AsyncResult.success({ type: "connected", connectionId: "second" }),
          );
        });
      }
      await act(async () => {
        report.resolve(AsyncResult.failure(Cause.fail(new Error("Focus report failed"))));
        await report.promise;
      });
      expect(mocks.focus).toHaveBeenCalledTimes(reconnect ? 2 : 1);
      await act(() => {
        document.dispatchEvent(new Event("visibilitychange"));
      });
      expect(mocks.focus).toHaveBeenCalledTimes(2);
      expect(mocks.focus).toHaveBeenLastCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({ connectionId: reconnect ? "second" : "first" }),
        }),
      );
    },
  );

  it("does not claim an available runtime from a server snapshot alone", async () => {
    mocks.list.mockResolvedValueOnce(AsyncResult.success(liveList));
    const response = deferred<PreviewAutomationResponse>();
    mocks.respond.mockImplementationOnce(async ({ input }) => response.resolve(input));
    await act(async () => {
      reconcilePreviewServerSessions(threadRef, liveList);
      appAtomRegistry.set(
        requestsAtom,
        AsyncResult.success({
          ...requestEvent,
          request: {
            ...requestEvent.request,
            operation: "status",
            tabId: snapshot.tabId,
            input: {},
          },
        }),
      );
      await response.promise;
    });
    await expect(response.promise).resolves.toMatchObject({
      ok: true,
      result: { available: false, tabId: snapshot.tabId },
    });
  });
});

const runtimeIdentity: PreviewAutomationRuntimeIdentity = {
  schemaVersion: 1,
  runtimeKind: "electron",
  runtimeInstanceId: "synthetic-desktop-runtime",
  appVersion: "1.2.3",
  buildCommit: "a".repeat(40),
};

async function remountWithRuntimeGetter(getter?: () => Promise<PreviewAutomationRuntimeIdentity>) {
  await act(() => renderer?.unmount());
  renderer = null;
  mocks.automationRequests.mockClear();
  Object.assign(window, {
    desktopBridge: getter ? { getPreviewAutomationRuntimeIdentity: getter } : {},
  });
  await act(() => {
    renderer = create(
      <AppAtomRegistryProvider>
        <PreviewAutomationHosts />
      </AppAtomRegistryProvider>,
    );
  });
}

describe("PreviewAutomationHosts runtime identity", () => {
  it("waits for the descriptor before registering it for every environment", async () => {
    const pending = deferred<PreviewAutomationRuntimeIdentity>();
    const getter = vi.fn(() => pending.promise);
    const secondEnvironmentId = EnvironmentId.make("second-environment");
    mocks.environments = [{ environmentId }, { environmentId: secondEnvironmentId }];
    await remountWithRuntimeGetter(getter);
    expect(getter).toHaveBeenCalledOnce();
    expect(mocks.automationRequests).not.toHaveBeenCalled();
    await act(async () => {
      pending.resolve(runtimeIdentity);
      await pending.promise;
    });
    for (const id of [environmentId, secondEnvironmentId]) {
      expect(mocks.automationRequests).toHaveBeenCalledWith({
        environmentId: id,
        input: expect.objectContaining({ environmentId: id, runtimeIdentity }),
      });
    }
  });

  it("registers legacy hosts when the optional getter is absent", async () => {
    await remountWithRuntimeGetter();
    expect(mocks.automationRequests).toHaveBeenCalled();
    for (const [target] of mocks.automationRequests.mock.calls) {
      expect(target.input).not.toHaveProperty("runtimeIdentity");
    }
  });

  it("registers legacy hosts after the descriptor getter rejects", async () => {
    await remountWithRuntimeGetter(() => Promise.reject(new Error("synthetic unavailable IPC")));
    expect(mocks.automationRequests).toHaveBeenCalled();
    for (const [target] of mocks.automationRequests.mock.calls) {
      expect(target.input).not.toHaveProperty("runtimeIdentity");
    }
  });

  it("ignores a descriptor resolved after unmount", async () => {
    const pending = deferred<PreviewAutomationRuntimeIdentity>();
    await remountWithRuntimeGetter(() => pending.promise);
    await act(() => renderer?.unmount());
    renderer = null;
    await act(async () => {
      pending.resolve(runtimeIdentity);
      await pending.promise;
    });
    expect(mocks.automationRequests).not.toHaveBeenCalled();
  });
});
