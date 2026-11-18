import { createMemoryHistory, createRootRoute, createRouter } from "@tanstack/react-router";
import type { DesktopBridge, DesktopUpdateState } from "@t3tools/contracts";
import type { JonesInstallPlanState } from "@t3tools/contracts/jones/installPlan";
import type {
  ThreadMonitorNativeState,
  ThreadMonitorPublisherFrame,
} from "@t3tools/contracts/jones/desktopThreadMonitor";
import { expect, it, vi } from "vite-plus/test";
import { act, render, stubGlobal } from "../../../test/render";
import { AppRoot } from "../../AppRoot";
import type { AppRouter } from "../../router";
import { PanelLayoutControls } from "../../components/chat/PanelLayoutControls";
import { WorkspacePageHeader } from "../../components/WorkspacePageHeader";

vi.mock(import("../../state/environments"), async (original) => ({
  ...(await original()),
  useEnvironments: (() => ({ environments: [] })) as never,
}));

function Controls() {
  return (
    <>
      <WorkspacePageHeader>Other workspace header</WorkspacePageHeader>
      <PanelLayoutControls
        terminalAvailable
        terminalOpen={false}
        terminalShortcutLabel={null}
        threadPanelOpen={false}
        threadPanelPresentation="inline"
        threadPanelShortcutLabel={null}
        rightPanelAvailable
        rightPanelOpen={false}
        rightPanelShortcutLabel={null}
        onToggleTerminal={() => {}}
        onToggleThreadPanel={() => {}}
        onToggleRightPanel={() => {}}
      />
    </>
  );
}

it("starts the real retained controllers once at AppRoot and releases their native subscriptions", async () => {
  const listeners = new Set<string>();
  const stops = new Set<string>();
  const subscribe = (key: string) => {
    expect(listeners.has(key)).toBe(false);
    listeners.add(key);
    return () => {
      listeners.delete(key);
      stops.add(key);
    };
  };
  const native: ThreadMonitorNativeState = {
    supported: true,
    enabled: false,
    visible: false,
    mainVisible: true,
    mainFocused: true,
    expanded: false,
    reason: null,
  };
  const desktop: DesktopUpdateState = {
    enabled: false,
    status: "idle",
    channel: "latest",
    currentVersion: "0.1.0-fixture.1",
    availableVersion: null,
    downloadedVersion: null,
    releaseNotes: [],
    omittedReleaseCount: 0,
    downloadPercent: null,
    checkedAt: null,
    message: null,
    errorContext: null,
    canRetry: false,
    hostArch: "arm64",
    appArch: "arm64",
    runningUnderArm64Translation: false,
  };
  const plan: JonesInstallPlanState = {
    schema: 1,
    current: null,
    last: null,
    laptop: { observedAt: "2026-11-16T00:00:00Z", activationBlocked: false },
    view: { complete: true },
  };
  const installPlan = vi.fn(async () => ({
    state: plan,
    publication: { kind: "withheld", code: "unavailable", message: "Synthetic native proof" },
  }));
  const fleetUpdates = vi.fn(async () => ({ schema: 1, enrollments: [], campaigns: [] }));
  const publish = vi.fn(async (_frame: ThreadMonitorPublisherFrame) => {});
  const setEnabled = vi.fn(async (enabled: boolean) => ({ ...native, enabled }));
  const setTicketProviderReady = vi.fn(async () => {});
  using _bridge = stubGlobal("desktopBridge", {
    getUpdateState: async () => desktop,
    onUpdateState: () => subscribe("install-update"),
    onInstallPlan: () => subscribe("install-plan"),
    onQuitShortcut: () => subscribe("quit"),
    installPlan,
    fleetUpdates,
    threadMonitor: {
      publish,
      getState: async () => native,
      onIntent: () => subscribe("monitor-intent"),
      onState: () => subscribe("monitor-state"),
      invalidateEnvironment: async () => {},
      setEnabled,
    },
    previewCompanion: {
      getState: async () => ({
        config: {
          enabled: false,
          environmentId: null,
          hostId: "fixture",
          label: "Fixture",
          browserOnly: true,
        },
        status: "awaiting_ticket",
        assignments: [],
        connectionGeneration: null,
      }),
      onState: () => subscribe("companion-state"),
      onTicketRequest: () => subscribe("companion-ticket"),
      onNotice: () => subscribe("companion-notice"),
      setTicketProviderReady,
    },
  } as unknown as DesktopBridge);
  const router = createRouter({
    routeTree: createRootRoute({ component: Controls }),
    history: createMemoryHistory({ initialEntries: ["/"] }),
  }) as unknown as AppRouter;
  await router.load();
  await using screen = await render(<AppRoot router={router} />, { router: false });
  await vi.waitFor(() => {
    expect(installPlan).toHaveBeenCalledWith({ type: "read" });
    expect(installPlan).toHaveBeenCalledWith({ type: "publication", acceptOrdinaryRecovery: 1 });
    expect(fleetUpdates).toHaveBeenCalledWith({ action: "read" });
    expect(publish).toHaveBeenCalledWith(expect.objectContaining({ type: "reset" }));
    expect(setTicketProviderReady).toHaveBeenCalledWith(true);
  });
  expect([...listeners]).toEqual(
    expect.arrayContaining([
      "install-update",
      "install-plan",
      "monitor-intent",
      "monitor-state",
      "companion-state",
      "companion-ticket",
      "companion-notice",
    ]),
  );
  expect(screen.container.querySelectorAll("[data-thread-monitor-toggle]")).toHaveLength(1);
  expect(
    screen.container.querySelector("[data-panel-layout-controls] [data-thread-monitor-toggle]"),
  ).not.toBeNull();
  await screen.getByRole("button", { name: "Show thread monitor", exact: true }).click();
  expect(setEnabled).toHaveBeenCalledWith(true);
  await act(() => router.invalidate());
  expect(publish.mock.calls.filter(([frame]) => frame.type === "reset")).toHaveLength(1);
  await screen[Symbol.asyncDispose]();
  expect(listeners.size).toBe(0);
  expect(stops.has("install-plan")).toBe(true);
  expect(stops.has("monitor-intent")).toBe(true);
  expect(stops.has("companion-ticket")).toBe(true);
  expect(setTicketProviderReady).toHaveBeenLastCalledWith(false);
});
