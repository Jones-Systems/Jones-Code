import { RegistryProvider } from "@effect/atom-react";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import type { EnvironmentServerConfig } from "@t3tools/client-runtime/connection";
import {
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  ThreadId,
  type ContextMenuItem,
} from "@t3tools/contracts";
import {
  createMemoryHistory,
  createRootRoute,
  createRouter,
  RouterProvider,
} from "@tanstack/react-router";
import { act, useSyncExternalStore } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import Sidebar from "../../components/Sidebar";
import { SidebarProvider } from "../../components/ui/sidebar";
import { TooltipProvider } from "../../components/ui/tooltip";
import {
  data,
  now,
  placements,
  primary,
} from "../../components/workstreams/nativeWorkstreamActions.fixtures";
import { ensureLocalApi } from "../../localApi";
import type { WorkstreamListView } from "../../state/workstreams";
import { makeThreadFixture } from "../../test-fixtures";
import { WorkstreamActionError } from "./workstreamActionSnapshot";

// Only environment/controller boundaries are synthetic. Sidebar, its rows, the assignment
// ref and dispatch, and WorkstreamNativeSidebar all run their production implementations.
const fixture = vi.hoisted(() => ({
  threads: [] as readonly EnvironmentThreadShell[],
  controller: null as WorkstreamListView | null,
  listeners: new Set<() => void>(),
  mutation: vi.fn(),
  pinAction: vi.fn<() => Promise<never>>(),
}));

vi.mock(import("../../state/entities"), async (original) => ({
  ...(await original()),
  useThreadShells: () => fixture.threads,
  readThreadShell: (ref: { threadId: string }) =>
    fixture.threads.find((thread) => thread.id === ref.threadId) ?? null,
  useProjects: () => [],
  useServerConfigs: () => serverConfigs,
  useAllEnvironmentProjectSnapshotsReady: () => true,
  useDelegatedUnderParent: () => new Set<string>(),
}));
vi.mock(import("../../state/environments"), async (original) => ({
  ...(await original()),
  usePrimaryEnvironmentId: () => EnvironmentId.make("env:a"),
  useConnectedEnvironmentIds: () => [EnvironmentId.make("env:a")],
  useEnvironmentIdentities: () => [],
  useEnvironmentMachines: () => new Map(),
}));
vi.mock(import("../../state/session"), async (original) => ({
  ...(await original()),
  useEnvironmentScope: () => true,
  readEnvironmentScope: () => true,
  ensureEnvironmentScope: async () => ({ status: "granted" as const }),
}));
vi.mock(import("../../state/workstreams"), async (original) => ({
  ...(await original()),
  useWorkstreams: () =>
    useSyncExternalStore(
      (listener) => {
        fixture.listeners.add(listener);
        return () => {
          fixture.listeners.delete(listener);
        };
      },
      () => fixture.controller!,
    ),
}));
vi.mock(import("../../state/use-orchestration-command"), async (original) => ({
  ...(await original()),
  useOrchestrationCommand: () => fixture.mutation,
}));
vi.mock(import("../../hooks/useThreadActions"), async (original) => {
  const actual = await original();
  return {
    ...actual,
    useThreadActions: () => ({
      ...actual.useThreadActions(),
      // A disconnected fixture could refuse before reaching a server mutation.
      // Count attempted pin changes as well as submitted orchestration commands.
      pinThread: fixture.pinAction,
      unpinThread: fixture.pinAction,
      confirmAndUnpinThread: fixture.pinAction,
    }),
  };
});

let root: Root;
let container: HTMLDivElement;
const environmentId = EnvironmentId.make("env:a");
const serverConfigs = new Map<EnvironmentId, EnvironmentServerConfig>([
  [
    environmentId,
    {
      environment: {
        environmentId,
        label: "Synthetic environment",
        platform: { os: "linux", arch: "x64" },
        serverVersion: "test",
        capabilities: { repositoryIdentity: false, threadPinning: true },
      },
      auth: {
        policy: "loopback-browser",
        bootstrapMethods: [],
        sessionMethods: [],
        sessionCookieName: "test-session",
      },
      cwd: "/synthetic",
      keybindingsConfigPath: "/synthetic/keybindings.json",
      keybindings: [],
      issues: [],
      availableEditors: [],
      observability: {
        logsDirectoryPath: "/synthetic/logs",
        localTracingEnabled: false,
        otlpTracesEnabled: false,
        otlpMetricsEnabled: false,
        otlpLogsEnabled: false,
      },
      settings: DEFAULT_SERVER_SETTINGS,
    },
  ],
]);
const assign = vi.fn<NonNullable<WorkstreamListView["assignNativeThread"]>>();
const controlPlacement = {
  ...primary,
  native_thread_id: "control",
  native_reference_id: "control-reference",
  membership_id: "control-membership",
  workstream_id: "beta",
};
const unexpected = async (): Promise<never> => {
  throw new Error("Unexpected controller operation");
};

function publish(controller: WorkstreamListView) {
  fixture.controller = controller;
  for (const listener of fixture.listeners) listener();
}

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.useFakeTimers({ toFake: ["Date"] });
  vi.setSystemTime(now);
  localStorage.clear();
  fixture.mutation.mockReset();
  fixture.mutation.mockImplementation(unexpected);
  fixture.pinAction.mockReset();
  fixture.pinAction.mockImplementation(unexpected);
  fixture.threads = [
    makeThreadFixture({
      environmentId,
      id: ThreadId.make("ordinary"),
      title: "Ordinary assignment target",
      pinnedAt: null,
    }),
    makeThreadFixture({
      environmentId,
      id: ThreadId.make("pinned"),
      title: "Pinned assignment target",
      pinnedAt: "2026-09-29T12:00:00Z",
      pinOrderKey: "a0",
    }),
    makeThreadFixture({
      environmentId,
      id: ThreadId.make("control"),
      title: "Untouched control thread",
      pinnedAt: null,
    }),
  ];
  assign.mockReset();
  assign.mockImplementation(async (thread, destination) => {
    const controller = fixture.controller!;
    publish({
      ...controller,
      placements: {
        ...controller.placements!,
        items: [
          ...controller.placements!.items.filter((item) => item.native_thread_id !== thread.id),
          ...(destination === null
            ? []
            : [
                {
                  ...primary,
                  native_thread_id: thread.id,
                  native_reference_id: `reference-${thread.id}`,
                  membership_id: `membership-${thread.id}`,
                  workstream_id: destination,
                },
              ]),
        ],
      },
    });
  });
  fixture.controller = {
    data,
    placements: { ...placements, items: [controlPlacement] },
    placementInventory: {
      coverage: "complete",
      identities: fixture.threads.map((thread) => ({
        source_instance_id: thread.environmentId,
        native_thread_id: thread.id,
      })),
      json: "[]",
      totalIdentities: 3,
    },
    assignmentAvailability: () => ({ state: "ready", reason: null }),
    error: null,
    loading: false,
    refresh: vi.fn(),
    assignNativeThread: assign,
    submit: unexpected,
    loadDetail: unexpected,
    loadReference: unexpected,
    runBindingOperation: unexpected,
  };
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});

afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  localStorage.clear();
  vi.restoreAllMocks();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

async function renderSidebar() {
  const route = createRootRoute({
    component: () => (
      <RegistryProvider>
        <TooltipProvider>
          <SidebarProvider>
            <Sidebar />
          </SidebarProvider>
        </TooltipProvider>
      </RegistryProvider>
    ),
  });
  const router = createRouter({
    routeTree: route,
    history: createMemoryHistory({ initialEntries: ["/"] }),
  });
  await router.load();
  await act(async () => root.render(<RouterProvider router={router} />));
}

function row(id: string) {
  const result = [...container.querySelectorAll<HTMLElement>("[data-thread-item]")].filter(
    (element) =>
      element.textContent?.includes(fixture.threads.find((thread) => thread.id === id)!.title),
  );
  expect(result, `one rendered production row for ${id}`).toHaveLength(1);
  return result[0]!;
}

function expectGroup(id: string, destination: string | null) {
  expect(row(id).closest("[data-native-workstream]")?.getAttribute("data-native-workstream")).toBe(
    JSON.stringify(destination),
  );
}

async function menu(id: string, selection: string | null = null) {
  let offered: readonly ContextMenuItem[] = [];
  // The shell returns a selected ID from the actual production menu. No ref or
  // assignment callback is injected, including for pinned rows outside the groups.
  const show = vi.spyOn(ensureLocalApi().contextMenu, "show").mockImplementation(async (items) => {
    offered = items;
    const item = items
      .flatMap((item) => [item, ...(item.children ?? [])])
      .find((item) => item.id === selection);
    return item && !item.disabled ? selection : null;
  });
  await act(async () =>
    row(id)
      .querySelector('[role="button"]')!
      .dispatchEvent(new MouseEvent("contextmenu", { bubbles: true, clientX: 20, clientY: 20 })),
  );
  expect(show).toHaveBeenCalledOnce();
  show.mockRestore();
  const items = offered.flatMap((item) => [item, ...(item.children ?? [])]);
  if (selection !== null) {
    const selected = items.find((item) => item.id === selection);
    expect(selected, `offered ${selection}`).toBeDefined();
    expect(selected!.disabled).not.toBe(true);
  }
  return items;
}

describe("production Sidebar Workstream assignment menus", () => {
  it("assigns, moves and unassigns an ordinary row through the production dispatcher", async () => {
    const before = structuredClone(fixture.threads);
    await renderSidebar();
    expectGroup("ordinary", null);
    await menu("ordinary", "assign-workstream:group:alpha");
    expect(assign).toHaveBeenCalledOnce();
    expectGroup("ordinary", "alpha");
    expect(
      (await menu("ordinary")).find((item) => item.id === "assign-workstream:group:alpha")?.label,
    ).toBe("alpha (primary)");
    await menu("ordinary", "assign-workstream:group:beta");
    expectGroup("ordinary", "beta");
    expect(
      (await menu("ordinary")).find((item) => item.id === "assign-workstream:group:beta")?.label,
    ).toBe("beta (primary)");
    await menu("ordinary", "assign-workstream:remove");
    expectGroup("ordinary", null);
    expect(assign.mock.calls.map(([thread, destination]) => [thread.id, destination])).toEqual([
      ["ordinary", "alpha"],
      ["ordinary", "beta"],
      ["ordinary", null],
    ]);
    expectGroup("control", "beta");
    expect(fixture.controller!.placements!.items).toEqual([controlPlacement]);
    expect(fixture.threads).toEqual(before);
    expect(fixture.mutation).not.toHaveBeenCalled();
    expect(fixture.pinAction).not.toHaveBeenCalled();
  });

  it("uses the registered menu for a pinned row and retains its pin while assignment changes", async () => {
    const before = structuredClone(fixture.threads);
    await renderSidebar();
    for (const destination of ["alpha", "beta", null]) {
      await menu(
        "pinned",
        destination === null
          ? "assign-workstream:remove"
          : `assign-workstream:group:${destination}`,
      );
      const offered = await menu("pinned");
      expect(offered.some((item) => item.id === "unpin")).toBe(true);
      expect(
        offered.filter((item) => item.label.endsWith(" (primary)")).map((item) => item.id),
      ).toEqual(destination === null ? [] : [`assign-workstream:group:${destination}`]);
      expect(row("pinned").closest("[data-native-workstream-thread]")).toBeNull();
      expect(
        row("pinned").querySelector('[aria-label="Pinned"], [aria-label="Unpin thread"]'),
      ).not.toBeNull();
      expect(fixture.threads).toEqual(before);
    }
    expect(assign.mock.calls.map(([thread, destination]) => [thread.id, destination])).toEqual([
      ["pinned", "alpha"],
      ["pinned", "beta"],
      ["pinned", null],
    ]);
    expectGroup("control", "beta");
    expect(fixture.controller!.placements!.items).toEqual([controlPlacement]);
    expect(fixture.mutation).not.toHaveBeenCalled();
    expect(fixture.pinAction).not.toHaveBeenCalled();
  });

  it.each(["denied", "unknown"] as const)(
    "preserves the pinned row and control state after a %s controller result",
    async (reason) => {
      const before = structuredClone(fixture.threads);
      assign.mockRejectedValueOnce(new WorkstreamActionError(reason));
      await renderSidebar();
      await menu("pinned", "assign-workstream:group:alpha");
      expect(assign).toHaveBeenCalledOnce();
      expect(container.querySelector('[role="alert"]')?.textContent).toContain(
        reason === "denied" ? "denied" : "effect is unknown",
      );
      expect(
        row("pinned").querySelector('[aria-label="Pinned"], [aria-label="Unpin thread"]'),
      ).not.toBeNull();
      expectGroup("control", "beta");
      expect(fixture.threads).toEqual(before);
      expect(fixture.controller!.placements!.items).toEqual([controlPlacement]);
      expect(fixture.mutation).not.toHaveBeenCalled();
      expect(fixture.pinAction).not.toHaveBeenCalled();
    },
  );
});
