import {
  AuthOrchestrationOperateScope,
  EnvironmentId,
  ThreadId,
  type ContextMenuItem,
} from "@t3tools/contracts";
import { AsyncResult } from "effect/reactivity";
import { beforeEach, describe, expect, it, vi } from "vite-plus/test";

import type { ThreadActionMenuId } from "../components/threadActionMenu.logic";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}

const state = vi.hoisted(() => ({
  granted: new Set<string>(),
  projection: vi.fn(),
  effects: [] as string[],
  completed: deferred<void>(),
  shown: deferred<void>(),
  settled: false,
  settlementRestore: true,
  show: vi.fn<
    (
      items: ReadonlyArray<ContextMenuItem<ThreadActionMenuId>>,
      position: { x: number; y: number },
    ) => Promise<ThreadActionMenuId | null>
  >(),
}));

function recordEffect(action: string) {
  state.effects.push(action);
  state.completed.resolve();
}

vi.mock("../components/CustomSnoozeDialog", () => ({ requestCustomSnooze: vi.fn() }));
vi.mock("react", async (importOriginal) => ({
  ...(await importOriginal<typeof import("react")>()),
  useCallback: (callback: unknown) => callback,
  useMemo: (factory: () => unknown) => factory(),
}));
vi.mock("@tanstack/react-router", () => ({
  useRouter: () => ({ navigate: async () => recordEffect("project-settings") }),
}));
vi.mock("../state/session", () => ({
  readEnvironmentScope: (environmentId: string, scope: string) =>
    scope === AuthOrchestrationOperateScope && state.granted.has(environmentId),
}));
vi.mock("../state/entities", () => ({
  readEnvironmentSupportsAutoSettleOptOut: () => true,
  readEnvironmentSupportsPinning: () => true,
  readEnvironmentSupportsSettlement: () => true,
  readEnvironmentSupportsSnooze: () => true,
  readEnvironmentSupportsTitleRegeneration: () => true,
  readThreadShell: () => ({
    id: "thread",
    environmentId: "secondary",
    projectId: "project",
    title: "Thread",
    branch: "main",
    worktreePath: null,
    runtime: null,
    latestRun: null,
    settledOverride: state.settled ? "settled" : null,
  }),
  useProjects: () => [{ id: "project", environmentId: "secondary" }],
}));
// Jones settlement restore: capability and the server-record restore it runs.
vi.mock("../jones/settlementRestore/settlementRestore", () => ({
  readEnvironmentSupportsSettlementRestore: () => state.settlementRestore,
  restoreCurrentSettlement: async (threadRef: { threadId: string }) => {
    recordEffect(`undo-settle:${threadRef.threadId}`);
    return AsyncResult.success(undefined);
  },
}));
vi.mock("../state/environments", () => ({ usePrimaryEnvironmentId: () => "primary" }));
vi.mock("../state/threads", () => ({
  threadEnvironment: { updateMetadata: "metadata", stopSession: "stop-session" },
}));
vi.mock("../rpc/atomRegistry", () => ({ appAtomRegistry: {} }));
vi.mock("../state/orchestration", () => ({
  orchestrationEnvironment: {
    v2: { threadProjection: () => "thread-projection" },
  },
}));
vi.mock("@t3tools/client-runtime/state/runtime", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@t3tools/client-runtime/state/runtime")>()),
  executeAtomQuery: (...args: unknown[]) => state.projection(...args),
}));
vi.mock("../state/use-atom-command", () => ({
  useAtomCommand: (command: string) => async () => {
    recordEffect(command);
    return AsyncResult.success(undefined);
  },
}));
vi.mock("../localApi", () => ({
  readLocalApi: () => ({
    contextMenu: {
      show: (...args: Parameters<typeof state.show>) => {
        const choice = state.show(...args);
        state.shown.resolve();
        return choice;
      },
      close: () => {},
    },
    dialogs: {
      confirm: async () => {
        recordEffect("confirm");
        return false;
      },
    },
  }),
}));
vi.mock("../logicalProject", () => ({
  deriveLogicalProjectKeyFromSettings: () => "project",
  derivePhysicalProjectKey: () => "project",
  selectProjectGroupingSettings: () => ({}),
}));
vi.mock("../sidebarProjectGrouping", () => ({
  buildPhysicalToLogicalProjectKeyMap: () => new Map(),
}));
vi.mock("../uiStateStore", () => ({
  useUiStateStore: (select: (store: unknown) => unknown) =>
    select({
      markThreadUnread: () => recordEffect("mark-unread"),
    }),
}));
vi.mock("../components/ui/toast", () => ({
  stackedThreadToast: (toast: unknown) => toast,
  toastManager: { add: () => state.completed.resolve() },
}));
vi.mock("../components/Sidebar.snooze", () => ({
  resolveSnoozePresets: () => [
    { id: "hour", label: "In 1 hour", whenLabel: "3 PM", snoozedUntil: "2099-01-01T00:00:00Z" },
  ],
  snoozeWakeDescription: () => "later",
}));
vi.mock("./useCopyToClipboard", () => ({
  useCopyToClipboard: () => ({ copyToClipboard: () => recordEffect("copy") }),
}));
vi.mock("./useHandleNewThread", () => ({
  useNewThreadHandler: () => async () => recordEffect("draft"),
}));
vi.mock("./useSettings", () => ({
  useClientSettings: (select: (settings: unknown) => unknown) =>
    select({
      confirmThreadDelete: true,
      confirmThreadArchive: true,
      timestampFormat: "12-hour",
    }),
}));
vi.mock("./useThreadActions", () => ({
  useThreadActions: () =>
    Object.fromEntries(
      [
        "markThreadUnread",
        "settleThread",
        "unsettleThread",
        "snoozeThread",
        "unsnoozeThread",
        "pinThread",
        "confirmAndUnpinThread",
        "archiveThread",
        "deleteThread",
      ].map((action) => [
        action,
        async () => {
          recordEffect(action === "markThreadUnread" ? "mark-unread" : action);
          return AsyncResult.success(undefined);
        },
      ]),
    ),
}));

import { useThreadActionMenu } from "./useThreadActionMenu";

const target = {
  environmentId: EnvironmentId.make("secondary"),
  threadId: ThreadId.make("thread"),
};
const position = { x: 10, y: 20 };
const createMenu = () =>
  useThreadActionMenu({
    threadRef: target,
    projectCwd: "/project",
    onStartRename: () => recordEffect("rename"),
  });

beforeEach(() => {
  state.settled = false;
  state.settlementRestore = true;
  state.granted = new Set(["primary"]);
  state.effects = [];
  state.completed = deferred<void>();
  state.shown = deferred<void>();
  state.show.mockReset().mockResolvedValue(null);
  state.projection.mockReset().mockResolvedValue(AsyncResult.success({ providerSessions: [] }));
});

describe("thread menu permissions", () => {
  it("opens immediately without waiting for a thread projection", () => {
    const projection = deferred<unknown>();
    state.projection.mockReturnValue(projection.promise);
    try {
      createMenu().openMenu(position);
      expect(state.show).toHaveBeenCalledExactlyOnceWith(expect.any(Array), position);
      expect(state.projection).not.toHaveBeenCalled();
    } finally {
      projection.resolve(AsyncResult.success({ providerSessions: [] }));
    }
  });

  it("keeps Stop reachable without loaded sessions and uses the authoritative stop command", async () => {
    state.granted.add("secondary");
    state.show.mockResolvedValue("stop-thread");
    createMenu().openMenu(position);
    await state.shown.promise;
    expect(
      state.show.mock.calls[0]![0].find((item) => item.id === "stop-thread")?.disabled,
    ).not.toBe(true);
    await state.completed.promise;
    expect(state.effects).toEqual(["stop-session"]);
  });

  it("disables mutations for a denied secondary environment", async () => {
    createMenu().openMenu(position);
    await state.shown.promise;
    const items = state.show.mock.calls[0]![0];
    expect(items.find((item) => item.id === "rename")?.disabled).toBe(true);
    expect(items.find((item) => item.id === "delete")?.disabled).toBe(true);
    expect(items.find((item) => item.id === "copy")?.disabled).not.toBe(true);
  });

  it("allows the target grant even when the primary environment is denied", async () => {
    state.granted = new Set(["secondary"]);
    createMenu().openMenu(position);
    await state.shown.promise;
    expect(state.show.mock.calls[0]![0].find((item) => item.id === "rename")?.disabled).not.toBe(
      true,
    );
  });

  it("refreshes availability when a retained menu opener gains permission", async () => {
    const menu = createMenu();
    menu.openMenu(position);
    await state.shown.promise;
    expect(state.show.mock.calls[0]![0].find((item) => item.id === "rename")?.disabled).toBe(true);
    state.granted.add("secondary");
    state.shown = deferred<void>();
    menu.openMenu(position);
    await state.shown.promise;
    expect(state.show.mock.calls[1]![0].find((item) => item.id === "rename")?.disabled).not.toBe(
      true,
    );
  });

  it.each([
    "rename",
    "regenerate-title",
    "delete",
    "pin",
    "settle",
    "archive",
    "stop-thread",
  ] as const)("%s rechecks after the native menu closes", async (action) => {
    state.granted.add("secondary");
    const choice = deferred<ThreadActionMenuId | null>();
    state.show.mockReturnValue(choice.promise);
    createMenu().openMenu(position);
    await state.shown.promise;
    state.granted.delete("secondary");
    choice.resolve(action);
    await state.completed.promise;
    expect(state.effects).toEqual([]);
  });

  it.each([
    ["new-thread-on-branch", "draft"],
    ["copy-thread-id", "copy"],
    ["mark-unread", "mark-unread"],
    ["project-settings", "project-settings"],
  ] as const)("keeps %s available without task permission", async (action, effect) => {
    state.show.mockResolvedValue(action);
    createMenu().openMenu(position);
    await state.completed.promise;
    expect(state.effects).toEqual([effect]);
  });
});

describe("Jones settlement restore menu item", () => {
  const ids = () => state.show.mock.calls[0]![0].map((item) => item.id as string);

  it("offers Undo settle beside Un-settle and restores from the server's record", async () => {
    state.granted.add("secondary");
    state.settled = true;
    state.show.mockResolvedValue("jones:undo-settle" as ThreadActionMenuId);
    createMenu().openMenu(position);
    await state.completed.promise;
    expect(ids().slice(ids().indexOf("unsettle"), ids().indexOf("unsettle") + 2)).toEqual([
      "unsettle",
      "jones:undo-settle",
    ]);
    expect(state.effects).toEqual(["undo-settle:thread"]);
  });

  it("is absent for active threads and servers without native restore", async () => {
    createMenu().openMenu(position);
    await state.shown.promise;
    state.settled = true;
    state.settlementRestore = false;
    state.shown = deferred<void>();
    createMenu().openMenu(position);
    await state.shown.promise;
    for (const call of state.show.mock.calls)
      expect(call[0].some((item) => (item.id as string) === "jones:undo-settle")).toBe(false);
  });
});
