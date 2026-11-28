import "~/index.css";
import { useAtomValue } from "@effect/atom-react";
import { createMemoryHistory } from "@tanstack/react-router";
import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import { personalThreadKey } from "@t3tools/client-runtime/jones/workstreams/personal";
import type { PersonalWorkstreamsState } from "@t3tools/client-runtime/jones/workstreams/personal";
import { ProjectId, ThreadId } from "@t3tools/contracts";
import { DEFAULT_CLIENT_SETTINGS } from "@t3tools/contracts/settings";
import type { Context } from "effect";
import * as DateTime from "effect/DateTime";
import { Atom } from "effect/reactivity";
import { useRef } from "react";
import { expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";
import { AppRoot } from "~/AppRoot";
import {
  threadContextDropTargetProps,
  useThreadContextDrop,
} from "~/components/chat/threadContextDrag";
import { toastManager } from "~/components/ui/toast";
import { __setClientSettingsForTests } from "~/hooks/useSettings";
import { useThreadTitleDrag } from "~/jones/threadTitleDrag/threadTitleDrag";
import { getRouter } from "~/router";
import { appAtomRegistry } from "~/rpc/atomRegistry";
import { readEnvironmentSupportsPinning } from "~/state/entities";
import { usePersonalWorkstreams } from "~/state/workstreamsPersonal";
import { useUiStateStore } from "~/uiStateStore";
import {
  dispatchedCommands,
  ENVIRONMENT_ID,
  installFakeEnvironment,
  refreshFakeShell,
  threadWithHistory,
  type FakeDriver,
  type FakeThread,
} from "../../../test/fakeEnvironment";
import { act, render } from "../../../test/render";
import { personalGroupCollapsedKey } from "./personalSidebarOrder";
import { personalThreadExpandedKey, personalThreadLimitKey } from "./personalThreadWindow";

vi.mock("~/components/ChatView", () => ({ default: () => null }));
vi.mock(import("~/state/workstreamsPersonal"), async (original) => ({
  ...(await original()),
  usePersonalWorkstreams: vi.fn(),
}));
vi.mock("../../../../../packages/client-runtime/src/connection/driver.ts", async (original) => {
  const [actual, { fakeDriver }, Layer] = await Promise.all([
    original<{ readonly ConnectionDriver: Context.Key<unknown, FakeDriver> }>(),
    import("../../../test/fakeEnvironment"),
    import("effect/Layer"),
  ]);
  return { ...actual, layer: Layer.succeed(actual.ConnectionDriver, fakeDriver) };
});
vi.mock(import("~/rpc/requestLatencyState"), async (original) => ({
  ...(await original()),
  trackRpcRequestSent: () => {},
}));
const placements = vi.hoisted((): Array<Promise<boolean>> => []);
vi.mock(import("./personalThreadPlacement"), async (original) => {
  const actual = await original();
  return {
    ...actual,
    placePersonalThread: (input: Parameters<typeof actual.placePersonalThread>[0]) => {
      const result = actual.placePersonalThread(input);
      placements.push(result);
      return result;
    },
  };
});

const at = "2026-03-17T19:12:28.000Z";
const woke = { lastSnoozeWakeAt: DateTime.makeUnsafe("2026-03-18T00:00:00Z") };
const individuallySnoozed = { snoozedUntil: DateTime.makeUnsafe("2098-01-01T00:00:00Z") };
const outsideProject = ProjectId.make("outside-project");
const makeThread = (id: string, fields: Partial<FakeThread> = {}): FakeThread => ({
  ...threadWithHistory(id, 0),
  ...fields,
});

function lane(id: string | null) {
  return [...document.querySelectorAll<HTMLElement>("[data-native-workstream]")].find(
    (section) => section.dataset.nativeWorkstream === JSON.stringify(id),
  );
}

function rows(id: string | null) {
  return [...(lane(id)?.querySelectorAll<HTMLElement>("[data-personal-thread]") ?? [])].map(
    (row) => JSON.parse(row.dataset.personalThread!)[1] as string,
  );
}

function row(id: string, pinned = false) {
  const attribute = pinned ? "threadItem" : "personalThread";
  const selector = pinned ? "[data-thread-item]" : "[data-personal-thread]";
  const key = pinned
    ? scopedThreadKey(scopeThreadRef(ENVIRONMENT_ID, ThreadId.make(id)))
    : JSON.stringify([ENVIRONMENT_ID, id]);
  const element = [...document.querySelectorAll<HTMLElement>(selector)].find(
    (entry) => entry.dataset[attribute] === key,
  );
  if (!element) throw new Error(`Missing row ${id}`);
  return element;
}

function press(
  target: EventTarget,
  type: string,
  element: Element,
  fraction: number,
  buttons = type === "pointerup" ? 0 : 1,
) {
  const bounds = element.getBoundingClientRect();
  const event = new PointerEvent(type, {
    bubbles: true,
    cancelable: true,
    pointerId: 1,
    pointerType: "mouse",
    isPrimary: true,
    button: 0,
    buttons,
    clientX: bounds.left + bounds.width / 2,
    clientY: bounds.top + bounds.height * fraction,
  });
  target.dispatchEvent(event);
  return event;
}

const insertion = () => document.querySelector("[data-personal-thread-insertion]");
const positionNotice = () => page.getByText("Thread can't be placed here", { exact: true });

async function beginDrag(from: HTMLElement) {
  const source = from.querySelector<HTMLElement>("[data-sidebar-row]") ?? from;
  const fromShelf = from.dataset.threadItem !== undefined;
  if (fromShelf) {
    await vi.waitFor(() => expect(readEnvironmentSupportsPinning(ENVIRONMENT_ID)).toBe(true));
  }
  await act(async () => {
    // A row that just moved during fixture hydration can temporarily suppress the press.
    await vi.waitFor(() =>
      expect(press(from, "pointerdown", source, 0.5).defaultPrevented).toBe(false),
    );
    if (fromShelf) press(document, "pointermove", source, 0.7);
  });
}

const shelfBoundaries = ["pointerleave", "outside viewport", "lostpointercapture"] as const;

function leaveShelfDrag(boundary: (typeof shelfBoundaries)[number], source: HTMLElement) {
  return act(() => {
    if (boundary === "lostpointercapture") {
      press(source, boundary, source, 0.5);
      return;
    }
    document.documentElement.dispatchEvent(
      new PointerEvent(boundary === "pointerleave" ? "pointerleave" : "pointermove", {
        bubbles: boundary !== "pointerleave",
        cancelable: true,
        pointerId: 1,
        pointerType: "mouse",
        isPrimary: true,
        buttons: 1,
        clientX: -1,
        clientY: source.getBoundingClientRect().top,
      }),
    );
  });
}

async function previewDropAt(from: HTMLElement, neighbor: HTMLElement, after = false) {
  const fraction = after ? 0.8 : 0.2;
  await beginDrag(from);
  await act(() => {
    press(document, "pointermove", neighbor, fraction);
  });
  const gap = insertion();
  if (gap) expect(after ? gap.previousElementSibling : gap.nextElementSibling).toBe(neighbor);
  return gap;
}

async function dropAt(from: HTMLElement, neighbor: HTMLElement, after = false) {
  const gap = await previewDropAt(from, neighbor, after);
  await act(() => press(document, "pointerup", gap ?? neighbor, gap ? 0.5 : after ? 0.8 : 0.2));
  return gap !== null;
}

async function selectProject(name: "render" | "All projects") {
  await page.getByLabelText(/^Filter threads by project/).click();
  const option = page.getByRole("option", { name, exact: true });
  await option.click();
  await expect.element(option).not.toBeInTheDocument();
}

const activeWrites = () =>
  dispatchedCommands().filter((command) => command.type === "thread.active.reorder");
const unpins = () => dispatchedCommands().filter((command) => command.type === "thread.unpin");
const pinWrites = () =>
  dispatchedCommands().filter((command) => command.type === "thread.pin.reorder");

async function withSidebar(
  {
    threads,
    assignments,
    folders = false,
    snoozed = false,
    workstreamsReady = true,
  }: {
    threads: FakeThread[];
    assignments: Readonly<Record<string, string>>;
    folders?: boolean;
    snoozed?: boolean;
    workstreamsReady?: boolean;
  },
  inspect: (fixture: {
    assign: ReturnType<typeof vi.fn>;
    finishDrop: () => Promise<boolean>;
    replaySavedOrder: () => Promise<void>;
    wakeAlpha: () => Promise<void>;
    wakeThread: (id: string) => Promise<void>;
    enableWorkstreams: () => Promise<void>;
    remount: () => Promise<void>;
  }) => Promise<void>,
) {
  const windowPreferences = [
    personalGroupCollapsedKey("alpha"),
    personalThreadExpandedKey("alpha"),
    personalThreadLimitKey("alpha"),
  ].map((key) => [key, localStorage.getItem(key)] as const);
  for (const [key] of windowPreferences) localStorage.removeItem(key);
  using _restoreWindow = {
    [Symbol.dispose]() {
      for (const [key, value] of windowPreferences) {
        if (value === null) localStorage.removeItem(key);
        else localStorage.setItem(key, value);
      }
    },
  };
  const state: PersonalWorkstreamsState = {
    workstreams: ["Alpha", "Beta"].map((name, index) => ({
      id: name.toLowerCase(),
      name,
      lifecycle: "active",
      color: null,
      orderKey: index === 0 ? "m" : "t",
      createdAt: at,
      updatedAt: at,
      ...(snoozed && index === 0
        ? { snoozedAt: at, snoozedUntil: "2099-01-01T00:00:00.000Z" }
        : {}),
    })),
    environments: [
      {
        environmentId: ENVIRONMENT_ID,
        status: workstreamsReady ? "ready" : "update_required",
        confirmed: true,
        workstreamCount: 2,
      },
    ],
    assignmentsByThreadKey: new Map(
      threads.flatMap(({ id }) => {
        const workstreamId = assignments[id];
        return workstreamId === undefined
          ? []
          : [
              [
                personalThreadKey({ environmentId: ENVIRONMENT_ID, id }),
                { threadId: id, workstreamId, revision: 1, updatedAt: at },
              ] as const,
            ];
      }),
    ),
    pendingThreadKeys: new Set(),
    canCreate: false,
  };
  let stateAtom = Atom.make(state);
  const assign = vi.fn<ReturnType<typeof usePersonalWorkstreams>["controller"]["assign"]>(
    async (thread, workstreamId) => {
      const current = appAtomRegistry.get(stateAtom);
      const next = new Map(current.assignmentsByThreadKey);
      next.set(personalThreadKey(thread), {
        threadId: thread.id,
        workstreamId,
        revision: 2,
        updatedAt: at,
      });
      appAtomRegistry.set(stateAtom, { ...current, assignmentsByThreadKey: next });
      return { ok: true, value: undefined };
    },
  );
  const unexpectedMutation = async () => {
    throw new Error("Unexpected Workstream definition write");
  };
  const controller: ReturnType<typeof usePersonalWorkstreams>["controller"] = {
    get state() {
      return stateAtom;
    },
    canAssign: () => true,
    assign,
    create: unexpectedMutation,
    rename: unexpectedMutation,
    setColor: unexpectedMutation,
    setLifecycle: unexpectedMutation,
    reorder: unexpectedMutation,
    remove: unexpectedMutation,
    createAndAssign: unexpectedMutation,
    setEnvironments: vi.fn(),
    loadCache: vi.fn(),
    dispose: vi.fn(),
  };
  vi.mocked(usePersonalWorkstreams).mockImplementation(function usePlacementWorkstreams() {
    return { controller, state: useAtomValue(stateAtom) };
  });
  using _environment = installFakeEnvironment(threads, {
    activeThreadReorder: true,
    pinnedThreadReorder: true,
    extraProjects: [{ id: outsideProject, title: "outside" }],
  });
  placements.length = 0;
  const finishDrop = async () => {
    await vi.waitFor(() => expect(placements).toHaveLength(1));
    return placements.shift()!;
  };
  __setClientSettingsForTests({
    ...DEFAULT_CLIENT_SETTINGS,
    boringSidebarEnabled: folders,
    sidebarWorkingShelfEnabled: false,
    onboardingCompletedAt: at,
  });
  useUiStateStore.getState().setSidebarProjectScopeKeys([]);
  await page.viewport(1400, 1000);
  const mount = async () => {
    const router = getRouter(
      createMemoryHistory({ initialEntries: [`/${ENVIRONMENT_ID}/${threads[0]!.id}`] }),
    );
    await router.load();
    const screen = await render(<AppRoot router={router} />, { router: false });
    await refreshFakeShell();
    await expect.element(page.getByLabelText(/^Filter threads by project/)).toBeVisible();
    await refreshFakeShell();
    return screen;
  };
  let screen = await mount();
  try {
    await inspect({
      assign,
      finishDrop,
      replaySavedOrder: async () => {
        const keys = new Map(activeWrites().map(({ threadId, orderKey }) => [threadId, orderKey]));
        const unpinned = new Set(unpins().map(({ threadId }) => threadId));
        for (const [index, thread] of threads.entries()) {
          threads[index] = {
            ...thread,
            ...(keys.has(thread.id) ? { activeOrderKey: keys.get(thread.id)! } : {}),
            ...(unpinned.has(thread.id) ? { pinnedAt: null } : {}),
          };
        }
        await refreshFakeShell();
      },
      wakeAlpha: () =>
        act(() => {
          const current = appAtomRegistry.get(stateAtom);
          appAtomRegistry.set(stateAtom, {
            ...current,
            workstreams: current.workstreams.map((workstream) =>
              workstream.id === "alpha" ? { ...workstream, snoozedUntil: null } : workstream,
            ),
          });
        }),
      wakeThread: async (id) => {
        const index = threads.findIndex((thread) => thread.id === id);
        threads[index] = { ...threads[index]!, snoozedUntil: null, ...woke };
        await refreshFakeShell();
      },
      enableWorkstreams: () =>
        act(() => {
          const current = appAtomRegistry.get(stateAtom);
          appAtomRegistry.set(stateAtom, {
            ...current,
            environments: current.environments.map((environment) => ({
              ...environment,
              status: "ready" as const,
            })),
          });
        }),
      remount: async () => {
        const saved = structuredClone(appAtomRegistry.get(stateAtom));
        await screen[Symbol.asyncDispose]();
        stateAtom = Atom.make(saved);
        screen = await mount();
      },
    });
  } finally {
    toastManager.close();
    await screen[Symbol.asyncDispose]();
    vi.mocked(usePersonalWorkstreams).mockReset();
    useUiStateStore.getState().setSidebarProjectScopeKeys([]);
  }
}

it("reorders a woken snoozed-group member in Unassigned and restores its saved membership on wake", async () => {
  await withSidebar(
    {
      threads: [
        makeThread("a1", { activeOrderKey: "m" }),
        makeThread("b1", { activeOrderKey: "z" }),
        makeThread("w1", { ...woke, activeOrderKey: "t" }),
        makeThread("w2", { ...woke, activeOrderKey: "x" }),
      ],
      assignments: { b1: "beta", w1: "alpha", w2: "alpha" },
      snoozed: true,
    },
    async ({ assign, finishDrop, replaySavedOrder, wakeAlpha }) => {
      await vi.waitFor(() => expect(rows(null)).toEqual(["a1", "w1", "w2"]));
      expect(lane("alpha")).toBeUndefined();
      await dropAt(row("w1"), row("a1"));
      expect(await finishDrop()).toBe(true);
      await replaySavedOrder();
      await vi.waitFor(() => expect(rows(null)).toEqual(["w1", "a1", "w2"]));

      await dropAt(row("w2"), row("b1"));
      expect(await finishDrop()).toBe(true);
      await replaySavedOrder();
      await vi.waitFor(() => expect(rows("beta")).toEqual(["w2", "b1"]));
      await wakeAlpha();
      await vi.waitFor(() => expect(lane("alpha")).toBeDefined());
      expect({
        assignments: assign.mock.calls.map(([thread, id]) => [thread.id, id]),
        alpha: rows("alpha"),
        unassigned: rows(null),
      }).toEqual({ assignments: [["w2", "beta"]], alpha: ["w1"], unassigned: ["a1"] });
    },
  );
});

it("keeps the project-filtered Alpha members in saved order when an unrelated drop would invert them", async () => {
  await withSidebar(
    {
      threads: [
        makeThread("other"),
        makeThread("sleep-anchor", { lastSnoozeWakeAt: DateTime.makeUnsafe("2026-03-18T00:00Z") }),
        makeThread("sleep-tail", { projectId: outsideProject }),
        makeThread("a4", { activeOrderKey: "t" }),
      ],
      assignments: { "sleep-anchor": "alpha", "sleep-tail": "alpha" },
      snoozed: true,
      folders: true,
    },
    async ({ assign, finishDrop, replaySavedOrder, wakeAlpha, remount }) => {
      await selectProject("render");
      await vi.waitFor(() => expect(rows(null)).toEqual(["other", "sleep-anchor", "a4"]));
      expect(lane("alpha")).toBeUndefined();
      expect(await dropAt(row("a4"), row("other"))).toBe(true);
      const saved = await finishDrop();
      const writes = activeWrites().map(({ threadId, orderKey }) => [threadId, orderKey]);
      const notices = positionNotice().elements().length;
      await replaySavedOrder();
      await wakeAlpha();
      await selectProject("All projects");
      await vi.waitFor(() => expect(rows("alpha")).toHaveLength(2));
      const woken = rows("alpha");
      await remount();
      await vi.waitFor(() => expect(rows("alpha")).toHaveLength(2));
      expect({ saved, writes, notices, woken, remounted: rows("alpha") }).toEqual({
        saved: false,
        writes: [],
        notices: 1,
        woken: ["sleep-anchor", "sleep-tail"],
        remounted: ["sleep-anchor", "sleep-tail"],
      });
      expect(assign).not.toHaveBeenCalled();
      expect(unpins()).toEqual([]);
    },
  );
});

it.each([
  "same project",
  "pin shelf",
  "outside project",
  "harmless rekey",
  "settled sibling",
  "different saved lane",
])("preserves saved order through individual and Workstream wake (%s)", async (scenario) => {
  const pinned = scenario === "pin shelf";
  const filtered = scenario === "outside project";
  const harmless = scenario === "harmless rekey";
  const settled = scenario === "settled sibling";
  const otherLane = scenario === "different saved lane";
  const tailId = harmless ? "early-tail" : "sleep-tail";
  const blocks = !harmless && !settled && !otherLane;
  const savedAlpha =
    settled || otherLane
      ? ["sleep-anchor"]
      : harmless
        ? [tailId, "sleep-anchor"]
        : ["sleep-anchor", tailId];
  await withSidebar(
    {
      threads: [
        makeThread("other"),
        makeThread("sleep-anchor", woke),
        makeThread(tailId, {
          ...individuallySnoozed,
          ...(filtered ? { projectId: outsideProject } : {}),
          ...(settled ? { settled: true } : {}),
        }),
        makeThread("a4", {
          activeOrderKey: "t",
          ...(pinned ? { pinnedAt: DateTime.makeUnsafe(at) } : {}),
        }),
      ],
      assignments: { "sleep-anchor": "alpha", [tailId]: otherLane ? "beta" : "alpha" },
      snoozed: true,
      folders: true,
    },
    async ({ assign, finishDrop, replaySavedOrder, wakeAlpha, wakeThread, remount }) => {
      if (filtered) await selectProject("render");
      await vi.waitFor(() =>
        expect(rows(null)).toEqual(
          pinned ? ["other", "sleep-anchor"] : ["other", "sleep-anchor", "a4"],
        ),
      );
      expect(lane("alpha")).toBeUndefined();
      expect(await dropAt(row("a4", pinned), row("other"))).toBe(true);
      const saved = await finishDrop();
      const writes = activeWrites().map(({ threadId, orderKey }) => [threadId, orderKey]);
      const notices = positionNotice().elements().length;
      await replaySavedOrder();
      await wakeThread(tailId);
      await wakeAlpha();
      if (filtered) await selectProject("All projects");
      await vi.waitFor(() => expect(rows("alpha")).toHaveLength(savedAlpha.length));
      const woken = rows("alpha");
      await remount();
      await vi.waitFor(() => expect(rows("alpha")).toHaveLength(savedAlpha.length));
      expect({
        saved,
        assignments: assign.mock.calls,
        unpinned: unpins().map(({ threadId }) => threadId),
        writes,
        notices,
        woken,
        remounted: rows("alpha"),
      }).toEqual({
        saved: !blocks,
        assignments: [],
        unpinned: [],
        writes: blocks
          ? []
          : [
              ["sleep-anchor", "n"],
              ["other", "h"],
              ["a4", "e"],
            ],
        notices: blocks ? 1 : 0,
        woken: savedAlpha,
        remounted: savedAlpha,
      });
      if (otherLane) expect(rows("beta")).toEqual([tailId]);
    },
  );
});

it("rejects a saved-lane inversion from the second stationary write before the first write", async () => {
  await withSidebar(
    {
      threads: [
        makeThread("a1", woke),
        makeThread("a2", individuallySnoozed),
        makeThread("a3", woke),
        makeThread("a4", { activeOrderKey: "t" }),
      ],
      assignments: { a1: "alpha", a2: "alpha", a3: "alpha" },
      snoozed: true,
    },
    async ({ assign, finishDrop }) => {
      await vi.waitFor(() => expect(rows(null)).toEqual(["a1", "a3", "a4"]));
      expect(await dropAt(row("a4"), row("a1"))).toBe(true);
      expect({
        saved: await finishDrop(),
        writes: activeWrites().map(({ threadId, orderKey }) => [threadId, orderKey]),
        notices: positionNotice().elements().length,
      }).toEqual({ saved: false, writes: [], notices: 1 });
      expect(assign).not.toHaveBeenCalled();
      expect(unpins()).toEqual([]);
    },
  );
});

it.each([
  { folders: false, fixedKey: null, tailKey: null },
  { folders: true, fixedKey: "d", tailKey: "p" },
])(
  "keeps an out-of-project sibling fixed with folders=$folders",
  async ({ folders, fixedKey, tailKey }) => {
    await withSidebar(
      {
        threads: [
          makeThread("a1"),
          makeThread("a2", { projectId: outsideProject, activeOrderKey: fixedKey }),
          makeThread("a3", { activeOrderKey: tailKey }),
          makeThread("a4", { activeOrderKey: "t" }),
        ],
        assignments: { a1: "alpha", a2: "alpha", a3: "alpha", a4: "alpha" },
        folders,
      },
      async ({ assign, finishDrop, replaySavedOrder, remount }) => {
        await vi.waitFor(() => expect(rows("alpha")).toEqual(["a1", "a2", "a3", "a4"]));
        await selectProject("render");
        await vi.waitFor(() => expect(rows("alpha")).toEqual(["a1", "a3", "a4"]));
        const gap = await previewDropAt(row("a4"), row("a1"));
        expect(gap).not.toBeNull();
        expect(positionNotice().elements()).toHaveLength(0);
        await act(() => press(document, "pointerup", gap!, 0.5));
        const saved = await finishDrop();
        expect(positionNotice().elements()).toHaveLength(fixedKey === null ? 1 : 0);
        expect(assign).not.toHaveBeenCalled();
        expect(unpins()).toEqual([]);
        const written = activeWrites().map(({ threadId }) => threadId);
        await replaySavedOrder();
        await selectProject("All projects");
        await vi.waitFor(() => expect(rows("alpha")).toHaveLength(4));
        const clearFilter = rows("alpha");
        const expected = fixedKey === null ? ["a1", "a2", "a3", "a4"] : ["a4", "a1", "a2", "a3"];
        await remount();
        await vi.waitFor(() => expect(rows("alpha")).toHaveLength(4));
        expect({ saved, written, clearFilter, remounted: rows("alpha") }).toEqual({
          saved: fixedKey !== null,
          written: fixedKey === null ? [] : ["a1", "a4"],
          clearFilter: expected,
          remounted: expected,
        });
      },
    );
  },
);

it.each([null, "t"])(
  "keeps a fixed project boundary when dropping a pin with retained key %s",
  async (key) => {
    await withSidebar(
      {
        threads: [
          makeThread("a1"),
          makeThread("a2", { pinnedAt: DateTime.makeUnsafe(at), activeOrderKey: key }),
          makeThread("a3", { projectId: outsideProject }),
        ],
        assignments: { a1: "alpha", a3: "alpha" },
      },
      async ({ assign, finishDrop, replaySavedOrder, remount }) => {
        await vi.waitFor(() => expect(rows("alpha")).toEqual(["a1", "a3"]));
        await selectProject("render");
        await vi.waitFor(() => expect(rows("alpha")).toEqual(["a1"]));
        const gap = await previewDropAt(row("a2", true), row("a1"), true);
        expect(gap).not.toBeNull();
        expect(positionNotice().elements()).toHaveLength(0);
        await act(() => press(document, "pointerup", gap!, 0.5));
        const saved = await finishDrop();
        expect(positionNotice().elements()).toHaveLength(key === null ? 0 : 1);
        await replaySavedOrder();
        await selectProject("All projects");
        await remount();
        await vi.waitFor(() => expect(rows("alpha")).toContain("a3"));
        expect({
          saved,
          assignments: assign.mock.calls.map(([thread, id]) => [thread.id, id]),
          unpinned: unpins().map(({ threadId }) => threadId),
          orderWrites: activeWrites().map(({ threadId }) => threadId),
          reloaded: rows("alpha"),
        }).toEqual(
          key === null
            ? {
                saved: true,
                assignments: [["a2", "alpha"]],
                unpinned: ["a2"],
                orderWrites: [],
                reloaded: ["a1", "a2", "a3"],
              }
            : {
                saved: false,
                assignments: [],
                unpinned: [],
                orderWrites: [],
                reloaded: ["a1", "a3"],
              },
        );
      },
    );
  },
);

it.each(shelfBoundaries)(
  "cancels a pin-shelf drag after %s and keeps a fresh in-window release possible",
  async (boundary) => {
    await withSidebar(
      {
        threads: [
          makeThread("a1"),
          makeThread("a2", { pinnedAt: DateTime.makeUnsafe(at) }),
          makeThread("a3"),
        ],
        assignments: { a1: "alpha", a3: "alpha" },
      },
      async ({ assign, finishDrop, replaySavedOrder }) => {
        await vi.waitFor(() => expect(rows("alpha")).toEqual(["a1", "a3"]));
        const pin = row("a2", true);
        const anchor = row("a1");
        expect(await previewDropAt(pin, anchor, true)).not.toBeNull();
        await leaveShelfDrag(boundary, pin);
        const gapCleared = insertion() === null;
        await act(() => {
          press(document, "pointermove", anchor, 0.8, 0);
          press(document, "pointerup", anchor, 0.8);
        });
        if (placements.length > 0) await finishDrop();
        expect({
          gapCleared,
          assignments: assign.mock.calls.length,
          unpins: unpins().length,
          writes: activeWrites().length,
          notices: positionNotice().elements().length,
        }).toEqual({ gapCleared: true, assignments: 0, unpins: 0, writes: 0, notices: 0 });

        const gap = await previewDropAt(pin, anchor, true);
        expect(gap).not.toBeNull();
        const release = boundary === "outside viewport" ? "pointerup" : "pointermove";
        await act(() => {
          press(document, release, gap!, 0.5, 0);
          if (release === "pointermove") press(document, "pointerup", anchor, 0.8);
        });
        expect(await finishDrop()).toBe(true);
        expect(assign).toHaveBeenCalledOnce();
        expect(unpins()).toHaveLength(1);
        expect(activeWrites()).toEqual([]);
        expect(insertion()).toBeNull();
        await replaySavedOrder();
        await vi.waitFor(() => expect(rows("alpha")).toEqual(["a1", "a2", "a3"]));
      },
    );
  },
);

it.each([...shelfBoundaries, "pointerup"] as const)(
  "does not let a late Workstream bridge claim a shelf press (%s)",
  async (release) => {
    await withSidebar(
      {
        threads: [
          makeThread("a1"),
          makeThread("a2", { pinnedAt: DateTime.makeUnsafe(at) }),
          makeThread("a3"),
          makeThread("a0", { pinnedAt: DateTime.makeUnsafe(at) }),
        ],
        assignments: { a1: "alpha", a3: "alpha" },
        workstreamsReady: false,
      },
      async ({ assign, enableWorkstreams, finishDrop }) => {
        expect(lane("alpha")).toBeUndefined();
        const pin = row("a2", true);
        await beginDrag(pin);
        if (release !== "pointerup") await leaveShelfDrag(release, pin);
        await enableWorkstreams();
        await vi.waitFor(() => expect(rows("alpha")).toEqual(["a1", "a3"]));
        expect(row("a2", true)).toBe(pin);
        const anchor = row("a1");
        if (release === "pointerup") await act(() => press(document, "pointermove", anchor, 0.8));
        const gap = insertion();
        await act(() => {
          if (release === "pointerup") press(document, "pointerup", anchor, 0.8);
          else {
            press(document, "pointermove", anchor, 0.8, 0);
            press(document, "pointerup", anchor, 0.8);
          }
        });
        const submitted = placements.length;
        if (submitted > 0) await finishDrop();
        expect({
          showedGap: gap !== null,
          gapCleared: insertion() === null,
          submitted,
          assignments: assign.mock.calls.length,
          unpins: unpins().length,
          activeWrites: activeWrites().length,
          pinWrites: pinWrites().length,
          notices: positionNotice().elements().length,
        }).toEqual({
          showedGap: false,
          gapCleared: true,
          submitted: 0,
          assignments: 0,
          unpins: 0,
          activeWrites: 0,
          pinWrites: 0,
          notices: 0,
        });
      },
    );
  },
);

function ComposerDropTarget({ onDrop }: { onDrop: Parameters<typeof useThreadContextDrop>[3] }) {
  const target = useRef<HTMLDivElement>(null);
  useThreadContextDrop(target, ENVIRONMENT_ID, "fixture", onDrop);
  return (
    <div
      {...threadContextDropTargetProps()}
      ref={target}
      style={{ position: "fixed", left: 600, top: 100, width: 200, height: 40 }}
    >
      Composer
    </div>
  );
}

it("preserves composer drops and local pin sorting while Workstreams is unavailable", async () => {
  await withSidebar(
    {
      threads: [
        makeThread("p1", { pinnedAt: DateTime.makeUnsafe(at) }),
        makeThread("p2", { pinnedAt: DateTime.makeUnsafe(at) }),
      ],
      assignments: {},
      workstreamsReady: false,
    },
    async ({ assign }) => {
      const mention = vi.fn();
      await using screen = await render(<ComposerDropTarget onDrop={mention} />, { router: false });
      const target = screen.container.querySelector("[data-thread-context-drop]")!;
      const source = row("p2", true);
      await dropAt(source, target as HTMLElement);
      expect(mention).toHaveBeenCalledExactlyOnceWith(
        [scopeThreadRef(ENVIRONMENT_ID, ThreadId.make("p2"))],
        expect.any(Function),
      );
      expect(pinWrites()).toEqual([]);
      expect(lane(null)).toBeUndefined();

      await dropAt(source, row("p1", true));
      await vi.waitFor(() => expect(pinWrites().map(({ threadId }) => threadId)).toContain("p2"));
      expect(placements).toEqual([]);
      expect(assign).not.toHaveBeenCalled();
      expect(unpins()).toEqual([]);
      expect(activeWrites()).toEqual([]);
    },
  );
});

function PinnedChatTitle() {
  const onPointerDown = useThreadTitleDrag(
    scopeThreadRef(ENVIRONMENT_ID, ThreadId.make("a2")),
    "Pinned chat",
  );
  return (
    <button
      type="button"
      onPointerDown={onPointerDown}
      style={{ position: "fixed", left: 600, top: 100 }}
    >
      Pinned chat
    </button>
  );
}

it("still places a pinned thread on a Workstream from its chat title", async () => {
  await withSidebar(
    {
      threads: [
        makeThread("a1"),
        makeThread("a2", { pinnedAt: DateTime.makeUnsafe(at) }),
        makeThread("a3"),
      ],
      assignments: { a1: "alpha", a3: "alpha" },
    },
    async ({ assign, finishDrop, replaySavedOrder }) => {
      await vi.waitFor(() => expect(rows("alpha")).toEqual(["a1", "a3"]));
      await using screen = await render(<PinnedChatTitle />, { router: false });
      const source = screen.container.querySelector("button")!;
      expect(await dropAt(source, row("a1"), true)).toBe(true);
      expect(await finishDrop()).toBe(true);
      expect(assign.mock.calls.map(([thread, id]) => [thread.id, id])).toEqual([["a2", "alpha"]]);
      expect(unpins().map(({ threadId }) => threadId)).toEqual(["a2"]);
      expect(activeWrites()).toEqual([]);
      await replaySavedOrder();
      await vi.waitFor(() => expect(rows("alpha")).toEqual(["a1", "a2", "a3"]));
    },
  );
});

it.each([
  { destination: "alpha", folders: false },
  { destination: null, folders: true },
])(
  "uses the requested non-natural active slot from the pin shelf (destination=$destination)",
  async ({ destination, folders }) => {
    await withSidebar(
      {
        threads: [
          makeThread("a1", { activeOrderKey: "m" }),
          makeThread("a2", { activeOrderKey: "t" }),
          makeThread("pin", { pinnedAt: DateTime.makeUnsafe(at), activeOrderKey: "z" }),
        ],
        assignments: destination === null ? {} : { a1: destination, a2: destination },
        folders,
      },
      async ({ assign, finishDrop, replaySavedOrder }) => {
        await vi.waitFor(() => expect(rows(destination)).toEqual(["a1", "a2"]));
        const showedGap = await dropAt(row("pin", true), row("a1"), true);
        expect(await finishDrop()).toBe(true);
        expect(assign.mock.calls.map(([thread, id]) => [thread.id, id])).toEqual(
          destination === null ? [] : [["pin", destination]],
        );
        expect(
          dispatchedCommands()
            .filter(({ type }) => type === "thread.unpin" || type === "thread.active.reorder")
            .map(({ type }) => type),
        ).toEqual(["thread.unpin", "thread.active.reorder"]);
        await replaySavedOrder();
        await vi.waitFor(() => expect(rows(destination)).toEqual(["a1", "pin", "a2"]));
        expect(showedGap).toBe(true);
      },
    );
  },
);

it("keeps the hidden tail behind a window-edge drop and assigns through the collapsed header", async () => {
  const initial = ["a1", "a2", "a3", "a4", "a5", "a6"];
  await withSidebar(
    {
      threads: [
        makeThread("u1", { activeOrderKey: "t" }),
        makeThread("u2", { activeOrderKey: "z" }),
        ...initial.map((id, index) => makeThread(id, { activeOrderKey: "bdfhjl"[index]! })),
      ],
      assignments: Object.fromEntries(initial.map((id) => [id, "alpha"])),
    },
    async ({ assign, finishDrop, replaySavedOrder, remount }) => {
      await vi.waitFor(() => expect(rows("alpha")).toEqual(initial.slice(0, 5)));
      expect(await dropAt(row("u1"), row("a5"), true)).toBe(true);
      expect(await finishDrop()).toBe(true);
      await replaySavedOrder();
      const showMore = page.getByRole("button", { name: "Show 2 more threads in Alpha" });
      await showMore.click();
      const expanded = [...initial.slice(0, 5), "u1", "a6"];
      await vi.waitFor(() => expect(rows("alpha")).toEqual(expanded));
      await remount();
      await vi.waitFor(() => expect(rows("alpha")).toEqual(expanded));

      await page.getByRole("button", { name: "Collapse Alpha", exact: true }).click();
      const header = page.getByRole("button", { name: "Expand Alpha", exact: true });
      await expect.element(header).toBeVisible();
      expect(rows("alpha")).toEqual([]);
      expect(await dropAt(row("u2"), header.element() as HTMLElement)).toBe(false);
      expect(await finishDrop()).toBe(true);
      expect(activeWrites().map(({ threadId }) => threadId)).toEqual(["u1"]);
      expect(assign.mock.calls.map(([thread, id]) => [thread.id, id])).toEqual([
        ["u1", "alpha"],
        ["u2", "alpha"],
      ]);
      await header.click();
      await vi.waitFor(() => expect(rows("alpha")).toEqual(initial.slice(0, 5)));
      await page.getByRole("button", { name: "Show 3 more threads in Alpha" }).click();
      await vi.waitFor(() => expect(rows("alpha")).toEqual([...expanded, "u2"]));
    },
  );
});
