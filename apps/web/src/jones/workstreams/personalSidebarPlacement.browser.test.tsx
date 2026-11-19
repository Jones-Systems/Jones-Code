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
import { expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";
import { AppRoot } from "~/AppRoot";
import { __setClientSettingsForTests } from "~/hooks/useSettings";
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

function press(target: EventTarget, type: string, element: Element, fraction: number) {
  const bounds = element.getBoundingClientRect();
  const event = new PointerEvent(type, {
    bubbles: true,
    cancelable: true,
    pointerId: 1,
    pointerType: "mouse",
    isPrimary: true,
    button: 0,
    buttons: type === "pointerup" ? 0 : 1,
    clientX: bounds.left + bounds.width / 2,
    clientY: bounds.top + bounds.height * fraction,
  });
  target.dispatchEvent(event);
  return event;
}

async function dropAt(from: HTMLElement, neighbor: HTMLElement, after = false) {
  const source = from.querySelector<HTMLElement>("[data-sidebar-row]") ?? from;
  const fraction = after ? 0.8 : 0.2;
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
  await act(() => {
    press(document, "pointermove", neighbor, fraction);
  });
  const gap = document.querySelector("[data-personal-thread-insertion]");
  if (gap) expect(after ? gap.previousElementSibling : gap.nextElementSibling).toBe(neighbor);
  await act(() => press(document, "pointerup", gap ?? neighbor, gap ? 0.5 : fraction));
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

async function withSidebar(
  {
    threads,
    assignments,
    folders = false,
    snoozed = false,
  }: {
    threads: FakeThread[];
    assignments: Readonly<Record<string, string>>;
    folders?: boolean;
    snoozed?: boolean;
  },
  inspect: (fixture: {
    assign: ReturnType<typeof vi.fn>;
    finishDrop: () => Promise<boolean>;
    replaySavedOrder: () => Promise<void>;
    wakeAlpha: () => Promise<void>;
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
      { environmentId: ENVIRONMENT_ID, status: "ready", confirmed: true, workstreamCount: 2 },
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
      remount: async () => {
        const saved = structuredClone(appAtomRegistry.get(stateAtom));
        await screen[Symbol.asyncDispose]();
        stateAtom = Atom.make(saved);
        screen = await mount();
      },
    });
  } finally {
    await screen[Symbol.asyncDispose]();
    vi.mocked(usePersonalWorkstreams).mockReset();
    useUiStateStore.getState().setSidebarProjectScopeKeys([]);
  }
}

it("reorders a woken snoozed-group member in Unassigned and restores its saved membership on wake", async () => {
  const woke = { lastSnoozeWakeAt: DateTime.makeUnsafe("2026-03-18T00:00:00Z") };
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
        await dropAt(row("a4"), row("a1"));
        const saved = await finishDrop();
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
        await dropAt(row("a2", true), row("a1"), true);
        const saved = await finishDrop();
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
