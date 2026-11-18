import * as DateTime from "effect/DateTime";
import "~/index.css";
import { createMemoryHistory } from "@tanstack/react-router";
import { Atom } from "effect/reactivity";
import type * as Context from "effect/Context";
import { DEFAULT_CLIENT_SETTINGS } from "@t3tools/contracts/settings";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import type { PersonalWorkstreamsState } from "@t3tools/client-runtime/jones/workstreams/personal";
import { personalThreadKey } from "@t3tools/client-runtime/jones/workstreams/personal";
import * as Schema from "effect/Schema";
import { AppRoot } from "~/AppRoot";
import { __setClientSettingsForTests } from "~/hooks/useSettings";
import { getRouter } from "~/router";
import { usePersonalWorkstreams } from "~/state/workstreamsPersonal";
import { writeLocalStorageValue } from "~/hooks/useLocalStorage";
import { personalNamedGroupsHiddenKey } from "./personalSidebarOrder";
import { personalSharedStatusFilterKey, personalStatusFilterKey } from "./personalStatusFilter";
import {
  ENVIRONMENT_ID,
  installFakeEnvironment,
  refreshFakeShell,
  threadWithHistory,
  type FakeDriver,
} from "../../../test/fakeEnvironment";
import { render } from "../../../test/render";

// Navigation exercises the real Sidebar and router. Conversation layout,
// timeline measurement and composer effects are covered by their own tests.
vi.mock("~/components/ChatView", () => ({ default: () => null }));
vi.mock(import("~/state/workstreamsPersonal"), async (original) => ({
  ...(await original()),
  usePersonalWorkstreams: vi.fn(),
}));
vi.mock(
  "../../../../../packages/client-runtime/src/connection/driver.ts",
  async (importOriginal) => {
    const [actual, { fakeDriver }, Layer] = await Promise.all([
      importOriginal<{ readonly ConnectionDriver: Context.Key<unknown, FakeDriver> }>(),
      import("../../../test/fakeEnvironment"),
      import("effect/Layer"),
    ]);
    return { ...actual, layer: Layer.succeed(actual.ConnectionDriver, fakeDriver) };
  },
);
vi.mock(import("~/rpc/requestLatencyState"), async (original) => ({
  ...(await original()),
  trackRpcRequestSent: () => {},
}));
vi.mock(import("~/state/server"), async (original) => {
  const actual = await original();
  const { compileResolvedKeybindingsConfig } = await import("@t3tools/shared/keybindings");
  return {
    ...actual,
    primaryServerKeybindingsAtom: Atom.make(
      compileResolvedKeybindingsConfig([
        { key: "mod+shift+]", command: "thread.next" },
        { key: "mod+shift+[", command: "thread.previous" },
        { key: "mod+1", command: "thread.jump.1" },
        { key: "mod+6", command: "thread.jump.6" },
        { key: "mod+8", command: "thread.jump.8" },
      ]),
    ),
  };
});

async function withSidebar(
  inspect: (fixture: {
    router: ReturnType<typeof getRouter>;
    rows: () => string[];
    visit: (id: string) => Promise<void>;
    step: (direction: "next" | "previous", expected: string) => Promise<void>;
    threadIds: readonly string[];
  }) => Promise<void>,
  pinnedFixture = false,
) {
  const threadIds = [
    "a1",
    "u1",
    "b1",
    "a2",
    "u2",
    ...Array.from({ length: 5 }, (_, index) => `a${index + 3}`),
  ];
  const threads = threadIds.map((id) => ({
    ...threadWithHistory(id, 1),
    ...(pinnedFixture && !id.startsWith("u")
      ? { pinnedAt: DateTime.makeUnsafe("2026-11-17T12:00:00Z") }
      : {}),
    runs: threadWithHistory(id, 1).runs.map((run) => ({
      ...run,
      status: "running" as const,
      completedAt: null,
    })),
  }));
  using _environment = installFakeEnvironment([
    ...threads,
    ...(pinnedFixture
      ? [
          {
            ...threadWithHistory("loose-pin", 1),
            pinnedAt: DateTime.makeUnsafe("2026-11-17T12:00:00Z"),
          },
        ]
      : []),
    { ...threadWithHistory("child", 1), sideChatOf: threads[2]!.id },
  ]);
  using _platform = vi.spyOn(navigator, "platform", "get").mockReturnValue("MacIntel");
  const at = "2026-11-16T12:00:00.000Z";
  const state: PersonalWorkstreamsState = {
    workstreams: ["alpha", "beta"].map((id, index) => ({
      id,
      name: id,
      lifecycle: "active",
      color: null,
      orderKey: index === 0 ? "n" : "t",
      createdAt: at,
      updatedAt: at,
    })),
    environments: [
      { environmentId: ENVIRONMENT_ID, status: "ready", confirmed: true, workstreamCount: 2 },
    ],
    assignmentsByThreadKey: new Map(
      threads
        .filter((thread) => !thread.id.startsWith("u"))
        .map((thread) => [
          personalThreadKey({ environmentId: ENVIRONMENT_ID, id: thread.id }),
          {
            threadId: thread.id,
            workstreamId: thread.id.startsWith("a") ? "alpha" : "beta",
            revision: 1,
            updatedAt: at,
          },
        ]),
    ),
    pendingThreadKeys: new Set(),
    canCreate: false,
  };
  const unexpectedMutation = vi.fn(async () => {
    throw new Error("Navigation must not mutate Workstreams");
  });
  const controller: ReturnType<typeof usePersonalWorkstreams>["controller"] = {
    state: Atom.make(state),
    canAssign: () => false,
    create: unexpectedMutation,
    rename: unexpectedMutation,
    setColor: unexpectedMutation,
    setLifecycle: unexpectedMutation,
    reorder: unexpectedMutation,
    remove: unexpectedMutation,
    assign: unexpectedMutation,
    createAndAssign: unexpectedMutation,
    setEnvironments: vi.fn(),
    loadCache: vi.fn(),
    dispose: vi.fn(),
  };
  vi.mocked(usePersonalWorkstreams).mockReturnValue({ controller, state });
  using _restore = {
    [Symbol.dispose]() {
      vi.mocked(usePersonalWorkstreams).mockReset();
      for (const key of [
        "jones:workstreams:collapsed:alpha",
        "jones:workstreams:collapsed:beta",
        "jones:workstreams:unassigned-collapsed",
        "jones:workstreams:thread-limit:alpha",
        "jones:workstreams:thread-expanded:alpha",
        "jones:workstreams:thread-limit:beta",
        "jones:workstreams:thread-expanded:beta",
        personalStatusFilterKey("alpha"),
        personalStatusFilterKey("beta"),
        personalNamedGroupsHiddenKey,
        personalSharedStatusFilterKey,
      ])
        localStorage.removeItem(key);
    },
  };
  __setClientSettingsForTests({
    ...DEFAULT_CLIENT_SETTINGS,
    sidebarWorkingShelfEnabled: false,
    onboardingCompletedAt: at,
  });
  await page.viewport(1400, 1000);
  const router = getRouter(createMemoryHistory({ initialEntries: [`/${ENVIRONMENT_ID}/a1`] }));
  await router.load();
  await using screen = await render(<AppRoot router={router} />, { router: false });
  await refreshFakeShell();
  await expect
    .element(page.getByRole("button", { name: "Collapse alpha", exact: true }))
    .toBeVisible();
  const rows = () =>
    [...screen.container.querySelectorAll<HTMLElement>("[data-personal-thread]")].map(
      (row) => JSON.parse(row.dataset.personalThread!)[1] as string,
    );
  await vi.waitFor(() => expect(rows()).toHaveLength(8), {
    timeout: 8000,
  });
  const visit = async (id: string) =>
    router.navigate({
      to: "/$environmentId/$threadId",
      params: { environmentId: ENVIRONMENT_ID, threadId: id },
    });
  const step = async (direction: "next" | "previous", expected: string) => {
    if (document.activeElement instanceof HTMLElement) document.activeElement.blur();
    await userEvent.keyboard(
      direction === "next"
        ? "{Meta>}{Shift>}]{/Shift}{/Meta}"
        : "{Meta>}{Shift>}[BracketLeft]{/Shift}{/Meta}",
    );
    await vi.waitFor(() =>
      expect(router.history.location.pathname).toBe(`/${ENVIRONMENT_ID}/${expected}`),
    );
  };
  await inspect({ router, rows, visit, step, threadIds });
}

it("traverses Unassigned before named Workstreams and skips collapsed groups", async () => {
  await withSidebar(async ({ router, rows, visit, step }) => {
    const header = page.getByRole("button", { name: "Hide all Workstreams", exact: true });
    const unassigned = page.getByRole("button", { name: "Collapse Unassigned", exact: true });
    const firstNamed = page.getByRole("button", { name: "Collapse alpha", exact: true });
    expect(header.element().getBoundingClientRect().bottom).toBeLessThanOrEqual(
      unassigned.element().getBoundingClientRect().top,
    );
    expect(unassigned.element().getBoundingClientRect().bottom).toBeLessThanOrEqual(
      firstNamed.element().getBoundingClientRect().top,
    );
    const order = rows();
    expect(order.slice(0, 2).sort()).toEqual(["u1", "u2"]);
    expect(order.slice(2, 7).every((id) => id.startsWith("a"))).toBe(true);
    expect(order[7]).toBe("b1");
    await visit(order[0]!);
    for (const id of order.slice(1)) await step("next", id);
    for (const id of order.slice(0, -1).reverse()) await step("previous", id);
    // Click the title's gesture layer rather than the background toggle beneath it.
    await userEvent.click(page.getByText("beta", { exact: true }));
    expect(rows()).toEqual(order.slice(0, -1));
    await visit("child");
    await step("previous", order[6]!);
    await visit("b1");
    await step("previous", order[6]!);
    await userEvent.click(page.getByText("beta", { exact: true }));
    await step("next", "b1");
    await userEvent.click(page.getByRole("button", { name: "Collapse Unassigned", exact: true }));
    expect(rows()).toEqual(order.slice(2));
    await userEvent.keyboard("{Meta>}1{/Meta}");
    await vi.waitFor(() =>
      expect(router.history.location.pathname).toBe(`/${ENVIRONMENT_ID}/${order[2]}`),
    );
    await userEvent.click(page.getByRole("button", { name: "Expand Unassigned", exact: true }));
    expect(rows()).toEqual(order);
    await userEvent.keyboard("{Meta>}1{/Meta}");
    await vi.waitFor(() =>
      expect(router.history.location.pathname).toBe(`/${ENVIRONMENT_ID}/${order[0]}`),
    );
  });
});

it("leaves hidden Workstream rows out of traversal and numeric jumps, keeping only the routed row as an anchor", async () => {
  await withSidebar(async ({ router, rows, visit, step }) => {
    const order = rows();
    const pathname = () => router.history.location.pathname;
    const clickDisclosureArrow = async (name: string) => {
      const disclosure = page.getByRole("button", { name, exact: true });
      const button = disclosure.element();
      const bounds = button.getBoundingClientRect();
      const arrow = button.querySelector("svg")!.getBoundingClientRect();
      await userEvent.click(disclosure, {
        position: { x: arrow.left + arrow.width / 2 - bounds.left, y: bounds.height / 2 },
      });
    };
    await clickDisclosureArrow("Hide all Workstreams");
    await vi.waitFor(() => expect([...rows()].sort()).toEqual(["u1", "u2"]));
    const unassigned = rows();
    // The route starts on a hidden Workstream row: it anchors the step but no other hidden row does.
    await step("previous", unassigned[1]!);
    await step("previous", unassigned[0]!);
    await step("next", unassigned[1]!);
    await visit("b1");
    await step("previous", unassigned[1]!);
    await userEvent.keyboard("{Meta>}1{/Meta}");
    await vi.waitFor(() => expect(pathname()).toBe(`/${ENVIRONMENT_ID}/${unassigned[0]}`));
    await userEvent.keyboard("{Meta>}6{/Meta}");
    expect(pathname()).toBe(`/${ENVIRONMENT_ID}/${unassigned[0]}`);
    await clickDisclosureArrow("Show all Workstreams");
    await vi.waitFor(() => expect(rows()).toEqual(order));
    await userEvent.keyboard("{Meta>}1{/Meta}");
    await vi.waitFor(() => expect(pathname()).toBe(`/${ENVIRONMENT_ID}/${order[0]}`));
  });
});

it("indents real Workstream thread rows by 8 px and leaves Unassigned rows flush", async () => {
  await withSidebar(async () => {
    const list = (label: string) =>
      document.querySelector<HTMLElement>(`[data-personal-thread-list][aria-label="${label}"]`)!;
    const offset = (label: string) =>
      list(label).querySelector("[data-personal-thread]")!.getBoundingClientRect().left -
      list(label).getBoundingClientRect().left;
    for (const label of ["alpha threads", "beta threads"]) {
      expect(getComputedStyle(list(label)).paddingLeft).toBe("8px");
      expect(offset(label)).toBeCloseTo(8, 1);
    }
    expect(getComputedStyle(list("Unassigned threads")).paddingLeft).toBe("0px");
    expect(offset("Unassigned threads")).toBeCloseTo(0, 1);
  });
});

it("uses local Workstream status overrides for rendered rows, traversal and numeric jumps", async () => {
  await withSidebar(async ({ router, rows, visit, step }) => {
    const setWorking = (id: string) =>
      writeLocalStorageValue(
        personalStatusFilterKey(id),
        "working",
        "working",
        Schema.Literals(["working"]),
      );
    setWorking("alpha");
    const globalFilter = page.getByRole("button", { name: /Showing all threads\. Next:/ });
    await userEvent.click(globalFilter);
    await userEvent.keyboard("{Enter}{Enter}");
    await vi.waitFor(() => {
      expect(rows()).toHaveLength(5);
      expect(rows().every((id) => id.startsWith("a"))).toBe(true);
    });
    const alphaOrder = rows();
    await visit(alphaOrder[0]!);
    await step("next", alphaOrder[1]!);
    setWorking("beta");
    await vi.waitFor(() => expect(rows()).toEqual([...alphaOrder, "b1"]));
    await visit(alphaOrder[4]!);
    await step("next", "b1");
    await step("previous", alphaOrder[4]!);
    await userEvent.keyboard("{Meta>}6{/Meta}");
    await vi.waitFor(() => expect(router.history.location.pathname).toBe(`/${ENVIRONMENT_ID}/b1`));
  });
});

it("keeps next, previous and numeric jumps aligned with truncated and expanded rows", async () => {
  await withSidebar(async ({ router, rows, visit, step, threadIds }) => {
    const order = rows().slice(2);
    expect(order.slice(0, 5).every((id) => id.startsWith("a"))).toBe(true);
    expect(order[5]).toBe("b1");
    await visit(order[4]!);
    await step("next", "b1");
    await step("previous", order[4]!);
    const hiddenAlpha = threadIds.find((id) => id.startsWith("a") && !order.includes(id))!;
    await visit(hiddenAlpha);
    await step("next", "b1");
    await visit(hiddenAlpha);
    await step("previous", order[4]!);
    await userEvent.keyboard("{Meta>}8{/Meta}");
    await vi.waitFor(() => expect(router.history.location.pathname).toBe(`/${ENVIRONMENT_ID}/b1`));
    await userEvent.click(
      page.getByRole("button", { name: "Show 2 more threads in alpha", exact: true }),
    );
    const expandedOrder = rows().slice(2);
    expect(expandedOrder).toHaveLength(8);
    await visit(expandedOrder[4]!);
    await step("next", expandedOrder[5]!);
    await step("next", expandedOrder[6]!);
    await step("next", "b1");
    await userEvent.keyboard("{Meta>}8{/Meta}");
    await vi.waitFor(() =>
      expect(router.history.location.pathname).toBe(`/${ENVIRONMENT_ID}/${expandedOrder[5]}`),
    );
    await userEvent.click(
      page.getByRole("button", { name: "Show only 5 threads in alpha", exact: true }),
    );
    await userEvent.keyboard("{Meta>}8{/Meta}");
    await vi.waitFor(() => expect(router.history.location.pathname).toBe(`/${ENVIRONMENT_ID}/b1`));
  });
});

it("shows assigned pinned members once inside Workstreams above the remaining pinned shelf", async () => {
  await withSidebar(async ({ rows, visit, step, router }) => {
    expect(rows()).toHaveLength(8);
    const header = document.querySelector("[data-personal-workstreams-header]")!;
    const findPin = () =>
      [...document.querySelectorAll<HTMLElement>("li[data-thread-item]")].find((row) =>
        row.dataset.threadItem?.includes("loose-pin"),
      );
    await vi.waitFor(() => expect(findPin()).toBeDefined());
    const pinRow = findPin()!;
    expect(header.compareDocumentPosition(pinRow) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(document.querySelectorAll("[data-personal-thread]")).toHaveLength(8);
    await expect
      .element(page.getByRole("button", { name: "Show 2 more threads in alpha", exact: true }))
      .toBeVisible();
    await visit(rows().at(-1)!);
    await step("next", "loose-pin");
    await userEvent.keyboard("{Meta>}1{/Meta}");
    await vi.waitFor(() =>
      expect(router.history.location.pathname).toBe(`/${ENVIRONMENT_ID}/${rows()[0]}`),
    );
  }, true);
});
