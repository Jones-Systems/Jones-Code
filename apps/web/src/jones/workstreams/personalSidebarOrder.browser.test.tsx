import { createMemoryHistory } from "@tanstack/react-router";
import { Atom } from "effect/reactivity";
import type * as Context from "effect/Context";
import { DEFAULT_CLIENT_SETTINGS } from "@t3tools/contracts/settings";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import type { PersonalWorkstreamsState } from "@t3tools/client-runtime/jones/workstreams/personal";
import { personalThreadKey } from "@t3tools/client-runtime/jones/workstreams/personal";
import { AppRoot } from "~/AppRoot";
import "~/components/ChatView";
import { __setClientSettingsForTests } from "~/hooks/useSettings";
import { getRouter } from "~/router";
import { usePersonalWorkstreams } from "~/state/workstreamsPersonal";
import {
  ENVIRONMENT_ID,
  installFakeEnvironment,
  threadWithHistory,
  type FakeDriver,
} from "../../../test/fakeEnvironment";
import { render } from "../../../test/render";

vi.mock("~/state/workstreamsPersonal", { spy: true });
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
    runs: threadWithHistory(id, 1).runs.map((run) => ({
      ...run,
      status: "running" as const,
      completedAt: null,
    })),
  }));
  using _environment = installFakeEnvironment([
    ...threads,
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
      vi.mocked(usePersonalWorkstreams).mockRestore();
      for (const key of [
        "jones:workstreams:collapsed:alpha",
        "jones:workstreams:collapsed:beta",
        "jones:workstreams:unassigned-collapsed",
        "jones:workstreams:thread-limit:alpha",
        "jones:workstreams:thread-expanded:alpha",
        "jones:workstreams:thread-limit:beta",
        "jones:workstreams:thread-expanded:beta",
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

it("traverses displayed Workstreams then Unassigned and skips collapsed groups", async () => {
  await withSidebar(async ({ router, rows, visit, step }) => {
    const order = rows();
    expect(order.slice(0, 5).every((id) => id.startsWith("a"))).toBe(true);
    expect(order[5]).toBe("b1");
    expect(order.slice(6).sort()).toEqual(["u1", "u2"]);
    await visit(order[0]!);
    await step("next", order[1]!);
    await step("previous", order[0]!);
    await visit(order[4]!);
    for (const id of order.slice(5)) await step("next", id);
    for (const id of order.slice(4, -1).reverse()) await step("previous", id);
    await userEvent.click(page.getByRole("button", { name: "Collapse beta", exact: true }));
    await visit(order[4]!);
    await step("next", order[6]!);
    await step("previous", order[4]!);
    await visit("child");
    await step("next", order[6]!);
    await visit("child");
    await step("previous", order[4]!);
    await visit("b1");
    await step("next", order[6]!);
    await visit("b1");
    await step("previous", order[4]!);
    await userEvent.click(page.getByRole("button", { name: "Expand beta", exact: true }));
    await step("next", "b1");
    await userEvent.click(page.getByRole("button", { name: "Collapse Unassigned", exact: true }));
    await step("next", "b1");
    await userEvent.keyboard("{Meta>}1{/Meta}");
    await vi.waitFor(() =>
      expect(router.history.location.pathname).toBe(`/${ENVIRONMENT_ID}/${order[0]}`),
    );
  });
});

it("keeps next, previous and numeric jumps aligned with truncated and expanded rows", async () => {
  await withSidebar(async ({ router, rows, visit, step, threadIds }) => {
    const order = rows();
    expect(order.slice(0, 5).every((id) => id.startsWith("a"))).toBe(true);
    expect(order[5]).toBe("b1");
    expect(order.slice(6).sort()).toEqual(["u1", "u2"]);
    await visit(order[4]!);
    await step("next", "b1");
    await step("previous", order[4]!);
    const hiddenAlpha = threadIds.find((id) => id.startsWith("a") && !order.includes(id))!;
    await visit(hiddenAlpha);
    await step("next", "b1");
    await visit(hiddenAlpha);
    await step("previous", order[4]!);
    await userEvent.keyboard("{Meta>}6{/Meta}");
    await vi.waitFor(() => expect(router.history.location.pathname).toBe(`/${ENVIRONMENT_ID}/b1`));
    await userEvent.click(
      page.getByRole("button", { name: "Show all 7 threads in alpha", exact: true }),
    );
    const expandedOrder = rows();
    expect(expandedOrder).toHaveLength(10);
    await visit(expandedOrder[4]!);
    await step("next", expandedOrder[5]!);
    await step("next", expandedOrder[6]!);
    await step("next", "b1");
    await userEvent.keyboard("{Meta>}6{/Meta}");
    await vi.waitFor(() =>
      expect(router.history.location.pathname).toBe(`/${ENVIRONMENT_ID}/${expandedOrder[5]}`),
    );
    await userEvent.click(
      page.getByRole("button", { name: "Show only 5 threads in alpha", exact: true }),
    );
    await userEvent.keyboard("{Meta>}6{/Meta}");
    await vi.waitFor(() => expect(router.history.location.pathname).toBe(`/${ENVIRONMENT_ID}/b1`));
  });
});
