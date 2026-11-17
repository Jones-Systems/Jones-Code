import "~/index.css";
import { RegistryContext, useAtomValue } from "@effect/atom-react";
import {
  groupPersonalThreads,
  personalThreadKey,
  type PersonalWorkstreamsController,
  type PersonalWorkstreamsState,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { Atom, AtomRegistry } from "effect/reactivity";
import { describe, expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import { act, render } from "../../../test/render";
import { WorkstreamNativeSidebar } from "../../components/workstreams/WorkstreamNativeSidebar";
import { makeThreadFixture } from "../../test-fixtures";
import {
  personalGroupCollapsedKey,
  personalNamedGroupsHiddenKey,
  usePersonalSidebarActiveThreads,
} from "./personalSidebarOrder";
import { personalSharedStatusFilterKey } from "./personalStatusFilter";
import { personalThreadExpandedKey, personalThreadLimitKey } from "./personalThreadWindow";

const environmentId = EnvironmentId.make("thread-window-browser-env");
const alphaId = "thread-window-browser-alpha";
const betaId = "thread-window-browser-beta";
const at = "2026-11-16T12:00:00.000Z";

function fixture(betaCount = 5) {
  const keys = [
    ...[alphaId, betaId].flatMap((id) => [
      personalGroupCollapsedKey(id),
      personalThreadLimitKey(id),
      personalThreadExpandedKey(id),
    ]),
    personalGroupCollapsedKey(null),
    personalNamedGroupsHiddenKey,
    personalSharedStatusFilterKey,
  ];
  const previous = new Map(keys.map((key) => [key, localStorage.getItem(key)]));
  for (const key of keys) localStorage.removeItem(key);
  const registry = AtomRegistry.make();
  const makeThreads = (prefix: string, count: number) =>
    Array.from({ length: count }, (_, index) =>
      makeThreadFixture({
        environmentId,
        id: ThreadId.make(`thread-window-${prefix}-${index + 1}`),
        title: `${prefix} conversation ${index + 1}`,
        pinnedAt: null,
        archivedAt: null,
        settledAt: null,
      }),
    );
  const alpha = makeThreads("Alpha", 13);
  const beta = makeThreads("Beta", betaCount);
  const unassigned = makeThreads("Unassigned", 7);
  const threads = [...alpha, ...beta, ...unassigned];
  const initial: PersonalWorkstreamsState = {
    workstreams: [
      { id: alphaId, name: "Alpha", orderKey: "n" },
      { id: betaId, name: "Beta", orderKey: "t" },
    ].map((workstream) => ({
      ...workstream,
      lifecycle: "active",
      color: null,
      createdAt: at,
      updatedAt: at,
    })),
    environments: [{ environmentId, status: "ready", confirmed: true, workstreamCount: 2 }],
    assignmentsByThreadKey: new Map(
      [...alpha, ...beta].map((thread) => [
        personalThreadKey(thread),
        {
          threadId: thread.id,
          workstreamId: alpha.includes(thread) ? alphaId : betaId,
          revision: 1,
          updatedAt: at,
        },
      ]),
    ),
    pendingThreadKeys: new Set(),
    canCreate: true,
  };
  const unexpectedMutation = vi.fn(async () => {
    throw new Error("Thread window preferences must not mutate the Workstreams controller");
  });
  const controller: PersonalWorkstreamsController = {
    state: Atom.make(initial),
    canAssign: () => true,
    create: unexpectedMutation,
    rename: unexpectedMutation,
    setColor: unexpectedMutation,
    setLifecycle: unexpectedMutation,
    reorder: unexpectedMutation,
    remove: unexpectedMutation,
    assign: unexpectedMutation,
    createAndAssign: unexpectedMutation,
  };
  return {
    registry,
    controller,
    alpha,
    beta,
    unassigned,
    threads,
    unexpectedMutation,
    [Symbol.dispose]() {
      registry.dispose();
      for (const [key, value] of previous) {
        if (value === null) localStorage.removeItem(key);
        else localStorage.setItem(key, value);
      }
    },
  };
}

type Fixture = ReturnType<typeof fixture>;

function NavigationProbe({
  setup,
  currentThread,
}: {
  setup: Fixture;
  currentThread: EnvironmentThreadShell | null;
}) {
  const state = useAtomValue(setup.controller.state);
  const { visible, traversal } = usePersonalSidebarActiveThreads(
    groupPersonalThreads(state, setup.threads),
    currentThread,
  );
  return (
    <>
      <output hidden data-window-visible>
        {JSON.stringify(visible.map((thread) => thread.id))}
      </output>
      <output hidden data-window-traversal>
        {JSON.stringify(traversal.map((thread) => thread.id))}
      </output>
    </>
  );
}

function Sidebar({
  setup,
  currentThread = null,
}: {
  setup: Fixture;
  currentThread?: EnvironmentThreadShell | null;
}) {
  return (
    <RegistryContext.Provider value={setup.registry}>
      <WorkstreamNativeSidebar
        controller={setup.controller}
        threads={setup.threads}
        renderThread={(thread) => <li data-window-thread={thread.id}>{thread.title}</li>}
        canOperate={() => true}
        canReorder={() => false}
        reorder={setup.unexpectedMutation}
      />
      <NavigationProbe setup={setup} currentThread={currentThread} />
    </RegistryContext.Provider>
  );
}

const ids = (threads: readonly EnvironmentThreadShell[]) => threads.map((thread) => thread.id);
const rowIds = (container: ParentNode) =>
  [...container.querySelectorAll<HTMLElement>("[data-window-thread]")].map(
    (row) => row.dataset.windowThread,
  );

function section(container: HTMLElement, id: string | null) {
  const result = [...container.querySelectorAll<HTMLElement>("[data-native-workstream]")].find(
    (entry) => entry.dataset.nativeWorkstream === JSON.stringify(id),
  );
  if (!result) throw new Error(`Missing Workstream section ${id}`);
  return result;
}

const options = (name: string) =>
  page.getByRole("button", { name: `Options for ${name}`, exact: true });
const radio = (limit: number) =>
  page.getByRole("menuitemradio", { name: String(limit), exact: true });
const showAll = (name: string) =>
  page.getByRole("button", { name: `Show all 13 threads in ${name}`, exact: true });
const showFewer = (name: string, limit: number) =>
  page.getByRole("button", { name: `Show only ${limit} threads in ${name}`, exact: true });

async function chooseLimit(name: string, limit: number) {
  await userEvent.click(options(name));
  await userEvent.click(radio(limit));
  await expect.element(radio(limit)).toHaveAttribute("aria-checked", "true");
  await userEvent.keyboard("{Escape}");
}

describe("personal Workstream thread windows", () => {
  it("preserves Show all when double-click rename is cancelled", async () => {
    using setup = fixture();
    await using screen = await render(<Sidebar setup={setup} />);
    await userEvent.click(showAll("Alpha"));
    await userEvent.dblClick(page.getByText("Alpha", { exact: true }));
    await expect.element(page.getByRole("textbox", { name: "Workstream name" })).toHaveFocus();
    await userEvent.keyboard("{Escape}");
    expect(rowIds(section(screen.container, alphaId))).toEqual(ids(setup.alpha));
    await expect.element(showFewer("Alpha", 5)).toBeVisible();
    expect(setup.unexpectedMutation).not.toHaveBeenCalled();
  });

  it("restores expansion for a native double-click after the first click has already collapsed", async () => {
    using setup = fixture();
    await using screen = await render(<Sidebar setup={setup} />);
    await userEvent.click(showAll("Alpha"));
    const title = () =>
      section(screen.container, alphaId).querySelector("[data-workstream-header] .line-clamp-2")!;
    await act(() => title().dispatchEvent(new MouseEvent("click", { bubbles: true, detail: 1 })));
    expect(rowIds(section(screen.container, alphaId))).toEqual([]);
    await act(() => {
      title().dispatchEvent(new MouseEvent("click", { bubbles: true, detail: 2 }));
      title().dispatchEvent(new MouseEvent("dblclick", { bubbles: true, detail: 2 }));
    });
    await expect.element(page.getByRole("textbox", { name: "Workstream name" })).toHaveFocus();
    await userEvent.keyboard("{Escape}");
    expect(rowIds(section(screen.container, alphaId))).toEqual(ids(setup.alpha));
    await expect.element(showFewer("Alpha", 5)).toBeVisible();
  });

  it("shows 5/10/15 in natural order, expands and resets, and leaves Unassigned unlimited", async () => {
    using setup = fixture();
    await using screen = await render(<Sidebar setup={setup} />);
    const alpha = section(screen.container, alphaId);
    const beta = section(screen.container, betaId);
    const unassigned = section(screen.container, null);
    expect(rowIds(alpha)).toEqual(ids(setup.alpha.slice(0, 5)));
    expect(rowIds(beta)).toEqual(ids(setup.beta));
    expect(rowIds(unassigned)).toEqual(ids(setup.unassigned));
    await expect
      .element(page.getByRole("button", { name: "Collapse Alpha", exact: true }))
      .toHaveAccessibleDescription(/13 threads/);
    await expect.element(showAll("Alpha")).toHaveTextContent("Show all 13");
    await expect.element(showAll("Alpha")).toHaveAttribute("aria-expanded", "false");
    await expect
      .element(page.getByRole("button", { name: "Show all 5 threads in Beta", exact: true }))
      .not.toBeInTheDocument();
    await expect.element(showFewer("Beta", 5)).not.toBeInTheDocument();
    await expect.element(options("Unassigned")).not.toBeInTheDocument();
    await expect
      .element(page.getByRole("button", { name: "Show all 7 threads in Unassigned", exact: true }))
      .not.toBeInTheDocument();

    await userEvent.click(options("Alpha"));
    await expect.element(page.getByText("Visible threads", { exact: true })).toBeVisible();
    await expect
      .element(page.getByRole("group", { name: "Default visible threads", exact: true }))
      .toBeVisible();
    await expect.element(radio(5)).toHaveAttribute("aria-checked", "true");
    await userEvent.click(radio(10));
    await expect.element(radio(10)).toHaveAttribute("aria-checked", "true");
    await expect.element(radio(5)).toHaveAttribute("aria-checked", "false");
    expect(rowIds(alpha)).toEqual(ids(setup.alpha.slice(0, 10)));
    await userEvent.click(radio(15));
    await expect.element(radio(15)).toHaveAttribute("aria-checked", "true");
    await expect.element(radio(10)).toHaveAttribute("aria-checked", "false");
    expect(rowIds(alpha)).toEqual(ids(setup.alpha));
    await userEvent.keyboard("{Escape}");
    await expect.element(showAll("Alpha")).not.toBeInTheDocument();
    await expect.element(showFewer("Alpha", 15)).not.toBeInTheDocument();

    await chooseLimit("Alpha", 5);
    await userEvent.click(showAll("Alpha"));
    expect(rowIds(alpha)).toEqual(ids(setup.alpha));
    await expect.element(showFewer("Alpha", 5)).toHaveTextContent("Show fewer");
    await expect.element(showFewer("Alpha", 5)).toHaveAttribute("aria-expanded", "true");
    await chooseLimit("Alpha", 10);
    expect(rowIds(alpha)).toEqual(ids(setup.alpha.slice(0, 10)));
    await expect.element(showAll("Alpha")).toHaveAttribute("aria-expanded", "false");
    await userEvent.click(showAll("Alpha"));
    await userEvent.click(showFewer("Alpha", 10));
    expect(rowIds(alpha)).toEqual(ids(setup.alpha.slice(0, 10)));
    await userEvent.click(showAll("Alpha"));
    await userEvent.click(page.getByRole("button", { name: "Collapse Alpha", exact: true }));
    await expect.poll(() => rowIds(alpha)).toEqual([]);
    await expect.element(showFewer("Alpha", 10)).not.toBeInTheDocument();
    await userEvent.click(page.getByRole("button", { name: "Expand Alpha", exact: true }));
    await expect.poll(() => rowIds(alpha)).toEqual(ids(setup.alpha.slice(0, 10)));
    await expect.element(showAll("Alpha")).toHaveAttribute("aria-expanded", "false");
    expect(rowIds(beta)).toEqual(ids(setup.beta));
    expect(rowIds(unassigned)).toEqual(ids(setup.unassigned));
    expect(setup.unexpectedMutation).not.toHaveBeenCalled();
  });

  it("persists each group's default and expansion independently across a remount", async () => {
    using setup = fixture(13);
    await using screen = await render(<Sidebar setup={setup} />);
    const rows = (id: string) => rowIds(section(screen.container, id));
    await chooseLimit("Alpha", 10);
    expect(rows(alphaId)).toEqual(ids(setup.alpha.slice(0, 10)));
    await expect.poll(() => rows(betaId)).toEqual(ids(setup.beta.slice(0, 5)));
    await userEvent.click(showAll("Alpha"));
    await chooseLimit("Beta", 15);
    expect(rows(alphaId)).toEqual(ids(setup.alpha));
    expect(rows(betaId)).toEqual(ids(setup.beta));
    await chooseLimit("Beta", 5);
    expect(rows(alphaId)).toEqual(ids(setup.alpha));
    expect(rows(betaId)).toEqual(ids(setup.beta.slice(0, 5)));
    await userEvent.click(showAll("Beta"));
    await userEvent.click(showFewer("Alpha", 10));
    expect(rows(alphaId)).toEqual(ids(setup.alpha.slice(0, 10)));
    expect(rows(betaId)).toEqual(ids(setup.beta));
    expect(localStorage.getItem(personalThreadLimitKey(alphaId))).toBe("10");
    expect(localStorage.getItem(personalThreadLimitKey(betaId))).toBe("5");
    expect(localStorage.getItem(personalThreadExpandedKey(alphaId))).toBe("false");
    expect(localStorage.getItem(personalThreadExpandedKey(betaId))).toBe("true");

    await screen.rerender(<div />);
    await screen.rerender(<Sidebar setup={setup} />);
    expect(rows(alphaId)).toEqual(ids(setup.alpha.slice(0, 10)));
    expect(rows(betaId)).toEqual(ids(setup.beta));
    await expect.element(showAll("Alpha")).toHaveAttribute("aria-expanded", "false");
    await expect.element(showFewer("Beta", 5)).toHaveAttribute("aria-expanded", "true");
    for (const [name, limit] of [
      ["Alpha", 10],
      ["Beta", 5],
    ] as const) {
      await userEvent.click(options(name));
      await expect.element(radio(limit)).toHaveAttribute("aria-checked", "true");
      await userEvent.keyboard("{Escape}");
    }
    await userEvent.click(page.getByRole("button", { name: "Collapse Beta", exact: true }));
    await userEvent.click(page.getByRole("button", { name: "Expand Beta", exact: true }));
    await expect.poll(() => rows(betaId)).toEqual(ids(setup.beta.slice(0, 5)));
    expect(rows(alphaId)).toEqual(ids(setup.alpha.slice(0, 10)));
    expect(setup.unexpectedMutation).not.toHaveBeenCalled();
  });

  it("keeps keyboard visibility aligned with rows while retaining a hidden current-route anchor", async () => {
    using setup = fixture();
    const currentThread = setup.alpha[12]!;
    await using screen = await render(<Sidebar setup={setup} currentThread={currentThread} />);
    const navigation = (kind: "visible" | "traversal") =>
      JSON.parse(screen.container.querySelector(`[data-window-${kind}]`)!.textContent!) as string[];
    const assertNavigation = async (
      visible: readonly EnvironmentThreadShell[],
      traversal: readonly EnvironmentThreadShell[] = visible,
    ) => {
      await vi.waitFor(() => {
        expect(navigation("visible")).toEqual(ids(visible));
        expect(navigation("visible")).toEqual(rowIds(screen.container));
        expect(navigation("traversal")).toEqual(ids(traversal));
      });
    };
    const tail = [...setup.beta, ...setup.unassigned];
    await assertNavigation(
      [...setup.alpha.slice(0, 5), ...tail],
      [...setup.alpha.slice(0, 5), currentThread, ...tail],
    );
    await chooseLimit("Alpha", 10);
    await assertNavigation(
      [...setup.alpha.slice(0, 10), ...tail],
      [...setup.alpha.slice(0, 10), currentThread, ...tail],
    );
    await userEvent.click(showAll("Alpha"));
    await assertNavigation([...setup.alpha, ...tail]);
    await userEvent.click(showFewer("Alpha", 10));
    await assertNavigation(
      [...setup.alpha.slice(0, 10), ...tail],
      [...setup.alpha.slice(0, 10), currentThread, ...tail],
    );
    await userEvent.click(page.getByRole("button", { name: "Collapse Alpha", exact: true }));
    await assertNavigation(tail, [currentThread, ...tail]);
    await userEvent.click(page.getByRole("button", { name: "Collapse Unassigned", exact: true }));
    await assertNavigation(setup.beta, [currentThread, ...setup.beta]);
    await userEvent.click(page.getByRole("button", { name: "Expand Alpha", exact: true }));
    await assertNavigation(
      [...setup.alpha.slice(0, 10), ...setup.beta],
      [...setup.alpha.slice(0, 10), currentThread, ...setup.beta],
    );
    expect(setup.unexpectedMutation).not.toHaveBeenCalled();
  });
});
