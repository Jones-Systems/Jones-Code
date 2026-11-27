import "~/index.css";
import { RegistryContext } from "@effect/atom-react";
import { Atom, AtomRegistry } from "effect/reactivity";
import { describe, expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import {
  createPersonalWorkstreamsController,
  type PersonalWorkstreamsController,
  type PersonalWorkstreamsState,
  type PersonalWorkstreamsTransport,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { EnvironmentId } from "@t3tools/contracts";
import { act, fakeTimers, render } from "../../../test/render";
import { WorkstreamSidebarSection } from "../../components/workstreams/WorkstreamSidebarSection";
import { ToastProvider, toastManager } from "../../components/ui/toast";
import { personalGroupCollapsedKey, personalNamedGroupsHiddenKey } from "./personalSidebarOrder";
import { personalSharedStatusFilterKey, personalStatusFilterKey } from "./personalStatusFilter";
import { personalThreadExpandedKey, personalThreadLimitKey } from "./personalThreadWindow";

const dragType = "application/x-jones-workstream";
const group = (name: string) => ({
  id: `menu-${name}`,
  name,
  lifecycle: "active" as const,
  color: null,
  orderKey: name,
  createdAt: "2026-11-12T12:00:00.000Z",
  updatedAt: "2026-11-12T12:00:00.000Z",
});
const groups = [group("Alpha"), group("Beta"), group("Gamma")];

function fixture(
  put?: PersonalWorkstreamsTransport["put"],
  memberCount = 0,
  newThreadAction?: React.ComponentProps<typeof WorkstreamSidebarSection>["newThreadAction"],
) {
  const keys = [
    personalGroupCollapsedKey(null),
    personalNamedGroupsHiddenKey,
    personalSharedStatusFilterKey,
    ...groups.flatMap((entry) => [
      personalGroupCollapsedKey(entry.id),
      personalStatusFilterKey(entry.id),
      personalThreadLimitKey(entry.id),
      personalThreadExpandedKey(entry.id),
    ]),
  ];
  const previous = new Map(keys.map((key) => [key, localStorage.getItem(key)]));
  for (const key of keys) localStorage.removeItem(key);
  const registry = AtomRegistry.make();
  const initial: PersonalWorkstreamsState = {
    workstreams: groups,
    environments: [
      {
        environmentId: EnvironmentId.make("snooze-fixture"),
        status: "ready",
        confirmed: true,
        workstreamCount: groups.length,
      },
    ],
    canSnooze: true,
    assignmentsByThreadKey: new Map(),
    pendingThreadKeys: new Set(),
    canCreate: true,
  };
  const state = Atom.make(initial);
  const mockController: PersonalWorkstreamsController = {
    state,
    canAssign: () => true,
    create: vi.fn(),
    rename: vi.fn<PersonalWorkstreamsController["rename"]>(async (id, name) => ({
      ok: true as const,
      value: { ...groups.find((entry) => entry.id === id)!, name },
    })),
    setSnoozedUntil: vi.fn(async (id, snoozedUntil) => {
      const current = registry.get(state);
      const value = { ...current.workstreams.find((entry) => entry.id === id)!, snoozedUntil };
      registry.set(state, {
        ...current,
        workstreams: current.workstreams.map((entry) => (entry.id === id ? value : entry)),
      });
      return { ok: true as const, value };
    }),
    setColor: vi.fn<PersonalWorkstreamsController["setColor"]>(async (id, color) => ({
      ok: true as const,
      value: { ...groups.find((entry) => entry.id === id)!, color },
    })),
    setLifecycle: vi.fn<PersonalWorkstreamsController["setLifecycle"]>(async (id, lifecycle) => {
      const current = registry.get(state);
      const value = { ...current.workstreams.find((entry) => entry.id === id)!, lifecycle };
      registry.set(state, {
        ...current,
        workstreams: current.workstreams.map((entry) => (entry.id === id ? value : entry)),
      });
      return { ok: true as const, value };
    }),
    reorder: vi.fn<PersonalWorkstreamsController["reorder"]>(async (id, { beforeId, afterId }) => {
      const current = registry.get(state);
      const value = current.workstreams.find((entry) => entry.id === id)!;
      const others = current.workstreams.filter((entry) => entry.id !== id);
      const index = afterId ? others.findIndex((entry) => entry.id === afterId) : others.length;
      expect(others[index - 1]?.id ?? null).toBe(beforeId);
      registry.set(state, {
        ...current,
        workstreams: [...others.slice(0, index), value, ...others.slice(index)],
      });
      return { ok: true as const, value };
    }),
    remove: vi.fn<PersonalWorkstreamsController["remove"]>(async (id) => {
      const current = registry.get(state);
      registry.set(state, {
        ...current,
        workstreams: current.workstreams.filter((entry) => entry.id !== id),
      });
      return { ok: true as const, value: undefined };
    }),
    assign: vi.fn(),
    createAndAssign: vi.fn(),
  };
  const realController = put
    ? createPersonalWorkstreamsController({
        registry,
        origin: "menu-test",
        now: () => "2026-11-12T13:00:00.000Z",
        transport: {
          subscribe: (connection, onSnapshot) => {
            onSnapshot({
              environmentId: connection.environmentId,
              sequence: 0,
              workstreams: groups.map((entry, index) => ({
                ...entry,
                orderKey: ["b", "c", "d"][index]!,
                deletedAt: null,
                rev: 1,
                origin: "menu-test",
              })),
              assignments: [],
              importIssue: null,
            });
            return () => {};
          },
          put,
          assign: vi.fn(),
        },
      })
    : null;
  realController?.setEnvironments([
    {
      environmentId: "menu-environment" as EnvironmentId,
      generation: 1,
      connected: true,
      capable: true,
      writable: true,
    },
  ]);
  const controller = realController ?? mockController;
  const ui = (
    <RegistryContext.Provider value={registry}>
      <ToastProvider>
        <div style={{ width: 360 }}>
          <WorkstreamSidebarSection
            controller={controller}
            newThreadAction={newThreadAction}
            memberCounts={new Map([["menu-Alpha", memberCount]])}
            summaries={
              new Map([
                [
                  "menu-Alpha",
                  {
                    total: memberCount,
                    running: memberCount ? 3 : 0,
                    attention: memberCount ? 2 : 0,
                    done: memberCount ? 6 : 0,
                    failed: memberCount ? 1 : 0,
                  },
                ],
              ])
            }
            renderMembers={(id, window) => (
              <div className="min-h-24">
                Members of {id ?? "Unassigned"}
                {id === "menu-Alpha"
                  ? Array.from(
                      {
                        length: window?.expanded
                          ? memberCount
                          : Math.min(memberCount, window?.limit ?? memberCount),
                      },
                      (_, index) => (
                        <div key={index} data-menu-member>
                          Conversation {index + 1}
                        </div>
                      ),
                    )
                  : null}
              </div>
            )}
          />
        </div>
      </ToastProvider>
    </RegistryContext.Provider>
  );
  return {
    registry,
    state,
    initial,
    controller,
    ui,
    [Symbol.dispose]() {
      realController?.dispose();
      registry.dispose();
      toastManager.close();
      for (const [key, value] of previous) {
        if (value === null) localStorage.removeItem(key);
        else localStorage.setItem(key, value);
      }
    },
  };
}
function deferredDelete() {
  let resolve!: () => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<void>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  const put = vi.fn<PersonalWorkstreamsTransport["put"]>(async (_connection, workstream) => {
    await promise;
    return { outcome: "applied", workstream, sequence: 1 };
  });
  return { put, resolve, reject };
}
async function options(name = "Alpha") {
  await page
    .getByRole("button", { name: new RegExp(`^(?:Collapse|Expand) ${name}$`) })
    .click({ button: "right" });
}
async function deletion(name = "Alpha") {
  await options(name);
  await userEvent.click(page.getByRole("menuitem", { name: "Delete Workstream…", exact: true }));
  await expect.element(page.getByRole("alertdialog")).toBeVisible();
}
function section(container: HTMLElement, name: string | null) {
  return [...container.querySelectorAll<HTMLElement>("[data-native-workstream]")].find(
    (element) =>
      element.dataset.nativeWorkstream === JSON.stringify(name === null ? null : `menu-${name}`),
  )!;
}
function header(container: HTMLElement, name: string | null) {
  return section(container, name).querySelector<HTMLButtonElement>("button")!;
}
function drag(target: Element, type: string, dataTransfer: DataTransfer, y?: number) {
  const rect = target.getBoundingClientRect();
  const event = new DragEvent(type, {
    bubbles: true,
    cancelable: true,
    dataTransfer,
    clientX: rect.left + rect.width / 2,
    clientY: y ?? rect.top + rect.height / 2,
  });
  target.dispatchEvent(event);
  return event;
}
async function startDrag(container: HTMLElement, name: string) {
  const transfer = new DataTransfer();
  await act(() => {
    drag(header(container, name), "dragstart", transfer);
  });
  expect(transfer.getData(dragType)).toBe(`menu-${name}`);
  return transfer;
}
function order(container: HTMLElement) {
  return [...container.querySelectorAll<HTMLElement>("[data-native-workstream]")].map(
    (element) => JSON.parse(element.dataset.nativeWorkstream!) as string | null,
  );
}

describe("Workstream options and header ordering", () => {
  it("leaves the old menu corner to disclosure and opens the context menu by keyboard", async () => {
    using setup = fixture(undefined, 14);
    await using screen = await render(setup.ui);
    const group = section(screen.container, "Alpha");
    const expanded = page.getByRole("button", { name: "Collapse Alpha", exact: true });
    const collapsed = page.getByRole("button", { name: "Expand Alpha", exact: true });

    await expect
      .element(page.getByRole("button", { name: "Options for Alpha", exact: true }))
      .not.toBeInTheDocument();
    await expanded.click({
      position: { x: header(screen.container, "Alpha").clientWidth - 3, y: 6 },
    });
    await expect.element(collapsed).toHaveAttribute("aria-expanded", "false");
    await expect.element(page.getByRole("menu")).not.toBeInTheDocument();
    await userEvent.keyboard("{Shift>}{F10}{/Shift}");
    await expect.element(page.getByRole("menuitem", { name: "Rename", exact: true })).toBeVisible();
    await userEvent.keyboard("{Escape}");
    await expect.element(page.getByRole("menu")).not.toBeInTheDocument();
    await expect.element(collapsed).toHaveFocus();
    await collapsed.click();
    expect(getComputedStyle(header(screen.container, null)).cursor).toBe("pointer");
    const draggable = group.querySelector<HTMLElement>("[data-workstream-header]")!;
    expect(draggable.draggable).toBe(true);
    expect(getComputedStyle(draggable).cursor).toBe("pointer");
    const title = group.querySelector<HTMLElement>(".line-clamp-2")!;
    expect(getComputedStyle(title).cursor).toBe("grab");
    const transfer = await startDrag(screen.container, "Alpha");
    const beta = section(screen.container, "Beta");
    const rect = beta.querySelector("[data-workstream-header]")!.getBoundingClientRect();
    await act(() => {
      drag(beta, "dragover", transfer, rect.top + rect.height * 0.8);
      drag(beta, "drop", transfer, rect.top + rect.height * 0.8);
    });
    expect(setup.controller.reorder).toHaveBeenCalled();
    await act(() =>
      setup.registry.set(setup.state, { ...setup.registry.get(setup.state), canCreate: false }),
    );
    expect(draggable.draggable).toBe(true);
    expect(getComputedStyle(draggable).cursor).toBe("pointer");
    expect(getComputedStyle(title).cursor).toBe("grab");
  });

  it("segments menu actions and display choices and supports inline rename", async () => {
    using setup = fixture();
    await using screen = await render(setup.ui);
    await options();
    await expect
      .element(page.getByRole("button", { name: "Collapse Alpha", exact: true }))
      .toBeVisible();
    expect(document.querySelectorAll('[data-slot="menu-separator"]')).toHaveLength(4);
    await expect
      .element(page.getByRole("menuitem", { name: "Move up", exact: true }))
      .toHaveAttribute("aria-disabled", "true");
    await expect
      .element(page.getByRole("menuitem", { name: "Workstream state Active", exact: true }))
      .toBeVisible();
    await userEvent.click(page.getByRole("menuitem", { name: "Rename", exact: true }));
    await userEvent.fill(page.getByRole("textbox", { name: "Workstream name" }), "Renamed");
    await userEvent.keyboard("{Enter}");
    expect(setup.controller.rename).toHaveBeenCalledWith("menu-Alpha", "Renamed");
    await options("Gamma");
    await expect
      .element(page.getByRole("menuitem", { name: "Move down", exact: true }))
      .toHaveAttribute("aria-disabled", "true");
    await userEvent.keyboard("{Escape}");
    await userEvent.click(page.getByRole("button", { name: "Collapse Alpha", exact: true }));
    await expect
      .poll(() => section(screen.container, "Alpha").textContent)
      .not.toContain("Members of menu-Alpha");
  });

  it("applies a preset from the context submenu and restores Automatic with the keyboard", async () => {
    let sequence = 0;
    const put = vi.fn<PersonalWorkstreamsTransport["put"]>(async (_connection, workstream) => ({
      outcome: "applied",
      workstream,
      sequence: ++sequence,
    }));
    using setup = fixture(put);
    await using screen = await render(setup.ui);
    const alpha = section(screen.container, "Alpha");
    const accent = () => alpha.style.getPropertyValue("--jones-board-section-accent");
    const initialAccent = accent();
    const changeColor = page.getByRole("menuitem", { name: "Change color", exact: true });
    const automatic = page.getByRole("menuitemradio", { name: "Automatic", exact: true });
    const rose = page.getByRole("menuitemradio", { name: "Rose", exact: true });

    await options();
    await expect.element(changeColor).toHaveAttribute("aria-haspopup", "menu");
    await userEvent.click(changeColor);
    await expect.element(automatic).toHaveAttribute("aria-checked", "true");
    await userEvent.click(rose);
    await expect.poll(accent).toBe("#F43F5E");
    await expect.element(changeColor).not.toBeInTheDocument();
    expect(put).toHaveBeenNthCalledWith(
      1,
      expect.anything(),
      expect.objectContaining({
        id: "menu-Alpha",
        color: "#F43F5E",
      }),
    );

    await act(() => header(screen.container, "Alpha").focus());
    await userEvent.keyboard("{Shift>}{F10}{/Shift}");
    await expect.element(changeColor).toBeVisible();
    await userEvent.keyboard("{Home}{ArrowDown}");
    await expect.element(changeColor).toHaveFocus();
    await userEvent.keyboard("{ArrowRight}");
    await expect.element(rose).toHaveAttribute("aria-checked", "true");
    await userEvent.keyboard("{Home}{Enter}");
    await expect.poll(accent).toBe(initialAccent);
    await expect.element(changeColor).not.toBeInTheDocument();
    expect(put).toHaveBeenNthCalledWith(
      2,
      expect.anything(),
      expect.objectContaining({
        id: "menu-Alpha",
        color: null,
      }),
    );
  });

  it("keeps an existing custom color selected until a different choice is made", async () => {
    using setup = fixture();
    setup.registry.set(setup.state, {
      ...setup.initial,
      workstreams: [{ ...groups[0]!, color: "#abcde0" }, ...groups.slice(1)],
    });
    await using screen = await render(setup.ui);
    const alpha = section(screen.container, "Alpha");
    const changeColor = page.getByRole("menuitem", { name: "Change color", exact: true });
    const custom = page.getByRole("menuitemradio", { name: "Custom (#ABCDE0)", exact: true });

    await options();
    await userEvent.click(changeColor);
    await expect.element(custom).toHaveAttribute("aria-checked", "true");
    await userEvent.click(custom);
    await expect.element(changeColor).not.toBeInTheDocument();
    expect(alpha.style.getPropertyValue("--jones-board-section-accent")).toBe("#abcde0");
    expect(setup.controller.setColor).not.toHaveBeenCalled();
  });

  it("uses a compact Show more footer and updates its count while the window menu stays open", async () => {
    using setup = fixture(undefined, 12);
    await using screen = await render(setup.ui);
    const alpha = section(screen.container, "Alpha");
    expect(alpha.querySelectorAll("[data-menu-member]")).toHaveLength(5);
    const showMore = page.getByRole("button", {
      name: "Show 7 more threads in Alpha",
      exact: true,
    });
    const button = showMore.element();
    expect(button.textContent?.trim()).toBe("Show 7 more");
    const buttonRect = button.getBoundingClientRect();
    const label = document.createRange();
    label.selectNodeContents(button);
    const labelRect = label.getBoundingClientRect();
    expect(buttonRect.height).toBe(matchMedia("(pointer: coarse)").matches ? 24 : 16);
    expect(buttonRect.width).toBeCloseTo(alpha.clientWidth, 0);
    expect(
      Math.abs((labelRect.left + labelRect.right) / 2 - (buttonRect.left + buttonRect.right) / 2),
    ).toBeLessThan(1);
    expect(button.querySelectorAll("[aria-hidden]")).toHaveLength(1);
    expect(button.querySelector("svg")?.getBoundingClientRect().width).toBe(10);
    const startDivider = getComputedStyle(button, "::before");
    const endDivider = getComputedStyle(button, "::after");
    expect([startDivider.height, endDivider.height]).toEqual(["1px", "1px"]);
    const dividerWidths = [startDivider, endDivider].map((divider) => parseFloat(divider.width));
    expect(Math.min(...dividerWidths)).toBeGreaterThan(0);
    expect(Math.max(...dividerWidths) - Math.min(...dividerWidths)).toBeLessThan(1);
    const style = getComputedStyle(button);
    expect(style.fontSize).toBe("11px");
    expect(style.fontWeight).toBe("400");
    expect(style.whiteSpace).toBe("nowrap");
    expect(style.cursor).toBe("pointer");
    const restingColor = style.color;
    const restingShadow = style.boxShadow;
    await userEvent.hover(showMore);
    const hoverColor = getComputedStyle(button).color;
    expect(hoverColor).not.toBe(restingColor);
    await userEvent.unhover(showMore);
    expect(getComputedStyle(button).color).toBe(restingColor);
    await act(() => header(screen.container, "Beta").focus());
    await userEvent.tab({ shift: true });
    await expect.element(showMore).toHaveFocus();
    expect(button.matches(":focus-visible")).toBe(true);
    const focusedStyle = getComputedStyle(button);
    expect(focusedStyle.color).toBe(hoverColor);
    expect(focusedStyle.boxShadow).not.toBe(restingShadow);
    await options();
    await expect.element(page.getByText("5 of 12 shown", { exact: true })).toBeVisible();
    await userEvent.click(page.getByRole("menuitemradio", { name: "10", exact: true }));
    await expect.element(page.getByRole("menu")).toBeVisible();
    const choices = document.querySelector('[aria-label="Default visible threads"]')!;
    expect(choices.textContent?.replace(/\s/g, "")).toBe("5·10·15");
    const selected = choices.querySelector('[aria-checked="true"]')!;
    const unselected = choices.querySelector('[aria-checked="false"]')!;
    expect(getComputedStyle(selected).backgroundColor).not.toBe(
      getComputedStyle(unselected).backgroundColor,
    );
    expect(selected.getBoundingClientRect().width).toBeGreaterThanOrEqual(24);
    await expect.element(page.getByText("10 of 12 shown", { exact: true })).toBeVisible();
    expect(alpha.querySelectorAll("[data-menu-member]")).toHaveLength(10);
    expect(button.textContent).toBe("Show 2 more");
    expect(button.getAttribute("aria-label")).toBe("Show 2 more threads in Alpha");
    await userEvent.click(page.getByRole("menuitemradio", { name: "15", exact: true }));
    await expect.element(page.getByRole("menu")).toBeVisible();
    await expect.element(page.getByText("12 of 12 shown", { exact: true })).toBeVisible();
    expect(alpha.querySelector("[data-workstream-window-footer]")).toBeNull();
    await userEvent.click(page.getByRole("menuitemradio", { name: "5", exact: true }));
    await userEvent.keyboard("{Escape}");
    await userEvent.click(showMore);
    expect(alpha.querySelectorAll("[data-menu-member]")).toHaveLength(12);
    await expect
      .element(page.getByRole("button", { name: "Show only 5 threads in Alpha", exact: true }))
      .toHaveTextContent("Show fewer");
    await options();
    await expect.element(page.getByText("12 of 12 shown", { exact: true })).toBeVisible();
    await userEvent.keyboard("{Escape}");
    await userEvent.click(
      page.getByRole("button", { name: "Show only 5 threads in Alpha", exact: true }),
    );
    expect(alpha.querySelectorAll("[data-menu-member]")).toHaveLength(5);
    expect(
      section(screen.container, null).querySelector("[data-workstream-window-footer]"),
    ).toBeNull();
  });

  it("starts a thread for the Workstream that was right-clicked", async () => {
    const startThread = vi.fn();
    using setup = fixture(undefined, 0, ({ id }) => ({ run: () => startThread(id) }));
    await using _screen = await render(setup.ui);
    await page
      .getByRole("button", { name: "Collapse Beta", exact: true })
      .click({ button: "right" });
    await page.getByRole("menuitem", { name: "New thread", exact: true }).click();
    expect(startThread).toHaveBeenCalledExactlyOnceWith("menu-Beta");
  });

  it("opens Workstream actions by right-click and uses the thread snooze presets", async () => {
    using setup = fixture();
    await using screen = await render(setup.ui);
    const header = section(screen.container, "Alpha").querySelector("[data-workstream-header]")!;
    await act(() => {
      header.dispatchEvent(new MouseEvent("contextmenu", { bubbles: true, cancelable: true }));
    });
    await expect.element(page.getByRole("menuitem", { name: "Rename", exact: true })).toBeVisible();
    await expect.element(page.getByRole("menuitem", { name: "Change color" })).toBeVisible();
    expect(document.body.textContent).not.toContain("Use global status filter");
    await userEvent.click(page.getByRole("menuitem", { name: "Snooze", exact: true }));
    await userEvent.click(page.getByRole("menuitem", { name: /^In 1 hour/ }));
    expect(setup.controller.setSnoozedUntil).toHaveBeenCalledWith("menu-Alpha", expect.any(String));
    await expect
      .element(page.getByRole("button", { name: "Collapse Alpha" }))
      .toHaveAttribute("aria-expanded", "true");
  });

  it("moves a group down and back up through the menu", async () => {
    using setup = fixture();
    await using screen = await render(setup.ui);
    await options();
    await userEvent.click(page.getByRole("menuitem", { name: "Move down", exact: true }));
    expect(order(screen.container)).toEqual([null, "menu-Beta", "menu-Alpha", "menu-Gamma"]);
    await options();
    await userEvent.click(page.getByRole("menuitem", { name: "Move up", exact: true }));
    expect(order(screen.container)).toEqual([null, "menu-Alpha", "menu-Beta", "menu-Gamma"]);
    expect(setup.controller.reorder).toHaveBeenNthCalledWith(1, "menu-Alpha", {
      beforeId: "menu-Beta",
      afterId: "menu-Gamma",
    });
    expect(setup.controller.reorder).toHaveBeenNthCalledWith(2, "menu-Alpha", {
      beforeId: null,
      afterId: "menu-Beta",
    });
  });

  it("retains an inline rename draft through controller rollback and saves a retry", async () => {
    const request = deferredDelete();
    using setup = fixture(request.put);
    await using screen = await render(setup.ui);
    await userEvent.dblClick(page.getByText("Alpha", { exact: true }));
    const input = page.getByRole("textbox", { name: "Workstream name", exact: true });
    await expect.element(input).toHaveFocus();
    expect(screen.container.querySelector("input")?.closest("button")).toBeNull();
    await userEvent.fill(input, "Retained draft");
    await userEvent.keyboard("{Enter}");
    expect(setup.registry.get(setup.controller.state).workstreams[0]!.name).toBe("Retained draft");
    await userEvent.keyboard("{Enter}{Escape}");
    expect(request.put).toHaveBeenCalledTimes(1);
    await act(() => request.reject(new Error("Rename transport failed")));
    await expect.element(input).toHaveValue("Retained draft");
    await expect.element(input).toHaveFocus();
    expect(setup.registry.get(setup.controller.state).workstreams[0]!.name).toBe("Alpha");
    request.put.mockImplementationOnce(async (_connection, workstream) => ({
      outcome: "applied",
      workstream,
      sequence: 1,
    }));
    await userEvent.fill(input, "Saved retry");
    await userEvent.keyboard("{Enter}");
    await expect.element(input).not.toBeInTheDocument();
    await expect
      .element(page.getByRole("button", { name: "Collapse Saved retry", exact: true }))
      .toHaveFocus();
    expect(setup.registry.get(setup.controller.state).workstreams[0]!.name).toBe("Saved retry");
    expect(request.put).toHaveBeenCalledTimes(2);
  });

  it.each(["blur", "Enter"])(
    "preserves destination focus after a pending %s rename succeeds",
    async (trigger) => {
      const request = deferredDelete();
      using setup = fixture(request.put);
      await using _screen = await render(
        <>
          {setup.ui}
          <input aria-label="Search threads" />
        </>,
      );
      await userEvent.dblClick(page.getByText("Alpha", { exact: true }));
      const input = page.getByRole("textbox", { name: "Workstream name", exact: true });
      await userEvent.fill(input, "Saved name");
      if (trigger === "Enter") await userEvent.keyboard("{Enter}");
      const search = page.getByRole("textbox", { name: "Search threads", exact: true });
      await userEvent.click(search);
      await userEvent.keyboard("keep typing");
      await act(() => request.resolve());
      await expect.element(input).not.toBeInTheDocument();
      await expect.element(search).toHaveFocus();
      await expect.element(search).toHaveValue("keep typing");
      expect(setup.registry.get(setup.controller.state).workstreams[0]!.name).toBe("Saved name");
      expect(request.put).toHaveBeenCalledTimes(1);
    },
  );

  it("shows lifecycle radios in model order and closes the current choice without writing", async () => {
    using setup = fixture();
    await using _screen = await render(setup.ui);
    await options();
    await userEvent.click(
      page.getByRole("menuitem", { name: "Workstream state Active", exact: true }),
    );
    await expect
      .element(page.getByRole("menuitemradio", { name: "Active", exact: true }))
      .toHaveAttribute("aria-checked", "true");
    expect(
      [...document.querySelectorAll('[aria-label="Workstream state"] [role="menuitemradio"]')].map(
        (element) => element.textContent,
      ),
    ).toEqual(["Planned", "Active", "Paused", "Completed", "Deferred", "Abandoned"]);
    await userEvent.click(page.getByRole("menuitemradio", { name: "Active", exact: true }));
    await expect.element(page.getByRole("menu")).not.toBeInTheDocument();
    expect(setup.controller.setLifecycle).not.toHaveBeenCalled();
    await options();
    await userEvent.click(
      page.getByRole("menuitem", { name: "Workstream state Active", exact: true }),
    );
    await userEvent.click(page.getByRole("menuitemradio", { name: "Paused", exact: true }));
    expect(setup.controller.setLifecycle).toHaveBeenCalledWith("menu-Alpha", "paused");
    await options();
    await expect
      .element(page.getByRole("menuitem", { name: "Workstream state Paused", exact: true }))
      .toBeVisible();
  });

  it("focuses Cancel and cancels deletion through Cancel, Escape and the backdrop", async () => {
    using setup = fixture();
    await using _screen = await render(setup.ui);
    await deletion();
    await expect.element(page.getByRole("alertdialog")).toHaveAccessibleName("Delete “Alpha”?");
    await expect
      .element(page.getByRole("alertdialog"))
      .toMatchTextContent(
        "Threads move to Unassigned. No threads are deleted. This cannot be undone.",
      );
    await expect.element(page.getByRole("button", { name: "Cancel", exact: true })).toHaveFocus();
    await userEvent.click(page.getByRole("button", { name: "Cancel", exact: true }));
    await expect.element(page.getByRole("alertdialog")).not.toBeInTheDocument();
    await expect
      .element(page.getByRole("button", { name: "Collapse Alpha", exact: true }))
      .toHaveFocus();
    await deletion();
    await userEvent.keyboard("{Escape}");
    await expect.element(page.getByRole("alertdialog")).not.toBeInTheDocument();
    await expect
      .element(page.getByRole("button", { name: "Collapse Alpha", exact: true }))
      .toHaveFocus();
    await deletion();
    const viewport = document.querySelector('[data-slot="alert-dialog-viewport"]')!;
    await act(() => {
      viewport.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true }));
    });
    await expect.element(page.getByRole("alertdialog")).not.toBeInTheDocument();
    await expect
      .element(page.getByRole("button", { name: "Collapse Alpha", exact: true }))
      .toHaveFocus();
    expect(setup.controller.remove).not.toHaveBeenCalled();
  });

  it("retains confirmation through optimistic removal and rollback after a rejected delete", async () => {
    const request = deferredDelete();
    using setup = fixture(request.put);
    await using screen = await render(setup.ui);
    await deletion();
    await userEvent.click(page.getByRole("button", { name: "Delete Workstream", exact: true }));
    expect(setup.registry.get(setup.controller.state).workstreams.map((entry) => entry.id)).toEqual(
      ["menu-Beta", "menu-Gamma"],
    );
    expect(order(screen.container)).toEqual([null, "menu-Beta", "menu-Gamma"]);
    await expect
      .element(page.getByRole("button", { name: "Collapse Alpha", exact: true }))
      .not.toBeInTheDocument();
    await expect
      .element(page.getByRole("button", { name: "Deleting…", exact: true }))
      .toBeDisabled();
    await expect.element(page.getByRole("button", { name: "Cancel", exact: true })).toBeDisabled();
    await userEvent.keyboard("{Escape}");
    await act(() => {
      document
        .querySelector('[data-slot="alert-dialog-viewport"]')!
        .dispatchEvent(new PointerEvent("pointerdown", { bubbles: true }));
      document
        .querySelector<HTMLButtonElement>('[data-slot="alert-dialog-popup"] button:last-child')!
        .click();
    });
    await expect.element(page.getByRole("alertdialog")).toBeVisible();
    expect(request.put).toHaveBeenCalledTimes(1);
    await act(() => request.reject(new Error("Delete transport failed")));
    await expect.element(page.getByRole("alertdialog")).toBeVisible();
    await expect.element(page.getByRole("alertdialog")).toHaveAccessibleName("Delete “Alpha”?");
    expect(order(screen.container)).toEqual([null, "menu-Alpha", "menu-Beta", "menu-Gamma"]);
    await expect.element(page.getByRole("button", { name: "Cancel", exact: true })).toBeEnabled();
    await expect.element(page.getByRole("button", { name: "Cancel", exact: true })).toHaveFocus();
    await expect
      .element(page.getByRole("button", { name: "Delete Workstream", exact: true }))
      .toBeEnabled();
    await expect
      .element(
        page.getByText("Couldn't confirm that change. Check the Workstream and try again.", {
          exact: true,
        }),
      )
      .toBeVisible();
    await userEvent.click(page.getByRole("button", { name: "Cancel", exact: true }));
    await expect
      .element(page.getByRole("button", { name: "Collapse Alpha", exact: true }))
      .toHaveFocus();
    await deletion();
    request.put.mockImplementationOnce(async (_connection, workstream) => ({
      outcome: "applied",
      workstream,
      sequence: 1,
    }));
    await userEvent.click(page.getByRole("button", { name: "Delete Workstream", exact: true }));
    await expect.element(page.getByRole("alertdialog")).not.toBeInTheDocument();
    await expect
      .element(page.getByRole("button", { name: "Collapse Beta", exact: true }))
      .toHaveFocus();
    expect(request.put).toHaveBeenCalledTimes(2);
  });

  it("waits for delete confirmation after optimistic removal, then focuses the next header or Unassigned", async () => {
    const request = deferredDelete();
    using setup = fixture(request.put);
    await using screen = await render(setup.ui);
    await deletion();
    expect(request.put).not.toHaveBeenCalled();
    await userEvent.click(page.getByRole("button", { name: "Delete Workstream", exact: true }));
    expect(order(screen.container)).toEqual([null, "menu-Beta", "menu-Gamma"]);
    await expect.element(page.getByRole("alertdialog")).toBeVisible();
    await expect
      .element(page.getByRole("button", { name: "Deleting…", exact: true }))
      .toBeDisabled();
    await expect.element(page.getByRole("button", { name: "Cancel", exact: true })).toBeDisabled();
    await act(() => request.resolve());
    await expect.element(page.getByRole("alertdialog")).not.toBeInTheDocument();
    await expect
      .element(page.getByRole("button", { name: "Collapse Beta", exact: true }))
      .toHaveFocus();
    expect(order(screen.container)).toEqual([null, "menu-Beta", "menu-Gamma"]);
    await deletion("Gamma");
    await userEvent.click(page.getByRole("button", { name: "Delete Workstream", exact: true }));
    await expect
      .element(page.getByRole("button", { name: "Collapse Unassigned", exact: true }))
      .toHaveFocus();
    expect(request.put).toHaveBeenCalledTimes(2);
    expect(request.put.mock.calls.map(([, workstream]) => workstream.id)).toEqual([
      "menu-Alpha",
      "menu-Gamma",
    ]);
    expect(request.put.mock.calls.every(([, workstream]) => workstream.deletedAt !== null)).toBe(
      true,
    );
  });

  it("unmounts a remotely removed group's confirmation without sending a delete", async () => {
    using setup = fixture();
    await using _screen = await render(setup.ui);
    await deletion();
    await act(() =>
      setup.registry.set(setup.state, { ...setup.initial, workstreams: groups.slice(1) }),
    );
    await expect.element(page.getByRole("alertdialog")).not.toBeInTheDocument();
    expect(setup.controller.remove).not.toHaveBeenCalled();
  });

  it("uses the header midpoint to move down even when the target group is expanded", async () => {
    using setup = fixture();
    await using screen = await render(setup.ui);
    const transfer = await startDrag(screen.container, "Alpha");
    expect(getComputedStyle(section(screen.container, "Alpha")).opacity).toBe("0.5");
    const beta = section(screen.container, "Beta");
    const rect = beta.querySelector("[data-workstream-header]")!.getBoundingClientRect();
    await act(() => {
      drag(beta, "dragover", transfer, rect.top + rect.height * 0.3);
    });
    expect(screen.container.querySelectorAll("[data-workstream-insertion]")).toHaveLength(0);
    const afterY = rect.top + rect.height * 0.8;
    expect(afterY).toBeLessThan(
      beta.getBoundingClientRect().top + beta.getBoundingClientRect().height / 2,
    );
    await act(() => {
      drag(beta, "dragover", transfer, afterY);
    });
    const lines = screen.container.querySelectorAll("[data-workstream-insertion]");
    expect(lines).toHaveLength(1);
    expect(lines[0]!.parentElement).toBe(section(screen.container, "Gamma").parentElement);
    expect(lines[0]!.getBoundingClientRect().width).toBe(
      section(screen.container, "Gamma").parentElement!.getBoundingClientRect().width,
    );
    await act(() => {
      drag(beta, "drop", transfer, afterY);
    });
    expect(setup.controller.reorder).toHaveBeenCalledWith("menu-Alpha", {
      beforeId: "menu-Beta",
      afterId: "menu-Gamma",
    });
    expect(order(screen.container)).toEqual([null, "menu-Beta", "menu-Alpha", "menu-Gamma"]);
    expect(screen.container.querySelectorAll("[data-workstream-insertion]")).toHaveLength(0);
    expect(getComputedStyle(section(screen.container, "Alpha")).opacity).toBe("1");
  });

  it("moves a lower workstream above the previous header and clears its indicator", async () => {
    using setup = fixture();
    await using screen = await render(setup.ui);
    const transfer = await startDrag(screen.container, "Beta");
    const alpha = section(screen.container, "Alpha");
    const rect = alpha.querySelector("[data-workstream-header]")!.getBoundingClientRect();
    const beforeY = rect.top + rect.height * 0.2;
    await act(() => drag(alpha, "dragover", transfer, beforeY));
    const lines = screen.container.querySelectorAll("[data-workstream-insertion]");
    expect(lines).toHaveLength(1);
    expect(lines[0]!.parentElement).toBe(alpha.parentElement);
    await act(() => drag(alpha, "drop", transfer, beforeY));
    expect(setup.controller.reorder).toHaveBeenCalledExactlyOnceWith("menu-Beta", {
      beforeId: null,
      afterId: "menu-Alpha",
    });
    expect(order(screen.container)).toEqual([null, "menu-Beta", "menu-Alpha", "menu-Gamma"]);
    expect(screen.container.querySelectorAll("[data-workstream-insertion]")).toHaveLength(0);
  });

  it("scrolls upward and finds an offscreen Workstream beneath a stationary drag", async () => {
    using setup = fixture();
    using _timers = fakeTimers();
    const viewport = document.createElement("div");
    viewport.dataset.slot = "scroll-area-viewport";
    viewport.style.cssText =
      "position:fixed;top:60px;left:8px;width:360px;height:220px;overflow:auto";
    await using _screen = await render(setup.ui, { container: viewport });
    const bounds = viewport.getBoundingClientRect();
    const x = bounds.left + bounds.width / 2;
    const y = bounds.top + 8;
    const alpha = section(viewport, "Alpha");
    const alphaHeader = alpha.querySelector("[data-workstream-header]")!;
    const alphaBounds = alphaHeader.getBoundingClientRect();
    const targetScrollTop = alphaBounds.top + alphaBounds.height / 5 - y;
    viewport.scrollTop = targetScrollTop + 100;
    expect(alphaHeader.getBoundingClientRect().bottom).toBeLessThan(bounds.top);
    expect(header(viewport, "Gamma").getBoundingClientRect().top).toBeLessThan(bounds.bottom);

    const transfer = await startDrag(viewport, "Gamma");
    await act(() => {
      drag(document.elementFromPoint(x, y)!, "dragover", transfer, y);
      for (let frame = 0; frame < 60 && viewport.scrollTop > targetScrollTop; frame++) {
        vi.advanceTimersToNextFrame();
      }
    });
    expect(viewport.scrollTop).toBeLessThanOrEqual(targetScrollTop);
    expect(viewport.querySelector("[data-workstream-insertion]")?.parentElement).toBe(
      alpha.parentElement,
    );

    await act(() => drag(document.elementFromPoint(x, y)!, "drop", transfer, y));
    expect(order(viewport)).toEqual([null, "menu-Gamma", "menu-Alpha", "menu-Beta"]);
    const releasedAt = viewport.scrollTop;
    await act(() => vi.advanceTimersByTime(200));
    expect(viewport.scrollTop).toBe(releasedAt);
  });

  it("keeps Unassigned fixed and supports dropping after the last named Workstream", async () => {
    using setup = fixture();
    await using screen = await render(setup.ui);
    const transfer = await startDrag(screen.container, "Alpha");
    const unassigned = section(screen.container, null);
    const headerY = unassigned.querySelector("button")!.getBoundingClientRect().top + 2;
    await act(() => drag(unassigned, "dragover", transfer, headerY));
    expect(screen.container.querySelector("[data-workstream-insertion]")).toBeNull();

    const gamma = section(screen.container, "Gamma");
    const rect = gamma.querySelector("[data-workstream-header]")!.getBoundingClientRect();
    const afterY = rect.top + rect.height * 0.8;
    await act(() => drag(gamma, "dragover", transfer, afterY));
    const line = screen.container.querySelector("[data-workstream-insertion]")!;
    expect(line.getBoundingClientRect().top).toBeGreaterThanOrEqual(
      gamma.getBoundingClientRect().bottom,
    );
    await act(() => drag(unassigned, "dragover", transfer, headerY));
    expect(screen.container.querySelector("[data-workstream-insertion]")).toBeNull();
    await act(() => drag(gamma, "dragover", transfer, afterY));
    await act(() => drag(header(screen.container, "Alpha"), "dragend", transfer));
    expect(screen.container.querySelector("[data-workstream-insertion]")).toBeNull();
    expect(getComputedStyle(section(screen.container, "Alpha")).opacity).toBe("1");
    expect(setup.controller.reorder).not.toHaveBeenCalled();

    const again = await startDrag(screen.container, "Alpha");
    await act(() => drag(gamma, "drop", again, afterY));
    expect(setup.controller.reorder).toHaveBeenCalledExactlyOnceWith("menu-Alpha", {
      beforeId: "menu-Gamma",
      afterId: null,
    });
    expect(order(screen.container)).toEqual([null, "menu-Beta", "menu-Gamma", "menu-Alpha"]);
  });

  it("rejects a header drop when the controller becomes read-only during the drag", async () => {
    using setup = fixture();
    await using screen = await render(setup.ui);
    const transfer = await startDrag(screen.container, "Gamma");
    const alpha = section(screen.container, "Alpha");
    const top = alpha.getBoundingClientRect().top + 1;
    await act(() => {
      drag(alpha, "dragover", transfer, top);
    });
    expect(screen.container.querySelectorAll("[data-workstream-insertion]")).toHaveLength(1);
    await act(() => setup.registry.set(setup.state, { ...setup.initial, canCreate: false }));
    await act(() => {
      drag(alpha, "drop", transfer, top);
    });
    expect(screen.container.querySelectorAll("[data-workstream-insertion]")).toHaveLength(0);
    expect(setup.controller.reorder).not.toHaveBeenCalled();
    expect(order(screen.container)).toEqual([null, "menu-Alpha", "menu-Beta", "menu-Gamma"]);
    await options();
    await expect
      .element(page.getByRole("menuitem", { name: "Delete Workstream…", exact: true }))
      .toHaveAttribute("aria-disabled", "true");
  });

  it("never starts header drag from a filter and keeps header clicks working", async () => {
    using setup = fixture();
    await using screen = await render(setup.ui);
    const button = section(screen.container, "Alpha").querySelector(
      'button[aria-label^="Working:"]',
    )!;
    const transfer = new DataTransfer();
    await act(() => {
      expect(drag(button, "dragstart", transfer).defaultPrevented).toBe(true);
    });
    expect(transfer.getData(dragType)).toBe("");
    expect(getComputedStyle(section(screen.container, "Alpha")).opacity).toBe("1");
    await userEvent.click(page.getByRole("button", { name: "Collapse Alpha", exact: true }));
    await expect
      .element(page.getByRole("button", { name: "Expand Alpha", exact: true }))
      .toBeVisible();
    expect(setup.controller.reorder).not.toHaveBeenCalled();
  });
});
