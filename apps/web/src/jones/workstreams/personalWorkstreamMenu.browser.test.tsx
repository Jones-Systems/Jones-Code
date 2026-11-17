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
import type { EnvironmentId } from "@t3tools/contracts";
import { act, render } from "../../../test/render";
import { WorkstreamSidebarSection } from "../../components/workstreams/WorkstreamSidebarSection";
import { ToastProvider, toastManager } from "../../components/ui/toast";
import { personalGroupCollapsedKey, personalNamedGroupsHiddenKey } from "./personalSidebarOrder";
import {
  personalSharedStatusFilterKey,
  personalStatusFilterKey,
  readPersonalStatusFilter,
  readSharedPersonalStatusChoice,
} from "./personalStatusFilter";
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

function fixture(put?: PersonalWorkstreamsTransport["put"], memberCount = 0) {
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
    environments: [],
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
  await userEvent.click(page.getByRole("button", { name: `Options for ${name}`, exact: true }));
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
  it("centres a symmetric 12px menu hit area without overlapping counters or toggling the group", async () => {
    using setup = fixture(undefined, 14);
    await using screen = await render(setup.ui);
    const group = section(screen.container, "Alpha");
    const menu = group.querySelector<HTMLButtonElement>('[aria-label="Options for Alpha"]')!;
    const bounds = menu.getBoundingClientRect();
    const glyph = menu.querySelector("svg")!.getBoundingClientRect();
    expect(bounds.width).toBe(12);
    expect(bounds.height).toBe(12);
    expect(glyph.left + glyph.width / 2).toBeCloseTo(bounds.left + 6, 1);
    expect(glyph.top + glyph.height / 2).toBeCloseTo(bounds.top + 6, 1);
    expect(
      group.querySelector("[data-personal-status-pill]")!.getBoundingClientRect().right,
    ).toBeLessThanOrEqual(bounds.left);
    for (const offset of [-5, -3, 0, 3, 5]) {
      expect(
        document.elementFromPoint(bounds.left + 6 + offset, bounds.top + 6)?.closest("button"),
      ).toBe(menu);
      await userEvent.click(page.getByRole("button", { name: "Options for Alpha", exact: true }), {
        position: { x: 6 + offset, y: 6 },
      });
      await expect
        .element(page.getByRole("menuitem", { name: "Rename", exact: true }))
        .toBeVisible();
      expect(header(screen.container, "Alpha").getAttribute("aria-expanded")).toBe("true");
      await userEvent.keyboard("{Escape}");
    }
    expect(getComputedStyle(header(screen.container, null)).cursor).toBe("pointer");
    const draggable = group.querySelector<HTMLElement>("[data-workstream-header]")!;
    expect(draggable.draggable).toBe(true);
    expect(getComputedStyle(draggable).cursor).toBe("pointer");
    const title = group.querySelector<HTMLElement>(".line-clamp-2")!;
    expect(getComputedStyle(title).cursor).toBe("grab");
    expect(menu.querySelector("svg")!.getAttribute("viewBox")).toBe("0 0 24 24");
    expect(glyph.width).toBe(12);
    expect(glyph.height).toBe(12);
    for (const x of [bounds.left - 1, bounds.right + 1])
      expect(document.elementFromPoint(x, bounds.top + 6)?.closest("button")).not.toBe(menu);
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
    expect(draggable.draggable).toBe(false);
    expect(getComputedStyle(draggable).cursor).toBe("pointer");
    expect(getComputedStyle(title).cursor).toBe("pointer");
  });

  it("segments menu actions and display choices and supports inline rename and color editing", async () => {
    using setup = fixture();
    await using screen = await render(setup.ui);
    await options();
    await expect
      .element(page.getByRole("button", { name: "Collapse Alpha", exact: true }))
      .toBeVisible();
    expect(document.querySelectorAll('[data-slot="menu-separator"]')).toHaveLength(5);
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
    await options();
    await userEvent.click(page.getByRole("menuitem", { name: "Change color", exact: true }));
    await userEvent.fill(page.getByRole("textbox", { name: "Hex color" }), "#abcdef");
    await userEvent.click(page.getByRole("button", { name: "Save", exact: true }));
    expect(setup.controller.setColor).toHaveBeenCalledWith("menu-Alpha", "#ABCDEF");
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

  it("uses the Divider footer and updates the visible count while the window menu stays open", async () => {
    using setup = fixture(undefined, 12);
    await using screen = await render(setup.ui);
    const alpha = section(screen.container, "Alpha");
    expect(alpha.querySelectorAll("[data-menu-member]")).toHaveLength(5);
    const footer = alpha.querySelector("[data-workstream-window-footer]")!;
    const lines = footer.querySelectorAll("span[aria-hidden]");
    expect(lines).toHaveLength(2);
    const button = footer.querySelector("button")!;
    expect(button.textContent).toBe("Show all 12");
    const footerRect = footer.getBoundingClientRect();
    const buttonRect = button.getBoundingClientRect();
    expect(
      Math.abs((buttonRect.left + buttonRect.right) / 2 - (footerRect.left + footerRect.right) / 2),
    ).toBeLessThan(1);
    expect(lines[0]!.getBoundingClientRect().right).toBeLessThan(buttonRect.left);
    expect(lines[1]!.getBoundingClientRect().left).toBeGreaterThan(buttonRect.right);
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
    await userEvent.click(page.getByRole("menuitemradio", { name: "15", exact: true }));
    await expect.element(page.getByRole("menu")).toBeVisible();
    await expect.element(page.getByText("12 of 12 shown", { exact: true })).toBeVisible();
    expect(alpha.querySelector("[data-workstream-window-footer]")).toBeNull();
    await userEvent.click(page.getByRole("menuitemradio", { name: "5", exact: true }));
    await userEvent.keyboard("{Escape}");
    await userEvent.click(
      page.getByRole("button", { name: "Show all 12 threads in Alpha", exact: true }),
    );
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

  it("resets a local exclusion from the menu without collapsing or changing the full summary", async () => {
    using setup = fixture(undefined, 12);
    await using screen = await render(setup.ui);
    const alpha = section(screen.container, "Alpha");
    const counter = alpha.querySelector<HTMLButtonElement>('button[aria-label^="Working:"]')!;
    expect(alpha.querySelector("button button")).toBeNull();
    for (const at of [1000, 1250]) {
      await act(() => {
        const event = new MouseEvent("click", { bubbles: true, detail: 1 });
        Object.defineProperty(event, "timeStamp", { value: at });
        counter.dispatchEvent(event);
      });
    }
    expect(readPersonalStatusFilter("menu-Alpha")).toBe("hide-working");
    await expect
      .element(page.getByRole("button", { name: "Collapse Alpha", exact: true }))
      .toHaveAttribute("aria-expanded", "true");
    await expect.element(page.getByText("5 of 12 threads shown", { exact: true })).toBeVisible();
    expect(alpha.textContent).toContain(
      "12 threads, 3 working, 2 need attention, 6 done, 1 failed.",
    );
    await options();
    await userEvent.click(
      page.getByRole("menuitem", { name: "Use global status filter", exact: true }),
    );
    expect(readPersonalStatusFilter("menu-Alpha")).toBeNull();
    expect(counter.getAttribute("aria-pressed")).toBe("false");
    expect(counter.getAttribute("aria-label")).toContain("using global status filter");
    await expect
      .element(page.getByRole("button", { name: "Collapse Alpha", exact: true }))
      .toHaveAttribute("aria-expanded", "true");
    expect(alpha.querySelector("input")).toBeNull();
    expect(setup.controller.rename).not.toHaveBeenCalled();
    expect(setup.controller.reorder).not.toHaveBeenCalled();
  });

  it("sends one group to the global filter from its menu while the Workstreams pill selects a status", async () => {
    using setup = fixture(undefined, 12);
    await using screen = await render(setup.ui);
    const bulk = screen.container.querySelector<HTMLElement>("[data-personal-workstreams-header]")!;
    const working = (root: ParentNode) =>
      root.querySelector<HTMLButtonElement>('button[aria-label^="Working:"]')!;
    const alpha = working(section(screen.container, "Alpha"));
    const beta = working(section(screen.container, "Beta"));
    await userEvent.click(working(bulk));
    expect(readSharedPersonalStatusChoice()).toEqual({ filter: "working", rev: 1 });
    expect(alpha.getAttribute("aria-label")).toContain("selected");
    expect(beta.getAttribute("aria-label")).toContain("selected");
    await options();
    await userEvent.click(
      page.getByRole("menuitem", { name: "Use global status filter", exact: true }),
    );
    expect(readPersonalStatusFilter("menu-Alpha")).toBeNull();
    expect(alpha.getAttribute("aria-label")).toContain("using global status filter");
    expect(alpha.getAttribute("aria-pressed")).toBe("false");
    // The shared choice and the other group keep their status.
    expect(beta.getAttribute("aria-pressed")).toBe("true");
    expect(working(bulk).getAttribute("aria-pressed")).toBe("true");
    expect(readSharedPersonalStatusChoice()).toEqual({ filter: "working", rev: 1 });
    await expect
      .element(page.getByRole("button", { name: "Collapse Alpha", exact: true }))
      .toHaveAttribute("aria-expanded", "true");
  });

  it("moves a group down and back up through the menu", async () => {
    using setup = fixture();
    await using screen = await render(setup.ui);
    await options();
    await userEvent.click(page.getByRole("menuitem", { name: "Move down", exact: true }));
    expect(order(screen.container)).toEqual(["menu-Beta", "menu-Alpha", "menu-Gamma", null]);
    await options();
    await userEvent.click(page.getByRole("menuitem", { name: "Move up", exact: true }));
    expect(order(screen.container)).toEqual(["menu-Alpha", "menu-Beta", "menu-Gamma", null]);
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
      .element(page.getByRole("button", { name: "Options for Alpha", exact: true }))
      .toHaveFocus();
    await deletion();
    await userEvent.keyboard("{Escape}");
    await expect.element(page.getByRole("alertdialog")).not.toBeInTheDocument();
    await expect
      .element(page.getByRole("button", { name: "Options for Alpha", exact: true }))
      .toHaveFocus();
    await deletion();
    const viewport = document.querySelector('[data-slot="alert-dialog-viewport"]')!;
    await act(() => {
      viewport.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true }));
    });
    await expect.element(page.getByRole("alertdialog")).not.toBeInTheDocument();
    await expect
      .element(page.getByRole("button", { name: "Options for Alpha", exact: true }))
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
    expect(order(screen.container)).toEqual(["menu-Beta", "menu-Gamma", null]);
    await expect
      .element(page.getByRole("button", { name: "Options for Alpha", exact: true }))
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
    expect(order(screen.container)).toEqual(["menu-Alpha", "menu-Beta", "menu-Gamma", null]);
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
      .element(page.getByRole("button", { name: "Options for Alpha", exact: true }))
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
    expect(order(screen.container)).toEqual(["menu-Beta", "menu-Gamma", null]);
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
    expect(order(screen.container)).toEqual(["menu-Beta", "menu-Gamma", null]);
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
    expect(order(screen.container)).toEqual(["menu-Beta", "menu-Alpha", "menu-Gamma", null]);
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
    expect(order(screen.container)).toEqual(["menu-Beta", "menu-Alpha", "menu-Gamma", null]);
    expect(screen.container.querySelectorAll("[data-workstream-insertion]")).toHaveLength(0);
  });

  it("accepts the gap above Unassigned as the tail but rejects its body and clears canceled drags", async () => {
    using setup = fixture();
    await using screen = await render(setup.ui);
    const transfer = await startDrag(screen.container, "Alpha");
    const unassigned = section(screen.container, null);
    const bottom = unassigned.getBoundingClientRect().bottom - 1;
    await act(() => {
      drag(unassigned, "dragover", transfer, bottom);
    });
    expect(screen.container.querySelectorAll("[data-workstream-insertion]")).toHaveLength(0);
    const top = unassigned.getBoundingClientRect().top - 2;
    await act(() => {
      drag(unassigned.parentElement!, "dragover", transfer, top);
    });
    expect(screen.container.querySelectorAll("[data-workstream-insertion]")).toHaveLength(1);
    await act(() => {
      drag(header(screen.container, "Alpha"), "dragend", transfer);
    });
    expect(screen.container.querySelectorAll("[data-workstream-insertion]")).toHaveLength(0);
    expect(getComputedStyle(section(screen.container, "Alpha")).opacity).toBe("1");
    expect(setup.controller.reorder).not.toHaveBeenCalled();
    const again = await startDrag(screen.container, "Alpha");
    await act(() => {
      drag(unassigned.parentElement!, "drop", again, top);
    });
    expect(setup.controller.reorder).toHaveBeenCalledWith("menu-Alpha", {
      beforeId: "menu-Gamma",
      afterId: null,
    });
    expect(order(screen.container)).toEqual(["menu-Beta", "menu-Gamma", "menu-Alpha", null]);
    const noOp = await startDrag(screen.container, "Alpha");
    await act(() => {
      drag(unassigned, "dragover", noOp, unassigned.getBoundingClientRect().top + 10);
      drag(unassigned, "drop", noOp, unassigned.getBoundingClientRect().top + 10);
    });
    expect(screen.container.querySelectorAll("[data-workstream-insertion]")).toHaveLength(0);
    expect(setup.controller.reorder).toHaveBeenCalledTimes(1);
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
    expect(order(screen.container)).toEqual(["menu-Alpha", "menu-Beta", "menu-Gamma", null]);
    await options();
    await expect
      .element(page.getByRole("menuitem", { name: "Delete Workstream…", exact: true }))
      .toHaveAttribute("aria-disabled", "true");
  });

  it("never starts header drag from options and keeps header clicks working", async () => {
    using setup = fixture();
    await using screen = await render(setup.ui);
    const button = section(screen.container, "Alpha").querySelector(
      '[aria-label="Options for Alpha"]',
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
