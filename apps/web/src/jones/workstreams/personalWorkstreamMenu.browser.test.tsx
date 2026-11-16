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
import { personalGroupCollapsedKey } from "./personalSidebarOrder";

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

function fixture(put?: PersonalWorkstreamsTransport["put"]) {
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
            renderMembers={(id) => <div className="h-24">Members of {id ?? "Unassigned"}</div>}
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
      for (const entry of groups) localStorage.removeItem(personalGroupCollapsedKey(entry.id));
      localStorage.removeItem(personalGroupCollapsedKey(null));
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
  it("opens four menu sections without collapsing and preserves the rename and color forms", async () => {
    using setup = fixture();
    await using screen = await render(setup.ui);
    await options();
    await expect
      .element(page.getByRole("button", { name: "Collapse Alpha", exact: true }))
      .toBeVisible();
    expect(document.querySelectorAll('[data-slot="menu-separator"]')).toHaveLength(3);
    await expect
      .element(page.getByRole("menuitem", { name: "Move up", exact: true }))
      .toHaveAttribute("aria-disabled", "true");
    await expect
      .element(page.getByRole("menuitem", { name: "Lifecycle Active", exact: true }))
      .toBeVisible();
    await userEvent.click(page.getByRole("menuitem", { name: "Rename", exact: true }));
    await userEvent.fill(page.getByRole("textbox", { name: "Workstream name" }), "Renamed");
    await userEvent.click(page.getByRole("button", { name: "Save", exact: true }));
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
    expect(section(screen.container, "Alpha").textContent).not.toContain("Members of menu-Alpha");
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

  it("shows lifecycle radios in model order and closes the current choice without writing", async () => {
    using setup = fixture();
    await using _screen = await render(setup.ui);
    await options();
    await userEvent.click(page.getByRole("menuitem", { name: "Lifecycle Active", exact: true }));
    await expect
      .element(page.getByRole("menuitemradio", { name: "Active", exact: true }))
      .toHaveAttribute("aria-checked", "true");
    expect(
      [...document.querySelectorAll('[role="menuitemradio"]')].map(
        (element) => element.textContent,
      ),
    ).toEqual(["Planned", "Active", "Paused", "Completed", "Deferred", "Abandoned"]);
    await userEvent.click(page.getByRole("menuitemradio", { name: "Active", exact: true }));
    await expect.element(page.getByRole("menu")).not.toBeInTheDocument();
    expect(setup.controller.setLifecycle).not.toHaveBeenCalled();
    await options();
    await userEvent.click(page.getByRole("menuitem", { name: "Lifecycle Active", exact: true }));
    await userEvent.click(page.getByRole("menuitemradio", { name: "Paused", exact: true }));
    expect(setup.controller.setLifecycle).toHaveBeenCalledWith("menu-Alpha", "paused");
    await options();
    await expect
      .element(page.getByRole("menuitem", { name: "Lifecycle Paused", exact: true }))
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
    await deletion();
    await userEvent.keyboard("{Escape}");
    await expect.element(page.getByRole("alertdialog")).not.toBeInTheDocument();
    await deletion();
    const viewport = document.querySelector('[data-slot="alert-dialog-viewport"]')!;
    await act(() => {
      viewport.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true }));
    });
    await expect.element(page.getByRole("alertdialog")).not.toBeInTheDocument();
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

  it("uses the target section midpoint and excludes the source when moving after the next group", async () => {
    using setup = fixture();
    await using screen = await render(setup.ui);
    const transfer = await startDrag(screen.container, "Alpha");
    expect(getComputedStyle(section(screen.container, "Alpha")).opacity).toBe("0.5");
    const beta = section(screen.container, "Beta");
    const rect = beta.getBoundingClientRect();
    await act(() => {
      drag(beta, "dragover", transfer, rect.top + rect.height * 0.3);
    });
    expect(screen.container.querySelectorAll("[data-workstream-insertion]")).toHaveLength(0);
    const afterY = rect.top + rect.height * 0.8;
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
