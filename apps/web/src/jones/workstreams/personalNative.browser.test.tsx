import "~/index.css";
import { RegistryContext } from "@effect/atom-react";
import { Atom, AtomRegistry } from "effect/reactivity";
import { describe, expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import {
  personalThreadKey,
  type PersonalWorkstreamsController,
  type PersonalWorkstreamsState,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { act, render } from "../../../test/render";
import { WorkstreamPicker } from "./WorkstreamPicker";
import {
  WorkstreamNativeSidebar,
  type NativeThreadExternalDrop,
  type NativeThreadAssignmentMenu,
} from "../../components/workstreams/WorkstreamNativeSidebar";
import { WorkstreamCreateForm } from "../../components/workstreams/WorkstreamSidebarSection";
import { dropPinnedThreadOnWorkstream } from "./sidebarThreadDrop";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
const env = EnvironmentId.make("synthetic-env");
const thread = {
  environmentId: env,
  id: ThreadId.make("synthetic-thread"),
  title: "Synthetic thread",
  pinnedAt: "2026-11-12T12:00:00.000Z",
} as EnvironmentThreadShell;
const workstream = {
  id: "synthetic-work",
  name: "Alpha",
  lifecycle: "active" as const,
  color: null,
  orderKey: "n",
  createdAt: "2026-11-12T12:00:00.000Z",
  updatedAt: "2026-11-12T12:00:00.000Z",
};
function fixture() {
  const registry = AtomRegistry.make();
  const initial: PersonalWorkstreamsState = {
    workstreams: [workstream],
    environments: [{ environmentId: env, status: "ready", confirmed: true, workstreamCount: 1 }],
    assignmentsByThreadKey: new Map(),
    pendingThreadKeys: new Set(),
    canCreate: true,
  };
  const state = Atom.make(initial);
  const controller: PersonalWorkstreamsController = {
    state,
    canAssign: () => true,
    create: vi.fn(async () => ({ ok: true as const, value: workstream })),
    rename: vi.fn(),
    setColor: vi.fn(),
    setLifecycle: vi.fn(),
    reorder: vi.fn(),
    remove: vi.fn(),
    assign: vi.fn(async () => ({ ok: true as const, value: undefined })),
    createAndAssign: vi.fn(async () => ({ ok: true as const, value: workstream })),
  };
  return {
    registry,
    initial,
    controller,
    state,
    [Symbol.dispose]() {
      registry.dispose();
      localStorage.removeItem("jones:workstreams:collapsed:synthetic-work");
      localStorage.removeItem("jones:workstreams:unassigned-collapsed");
    },
  };
}
describe("personal Workstreams native presentation", () => {
  it("collapses Unassigned independently and restores the choice after remount", async () => {
    using setup = fixture();
    const { registry, controller, initial, state } = setup;
    const assigned = {
      ...thread,
      id: ThreadId.make("assigned"),
      title: "Assigned conversation",
      pinnedAt: null,
    };
    const populated = {
      ...initial,
      assignmentsByThreadKey: new Map([
        [
          personalThreadKey(assigned),
          { threadId: assigned.id, workstreamId: workstream.id, revision: 1, updatedAt: null },
        ],
      ]),
    };
    registry.set(state, populated);
    const sidebar = () => (
      <RegistryContext.Provider value={registry}>
        <WorkstreamNativeSidebar
          controller={controller}
          threads={[assigned, thread]}
          renderThread={(entry) => <li>{entry.title}</li>}
          canOperate={() => true}
          canReorder={() => false}
          reorder={async () => {}}
        />
      </RegistryContext.Provider>
    );
    await using screen = await render(sidebar());
    await userEvent.click(page.getByRole("button", { name: "Collapse Unassigned", exact: true }));
    await expect.element(page.getByText(thread.title, { exact: true })).not.toBeInTheDocument();
    await expect.element(page.getByText(assigned.title, { exact: true })).toBeVisible();
    await userEvent.click(page.getByRole("button", { name: "Collapse Alpha", exact: true }));
    await expect.element(page.getByText(assigned.title, { exact: true })).not.toBeInTheDocument();
    await screen.rerender(<div />);
    registry.set(state, populated);
    await screen.rerender(sidebar());
    await expect
      .element(page.getByRole("button", { name: "Expand Unassigned", exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByRole("button", { name: "Expand Alpha", exact: true }))
      .toBeVisible();
    await userEvent.click(page.getByRole("button", { name: "Expand Unassigned", exact: true }));
    await expect.element(page.getByText(thread.title, { exact: true })).toBeVisible();
    await expect.element(page.getByText(assigned.title, { exact: true })).not.toBeInTheDocument();
  });
  it("keeps counts and status readable while closed, including pinned and filtered members", async () => {
    using setup = fixture();
    const { registry, controller, initial, state } = setup;
    const running = { ...thread, runtime: { status: "running" } } as EnvironmentThreadShell;
    const waiting = {
      ...thread,
      id: ThreadId.make("waiting"),
      hasPendingUserInput: true,
      runtime: null,
    } as EnvironmentThreadShell;
    const failed = {
      ...thread,
      id: ThreadId.make("failed"),
      runtime: { status: "failed" },
    } as EnvironmentThreadShell;
    const members = [running, waiting, failed];
    registry.set(state, {
      ...initial,
      assignmentsByThreadKey: new Map(
        members.map((entry) => [
          personalThreadKey(entry),
          { threadId: entry.id, workstreamId: workstream.id, revision: 1, updatedAt: null },
        ]),
      ),
    });
    await using _screen = await render(
      <RegistryContext.Provider value={registry}>
        <WorkstreamNativeSidebar
          controller={controller}
          threads={[]}
          summaryThreads={[...members, running]}
          renderThread={(entry) => <li>{entry.title}</li>}
          canOperate={() => true}
          canReorder={() => false}
          reorder={async () => {}}
        />
      </RegistryContext.Provider>,
    );
    await userEvent.click(page.getByRole("button", { name: "Collapse Alpha", exact: true }));
    const header = page.getByRole("button", { name: "Expand Alpha", exact: true });
    await expect.element(header).toMatchTextContent(/3 threads/);
    await expect.element(header).toMatchTextContent(/1 running/);
    await expect.element(header).toMatchTextContent(/1 need attention/);
    await expect.element(header).toMatchTextContent(/1 failed/);
    const element = _screen.container.querySelector<HTMLElement>('[aria-label="Expand Alpha"]')!;
    expect(element.getBoundingClientRect().height).toBeGreaterThanOrEqual(64);
  });
  it("creates from the native form and uses the picker for assign, unassign and create-and-assign", async () => {
    using setup = fixture();
    const { registry, controller } = setup;
    const close = vi.fn();
    await using screen = await render(
      <RegistryContext.Provider value={registry}>
        <WorkstreamCreateForm
          controller={controller}
          open
          onClose={close}
          onPendingChange={() => {}}
        />
      </RegistryContext.Provider>,
    );
    await userEvent.fill(page.getByRole("textbox", { name: "Workstream name" }), "New group");
    await userEvent.click(page.getByRole("button", { name: "Create", exact: true }));
    expect(controller.create).toHaveBeenCalledWith({ name: "New group" });
    await screen.rerender(
      <RegistryContext.Provider value={registry}>
        <WorkstreamPicker controller={controller} thread={thread} onClose={close} />
      </RegistryContext.Provider>,
    );
    await userEvent.click(page.getByRole("button", { name: "Alpha", exact: true }));
    expect(controller.assign).toHaveBeenCalledWith(thread, "synthetic-work");
    await userEvent.click(page.getByRole("button", { name: "Unassigned", exact: true }));
    expect(controller.assign).toHaveBeenLastCalledWith(thread, null);
    await userEvent.fill(page.getByRole("textbox", { name: "Search Workstreams" }), "Second group");
    await userEvent.click(page.getByRole("button", { name: "Create “Second group” and move" }));
    expect(controller.createAndAssign).toHaveBeenCalledWith(thread, "Second group");
  });
  it("renders a pushed controller update without refresh and keeps a cached group visible while offline", async () => {
    using setup = fixture();
    const { registry, controller, initial, state } = setup;
    await using _screen = await render(
      <RegistryContext.Provider value={registry}>
        <WorkstreamNativeSidebar
          controller={controller}
          threads={[thread]}
          renderThread={(entry) => <li>{entry.title}</li>}
          canOperate={() => true}
          canReorder={() => false}
          reorder={async () => {}}
        />
      </RegistryContext.Provider>,
    );
    await expect
      .element(page.getByRole("button", { name: "Collapse Alpha", exact: true }))
      .toBeVisible();
    await act(() =>
      registry.set(state, {
        ...initial,
        workstreams: [{ ...workstream, name: "Changed on second client" }],
      }),
    );
    await expect
      .element(page.getByRole("button", { name: "Collapse Changed on second client", exact: true }))
      .toBeVisible();
    await act(() =>
      registry.set(state, {
        ...initial,
        environments: [
          { environmentId: env, status: "offline", confirmed: false, workstreamCount: 1 },
        ],
      }),
    );
    await expect
      .element(page.getByRole("button", { name: "Collapse Alpha", exact: true }))
      .toBeVisible();
  });
  it("ignores same-group header drops and cancels a drag whose assignment changed", async () => {
    using setup = fixture();
    const { registry, controller, initial, state } = setup;
    const native = { ...thread, pinnedAt: null };
    const assigned = {
      threadId: native.id,
      workstreamId: workstream.id,
      revision: 1,
      updatedAt: null,
    };
    registry.set(state, {
      ...initial,
      assignmentsByThreadKey: new Map([[personalThreadKey(native), assigned]]),
    });
    const reorder = vi.fn(async () => {});
    await using screen = await render(
      <RegistryContext.Provider value={registry}>
        <WorkstreamNativeSidebar
          controller={controller}
          threads={[native]}
          renderThread={(entry) => <li>{entry.title}</li>}
          canOperate={() => true}
          canReorder={() => true}
          reorder={reorder}
        />
      </RegistryContext.Provider>,
    );
    const point = (node: Element) => {
      const rect = node.getBoundingClientRect();
      return { clientX: rect.left + rect.width / 2, clientY: rect.top + rect.height / 2 };
    };
    const row = screen.container.querySelector("[data-personal-thread]")!;
    const header = screen.container.querySelector('[aria-label="Collapse Alpha"]')!;
    const pointer = (type: string, at: ReturnType<typeof point>, buttons = 1) =>
      new PointerEvent(type, {
        bubbles: true,
        cancelable: true,
        isPrimary: true,
        pointerId: 1,
        button: 0,
        buttons,
        ...at,
      });
    await act(() => {
      row.dispatchEvent(pointer("pointerdown", point(row)));
      document.dispatchEvent(pointer("pointermove", point(header)));
      document.dispatchEvent(pointer("pointerup", point(header), 0));
    });
    expect(controller.assign).not.toHaveBeenCalled();
    expect(reorder).not.toHaveBeenCalled();
    await act(() => {
      row.dispatchEvent(pointer("pointerdown", point(row)));
      document.dispatchEvent(pointer("pointermove", point(header)));
    });
    await act(() => registry.set(state, initial));
    await act(() => document.dispatchEvent(pointer("pointerup", point(header), 0)));
    expect(controller.assign).not.toHaveBeenCalled();
    expect(reorder).not.toHaveBeenCalled();
  });
  it("context unassign and external pinned drop confirm assignment before unpin; unpin failure keeps the assignment", async () => {
    using setup = fixture();
    const { registry, controller } = setup;
    const assignmentMenuRef: {
      current: ((entry: EnvironmentThreadShell) => NativeThreadAssignmentMenu) | null;
    } = { current: null };
    const externalDropRef: { current: NativeThreadExternalDrop | null } = { current: null };
    await using _screen = await render(
      <RegistryContext.Provider value={registry}>
        <WorkstreamNativeSidebar
          controller={controller}
          threads={[thread]}
          assignmentMenuRef={assignmentMenuRef}
          externalDropRef={externalDropRef}
          renderThread={(entry) => <li>{entry.title}</li>}
          canOperate={() => true}
          canReorder={() => false}
          reorder={async () => {}}
        />
      </RegistryContext.Provider>,
    );
    await act(() => assignmentMenuRef.current!(thread).onAction("assign-workstream:remove"));
    expect(controller.assign).toHaveBeenLastCalledWith(thread, null);
    const section = document.querySelector<HTMLElement>("[data-native-workstream]")!;
    const rect = section.getBoundingClientRect();
    const unpin = vi.fn(async () => {
      throw new Error("synthetic failure");
    });
    const onError = vi.fn();
    await act(async () => {
      expect(
        dropPinnedThreadOnWorkstream({
          bridge: externalDropRef.current,
          thread,
          point: { x: rect.left + 2, y: rect.top + 2 },
          unpin,
          onError,
        }),
      ).toBe(true);
      await Promise.resolve();
    });
    expect(controller.assign).toHaveBeenLastCalledWith(thread, "synthetic-work");
    expect(unpin).toHaveBeenCalledOnce();
    expect(onError).toHaveBeenCalledOnce();
  });
});
