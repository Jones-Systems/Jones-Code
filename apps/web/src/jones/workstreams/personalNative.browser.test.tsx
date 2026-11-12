import "~/index.css";
import { RegistryContext } from "@effect/atom-react";
import { Atom, AtomRegistry } from "effect/reactivity";
import { describe, expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import type {
  PersonalWorkstreamsController,
  PersonalWorkstreamsState,
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
  return { registry, initial, controller, state };
}
describe("personal Workstreams native presentation", () => {
  it("creates from the native form and uses the picker for assign, unassign and create-and-assign", async () => {
    const { registry, controller } = fixture();
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
    const { registry, controller, initial, state } = fixture();
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
    await expect.element(page.getByRole("button", { name: "Alpha", exact: true })).toBeVisible();
    await act(() =>
      registry.set(state, {
        ...initial,
        workstreams: [{ ...workstream, name: "Changed on second client" }],
      }),
    );
    await expect
      .element(page.getByRole("button", { name: "Changed on second client", exact: true }))
      .toBeVisible();
    await act(() =>
      registry.set(state, {
        ...initial,
        environments: [
          { environmentId: env, status: "offline", confirmed: false, workstreamCount: 1 },
        ],
      }),
    );
    await expect.element(page.getByRole("button", { name: "Alpha", exact: true })).toBeVisible();
  });
  it("context unassign and external pinned drop confirm assignment before unpin; unpin failure keeps the assignment", async () => {
    const { registry, controller } = fixture();
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
