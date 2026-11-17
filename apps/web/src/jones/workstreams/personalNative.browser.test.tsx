import "~/index.css";
import { useRef, useState } from "react";
import { RegistryContext } from "@effect/atom-react";
import { Atom, AtomRegistry } from "effect/reactivity";
import { describe, expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import {
  personalThreadKey,
  type PersonalWorkstreamsController,
  type PersonalWorkstreamsState,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { EnvironmentId, RunId, ThreadId, type ScopedThreadRef } from "@t3tools/contracts";
import { scopedThreadKey, scopeThreadRef } from "@t3tools/client-runtime/environment";
import { makeThreadFixture } from "../../test-fixtures";
import { summarizePersonalThreads } from "./personalPresentation";
import { act, render } from "../../../test/render";
import { WorkstreamPicker } from "./WorkstreamPicker";
import { PersonalSidebarGroupHeader } from "./PersonalSidebarGroupHeader";
import {
  WorkstreamNativeSidebar,
  type NativeThreadExternalDrop,
  type NativeThreadAssignmentMenu,
} from "../../components/workstreams/WorkstreamNativeSidebar";
import { WorkstreamCreateForm } from "../../components/workstreams/WorkstreamSidebarSection";
import { dropPinnedThreadOnWorkstream } from "./sidebarThreadDrop";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import {
  THREAD_CONTEXT_DROP_EVENT,
  threadContextDropTargetProps,
  useThreadContextDrop,
  useThreadContextDragGhost,
} from "../../components/chat/threadContextDrag";
import { ThreadContextDragGhost } from "../../components/chat/ThreadContextDragGhost";
import { useThreadSelectionStore } from "../../threadSelectionStore";
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
      localStorage.removeItem("jones:workstreams:named-hidden");
      localStorage.removeItem("jones:workstreams:shared-status-filter");
    },
  };
}

function ComposerReferenceTarget() {
  const target = useRef<HTMLFormElement | null>(null);
  const [references, setReferences] = useState<ReadonlyArray<ScopedThreadRef>>([]);
  const ghost = useThreadContextDragGhost();
  useThreadContextDrop(target, env, "synthetic-composer", (refs) => setReferences(refs));
  return (
    <form
      ref={target}
      aria-label="Reference composer"
      {...threadContextDropTargetProps()}
      style={{ height: 120 }}
    >
      Drop thread references here
      <output data-composer-references>{JSON.stringify(references)}</output>
      <output hidden data-composer-drag-ghost>
        {JSON.stringify(ghost)}
      </output>
    </form>
  );
}

const member = {
  ...thread,
  id: ThreadId.make("member"),
  title: "Workstream member",
  pinnedAt: null,
};
const unassigned = {
  ...thread,
  id: ThreadId.make("unassigned"),
  title: "Unassigned conversation",
  pinnedAt: null,
};

async function composerDragFixture(readOnly = false) {
  await page.viewport(1000, 800);
  const setup = fixture();
  setup.registry.set(setup.state, {
    ...setup.initial,
    assignmentsByThreadKey: new Map([
      [
        personalThreadKey(member),
        { threadId: member.id, workstreamId: workstream.id, revision: 1, updatedAt: null },
      ],
    ]),
  });
  setup.controller.canAssign = () => !readOnly;
  const reorder = vi.fn(async () => {});
  const placeThread = vi.fn(async () => true);
  const previousSelection = useThreadSelectionStore.getState();
  useThreadSelectionStore.getState().clearSelection();
  return {
    ...setup,
    reorder,
    placeThread,
    ui: (
      <RegistryContext.Provider value={setup.registry}>
        <div style={{ display: "flex", gap: 24, width: 664, alignItems: "flex-start" }}>
          <div style={{ width: 320, flexShrink: 0 }}>
            <WorkstreamNativeSidebar
              controller={setup.controller}
              threads={[member, unassigned]}
              renderThread={(entry) => <li style={{ height: 48 }}>{entry.title}</li>}
              canOperate={() => !readOnly}
              canReorder={() => !readOnly}
              reorder={reorder}
              placeThread={placeThread}
            />
          </div>
          <div style={{ width: 320 }}>
            <ComposerReferenceTarget />
            <div data-outside-composer style={{ height: 120 }}>
              Outside composer
            </div>
          </div>
        </div>
        <ThreadContextDragGhost />
      </RegistryContext.Provider>
    ),
    [Symbol.dispose]() {
      useThreadSelectionStore.setState({
        selectedThreadKeys: previousSelection.selectedThreadKeys,
        anchorThreadKey: previousSelection.anchorThreadKey,
      });
      setup[Symbol.dispose]();
    },
  };
}

const dragPoint = (node: Element, fraction = 0.5) => {
  const rect = node.getBoundingClientRect();
  return { x: rect.left + rect.width / 2, y: rect.top + rect.height * fraction };
};
function dragPointer(
  target: EventTarget,
  type: string,
  point: { x: number; y: number },
  buttons = 1,
) {
  target.dispatchEvent(
    new PointerEvent(type, {
      bubbles: true,
      cancelable: true,
      isPrimary: true,
      pointerId: 1,
      button: 0,
      buttons,
      clientX: point.x,
      clientY: point.y,
    }),
  );
}
const nativeRow = (container: HTMLElement, entry: EnvironmentThreadShell) =>
  [...container.querySelectorAll<HTMLElement>("[data-personal-thread]")].find(
    (row) => row.dataset.personalThread === personalThreadKey(entry),
  )!;
const composerReferences = (container: HTMLElement) =>
  JSON.parse(container.querySelector("[data-composer-references]")!.textContent!);

describe("personal thread composer reference drags", () => {
  it.each([
    { entry: unassigned, readOnly: false },
    { entry: member, readOnly: false },
    { entry: unassigned, readOnly: true },
    { entry: member, readOnly: true },
  ])(
    "drops $entry.title through the composer listener with readOnly=$readOnly",
    async ({ entry, readOnly }) => {
      using setup = await composerDragFixture(readOnly);
      await using screen = await render(setup.ui);
      const source = nativeRow(screen.container, entry);
      const composer = screen.container.querySelector<HTMLElement>("[data-thread-context-drop]")!;
      const at = dragPoint(composer);
      expect(document.elementFromPoint(at.x, at.y)?.closest("[data-thread-context-drop]")).toBe(
        composer,
      );
      await act(() => {
        dragPointer(source, "pointerdown", dragPoint(source));
        dragPointer(document, "pointermove", at);
      });
      expect(composer.dataset.threadContextOver).toBe("true");
      expect(document.querySelector("[data-personal-thread-insertion]")).toBeNull();
      const lifted = document.querySelector<HTMLElement>("[data-personal-thread-drag]");
      if (readOnly) expect(lifted).toBeNull();
      else expect(lifted?.hidden).toBe(true);
      expect(
        JSON.parse(composer.querySelector("[data-composer-drag-ghost]")!.textContent!),
      ).toEqual({
        ...at,
        title: entry.title,
        count: 1,
      });
      await act(() => dragPointer(document, "pointerup", at, 0));
      expect(composerReferences(screen.container)).toEqual([
        scopeThreadRef(entry.environmentId, entry.id),
      ]);
      expect(composer.hasAttribute("data-thread-context-over")).toBe(false);
      expect(document.querySelector("[data-personal-thread-drag]")).toBeNull();
      expect(source.style.opacity).toBe("");
      expect(composer.querySelector("[data-composer-drag-ghost]")!.textContent).toBe("null");
      await expect.element(page.getByText(entry.title, { exact: true })).toBeVisible();
      expect(setup.controller.assign).not.toHaveBeenCalled();
      expect(setup.reorder).not.toHaveBeenCalled();
      expect(setup.placeThread).not.toHaveBeenCalled();
    },
  );

  it.each([true, false])(
    "matches pinned selected-thread references when the dragged row is selected=%s",
    async (selected) => {
      using setup = await composerDragFixture();
      const other = scopeThreadRef(
        EnvironmentId.make("other-environment"),
        ThreadId.make("other-thread"),
      );
      const active = scopeThreadRef(member.environmentId, member.id);
      useThreadSelectionStore.setState({
        selectedThreadKeys: new Set([
          scopedThreadKey(other),
          ...(selected ? [scopedThreadKey(active)] : []),
        ]),
      });
      await using screen = await render(setup.ui);
      const source = nativeRow(screen.container, member);
      const at = dragPoint(screen.container.querySelector("[data-thread-context-drop]")!);
      await act(() => {
        dragPointer(source, "pointerdown", dragPoint(source));
        dragPointer(document, "pointermove", at);
      });
      if (selected) await expect.element(page.getByText("+1", { exact: true })).toBeVisible();
      await act(() => dragPointer(document, "pointerup", at, 0));
      expect(composerReferences(screen.container)).toEqual(selected ? [other, active] : [active]);
      expect(setup.placeThread).not.toHaveBeenCalled();
    },
  );

  it("restores the lifted card and exact sidebar placement after leaving the composer", async () => {
    using setup = await composerDragFixture();
    await using screen = await render(setup.ui);
    const source = nativeRow(screen.container, unassigned);
    const neighbor = nativeRow(screen.container, member);
    const composer = screen.container.querySelector<HTMLElement>("[data-thread-context-drop]")!;
    const before = dragPoint(neighbor, 0.2);
    await act(() => {
      dragPointer(source, "pointerdown", dragPoint(source));
      dragPointer(document, "pointermove", dragPoint(composer));
    });
    expect(composer.dataset.threadContextOver).toBe("true");
    await act(() => dragPointer(document, "pointermove", before));
    expect(composer.hasAttribute("data-thread-context-over")).toBe(false);
    expect(document.querySelector<HTMLElement>("[data-personal-thread-drag]")?.hidden).toBe(false);
    const insertion = document.querySelector<HTMLElement>("[data-personal-thread-insertion]");
    expect(insertion?.parentElement).toBe(neighbor);
    expect(insertion?.dataset.personalThreadInsertion).toBe("before");
    await act(() => dragPointer(document, "pointerup", before, 0));
    expect(setup.placeThread).toHaveBeenCalledExactlyOnceWith(
      unassigned,
      workstream.id,
      member,
      false,
    );
    expect(composerReferences(screen.container)).toEqual([]);
  });

  it("does not use the last sidebar insertion when released outside every drop target", async () => {
    using setup = await composerDragFixture();
    await using screen = await render(setup.ui);
    const source = nativeRow(screen.container, unassigned);
    const composer = screen.container.querySelector<HTMLElement>("[data-thread-context-drop]")!;
    const outside = dragPoint(screen.container.querySelector("[data-outside-composer]")!);
    await act(() => {
      dragPointer(source, "pointerdown", dragPoint(source));
      dragPointer(document, "pointermove", dragPoint(nativeRow(screen.container, member), 0.2));
    });
    expect(document.querySelector("[data-personal-thread-insertion]")).not.toBeNull();
    await act(() => dragPointer(document, "pointermove", dragPoint(composer)));
    expect(composer.dataset.threadContextOver).toBe("true");
    await act(() => {
      dragPointer(document, "pointermove", outside);
      dragPointer(document, "pointerup", outside, 0);
    });
    expect(composerReferences(screen.container)).toEqual([]);
    expect(composer.hasAttribute("data-thread-context-over")).toBe(false);
    expect(document.querySelector("[data-personal-thread-drag]")).toBeNull();
    expect(document.querySelector("[data-personal-thread-insertion]")).toBeNull();
    expect(setup.placeThread).not.toHaveBeenCalled();
    expect(setup.controller.assign).not.toHaveBeenCalled();
    expect(setup.reorder).not.toHaveBeenCalled();
  });

  it("accepts a composer release reported as buttons=0 without a duplicate drop", async () => {
    using setup = await composerDragFixture();
    await using screen = await render(setup.ui);
    const source = nativeRow(screen.container, unassigned);
    const composer = screen.container.querySelector<HTMLElement>("[data-thread-context-drop]")!;
    let drops = 0;
    composer.addEventListener(THREAD_CONTEXT_DROP_EVENT, () => {
      drops += 1;
    });
    await act(() => {
      dragPointer(source, "pointerdown", dragPoint(source));
      dragPointer(document, "pointermove", dragPoint(nativeRow(screen.container, member), 0.2));
      dragPointer(document, "pointermove", dragPoint(composer), 0);
      dragPointer(document, "pointerup", dragPoint(composer), 0);
    });
    expect(composerReferences(screen.container)).toEqual([
      scopeThreadRef(unassigned.environmentId, unassigned.id),
    ]);
    expect(drops).toBe(1);
    expect(setup.placeThread).not.toHaveBeenCalled();
    expect(document.querySelector("[data-personal-thread-drag]")).toBeNull();
  });

  it.each(["Escape", "pointercancel", "unmount"])(
    "cleans the composer highlight and ghost on %s",
    async (cancel) => {
      using setup = await composerDragFixture();
      await using screen = await render(setup.ui);
      const source = nativeRow(screen.container, member);
      const composer = screen.container.querySelector<HTMLElement>("[data-thread-context-drop]")!;
      const at = dragPoint(composer);
      await act(() => {
        dragPointer(source, "pointerdown", dragPoint(source));
        dragPointer(document, "pointermove", at);
      });
      expect(composer.dataset.threadContextOver).toBe("true");
      if (cancel === "unmount") await screen.unmount();
      else
        await act(() => {
          if (cancel === "Escape")
            document.dispatchEvent(new KeyboardEvent("keydown", { bubbles: true, code: "Escape" }));
          else dragPointer(document, "pointercancel", at, 0);
        });
      expect(composer.hasAttribute("data-thread-context-over")).toBe(false);
      expect(document.querySelector("[data-personal-thread-drag]")).toBeNull();
      expect(source.style.opacity).toBe("");
      await act(() => dragPointer(document, "pointerup", at, 0));
      expect(composer.querySelector("[data-composer-references]")!.textContent).toBe("[]");
      expect(setup.placeThread).not.toHaveBeenCalled();
      expect(setup.controller.assign).not.toHaveBeenCalled();
      if (cancel !== "unmount")
        await expect.element(page.getByText(member.title, { exact: true })).toBeVisible();
    },
  );
});

describe("personal Workstreams native presentation", () => {
  it("deduplicates header totals without treating idle or read completions as Done", () => {
    const visited = "2026-11-12T11:00:00.000Z";
    const completed = "2026-11-12T12:00:00.000Z";
    const base = makeThreadFixture();
    const runtime = {
      status: "running" as const,
      providerName: "Codex",
      providerInstanceId: base.providerInstanceId,
      activeRunId: null,
      lastError: null,
      updatedAt: visited,
    };
    const done = makeThreadFixture({
      id: ThreadId.make("done"),
      runtime: null,
      lastVisitedAt: visited,
      latestRun: {
        runId: RunId.make("completed-run"),
        status: "completed",
        assistantMessageId: null,
        requestedAt: visited,
        startedAt: visited,
        completedAt: completed,
      },
    });
    const approval = makeThreadFixture({
      id: ThreadId.make("approval"),
      hasPendingApprovals: true,
    });
    const roster = [
      base,
      done,
      { ...done, id: ThreadId.make("read"), lastVisitedAt: completed },
      { ...base, id: ThreadId.make("working"), runtime },
      { ...base, id: ThreadId.make("failed"), runtime: { ...runtime, status: "failed" as const } },
      approval,
      approval,
      { ...approval, environmentId: EnvironmentId.make("another-env") },
      { ...base, id: ThreadId.make("request"), pendingGuestMessageCount: 1 },
    ];
    expect(summarizePersonalThreads(roster)).toEqual({
      total: 8,
      running: 1,
      attention: 3,
      failed: 1,
      done: 1,
    });
  });

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
    await expect
      .element(page.getByRole("button", { name: "Collapse Unassigned", exact: true }))
      .toMatchTextContent(/1 thread$/);
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
    await expect.element(header).toHaveAccessibleDescription(/3 threads/);
    await expect.element(header).toHaveAccessibleDescription(/1 working/);
    await expect.element(header).toHaveAccessibleDescription(/1 need attention/);
    await expect.element(header).toHaveAccessibleDescription(/1 failed/);
  });
  it("keeps attention in the accessible summary and numbers, with only failures in metadata", async () => {
    const summary = { total: 4, running: 1, attention: 2, done: 1, failed: 0 };
    await using screen = await render(
      <PersonalSidebarGroupHeader name="Alpha" collapsed onToggle={() => {}} summary={summary} />,
    );
    const header = page.getByRole("button", { name: "Expand Alpha", exact: true });
    await expect
      .element(header)
      .toHaveAccessibleDescription("4 threads, 1 working, 2 need attention, 1 done, 0 failed.");
    await expect.element(page.getByText("4 threads", { exact: true })).toBeVisible();
    await expect.element(page.getByText("2", { exact: true })).toBeVisible();
    await expect.element(page.getByText(/^[0-9]+ needs? attention$/)).not.toBeInTheDocument();
    await expect.element(page.getByText("0 failed", { exact: true })).not.toBeInTheDocument();
    await screen.rerender(
      <PersonalSidebarGroupHeader
        name="Alpha"
        collapsed
        onToggle={() => {}}
        summary={{ ...summary, failed: 1 }}
      />,
    );
    await expect.element(page.getByText("1 failed", { exact: true })).toBeVisible();
    await expect
      .element(header)
      .toHaveAccessibleDescription("4 threads, 1 working, 2 need attention, 1 done, 1 failed.");
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
  it("reports an unexpected assignment failure and releases the guard for the next drop", async () => {
    using setup = fixture();
    const { registry, controller } = setup;
    vi.mocked(controller.assign).mockRejectedValueOnce(new Error("synthetic assignment failure"));
    const externalDropRef: { current: NativeThreadExternalDrop | null } = { current: null };
    await using _screen = await render(
      <RegistryContext.Provider value={registry}>
        <WorkstreamNativeSidebar
          controller={controller}
          threads={[thread]}
          externalDropRef={externalDropRef}
          renderThread={(entry) => <li>{entry.title}</li>}
          canOperate={() => true}
          canReorder={() => false}
          reorder={async () => {}}
        />
      </RegistryContext.Provider>,
    );
    const section = _screen.container.querySelector<HTMLElement>("[data-native-workstream]")!;
    const rect = section.getBoundingClientRect();
    const point = { x: rect.left + 2, y: rect.top + 2 };
    await act(async () => {
      expect(await externalDropRef.current!.dropExternal(point, thread)).toBe(false);
    });
    await act(async () => {
      expect(await externalDropRef.current!.dropExternal(point, thread)).toBe(true);
    });
    expect(controller.assign).toHaveBeenCalledTimes(2);
  });
  it.each([true, false])(
    "saves consecutive menu assignments with pinned=%s while the first save is pending",
    async (pinned) => {
      using setup = fixture();
      const { registry, controller } = setup;
      const entry = { ...thread, pinnedAt: pinned ? thread.pinnedAt : null };
      let finish!: (value: { ok: true; value: undefined }) => void;
      vi.mocked(controller.assign).mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            finish = resolve;
          }),
      );
      const assignmentMenuRef: {
        current: ((entry: EnvironmentThreadShell) => NativeThreadAssignmentMenu) | null;
      } = { current: null };
      await using _screen = await render(
        <RegistryContext.Provider value={registry}>
          <WorkstreamNativeSidebar
            controller={controller}
            threads={[entry]}
            assignmentMenuRef={assignmentMenuRef}
            renderThread={(row) => <li>{row.title}</li>}
            canOperate={() => true}
            canReorder={() => false}
            reorder={async () => {}}
          />
        </RegistryContext.Provider>,
      );
      expect(
        assignmentMenuRef.current!(entry).items.find(
          (item) => item.id === "assign-workstream:remove",
        )?.checked,
      ).toBe(true);
      await act(() =>
        registry.set(setup.state, {
          ...setup.initial,
          assignmentsByThreadKey: new Map([
            [
              personalThreadKey(entry),
              { threadId: entry.id, workstreamId: workstream.id, revision: 1, updatedAt: null },
            ],
          ]),
        }),
      );
      expect(
        assignmentMenuRef.current!(entry).items.find(
          (item) => item.id === `assign-workstream:group:${workstream.id}`,
        )?.checked,
      ).toBe(true);
      expect(
        assignmentMenuRef.current!(entry).items.find(
          (item) => item.id === "assign-workstream:remove",
        )?.checked,
      ).toBe(false);
      try {
        await act(() =>
          assignmentMenuRef.current!(entry).onAction(`assign-workstream:group:${workstream.id}`),
        );
        await act(() => assignmentMenuRef.current!(entry).onAction("assign-workstream:remove"));
        expect(controller.assign).toHaveBeenCalledTimes(2);
        expect(controller.assign).toHaveBeenLastCalledWith(entry, null);
      } finally {
        await act(() => finish({ ok: true, value: undefined }));
      }
    },
  );
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
