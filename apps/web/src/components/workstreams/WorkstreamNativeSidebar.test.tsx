// @vitest-environment jsdom

import { act, useState, useCallback, useRef } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import type { WorkstreamCommand, WorkstreamReceipt } from "@t3tools/contracts";
import type { WorkstreamDetailView, WorkstreamListView } from "../../state/workstreams";
import { WorkstreamCreateForm, WorkstreamSidebarSection } from "./WorkstreamSidebarSection";
import { SidebarThreadHeader } from "../sidebar/SidebarThreadHeader";
import { SidebarProvider } from "../ui/sidebar";
import { canEditWorkstreams } from "./nativeWorkstreamActions";
import { summarizeWorkstreamThreadStatuses } from "./workstreamThreadStatus";
import { WorkstreamNativeSidebar } from "./WorkstreamNativeSidebar";
import { groupNativeThreadsByWorkstream } from "./nativeThreadGrouping";
import { data, now, placements, reference, thread } from "./nativeWorkstreamActions.fixtures";

let root: Root;
let container: HTMLDivElement;
const threads = [
  { ...thread, title: "First native conversation" },
  {
    ...thread,
    id: "unassigned",
    title: "Unassigned native conversation",
    projectId: "another-repo",
  },
] as unknown as readonly EnvironmentThreadShell[];
const detail = {
  detail: {
    context: { owner_id: "owner", server_generation: 7, registry_version: 11 },
    workstream: {},
  },
  references: { items: [reference] },
  memberships: { items: [] },
} as unknown as WorkstreamDetailView;
const committed = {
  state: "committed",
  registry_version: 12,
  effects: { workstream_versions: [] },
} as unknown as WorkstreamReceipt;
const reorder = vi.fn(async () => undefined);
let controller: WorkstreamListView;

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.spyOn(Date, "now").mockReturnValue(now);
  localStorage.clear();
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
  const submit = vi.fn(async (_command: WorkstreamCommand) => committed);
  controller = {
    data,
    placements,
    placementInventory: {
      coverage: "complete",
      identities: threads.map((item) => ({
        source_instance_id: item.environmentId,
        native_thread_id: item.id,
      })),
      json: "[]",
      totalIdentities: threads.length,
    },
    error: null,
    loading: false,
    refresh: vi.fn(),
    submit,
    runBindingOperation: (operation) => operation(submit),
    loadDetail: vi.fn(async () => detail),
    loadReference: vi.fn(),
  };
  reorder.mockClear();
});

afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  localStorage.clear();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

async function render(
  visibleThreads: readonly EnvironmentThreadShell[] = threads,
  summaryThreads: readonly EnvironmentThreadShell[] = visibleThreads,
  movement: Partial<
    Pick<
      Parameters<typeof WorkstreamNativeSidebar>[0],
      "captureDrag" | "reorderSelection" | "onVisibleGroupsChange" | "onMovementError"
    >
  > = {},
) {
  const group = (members: readonly EnvironmentThreadShell[]) =>
    groupNativeThreadsByWorkstream({
      workstreams: controller.data?.items ?? [],
      placements: controller.placements?.items ?? [],
      threads: members,
      trustedNow: new Date(now).toISOString(),
      trustedEnvironments: new Map(
        (controller.placements?.trustedEnvironments ?? []).map((entry) => [
          entry.environmentId,
          entry,
        ]),
      ),
    });
  await act(async () =>
    root.render(
      <WorkstreamNativeSidebar
        {...movement}
        controller={controller}
        grouping={group(visibleThreads)}
        summaryGrouping={group(summaryThreads)}
        renderThread={(item) => (
          <li>
            <button type="button">{item.title}</button>
          </li>
        )}
        canReorder={() => true}
        reorder={reorder}
      />,
    ),
  );
}

async function clickLabel(label: string) {
  const button = document.querySelector<HTMLElement>(`[aria-label="${label}"]`);
  expect(button, label).not.toBeNull();
  await act(async () => button!.click());
}

async function clickMenu(text: string) {
  const item = [...document.querySelectorAll<HTMLElement>('[role="menuitem"]')].find(
    (entry) => entry.textContent?.trim() === text,
  );
  expect(item, text).not.toBeUndefined();
  await act(async () => item!.click());
}

async function dragEvent(element: Element, type: string, clientY = 0) {
  const event = new Event(type, { bubbles: true, cancelable: true });
  Object.defineProperties(event, {
    dataTransfer: { value: { effectAllowed: "none", dropEffect: "none", setData: vi.fn() } },
    clientY: { value: clientY },
  });
  await act(async () => {
    element.dispatchEvent(event);
  });
  return event;
}

function nativeRow(index: number) {
  return [...container.querySelectorAll("button")].find(
    (button) => button.textContent === threads[index]!.title,
  )!;
}

describe("native Workstream sidebar interactions", () => {
  it("captures the selected group at HTML drag start and submits memberships in that stable order", async () => {
    const second = threads[1]!;
    controller = {
      ...controller,
      placements: {
        ...placements,
        items: [
          ...placements.items,
          {
            ...placements.items[0]!,
            native_thread_id: second.id,
            membership_id: "second-member",
            native_reference_id: "second-reference",
          },
        ],
      },
    };
    const secondReference = {
      ...reference,
      native_reference_id: "second-reference",
      identity: { ...reference.identity, native_id: second.id },
      registration: {
        ...reference.registration,
        evidence: { ...reference.registration.evidence!, native_id: second.id },
      },
    };
    let batchCompletion: Promise<unknown> | null = null;
    controller = {
      ...controller,
      loadDetail: vi.fn(async () => ({
        ...detail,
        references: { ...detail.references, items: [reference, secondReference] },
      })),
      runBindingOperation: (operation) => {
        const pending = operation(controller.submit);
        batchCompletion = pending;
        return pending;
      },
    };
    const captureDrag = vi.fn(() => [second, threads[0]!]);
    const submit = vi.mocked(controller.submit);
    submit.mockResolvedValueOnce({
      ...committed,
      state: "committed",
      registry_version: 12,
      effects: {
        workstream_versions: [
          { workstream_id: "alpha", version: 4 },
          { workstream_id: "beta", version: 4 },
        ],
      },
    } as unknown as WorkstreamReceipt);
    await render(threads, threads, { captureDrag });
    await dragEvent(nativeRow(0), "dragstart");
    const target = container.querySelector('[aria-label="Collapse beta"]')!.closest("li")!;
    await dragEvent(target, "drop");
    expect(batchCompletion).not.toBeNull();
    await act(async () => {
      await batchCompletion;
    });
    expect(captureDrag).toHaveBeenCalledWith(threads[0]);
    expect(submit).toHaveBeenCalledTimes(2);
    expect(submit.mock.calls.map(([command]) => command.action)).toEqual([
      expect.objectContaining({
        operation: "move_primary",
        source_membership_id: "second-member",
        expected_source_version: 3,
      }),
      expect.objectContaining({
        operation: "move_primary",
        source_membership_id: "membership",
        expected_source_version: 4,
        expected_destination_version: 4,
      }),
    ]);
    expect(reorder).not.toHaveBeenCalled();
  });

  it("reports the expanded group order and excludes collapsed member ranges", async () => {
    const visible = vi.fn();
    await render(threads, threads, { onVisibleGroupsChange: visible });
    expect(visible).toHaveBeenLastCalledWith(["alpha", "beta", null]);
    await clickLabel("Collapse alpha");
    expect(visible).toHaveBeenLastCalledWith(["beta", null]);
    expect(nativeRow(0)).toBeUndefined();
    await clickLabel("Collapse Unassigned");
    expect(visible).toHaveBeenLastCalledWith(["beta"]);
    await clickLabel("Expand alpha");
    expect(visible).toHaveBeenLastCalledWith(["alpha", "beta"]);
    expect(nativeRow(0)).toBeDefined();
  });

  it("drags the native row into another Workstream with compatible target feedback", async () => {
    await render();
    expect(container.querySelector('[aria-label="New Workstream name"]')).toBeNull();
    const row = nativeRow(0);
    expect(row.closest('[draggable="true"]')?.textContent).not.toContain("alpha");
    await dragEvent(row, "dragstart");
    const target = container.querySelector('[aria-label="Collapse beta"]')!.closest("li")!;
    expect((await dragEvent(target, "dragover")).defaultPrevented).toBe(true);
    expect(target.getAttribute("data-drop-target")).toBe("thread");
    await dragEvent(target, "drop");
    await vi.waitFor(() => expect(controller.submit).toHaveBeenCalledOnce());
    expect(controller.submit).toHaveBeenCalledWith(
      expect.objectContaining({
        action: expect.objectContaining({
          operation: "move_primary",
          destination_workstream_id: "beta",
        }),
      }),
    );
    expect(reorder).not.toHaveBeenCalled();
    expect(container.querySelector('[data-drop-target="thread"]')).toBeNull();
  });

  it("drops a native row on Unassigned to remove its primary membership", async () => {
    await render();
    await dragEvent(nativeRow(0), "dragstart");
    const target = container
      .querySelector('[aria-label="Collapse Unassigned"]')!
      .closest('[data-thread-drop-header="__unassigned__"]')!.parentElement!;
    await dragEvent(target, "dragover");
    expect(target.getAttribute("data-drop-target")).toBe("thread");
    await dragEvent(target, "drop");
    await vi.waitFor(() => expect(controller.submit).toHaveBeenCalledOnce());
    expect(controller.submit).toHaveBeenCalledWith(
      expect.objectContaining({
        action: expect.objectContaining({
          operation: "remove_membership",
          membership_id: "membership",
        }),
      }),
    );
  });

  it("shows the insertion edge and reorders native rows without changing membership", async () => {
    controller = {
      ...controller,
      placements: {
        ...placements,
        items: [
          ...placements.items,
          {
            ...placements.items[0]!,
            native_reference_id: "reference-two",
            native_thread_id: "unassigned",
            membership_id: "member-two",
          },
        ],
      },
    };
    await render();
    await dragEvent(nativeRow(0), "dragstart");
    const target = nativeRow(1).closest('[draggable="true"]')!;
    await dragEvent(target, "dragover", 10);
    expect(target.getAttribute("data-drop-position")).toBe("after");
    await dragEvent(target, "drop", 10);
    expect(reorder).toHaveBeenCalledWith(threads[0], threads[1], true);
    expect(controller.submit).not.toHaveBeenCalled();
  });

  it("starts group reordering only from the group grip and never submits membership changes", async () => {
    await render();
    const header = container.querySelector('[aria-label="Collapse alpha"]')!.parentElement!;
    expect(header.closest('[draggable="true"]')).toBeNull();
    const handle = container.querySelector('[aria-label="Drag Workstream alpha to reorder"]')!;
    await dragEvent(handle, "dragstart");
    const target = container.querySelector('[aria-label="Collapse beta"]')!.closest("li")!;
    await dragEvent(target, "dragover", 10);
    expect(target.getAttribute("data-drop-target")).toBeNull();
    await dragEvent(target, "drop", 10);
    await vi.waitFor(() => expect(controller.submit).toHaveBeenCalled());
    expect(
      vi
        .mocked(controller.submit)
        .mock.calls.every(([command]) => command.action.operation === "update_workstream"),
    ).toBe(true);
    expect(reorder).not.toHaveBeenCalled();
  });

  it("clears drop feedback on cancellation and rejects a drop after write access changes", async () => {
    await render();
    await dragEvent(nativeRow(0), "dragstart");
    const target = container.querySelector('[aria-label="Collapse beta"]')!.closest("li")!;
    await dragEvent(target, "dragover");
    await dragEvent(nativeRow(0), "dragend");
    expect(container.querySelector('[data-drop-target="thread"]')).toBeNull();
    await dragEvent(nativeRow(0), "dragstart");
    controller = {
      ...controller,
      data: {
        ...data,
        binding: { ...data.binding, authorizationRevision: 2, permissions: ["workstreams:read"] },
      },
    };
    await render();
    await dragEvent(target, "drop");
    expect(controller.submit).not.toHaveBeenCalled();
  });

  it("does not treat selected native text as a thread drag", async () => {
    await render();
    const row = nativeRow(0);
    const selection = window.getSelection()!;
    const range = document.createRange();
    range.selectNodeContents(row);
    selection.addRange(range);
    expect((await dragEvent(row, "dragstart")).defaultPrevented).toBe(true);
    const target = container.querySelector('[aria-label="Collapse beta"]')!.closest("li")!;
    await dragEvent(target, "drop");
    expect(controller.submit).not.toHaveBeenCalled();
    selection.removeAllRanges();
  });

  it("ignores external drags and refuses row drags without verified placements", async () => {
    controller = { ...controller, placements: null };
    await render();
    const target = container.querySelector('[aria-label="Collapse beta"]')!.closest("li")!;
    expect((await dragEvent(target, "dragover")).defaultPrevented).toBe(false);
    expect((await dragEvent(nativeRow(0), "dragstart")).defaultPrevented).toBe(true);
    await dragEvent(target, "drop");
    expect(controller.submit).not.toHaveBeenCalled();
    expect(reorder).not.toHaveBeenCalled();
  });

  it("preserves all native conversations under partial or untrusted placement coverage", async () => {
    controller = {
      ...controller,
      placements: null,
      placementInventory: { ...controller.placementInventory, coverage: "partial", identities: [] },
    };
    await render();
    const unassigned = container.querySelector('[aria-label="Unassigned threads"]');
    for (const item of threads) expect(unassigned?.textContent).toContain(item.title);
    expect(container.textContent).toContain("Thread assignments are unavailable");
    await clickLabel("Workstream actions for First native conversation");
    const assignment = [...document.querySelectorAll('[role="menuitem"]')].find(
      (entry) => entry.textContent === "Assign to beta",
    );
    expect(assignment?.getAttribute("aria-disabled")).toBe("true");
    expect(controller.submit).not.toHaveBeenCalled();
  });

  it("persists collapse without removing Unassigned conversations", async () => {
    await render();
    await clickLabel("Collapse alpha");
    expect(container.textContent).not.toContain("First native conversation");
    expect(container.textContent).toContain("Unassigned native conversation");
    await act(async () => root.unmount());
    root = createRoot(container);
    await render();
    expect(container.textContent).not.toContain("First native conversation");
    await clickLabel("Expand alpha");
    expect(container.textContent).toContain("First native conversation");
  });

  it("moves primary membership through the real menu without a native reorder or secondary change", async () => {
    await render();
    await clickLabel("Workstream actions for First native conversation");
    await clickMenu("Move to beta");
    await vi.waitFor(() => expect(controller.submit).toHaveBeenCalledOnce());
    expect(controller.submit).toHaveBeenCalledWith(
      expect.objectContaining({
        expected_registry_version: 11,
        action: {
          operation: "move_primary",
          source_workstream_id: "alpha",
          expected_source_version: 3,
          source_membership_id: "membership",
          destination_workstream_id: "beta",
          expected_destination_version: 3,
        },
      }),
    );
    expect(reorder).not.toHaveBeenCalled();
    expect(placements.items[1]?.kind).toBe("secondary");
  });

  it("reorders actual native members through the menu while leaving registry membership alone", async () => {
    controller = {
      ...controller,
      placements: {
        ...placements,
        items: [
          ...placements.items,
          {
            ...placements.items[0]!,
            native_reference_id: "reference-two",
            native_thread_id: "unassigned",
            membership_id: "member-two",
          },
        ],
      },
    };
    await render();
    await clickLabel("Workstream actions for First native conversation");
    await clickMenu("Move thread down");
    expect(reorder).toHaveBeenCalledWith(threads[0], threads[1], true);
    expect(controller.submit).not.toHaveBeenCalled();
  });

  it("cancels a pending membership lookup when write authority changes", async () => {
    let resolveDetail!: (value: WorkstreamDetailView) => void;
    const pending = new Promise<WorkstreamDetailView>((resolve) => {
      resolveDetail = resolve;
    });
    const loadDetail = vi.fn(
      (_id: string, _options?: { readonly signal?: AbortSignal }) => pending,
    );
    controller = { ...controller, loadDetail };
    await render();
    await clickLabel("Workstream actions for First native conversation");
    await clickMenu("Move to beta");
    expect(loadDetail).toHaveBeenCalledOnce();
    controller = {
      ...controller,
      data: {
        ...data,
        binding: { ...data.binding, authorizationRevision: 2, permissions: ["workstreams:read"] },
      },
    };
    await render();
    expect(loadDetail.mock.calls[0]?.[1]?.signal?.aborted).toBe(true);
    await act(async () => {
      resolveDetail(detail);
      await pending;
    });
    expect(controller.submit).not.toHaveBeenCalled();
    expect(container.textContent).toContain("Workstreams are read-only");
  });
});

function CreationHeader({ controller }: { readonly controller: WorkstreamListView }) {
  const [open, setOpen] = useState(false);
  const [pending, setPending] = useState(false);
  const close = useCallback(() => setOpen(false), []);
  const searchRef = useRef<HTMLInputElement | null>(null);
  return (
    <SidebarProvider>
      <SidebarThreadHeader
        hasProjects
        projectScope={null}
        onNewProject={() => undefined}
        onNewThread={() => undefined}
        onNewWorkstream={() => setOpen(true)}
        newWorkstreamDisabled={
          !canEditWorkstreams(controller.data) || controller.loading || pending || open
        }
        newThreadDisabled={false}
        newThreadShortcutLabel={null}
        newThreadInProjectShortcutLabel={null}
        showNewThreadInProjectHint={false}
        searchInputRef={searchRef}
        searchQuery=""
        onSearchQueryChange={() => undefined}
        onSearchKeyDown={() => undefined}
        isSearching={false}
        searchResultCount={0}
        activeSearchResultIndex={0}
        onClearSearch={() => undefined}
      />
      <WorkstreamCreateForm
        controller={controller}
        open={open}
        onClose={close}
        onPendingChange={setPending}
      />
    </SidebarProvider>
  );
}

async function renderCreation() {
  vi.stubGlobal(
    "matchMedia",
    vi.fn(() => ({
      matches: false,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    })),
  );
  await act(async () => root.render(<CreationHeader controller={controller} />));
}

function nameInput() {
  return container.querySelector<HTMLInputElement>('[aria-label="New Workstream name"]');
}

async function enterName(value: string) {
  await act(async () => {
    const input = nameInput()!;
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!.call(input, value);
    input.dispatchEvent(new Event("input", { bubbles: true }));
  });
}

async function submitCreation() {
  await act(async () => {
    container
      .querySelector("form")!
      .dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
  });
}

describe("Workstream toolbar creation", () => {
  it("opens a focused temporary form between the toolbar actions and cancels outside or with Escape", async () => {
    await renderCreation();
    expect(nameInput()).toBeNull();
    const actions = [...container.querySelectorAll("button[aria-label]")].map((button) =>
      button.getAttribute("aria-label"),
    );
    expect(actions).toEqual(["Add project", "New Workstream", "New thread"]);
    await clickLabel("New Workstream");
    expect(document.activeElement).toBe(nameInput());
    await enterName("Discard me");
    await act(async () => nameInput()!.dispatchEvent(new Event("pointerdown", { bubbles: true })));
    expect(nameInput()?.value).toBe("Discard me");
    await act(async () => document.body.dispatchEvent(new Event("pointerdown", { bubbles: true })));
    expect(nameInput()).toBeNull();
    await clickLabel("New Workstream");
    expect(nameInput()?.value).toBe("");
    await act(async () =>
      document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true })),
    );
    expect(nameInput()).toBeNull();
    expect(controller.submit).not.toHaveBeenCalled();
  });

  it("submits the trimmed typed create command through form submission and closes only on success", async () => {
    await renderCreation();
    await clickLabel("New Workstream");
    await enterName("  Release prep  ");
    await submitCreation();
    await vi.waitFor(() => expect(nameInput()).toBeNull());
    expect(controller.submit).toHaveBeenCalledOnce();
    expect(controller.submit).toHaveBeenCalledWith(
      expect.objectContaining({
        expected_server_generation: 7,
        expected_registry_version: 11,
        action: expect.objectContaining({
          operation: "create_workstream",
          name: "Release prep",
          lifecycle: "planned",
        }),
      }),
    );
  });

  it("retains the name and reports a rejected receipt or transport error for correction", async () => {
    vi.mocked(controller.submit).mockResolvedValueOnce({
      state: "rejected",
      error: { code: "validation_error" },
    } as unknown as WorkstreamReceipt);
    await renderCreation();
    await clickLabel("New Workstream");
    await enterName("Release prep");
    await submitCreation();
    await vi.waitFor(() =>
      expect(container.querySelector('[role="alert"]')?.textContent).toContain("validation_error"),
    );
    expect(nameInput()?.value).toBe("Release prep");
    vi.mocked(controller.submit).mockRejectedValueOnce(new Error("Connection failed"));
    await act(async () =>
      [...container.querySelectorAll("button")]
        .find((button) => button.textContent === "Create")!
        .click(),
    );
    await vi.waitFor(() =>
      expect(container.querySelector('[role="alert"]')?.textContent).toBe("Connection failed"),
    );
    expect(nameInput()?.value).toBe("Release prep");
  });

  it("blocks duplicate pending creates even after dismissal and respects loading and authority", async () => {
    let resolve!: (receipt: WorkstreamReceipt) => void;
    vi.mocked(controller.submit).mockImplementationOnce(
      () =>
        new Promise((done) => {
          resolve = done;
        }),
    );
    await renderCreation();
    await clickLabel("New Workstream");
    await enterName("Release prep");
    await submitCreation();
    await vi.waitFor(() => expect(controller.submit).toHaveBeenCalledOnce());
    await submitCreation();
    expect(controller.submit).toHaveBeenCalledOnce();
    await act(async () =>
      document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true })),
    );
    expect(nameInput()).toBeNull();
    expect(
      container.querySelector<HTMLButtonElement>('[aria-label="New Workstream"]')?.disabled,
    ).toBe(true);
    await act(async () => resolve(committed));
    expect(
      container.querySelector<HTMLButtonElement>('[aria-label="New Workstream"]')?.disabled,
    ).toBe(false);
    controller = { ...controller, loading: true };
    await renderCreation();
    expect(
      container.querySelector<HTMLButtonElement>('[aria-label="New Workstream"]')?.disabled,
    ).toBe(true);
    controller = { ...controller, loading: false };
    await renderCreation();
    await clickLabel("New Workstream");
    await enterName("Must not cross authority");
    controller = {
      ...controller,
      loading: false,
      data: { ...data, binding: { ...data.binding, permissions: ["workstreams:read"] } },
    };
    await renderCreation();
    expect(
      container.querySelector<HTMLButtonElement>('[aria-label="New Workstream"]')?.disabled,
    ).toBe(true);
    expect(nameInput()).toBeNull();
    expect(controller.submit).toHaveBeenCalledOnce();
  });
});

function statusThread(
  id: string,
  status: NonNullable<EnvironmentThreadShell["runtime"]>["status"] | null,
  changes: Partial<EnvironmentThreadShell> = {},
): EnvironmentThreadShell {
  return {
    ...threads[0]!,
    id,
    title: id,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    runtime:
      status === null
        ? null
        : {
            status,
            activeRunId: null,
            providerInstanceId: "synthetic-provider",
            providerName: "Synthetic",
            lastError: null,
            lastErrorClass: null,
            updatedAt: new Date(now).toISOString(),
          },
    ...changes,
  } as EnvironmentThreadShell;
}

function primaryPlacementsFor(members: readonly EnvironmentThreadShell[]) {
  return members.map((member, index) => ({
    ...placements.items[0]!,
    native_reference_id: `reference-${index}`,
    membership_id: `membership-${index}`,
    native_thread_id: member.id,
    source_instance_id: member.environmentId,
  }));
}

describe("Workstream live thread summaries", () => {
  it("matches native row precedence for running, connecting, approval, input, failed and background states", () => {
    const members = [
      statusThread("running", "running"),
      statusThread("starting", "starting"),
      statusThread("input", "running", { hasPendingUserInput: true }),
      statusThread("approval", "running", { hasPendingApprovals: true }),
      statusThread("failed", "failed"),
      statusThread("background", "idle"),
      statusThread("monitoring", "completed"),
      statusThread("unknown", null),
      statusThread("ready", "completed"),
    ];
    expect(
      summarizeWorkstreamThreadStatuses({
        groups: [{ workstream: data.items[0]!, threads: members }],
      }).get("alpha"),
    ).toEqual({ total: 9, running: 2, waiting: 2, failed: 1 });
    expect(
      summarizeWorkstreamThreadStatuses({
        groups: [
          {
            workstream: data.items[0]!,
            threads: [
              statusThread("approval-error", "failed", { hasPendingApprovals: true }),
              statusThread("input-error", "failed", { hasPendingUserInput: true }),
            ],
          },
        ],
      }).get("alpha"),
    ).toEqual({ total: 2, running: 0, waiting: 2, failed: 0 });
  });

  it("counts distinct environment/thread pairs and never invents a running state for unknown sessions", () => {
    const running = statusThread("same", "running");
    const remote = { ...running, environmentId: "env:other" } as EnvironmentThreadShell;
    expect(
      summarizeWorkstreamThreadStatuses({
        groups: [
          {
            workstream: data.items[0]!,
            threads: [running, running, remote, statusThread("unknown", null)],
          },
        ],
      }).get("alpha"),
    ).toEqual({ total: 3, running: 2, waiting: 0, failed: 0 });
    expect(summarizeWorkstreamThreadStatuses({ groups: [] }).size).toBe(0);
  });

  it("keeps all known primary members in collapsed counts and replaces input/running indicators with Failed", async () => {
    const members = [
      statusThread("running", "running"),
      statusThread("waiting", "running", { hasPendingUserInput: true }),
      statusThread("pinned", "idle", { pinnedAt: new Date(now).toISOString() }),
      statusThread("settled", "idle", { settledOverride: "settled" }),
      statusThread("snoozed", "idle", { snoozedAt: new Date(now).toISOString() }),
      statusThread("archived", null, { archivedAt: new Date(now).toISOString() }),
    ];
    const primary = primaryPlacementsFor(members);
    controller = {
      ...controller,
      placements: {
        ...placements,
        items: [
          ...primary,
          ...primary.map((item) => ({
            ...item,
            kind: "secondary" as const,
            workstream_id: "beta",
            membership_id: `${item.membership_id}-secondary`,
          })),
        ],
      },
    };
    await render(members.slice(0, 2), members);
    const count = () =>
      container.querySelector('[aria-label="alpha: 1 of 6 known threads running"]');
    expect(count()?.textContent).toBe("1/6");
    expect(
      container.querySelector('[aria-label="beta: 0 of 0 known threads running"]')?.textContent,
    ).toBe("0/0");
    const indicators = count()!.parentElement!;
    expect(indicators.children[0]?.getAttribute("aria-label")).toBe(
      "1 thread waiting for input or approval",
    );
    expect(indicators.children[1]?.querySelector("svg")?.getAttribute("aria-label")).toBe(
      "1 running thread",
    );
    expect(indicators.children[1]?.querySelector("svg")?.getAttribute("class")).toContain(
      "motion-safe:visible-animate-spin",
    );
    expect(indicators.children[0]?.getAttribute("class")).toContain("bg-waiting");
    expect(indicators.children[1]?.getAttribute("class")).toContain("text-info-foreground");
    const header = container.querySelector('[aria-label="Collapse alpha"]')!.parentElement!;
    expect(header.textContent).not.toContain("active");
    await clickLabel("Collapse alpha");
    expect(count()?.textContent).toBe("1/6");
    expect(
      [...container.querySelectorAll("button")].some((button) => button.textContent === "running"),
    ).toBe(false);
    const mixedFailure = [
      ...members.slice(0, 2),
      statusThread("pinned", "failed"),
      ...members.slice(3),
    ];
    await render(mixedFailure.slice(0, 2), mixedFailure);
    expect(count()?.parentElement?.textContent).toBe("Failed1/6");
    expect(count()?.parentElement?.querySelector("svg")).toBeNull();
    const failedMembers = [statusThread("running", "failed"), ...members.slice(1)];
    await render(failedMembers.slice(0, 2), failedMembers);
    const failedCount = container.querySelector(
      '[aria-label="alpha: 0 of 6 known threads running"]',
    )!;
    const failedIndicators = failedCount.parentElement!;
    expect(failedIndicators.textContent).toBe("Failed0/6");
    expect(failedIndicators.querySelector('[aria-label="1 failed thread"]')?.className).toContain(
      "text-thread-failed",
    );
    expect(failedIndicators.querySelector("svg")).toBeNull();
    expect(
      failedIndicators.querySelector('[aria-label="1 thread waiting for input or approval"]'),
    ).toBeNull();
    const recoveredMembers = [statusThread("running", "completed"), ...members.slice(1)];
    await render(recoveredMembers.slice(0, 2), recoveredMembers);
    expect(container.querySelector('[aria-label="1 failed thread"]')).toBeNull();
    expect(
      container.querySelector('[aria-label="1 thread waiting for input or approval"]'),
    ).not.toBeNull();
    expect(
      container.querySelector('[aria-label="alpha: 0 of 6 known threads running"]')?.textContent,
    ).toBe("0/6");
  });

  it("uses zero counts when primary placement trust is unavailable, keeping visible threads unassigned", async () => {
    controller = { ...controller, placements: null };
    await render([statusThread("thread", "running")]);
    expect(
      container.querySelector('[aria-label="alpha: 0 of 0 known threads running"]')?.textContent,
    ).toBe("0/0");
    expect(container.querySelector('[aria-label="Unassigned threads"]')?.textContent).toContain(
      "thread",
    );
    expect(container.querySelector('[aria-label="1 running thread"]')).toBeNull();
  });
});

it("keeps parent visibility updates stable when collapsed state is unchanged", async () => {
  let renders = 0;
  function VisibilityHost() {
    const [ids, setIds] = useState<readonly (string | null)[]>([]);
    renders += 1;
    if (renders > 5) throw new Error("Unchanged visibility recursively rendered its parent.");
    return (
      <>
        <WorkstreamSidebarSection controller={controller} onVisibleGroupsChange={setIds} />
        <output data-testid="visible-group-count">{ids.length}</output>
      </>
    );
  }
  await act(async () => root.render(<VisibilityHost />));
  expect(container.querySelector('[data-testid="visible-group-count"]')?.textContent).toBe(
    String(data.items.length + 1),
  );
});
