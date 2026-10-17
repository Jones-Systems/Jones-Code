// @vitest-environment jsdom

import { act, useState, useCallback, useRef } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import {
  buildWorkstreamAssignmentIndex,
  type WorkstreamAssignmentIndex,
} from "@t3tools/client-runtime/state/workstreams";
import type {
  MembershipEpisode,
  WorkstreamCommand,
  WorkstreamReceipt,
  WorkstreamsRegistrationContextAcceptedResponse,
} from "@t3tools/contracts";
import type { WorkstreamDetailView, WorkstreamListView } from "../../state/workstreams";
import { WorkstreamCreateForm } from "./WorkstreamSidebarSection";
import { SidebarThreadHeader } from "../sidebar/SidebarThreadHeader";
import { SidebarProvider } from "../ui/sidebar";
import { canEditWorkstreams } from "./nativeWorkstreamActions";
import { summarizeWorkstreamThreadStatuses } from "./workstreamThreadStatus";
import { WorkstreamNativeSidebar } from "./WorkstreamNativeSidebar";
import { dismissContextMenu, showContextMenuFallback } from "../../contextMenuFallback";
import { buildThreadActionMenuItems } from "../threadActionMenu.logic";
import { groupNativeThreadsByWorkstream } from "./nativeThreadGrouping";
import { data, now, placements, reference, thread } from "./nativeWorkstreamActions.fixtures";

import { assignNativeThread } from "../../jones/workstreams/nativeThreadAssignment";

const registrationContext: WorkstreamsRegistrationContextAcceptedResponse = {
  protocol: "workstreams-registration-context/2.0.0",
  state: "ready",
  ...placements.context,
  grant_id: "grant",
  sources: [
    {
      provider: "t3",
      source_instance_id: thread.environmentId,
      authority_namespace: "authority",
      store_generation: 1,
      resource_kind: "thread",
      id_kind: "internal",
      account_provenance: { kind: "not_account_scoped" },
      native_protocol: "workstreams-t3-provider/1.0.0",
      build: { repository: "Jones-Systems/Jones-Code", sha: "a".repeat(40), tree: "b".repeat(40) },
    },
  ],
};

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
  const submit = vi.fn(async (command: WorkstreamCommand) => ({
    ...committed,
    command_id: command.command_id,
    owner_id: "owner",
    server_generation: 7,
    operation: command.action.operation,
  }));
  controller = {
    data,
    registrationContext,
    assignNativeThread: (selected, destination, options) =>
      assignNativeThread({
        ...options,
        data: controller.data!,
        thread: selected,
        destination,
        runBindingOperation: controller.runBindingOperation,
        loadSnapshot: async () => {
          const loaded = await controller.loadDetail(destination ?? "alpha", options);
          return {
            data: controller.data!,
            registrationContext: controller.registrationContext!,
            placements: controller.placements!,
            reference: { context: loaded.detail.context, reference, latest_observation: null },
            destinationMemberships:
              destination === null
                ? null
                : {
                    context: loaded.detail.context,
                    items: loaded.memberships.items,
                    next_cursor: null,
                  },
          };
        },
      }),
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
  dismissContextMenu();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

async function render(
  visibleThreads: readonly EnvironmentThreadShell[] = threads,
  summaryThreads: readonly EnvironmentThreadShell[] = visibleThreads,
  assignment: {
    readonly assignmentIndex?: WorkstreamAssignmentIndex;
    readonly verificationNow?: string;
  } = {},
) {
  const group = (members: readonly EnvironmentThreadShell[]) =>
    groupNativeThreadsByWorkstream({
      ...assignment,
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
        controller={controller}
        grouping={group(visibleThreads)}
        summaryGrouping={group(summaryThreads)}
        renderThread={(item, assignment) => (
          <li>
            <div
              role="button"
              tabIndex={0}
              onContextMenu={(event) => {
                event.preventDefault();
                void showContextMenuFallback(
                  buildThreadActionMenuItems({
                    canOperate: true,
                    branch: null,
                    projectFilter: null,
                    isPinned: false,
                    isSettled: false,
                    autoSettleEnabled: true,
                    isSnoozed: false,
                    canSnoozeNow: true,
                    isRegeneratingTitle: false,
                    isRunning: false,
                    snoozePresets: [],
                    supports: {
                      settlement: true,
                      autoSettleOptOut: true,
                      snooze: true,
                      pinning: true,
                      titleRegeneration: true,
                    },
                    assignmentItems: assignment.items,
                  }),
                  { x: event.clientX, y: event.clientY },
                ).then((action) => {
                  if (action) assignment.onAction(action);
                });
              }}
            >
              {item.title}
            </div>
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

async function openAssignment(index = 0) {
  await act(async () => {
    const row = nativeRow(index);
    row.dispatchEvent(new MouseEvent("pointerdown", { bubbles: true, button: 2 }));
    row.dispatchEvent(new MouseEvent("contextmenu", { bubbles: true, cancelable: true }));
  });
  await act(async () => {
    await new Promise<void>((resolve) => requestAnimationFrame(() => resolve()));
  });
  await clickMenu("Assign thread");
}

async function clickMenu(text: string) {
  const item = [...document.querySelectorAll<HTMLButtonElement>("button")].find(
    (entry) => !container.contains(entry) && entry.textContent?.trim() === text,
  );
  expect(item, text).not.toBeUndefined();
  await act(async () => item!.click());
}

async function pointerEvent(element: EventTarget, type: string, clientY = 0, buttons = 1) {
  const event = new MouseEvent(type, {
    bubbles: true,
    cancelable: true,
    clientX: 10,
    clientY,
    buttons,
    button: 0,
  });
  Object.defineProperties(event, { pointerId: { value: 1 }, isPrimary: { value: true } });
  await act(async () => {
    element.dispatchEvent(event);
  });
  return event;
}

async function pointAt(target: Element, clientY = 10) {
  Object.defineProperty(document, "elementFromPoint", { configurable: true, value: () => target });
  await pointerEvent(document, "pointermove", clientY);
}

async function release(clientY = 10) {
  await pointerEvent(document, "pointerup", clientY, 0);
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
  return [...container.querySelectorAll<HTMLElement>('[role="button"]')].find(
    (button) => button.textContent === threads[index]!.title,
  )!;
}

describe("native Workstream sidebar interactions", () => {
  it("drags the native row into another Workstream with compatible target feedback", async () => {
    await render();
    expect(container.querySelector('[aria-label="New Workstream name"]')).toBeNull();
    const row = nativeRow(0);
    await pointerEvent(row, "pointerdown");
    const target = container.querySelector('[aria-label="Collapse beta"]')!.closest("li")!;
    await pointAt(target);
    expect(target.getAttribute("data-drop-target")).toBe("thread");
    await release();
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
    await pointerEvent(nativeRow(0), "pointerdown");
    const target = container.querySelector('[aria-label="Collapse Unassigned"]')!.parentElement!;
    await pointAt(target);
    expect(target.getAttribute("data-drop-target")).toBe("thread");
    await release();
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
    await pointerEvent(nativeRow(0), "pointerdown");
    const target = nativeRow(1).closest("[data-native-workstream-thread]")!;
    await pointAt(target);
    expect(target.getAttribute("data-drop-position")).toBe("after");
    await release();
    expect(reorder).toHaveBeenCalledWith(threads[0], threads[1], true);
    expect(controller.submit).not.toHaveBeenCalled();
  });

  it("refuses to reorder against a neighbor whose displayed membership has not been checked", async () => {
    controller = {
      ...controller,
      placementInventory: {
        ...controller.placementInventory,
        coverage: "partial",
        identities: controller.placementInventory.identities.filter(
          (identity) => identity.native_thread_id === threads[0]!.id,
        ),
      },
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
    await pointerEvent(nativeRow(0), "pointerdown");
    const target = nativeRow(1).closest("[data-native-workstream-thread]")!;
    await pointAt(target);
    expect(target.getAttribute("data-drop-position")).toBeNull();
    await release();
    expect(reorder).not.toHaveBeenCalled();
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

  it("clears pointer feedback on Escape and rejects a drop after write access changes", async () => {
    await render();
    await pointerEvent(nativeRow(0), "pointerdown");
    const target = container.querySelector('[aria-label="Collapse beta"]')!.closest("li")!;
    await pointAt(target);
    await act(async () => document.dispatchEvent(new KeyboardEvent("keydown", { code: "Escape" })));
    expect(container.querySelector('[data-drop-target="thread"]')).toBeNull();
    await release();
    expect(controller.submit).not.toHaveBeenCalled();
    await pointerEvent(nativeRow(0), "pointerdown");
    await pointAt(target);
    controller = {
      ...controller,
      data: {
        ...data,
        binding: { ...data.binding, authorizationRevision: 2, permissions: ["workstreams:read"] },
      },
    };
    await render();
    await release();
    expect(controller.submit).not.toHaveBeenCalled();
    expect(reorder).not.toHaveBeenCalled();
  });

  it("leaves clicks below the ordinary sidebar threshold idle and cancels on pointercancel", async () => {
    await render();
    const target = container.querySelector('[aria-label="Collapse beta"]')!.closest("li")!;
    await pointerEvent(nativeRow(0), "pointerdown");
    await pointAt(target, 6);
    expect(container.querySelector('[data-drop-target="thread"]')).toBeNull();
    await release(6);
    expect(controller.submit).not.toHaveBeenCalled();
    await pointerEvent(nativeRow(0), "pointerdown");
    await pointAt(target);
    await pointerEvent(document, "pointercancel");
    await release();
    expect(controller.submit).not.toHaveBeenCalled();
  });

  it("ignores external drags and refuses row drags without an activated source", async () => {
    controller = { ...controller, placements: null, registrationContext: null };
    await render();
    const target = container.querySelector('[aria-label="Collapse beta"]')!.closest("li")!;
    expect((await dragEvent(target, "dragover")).defaultPrevented).toBe(false);
    await pointerEvent(nativeRow(0), "pointerdown");
    await pointAt(target);
    await release();
    expect(controller.submit).not.toHaveBeenCalled();
    expect(reorder).not.toHaveBeenCalled();
  });

  it("allows choosing an assignment outside the initial placement inventory", async () => {
    controller = {
      ...controller,
      placementInventory: { ...controller.placementInventory, coverage: "partial", identities: [] },
    };
    await render();
    expect(container.querySelector('[aria-label="Other threads"]')?.textContent).toContain(
      threads[1]!.title,
    );
    await openAssignment(1);
    expect(document.body.textContent).toContain("Assignment not checked");
    await clickMenu("beta");
    await vi.waitFor(() =>
      expect(controller.loadDetail).toHaveBeenCalledWith(
        "beta",
        expect.objectContaining({ signal: expect.any(AbortSignal) }),
      ),
    );
  });

  it("checks an unchecked row before a same-group drop instead of reordering it", async () => {
    const assignment = vi.fn(async () => undefined);
    controller = {
      ...controller,
      assignNativeThread: assignment,
      placementInventory: { ...controller.placementInventory, coverage: "partial", identities: [] },
    };
    await render();
    await pointerEvent(nativeRow(0), "pointerdown");
    const target = container.querySelector('[aria-label="Collapse alpha"]')!.closest("li")!;
    await pointAt(target);
    expect(target.getAttribute("data-drop-target")).toBe("thread");
    await release();
    expect(assignment).toHaveBeenCalledWith(
      threads[0],
      "alpha",
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    );
    expect(reorder).not.toHaveBeenCalled();
  });

  it("checks an unchecked row dropped on Other threads before removing primary membership", async () => {
    const assignment = vi.fn(async () => undefined);
    controller = {
      ...controller,
      assignNativeThread: assignment,
      placements: null,
      placementInventory: { ...controller.placementInventory, coverage: "partial", identities: [] },
    };
    await render();
    await pointerEvent(nativeRow(0), "pointerdown");
    const target = container.querySelector('[aria-label="Collapse Other threads"]')!.parentElement!;
    await pointAt(target);
    expect(target.getAttribute("data-drop-target")).toBe("thread");
    await release();
    expect(assignment).toHaveBeenCalledWith(
      threads[0],
      null,
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    );
    expect(reorder).not.toHaveBeenCalled();
  });

  it("keeps unchecked primary and removal choices enabled without asserting stale memberships", async () => {
    const assignment = vi.fn(async () => undefined);
    controller = {
      ...controller,
      assignNativeThread: assignment,
      placementInventory: { ...controller.placementInventory, coverage: "partial", identities: [] },
    };
    await render();
    await openAssignment();
    expect(document.body.textContent).toContain("Assignment not checked");
    expect(document.body.textContent).not.toContain("alpha (primary)");
    expect(document.body.textContent).not.toContain("beta (secondary)");
    await clickMenu("alpha");
    expect(assignment).toHaveBeenCalledWith(
      threads[0],
      "alpha",
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    );
    await openAssignment(1);
    await clickMenu("Unassigned (remove primary)");
    expect(assignment).toHaveBeenLastCalledWith(
      threads[1],
      null,
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    );
  });

  it("admits only one assignment while the chosen action is pending", async () => {
    let complete!: () => void;
    const assignment = vi.fn(
      () =>
        new Promise<void>((resolve) => {
          complete = resolve;
        }),
    );
    controller = { ...controller, assignNativeThread: assignment };
    await render();
    await openAssignment();
    const item = [...document.querySelectorAll<HTMLButtonElement>("button")].find(
      (entry) => !container.contains(entry) && entry.textContent === "beta (secondary)",
    )!;
    await act(async () => {
      item.click();
      item.click();
    });
    expect(assignment).toHaveBeenCalledOnce();
    expect(container.textContent).toContain("Saving Workstream change");
    await act(async () => complete());
    expect(container.textContent).not.toContain("Saving Workstream change");
  });

  it("preserves all native conversations under partial or untrusted placement coverage", async () => {
    controller = {
      ...controller,
      placements: null,
      registrationContext: null,
      placementInventory: { ...controller.placementInventory, coverage: "partial", identities: [] },
    };
    await render();
    const unassigned = container.querySelector('[aria-label="Other threads"]');
    for (const item of threads) expect(unassigned?.textContent).toContain(item.title);
    expect(container.textContent).toContain("Thread assignments are unavailable");
    await openAssignment();
    const assignment = [...document.querySelectorAll<HTMLButtonElement>("button")].find(
      (entry) => !container.contains(entry) && entry.textContent === "beta",
    );
    expect(assignment?.disabled).toBe(true);
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
    await openAssignment();
    await clickMenu("beta (secondary)");
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

  it("removes only primary membership through Unassigned and retains secondary context without a bottom strip", async () => {
    await render();
    expect(
      container.querySelector(
        '[aria-label="Drag First native conversation to a Workstream or reorder"]',
      ),
    ).toBeNull();
    expect(
      container.querySelector('[aria-label="Workstream actions for First native conversation"]'),
    ).toBeNull();
    await openAssignment();
    await clickMenu("Unassigned (remove primary)");
    await vi.waitFor(() => expect(controller.submit).toHaveBeenCalledOnce());
    expect(controller.submit).toHaveBeenCalledWith(
      expect.objectContaining({
        action: {
          operation: "remove_membership",
          workstream_id: "alpha",
          expected_version: 3,
          membership_id: "membership",
        },
      }),
    );
    expect(placements.items[1]?.kind).toBe("secondary");
    expect(reorder).not.toHaveBeenCalled();
  });

  it("refuses a native or fallback menu selection after the Workstream controls unmount", async () => {
    await render();
    await openAssignment();
    await act(async () => root.unmount());
    root = createRoot(container);
    await clickMenu("beta (secondary)");
    expect(controller.loadDetail).not.toHaveBeenCalled();
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
    await openAssignment();
    await clickMenu("beta (secondary)");
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

  it("uses zero counts when primary placement trust is unavailable, keeping visible threads unchecked", async () => {
    controller = { ...controller, placements: null, registrationContext: null };
    await render([statusThread("thread", "running")]);
    expect(
      container.querySelector('[aria-label="alpha: 0 of 0 known threads running"]')?.textContent,
    ).toBe("0/0");
    expect(container.querySelector('[aria-label="Other threads"]')?.textContent).toContain(
      "thread",
    );
    expect(container.querySelector('[aria-label="1 running thread"]')).toBeNull();
  });
});

describe("durable Workstream assignment display", () => {
  const episode = (id: string, workstreamId: string): MembershipEpisode => ({
    membership_id: id,
    workstream_id: workstreamId,
    native_reference_id: reference.native_reference_id,
    kind: "primary",
    opened: {
      at: "2026-09-30T11:00:00Z",
      actor: { principal_id: "owner" },
      command_id: "command-open-0001",
      registry_version: 1,
    },
    closed: null,
  });
  const index = (memberships: readonly MembershipEpisode[], complete = true) =>
    buildWorkstreamAssignmentIndex({
      ownerId: "owner",
      registryVersion: 11,
      references: [reference],
      memberships,
      complete,
    });
  const menuButton = (text: string) =>
    [...document.querySelectorAll<HTMLButtonElement>("button")].find(
      (entry) => !container.contains(entry) && entry.textContent?.trim() === text,
    );
  const useIndex = (
    assignmentIndex: WorkstreamAssignmentIndex,
    changes: Partial<WorkstreamListView> = {},
  ) => {
    controller = {
      ...controller,
      placements: null,
      placementInventory: { coverage: "complete", identities: [], json: "[]", totalIdentities: 0 },
      assignmentIndex,
      stale: false,
      ...changes,
    };
    return { assignmentIndex, verificationNow: new Date(now).toISOString() };
  };

  it("groups and counts a recorded member without current proof and confirms Unassigned", async () => {
    const options = useIndex(index([episode("membership", "alpha")]));
    const running = statusThread("thread", "running");
    await render([running, threads[1]!], [running, threads[1]!], options);
    expect(
      container.querySelector('[aria-label="alpha: 1 of 1 known threads running"]')?.textContent,
    ).toBe("1/1");
    expect(container.querySelector('[aria-label="Unassigned threads"]')?.textContent).toContain(
      threads[1]!.title,
    );
    expect(container.textContent).not.toContain("Thread assignments are unavailable");
    expect(container.textContent).not.toContain("Showing the last loaded Workstreams");
  });

  it("offers recorded membership choices without claiming the row is unchecked", async () => {
    const assignment = vi.fn(async () => undefined);
    const options = useIndex(index([episode("membership", "alpha")]), {
      assignNativeThread: assignment,
    });
    await render(threads, threads, options);
    await openAssignment();
    expect(document.body.textContent).not.toContain("Assignment unknown");
    expect(document.body.textContent).not.toContain("Assignment not checked");
    expect(menuButton("alpha (primary)")?.disabled).toBe(true);
    await clickMenu("beta");
    expect(assignment).toHaveBeenCalledWith(
      threads[0],
      "beta",
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    );
  });

  it("never confirms Unassigned from an incomplete index", async () => {
    const options = useIndex(index([episode("membership", "alpha")], false));
    await render(threads, threads, options);
    expect(container.querySelector('[aria-label="Assignment unknown"]')?.textContent).toContain(
      threads[1]!.title,
    );
    expect(container.querySelector('[aria-label="Unassigned threads"]')).toBeNull();
    expect(container.textContent).toContain("Some thread assignments could not be loaded");
    expect(container.querySelector('[aria-label="Collapse alpha"]')).not.toBeNull();
    await openAssignment(1);
    expect(document.body.textContent).toContain("Assignment unknown");
  });

  it("shows stale Workstreams read-only without confirming Unassigned", async () => {
    const assignment = vi.fn(async () => undefined);
    const options = useIndex(index([episode("membership", "alpha")]), {
      assignNativeThread: assignment,
      stale: true,
    });
    await render(threads, threads, options);
    expect(container.textContent).toContain(
      "Showing the last loaded Workstreams. Refresh to update.",
    );
    expect(container.querySelector('[aria-label="Assignment unknown"]')?.textContent).toContain(
      threads[1]!.title,
    );
    expect(container.querySelector('[aria-label="Workstream threads"]')?.textContent).toContain(
      threads[0]!.title,
    );
    await pointerEvent(nativeRow(0), "pointerdown");
    await pointAt(container.querySelector('[aria-label="Collapse beta"]')!.closest("li")!);
    await release();
    expect(container.querySelector('[data-drop-target="thread"]')).toBeNull();
    await openAssignment();
    expect(menuButton("beta")?.disabled).toBe(true);
    expect(menuButton("Unassigned (remove primary)")?.disabled).toBe(true);
    expect(assignment).not.toHaveBeenCalled();
  });

  it("warns about conflicting primaries and blocks every membership change", async () => {
    const assignment = vi.fn(async () => undefined);
    const options = useIndex(
      index([episode("membership", "alpha"), episode("membership-beta", "beta")]),
      { assignNativeThread: assignment },
    );
    await render(threads, threads, options);
    expect(container.querySelector('[role="alert"]')?.textContent).toContain(
      "conflicting Workstream assignments",
    );
    expect(container.querySelector('[aria-label="Assignment unknown"]')?.textContent).toContain(
      threads[0]!.title,
    );
    await pointerEvent(nativeRow(0), "pointerdown");
    const target = container.querySelector('[aria-label="Collapse beta"]')!.closest("li")!;
    await pointAt(target);
    expect(target.getAttribute("data-drop-target")).not.toBe("thread");
    await release();
    await openAssignment();
    expect(document.body.textContent).toContain("Conflicting assignments; changes blocked");
    for (const label of ["alpha", "beta", "Unassigned (remove primary)"])
      expect(menuButton(label)?.disabled, label).toBe(true);
    expect(assignment).not.toHaveBeenCalled();
    expect(controller.submit).not.toHaveBeenCalled();
  });
});
