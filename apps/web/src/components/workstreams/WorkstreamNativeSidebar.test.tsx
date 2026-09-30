// @vitest-environment jsdom

import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import type { WorkstreamCommand, WorkstreamReceipt } from "@t3tools/contracts";
import type { WorkstreamDetailView, WorkstreamListView } from "../../state/workstreams";
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

async function render() {
  const grouping = groupNativeThreadsByWorkstream({
    workstreams: controller.data?.items ?? [],
    placements: controller.placements?.items ?? [],
    threads,
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
        grouping={grouping}
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
  it("drags the native row into another Workstream with compatible target feedback", async () => {
    await render();
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
    const target = container.querySelector('[aria-label="Collapse Unassigned"]')!.parentElement!;
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
