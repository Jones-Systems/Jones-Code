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

describe("native Workstream sidebar interactions", () => {
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
