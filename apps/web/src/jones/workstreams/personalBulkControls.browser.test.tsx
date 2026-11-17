import "~/index.css";
import { RegistryContext } from "@effect/atom-react";
import {
  personalThreadKey,
  type PersonalWorkstreamsController,
  type PersonalWorkstreamsState,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { EnvironmentId, RunId, ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { Atom, AtomRegistry } from "effect/reactivity";
import { describe, expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import { act, render } from "../../../test/render";
import { WorkstreamNativeSidebar } from "../../components/workstreams/WorkstreamNativeSidebar";
import { makeThreadFixture } from "../../test-fixtures";
import { personalGroupCollapsedKey, personalNamedGroupsHiddenKey } from "./personalSidebarOrder";
import {
  personalSharedStatusFilterKey,
  personalStatusFilterKey,
  readSharedPersonalStatusChoice,
} from "./personalStatusFilter";
import { personalThreadExpandedKey, personalThreadLimitKey } from "./personalThreadWindow";

const environmentId = EnvironmentId.make("bulk-controls-env");
const alphaId = "bulk-alpha";
const betaId = "bulk-beta";
const at = "2026-11-16T12:00:00.000Z";
const completed = "2099-11-16T12:00:00.000Z";
const visited = "2099-11-16T11:00:00.000Z";

type Status = "working" | "input" | "done" | "failed";

function makeThread(id: string, status: Status) {
  const base = makeThreadFixture({
    environmentId,
    id: ThreadId.make(id),
    title: id,
    pinnedAt: null,
    archivedAt: null,
    settledAt: null,
    lastVisitedAt: visited,
    hasPendingUserInput: status === "input",
    latestRun: {
      runId: RunId.make(`run-${id}`),
      status: "completed",
      assistantMessageId: null,
      requestedAt: visited,
      startedAt: visited,
      completedAt: completed,
    },
    runtime: null,
  });
  return {
    ...base,
    source: { ...base.source, latestRunCompletedAt: DateTime.makeUnsafe(completed) },
    runtime:
      status === "working" || status === "failed"
        ? {
            status: status === "working" ? ("running" as const) : ("failed" as const),
            providerName: "Codex",
            providerInstanceId: base.providerInstanceId,
            activeRunId: null,
            lastError: status === "failed" ? "Provider failed" : null,
            updatedAt: completed,
          }
        : null,
  };
}

function fixture({ named = true }: { named?: boolean } = {}) {
  const keys = [
    personalGroupCollapsedKey(null),
    personalNamedGroupsHiddenKey,
    personalSharedStatusFilterKey,
    ...[alphaId, betaId].flatMap((id) => [
      personalGroupCollapsedKey(id),
      personalStatusFilterKey(id),
      personalThreadLimitKey(id),
      personalThreadExpandedKey(id),
    ]),
  ];
  const previous = new Map(keys.map((key) => [key, localStorage.getItem(key)]));
  for (const key of keys) localStorage.removeItem(key);
  const registry = AtomRegistry.make();
  // Alpha is long enough to need the Show all footer; Unassigned would change every total if it were counted.
  const alpha = [
    makeThread("alpha-working-1", "working"),
    makeThread("alpha-working-2", "working"),
    makeThread("alpha-input", "input"),
    makeThread("alpha-done-1", "done"),
    makeThread("alpha-done-2", "done"),
    makeThread("alpha-failed", "failed"),
  ];
  const beta = [
    makeThread("beta-working", "working"),
    makeThread("beta-done-1", "done"),
    makeThread("beta-done-2", "done"),
  ];
  const unassigned = [
    makeThread("loose-working-1", "working"),
    makeThread("loose-working-2", "working"),
    makeThread("loose-input", "input"),
    makeThread("loose-done", "done"),
  ];
  const threads = [...alpha, ...beta, ...unassigned];
  const initial: PersonalWorkstreamsState = {
    workstreams: named
      ? [
          { id: alphaId, name: "Alpha", orderKey: "n" },
          { id: betaId, name: "Beta", orderKey: "t" },
        ].map((workstream) => ({
          ...workstream,
          lifecycle: "active" as const,
          color: null,
          createdAt: at,
          updatedAt: at,
        }))
      : [],
    environments: [{ environmentId, status: "ready", confirmed: true, workstreamCount: 2 }],
    assignmentsByThreadKey: new Map(
      named
        ? [...alpha, ...beta].map((thread) => [
            personalThreadKey(thread),
            {
              threadId: thread.id,
              workstreamId: alpha.includes(thread) ? alphaId : betaId,
              revision: 1,
              updatedAt: at,
            },
          ])
        : [],
    ),
    pendingThreadKeys: new Set(),
    canCreate: true,
  };
  const unexpectedMutation = vi.fn(async () => {
    throw new Error("Bulk controls must not mutate the Workstreams controller");
  });
  const controller: PersonalWorkstreamsController = {
    state: Atom.make(initial),
    canAssign: () => true,
    create: unexpectedMutation,
    rename: unexpectedMutation,
    setColor: unexpectedMutation,
    setLifecycle: unexpectedMutation,
    reorder: unexpectedMutation,
    remove: unexpectedMutation,
    assign: unexpectedMutation,
    createAndAssign: unexpectedMutation,
  };
  return {
    registry,
    controller,
    threads,
    unexpectedMutation,
    [Symbol.dispose]() {
      registry.dispose();
      for (const [key, value] of previous) {
        if (value === null) localStorage.removeItem(key);
        else localStorage.setItem(key, value);
      }
    },
  };
}

type Fixture = ReturnType<typeof fixture>;

function Sidebar({
  setup,
  rows = setup.threads,
  width = 320,
}: {
  setup: Fixture;
  // Rows can be a status-filtered subset; summaries always come from every thread.
  rows?: Fixture["threads"];
  width?: number;
}) {
  return (
    <RegistryContext.Provider value={setup.registry}>
      <div style={{ width }}>
        <WorkstreamNativeSidebar
          controller={setup.controller}
          threads={rows}
          summaryThreads={setup.threads}
          renderThread={(thread) => <li data-bulk-thread={thread.id}>{thread.title}</li>}
          canOperate={() => true}
          canReorder={() => false}
          reorder={setup.unexpectedMutation}
        />
      </div>
    </RegistryContext.Provider>
  );
}

const header = (container: ParentNode) =>
  container.querySelector<HTMLElement>("[data-personal-workstreams-header]")!;
const control = (container: ParentNode, label: string) =>
  container.querySelector<HTMLButtonElement>(`button[aria-label="${label}"]`)!;
const pill = (container: ParentNode) =>
  container.querySelector<HTMLElement>("[data-personal-status-pill]")!;
const counter = (container: ParentNode, name: string) =>
  container.querySelector<HTMLButtonElement>(`button[aria-label^="${name}:"]`)!;
const sectionFor = (container: ParentNode, id: string | null) =>
  [...container.querySelectorAll<HTMLElement>("[data-native-workstream]")].find(
    (entry) => entry.dataset.nativeWorkstream === JSON.stringify(id),
  ) ?? null;
const rowsIn = (section: ParentNode | null) =>
  [...(section?.querySelectorAll<HTMLElement>("[data-bulk-thread]") ?? [])].map(
    (row) => row.dataset.bulkThread,
  );
const listsIn = (container: ParentNode) =>
  [...container.querySelectorAll<HTMLElement>("[data-personal-thread-list]")].map((list) =>
    list.getAttribute("aria-label"),
  );
const collapsedValues = () =>
  [alphaId, betaId, null].map((id) => localStorage.getItem(personalGroupCollapsedKey(id)));
async function clickAt(target: Element, timeStamp: number) {
  await act(() => {
    const event = new MouseEvent("click", { bubbles: true, detail: 1 });
    Object.defineProperty(event, "timeStamp", { value: timeStamp });
    target.dispatchEvent(event);
  });
}

describe("Workstreams bulk controls", () => {
  it("lays out label, plus, minus, status pill and rightmost chevron in a transparent row above the groups", async () => {
    using setup = fixture();
    await using screen = await render(<Sidebar setup={setup} />);
    const row = header(screen.container);
    const label = [...row.querySelectorAll("span")].find(
      (entry) => entry.textContent === "Workstreams",
    )!;
    const plus = control(row, "Expand all Workstreams");
    const minus = control(row, "Collapse all Workstreams");
    const chevron = control(row, "Hide all Workstreams");
    const rect = (element: Element) => element.getBoundingClientRect();
    expect(rect(label).right).toBeLessThanOrEqual(rect(plus).left);
    expect(rect(minus).left - rect(plus).right).toBeLessThanOrEqual(4);
    expect(rect(plus).right).toBeLessThanOrEqual(rect(minus).left);
    expect(rect(minus).right).toBeLessThanOrEqual(rect(pill(row)).left);
    expect(rect(pill(row)).right).toBeLessThanOrEqual(rect(chevron).left);
    const padding = parseFloat(getComputedStyle(row).paddingRight);
    expect(Math.abs(rect(chevron).right - (rect(row).right - padding))).toBeLessThan(1);
    expect(rect(row).bottom).toBeLessThanOrEqual(rect(sectionFor(screen.container, alphaId)!).top);
    expect(sectionFor(screen.container, alphaId)!.compareDocumentPosition(row)).toBe(
      Node.DOCUMENT_POSITION_PRECEDING,
    );
    // A plain row: no card, fill or border, and nothing but the label, three icon controls and the pill.
    const style = getComputedStyle(row);
    expect(style.backgroundColor).toBe("rgba(0, 0, 0, 0)");
    expect(style.borderTopWidth).toBe("0px");
    expect(row.querySelectorAll("svg")).toHaveLength(3);
    expect(row.textContent).toBe(`Workstreams${pill(row).textContent}`);
    expect(chevron.getAttribute("aria-expanded")).toBe("true");
  });

  it("totals the unfiltered counts of the named groups only", async () => {
    using setup = fixture();
    const filteredRows = setup.threads.filter((thread) => !thread.id.includes("done"));
    await using screen = await render(<Sidebar setup={setup} rows={filteredRows} />);
    const alpha = sectionFor(screen.container, alphaId)!;
    const beta = sectionFor(screen.container, betaId)!;
    expect(pill(alpha).textContent).toBe("2·1·2");
    expect(pill(beta).textContent).toBe("1·0·2");
    // Unassigned holds 2 working, 1 input and 1 done; none of it is in the total.
    expect(pill(header(screen.container)).textContent).toBe("3·1·4");
    expect(rowsIn(alpha)).not.toContain("alpha-done-1");
    expect(counter(header(screen.container), "Working").getAttribute("aria-label")).toContain(
      "Working: 3, using global status filter",
    );
  });

  it("uses the same pill as a group header", async () => {
    using setup = fixture();
    await using screen = await render(<Sidebar setup={setup} />);
    const shared = pill(header(screen.container));
    const group = pill(sectionFor(screen.container, alphaId)!);
    expect(shared.className).toBe(group.className);
    expect(shared.hasAttribute("data-workstream-header-control")).toBe(true);
    expect(shared.getAttribute("draggable")).toBe("false");
    for (const name of ["Working", "Needs input", "Done"]) {
      const a = counter(shared, name);
      const b = counter(group, name);
      expect(a.className).toBe(b.className);
      for (const property of ["color", "paddingLeft", "fontSize", "fontWeight"] as const) {
        expect(getComputedStyle(a)[property]).toBe(getComputedStyle(b)[property]);
      }
    }
    const sharedStyle = getComputedStyle(shared);
    const groupStyle = getComputedStyle(group);
    expect(shared.getBoundingClientRect().height).toBe(24);
    for (const property of ["backgroundColor", "borderRadius", "fontSize", "gap"] as const) {
      expect(sharedStyle[property]).toBe(groupStyle[property]);
    }
    expect(sharedStyle.backgroundColor).not.toBe("rgba(0, 0, 0, 0)");
  });

  it("applies one status to every named group with the group pill's click and double-click semantics", async () => {
    using setup = fixture();
    await using screen = await render(<Sidebar setup={setup} />);
    const shared = counter(header(screen.container), "Working");
    const alpha = counter(sectionFor(screen.container, alphaId)!, "Working");
    const beta = counter(sectionFor(screen.container, betaId)!, "Working");
    const unassignedHasPill = sectionFor(screen.container, null)!.querySelector(
      "[data-personal-status-pill]",
    );
    expect(unassignedHasPill).toBeNull();
    await clickAt(shared, 1000);
    expect(shared.getAttribute("aria-pressed")).toBe("true");
    expect(shared.getAttribute("aria-label")).toContain("selected");
    expect(alpha.getAttribute("aria-label")).toContain("selected");
    expect(beta.getAttribute("aria-label")).toContain("selected");
    expect(readSharedPersonalStatusChoice()).toEqual({ filter: "working", rev: 1 });
    // A second click outside the 250 ms window clears the selected status.
    await clickAt(shared, 1500);
    expect(shared.getAttribute("aria-pressed")).toBe("false");
    expect(alpha.getAttribute("aria-label")).toContain("using global status filter");
    expect(readSharedPersonalStatusChoice()).toEqual({ filter: null, rev: 2 });
    // Two clicks within 250 ms exclude the status; a third inside the window keeps the exclusion.
    await clickAt(shared, 2000);
    await clickAt(shared, 2200);
    expect(shared.getAttribute("aria-label")).toContain("excluded");
    expect(getComputedStyle(shared).textDecorationLine).toContain("line-through");
    expect(alpha.getAttribute("aria-label")).toContain("excluded");
    expect(beta.getAttribute("aria-label")).toContain("excluded");
    expect(readSharedPersonalStatusChoice()).toEqual({ filter: "hide-working", rev: 4 });
    await clickAt(shared, 2300);
    expect(shared.getAttribute("aria-label")).toContain("excluded");
    // Dragging from the pill must not start a Workstream drag.
    const dragStart = new DragEvent("dragstart", {
      bubbles: true,
      cancelable: true,
      dataTransfer: new DataTransfer(),
    });
    await act(() => void shared.dispatchEvent(dragStart));
    expect(dragStart.defaultPrevented).toBe(true);
    expect(setup.unexpectedMutation).not.toHaveBeenCalled();
  });

  it("collapses and expands every named group while titles, counts and failures stay visible", async () => {
    using setup = fixture();
    await using screen = await render(<Sidebar setup={setup} />);
    const alpha = sectionFor(screen.container, alphaId)!;
    // Beta starts collapsed, Alpha expanded with its whole list shown.
    await userEvent.click(page.getByRole("button", { name: "Collapse Beta", exact: true }));
    await userEvent.click(
      page.getByRole("button", { name: "Show all 6 threads in Alpha", exact: true }),
    );
    expect(rowsIn(alpha)).toHaveLength(6);
    expect(localStorage.getItem(personalThreadExpandedKey(alphaId))).toBe("true");
    const loose = rowsIn(sectionFor(screen.container, null));
    expect(loose).toHaveLength(4);

    await userEvent.click(
      page.getByRole("button", { name: "Collapse all Workstreams", exact: true }),
    );
    expect(listsIn(screen.container)).toEqual(["Unassigned threads"]);
    await expect
      .element(page.getByRole("button", { name: "Expand Alpha", exact: true }))
      .toHaveAttribute("aria-expanded", "false");
    await expect
      .element(page.getByRole("button", { name: "Expand Beta", exact: true }))
      .toHaveAttribute("aria-expanded", "false");
    await expect.element(page.getByText("Alpha", { exact: true })).toBeVisible();
    await expect.element(page.getByText("Beta", { exact: true })).toBeVisible();
    await expect.element(page.getByText("6 threads", { exact: true })).toBeVisible();
    await expect.element(page.getByText("3 threads", { exact: true })).toBeVisible();
    await expect.element(page.getByText("1 failed", { exact: true })).toBeVisible();
    expect(pill(sectionFor(screen.container, alphaId)!).textContent).toBe("2·1·2");
    expect(pill(sectionFor(screen.container, betaId)!).textContent).toBe("1·0·2");
    // Collapsing returns each group's thread window to its default, as collapsing one group does.
    expect(localStorage.getItem(personalThreadExpandedKey(alphaId))).toBe("false");
    // Unassigned is not a named group and keeps its list.
    expect(rowsIn(sectionFor(screen.container, null))).toEqual(loose);
    expect(localStorage.getItem(personalGroupCollapsedKey(null))).toBeNull();

    await userEvent.click(
      page.getByRole("button", { name: "Expand all Workstreams", exact: true }),
    );
    expect(listsIn(screen.container)).toEqual([
      "Alpha threads",
      "Beta threads",
      "Unassigned threads",
    ]);
    expect(rowsIn(sectionFor(screen.container, alphaId))).toHaveLength(5);
    expect(rowsIn(sectionFor(screen.container, betaId))).toHaveLength(3);
    await expect
      .element(page.getByRole("button", { name: "Collapse Alpha", exact: true }))
      .toHaveAttribute("aria-expanded", "true");

    // Unassigned collapsed by hand stays collapsed through Expand all and Collapse all.
    await userEvent.click(page.getByRole("button", { name: "Collapse Unassigned", exact: true }));
    await userEvent.click(
      page.getByRole("button", { name: "Collapse all Workstreams", exact: true }),
    );
    await userEvent.click(
      page.getByRole("button", { name: "Expand all Workstreams", exact: true }),
    );
    expect(listsIn(screen.container)).toEqual(["Alpha threads", "Beta threads"]);
    await expect
      .element(page.getByRole("button", { name: "Expand Unassigned", exact: true }))
      .toHaveAttribute("aria-expanded", "false");
    expect(setup.unexpectedMutation).not.toHaveBeenCalled();
  });

  it("hides and shows the named groups, restoring each group's expansion exactly", async () => {
    using setup = fixture();
    await using screen = await render(<Sidebar setup={setup} />);
    await userEvent.click(page.getByRole("button", { name: "Collapse Beta", exact: true }));
    await userEvent.click(
      page.getByRole("button", { name: "Show all 6 threads in Alpha", exact: true }),
    );
    const before = {
      collapsed: collapsedValues(),
      expanded: [alphaId, betaId].map((id) => localStorage.getItem(personalThreadExpandedKey(id))),
      alphaRows: rowsIn(sectionFor(screen.container, alphaId)),
    };
    // Alpha was never toggled, so only Beta has a stored collapsed value.
    expect(before.collapsed).toEqual([null, "true", null]);
    expect(before.alphaRows).toHaveLength(6);

    const row = header(screen.container);
    const chevron = control(row, "Hide all Workstreams");
    const controls = chevron.getAttribute("aria-controls")!;
    expect(row.querySelector(`#${CSS.escape(controls)}`)).toBeNull();
    expect(screen.container.querySelector(`#${CSS.escape(controls)}`)).not.toBeNull();
    await userEvent.click(chevron);

    // Only the hidden key is written; every group's own state is untouched.
    expect(localStorage.getItem(personalNamedGroupsHiddenKey)).toBe("true");
    expect(collapsedValues()).toEqual(before.collapsed);
    expect(
      [alphaId, betaId].map((id) => localStorage.getItem(personalThreadExpandedKey(id))),
    ).toEqual(before.expanded);
    expect(sectionFor(screen.container, alphaId)).toBeNull();
    expect(sectionFor(screen.container, betaId)).toBeNull();
    // The controlled container stays in the DOM, hidden and empty, so aria-controls resolves.
    const controlled = screen.container.querySelector<HTMLElement>(`#${CSS.escape(controls)}`)!;
    expect(controlled.hidden).toBe(true);
    expect(controlled.childElementCount).toBe(0);
    // The header stays, with no eye icon, count or notice; Unassigned stays visible.
    expect(header(screen.container)).toBe(row);
    expect(row.textContent).toBe(`Workstreams${pill(row).textContent}`);
    expect(pill(row).textContent).toBe("3·1·4");
    expect(row.querySelectorAll("svg")).toHaveLength(3);
    expect(chevron.getAttribute("aria-label")).toBe("Show all Workstreams");
    expect(chevron.getAttribute("aria-expanded")).toBe("false");
    expect(control(row, "Expand all Workstreams").disabled).toBe(true);
    expect(control(row, "Collapse all Workstreams").disabled).toBe(true);
    expect(listsIn(screen.container)).toEqual(["Unassigned threads"]);
    expect(rowsIn(sectionFor(screen.container, null))).toHaveLength(4);
    expect(screen.container.textContent).not.toMatch(/hidden|Show workstreams/i);

    await screen.rerender(null);
    await screen.rerender(<Sidebar setup={setup} />);
    expect(sectionFor(screen.container, alphaId)).toBeNull();

    await act(() => control(header(screen.container), "Show all Workstreams").focus());
    await userEvent.keyboard("{Enter}");
    expect(localStorage.getItem(personalNamedGroupsHiddenKey)).toBe("false");
    expect(rowsIn(sectionFor(screen.container, alphaId))).toEqual(before.alphaRows);
    expect(rowsIn(sectionFor(screen.container, betaId))).toEqual([]);
    expect(collapsedValues()).toEqual(before.collapsed);
    await expect
      .element(page.getByRole("button", { name: "Show only 5 threads in Alpha", exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByRole("button", { name: "Expand Beta", exact: true }))
      .toHaveAttribute("aria-expanded", "false");
    expect(control(header(screen.container), "Expand all Workstreams").disabled).toBe(false);
  });

  it("offsets named member lists by exactly 8 CSS px and leaves Unassigned flush", async () => {
    using setup = fixture();
    await using screen = await render(<Sidebar setup={setup} />);
    const listIn = (id: string | null) =>
      sectionFor(screen.container, id)!.querySelector<HTMLElement>("[data-personal-thread-list]")!;
    const firstRowOffset = (id: string | null) =>
      sectionFor(screen.container, id)!
        .querySelector("[data-personal-thread]")!
        .getBoundingClientRect().left - listIn(id).getBoundingClientRect().left;
    for (const id of [alphaId, betaId]) {
      expect(getComputedStyle(listIn(id)).paddingLeft).toBe("8px");
      expect(firstRowOffset(id)).toBeCloseTo(8, 1);
    }
    expect(getComputedStyle(listIn(null)).paddingLeft).toBe("0px");
    expect(firstRowOffset(null)).toBeCloseTo(0, 1);
  });

  it("keeps the whole label at the default sidebar width by compressing the pill like a group header", async () => {
    using setup = fixture();
    // 239 px is the default sidebar's content width, where the standard pill leaves the label 10 px short.
    await using screen = await render(<Sidebar setup={setup} width={239} />);
    const row = header(screen.container);
    const label = [...row.querySelectorAll("span")].find(
      (entry) => entry.textContent === "Workstreams",
    )!;
    const shared = pill(row);
    await expect.poll(() => label.scrollWidth - label.clientWidth).toBeLessThanOrEqual(0);
    const spacing = Number(shared.style.getPropertyValue("--personal-status-spacing"));
    expect(spacing).toBeGreaterThanOrEqual(0.25);
    expect(spacing).toBeLessThan(1);
    const chevron = control(row, "Hide all Workstreams");
    const padding = parseFloat(getComputedStyle(row).paddingRight);
    expect(shared.getBoundingClientRect().right).toBeLessThanOrEqual(
      chevron.getBoundingClientRect().left,
    );
    expect(
      Math.abs(
        chevron.getBoundingClientRect().right - (row.getBoundingClientRect().right - padding),
      ),
    ).toBeLessThan(1);
  });

  it("leaves the pill at standard spacing when the row has room", async () => {
    using setup = fixture();
    await using screen = await render(<Sidebar setup={setup} />);
    const row = header(screen.container);
    const label = [...row.querySelectorAll("span")].find(
      (entry) => entry.textContent === "Workstreams",
    )!;
    expect(label.scrollWidth).toBeLessThanOrEqual(label.clientWidth);
    expect(pill(row).style.getPropertyValue("--personal-status-spacing")).toBe("1");
  });

  it("offers no bulk controls before any named group exists", async () => {
    using setup = fixture({ named: false });
    await using screen = await render(<Sidebar setup={setup} />);
    expect(header(screen.container)).toBeNull();
    expect(listsIn(screen.container)).toEqual(["Unassigned threads"]);
    expect(rowsIn(sectionFor(screen.container, null))).toHaveLength(setup.threads.length);
  });
});
