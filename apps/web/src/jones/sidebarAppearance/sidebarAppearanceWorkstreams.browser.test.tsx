import "~/index.css";
import "./sidebarAppearance.cards.css";
import "./sidebarAppearance.workstreams.css";
import { RegistryContext } from "@effect/atom-react";
import { Atom, AtomRegistry } from "effect/reactivity";
import {
  personalThreadKey,
  type PersonalWorkstreamsController,
  type PersonalWorkstreamsState,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import type { ComponentProps, CSSProperties, ReactNode } from "react";
import { describe, expect, it, vi } from "vite-plus/test";
import { act, render } from "../../../test/render";
import { NonSortableThreadRow } from "../../components/sidebar/SidebarThreadRow";
import { WorkstreamSidebarSection } from "../../components/workstreams/WorkstreamSidebarSection";
import { ToastProvider } from "../../components/ui/toast";
import { makeThreadFixture } from "../../test-fixtures";
import { PersonalSidebarGroupHeader } from "../workstreams/PersonalSidebarGroupHeader";
import { PersonalWorkstreamsHeader } from "../workstreams/PersonalWorkstreamsHeader";
import { ThreadStatusFilterPill } from "../threadStatusFilter/ThreadStatusFilterPill";
import {
  personalGroupCollapsedKey,
  personalNamedGroupsHiddenKey,
} from "../workstreams/personalSidebarOrder";
import {
  personalSharedStatusFilterKey,
  personalStatusFilterKey,
} from "../workstreams/personalStatusFilter";
import {
  personalThreadExpandedKey,
  personalThreadLimitKey,
} from "../workstreams/personalThreadWindow";
import {
  personalSidebarRosterTotals,
  personalSidebarSummaryThreads,
} from "../workstreams/personalPresentation";
import { SidebarAppearanceOverride } from "./SidebarAppearanceContext";
import {
  DEFAULT_SIDEBAR_APPEARANCE,
  SIDEBAR_APPEARANCE_PRESETS,
  sidebarAppearanceScope,
  type SidebarAppearance,
} from "./model";

const original = { ...DEFAULT_SIDEBAR_APPEARANCE, ...SIDEBAR_APPEARANCE_PRESETS.original };
const summary = { total: 10, running: 3, attention: 2, done: 4, failed: 1 };
const noop = () => {};
function Scope({ value, children }: { value: SidebarAppearance | null; children: ReactNode }) {
  if (!value) return <div style={{ width: 260 }}>{children}</div>;
  const scope = sidebarAppearanceScope(value);
  return (
    <SidebarAppearanceOverride value={value}>
      <div {...scope.attributes} style={{ ...scope.variables, width: 260 }}>
        {children}
      </div>
    </SidebarAppearanceOverride>
  );
}
function Header() {
  return (
    <PersonalSidebarGroupHeader
      name={"Workstream title ".repeat(8)}
      collapsed={false}
      onToggle={noop}
      summary={summary}
      rosterTotal={12}
    />
  );
}
function query(root: ParentNode, selector: string) {
  return root.querySelector<HTMLElement>(selector)!;
}
function visibleLines(title: HTMLElement) {
  const range = document.createRange();
  const text = title.firstChild!;
  const lines = new Map<number, string>();
  for (let i = 0; i < text.textContent!.length; i++) {
    range.setStart(text, i);
    range.setEnd(text, i + 1);
    const rect = range.getBoundingClientRect();
    if (rect.width && rect.top < title.getBoundingClientRect().bottom)
      lines.set(rect.top, (lines.get(rect.top) ?? "") + text.textContent![i]);
  }
  return [...lines.values()];
}

const thread = makeThreadFixture({ title: "Card size control", branch: null, worktreePath: null });
const rowProps: ComponentProps<typeof NonSortableThreadRow> = {
  thread,
  variant: "card",
  variantAction: "settle",
  settlementSupported: false,
  snoozeSupported: false,
  pinningSupported: false,
  isPinned: false,
  dropVerb: null,
  dragOverPinned: false,
  sweepAction: null,
  snoozeWakeLabelText: null,
  wokeAt: null,
  isActive: false,
  isRouteThread: false,
  readOpensPullRequestsInRightPanel: () => false,
  jumpLabel: null,
  environmentLabel: "Synthetic environment",
  environmentMachine: "server",
  project: null,
  projectDisplayName: null,
  providerEntryByInstanceId: new Map(),
  timestampFormat: "locale",
  onThreadClick: noop,
  onThreadActivate: noop,
  onStartRename: noop,
  onRenameTitleChange: noop,
  onCommitRename: noop,
  onCancelRename: noop,
  isRenaming: false,
  renamingTitle: "",
  onContextMenu: noop,
  onSettle: noop,
  onActionSweepStart: noop,
  onUnsettle: noop,
  onSnooze: noop,
  onUnsnooze: noop,
  onUnpin: noop,
  onAcknowledgeWoke: noop,
};

describe("Workstream appearance on real components", () => {
  it("keeps Original and absent-provider rendering equal; spacing changes neither cards nor the 12px kebab", async () => {
    const ui = (value: SidebarAppearance | null) => (
      <Scope value={value}>
        <div data-workstream-group-slot className="py-1">
          <Header />
          <ul>
            <NonSortableThreadRow {...rowProps} />
          </ul>
        </div>
      </Scope>
    );
    await using screen = await render(ui(null));
    const header = () => query(screen.container, "[data-personal-group-header]");
    const card = () => query(screen.container, '[data-testid="sidebar-row-card"]');
    const before = card().getBoundingClientRect();
    const base = getComputedStyle(header()).paddingTop;
    const titleColor = getComputedStyle(query(header(), ".line-clamp-2")).color;
    await screen.rerender(ui(original));
    expect(getComputedStyle(header()).paddingTop).toBe(base);
    expect(getComputedStyle(query(header(), ".line-clamp-2")).color).toBe(titleColor);
    expect(getComputedStyle(query(header(), ".line-clamp-2")).fontWeight).toBe("600");
    expect(query(header(), "[data-personal-group-count]")).toBeNull();
    for (const [spacing, headerPadding, slotPadding] of [
      ["roomy", "6px", "6px"],
      ["tight", "3px", "2px"],
      ["default", "4px", "4px"],
    ] as const) {
      await screen.rerender(ui({ ...original, workstreamSpacing: spacing }));
      expect(getComputedStyle(header()).paddingTop).toBe(headerPadding);
      expect(
        getComputedStyle(query(screen.container, "[data-workstream-group-slot]")).paddingTop,
      ).toBe(slotPadding);
      const rect = card().getBoundingClientRect();
      expect(rect.width).toBe(before.width);
      expect(rect.height).toBe(before.height);
    }
  });

  it("changes both title and measurement probe typography and refits a narrow header", async () => {
    await using screen = await render(
      <Scope value={original}>
        <Header />
      </Scope>,
    );
    const originalColor = getComputedStyle(query(screen.container, ".line-clamp-2")).color;
    for (const [weight, expected] of [
      ["medium", "500"],
      ["regular", "400"],
      ["default", "600"],
    ] as const) {
      await screen.rerender(
        <Scope
          value={{ ...original, workstreamTitleWeight: weight, workstreamTitleBrightness: "muted" }}
        >
          <Header />
        </Scope>,
      );
      const title = query(screen.container, ".line-clamp-2");
      const probe = query(screen.container, "[data-personal-group-title].w-max");
      const pill = query(screen.container, "[data-personal-status-pill]");
      await expect.poll(() => getComputedStyle(probe).fontWeight).toBe(expected);
      expect(getComputedStyle(title).fontWeight).toBe(expected);
      expect(getComputedStyle(title).color).toBe(getComputedStyle(probe).color);
      expect(getComputedStyle(title).color).not.toBe(originalColor);
      const spacing = pill.style.getPropertyValue("--personal-status-spacing");
      pill.style.setProperty("--personal-status-spacing", "0.25");
      const minimumLines = visibleLines(title);
      pill.style.setProperty("--personal-status-spacing", spacing);
      await expect.poll(() => visibleLines(title)).toEqual(minimumLines);
      expect(minimumLines).toHaveLength(2);
      expect(title.getBoundingClientRect().right).toBeLessThanOrEqual(
        pill.getBoundingClientRect().left,
      );
      expect(
        query(screen.container, "[data-personal-group-header]").scrollWidth,
      ).toBeLessThanOrEqual(260);
    }
  });

  it("changes only named/shared pill backgrounds, preserving semantic colours and chrome", async () => {
    const ui = (value: SidebarAppearance | null) => (
      <Scope value={value}>
        <Header />
        <PersonalWorkstreamsHeader
          summary={summary}
          hidden={false}
          onHiddenChange={noop}
          onExpandAll={noop}
          onCollapseAll={noop}
          statusFilter={null}
          onStatusFilterChange={noop}
          controlsId="groups"
        />
        <ThreadStatusFilterPill
          counts={{ working: 3, input: 2, done: 4 }}
          filter="all"
          onCycle={noop}
        />
      </Scope>
    );
    await using screen = await render(ui(null));
    const pills = () => [
      ...screen.container.querySelectorAll<HTMLElement>("[data-personal-status-pill]"),
    ];
    const colors = () =>
      pills().map((pill) =>
        [...pill.querySelectorAll("button, span.shrink-0")].map(
          (node) => getComputedStyle(node).color,
        ),
      );
    const semanticColors = colors();
    const chrome = () => query(screen.container, 'button[aria-label^="3 working, 2 needs input"]');
    const chromeBackground = getComputedStyle(chrome()).backgroundColor;
    const baseBackground = getComputedStyle(pills()[0]!).backgroundColor;
    expect(baseBackground).toBe(chromeBackground);
    for (const opacity of [30, 15, 4, 0] as const) {
      await screen.rerender(ui({ ...original, pillBackgroundOpacity: opacity }));
      for (const pill of pills()) {
        const expected = document.createElement("span");
        pill.append(expected);
        expected.style.backgroundColor = `color-mix(in oklab, var(--color-muted) ${opacity}%, transparent)`;
        expect(getComputedStyle(pill).backgroundColor).toBe(
          getComputedStyle(expected).backgroundColor,
        );
        expected.remove();
        expect(getComputedStyle(pill).opacity).toBe("1");
        if (opacity === 30) expect(getComputedStyle(pill).backgroundColor).toBe(baseBackground);
      }
      expect(colors()).toEqual(semanticColors);
      expect(getComputedStyle(chrome()).backgroundColor).toBe(chromeBackground);
    }
  });
});

it("keeps the tiny failed/roster footer aligned and invariant through real section filters, windows and collapse", async () => {
  const id = "appearance-footer";
  const at = "2026-11-17T12:00:00.000Z";
  const members = Array.from({ length: 12 }, (_, index) => ({
    ...thread,
    id: `appearance-${index}` as typeof thread.id,
    snoozedAt: index >= 10 ? at : null,
    snoozedUntil: index >= 10 ? "2099-01-01T00:00:00.000Z" : null,
  }));
  const initial: PersonalWorkstreamsState = {
    workstreams: [
      {
        id,
        name: "Appearance",
        lifecycle: "active",
        color: null,
        orderKey: "n",
        createdAt: at,
        updatedAt: at,
      },
    ],
    environments: [],
    pendingThreadKeys: new Set(),
    canCreate: true,
    assignmentsByThreadKey: new Map(
      members.map((member) => [
        personalThreadKey(member),
        {
          threadId: member.id,
          workstreamId: id,
          revision: 1,
          updatedAt: at,
        },
      ]),
    ),
  };
  const keys = [
    personalGroupCollapsedKey(id),
    personalGroupCollapsedKey(null),
    personalNamedGroupsHiddenKey,
    personalSharedStatusFilterKey,
    personalStatusFilterKey(id),
    personalThreadLimitKey(id),
    personalThreadExpandedKey(id),
  ];
  const previous = new Map(keys.map((key) => [key, localStorage.getItem(key)]));
  for (const key of keys) localStorage.removeItem(key);
  const registry = AtomRegistry.make();
  const controller: PersonalWorkstreamsController = {
    state: Atom.make(initial),
    canAssign: () => true,
    create: vi.fn(),
    rename: vi.fn(),
    setColor: vi.fn(),
    setLifecycle: vi.fn(),
    reorder: vi.fn(),
    remove: vi.fn(),
    assign: vi.fn(),
    createAndAssign: vi.fn(),
  };
  const rosterTotals = personalSidebarRosterTotals(initial, members);
  const awakeMembers = personalSidebarSummaryThreads(members, at);
  expect(awakeMembers).toHaveLength(10);
  expect(rosterTotals.get(id)).toBe(12);
  const ui = (memberCount = awakeMembers.length, failed = 1, total = 12, counts = summary) => (
    <RegistryContext.Provider value={registry}>
      <ToastProvider>
        <Scope value={DEFAULT_SIDEBAR_APPEARANCE}>
          <WorkstreamSidebarSection
            controller={controller}
            summaries={new Map([[id, { ...counts, failed }]])}
            rosterTotals={new Map([[id, total]])}
            memberCounts={new Map([[id, memberCount]])}
            renderMembers={(group, window) =>
              group === id ? (
                <div data-members>
                  {Array.from(
                    {
                      length: window?.expanded
                        ? memberCount
                        : Math.min(memberCount, window?.limit ?? memberCount),
                    },
                    (_, i) => (
                      <div key={i} data-member>
                        Member {i}
                      </div>
                    ),
                  )}
                </div>
              ) : null
            }
          />
        </Scope>
      </ToastProvider>
    </RegistryContext.Provider>
  );
  try {
    await using screen = await render(ui());
    const header = () => query(screen.container, "[data-personal-group-header]");
    const footer = () => query(header(), "[data-personal-group-count]");
    const verifyFooter = (text = "1 failed·12", total = 12) => {
      expect(footer().textContent).toBe(text);
      expect(footer().getAttribute("aria-hidden")).toBe("true");
      const style = getComputedStyle(footer());
      expect(style.fontSize).toBe("10px");
      expect(parseFloat(style.lineHeight)).toBeGreaterThan(10);
      expect(style.whiteSpace).toBe("nowrap");
      const pill = query(header(), "[data-personal-status-pill]");
      expect(
        Math.abs(footer().getBoundingClientRect().right - pill.getBoundingClientRect().right),
      ).toBeLessThan(1);
      const parts = [...footer().children].map((child) => child.getBoundingClientRect());
      expect(new Set(parts.map((rect) => rect.bottom)).size).toBe(1);
      const disclosure = query(header(), "button[aria-describedby]");
      expect(
        document.getElementById(disclosure.getAttribute("aria-describedby")!)?.textContent,
      ).toContain(`${total} threads`);
    };
    verifyFooter();
    expect(screen.container.querySelectorAll("[data-member]")).toHaveLength(5);
    await act(() =>
      query(screen.container, 'button[aria-label="Show 5 more threads in Appearance"]').click(),
    );
    expect(screen.container.querySelectorAll("[data-member]")).toHaveLength(10);
    verifyFooter();
    await act(() =>
      query(screen.container, 'button[aria-label="Show only 5 threads in Appearance"]').click(),
    );
    expect(screen.container.querySelectorAll("[data-member]")).toHaveLength(5);
    verifyFooter();
    await act(() => query(header(), 'button[aria-label^="Working:"]').click());
    await screen.rerender(ui(3));
    expect(screen.container.querySelectorAll("[data-member]")).toHaveLength(3);
    verifyFooter();
    await act(() => query(header(), 'button[aria-label="Collapse Appearance"]').click());
    expect(screen.container.querySelectorAll("[data-member]")).toHaveLength(0);
    verifyFooter();
    const emptySummary = { total: 0, running: 0, attention: 0, done: 0, failed: 0 };
    await screen.rerender(ui(0, 0, 0, emptySummary));
    verifyFooter("0", 0);
    expect(footer().children).toHaveLength(1);
    const snoozedOnlyTotal = personalSidebarRosterTotals(initial, members.slice(10)).get(id)!;
    expect(personalSidebarSummaryThreads(members.slice(10), at)).toHaveLength(0);
    await screen.rerender(ui(0, 0, snoozedOnlyTotal, emptySummary));
    verifyFooter("2", 2);
  } finally {
    registry.dispose();
    for (const [key, value] of previous) {
      if (value === null) localStorage.removeItem(key);
      else localStorage.setItem(key, value);
    }
  }
});

it("keeps separate thread and section outlines in named and Unassigned groups as tint changes", async () => {
  const ui = (tintOpacity: number, cardBorder: number, groupBorder: number) => (
    <Scope value={DEFAULT_SIDEBAR_APPEARANCE}>
      <div
        data-jones-thread-board
        style={
          {
            "--jones-board-tint-opacity": `${tintOpacity}%`,
            "--jones-board-tint-brightness": "125%",
            "--jones-board-card-border-width": `${cardBorder}px`,
            "--jones-board-group-border-width": `${groupBorder}px`,
            "--jones-board-unassigned-background": "#1F2937",
          } as CSSProperties
        }
      >
        <section
          data-jones-board-section="workstream"
          style={
            {
              "--jones-board-section-accent": "#14B8A6",
              "--jones-board-section-rail": "#14B8A6",
            } as CSSProperties
          }
        >
          <ul>
            <NonSortableThreadRow {...rowProps} />
          </ul>
        </section>
        <section data-jones-board-section="unassigned">
          <ul>
            <NonSortableThreadRow {...rowProps} />
          </ul>
        </section>
      </div>
    </Scope>
  );
  await using screen = await render(ui(0, 2, 1));
  const named = query(screen.container, '[data-jones-board-section="workstream"]');
  const unassigned = query(screen.container, '[data-jones-board-section="unassigned"]');
  const outline = (element: HTMLElement) => getComputedStyle(element, "::after");
  const card = (section: HTMLElement) => query(section, "[data-jones-sidebar-card-surface]");
  const readOutlines = () =>
    [named, unassigned, card(named), card(unassigned)].map((element) => ({
      color: outline(element).borderTopColor,
      width: outline(element).borderTopWidth,
    }));
  const before = readOutlines();
  expect(before.map(({ width }) => width)).toEqual(["1px", "1px", "2px", "2px"]);
  expect(before.every(({ color }) => color !== "rgba(0, 0, 0, 0)")).toBe(true);
  expect(getComputedStyle(unassigned).backgroundColor).toBe("rgb(31, 41, 55)");
  const beforeTint = getComputedStyle(named, "::before").backgroundColor;
  await screen.rerender(ui(40, 2, 1));
  expect(getComputedStyle(named, "::before").backgroundColor).not.toBe(beforeTint);
  expect(getComputedStyle(named, "::before").filter).toBe("brightness(1.25)");
  expect(readOutlines()).toEqual(before);
  await screen.rerender(ui(40, 0, 1));
  expect(readOutlines().map(({ width }) => width)).toEqual(["1px", "1px", "0px", "0px"]);
});
