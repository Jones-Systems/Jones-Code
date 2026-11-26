import type { ComponentProps } from "react";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import { DndContext, useSensor, useSensors } from "@dnd-kit/core";
import { SortableContext } from "@dnd-kit/sortable";
import { RunId } from "@t3tools/contracts";
import { act, render } from "../../../test/render";
import { makeThreadFixture } from "../../test-fixtures";
import { NonSortableThreadRow, SortableThreadRow } from "../../components/sidebar/SidebarThreadRow";
import { SidebarPointerSensor } from "../../components/Sidebar.pointer";
import { liftPersonalThreadCard } from "../workstreams/personalThreadDrag";
import { SidebarAppearanceOverride } from "./SidebarAppearanceContext";
import {
  ORIGINAL_SIDEBAR_APPEARANCE,
  sidebarAppearanceScope,
  type SidebarAppearance,
} from "./model";
import "~/index.css";
import "./sidebarAppearance.cards.css";

const device = vi.hoisted(() => ({ label: "Laptop", visible: "Laptop", override: false }));
vi.mock("../../state/environments", async (original) => ({
  ...(await original<typeof import("../../state/environments")>()),
  useEnvironment: () => ({
    label: device.label,
    displayLabel: device.visible,
    serverConfig: { settings: { environmentIcon: device.override ? "server" : null } },
  }),
}));
vi.mock("../../state/entities", async (original) => ({
  ...(await original<typeof import("../../state/entities")>()),
  useThreadShell: () => null,
}));
vi.mock("../../state/session", async (original) => ({
  ...(await original<typeof import("../../state/session")>()),
  useEnvironmentScope: () => true,
}));
vi.mock("../../hooks/useSupportsMultiplePullRequests", () => ({
  useSupportsMultiplePullRequests: () => true,
}));
function rowProps(
  thread = makeThreadFixture({ runtime: null }),
): ComponentProps<typeof NonSortableThreadRow> {
  const noop = () => undefined;
  return {
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
    environmentLabel: "Laptop",
    environmentMachine: "laptop",
    scratchMachineLabel: null,
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
}
function Rows({
  value,
  sortable = false,
  props = rowProps(),
  onDragStart,
}: {
  value: SidebarAppearance;
  sortable?: boolean;
  props?: ComponentProps<typeof NonSortableThreadRow>;
  onDragStart?: ComponentProps<typeof DndContext>["onDragStart"];
}) {
  const scope = sidebarAppearanceScope(value);
  const sensors = useSensors(
    useSensor(SidebarPointerSensor, {
      distance: 6,
      onAttach: () => undefined,
      onFinish: () => undefined,
    }),
  );
  return (
    <SidebarAppearanceOverride value={value}>
      <div {...scope.attributes} style={scope.variables}>
        <header data-testid="header" style={{ height: "2rem", fontSize: "0.75rem" }}>
          Workstream <button style={{ width: 12, height: 12 }}>…</button>
        </header>
        <DndContext sensors={sensors} {...(onDragStart ? { onDragStart } : {})}>
          <SortableContext items={[props.thread.id]}>
            <ul style={{ width: 300 }}>
              {sortable ? (
                <SortableThreadRow
                  {...props}
                  sortableId={props.thread.id}
                  sortableDisabled={false}
                  contextDrag={false}
                />
              ) : (
                <NonSortableThreadRow {...props} />
              )}
            </ul>
          </SortableContext>
        </DndContext>
      </div>
      <p data-testid="chat" style={{ fontSize: "1rem" }}>
        Chat text
      </p>
    </SidebarAppearanceOverride>
  );
}
function deviceDefaults(): Disposable {
  const previous = { ...device };
  Object.assign(device, { label: "Laptop", visible: "Laptop", override: false });
  return { [Symbol.dispose]: () => Object.assign(device, previous) };
}

it.each([16, 20])(
  "scales only card geometry at a %spx root and preserves lifted clone geometry",
  async (rootSize) => {
    using _device = deviceDefaults();
    const previous = document.documentElement.style.fontSize;
    document.documentElement.style.fontSize = `${rootSize}px`;
    try {
      await using screen = await render(<Rows value={ORIGINAL_SIDEBAR_APPEARANCE} sortable />);
      const originalHeader = screen.container
        .querySelector("header")!
        .getBoundingClientRect().height;
      for (const scale of [1, 0.93, 0.87] as const) {
        await screen.rerender(
          <Rows value={{ ...ORIGINAL_SIDEBAR_APPEARANCE, cardScale: scale }} sortable />,
        );
        const card = screen.container.querySelector<HTMLElement>("[data-jones-sidebar-card]")!;
        const content = card.querySelector<HTMLElement>("[data-jones-sidebar-card-content]")!;
        expect(content.getBoundingClientRect().height).toBeCloseTo(4.875 * rootSize * scale, 1);
        expect(
          parseFloat(getComputedStyle(card.querySelector("[data-jones-thread-title]")!).fontSize),
        ).toBeCloseTo(0.875 * rootSize * scale, 2);
        expect(screen.container.querySelector("header")!.getBoundingClientRect().height).toBe(
          originalHeader,
        );
        expect(
          getComputedStyle(screen.container.querySelector('[data-testid="chat"]')!).fontSize,
        ).toBe(`${rootSize}px`);
        expect(screen.container.querySelector("header button")!.getBoundingClientRect().width).toBe(
          12,
        );
        const before = card.getBoundingClientRect();
        const lifted = liftPersonalThreadCard(card, { x: before.x, y: before.y });
        try {
          const ghost = document.querySelector<HTMLElement>("[data-personal-thread-drag]")!;
          expect(
            ghost.querySelector("[data-jones-sidebar-card-content]")!.getBoundingClientRect()
              .height,
          ).toBeCloseTo(content.getBoundingClientRect().height, 1);
          expect(ghost.getBoundingClientRect().width).toBeCloseTo(before.width, 1);
          lifted.move({ x: before.x, y: before.y + 30 });
          expect(ghost.getBoundingClientRect().top).toBeCloseTo(before.top + 30, 1);
        } finally {
          lifted.dispose();
        }
        expect(card.style.opacity).toBe("");
      }
      for (const variant of ["slim", "folder", "folder-multiline"] as const) {
        await screen.rerender(
          <Rows value={ORIGINAL_SIDEBAR_APPEARANCE} props={{ ...rowProps(), variant }} />,
        );
        const baseline = screen.container
          .querySelector("[data-sidebar-row]")!
          .getBoundingClientRect().height;
        await screen.rerender(
          <Rows
            value={{ ...ORIGINAL_SIDEBAR_APPEARANCE, cardScale: 0.87 }}
            props={{ ...rowProps(), variant }}
          />,
        );
        expect(
          screen.container.querySelector("[data-sidebar-row]")!.getBoundingClientRect().height,
        ).toBe(baseline);
        expect(screen.container.querySelector("[data-jones-sidebar-card]")).toBeNull();
      }
    } finally {
      document.documentElement.style.fontSize = previous;
    }
  },
);

it.each(["card", "slim", "folder", "folder-multiline"] as const)(
  "retains %s semantic colours at zero and brightens toward foreground without changing weight",
  async (variant) => {
    using _device = deviceDefaults();
    const base = makeThreadFixture({ runtime: null });
    const runtime = {
      status: "failed" as const,
      providerName: "Codex",
      providerInstanceId: base.providerInstanceId,
      activeRunId: null,
      lastError: null,
      updatedAt: "2026-11-17T12:00:00Z",
    };
    const unread = makeThreadFixture({
      runtime: null,
      lastVisitedAt: "2026-11-17T10:00:00Z",
      latestRun: {
        runId: RunId.make("appearance-run"),
        status: "completed",
        assistantMessageId: null,
        requestedAt: "2026-11-17T11:00:00Z",
        startedAt: null,
        completedAt: "2026-11-17T12:00:00Z",
      },
    });
    const states = [
      { thread: base, active: false, tone: "receded" },
      { thread: base, active: true, tone: "primary" },
      { thread: unread, active: false, tone: variant === "slim" ? "unread" : "primary" },
      {
        thread: { ...base, runtime },
        active: false,
        tone: variant === "slim" ? "idle" : "failed-read",
      },
      {
        thread: { ...base, runtime: { ...runtime, status: "running" as const } },
        active: false,
        tone: "receded",
      },
    ];
    await using screen = await render(<Rows value={ORIGINAL_SIDEBAR_APPEARANCE} />);
    for (const state of states) {
      const props = { ...rowProps(state.thread), variant, isActive: state.active };
      await screen.rerender(<Rows value={ORIGINAL_SIDEBAR_APPEARANCE} props={props} />);
      const title = () => screen.container.querySelector<HTMLElement>("[data-jones-thread-title]")!;
      expect(title().dataset.jonesThreadTitle).toBe(state.tone);
      const baseline = getComputedStyle(title()).color;
      const weight = getComputedStyle(title()).fontWeight;
      // State changes can start the existing working-row opacity transition.
      // Capture its settled baseline before comparing brightness-only updates.
      await expect
        .poll(() => screen.container.querySelector("[data-sidebar-row]")!.getAnimations().length)
        .toBe(0);
      const rowOpacity = getComputedStyle(
        screen.container.querySelector("[data-sidebar-row]")!,
      ).opacity;
      for (const boost of [50, 100]) {
        await screen.rerender(
          <Rows
            value={{ ...ORIGINAL_SIDEBAR_APPEARANCE, threadTitleBrightnessBoost: boost }}
            props={props}
          />,
        );
        expect(getComputedStyle(title()).fontWeight).toBe(weight);
        expect(
          getComputedStyle(screen.container.querySelector("[data-sidebar-row]")!).opacity,
        ).toBe(rowOpacity);
        if (boost === 100) {
          const expected = document.createElement("span");
          expected.style.color =
            "color-mix(in oklab, light-dark(var(--color-black), var(--color-white)) 100%, transparent)";
          screen.container.append(expected);
          expect(getComputedStyle(title()).color).toBe(getComputedStyle(expected).color);
          expected.remove();
        }
        expect(getComputedStyle(title()).color).not.toBe(baseline);
      }
      await userEvent.keyboard("{Tab}");
      await act(() => screen.container.querySelector<HTMLElement>("[data-sidebar-row]")!.focus());
      const probe = document.createElement("span");
      probe.style.color =
        "color-mix(in oklab, light-dark(var(--color-black), var(--color-white)) 100%, transparent)";
      screen.container.append(probe);
      expect(getComputedStyle(title()).color).toBe(getComputedStyle(probe).color);
      probe.remove();
      await act(() => screen.container.querySelector<HTMLElement>("[data-sidebar-row]")!.blur());
      await screen.rerender(<Rows value={ORIGINAL_SIDEBAR_APPEARANCE} props={props} />);
      expect(getComputedStyle(title()).color).toBe(baseline);
    }
  },
);

it.each(["Laptop", "VPS"])(
  "hides only known %s names, retains the accessible name and shows a focus/hover tooltip",
  async (label) => {
    using _device = deviceDefaults();
    device.label = label;
    device.visible = label;
    const value = { ...ORIGINAL_SIDEBAR_APPEARANCE, deviceLabel: "icon" as const };
    await using screen = await render(<Rows value={value} />);
    expect(screen.container.querySelector("[data-jones-device-label]")!.textContent).toBe("");
    const row = screen.container.querySelector<HTMLElement>("[data-sidebar-row]")!;
    expect(row.getAttribute("aria-label")).toContain(`, ${label}`);
    const spacerWidth = screen.container
      .querySelector("[data-jones-device-label]")!
      .getBoundingClientRect().width;
    await act(() => row.focus());
    // Base UI renders this popup without a tooltip role; the row carries its accessible name.
    const popup = () =>
      document.querySelector<HTMLElement>('[data-slot="tooltip-popup"][data-open]');
    await expect.poll(popup).not.toBeNull();
    await expect.element(page.elementLocator(popup()!)).toBeVisible();
    await expect.poll(() => popup()?.textContent).toContain(label);
    expect(document.activeElement).toBe(row);
    await act(() => row.blur());
    await expect.poll(popup).toBeNull();
    await userEvent.hover(screen.getByTestId("sidebar-row-card"));
    await expect.poll(popup).not.toBeNull();
    await expect.element(page.elementLocator(popup()!)).toBeVisible();
    await expect.poll(() => popup()?.textContent).toContain(label);
    expect(row.getAttribute("aria-label")).toContain(`, ${label}`);
    await screen.rerender(<Rows value={ORIGINAL_SIDEBAR_APPEARANCE} />);
    expect(
      screen.container.querySelector("[data-jones-device-label]")!.getBoundingClientRect().width,
    ).toBeCloseTo(spacerWidth, 1);
    for (const [label, visible, override] of [
      ["Mini", "Mini", false],
      ["Laptop", "Office", false],
      ["Laptop", "Laptop", true],
      ["Custom machine", "Laptop", false],
    ] as const) {
      device.label = label;
      device.visible = visible;
      device.override = override;
      await screen.rerender(<Rows value={value} props={{ ...rowProps() }} />);
      expect(screen.container.querySelector("[data-jones-device-label]")!.textContent).toBe(
        visible,
      );
    }
  },
);

it.each([0.93, 0.87] as const)(
  "the real native sorter measures the %s card on pointer activation",
  async (scale) => {
    using _device = deviceDefaults();
    const start = vi.fn();
    await using screen = await render(
      <Rows
        value={{ ...ORIGINAL_SIDEBAR_APPEARANCE, cardScale: scale }}
        sortable
        onDragStart={start}
      />,
    );
    const row = screen.container.querySelector<HTMLElement>("[data-sidebar-row]")!;
    const card = screen.container.querySelector<HTMLElement>("[data-jones-sidebar-card]")!;
    const rect = card.getBoundingClientRect();
    const pointer = (type: string, y: number) =>
      new PointerEvent(type, {
        bubbles: true,
        isPrimary: true,
        pointerType: "mouse",
        pointerId: 1,
        button: 0,
        buttons: type === "pointerup" ? 0 : 1,
        clientX: rect.left + 30,
        clientY: y,
      });
    await act(() => row.dispatchEvent(pointer("pointerdown", rect.top + 10)));
    try {
      await act(() => document.dispatchEvent(pointer("pointermove", rect.top + 30)));
      expect(start).toHaveBeenCalledOnce();
      const active = start.mock.calls[0]![0].active;
      expect(active.rect.current.initial.height).toBeCloseTo(rect.height, 1);
    } finally {
      await act(() => document.dispatchEvent(pointer("pointerup", rect.top + 30)));
    }
  },
);
