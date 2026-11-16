import type { ComponentProps } from "react";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import { DndContext, useSensor, useSensors } from "@dnd-kit/core";
import { SortableContext } from "@dnd-kit/sortable";
import { act, render } from "../../../test/render";
import { makeThreadFixture } from "../../test-fixtures";
import { SortableThreadRow } from "../../components/sidebar/SidebarThreadRow";
import { CustomSnoozeDialogHost } from "../../components/CustomSnoozeDialog";
import { SidebarPointerSensor } from "../../components/Sidebar.pointer";
import "~/index.css";
vi.mock("../../state/session", async (original) => ({
  ...(await original<typeof import("../../state/session")>()),
  useEnvironmentScope: () => true,
}));
vi.mock("../../state/entities", async (original) => ({
  ...(await original<typeof import("../../state/entities")>()),
  useThreadShell: () => null,
}));

const now = Date.parse("2026-11-16T17:00:00.000Z");

it.each(
  ["Tomorrow", "Next week", "Custom…"].flatMap((label) =>
    ["click", "movement", "keyboard"].map((gesture) => ({ label, gesture })),
  ),
)("snoozes a sortable row with $label via $gesture", async ({ label, gesture }) => {
  using _clock = { [Symbol.dispose]: () => vi.useRealTimers() };
  vi.setSystemTime(now);
  const thread = makeThreadFixture({ title: "Synthetic snooze", runtime: null });
  const snooze = vi.fn();
  const attach = vi.fn();
  const click = vi.fn();
  const noop = () => undefined;
  const props: ComponentProps<typeof SortableThreadRow> = {
    thread,
    sortableId: thread.id,
    sortableDisabled: false,
    contextDrag: false,
    variant: "card",
    variantAction: "settle",
    settlementSupported: false,
    snoozeSupported: true,
    pinningSupported: false,
    isPinned: false,
    dropVerb: null,
    dragOverPinned: false,
    sweepAction: null,
    snoozeWakeLabelText: null,
    wokeAt: null,
    isActive: false,
    isRouteThread: false,
    openPullRequestsInRightPanel: false,
    jumpLabel: null,
    currentEnvironmentId: thread.environmentId,
    environmentLabel: "Synthetic environment",
    environmentMachine: "server",
    scratchMachineLabel: null,
    project: null,
    projectDisplayName: null,
    providerEntryByInstanceId: new Map(),
    timestampFormat: "locale",
    onThreadClick: click,
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
    onSnooze: snooze,
    onUnsnooze: noop,
    onUnpin: noop,
    onAcknowledgeWoke: noop,
  };
  function Fixture() {
    const sensors = useSensors(
      useSensor(SidebarPointerSensor, {
        distance: 6,
        onAttach: attach,
        onFinish: noop,
      }),
    );
    return (
      <DndContext sensors={sensors}>
        <SortableContext items={[thread.id]}>
          <ul style={{ width: 300 }}>
            <SortableThreadRow {...props} />
          </ul>
        </SortableContext>
        <CustomSnoozeDialogHost />
      </DndContext>
    );
  }
  await using screen = await render(<Fixture />);
  await userEvent.hover(screen.getByTestId("sidebar-row-card"));
  await screen.getByRole("button", { name: "Snooze thread", exact: true }).click();
  const choice = page.getByRole("menuitem", { name: new RegExp(label) });
  if (gesture === "click") await choice.click();
  else if (gesture === "keyboard") {
    (choice.element() as HTMLElement).focus();
    await userEvent.keyboard("{Enter}");
  } else {
    const item = page.getByRole("menuitem", { name: new RegExp(label) }).element();
    await act(() =>
      item.dispatchEvent(
        new PointerEvent("pointerdown", {
          bubbles: true,
          isPrimary: true,
          button: 0,
          buttons: 1,
          pointerId: 1,
          clientX: 20,
          clientY: 20,
        }),
      ),
    );
    await act(() =>
      document.dispatchEvent(
        new PointerEvent("pointermove", {
          bubbles: true,
          isPrimary: true,
          buttons: 1,
          pointerId: 1,
          clientX: 28,
          clientY: 20,
        }),
      ),
    );
    await act(() =>
      item.dispatchEvent(
        new PointerEvent("pointerup", {
          bubbles: true,
          isPrimary: true,
          button: 0,
          pointerId: 1,
          clientX: 28,
          clientY: 20,
        }),
      ),
    );
    await act(() =>
      item.dispatchEvent(
        new MouseEvent("click", { bubbles: true, button: 0, clientX: 28, clientY: 20 }),
      ),
    );
  }
  if (label === "Custom…") {
    await expect.element(page.getByRole("dialog")).toBeVisible();
    await page.getByRole("button", { name: "Snooze", exact: true }).click();
  }
  expect(attach).not.toHaveBeenCalled();
  expect(snooze).toHaveBeenCalledOnce();
  expect(Date.parse(snooze.mock.calls[0]![1].snoozedUntil)).toBeGreaterThan(now);
  await expect
    .element(page.getByRole("menuitem", { name: new RegExp(label) }))
    .not.toBeInTheDocument();
  expect(click).not.toHaveBeenCalled();
});
