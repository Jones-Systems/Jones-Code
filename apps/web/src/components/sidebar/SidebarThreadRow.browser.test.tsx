import type { ComponentProps } from "react";
import { expect, it, vi } from "vite-plus/test";
import { act, render } from "../../../test/render";
import { makeThreadFixture } from "../../test-fixtures";
import type { SidebarThreadSummary } from "../../types";
import { NonSortableThreadRow } from "./SidebarThreadRow";

const state = vi.hoisted(() => ({ live: null as SidebarThreadSummary | null }));
vi.mock("@dnd-kit/sortable", async (original) => ({
  ...(await original<typeof import("@dnd-kit/sortable")>()),
  useSortable: () => {
    throw new Error("Workstream rows must not register with the native sorter");
  },
}));
vi.mock("../../state/entities", async (original) => ({
  ...(await original<typeof import("../../state/entities")>()),
  useThreadShell: () => state.live,
}));

it("renders live Workstream row updates and activation without a native sortable registration", async () => {
  state.live = null;
  const thread = makeThreadFixture({
    title: "Quiet shell title",
    branch: null,
    worktreePath: null,
  });
  const click = vi.fn();
  const noop = () => undefined;
  const props: ComponentProps<typeof NonSortableThreadRow> = {
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
    onSnooze: noop,
    onUnsnooze: noop,
    onUnpin: noop,
    onAcknowledgeWoke: noop,
  };
  try {
    await using screen = await render(
      <ul>
        <NonSortableThreadRow {...props} />
      </ul>,
    );
    expect(screen.container.textContent).toContain("Quiet shell title");
    state.live = { ...thread, title: "Latest live title" };
    await screen.rerender(
      <ul>
        <NonSortableThreadRow {...props} thread={{ ...thread }} />
      </ul>,
    );
    expect(screen.container.textContent).toContain("Latest live title");
    expect(screen.container.textContent).not.toContain("Quiet shell title");
    await act(() =>
      screen.container.querySelector<HTMLElement>('[data-testid="sidebar-row-card"]')!.click(),
    );
    expect(click).toHaveBeenCalledOnce();
    expect(click.mock.calls[0]?.[1]).toEqual({
      environmentId: thread.environmentId,
      threadId: thread.id,
    });
  } finally {
    state.live = null;
  }
});
