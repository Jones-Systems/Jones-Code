import type { ComponentProps } from "react";
import { expect, it, vi } from "vite-plus/test";
import { act, render } from "../../../test/render";
import { makeThreadFixture } from "../../test-fixtures";
import type { SidebarThreadSummary } from "../../types";
import { NonSortableThreadRow } from "../../components/sidebar/SidebarThreadRow";

vi.mock("../../hooks/useSupportsMultiplePullRequests", () => ({
  useSupportsMultiplePullRequests: () => true,
}));

const state = vi.hoisted(() => ({
  live: null as SidebarThreadSummary | null,
  displayLabel: "Mini",
  present: true,
}));
vi.mock("../../state/environments", async (original) => ({
  ...(await original<typeof import("../../state/environments")>()),
  useEnvironment: () =>
    state.present
      ? {
          label: "Original Mac",
          displayLabel: state.displayLabel,
          serverConfig: {
            settings: { environmentIcon: null },
            environment: { label: "Original Mac" },
          },
        }
      : null,
}));
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

it("keeps the local device subtitle, propagates names and exposes attached worktree details without branch subtitle text", async () => {
  state.live = null;
  const thread = makeThreadFixture({
    title: "Quiet shell title",
    branch: "feature/attached",
    worktreePath: "/synthetic/worktree",
    pullRequests: [
      {
        host: "github.com",
        repository: "example/repo",
        number: 7,
        url: "https://github.com/example/repo/pull/7",
        source: "manual",
        linkedAt: "2026-01-01T00:00:00Z",
        stack: null,
        snapshot: null,
      },
    ],
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
    readOpensPullRequestsInRightPanel: () => false,
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
    expect(screen.container.textContent).toContain("Mini");
    expect(screen.container.textContent).not.toContain("feature/attached");
    const indicator = screen.container.querySelector<HTMLElement>(
      `[data-testid="thread-worktree-${thread.id}"]`,
    )!;
    await act(() => indicator.focus());
    expect(document.activeElement).toBe(indicator);
    const pr = screen.container.querySelector<HTMLElement>('[aria-label*="PR #7"]')!;
    expect(pr).not.toBeNull();
    expect(indicator.compareDocumentPosition(pr) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(indicator.getAttribute("aria-label")).toContain("/synthetic/worktree");
    expect(indicator.getAttribute("aria-label")).toContain("feature/attached");
    state.displayLabel = "Office";
    state.live = { ...thread, title: "Latest live title" };
    await screen.rerender(
      <ul>
        <NonSortableThreadRow {...props} thread={{ ...thread }} />
      </ul>,
    );
    expect(screen.container.textContent).toContain("Latest live title");
    expect(screen.container.textContent).toContain("Office");
    state.live = { ...thread, branch: null, worktreePath: null };
    state.displayLabel = "Mini";
    await screen.rerender(
      <ul>
        <NonSortableThreadRow {...props} />
      </ul>,
    );
    expect(
      screen.container.querySelector(`[data-testid="thread-worktree-${thread.id}"]`),
    ).toBeNull();
    expect(screen.container.textContent).toContain("Mini");

    await act(() =>
      screen.container.querySelector<HTMLElement>('[data-testid="sidebar-row-card"]')!.click(),
    );
    state.present = false;
    await screen.rerender(
      <ul>
        <NonSortableThreadRow
          {...props}
          currentEnvironmentId={null}
          environmentLabel="Unrecognized host"
          environmentMachine="cloud"
        />
      </ul>,
    );
    expect(screen.container.textContent).toContain("Unrecognized host");
    expect(
      screen.container.querySelector(`[data-testid="thread-worktree-${thread.id}"]`),
    ).toBeNull();
    expect(click).toHaveBeenCalledOnce();
    expect(click.mock.calls[0]?.[1]).toEqual({
      environmentId: thread.environmentId,
      threadId: thread.id,
    });
  } finally {
    state.live = null;
    state.present = true;
  }
});
