import { EnvironmentId, type EnvironmentMachineKind } from "@t3tools/contracts";
import type { ComponentProps } from "react";
import { expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";
import { act, render } from "../../../test/render";
import { makeThreadFixture } from "../../test-fixtures";
import type { SidebarThreadSummary } from "../../types";
import { NonSortableThreadRow } from "../../components/sidebar/SidebarThreadRow";
import { EnvironmentBadgeAppearanceSettings } from "../sidebarAppearance/EnvironmentBadgeAppearanceSettings";

vi.mock("../../hooks/useSupportsMultiplePullRequests", () => ({
  useSupportsMultiplePullRequests: () => true,
}));

const state = vi.hoisted(() => ({
  live: null as SidebarThreadSummary | null,
  displayLabel: "Mini",
  present: true,
}));
const officeId = EnvironmentId.make("environment-badge-office-fixture");
const labId = EnvironmentId.make("environment-badge-lab-fixture");
const badgeEnvironments = [
  {
    environmentId: officeId,
    label: "Studio",
    displayLabel: "Studio",
    displayUrl: "https://office.example",
    serverConfig: null,
  },
  {
    environmentId: labId,
    label: "Studio",
    displayLabel: "Studio",
    displayUrl: "https://lab.example",
    serverConfig: null,
  },
];
const badgeMachineKinds = new Map<EnvironmentId, EnvironmentMachineKind>([
  [officeId, "cloud"],
  [labId, "mac-mini"],
]);
vi.mock("../../state/environments", async (original) => ({
  ...(await original<typeof import("../../state/environments")>()),
  useEnvironments: () => ({ environments: badgeEnvironments }),
  useEnvironmentMachines: () => badgeMachineKinds,
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

function rowProps(thread: SidebarThreadSummary): ComponentProps<typeof NonSortableThreadRow> {
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
    currentEnvironmentId: thread.environmentId,
    environmentLabel: "Synthetic environment",
    environmentMachine: "server",
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
  const props = { ...rowProps(thread), onThreadClick: click };
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

it("applies badge display and fill independently to same-named environments and keeps changes on remount", async () => {
  const row = (environmentId: EnvironmentId, environmentMachine: EnvironmentMachineKind) => (
    <NonSortableThreadRow
      {...rowProps(makeThreadFixture({ environmentId }))}
      environmentLabel="Studio"
      environmentMachine={environmentMachine}
    />
  );
  const board = (
    <ul>
      <li data-testid="office">
        <ul>{row(officeId, "cloud")}</ul>
      </li>
      <li data-testid="lab">
        <ul>{row(labId, "mac-mini")}</ul>
      </li>
    </ul>
  );
  const select = async (control: string, option: string) => {
    await page.getByRole("combobox", { name: control }).click();
    await page.getByRole("option", { name: option, exact: true }).click();
  };
  const badge = (name: "office" | "lab", field: "icon" | "label") =>
    page.getByTestId(name).element().querySelector<SVGSVGElement>(`[data-jones-device-${field}]`);
  state.present = false;
  try {
    {
      await using screen = await render(
        <>
          {board}
          <EnvironmentBadgeAppearanceSettings />
        </>,
      );
      expect(badge("office", "label")?.textContent).toBe("Studio");
      expect(badge("lab", "icon")?.getAttribute("fill")).toBe("currentColor");

      await select("Badge display for Studio · office.example", "Icon only");
      await select("Icon style for Studio · office.example", "Filled");
      await select("Icon style for Studio · lab.example", "Outline");
      expect(badge("office", "label")?.textContent).toBe("");
      expect(badge("office", "icon")?.style.fill).toContain("color-mix");
      expect(badge("lab", "label")?.textContent).toBe("Mini");
      expect(badge("lab", "icon")?.getAttribute("fill")).toBe("none");
      await expect
        .element(screen.getByTestId("office").getByRole("button", { name: /Studio/ }))
        .toBeVisible();
      await select("Badge display for Studio · lab.example", "Name only");
      expect(badge("lab", "icon")).toBeNull();
    }
    await using remounted = await render(
      <>
        {board}
        <EnvironmentBadgeAppearanceSettings />
      </>,
    );
    expect(badge("office", "label")?.textContent).toBe("");
    expect(badge("office", "icon")?.style.fill).toContain("color-mix");
    expect(badge("lab", "icon")).toBeNull();
    await select("Badge display for Studio · lab.example", "Icon and name");
    expect(badge("lab", "icon")?.getAttribute("fill")).toBe("none");
    await select("Badge display for Studio · office.example", "Use default");
    expect(badge("office", "label")?.textContent).toBe("Studio");
    expect(badge("office", "icon")?.style.fill).toContain("color-mix");
    await expect
      .element(remounted.getByRole("combobox", { name: /Badge display.*office/ }))
      .toHaveTextContent("Use default");
  } finally {
    state.present = true;
    for (const environmentId of [officeId, labId]) {
      localStorage.removeItem(`jones:thread-board-environment-badge:v1:${environmentId}`);
    }
  }
});
