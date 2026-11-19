import { EnvironmentId, RunId, ThreadId } from "@t3tools/contracts";
import type { ComponentProps } from "react";
import type { NonSortableThreadRow } from "../../components/sidebar/SidebarThreadRow";
import { makeThreadFixture } from "../../test-fixtures";

export type ThreadCardScenario = "read" | "unread" | "failed" | "open";

/** The title tones the sidebar card gives a thread, as its appearance browser test pins them. */
export const THREAD_CARD_SCENARIOS: ReadonlyArray<{
  readonly value: ThreadCardScenario;
  readonly label: string;
}> = [
  { value: "read", label: "Read" },
  { value: "unread", label: "Unread" },
  { value: "failed", label: "Failed" },
  { value: "open", label: "Open" },
];

const ENVIRONMENT_ID = EnvironmentId.make("design-review-environment");
const MINUTE_MS = 60_000;

type ThreadRowProps = ComponentProps<typeof NonSortableThreadRow>;

/**
 * One synthetic Workstream card. Each pane gets its own thread id so the two copies never share a
 * row key, while everything the card shows stays identical.
 */
export function syntheticThreadCard(
  scenario: ThreadCardScenario,
  pane: string,
  nowMs: number,
): ThreadRowProps {
  const ago = (minutes: number) => new Date(nowMs - minutes * MINUTE_MS).toISOString();
  const base = makeThreadFixture({
    environmentId: ENVIRONMENT_ID,
    id: ThreadId.make(`design-review-${pane}`),
    title: "Polish the settings panel spacing",
    runtime: null,
    updatedAt: ago(42),
  });
  const thread =
    scenario === "unread"
      ? {
          ...base,
          lastVisitedAt: ago(90),
          latestRun: {
            runId: RunId.make(`design-review-${pane}-run`),
            status: "completed" as const,
            assistantMessageId: null,
            requestedAt: ago(60),
            startedAt: null,
            completedAt: ago(42),
          },
        }
      : scenario === "failed"
        ? {
            ...base,
            runtime: {
              status: "failed" as const,
              providerName: "Codex",
              providerInstanceId: base.providerInstanceId,
              activeRunId: null,
              lastError: null,
              updatedAt: ago(42),
            },
          }
        : base;
  const ignore = () => undefined;
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
    isActive: scenario === "open",
    isRouteThread: scenario === "open",
    readOpensPullRequestsInRightPanel: () => false,
    jumpLabel: null,
    currentEnvironmentId: ENVIRONMENT_ID,
    environmentLabel: "Laptop",
    environmentMachine: "laptop",
    scratchMachineLabel: null,
    project: null,
    projectDisplayName: "Jones Code",
    providerEntryByInstanceId: new Map(),
    timestampFormat: "locale",
    onThreadClick: ignore,
    onThreadActivate: ignore,
    onStartRename: ignore,
    onRenameTitleChange: ignore,
    onCommitRename: ignore,
    onCancelRename: ignore,
    isRenaming: false,
    renamingTitle: "",
    onContextMenu: ignore,
    onSettle: ignore,
    onActionSweepStart: ignore,
    onUnsettle: ignore,
    onSnooze: ignore,
    onUnsnooze: ignore,
    onUnpin: ignore,
    onAcknowledgeWoke: ignore,
  };
}
