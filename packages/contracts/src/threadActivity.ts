import type { OrchestrationThreadShell } from "./orchestration.ts";

export type ThreadActivityInput = Pick<
  OrchestrationThreadShell,
  | "session"
  | "hasPendingApprovals"
  | "hasPendingUserInput"
  | "hasActionableProposedPlan"
  | "backgroundLiveness"
  | "interactionMode"
  | "latestTurn"
>;

export function classifyThreadActivity(thread: ThreadActivityInput) {
  const foreground = thread.hasPendingApprovals
    ? "waiting_approval"
    : thread.hasPendingUserInput
      ? "waiting_input"
      : thread.session?.status === "starting"
        ? "starting"
        : thread.session?.status === "running"
          ? "running"
          : thread.interactionMode === "plan" &&
              thread.latestTurn?.startedAt != null &&
              thread.latestTurn.completedAt !== null &&
              thread.hasActionableProposedPlan
            ? "waiting_plan"
            : "idle";
  const background = thread.backgroundLiveness ?? null;
  return {
    operating: foreground === "starting" || foreground === "running" || background !== null,
    foreground,
    background,
  } as const;
}

export function isOperatingThread(
  thread: ThreadActivityInput & Pick<OrchestrationThreadShell, "archivedAt">,
): boolean {
  return thread.archivedAt === null && classifyThreadActivity(thread).operating;
}
