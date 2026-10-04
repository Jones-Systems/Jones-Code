import type { ProviderInteractionMode } from "./providerPolicy.ts";

/** Legacy sidebar input only; native V2 operating state requires a bound runtime observation. */
export interface ThreadActivityInput {
  readonly session: {
    readonly status: "idle" | "starting" | "running" | "ready" | "interrupted" | "stopped" | "error";
  } | null;
  readonly hasPendingApprovals: boolean;
  readonly hasPendingUserInput: boolean;
  readonly hasActionableProposedPlan: boolean;
  readonly backgroundLiveness?: "working" | "monitoring" | null | undefined;
  readonly interactionMode: ProviderInteractionMode;
  readonly latestTurn: {
    readonly startedAt: string | null;
    readonly completedAt: string | null;
  } | null;
}

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
  thread: ThreadActivityInput & { readonly archivedAt: string | null },
): boolean {
  return thread.archivedAt === null && classifyThreadActivity(thread).operating;
}
