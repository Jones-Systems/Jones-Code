import type { EnvironmentId, OrchestrationV2ThreadShell, ThreadId } from "@t3tools/contracts";
import { THREAD_MONITOR_GUARDED_SETTLE_CAPABILITY } from "@t3tools/contracts/jones/guardedThreadSettlement";
import {
  THREAD_MONITOR_LIMITS,
  type ThreadMonitorCounts,
  type ThreadMonitorEnvironment,
  type ThreadMonitorGroup,
  type ThreadMonitorRow,
  type ThreadMonitorStatus,
} from "@t3tools/contracts/jones/desktopThreadMonitor";
import { backgroundWorkHoldsCompletion } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import { pendingSubagentRequestsByThread } from "@t3tools/shared/jones/threadAttention";
import * as DateTime from "effect/DateTime";
import { presentThreadShell } from "../../state/models.ts";
import { effectiveSnoozed } from "../../state/threadSettled.ts";

export interface MonitorEnvironmentInput {
  readonly environment: ThreadMonitorEnvironment;
  readonly threads: ReadonlyArray<OrchestrationV2ThreadShell>;
  readonly projectLabels: ReadonlyMap<string, string>;
  readonly canOperate: boolean;
  readonly guardedSettleCapability?: string;
  readonly canQueue: boolean;
  readonly delegatedUnderParent?: ReadonlySet<string>;
  readonly delegatedAttention?: ReadonlyArray<{
    readonly parentThreadId: ThreadId;
    readonly request: "approval" | "user_input";
  }>;
  readonly pendingActionThreadIds?: ReadonlySet<ThreadId>;
}
export interface MonitorCohort {
  readonly rows: ReadonlyArray<ThreadMonitorRow>;
  readonly counts: ThreadMonitorCounts;
  readonly nextDoneExpiryMs: number | null;
}
export const monitorThreadKey = (ref: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
}): string => JSON.stringify([ref.environmentId, ref.threadId]);
export function isRecentMonitorCompletion(
  completedAt: string | null | undefined,
  nowMs: number,
): boolean {
  const completedMs = completedAt == null ? NaN : Date.parse(completedAt);
  const age = nowMs - completedMs;
  return (
    Number.isFinite(nowMs) &&
    Number.isFinite(completedMs) &&
    age >= 0 &&
    age < THREAD_MONITOR_LIMITS.doneWindowMs
  );
}
const active = (status: string | null | undefined) =>
  status != null && ["preparing", "queued", "starting", "running", "waiting"].includes(status);
const terminal = (status: string) =>
  ["completed", "interrupted", "cancelled", "rolled_back"].includes(status);
const available = (reason: string | null) => ({ available: reason === null, reason });
const labels: Record<ThreadMonitorStatus, string> = {
  approval: "Pending Approval",
  input: "Awaiting Input",
  plan: "Plan Ready",
  working: "Working",
  connecting: "Connecting",
  checkpoint: "Finishing checkpoint",
  waiting: "Waiting",
  idle: "Idle",
  failed: "Failed",
  limited: "Usage limit",
  completed: "Completed",
  interrupted: "Interrupted",
  cancelled: "Cancelled",
  rolled_back: "Rolled back",
  unknown: "Status unavailable",
};
export function buildMonitorCohort(
  inputs: ReadonlyArray<MonitorEnvironmentInput>,
  nowMs: number,
): MonitorCohort {
  const rows: ThreadMonitorRow[] = [];
  let nextDoneExpiryMs: number | null = null;
  for (const input of inputs) {
    const env = input.environment;
    const childRequests = pendingSubagentRequestsByThread(input.threads, input.delegatedAttention);
    for (const thread of input.threads) {
      if (
        thread.deletedAt !== null ||
        thread.archivedAt !== null ||
        thread.settledAt !== null ||
        thread.settledOverride === "settled" ||
        thread.lineage.relationshipToParent === "subagent" ||
        input.delegatedUnderParent?.has(`${env.environmentId}:${thread.id}`) ||
        input.delegatedUnderParent?.has(
          monitorThreadKey({ environmentId: env.environmentId, threadId: thread.id }),
        )
      )
        continue;
      const child = childRequests.get(thread.id) ?? null;
      const summary = presentThreadShell(env.environmentId, thread, child);
      if (effectiveSnoozed(summary, { now: DateTime.formatIso(DateTime.makeUnsafe(nowMs)) }))
        continue;
      const live =
        active(thread.activityRunStatus) || active(thread.status) || thread.activeRunId !== null;
      const background = backgroundWorkHoldsCompletion(thread.pendingBackgroundTasks ?? []);
      const goal = thread.goal != null && thread.goal.status !== "complete";
      const pending = thread.pendingRuntimeRequest;
      let group: ThreadMonitorGroup;
      let status: ThreadMonitorStatus;
      if ((pending !== null && pending.kind !== "auth_refresh") || child !== null) {
        group = "needsYou";
        status = (pending !== null ? pending.kind === "user_input" : child === "user_input")
          ? "input"
          : "approval";
      } else if (thread.hasActionableProposedPlan && !live) {
        group = "needsYou";
        status = "plan";
      } else if (thread.blockedOnThreadWait) {
        group = "waiting";
        status = "waiting";
      } else if (live) {
        group = "working";
        status =
          thread.status === "preparing" ||
          thread.status === "starting" ||
          thread.status === "queued"
            ? "connecting"
            : thread.status === "waiting"
              ? "checkpoint"
              : "working";
      } else if (background || goal) {
        group = "waiting";
        status = "waiting";
      } else if (thread.status === "failed") {
        group =
          thread.limitRecovery != null && thread.limitRecovery.autoResume ? "waiting" : "needsYou";
        status = thread.lastErrorClass === "usage_limit" ? "limited" : "failed";
      } else if (terminal(thread.status)) {
        group = "done";
        status = thread.status as ThreadMonitorStatus;
      } else if (
        thread.status === "idle" &&
        thread.latestRunId !== null &&
        thread.latestRunCompletedAt != null
      ) {
        group = "done";
        status = "completed";
      } else {
        group = "waiting";
        status = "idle";
      }
      const completedAt =
        thread.latestRunCompletedAt == null
          ? null
          : DateTime.formatIso(thread.latestRunCompletedAt);
      const completionTimeUnavailable =
        group === "done" &&
        (completedAt === null ||
          !Number.isFinite(Date.parse(completedAt)) ||
          Date.parse(completedAt) > nowMs);
      if (group === "done" && !isRecentMonitorCompletion(completedAt, nowMs)) {
        if (!completionTimeUnavailable) continue;
        group = "waiting";
        status = "unknown";
      }
      if (group === "done") {
        const expiry = Date.parse(completedAt!) + THREAD_MONITOR_LIMITS.doneWindowMs;
        nextDoneExpiryMs = nextDoneExpiryMs === null ? expiry : Math.min(nextDoneExpiryMs, expiry);
      }
      const unsafe =
        env.transport !== "connected"
          ? "Environment is offline"
          : env.freshness !== "live"
            ? "Waiting for a fresh snapshot"
            : !input.canOperate
              ? "Operation permission required"
              : input.pendingActionThreadIds?.has(thread.id)
                ? "An action is pending or its outcome is unknown"
                : null;
      const settleReason =
        unsafe ??
        (input.guardedSettleCapability !== THREAD_MONITOR_GUARDED_SETTLE_CAPABILITY
          ? "Server does not support safe monitor settlement"
          : pending !== null || child !== null
            ? "Resolve all pending requests first"
            : live
              ? "Thread has active or queued work"
              : thread.hasActionableProposedPlan
                ? "Thread has an actionable plan"
                : goal
                  ? "Thread has an unfinished goal"
                  : (thread.pendingBackgroundTasks?.length ?? 0) > 0 ||
                      thread.pullRequests?.some((link) => link.watch !== undefined)
                    ? "Thread has background work"
                    : null);
      rows.push({
        publisherGeneration: env.publisherGeneration,
        environmentId: env.environmentId,
        environmentEpoch: env.environmentEpoch,
        threadId: thread.id,
        title: thread.title.slice(0, 1024),
        projectLabel: (input.projectLabels.get(thread.projectId) ?? "").slice(0, 512),
        group,
        status,
        statusLabel: completionTimeUnavailable ? "Completion time unavailable" : labels[status],
        updatedAt: DateTime.formatIso(thread.updatedAt),
        completedAt,
        completionTimeUnavailable,
        pendingChildAttention: child,
        actions: {
          reply: available(
            unsafe ??
              ((live || pending !== null) && !input.canQueue
                ? "Server cannot safely queue this reply"
                : null),
          ),
          settle: available(settleReason),
          respondToApproval: available(
            unsafe ?? "Select current request to validate response capability",
          ),
          respondToUserInput: available(
            unsafe ?? "Select current request to validate response capability",
          ),
          usePlan: available(unsafe ?? "Select current plan to validate implementation context"),
          restore: available("Restore requires the exact successful settle command"),
        },
      });
    }
  }
  rows.sort(
    (a, b) =>
      Date.parse(b.updatedAt) - Date.parse(a.updatedAt) ||
      monitorThreadKey(a).localeCompare(monitorThreadKey(b)),
  );
  const counts: { needsYou: number; working: number; waiting: number; done: number } = {
    needsYou: 0,
    working: 0,
    waiting: 0,
    done: 0,
  };
  for (const row of rows) counts[row.group]++;
  return { rows, counts, nextDoneExpiryMs };
}
