import * as DateTime from "effect/DateTime";
import type { ClientSettings } from "@t3tools/contracts/settings";
import type { SidebarThreadSummary } from "../../types";
import {
  hasUnseenCompletion,
  resolveSidebarThreadStatus,
  resolveSidebarV2TopStatus,
  resolveThreadStatusPill,
} from "../../components/Sidebar.logic";

type ThreadStatus = "working" | "input" | "done";
export type ThreadStatusFilter = "all" | ThreadStatus | "hide-working";
export type ThreadStatusCounts = Record<ThreadStatus, number>;
export type StatusThread = Parameters<typeof resolveThreadStatusPill>[0]["thread"] & {
  latestUserMessageAt?: string | null | undefined;
  source?: SidebarThreadSummary["source"] | undefined;
};
export type ThreadActivityWindow = Pick<
  ClientSettings,
  "threadActivityWindowEnabled" | "threadActivityWindowAmount" | "threadActivityWindowUnit"
>;

const unitMilliseconds = {
  minutes: 60_000,
  hours: 3_600_000,
  days: 86_400_000,
  weeks: 604_800_000,
};

export function threadActivityWindowMs(window: ThreadActivityWindow): number {
  return window.threadActivityWindowAmount * unitMilliseconds[window.threadActivityWindowUnit];
}

export function threadIsInActivityWindow(
  thread: StatusThread,
  window: ThreadActivityWindow,
  now: number,
): boolean {
  if (!window.threadActivityWindowEnabled) return true;
  // Old servers synthesize completedAt from updatedAt, which also changes for metadata edits.
  const completedAt = thread.source?.latestRunCompletedAt;
  const completedMs = completedAt == null ? NaN : DateTime.toEpochMillis(completedAt);
  const timestamps = [
    thread.latestUserMessageAt,
    thread.latestRun?.requestedAt,
    thread.latestRun?.startedAt,
  ]
    .map((value) => Date.parse(value ?? ""))
    .filter(Number.isFinite);
  if (Number.isFinite(completedMs)) timestamps.push(completedMs);
  return timestamps.length > 0 && Math.max(...timestamps) >= now - threadActivityWindowMs(window);
}

export function nextThreadStatusFilter(filter: ThreadStatusFilter): ThreadStatusFilter {
  switch (filter) {
    case "all":
      return "working";
    case "working":
      return "input";
    case "input":
      return "done";
    case "done":
    case "hide-working":
      return "all";
  }
}

export function classifyThreadStatus(
  thread: StatusThread,
  layout: "current" | "legacy",
  isWoke = false,
  activityWindow?: ThreadActivityWindow,
  now = Date.now(),
): ThreadStatus | null {
  if (activityWindow?.threadActivityWindowEnabled) {
    if (!threadIsInActivityWindow(thread, activityWindow, now)) return null;
    const status = resolveSidebarThreadStatus(thread);
    if (status === "approval" || status === "input" || status === "request") return "input";
    if (status === "failed" || status === "limited") return null;
    if (
      status === "working" ||
      status === "waiting" ||
      (thread.pendingBackgroundTasks?.length ?? 0) > 0
    )
      return "working";
    return thread.latestRun?.status === "completed" &&
      thread.source?.latestRunCompletedAt != null &&
      Number.isFinite(Date.parse(thread.latestRun.completedAt ?? ""))
      ? "done"
      : null;
  }
  const pill = resolveThreadStatusPill({ thread });
  if (layout === "legacy") {
    switch (pill?.label) {
      case "Working":
      case "Waiting":
      case "Connecting":
        return "working";
      case "Pending Approval":
      case "Awaiting Input":
      case "Guest Request":
        return "input";
      case "Completed":
        return "done";
      default:
        return null;
    }
  }
  const status = resolveSidebarThreadStatus(thread);
  const topStatus = resolveSidebarV2TopStatus({
    status,
    isUnread: hasUnseenCompletion(thread),
    isWoke,
  });
  if (topStatus === "working" || topStatus === "waiting") return "working";
  if (topStatus === "approval" || topStatus === "input") return "input";
  return topStatus === "done" ? "done" : null;
}

export function selectThreadStatus<T>(
  scopedThreads: ReadonlyArray<T>,
  filter: ThreadStatusFilter,
  classify: (thread: T) => ThreadStatus | null,
): { counts: ThreadStatusCounts; threads: ReadonlyArray<T> } {
  const counts: ThreadStatusCounts = { working: 0, input: 0, done: 0 };
  const selected: T[] = [];
  for (const thread of scopedThreads) {
    const status = classify(thread);
    if (status !== null) counts[status] += 1;
    if (filter === "hide-working" ? status !== "working" : status === filter) {
      selected.push(thread);
    }
  }
  return { counts, threads: filter === "all" ? scopedThreads : selected };
}

export function nextThreadStatusWakeAt(
  scopedThreads: ReadonlyArray<{ snoozedUntil?: string | null }>,
  now: number,
): number | null {
  let next: number | null = null;
  for (const thread of scopedThreads) {
    const wakeAt = Date.parse(thread.snoozedUntil ?? "");
    if (wakeAt > now && (next === null || wakeAt < next)) next = wakeAt;
  }
  return next;
}
