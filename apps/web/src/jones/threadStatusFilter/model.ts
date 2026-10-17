import {
  hasUnseenCompletion,
  resolveSidebarThreadStatus,
  resolveSidebarV2TopStatus,
  resolveThreadStatusPill,
} from "../../components/Sidebar.logic";

export type ThreadStatusFilter = "all" | "working" | "input" | "done";
export type ThreadStatusCounts = Record<Exclude<ThreadStatusFilter, "all">, number>;
export type StatusThread = Parameters<typeof resolveThreadStatusPill>[0]["thread"];

export function nextThreadStatusFilter(filter: ThreadStatusFilter): ThreadStatusFilter {
  switch (filter) {
    case "all":
      return "working";
    case "working":
      return "input";
    case "input":
      return "done";
    case "done":
      return "all";
  }
}

export function classifyThreadStatus(
  thread: StatusThread,
  layout: "current" | "legacy",
  isWoke = false,
): Exclude<ThreadStatusFilter, "all"> | null {
  const pill = resolveThreadStatusPill({ thread });
  if (layout === "legacy") {
    switch (pill?.label) {
      case "Working":
      case "Connecting":
        return "working";
      case "Pending Approval":
      case "Awaiting Input":
      case "Plan Ready":
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
  if (topStatus === "working") return "working";
  if (topStatus === "approval" || topStatus === "input") return "input";
  if (status === "ready" && pill?.label === "Plan Ready") return "input";
  return topStatus === "done" ? "done" : null;
}

export function selectThreadStatus<T>(
  scopedThreads: ReadonlyArray<T>,
  filter: ThreadStatusFilter,
  classify: (thread: T) => Exclude<ThreadStatusFilter, "all"> | null,
): { counts: ThreadStatusCounts; threads: ReadonlyArray<T> } {
  const counts: ThreadStatusCounts = { working: 0, input: 0, done: 0 };
  const selected: T[] = [];
  for (const thread of scopedThreads) {
    const status = classify(thread);
    if (status !== null) counts[status] += 1;
    if (status === filter) selected.push(thread);
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
