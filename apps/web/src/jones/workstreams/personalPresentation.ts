import { effectiveSnoozed } from "@t3tools/client-runtime/state/thread-settled";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import {
  groupPersonalThreads,
  type PersonalWorkstreamsState,
  personalThreadKey,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { resolveSidebarThreadStatus } from "../../components/Sidebar.logic";
import { classifyThreadStatus } from "../threadStatusFilter/model";

export function summarizePersonalThreads(
  threads: readonly EnvironmentThreadShell[],
  now = Date.now(),
) {
  const seen = new Set<string>();
  let running = 0;
  let attention = 0;
  let failed = 0;
  let done = 0;
  for (const thread of threads) {
    const key = personalThreadKey(thread);
    if (seen.has(key)) continue;
    seen.add(key);
    if (effectiveSnoozed(thread, { now: new Date(now).toISOString() })) continue;
    const statusThread = { ...thread, runtime: thread.runtime ?? null };
    const status = resolveSidebarThreadStatus(statusThread);
    const category = classifyThreadStatus(statusThread, "current", false, undefined, now);
    if (category === "working") running += 1;
    if (category === "done") done += 1;
    if (status === "approval" || status === "input" || status === "request") attention += 1;
    if (status === "failed" || status === "limited") failed += 1;
  }
  return { total: seen.size, running, attention, failed, done };
}

export type PersonalThreadSummary = ReturnType<typeof summarizePersonalThreads>;

export function formatPersonalThreadCount(total: number) {
  return `${total} ${total === 1 ? "thread" : "threads"}`;
}

/** Named Workstreams own their pinned and working members; the other shelves keep unassigned rows. */
export function personalSidebarRoster(
  state: PersonalWorkstreamsState,
  partition: {
    pinnedThreads: readonly EnvironmentThreadShell[];
    activeThreads: readonly EnvironmentThreadShell[];
    workingThreads: readonly EnvironmentThreadShell[];
  },
) {
  const candidates = [
    ...partition.activeThreads,
    ...partition.pinnedThreads,
    ...partition.workingThreads,
  ];
  const { workstreamIdByThreadKey } = groupPersonalThreads(state, candidates);
  const assigned = (thread: EnvironmentThreadShell) =>
    workstreamIdByThreadKey.has(personalThreadKey(thread));
  const activeKeys = new Set(partition.activeThreads.map(personalThreadKey));
  return {
    pinnedThreads: partition.pinnedThreads.filter((thread) => !assigned(thread)),
    activeThreads: candidates.filter(
      (thread) => assigned(thread) || activeKeys.has(personalThreadKey(thread)),
    ),
    workingThreads: partition.workingThreads.filter((thread) => !assigned(thread)),
  };
}

export function personalSidebarSummaryThreads(
  threads: readonly EnvironmentThreadShell[],
  now: string,
) {
  return threads.filter(
    (thread) => thread.settledOverride !== "settled" && !effectiveSnoozed(thread, { now }),
  );
}
