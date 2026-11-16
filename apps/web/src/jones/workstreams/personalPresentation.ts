import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import { personalThreadKey } from "@t3tools/client-runtime/jones/workstreams/personal";
import { resolveSidebarThreadStatus } from "../../components/Sidebar.logic";
import { classifyThreadStatus } from "../threadStatusFilter/model";

export function summarizePersonalThreads(threads: readonly EnvironmentThreadShell[]) {
  const seen = new Set<string>();
  let running = 0;
  let attention = 0;
  let failed = 0;
  let done = 0;
  for (const thread of threads) {
    const key = personalThreadKey(thread);
    if (seen.has(key)) continue;
    seen.add(key);
    const statusThread = { ...thread, runtime: thread.runtime ?? null };
    const status = resolveSidebarThreadStatus(statusThread);
    const category = classifyThreadStatus(statusThread, "current");
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
