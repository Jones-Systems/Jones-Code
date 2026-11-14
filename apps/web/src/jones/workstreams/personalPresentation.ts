import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import { personalThreadKey } from "@t3tools/client-runtime/jones/workstreams/personal";
import { resolveSidebarThreadStatus } from "../../components/Sidebar.logic";

export function summarizePersonalThreads(threads: readonly EnvironmentThreadShell[]) {
  const seen = new Set<string>();
  let running = 0;
  let attention = 0;
  let failed = 0;
  for (const thread of threads) {
    const key = personalThreadKey(thread);
    if (seen.has(key)) continue;
    seen.add(key);
    const status = resolveSidebarThreadStatus({ ...thread, runtime: thread.runtime ?? null });
    if (status === "working" || status === "preparing") running += 1;
    else if (status === "approval" || status === "input" || status === "request") attention += 1;
    else if (status === "failed" || status === "limited") failed += 1;
  }
  return { total: seen.size, running, attention, failed };
}

export type PersonalThreadSummary = ReturnType<typeof summarizePersonalThreads>;
