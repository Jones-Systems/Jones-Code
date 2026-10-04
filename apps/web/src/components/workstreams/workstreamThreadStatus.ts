import { resolveSidebarThreadStatus } from "../Sidebar.logic";
import type { ThreadOperatingState } from "../../state/threads";
import {
  nativeWorkstreamThreadKey,
  type NativeWorkstreamThreadGrouping,
} from "./nativeThreadGrouping";

export interface WorkstreamThreadStatusSummary {
  readonly total: number;
  readonly running: number;
  readonly waiting: number;
  readonly failed: number;
}

export const EMPTY_WORKSTREAM_THREAD_STATUS: WorkstreamThreadStatusSummary = {
  total: 0,
  running: 0,
  waiting: 0,
  failed: 0,
};

type StatusThread = Parameters<typeof resolveSidebarThreadStatus>[0] & {
  readonly environmentId: string;
  readonly id: string;
};

export function summarizeWorkstreamThreadStatuses<T extends StatusThread>(
  grouping: Pick<NativeWorkstreamThreadGrouping<T>, "groups">,
  getOperatingState?: (thread: T) => ThreadOperatingState | undefined,
): ReadonlyMap<string, WorkstreamThreadStatusSummary> {
  const summaries = new Map<string, WorkstreamThreadStatusSummary>();
  for (const group of grouping.groups) {
    const seen = new Set<string>();
    let running = 0;
    let waiting = 0;
    let failed = 0;
    for (const thread of group.threads) {
      const key = nativeWorkstreamThreadKey(thread.environmentId, thread.id);
      if (seen.has(key)) continue;
      seen.add(key);
      const current = getOperatingState?.(thread);
      const status = resolveSidebarThreadStatus(thread, current);
      if (getOperatingState !== undefined) {
        if (current?.workstreamRunning === true) running += 1;
        if (current?.foregroundAttention !== null && current?.foregroundAttention !== undefined)
          waiting += 1;
        if (status === "failed") failed += 1;
        continue;
      }
      if (status === "working") running += 1;
      else if (status === "approval" || status === "input") waiting += 1;
      else if (status === "failed") failed += 1;
    }
    summaries.set(group.workstream.workstreamId, { total: seen.size, running, waiting, failed });
  }
  return summaries;
}
