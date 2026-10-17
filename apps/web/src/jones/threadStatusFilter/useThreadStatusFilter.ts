import { useCallback, useMemo, useState } from "react";
import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import { threadWokeAt } from "@t3tools/client-runtime/state/thread-settled";
import { resolveThreadLastVisitedAt } from "../../components/Sidebar.logic";
import type { SidebarThreadSummary } from "../../types";
import { useUiStateStore } from "../../uiStateStore";
import {
  classifyThreadStatus,
  nextThreadStatusFilter,
  selectThreadStatus,
  type ThreadStatusFilter,
} from "./model";

export function useThreadStatusFilter(
  scopedThreads: ReadonlyArray<SidebarThreadSummary>,
  layout: "current" | "legacy",
  now?: string,
) {
  const [statusFilter, setStatusFilter] = useState<ThreadStatusFilter>("all");
  const cycleStatusFilter = useCallback(() => setStatusFilter(nextThreadStatusFilter), []);
  const selection = useThreadStatusSelection(scopedThreads, statusFilter, layout, now);
  return { statusFilter, cycleStatusFilter, ...selection };
}

export function useThreadStatusSelection(
  scopedThreads: ReadonlyArray<SidebarThreadSummary>,
  statusFilter: ThreadStatusFilter,
  layout: "current" | "legacy",
  now?: string,
) {
  const visits = useUiStateStore((state) => state.threadLastVisitedAtById);
  const selection = useMemo(() => {
    const observedNow = new Date(
      Math.max(new Date().getTime(), now ? Date.parse(now) : 0),
    ).toISOString();
    return selectThreadStatus(scopedThreads, statusFilter, (thread) => {
      const lastVisitedAt = resolveThreadLastVisitedAt(
        thread.lastVisitedAt,
        visits[scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id))],
      );
      const wokeAt = layout === "current" ? threadWokeAt(thread, { now: observedNow }) : null;
      const isWoke =
        wokeAt !== null &&
        Number.isFinite(Date.parse(wokeAt)) &&
        (!lastVisitedAt ||
          !Number.isFinite(Date.parse(lastVisitedAt)) ||
          Date.parse(lastVisitedAt) < Date.parse(wokeAt)) &&
        thread.settledOverride !== "settled";
      return classifyThreadStatus({ ...thread, lastVisitedAt }, layout, isWoke);
    });
  }, [scopedThreads, statusFilter, visits, layout, now]);
  return selection;
}
