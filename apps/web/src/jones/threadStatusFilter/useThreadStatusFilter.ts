import { useState } from "react";
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
  activeThreadKey: string | null = null,
) {
  const [statusFilter, setStatusFilter] = useState<ThreadStatusFilter>("all");
  const cycleStatusFilter = () => setStatusFilter(nextThreadStatusFilter);
  const selection = useThreadStatusSelection(
    scopedThreads,
    statusFilter,
    layout,
    now,
    activeThreadKey,
  );
  return { statusFilter, cycleStatusFilter, ...selection };
}

export function useThreadStatusSelection(
  scopedThreads: ReadonlyArray<SidebarThreadSummary>,
  statusFilter: ThreadStatusFilter,
  layout: "current" | "legacy",
  now?: string,
  activeThreadKey: string | null = null,
) {
  const [retention, setRetention] = useState({
    statusFilter,
    layout,
    activeThreadKey,
    retainedThreadKey: null as string | null,
  });
  const visits = useUiStateStore((state) => state.threadLastVisitedAtById);
  const selection = (() => {
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
  })();
  const activeIsInScope =
    activeThreadKey !== null &&
    scopedThreads.some(
      (thread) =>
        scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id)) === activeThreadKey,
    );
  const activeQualifies = selection.threads.some(
    (thread) =>
      scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id)) === activeThreadKey,
  );
  const sameSelection =
    retention.statusFilter === statusFilter &&
    retention.layout === layout &&
    retention.activeThreadKey === activeThreadKey;
  const retainedThreadKey =
    (statusFilter === "done" || statusFilter === "input") &&
    activeIsInScope &&
    (activeQualifies || (sameSelection && retention.retainedThreadKey === activeThreadKey))
      ? activeThreadKey
      : null;
  if (!sameSelection || retention.retainedThreadKey !== retainedThreadKey) {
    setRetention({ statusFilter, layout, activeThreadKey, retainedThreadKey });
  }
  if (retainedThreadKey === null || activeQualifies) return selection;
  const selected = new Set(selection.threads);
  return {
    counts: selection.counts,
    threads: scopedThreads.filter(
      (thread) =>
        selected.has(thread) ||
        scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id)) === retainedThreadKey,
    ),
  };
}
