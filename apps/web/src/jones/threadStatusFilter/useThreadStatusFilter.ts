import { useClientSettings } from "../../hooks/useSettings";
import { useNowMinute } from "../../hooks/useNowMinute";
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
  threadIsInActivityWindow,
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
  const hideWorkingThreads = () => setStatusFilter("hide-working");
  const selection = useThreadStatusSelection(
    scopedThreads,
    statusFilter,
    layout,
    now,
    activeThreadKey,
  );
  return { statusFilter, cycleStatusFilter, hideWorkingThreads, ...selection };
}

export function useThreadStatusSelection(
  scopedThreads: ReadonlyArray<SidebarThreadSummary>,
  statusFilter: ThreadStatusFilter,
  layout: "current" | "legacy",
  now?: string,
  activeThreadKey: string | null = null,
) {
  const topLevelThreads = scopedThreads.filter(
    (thread) =>
      thread.lineage.parentThreadId === null &&
      thread.lineage.relationshipToParent === null &&
      thread.delegatedFrom === undefined,
  );
  const enabled = useClientSettings((settings) => settings.threadActivityWindowEnabled);
  const amount = useClientSettings((settings) => settings.threadActivityWindowAmount);
  const unit = useClientSettings((settings) => settings.threadActivityWindowUnit);
  const activityWindow = {
    threadActivityWindowEnabled: enabled,
    threadActivityWindowAmount: amount,
    threadActivityWindowUnit: unit,
  };
  useNowMinute();
  const suppliedNow = Date.parse(now ?? "");
  const currentTimeMs = new Date().getTime();
  const observedNowMs = suppliedNow > currentTimeMs ? suppliedNow : currentTimeMs;
  const windowIdentity = `${activityWindow.threadActivityWindowEnabled}:${activityWindow.threadActivityWindowAmount}:${activityWindow.threadActivityWindowUnit}`;
  const [retention, setRetention] = useState({
    windowIdentity,
    statusFilter,
    layout,
    activeThreadKey,
    retainedThreadKey: null as string | null,
  });
  const visits = useUiStateStore((state) => state.threadLastVisitedAtById);
  const selection = (() => {
    const observedNow = new Date(observedNowMs).toISOString();
    const classify = (thread: SidebarThreadSummary, recent = true) => {
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
      return classifyThreadStatus(
        { ...thread, lastVisitedAt },
        layout,
        isWoke,
        recent ? activityWindow : undefined,
        observedNowMs,
      );
    };
    const result = selectThreadStatus(topLevelThreads, statusFilter, classify);
    if (statusFilter === "all") return { ...result, threads: scopedThreads };
    return statusFilter === "hide-working"
      ? {
          counts: result.counts,
          threads: scopedThreads.filter((thread) => classify(thread, false) !== "working"),
        }
      : result;
  })();
  const activeIsInScope =
    activeThreadKey !== null &&
    topLevelThreads.some(
      (thread) =>
        scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id)) === activeThreadKey &&
        threadIsInActivityWindow(thread, activityWindow, observedNowMs),
    );
  const activeQualifies = selection.threads.some(
    (thread) =>
      scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id)) === activeThreadKey,
  );
  const sameSelection =
    retention.windowIdentity === windowIdentity &&
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
    setRetention({ windowIdentity, statusFilter, layout, activeThreadKey, retainedThreadKey });
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
