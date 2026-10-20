import type { EnvironmentId, ThreadId } from "@t3tools/contracts";

import {
  settlementEntryId,
  type SettlementArrival,
  type SettlementEntry,
  type SettlementInboxState,
} from "./model.ts";

function compareText(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}

// Deterministic metadata conflict resolution keeps the union associative as well
// as commutative. A confirmed shell observation refreshes labels after merging.
function compareEntry(a: SettlementEntry, b: SettlementEntry): number {
  return (
    Number((a.projectRemaining?.length ?? 0) > 0) - Number((b.projectRemaining?.length ?? 0) > 0) ||
    a.detectedAtMs - b.detectedAtMs ||
    compareText(a.title, b.title) ||
    Number(a.threadGone) - Number(b.threadGone) ||
    compareText(JSON.stringify(a.projectRemaining ?? []), JSON.stringify(b.projectRemaining ?? []))
  );
}

export function mergeInbox(a: SettlementInboxState, b: SettlementInboxState): SettlementInboxState {
  const entries = new Map<string, SettlementEntry>();
  for (const entry of [...a.entries, ...b.entries]) {
    const existing = entries.get(entry.id);
    if (existing === undefined || compareEntry(existing, entry) < 0) entries.set(entry.id, entry);
  }
  return {
    version: 1,
    entries: [...entries.values()].sort((left, right) => compareText(left.id, right.id)),
    acknowledged: [...new Set([...a.acknowledged, ...b.acknowledged])].sort(compareText),
  };
}

export function addArrivals(
  inbox: SettlementInboxState,
  arrivals: readonly SettlementArrival[],
  nowMs: number,
): SettlementInboxState {
  if (arrivals.length === 0) return inbox;
  return mergeInbox(inbox, {
    version: 1,
    entries: arrivals.map((arrival) => ({
      ...arrival,
      id: settlementEntryId(arrival.environmentId, arrival.threadId, arrival.settledAtMs),
      detectedAtMs: nowMs,
      threadGone: false,
    })),
    acknowledged: [],
  });
}

export function refreshThreads(
  inbox: SettlementInboxState,
  environmentId: EnvironmentId,
  titles: ReadonlyMap<ThreadId, string>,
  gone: ReadonlySet<ThreadId>,
): SettlementInboxState {
  let changed = false;
  const entries = inbox.entries.map((entry) => {
    if (entry.environmentId !== environmentId) return entry;
    const title = titles.get(entry.threadId) ?? entry.title;
    const threadGone = gone.has(entry.threadId) || !titles.has(entry.threadId);
    if (title === entry.title && threadGone === entry.threadGone) return entry;
    changed = true;
    return { ...entry, title, threadGone };
  });
  return changed ? { ...inbox, entries } : inbox;
}

export function dismiss(
  inbox: SettlementInboxState,
  shownIds: readonly string[],
): SettlementInboxState {
  return mergeInbox(inbox, { version: 1, entries: [], acknowledged: [...shownIds] });
}

export function visibleEntries(inbox: SettlementInboxState): SettlementEntry[] {
  const latest = new Map<string, SettlementEntry>();
  for (const entry of inbox.entries) {
    const key = JSON.stringify([entry.environmentId, entry.threadId]);
    const previous = latest.get(key);
    if (
      previous === undefined ||
      entry.settledAtMs > previous.settledAtMs ||
      (entry.settledAtMs === previous.settledAtMs && compareEntry(previous, entry) < 0)
    )
      latest.set(key, entry);
  }
  const acknowledged = new Set(inbox.acknowledged);
  // Select the latest before filtering acknowledgements: dismissing a re-settle
  // must never uncover an older, superseded notification for that thread.
  return [...latest.values()]
    .filter((entry) => !acknowledged.has(entry.id))
    .sort((a, b) => b.settledAtMs - a.settledAtMs || compareText(a.id, b.id));
}
