import { promptIsWaiting, type PromptAttention, type PromptBand } from "./promptAttention";
import type { PromptItem, PromptStage } from "./promptItems";

/**
 * Grouping, filtering and search over the loaded sample. All of it is client-side; the caller
 * supplies attention and labels so these functions stay pure.
 */

export type PromptGroupBy = "default" | "destination" | "queue-state" | "none";
export type PromptFilter = "all" | "needs-you" | "paused" | "waiting" | "unassigned";

export interface PromptGroup {
  readonly key: string;
  readonly label: string;
  readonly items: readonly PromptItem[];
}

export interface PromptOrganizeContext {
  readonly attention: (item: PromptItem) => PromptAttention;
  readonly destinationLabel: (item: PromptItem) => string;
  /** The item's Workstream for default Queued and Sent grouping; null when it has none. */
  readonly workstream: (
    item: PromptItem,
  ) => { readonly key: string; readonly label: string; readonly unrecognized?: boolean } | null;
  readonly queueStateLabel: (item: PromptItem) => string;
  readonly unassigned?: (item: PromptItem) => boolean;
}

export const promptGroupOptions: Readonly<Record<PromptStage, readonly PromptGroupBy[]>> = {
  pending: ["default", "destination"],
  queued: ["default", "destination", "queue-state", "none"],
  sent: ["default", "destination", "none"],
};
export const promptFilterOptions: Readonly<Record<PromptStage, readonly PromptFilter[]>> = {
  pending: ["all", "needs-you", "paused"],
  queued: ["all", "needs-you", "waiting"],
  sent: ["all", "needs-you", "unassigned"],
};

const bandOrder: readonly PromptBand[] = ["needs-you", "releasing", "paused", "waiting", "other"];
const bandLabels: Readonly<Record<PromptBand, string>> = {
  "needs-you": "Needs you",
  releasing: "Releasing",
  paused: "Paused",
  waiting: "Waiting",
  other: "Other",
};

const byNumber = (left: number | null, right: number | null, direction: 1 | -1) => {
  if (left === right) return 0;
  if (left === null) return 1;
  if (right === null) return -1;
  return (left - right) * direction;
};

function compareItems(stage: PromptStage, context: PromptOrganizeContext) {
  return (left: PromptItem, right: PromptItem) => {
    if (stage === "pending") {
      const leftBand = context.attention(left).band;
      const rightBand = context.attention(right).band;
      if (leftBand !== rightBand) return bandOrder.indexOf(leftBand) - bandOrder.indexOf(rightBand);
      // Soonest release first; items needing the person are oldest first.
      if (leftBand === "releasing") {
        const order = byNumber(
          left.draft?.remaining_ms ?? null,
          right.draft?.remaining_ms ?? null,
          1,
        );
        if (order !== 0) return order;
      }
      return byNumber(left.time, right.time, 1) || left.key.localeCompare(right.key);
    }
    if (stage === "queued") {
      const lane = (item: PromptItem) =>
        item.destinations.some((destination) => destination.queueItem?.lane === "high") ? 0 : 1;
      return (
        lane(left) - lane(right) ||
        byNumber(left.time, right.time, -1) ||
        left.key.localeCompare(right.key)
      );
    }
    return byNumber(left.time, right.time, -1) || left.key.localeCompare(right.key);
  };
}

export function sortPromptItems(
  items: readonly PromptItem[],
  stage: PromptStage,
  context: PromptOrganizeContext,
) {
  return items.toSorted(compareItems(stage, context));
}

function groupKey(
  item: PromptItem,
  stage: PromptStage,
  groupBy: PromptGroupBy,
  context: PromptOrganizeContext,
): { key: string; label: string; rank: number } {
  if (groupBy === "destination") {
    const label = context.destinationLabel(item);
    return { key: `destination:${label}`, label, rank: 0 };
  }
  if (groupBy === "queue-state") {
    const label = context.queueStateLabel(item);
    return { key: `state:${label}`, label, rank: 0 };
  }
  if (stage === "pending") {
    const band = context.attention(item).band;
    return { key: `band:${band}`, label: bandLabels[band], rank: bandOrder.indexOf(band) };
  }
  const workstream = context.workstream(item);
  return workstream
    ? {
        key: `workstream:${workstream.key}`,
        label: workstream.label,
        rank: workstream.unrecognized ? 2 : 0,
      }
    : { key: "workstream:none", label: "No workstream", rank: 1 };
}

/** Groups in a stable order: bands by priority, then labels alphabetically, empty last. */
export function groupPromptItems(
  items: readonly PromptItem[],
  stage: PromptStage,
  groupBy: PromptGroupBy,
  context: PromptOrganizeContext,
): readonly PromptGroup[] {
  const sorted = sortPromptItems(items, stage, context);
  if (groupBy === "none") return sorted.length ? [{ key: "all", label: "All", items: sorted }] : [];
  const groups = new Map<string, { label: string; rank: number; items: PromptItem[] }>();
  for (const item of sorted) {
    const { key, label, rank } = groupKey(item, stage, groupBy, context);
    const group = groups.get(key);
    if (group) group.items.push(item);
    else groups.set(key, { label, rank, items: [item] });
  }
  return [...groups.entries()]
    .toSorted(
      ([, left], [, right]) => left.rank - right.rank || left.label.localeCompare(right.label),
    )
    .map(([key, group]) => ({ key, label: group.label, items: group.items }));
}

export function filterPromptItems(
  items: readonly PromptItem[],
  filter: PromptFilter,
  context: Pick<PromptOrganizeContext, "attention" | "unassigned">,
  workstreamKey: string | null = null,
  workstreamKeys: (item: PromptItem) => readonly string[] = () => [],
) {
  return items.filter((item) => {
    if (workstreamKey !== null && !workstreamKeys(item).includes(workstreamKey)) return false;
    if (filter === "all") return true;
    if (filter === "unassigned") return context.unassigned?.(item) === true;
    const attention = context.attention(item);
    if (filter === "needs-you") return attention.needsYou;
    if (filter === "paused") return attention.band === "paused";
    return promptIsWaiting(item);
  });
}

/**
 * A stage count from one source. `+` means the sample may hold more (capped, partial); `?`
 * means part of the count is unknown (a source failed, is stale or has not reported).
 */
export interface PromptCount {
  readonly count: number;
  readonly qualifier: "" | "+" | "?";
}

/** Sums per-source counts. Null when no source has reported; unknown sources add `?`. */
export function combinePromptCounts(counts: readonly (PromptCount | null)[]): PromptCount | null {
  const known = counts.filter((count): count is PromptCount => count !== null);
  if (known.length === 0) return null;
  const total = known.reduce((sum, count) => sum + count.count, 0);
  const unknown = known.length !== counts.length || known.some((count) => count.qualifier === "?");
  return {
    count: total,
    qualifier: unknown ? "?" : known.some((count) => count.qualifier === "+") ? "+" : "",
  };
}

/** Case-insensitive substring match over text, labels and exact identifiers. */
export function searchPromptItems(
  items: readonly PromptItem[],
  query: string,
  fields: (item: PromptItem) => readonly (string | null | undefined)[],
) {
  const needle = query.trim().toLocaleLowerCase();
  if (!needle) return items;
  return items.filter((item) =>
    fields(item).some((field) => field?.toLocaleLowerCase().includes(needle)),
  );
}

/** Identifiers every item can be found by, before labels are added by the caller. */
export function promptSearchIdentifiers(item: PromptItem): readonly (string | null)[] {
  return [
    item.text.kind === "available" ? item.text.text : null,
    item.key,
    item.draft?.id ?? null,
    item.entry?.command_id ?? null,
    item.draft?.command_id ?? null,
    item.draft?.routing_target ?? null,
    ...item.destinations.flatMap((destination) => [
      destination.queueRequestId,
      destination.deliveryId,
      destination.threadId,
      destination.queueItem?.workstream_id ?? null,
      destination.handoff?.confirmation.receiptId ?? null,
    ]),
  ];
}
