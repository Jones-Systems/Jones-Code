import { nativeWorkstreamThreadKey } from "@t3tools/client-runtime/state/workstreams";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import type { ThreadListV2ListItem, ThreadListV2ThreadListItem } from "../threads/threadListV2";
import type { ThreadMoveDestination } from "../threads/threadOrder";

export interface MobileWorkstreamGroup {
  readonly key: string;
  readonly name: string;
  readonly color: string;
  readonly threadKeys: ReadonlySet<string>;
}

export interface MobileWorkstreamProjection {
  readonly enabled: boolean;
  readonly groups: readonly MobileWorkstreamGroup[];
  readonly collapsedKeys: ReadonlySet<string>;
  readonly secondaryLabelsByKey: ReadonlyMap<string, readonly string[]>;
  readonly selectedThreadKey?: string | null;
  readonly searching?: boolean;
}

export function projectMobileWorkstreamList(
  items: readonly ThreadListV2ListItem[],
  projection: MobileWorkstreamProjection,
): ThreadListV2ListItem[] {
  if (!projection.enabled) return [...items];
  const active: ThreadListV2ThreadListItem[] = [];
  const pinned: ThreadListV2ListItem[] = [];
  const tail: ThreadListV2ListItem[] = [];
  let nativeShelf = false;
  for (const item of items) {
    if (
      item.type === "v2-working-shelf" ||
      item.type === "v2-snoozed-shelf" ||
      item.type === "v2-settled-shelf"
    )
      nativeShelf = true;
    if (
      !nativeShelf &&
      item.type === "v2-thread" &&
      item.item.variant === "card" &&
      !item.item.snoozed
    ) {
      if (item.item.pinned) pinned.push(item);
      else active.push(item);
    } else tail.push(item);
  }
  const groupForKey = new Map<string, MobileWorkstreamGroup | null>();
  for (const group of projection.groups) {
    for (const key of group.threadKeys) {
      groupForKey.set(key, groupForKey.has(key) ? null : group);
    }
  }
  const buckets = new Map<string, ThreadListV2ThreadListItem[]>();
  const unassigned: ThreadListV2ThreadListItem[] = [];
  for (const item of active) {
    const key = nativeWorkstreamThreadKey(item.item.thread.environmentId, item.item.thread.id);
    const group = groupForKey.get(key);
    if (!group) unassigned.push(item);
    else {
      const bucket = buckets.get(group.key) ?? [];
      bucket.push(item);
      buckets.set(group.key, bucket);
    }
  }
  const result: ThreadListV2ListItem[] = [...pinned];
  const append = (group: MobileWorkstreamGroup, rows: readonly ThreadListV2ThreadListItem[]) => {
    if (rows.length === 0) return;
    const expanded = projection.searching === true || !projection.collapsedKeys.has(group.key);
    result.push({
      type: "v2-workstream",
      key: `v2-workstream:${group.key}`,
      groupKey: group.key,
      name: group.name,
      color: group.color,
      count: rows.length,
      expanded,
    });
    const visible = expanded
      ? rows
      : rows.filter(
          (item) =>
            `${item.item.thread.environmentId}:${item.item.thread.id}` ===
            projection.selectedThreadKey,
        );
    result.push(
      ...visible.map((item, index) => ({
        ...item,
        canMoveUp: item.canMoveUp && expanded && index > 0,
        canMoveDown: item.canMoveDown && expanded && index < visible.length - 1,
      })),
    );
  };
  for (const group of projection.groups) append(group, buckets.get(group.key) ?? []);
  append(
    { key: "unassigned", name: "Unassigned", color: "#737373", threadKeys: new Set() },
    unassigned,
  );
  result.push(...tail);
  return result.map((item, index) => {
    if (item.type !== "v2-thread" && item.type !== "v2-pending") return item;
    const next = result[index + 1];
    const showTrailingDivider =
      next?.type === "v2-thread" || (next?.type === "v2-pending" && !next.showPendingDivider);
    return {
      ...item,
      showTrailingDivider,
      ...(item.type === "v2-thread"
        ? {
            secondaryWorkstreamLabel: (
              projection.secondaryLabelsByKey.get(
                nativeWorkstreamThreadKey(item.item.thread.environmentId, item.item.thread.id),
              ) ?? []
            ).join(" · "),
          }
        : {}),
    };
  });
}

export function mobileWorkstreamMoveDestination(
  items: readonly ThreadListV2ListItem[],
  thread: EnvironmentThreadShell,
  direction: ThreadMoveDestination,
): ThreadMoveDestination | null {
  if (typeof direction !== "string" || thread.pinnedAt != null) return direction;
  const index = items.findIndex(
    (item) =>
      item.type === "v2-thread" &&
      item.item.thread.environmentId === thread.environmentId &&
      item.item.thread.id === thread.id,
  );
  const neighbor = items[index + (direction === "up" ? -1 : 1)];
  if (
    index < 0 ||
    neighbor?.type !== "v2-thread" ||
    neighbor.item.pinned ||
    neighbor.item.variant !== "card" ||
    neighbor.item.snoozed
  )
    return null;
  return {
    targetId: `${neighbor.item.thread.environmentId}:${neighbor.item.thread.id}`,
    placement: direction === "up" ? "before" : "after",
  };
}
