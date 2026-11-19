import { pinOrderKeyBetween } from "@t3tools/client-runtime/state/thread-sort";

import { planWritableSidebarReorder } from "./sidebarThreadDrop";

function planKeylessMemberPlacement(
  order: readonly string[],
  movedIndex: number,
  keysById: ReadonlyMap<string, string | null | undefined>,
  writableKeys: ReadonlySet<string>,
) {
  const firstKeyed = order.findIndex(
    (id, index) => index !== movedIndex && keysById.get(id) != null,
  );
  const end = firstKeyed === -1 ? order.length : firstKeyed;
  if (movedIndex >= end || order.slice(end).some((id) => keysById.get(id) == null)) {
    return undefined;
  }

  const reserved = new Set(keysById.values());
  const assignments: Array<{ id: string; orderKey: string }> = [];
  let upper = firstKeyed === -1 ? null : (keysById.get(order[firstKeyed]!) ?? null);
  // Keyless members sort first. Save the affected tail from the end so each
  // applied write preserves the relative order of the other members.
  for (let index = end - 1; index >= movedIndex; index--) {
    const id = order[index]!;
    if (!writableKeys.has(id)) return null;
    let key = pinOrderKeyBetween(null, upper);
    while (key !== null && reserved.has(key)) key = pinOrderKeyBetween(null, key);
    if (key === null) return undefined;
    assignments.push({ id, orderKey: key });
    upper = key;
  }
  return assignments;
}

export function planWorkstreamThreadReorder(input: {
  readonly activeOrder: readonly string[];
  readonly keysById: ReadonlyMap<string, string | null | undefined>;
  readonly movedId: string;
  readonly neighborId: string;
  readonly after: boolean;
  readonly writableKeys: ReadonlySet<string>;
  readonly groupIdByKey: ReadonlyMap<string, string | null | undefined>;
  readonly destinationGroupId?: string | null;
}): ReturnType<typeof planWritableSidebarReorder> {
  const { activeOrder, keysById, movedId, neighborId, after, writableKeys, groupIdByKey } = input;
  if (
    (!activeOrder.includes(movedId) && input.destinationGroupId === undefined) ||
    !activeOrder.includes(neighborId) ||
    !writableKeys.has(movedId) ||
    !writableKeys.has(neighborId)
  ) {
    return null;
  }

  const groupId =
    input.destinationGroupId === undefined
      ? (groupIdByKey.get(movedId) ?? null)
      : input.destinationGroupId;
  if ((groupIdByKey.get(neighborId) ?? null) !== groupId) return null;
  const order = activeOrder.filter(
    (key) => key === movedId || (groupIdByKey.get(key) ?? null) === groupId,
  );
  if (movedId === neighborId) return [];
  const next = order.filter((key) => key !== movedId);
  const movedIndex = next.indexOf(neighborId) + (after ? 1 : 0);
  next.splice(movedIndex, 0, movedId);
  if (next.every((key, index) => key === order[index])) return [];

  const keyless = planKeylessMemberPlacement(next, movedIndex, keysById, writableKeys);
  if (keyless !== undefined) return keyless;

  const assignments = planWritableSidebarReorder(next, keysById, movedId, writableKeys);
  if (assignments === null) return null;

  const assignedIds = new Set(assignments.map(({ id }) => id));
  let hasAssignedBefore = false;
  for (const id of next) {
    // Active keyless rows sort first, so a frozen keyless row cannot follow a new key.
    if (hasAssignedBefore && !writableKeys.has(id) && keysById.get(id) == null) return null;
    hasAssignedBefore ||= assignedIds.has(id);
  }
  if (assignments.length < 2) return assignments;

  const leftward: Array<(typeof assignments)[number]> = [];
  const rightward: typeof leftward = [];
  let moved: (typeof assignments)[number] | undefined;
  for (const assignment of assignments) {
    if (assignment.id === movedId) {
      moved = assignment;
      continue;
    }
    const previousKey = keysById.get(assignment.id);
    (previousKey != null && assignment.orderKey < previousKey ? leftward : rightward).push(
      assignment,
    );
  }

  // The fallback emits keys in final lane order. Move earlier siblings left first,
  // then later siblings right first (including keyless rows), so partial saves keep their order.
  const ordered = leftward.concat(rightward.reverse());
  if (moved) ordered.push(moved);
  return ordered;
}
