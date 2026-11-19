import { pinOrderKeyBetween } from "@t3tools/client-runtime/state/thread-sort";

import { planWritableSidebarReorder } from "./sidebarThreadDrop";

type OrderAssignment = { id: string; orderKey: string };

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
  const assignments: OrderAssignment[] = [];
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

function planTiedKeyWindow(
  order: readonly string[],
  movedIndex: number,
  keysById: ReadonlyMap<string, string | null | undefined>,
  writableKeys: ReadonlySet<string>,
): OrderAssignment[] | null {
  const keyAt = (index: number) => {
    const id = order[index];
    return id === undefined ? null : (keysById.get(id) ?? null);
  };
  const tiedKey = keyAt(movedIndex - 1);
  if (tiedKey === null || keyAt(movedIndex + 1) !== tiedKey) return null;

  const reserved = new Set(keysById.values());
  let best: OrderAssignment[] | null = null;
  // A strict interval opens only after clearing the tie on one side of the slot.
  // Try both sides, preferring the smaller window and keeping earlier rows on a tie.
  for (const direction of [1, -1]) {
    let edge = movedIndex + direction;
    while (keyAt(edge) === tiedKey && writableKeys.has(order[edge]!)) edge += direction;
    const boundary = keyAt(edge);
    const size = Math.abs(edge - movedIndex);
    if (boundary === tiedKey || (best && best.length <= size)) continue;

    const start = direction === 1 ? movedIndex : edge + 1;
    const upper = direction === 1 ? boundary : tiedKey;
    let lower = direction === 1 ? tiedKey : boundary;
    const assignments: OrderAssignment[] = [];
    for (let index = start; index < start + size; index++) {
      let key = pinOrderKeyBetween(lower, upper);
      while (key !== null && reserved.has(key)) key = pinOrderKeyBetween(key, upper);
      if (key === null) break;
      assignments.push({ id: order[index]!, orderKey: key });
      lower = key;
    }
    if (assignments.length === size) best = assignments;
  }
  return best;
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

  let assignments = planWritableSidebarReorder(next, keysById, movedId, writableKeys);
  if (assignments !== null) {
    const assignedIds = new Set(assignments.map(({ id }) => id));
    let hasAssignedBefore = false;
    for (const id of next) {
      // Active keyless rows sort first, so a frozen keyless row cannot follow a new key.
      if (hasAssignedBefore && !writableKeys.has(id) && keysById.get(id) == null) {
        assignments = null;
        break;
      }
      hasAssignedBefore ||= assignedIds.has(id);
    }
  }
  // The local split touches only the moved row and keyed siblings past the keyless prefix.
  assignments ??= planTiedKeyWindow(next, movedIndex, keysById, writableKeys);
  if (assignments === null || assignments.length < 2) return assignments;

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
