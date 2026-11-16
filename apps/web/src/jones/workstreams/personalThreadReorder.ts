import { planWritableSidebarReorder } from "./sidebarThreadDrop";

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
  next.splice(next.indexOf(neighborId) + (after ? 1 : 0), 0, movedId);
  if (next.every((key, index) => key === order[index])) return [];

  const assignments = planWritableSidebarReorder(next, keysById, movedId, writableKeys);
  if (assignments === null) return null;

  const assignedIds = new Set(assignments.map(({ id }) => id));
  let hasAssignedBefore = false;
  for (const id of next) {
    // Active keyless rows sort first, so a frozen keyless row cannot follow a new key.
    if (hasAssignedBefore && !writableKeys.has(id) && keysById.get(id) == null) return null;
    hasAssignedBefore ||= assignedIds.has(id);
  }
  return assignments;
}
