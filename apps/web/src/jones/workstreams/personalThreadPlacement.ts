import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import {
  groupPersonalThreads,
  openPersonalWorkstreams,
  visiblePersonalWorkstreamId,
  type PersonalWorkstreamsController,
  type PersonalWorkstreamsFailure,
  type PersonalWorkstreamsState,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import { sortActiveThreadsByOrderKey } from "@t3tools/client-runtime/state/thread-sort";

import { planWorkstreamThreadReorder } from "./personalThreadReorder";

export interface PersonalThreadPlacementInput {
  readonly thread: EnvironmentThreadShell;
  readonly destinationId: string | null;
  readonly neighbor: EnvironmentThreadShell | null;
  readonly after: boolean;
  readonly state: PersonalWorkstreamsState;
  readonly savedOrder: {
    readonly state: PersonalWorkstreamsState;
    readonly readRoster: () => readonly EnvironmentThreadShell[];
  };
  readonly activeThreads: readonly EnvironmentThreadShell[];
  readonly orderRoster: readonly EnvironmentThreadShell[];
  readonly activeKeysById: ReadonlyMap<string, string | null | undefined>;
  readonly reorderableKeys: ReadonlySet<string>;
  readonly allowReorder: boolean;
  readonly controller: Pick<PersonalWorkstreamsController, "assign" | "canAssign">;
  readonly canOperate: (threads: readonly EnvironmentThreadShell[]) => boolean;
  readonly unpin?: (() => Promise<boolean>) | undefined;
  readonly reorder: (thread: EnvironmentThreadShell, orderKey: string) => Promise<boolean>;
  readonly onAssignmentFailure: (failure: PersonalWorkstreamsFailure) => void;
  readonly onPositionRejected: () => void;
  readonly onPositionFailure: (assignmentSaved: boolean, cause: unknown) => void;
}

const shellKey = (thread: EnvironmentThreadShell): string =>
  scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id));

type OrderWrites = NonNullable<ReturnType<typeof planWorkstreamThreadReorder>>;

function preservesSavedWorkstreamOrder(
  source: PersonalThreadPlacementInput["savedOrder"],
  membersByKey: ReadonlyMap<string, EnvironmentThreadShell>,
  movedId: string,
  plan: OrderWrites,
): boolean {
  const writesByWorkstream = new Map<string, OrderWrites[number][]>();
  for (const write of plan) {
    if (write.id === movedId) continue;
    const member = membersByKey.get(write.id);
    if (!member) return false;
    const workstreamId = visiblePersonalWorkstreamId(source.state, member);
    if (workstreamId === null) continue;
    const writes = writesByWorkstream.get(workstreamId);
    if (writes) writes.push(write);
    else writesByWorkstream.set(workstreamId, [write]);
  }
  if (writesByWorkstream.size === 0) return true;

  const stationary = source
    .readRoster()
    .filter((member) => member.pinnedAt == null && shellKey(member) !== movedId);
  const { groups } = groupPersonalThreads(source.state, stationary);
  for (const { workstream, threads } of groups) {
    const writes = writesByWorkstream.get(workstream.id);
    if (!writes) continue;
    const order = sortActiveThreadsByOrderKey(threads);
    const indexByKey = new Map(order.map((member, index) => [shellKey(member), index]));
    for (const { id, orderKey } of writes) {
      const index = indexByKey.get(id);
      if (index === undefined) return false;
      const updated = { ...order[index]!, activeOrderKey: orderKey };
      const previous = order[index - 1];
      const next = order[index + 1];
      // In each prefix only this row changes, so the saved order still holds if it fits its neighbors.
      if (
        (previous && sortActiveThreadsByOrderKey([previous, updated])[0] !== previous) ||
        (next && sortActiveThreadsByOrderKey([updated, next])[0] !== updated)
      )
        return false;
      order[index] = updated;
    }
  }
  return true;
}

// Assignment exceptions propagate to the caller: their membership outcome is unknown.
// Order failures report only membership confirmed by a successful assignment response.
export async function placePersonalThread(input: PersonalThreadPlacementInput): Promise<boolean> {
  const { thread, destinationId, neighbor, controller } = input;
  if (!input.canOperate(neighbor ? [thread, neighbor] : [thread])) return false;
  if (
    destinationId !== null &&
    !openPersonalWorkstreams(input.state).some((workstream) => workstream.id === destinationId)
  )
    return false;

  const movedId = shellKey(thread);
  const isVisible = (id: string) => input.activeThreads.some((member) => shellKey(member) === id);
  if (thread.pinnedAt == null && !isVisible(movedId)) return false;
  const currentGroup = visiblePersonalWorkstreamId(input.state, thread);
  const changesGroup = currentGroup !== destinationId;
  if (changesGroup && !controller.canAssign(thread)) return false;
  const confirmUnpin = thread.pinnedAt == null ? undefined : input.unpin;

  let assignments: OrderWrites = [];
  let membersByKey: ReadonlyMap<string, EnvironmentThreadShell> | undefined;
  if (neighbor !== null) {
    if (
      !input.allowReorder ||
      neighbor.pinnedAt != null ||
      (thread.pinnedAt != null && (!confirmUnpin || !input.reorderableKeys.has(movedId)))
    )
      return false;
    // Order keys outlive filters. Keep hidden active siblings in the plan.
    const roster = input.orderRoster.filter(
      (member) => member.pinnedAt == null && (!confirmUnpin || shellKey(member) !== movedId),
    );
    // Unpin preserves the active sort fields. Plan its resulting position before any writes.
    if (confirmUnpin) roster.push(thread);
    const activeMembers = sortActiveThreadsByOrderKey(roster);
    membersByKey = new Map(activeMembers.map((member) => [shellKey(member), member]));
    const grouping = groupPersonalThreads(input.state, activeMembers);
    const groupIdByKey = new Map(
      grouping.groups.flatMap((group) =>
        group.threads.map((member) => [shellKey(member), group.workstream.id] as const),
      ),
    );
    const neighborId = shellKey(neighbor);
    if (
      neighborId === movedId ||
      !isVisible(neighborId) ||
      !membersByKey.has(neighborId) ||
      (groupIdByKey.get(neighborId) ?? null) !== destinationId
    )
      return false;

    const writable = new Set(
      [...membersByKey].flatMap(([key, member]) =>
        input.reorderableKeys.has(key) && input.canOperate([member]) ? [key] : [],
      ),
    );
    if (confirmUnpin) writable.add(movedId);
    if (!writable.has(movedId) || !writable.has(neighborId)) return false;
    const plan = planWorkstreamThreadReorder({
      activeOrder: [...membersByKey.keys()],
      keysById: input.activeKeysById,
      movedId,
      neighborId,
      after: input.after,
      writableKeys: writable,
      groupIdByKey,
      destinationGroupId: destinationId,
    });
    // Unassigned can borrow woken members of a snoozed Workstream. Check their saved lane too.
    if (
      plan === null ||
      (destinationId === null &&
        !preservesSavedWorkstreamOrder(input.savedOrder, membersByKey, movedId, plan))
    ) {
      input.onPositionRejected();
      return false;
    }
    assignments = plan;
  }

  let assignmentSaved = false;
  if (changesGroup) {
    const result = await controller.assign(thread, destinationId);
    if (!result.ok) {
      input.onAssignmentFailure(result.failure);
      return false;
    }
    assignmentSaved = true;
  }
  // Active reorder rejects a pinned arrival. Wait for the unpin receipt before touching any keys.
  if (confirmUnpin && !(await confirmUnpin())) return false;
  for (const assignment of assignments) {
    try {
      const target = assignment.id === movedId ? thread : membersByKey?.get(assignment.id);
      if (!target || !input.canOperate([target]))
        throw new Error("The drop position is no longer writable. Refresh before retrying.");
      if (!(await input.reorder(target, assignment.orderKey)))
        throw new Error("The drop position could not be saved. Refresh before retrying.");
    } catch (cause) {
      input.onPositionFailure(assignmentSaved, cause);
      return false;
    }
  }
  return true;
}
