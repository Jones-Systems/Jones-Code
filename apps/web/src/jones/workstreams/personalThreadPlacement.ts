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
  readonly onPositionFailure: (assignmentSaved: boolean, cause: unknown) => void;
}

const shellKey = (thread: EnvironmentThreadShell): string =>
  scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id));

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

  let assignments: NonNullable<ReturnType<typeof planWorkstreamThreadReorder>> = [];
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
    if (plan === null) return false;
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
