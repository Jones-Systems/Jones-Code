import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import {
  groupPersonalThreads,
  personalThreadKey,
  type PersonalWorkstreamsController,
  type PersonalWorkstreamsFailure,
  type PersonalWorkstreamsState,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";

import { planWorkstreamThreadReorder } from "./personalThreadReorder";

export interface PersonalThreadPlacementInput {
  readonly thread: EnvironmentThreadShell;
  readonly destinationId: string | null;
  readonly neighbor: EnvironmentThreadShell | null;
  readonly after: boolean;
  readonly state: PersonalWorkstreamsState;
  readonly activeThreads: readonly EnvironmentThreadShell[];
  readonly activeKeys: readonly string[];
  readonly activeKeysById: ReadonlyMap<string, string | null | undefined>;
  readonly reorderableKeys: ReadonlySet<string>;
  readonly threadByKey: ReadonlyMap<string, EnvironmentThreadShell>;
  readonly allowReorder: boolean;
  readonly controller: Pick<PersonalWorkstreamsController, "assign" | "canAssign">;
  readonly canOperate: (threads: readonly EnvironmentThreadShell[]) => boolean;
  readonly reorder: (thread: EnvironmentThreadShell, orderKey: string) => Promise<boolean>;
  readonly onAssignmentFailure: (failure: PersonalWorkstreamsFailure) => void;
  readonly onPositionFailure: (assignmentSaved: boolean, cause: unknown) => void;
}

const shellKey = (thread: EnvironmentThreadShell): string =>
  scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id));

// Assignment exceptions propagate to the caller: their membership outcome is unknown.
// Order failures report only membership confirmed by a successful assignment response.
export async function placePersonalThread(input: PersonalThreadPlacementInput): Promise<boolean> {
  const { thread, destinationId, controller } = input;
  // Unassigned retains its natural order; its rows never specify a manual slot.
  const neighbor = destinationId === null ? null : input.neighbor;
  if (!input.canOperate(neighbor ? [thread, neighbor] : [thread])) return false;
  if (
    destinationId !== null &&
    !input.state.workstreams.some((workstream) => workstream.id === destinationId)
  )
    return false;

  const movedId = shellKey(thread);
  if (!input.activeKeys.includes(movedId) && thread.pinnedAt == null) return false;
  const currentGroup =
    input.state.assignmentsByThreadKey.get(personalThreadKey(thread))?.workstreamId ?? null;
  const changesGroup = currentGroup !== destinationId;
  if (changesGroup && !controller.canAssign(thread)) return false;

  let assignments: NonNullable<ReturnType<typeof planWorkstreamThreadReorder>> = [];
  if (neighbor !== null) {
    if (!input.allowReorder) return false;
    const grouping = groupPersonalThreads(input.state, input.activeThreads);
    const groupIdByKey = new Map(
      grouping.groups.flatMap((group) =>
        group.threads.map((member) => [shellKey(member), group.workstream.id] as const),
      ),
    );
    const neighborId = shellKey(neighbor);
    if (
      neighborId === movedId ||
      !input.activeThreads.some((member) => shellKey(member) === neighborId) ||
      (groupIdByKey.get(neighborId) ?? null) !== destinationId
    )
      return false;

    const writable = new Set(
      [...input.reorderableKeys].filter((key) => {
        const candidate = input.threadByKey.get(key);
        return candidate !== undefined && input.canOperate([candidate]);
      }),
    );
    if (thread.pinnedAt != null) writable.add(movedId);
    const plan = planWorkstreamThreadReorder({
      activeOrder: input.activeKeys,
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
  for (const assignment of assignments) {
    try {
      const target =
        input.threadByKey.get(assignment.id) ?? (assignment.id === movedId ? thread : undefined);
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
