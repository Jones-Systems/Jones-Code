import type { ContextMenuItem } from "@t3tools/contracts";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import {
  openPersonalWorkstreams,
  personalThreadKey,
  visiblePersonalWorkstreamId,
  type PersonalWorkstreamsController,
  type PersonalWorkstreamsState,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import type { ThreadActionMenuId } from "../../components/threadActionMenu.logic";
import { showPersonalAssignment, showPersonalFailure } from "./personalNotifications";

export async function assignPersonalThread(
  controller: PersonalWorkstreamsController,
  thread: EnvironmentThreadShell,
  id: string | null,
  name: string | null,
): Promise<boolean> {
  try {
    const result = await controller.assign(thread, id);
    if (!result.ok) showPersonalFailure(result.failure);
    else showPersonalAssignment(name, thread.pinnedAt != null);
    return result.ok;
  } catch {
    showPersonalFailure({
      action: id === null ? "unassign" : "assign",
      reason: "failed",
      environmentId: thread.environmentId,
      dedupeKey: `assign:${personalThreadKey(thread)}:failed`,
    });
    return false;
  }
}

export function personalThreadAssignmentMenu(input: {
  controller: PersonalWorkstreamsController;
  state: PersonalWorkstreamsState;
  thread: EnvironmentThreadShell;
  canOperate: () => boolean;
  openPicker: () => void;
}) {
  const { controller, state, thread, canOperate, openPicker } = input;
  const canAssign = () => canOperate() && controller.canAssign(thread);
  const disabled = !canAssign();
  const workstreams = openPersonalWorkstreams(state);
  const visibleId = visiblePersonalWorkstreamId(state, thread);
  const savedId = state.assignmentsByThreadKey.get(personalThreadKey(thread))?.workstreamId;
  const items: ReadonlyArray<ContextMenuItem<ThreadActionMenuId>> = [
    {
      id: "assign-workstream:picker",
      label: "Search or create Workstream…",
      disabled,
    },
    ...workstreams.map((item) => ({
      id: `assign-workstream:group:${item.id}` as const,
      label: item.name,
      checked: visibleId === item.id,
      disabled,
    })),
    {
      id: "assign-workstream:remove",
      label: visibleId === null && savedId != null ? "Remove saved Workstream" : "Unassigned",
      checked: savedId == null,
      disabled,
      separatorBefore: true,
    },
  ];
  return {
    items,
    onAction(action: ThreadActionMenuId) {
      if (!canAssign()) return;
      // The controller serializes each thread's intents, including concurrent drag and menu choices.
      if (action === "assign-workstream:picker") openPicker();
      else if (action === "assign-workstream:remove")
        void assignPersonalThread(controller, thread, null, null);
      else {
        const item = workstreams.find((entry) => action === `assign-workstream:group:${entry.id}`);
        if (item) void assignPersonalThread(controller, thread, item.id, item.name);
      }
    },
  };
}
