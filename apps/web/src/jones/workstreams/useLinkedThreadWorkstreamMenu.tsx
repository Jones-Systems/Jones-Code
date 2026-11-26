import { AuthOrchestrationOperateScope } from "@t3tools/contracts";
import type { PersonalThreadRef } from "@t3tools/client-runtime/jones/workstreams/personal";
import { useState, type MouseEvent } from "react";

import { reportMarkdownActionFailure } from "../../components/chat/markdownActionFailure";
import { contextMenuSourceOf } from "../../contextMenuSource";
import { readLocalApi } from "../../localApi";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import { readThreadShell } from "../../state/entities";
import { ensureEnvironmentScope, readEnvironmentScope } from "../../state/session";
import { personalWorkstreamsController as controller } from "../../state/workstreamsPersonal";
import { projectWorkstreamSnoozes } from "./personalSnooze";
import { personalThreadAssignmentMenu } from "./personalThreadAssignmentMenu";
import { WorkstreamPicker } from "./WorkstreamPicker";

export function useLinkedThreadWorkstreamMenu(thread: PersonalThreadRef | null) {
  const [pickerThread, setPickerThread] = useState<PersonalThreadRef | null>(null);

  const onContextMenu = (event: MouseEvent<HTMLAnchorElement>) => {
    if (event.defaultPrevented || thread === null) return;
    const api = readLocalApi();
    const state = appAtomRegistry.get(controller.state);
    if (!api || !state.environments.some((entry) => entry.status !== "update_required")) return;

    event.preventDefault();
    event.stopPropagation();
    const { href } = event.currentTarget;
    const bounds = event.currentTarget.getBoundingClientRect();
    const position =
      event.clientX === 0 && event.clientY === 0
        ? { x: bounds.left, y: bounds.bottom }
        : { x: event.clientX, y: event.clientY };
    const source = contextMenuSourceOf(event);
    const selection = window.getSelection()?.toString() ?? "";
    const isDesktop = Boolean(window.desktopBridge);
    const { environmentId, id: threadId } = thread;

    void (async () => {
      await ensureEnvironmentScope(environmentId, AuthOrchestrationOperateScope);
      const now = new Date();
      const { visibleState } = projectWorkstreamSnoozes(appAtomRegistry.get(controller.state), {
        utcMinute: now.toISOString().slice(0, 16),
        wakeClockMs: now.getTime(),
      });
      const assignment = personalThreadAssignmentMenu({
        controller,
        state: visibleState,
        thread: readThreadShell({ environmentId, threadId }) ?? thread,
        canOperate: () => readEnvironmentScope(environmentId, AuthOrchestrationOperateScope),
        openPicker: () => setPickerThread(thread),
      });
      const action = await api.contextMenu.show(
        [
          {
            id: "assign-thread",
            label: "Assign thread",
            icon: "folder",
            children: assignment.items,
          },
          ...(isDesktop
            ? []
            : ([
                ...(selection ? [{ id: "copy-selection", label: "Copy" } as const] : []),
                { id: "copy-link", label: "Copy Link", icon: "copy" },
              ] as const)),
        ],
        position,
        source,
        isDesktop ? Boolean(selection) : undefined,
      );
      if (action === "copy-link" || action === "copy-selection") {
        await navigator.clipboard.writeText(action === "copy-link" ? href : selection);
      } else if (action !== null) {
        assignment.onAction(action);
      }
    })().catch((cause) =>
      reportMarkdownActionFailure({ operation: "show-thread-context-menu", target: href }, cause),
    );
  };

  return {
    onContextMenu,
    picker:
      pickerThread === null ? null : (
        <WorkstreamPicker
          controller={controller}
          thread={pickerThread}
          onClose={() => setPickerThread(null)}
        />
      ),
  };
}
