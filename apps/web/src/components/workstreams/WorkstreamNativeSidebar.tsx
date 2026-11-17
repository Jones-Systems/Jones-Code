import {
  Fragment,
  useLayoutEffect,
  useRef,
  useState,
  type ReactNode,
  type PointerEvent as ReactPointerEvent,
  type RefObject,
} from "react";
import { useAtomValue } from "@effect/atom-react";
import {
  groupPersonalThreads,
  personalThreadKey,
  type PersonalWorkstreamsController,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import type { ContextMenuItem } from "@t3tools/contracts";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import type { ThreadActionMenuId } from "../threadActionMenu.logic";
import { SIDEBAR_DRAG_DISTANCE, SidebarPointerSensor } from "../Sidebar.pointer";
import {
  dropThreadContext,
  endThreadContextDrag,
  moveThreadContextDrag as moveThreadContextDragGhost,
} from "../chat/threadContextDrag";
import { WorkstreamSidebarSection } from "./WorkstreamSidebarSection";
import { WorkstreamPicker } from "../../jones/workstreams/WorkstreamPicker";
import {
  showPersonalAssignment,
  showPersonalFailure,
} from "../../jones/workstreams/personalNotifications";
import {
  personalSidebarRosterTotals,
  summarizePersonalThreads,
} from "../../jones/workstreams/personalPresentation";
import {
  liftPersonalThreadCard,
  personalThreadContextRefs,
} from "../../jones/workstreams/personalThreadDrag";
import { windowPersonalThreads } from "../../jones/workstreams/personalThreadWindow";
import { createSidebarDragAutoScroll } from "../../jones/workstreams/sidebarDragAutoScroll";
export interface NativeThreadAssignmentMenu {
  readonly items: ReadonlyArray<ContextMenuItem<ThreadActionMenuId>>;
  readonly onAction: (action: ThreadActionMenuId) => void;
}

export interface NativeThreadExternalDrop {
  readonly locateExternal: (
    point: { x: number; y: number },
    thread: EnvironmentThreadShell,
  ) => boolean;
  readonly dropExternal: (
    point: { x: number; y: number },
    thread: EnvironmentThreadShell,
  ) => Promise<boolean> | null;
  readonly clear: () => void;
}

export function WorkstreamNativeSidebar({
  controller,
  threads,
  summaryThreads = threads,
  rosterThreads = summaryThreads,
  renderThread,
  assignmentMenuRef,
  externalDropRef,
  canOperate,
  canReorder,
  reorder,
  placeThread,
}: {
  controller: PersonalWorkstreamsController;
  threads: readonly EnvironmentThreadShell[];
  summaryThreads?: readonly EnvironmentThreadShell[];
  rosterThreads?: readonly EnvironmentThreadShell[];
  renderThread: (
    thread: EnvironmentThreadShell,
    assignment: NativeThreadAssignmentMenu,
  ) => ReactNode;
  assignmentMenuRef?: RefObject<
    ((thread: EnvironmentThreadShell) => NativeThreadAssignmentMenu) | null
  >;
  externalDropRef?: RefObject<NativeThreadExternalDrop | null>;
  canOperate: (thread: EnvironmentThreadShell) => boolean;
  canReorder: (thread: EnvironmentThreadShell) => boolean;
  reorder: (
    thread: EnvironmentThreadShell,
    neighbor: EnvironmentThreadShell,
    after: boolean,
  ) => Promise<void>;
  placeThread?: (
    thread: EnvironmentThreadShell,
    destinationId: string | null,
    neighbor: EnvironmentThreadShell | null,
    after: boolean,
  ) => Promise<boolean>;
}) {
  const state = useAtomValue(controller.state);
  const grouping = groupPersonalThreads(state, threads);
  const summaryGrouping = groupPersonalThreads(state, summaryThreads);
  const [picker, setPicker] = useState<EnvironmentThreadShell | null>(null);
  const [drop, setDrop] = useState<string | null | undefined>();
  const [dropRow, setDropRow] = useState<{
    id: string | null;
    key: string | null;
    after: boolean;
    height: number;
  } | null>(null);
  const draggedHeight = useRef(0);
  const root = useRef<HTMLDivElement | null>(null);
  const sensor = useRef<SidebarPointerSensor | null>(null);
  const busy = useRef(false);
  const lifted = useRef<ReturnType<typeof liftPersonalThreadCard> | null>(null);
  const externalTarget = useRef<{
    key: string;
    sourceId: string | null;
    target: { id: string | null; neighborKey: string | null; after: boolean };
  } | null>(null);
  const canMove = (thread: EnvironmentThreadShell) =>
    canOperate(thread) && controller.canAssign(thread);
  const move = async (thread: EnvironmentThreadShell, id: string | null) => {
    if (!canMove(thread)) return false;
    // The controller serializes each thread's intents; a pending drag must not discard a menu choice.
    return controller
      .assign(thread, id)
      .then((result) => {
        if (!result.ok) showPersonalFailure(result.failure);
        else
          showPersonalAssignment(
            id === null
              ? null
              : (state.workstreams.find((item) => item.id === id)?.name ?? "Workstream"),
            thread.pinnedAt != null,
          );
        return result.ok;
      })
      .catch(() => {
        showPersonalFailure({
          action: id === null ? "unassign" : "assign",
          reason: "failed",
          environmentId: thread.environmentId,
          dedupeKey: `assign:${personalThreadKey(thread)}:failed`,
        });
        return false;
      });
  };
  const menu = (thread: EnvironmentThreadShell): NativeThreadAssignmentMenu => ({
    items: [
      {
        id: "assign-workstream:picker",
        label: "Search or create Workstream…",
        disabled: !canMove(thread),
      },
      ...state.workstreams.map((item) => ({
        id: `assign-workstream:group:${item.id}` as ThreadActionMenuId,
        label: item.name,
        checked:
          state.assignmentsByThreadKey.get(personalThreadKey(thread))?.workstreamId === item.id,
        disabled: !canMove(thread),
      })),
      {
        id: "assign-workstream:remove",
        label: "Unassigned",
        checked: state.assignmentsByThreadKey.get(personalThreadKey(thread))?.workstreamId == null,
        disabled: !canMove(thread),
        separatorBefore: true,
      },
    ],
    onAction: (action) => {
      if (!canMove(thread)) return;
      if (action === "assign-workstream:picker") setPicker(thread);
      else if (action === "assign-workstream:remove") void move(thread, null);
      else {
        const item = state.workstreams.find(
          (entry) => action === `assign-workstream:group:${entry.id}`,
        );
        if (item) void move(thread, item.id);
      }
    },
  });
  type Target = { id: string | null; neighborKey: string | null; after: boolean };
  const groupId = (thread: EnvironmentThreadShell) => {
    const id = state.assignmentsByThreadKey.get(personalThreadKey(thread))?.workstreamId;
    return id && state.workstreams.some((item) => item.id === id) ? id : null;
  };
  const validTarget = (target: Target) => {
    if (target.id !== null && !state.workstreams.some((item) => item.id === target.id))
      return false;
    return (
      target.neighborKey === null ||
      threads.some(
        (entry) => personalThreadKey(entry) === target.neighborKey && groupId(entry) === target.id,
      )
    );
  };
  const locate = (
    point: { x: number; y: number },
    thread: EnvironmentThreadShell,
    external = false,
  ): Target | null => {
    const reject = () => {
      setDropRow(null);
      setDrop(undefined);
      return null;
    };
    const element = document.elementFromPoint(point.x, point.y);
    const section = element?.closest<HTMLElement>("[data-native-workstream]");
    if (!section || !root.current?.contains(section) || !canMove(thread)) return reject();
    const id = JSON.parse(section.dataset.nativeWorkstream!) as string | null;
    const list = section.querySelector<HTMLElement>("[data-personal-thread-list]");
    if (id !== null && !state.workstreams.some((item) => item.id === id)) return reject();
    const key = personalThreadKey(thread);
    const gap = element?.closest<HTMLElement>("[data-personal-thread-insertion]");
    if (gap) {
      // The placeholder displaced its neighbor; keep its captured slot rather than re-hit-testing shifted rows.
      const target = JSON.parse(gap.dataset.personalThreadSlot!) as Target;
      const neighbor = threads.find((entry) => personalThreadKey(entry) === target.neighborKey);
      if (
        !validTarget(target) ||
        !(external || canReorder(thread)) ||
        (neighbor && !canReorder(neighbor))
      )
        return reject();
      setDrop(id);
      return target;
    }
    const row = element?.closest<HTMLElement>("[data-personal-thread]");
    let neighbor = threads.find(
      (entry) => personalThreadKey(entry) === row?.dataset.personalThread,
    );
    let after = row
      ? point.y > row.getBoundingClientRect().top + row.getBoundingClientRect().height / 2
      : false;
    if (placeThread && !row && list?.contains(element ?? null)) {
      const rows = [...list.querySelectorAll<HTMLElement>("[data-personal-thread]")];
      const last = rows.findLast((entry) => entry.dataset.personalThread !== key);
      if (!last && rows.length > 0) return reject();
      neighbor = threads.find((entry) => personalThreadKey(entry) === last?.dataset.personalThread);
      after = true;
    }
    if (neighbor && (groupId(neighbor) !== id || personalThreadKey(neighbor) === key))
      return reject();
    const canPosition = (external || canReorder(thread)) && (!neighbor || canReorder(neighbor));
    const ordinal =
      neighbor !== undefined && canPosition && (Boolean(placeThread) || groupId(thread) === id);
    if ((!ordinal || !neighbor) && !external && groupId(thread) === id) return reject();
    const members =
      id === null
        ? grouping.ungrouped
        : (grouping.groups.find((group) => group.workstream.id === id)?.threads ?? []);
    if (ordinal && groupId(thread) === id && neighbor) {
      const sourceIndex = members.findIndex((entry) => personalThreadKey(entry) === key);
      const targetIndex = members.findIndex(
        (entry) => personalThreadKey(entry) === personalThreadKey(neighbor),
      );
      const slot = targetIndex + (after ? 1 : 0);
      if (sourceIndex >= 0 && (slot === sourceIndex || slot === sourceIndex + 1)) return reject();
    }
    const target = {
      id,
      neighborKey: ordinal && neighbor ? personalThreadKey(neighbor) : null,
      after,
    };
    setDrop(id);
    setDropRow(
      list &&
        canPosition &&
        (ordinal || (members.length === 0 && placeThread)) &&
        draggedHeight.current > 0
        ? { id, key: target.neighborKey, after, height: draggedHeight.current }
        : null,
    );
    return target;
  };
  const place = async (thread: EnvironmentThreadShell, target: Target, external = false) => {
    if (busy.current || !canMove(thread) || !validTarget(target)) return false;
    if (
      external &&
      placeThread &&
      !summaryThreads.some((entry) => personalThreadKey(entry) === personalThreadKey(thread))
    )
      return false;
    const neighbor =
      threads.find((entry) => personalThreadKey(entry) === target.neighborKey) ?? null;
    if (placeThread) {
      if (neighbor && !external && (!canReorder(thread) || !canReorder(neighbor))) return false;
      busy.current = true;
      return Promise.resolve()
        .then(() => placeThread(thread, target.id, neighbor, target.after))
        .catch(() => {
          showPersonalFailure({
            action: "reorder",
            reason: "failed",
            environmentId: thread.environmentId,
            dedupeKey: `reorder:${personalThreadKey(thread)}:failed`,
          });
          return false;
        })
        .finally(() => {
          busy.current = false;
        });
    }
    if (external || groupId(thread) !== target.id) return move(thread, target.id);
    if (!neighbor || !canReorder(thread) || !canReorder(neighbor)) return false;
    busy.current = true;
    return Promise.resolve()
      .then(() => reorder(thread, neighbor, target.after))
      .then(() => true)
      .catch(() => {
        showPersonalFailure({
          action: "reorder",
          reason: "failed",
          environmentId: thread.environmentId,
          dedupeKey: `reorder:${personalThreadKey(thread)}:failed`,
        });
        return false;
      })
      .finally(() => {
        busy.current = false;
      });
  };
  const pointerActions = useRef({ locate, place, threads, groupId, validTarget });
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- A gesture may span controller updates; release uses the latest committed rows and membership.
  useLayoutEffect(() => {
    pointerActions.current = { locate, place, threads, groupId, validTarget };
  });
  const clearDrop = () => {
    setDrop(undefined);
    setDropRow(null);
  };
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Publish committed menu and drop handles for Sidebar's single pointer gesture owner.
  useLayoutEffect(() => {
    if (assignmentMenuRef) assignmentMenuRef.current = menu;
    if (externalDropRef)
      externalDropRef.current = {
        locateExternal: (point, thread) => {
          const source = [
            ...document.querySelectorAll<HTMLElement>("[data-thread-item], [data-personal-thread]"),
          ].find(
            (row) =>
              row.dataset.threadItem === personalThreadKey(thread) ||
              row.dataset.personalThread === personalThreadKey(thread),
          );
          draggedHeight.current = source?.getBoundingClientRect().height ?? 0;
          const target = locate(point, thread, true);
          const previous = externalTarget.current;
          externalTarget.current = target
            ? {
                key: personalThreadKey(thread),
                sourceId:
                  previous?.key === personalThreadKey(thread) ? previous.sourceId : groupId(thread),
                target,
              }
            : null;
          return target !== null;
        },
        dropExternal: (point, thread) => {
          const captured = externalTarget.current;
          externalTarget.current = null;
          if (
            captured &&
            captured.key === personalThreadKey(thread) &&
            (captured.sourceId !== groupId(thread) || !validTarget(captured.target))
          ) {
            clearDrop();
            return Promise.resolve(false);
          }
          const target = locate(point, thread, true);
          clearDrop();
          return target ? place(thread, target, true) : null;
        },
        clear: () => {
          externalTarget.current = null;
          clearDrop();
        },
      };
    return () => {
      if (assignmentMenuRef) assignmentMenuRef.current = null;
      if (externalDropRef) externalDropRef.current = null;
    };
  });
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Retire the owned pointer sensor and its DOM-only lifted card on unmount.
  useLayoutEffect(
    () => () => {
      sensor.current?.cancel();
      lifted.current?.dispose();
      lifted.current = null;
    },
    [],
  );
  const pointerDown = (event: ReactPointerEvent, thread: EnvironmentThreadShell) => {
    if (
      !event.isPrimary ||
      event.button !== 0 ||
      (event.target as HTMLElement).closest("button,a,input,textarea,select")
    )
      return;
    let target: Target | null = null;
    const source = event.currentTarget as HTMLElement;
    const key = personalThreadKey(thread);
    const startedGroup = groupId(thread);
    const currentThread = () =>
      pointerActions.current.threads.find((entry) => personalThreadKey(entry) === key);
    const outsideSidebar = (point: { x: number; y: number }) => {
      const bounds = root.current?.getBoundingClientRect();
      return bounds !== undefined && (point.x < bounds.left || point.x > bounds.right);
    };
    const locateCurrent = (point: { x: number; y: number }) => {
      const actions = pointerActions.current;
      const currentThread = actions.threads.find((entry) => personalThreadKey(entry) === key);
      target =
        currentThread && actions.groupId(currentThread) === startedGroup
          ? actions.locate(point, currentThread)
          : null;
    };
    sensor.current?.cancel();
    let autoScroll: ReturnType<typeof createSidebarDragAutoScroll> | null = null;
    sensor.current = new SidebarPointerSensor({
      active: key,
      event: event.nativeEvent,
      options: {
        distance: SIDEBAR_DRAG_DISTANCE,
        onAttach: () => {},
        onFinish: () => {
          autoScroll?.dispose();
          sensor.current = null;
          lifted.current?.dispose();
          lifted.current = null;
          endThreadContextDrag();
          clearDrop();
        },
        onDrop: (point) => {
          if (outsideSidebar(point)) {
            target = null;
            const current = currentThread();
            if (current) dropThreadContext(point, personalThreadContextRefs(current));
            return true;
          }
          if (target && !pointerActions.current.validTarget(target)) target = null;
          else locateCurrent(point);
          return false;
        },
      },
      onPending: () => {},
      onAbort: () => {},
      onStart: (point) => {
        if (canMove(thread) && !busy.current) {
          draggedHeight.current = source.getBoundingClientRect().height;
          lifted.current = liftPersonalThreadCard(source, point);
          autoScroll = createSidebarDragAutoScroll(source, locateCurrent);
        }
      },
      onMove: (point) => {
        autoScroll?.move(point);
        const outside = outsideSidebar(point);
        lifted.current?.setVisible(!outside);
        if (outside) {
          target = null;
          clearDrop();
          const current = currentThread();
          if (current)
            moveThreadContextDragGhost(point, {
              title: current.title.trim() || "Thread",
              count: personalThreadContextRefs(current).length,
            });
          else endThreadContextDrag();
          return;
        }
        endThreadContextDrag();
        lifted.current?.move(point);
        locateCurrent(point);
      },
      onCancel: () => {
        target = null;
      },
      onEnd: () => {
        if (!target) return;
        const actions = pointerActions.current;
        const currentThread = actions.threads.find((entry) => personalThreadKey(entry) === key);
        if (!currentThread || actions.groupId(currentThread) !== startedGroup) return;
        void actions.place(currentThread, target);
      },
    });
  };
  const renderGap = () =>
    dropRow ? (
      <li
        aria-hidden
        data-personal-thread-insertion={dropRow.after ? "after" : "before"}
        data-personal-thread-slot={JSON.stringify({
          id: dropRow.id,
          neighborKey: dropRow.key,
          after: dropRow.after,
        })}
        style={{ height: dropRow.height }}
      />
    ) : null;
  return (
    <div ref={root}>
      {picker ? (
        <WorkstreamPicker controller={controller} thread={picker} onClose={() => setPicker(null)} />
      ) : null}
      <WorkstreamSidebarSection
        controller={controller}
        rosterTotals={personalSidebarRosterTotals(state, rosterThreads)}
        dropTargetId={drop}
        memberCounts={
          new Map(grouping.groups.map((group) => [group.workstream.id, group.threads.length]))
        }
        summaries={
          new Map(
            summaryGrouping.groups.map((group) => [
              group.workstream.id,
              summarizePersonalThreads(group.threads),
            ]),
          )
        }
        unassignedSummary={summarizePersonalThreads(grouping.ungrouped)}
        renderMembers={(id, window) => {
          const allMembers =
            id === null
              ? grouping.ungrouped
              : (grouping.groups.find((group) => group.workstream.id === id)?.threads ?? []);
          const members = window
            ? windowPersonalThreads(allMembers, window.limit, window.expanded)
            : allMembers;
          return (
            <ul
              data-personal-thread-list
              aria-label={
                id === null
                  ? "Unassigned threads"
                  : `${state.workstreams.find((item) => item.id === id)?.name ?? "Workstream"} threads`
              }
              // Named members sit a fixed 8 CSS px in, independent of the root font size; Unassigned stays flush.
              {...(id === null ? {} : { style: { paddingLeft: "8px" } })}
            >
              {members.length === 0 ? (
                <li className="relative px-3 pb-3 text-xs text-sidebar-muted-foreground">
                  No threads match the current filters
                </li>
              ) : null}
              {members.map((thread) => (
                <Fragment key={personalThreadKey(thread)}>
                  {dropRow?.id === id && dropRow.key === personalThreadKey(thread) && !dropRow.after
                    ? renderGap()
                    : null}
                  <li
                    data-personal-thread={personalThreadKey(thread)}
                    className="relative cursor-grab active:cursor-grabbing"
                    onPointerDown={(event) => pointerDown(event, thread)}
                  >
                    <ul>{renderThread(thread, menu(thread))}</ul>
                  </li>
                  {dropRow?.id === id && dropRow.key === personalThreadKey(thread) && dropRow.after
                    ? renderGap()
                    : null}
                </Fragment>
              ))}
              {dropRow?.id === id && dropRow.key === null ? renderGap() : null}
            </ul>
          );
        }}
      />
    </div>
  );
}
