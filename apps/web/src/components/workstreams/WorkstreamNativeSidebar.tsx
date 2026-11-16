import {
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
import { WorkstreamSidebarSection } from "./WorkstreamSidebarSection";
import { WorkstreamPicker } from "../../jones/workstreams/WorkstreamPicker";
import { showPersonalFailure } from "../../jones/workstreams/personalNotifications";
import { summarizePersonalThreads } from "../../jones/workstreams/personalPresentation";
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
  renderThread,
  assignmentMenuRef,
  externalDropRef,
  canOperate,
  canReorder,
  reorder,
}: {
  controller: PersonalWorkstreamsController;
  threads: readonly EnvironmentThreadShell[];
  summaryThreads?: readonly EnvironmentThreadShell[];
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
}) {
  const state = useAtomValue(controller.state);
  const grouping = groupPersonalThreads(state, threads);
  const summaryGrouping = groupPersonalThreads(state, summaryThreads);
  const [picker, setPicker] = useState<EnvironmentThreadShell | null>(null);
  const [drop, setDrop] = useState<string | null | undefined>();
  const [dropRow, setDropRow] = useState<{ key: string; after: boolean } | null>(null);
  const root = useRef<HTMLDivElement | null>(null);
  const sensor = useRef<SidebarPointerSensor | null>(null);
  const busy = useRef(false);
  const canMove = (thread: EnvironmentThreadShell) =>
    canOperate(thread) && controller.canAssign(thread);
  const move = async (thread: EnvironmentThreadShell, id: string | null) => {
    if (busy.current || !canMove(thread)) return false;
    busy.current = true;
    return controller
      .assign(thread, id)
      .then((result) => {
        if (!result.ok) showPersonalFailure(result.failure);
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
      })
      .finally(() => {
        busy.current = false;
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
        disabled: !canMove(thread),
      })),
      {
        id: "assign-workstream:remove",
        label: "Unassigned",
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
  const locate = (point: { x: number; y: number }, thread: EnvironmentThreadShell) => {
    setDropRow(null);
    const element = document.elementFromPoint(point.x, point.y);
    const section = element?.closest<HTMLElement>("[data-native-workstream]");
    if (!section || !root.current?.contains(section) || !canMove(thread)) {
      setDrop(undefined);
      return null;
    }
    const id = JSON.parse(section.dataset.nativeWorkstream!) as string | null;
    setDrop(id);
    const row = element?.closest<HTMLElement>("[data-personal-thread]");
    const neighbor = threads.find(
      (entry) => personalThreadKey(entry) === row?.dataset.personalThread,
    );
    const after = row
      ? point.y > row.getBoundingClientRect().top + row.getBoundingClientRect().height / 2
      : false;
    if (
      neighbor &&
      personalThreadKey(neighbor) !== personalThreadKey(thread) &&
      (grouping.workstreamIdByThreadKey.get(personalThreadKey(thread)) ?? null) === id &&
      canReorder(thread) &&
      canReorder(neighbor)
    ) {
      setDropRow({ key: personalThreadKey(neighbor), after });
    }
    return { id, neighbor, after };
  };
  const pointerActions = useRef({ locate, move, canReorder, reorder, threads, grouping });
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- A gesture may span controller updates; release uses the latest committed rows and membership.
  useLayoutEffect(() => {
    pointerActions.current = { locate, move, canReorder, reorder, threads, grouping };
  });
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Publish committed menu and drop handles for Sidebar's single pointer gesture owner.
  useLayoutEffect(() => {
    if (assignmentMenuRef) assignmentMenuRef.current = menu;
    if (externalDropRef)
      externalDropRef.current = {
        locateExternal: (point, thread) => locate(point, thread) !== null,
        dropExternal: (point, thread) => {
          const target = locate(point, thread);
          setDrop(undefined);
          setDropRow(null);
          return target ? move(thread, target.id) : null;
        },
        clear: () => {
          setDrop(undefined);
          setDropRow(null);
        },
      };
    return () => {
      if (assignmentMenuRef) assignmentMenuRef.current = null;
      if (externalDropRef) externalDropRef.current = null;
    };
  });
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Retire the owned pointer sensor on unmount.
  useLayoutEffect(() => () => sensor.current?.cancel(), []);
  const pointerDown = (event: ReactPointerEvent, thread: EnvironmentThreadShell) => {
    if (
      !event.isPrimary ||
      event.button !== 0 ||
      !canMove(thread) ||
      busy.current ||
      (event.target as HTMLElement).closest("button,a,input,textarea,select")
    )
      return;
    let target: ReturnType<typeof locate> = null;
    const key = personalThreadKey(thread);
    const startedGroup = grouping.workstreamIdByThreadKey.get(key) ?? null;
    const locateCurrent = (point: { x: number; y: number }) => {
      const actions = pointerActions.current;
      const currentThread = actions.threads.find((entry) => personalThreadKey(entry) === key);
      target = currentThread ? actions.locate(point, currentThread) : null;
    };
    sensor.current?.cancel();
    sensor.current = new SidebarPointerSensor({
      active: personalThreadKey(thread),
      event: event.nativeEvent,
      options: {
        distance: SIDEBAR_DRAG_DISTANCE,
        onAttach: () => {},
        onFinish: () => {
          sensor.current = null;
          setDrop(undefined);
          setDropRow(null);
        },
        onDrop: (point) => {
          locateCurrent(point);
          return false;
        },
      },
      onPending: () => {},
      onAbort: () => {},
      onStart: () => {},
      onMove: (point) => {
        locateCurrent(point);
      },
      onCancel: () => {
        target = null;
      },
      onEnd: () => {
        if (!target) return;
        const actions = pointerActions.current;
        const currentThread = actions.threads.find((entry) => personalThreadKey(entry) === key);
        if (!currentThread || busy.current) return;
        const current = actions.grouping.workstreamIdByThreadKey.get(key) ?? null;
        if (current !== startedGroup) return;
        if (current !== target.id) {
          void actions.move(currentThread, target.id);
          return;
        }
        const neighbor = target.neighbor;
        if (
          !neighbor ||
          personalThreadKey(neighbor) === key ||
          (actions.grouping.workstreamIdByThreadKey.get(personalThreadKey(neighbor)) ?? null) !==
            current ||
          !actions.canReorder(currentThread) ||
          !actions.canReorder(neighbor)
        )
          return;
        busy.current = true;
        void actions
          .reorder(currentThread, neighbor, target.after)
          .catch(() =>
            showPersonalFailure({
              action: "reorder",
              reason: "failed",
              environmentId: currentThread.environmentId,
              dedupeKey: `reorder:${key}:failed`,
            }),
          )
          .finally(() => {
            busy.current = false;
          });
      },
    });
  };
  return (
    <div ref={root}>
      {picker ? (
        <WorkstreamPicker controller={controller} thread={picker} onClose={() => setPicker(null)} />
      ) : null}
      <WorkstreamSidebarSection
        controller={controller}
        summaries={
          new Map(
            summaryGrouping.groups.map((group) => [
              group.workstream.id,
              summarizePersonalThreads(group.threads),
            ]),
          )
        }
        unassignedSummary={summarizePersonalThreads(grouping.ungrouped)}
        renderMembers={(id) => {
          const members =
            id === null
              ? grouping.ungrouped
              : (grouping.groups.find((group) => group.workstream.id === id)?.threads ?? []);
          return (
            <ul
              aria-label={id === null ? "Unassigned threads" : "Workstream threads"}
              className={drop === id ? "rounded ring-1 ring-primary" : ""}
            >
              {members.length === 0 ? (
                <li className="px-3 pb-3 text-xs text-sidebar-muted-foreground">
                  No active threads
                </li>
              ) : null}
              {members.map((thread) => (
                <li
                  key={personalThreadKey(thread)}
                  data-personal-thread={personalThreadKey(thread)}
                  className={`relative ${canMove(thread) ? "cursor-grab active:cursor-grabbing" : ""}`}
                  onPointerDown={(event) => pointerDown(event, thread)}
                >
                  {dropRow?.key === personalThreadKey(thread) ? (
                    <div
                      aria-hidden
                      className={`pointer-events-none absolute inset-x-1 z-20 h-0.5 bg-primary ${dropRow.after ? "bottom-0" : "top-0"}`}
                    />
                  ) : null}
                  <ul>{renderThread(thread, menu(thread))}</ul>
                </li>
              ))}
            </ul>
          );
        }}
      />
    </div>
  );
}
