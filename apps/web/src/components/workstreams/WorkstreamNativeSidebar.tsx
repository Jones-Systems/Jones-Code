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
  renderThread,
  assignmentMenuRef,
  externalDropRef,
  canOperate,
  canReorder,
  reorder,
}: {
  controller: PersonalWorkstreamsController;
  threads: readonly EnvironmentThreadShell[];
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
  const [picker, setPicker] = useState<EnvironmentThreadShell | null>(null);
  const [drop, setDrop] = useState<string | null | undefined>();
  const root = useRef<HTMLDivElement | null>(null);
  const sensor = useRef<SidebarPointerSensor | null>(null);
  const canMove = (thread: EnvironmentThreadShell) =>
    canOperate(thread) && controller.canAssign(thread);
  const move = async (thread: EnvironmentThreadShell, id: string | null) => {
    const result = await controller.assign(thread, id);
    if (!result.ok) showPersonalFailure(result.failure);
    return result.ok;
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
    return {
      id,
      neighbor,
      after: row
        ? point.y > row.getBoundingClientRect().top + row.getBoundingClientRect().height / 2
        : false,
    };
  };
  // oxlint-disable-next-line t3code/react-effect-budget -- Publish committed menu and drop handles for Sidebar's single pointer gesture owner.
  useLayoutEffect(() => {
    if (assignmentMenuRef) assignmentMenuRef.current = menu;
    if (externalDropRef)
      externalDropRef.current = {
        locateExternal: (point, thread) => locate(point, thread) !== null,
        dropExternal: (point, thread) => {
          const target = locate(point, thread);
          setDrop(undefined);
          return target ? move(thread, target.id) : null;
        },
        clear: () => setDrop(undefined),
      };
    return () => {
      if (assignmentMenuRef) assignmentMenuRef.current = null;
      if (externalDropRef) externalDropRef.current = null;
    };
  });
  // oxlint-disable-next-line t3code/react-effect-budget -- Retire the owned pointer sensor on unmount.
  useLayoutEffect(() => () => sensor.current?.cancel(), []);
  const pointerDown = (event: ReactPointerEvent, thread: EnvironmentThreadShell) => {
    if (
      !event.isPrimary ||
      event.button !== 0 ||
      !canMove(thread) ||
      (event.target as HTMLElement).closest("button,a,input,textarea,select")
    )
      return;
    let target: ReturnType<typeof locate> = null;
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
        },
        onDrop: (point) => {
          target = locate(point, thread);
          return false;
        },
      },
      onPending: () => {},
      onAbort: () => {},
      onStart: () => {},
      onMove: (point) => {
        target = locate(point, thread);
      },
      onCancel: () => {
        target = null;
      },
      onEnd: () => {
        if (!target) return;
        const current =
          state.assignmentsByThreadKey.get(personalThreadKey(thread))?.workstreamId ?? null;
        if (
          current === target.id &&
          target.neighbor &&
          canReorder(thread) &&
          canReorder(target.neighbor)
        ) {
          void reorder(thread, target.neighbor, target.after).catch(() =>
            showPersonalFailure({
              action: "reorder",
              reason: "failed",
              environmentId: thread.environmentId,
              dedupeKey: `reorder:${personalThreadKey(thread)}:failed`,
            }),
          );
        } else void move(thread, target.id);
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
              {members.map((thread) => (
                <li
                  key={personalThreadKey(thread)}
                  data-personal-thread={personalThreadKey(thread)}
                  onPointerDown={(event) => pointerDown(event, thread)}
                >
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
