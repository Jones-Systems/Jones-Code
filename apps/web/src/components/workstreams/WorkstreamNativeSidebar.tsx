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
import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import {
  groupPersonalThreads,
  personalThreadKey,
  openPersonalWorkstreams,
  visiblePersonalWorkstreamId,
  type PersonalWorkstreamsController,
  type PersonalThreadRef,
  type PersonalWorkstreamView,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import { SIDEBAR_DRAG_DISTANCE, SidebarPointerSensor } from "../Sidebar.pointer";
import {
  dropThreadContext,
  endThreadContextDrag,
  moveThreadContextDrag as moveThreadContextDragGhost,
} from "../chat/threadContextDrag";
import { WorkstreamSidebarSection } from "./WorkstreamSidebarSection";
import type { WorkstreamNameIconProject } from "../../jones/workstreams/WorkstreamNameIconPicker";
import type { WorkstreamThreadAction } from "../../jones/workstreams/useStartWorkstreamThread";
import { showPersonalFailure } from "../../jones/workstreams/personalNotifications";
import { assignPersonalThread } from "../../jones/workstreams/personalThreadAssignmentMenu";
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
import { workstreamReferenceMembers } from "../../jones/workstreams/workstreamReferenceDrop";
export interface NativeThreadExternalDrop {
  readonly locateExternal: (
    point: { x: number; y: number },
    thread: EnvironmentThreadShell,
    options?: { fromMember: boolean },
  ) => boolean;
  readonly dropExternal: (
    point: { x: number; y: number },
    thread: EnvironmentThreadShell,
    options?: { fromMember?: boolean; unpin?: () => Promise<boolean> },
  ) => Promise<boolean> | null;
  readonly clear: () => void;
}

export function WorkstreamNativeSidebar({
  controller,
  nameIconProjects,
  newThreadAction,
  drafts,
  threads,
  summaryThreads = threads,
  rosterThreads = summaryThreads,
  readReferenceThreads,
  snoozedWorkstreams = [],
  renderThread,
  onOpenHistoryThread,
  externalDropRef,
  canOperate,
  canReorder,
  reorder,
  placeThread,
}: {
  controller: PersonalWorkstreamsController;
  nameIconProjects?: readonly WorkstreamNameIconProject[] | undefined;
  newThreadAction?: ((workstream: PersonalWorkstreamView) => WorkstreamThreadAction) | undefined;
  drafts?: ReactNode;
  threads: readonly EnvironmentThreadShell[];
  summaryThreads?: readonly EnvironmentThreadShell[];
  rosterThreads?: readonly EnvironmentThreadShell[];
  readReferenceThreads?: () => readonly EnvironmentThreadShell[];
  snoozedWorkstreams?: readonly PersonalWorkstreamView[];
  onOpenHistoryThread?: (thread: PersonalThreadRef) => void;
  renderThread: (thread: EnvironmentThreadShell) => ReactNode;
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
    unpin?: () => Promise<boolean>,
  ) => Promise<boolean>;
}) {
  const allWorkstreams = useAtomValue(controller.state);
  const snoozedIds = new Set(snoozedWorkstreams.map((item) => item.id));
  const state =
    snoozedIds.size === 0
      ? allWorkstreams
      : {
          ...allWorkstreams,
          workstreams: allWorkstreams.workstreams.filter((item) => !snoozedIds.has(item.id)),
        };
  const openWorkstreams = openPersonalWorkstreams(state);
  const grouping = groupPersonalThreads(state, threads);
  const summaryGrouping = groupPersonalThreads(state, summaryThreads);
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
    const name =
      id === null ? null : (state.workstreams.find((item) => item.id === id)?.name ?? "Workstream");
    return assignPersonalThread(controller, thread, id, name);
  };
  type Target = { id: string | null; neighborKey: string | null; after: boolean };
  const groupId = (thread: EnvironmentThreadShell) => visiblePersonalWorkstreamId(state, thread);
  const validTarget = (target: Target) => {
    if (target.id !== null && !openWorkstreams.some((item) => item.id === target.id)) return false;
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
  const place = async (
    thread: EnvironmentThreadShell,
    target: Target,
    external = false,
    unpin?: () => Promise<boolean>,
  ) => {
    if (busy.current || !canMove(thread) || !validTarget(target)) return false;
    if (
      external &&
      placeThread &&
      !summaryThreads.some((entry) => personalThreadKey(entry) === personalThreadKey(thread))
    )
      return false;
    const neighbor =
      threads.find((entry) => personalThreadKey(entry) === target.neighborKey) ?? null;
    if (!placeThread && (external || groupId(thread) !== target.id)) {
      const assigned = await move(thread, target.id);
      return assigned && (unpin ? await unpin() : true);
    }
    if (!placeThread && !neighbor) return false;
    if (neighbor && ((!external && !canReorder(thread)) || !canReorder(neighbor))) return false;
    const key = personalThreadKey(thread);
    const visibleOrder = () => {
      const section = [
        ...(root.current?.querySelectorAll<HTMLElement>("[data-native-workstream]") ?? []),
      ].find((entry) => entry.dataset.nativeWorkstream === JSON.stringify(target.id));
      return [
        ...(section?.querySelectorAll<HTMLElement>(
          "[data-personal-thread-list] > [data-personal-thread]",
        ) ?? []),
      ].map((row) => row.dataset.personalThread);
    };
    const before = neighbor ? visibleOrder().filter((entry) => entry !== key) : [];
    const neighborIndex = before.indexOf(target.neighborKey ?? undefined);
    let saved: boolean | undefined = false;
    busy.current = true;
    try {
      if (placeThread) saved = await placeThread(thread, target.id, neighbor, target.after, unpin);
      else if (neighbor) {
        await reorder(thread, neighbor, target.after);
        saved = true;
      }
    } catch {
      saved = undefined;
    }
    busy.current = false;
    if (neighborIndex !== -1) {
      requestAnimationFrame(() => {
        const renderedIndex = visibleOrder().indexOf(key);
        console.debug("[workstream thread drop]", {
          destination: target.id === null ? "unassigned" : "workstream",
          requestedPosition: neighborIndex + (target.after ? 2 : 1),
          renderedPositionAtNextFrame: renderedIndex === -1 ? null : renderedIndex + 1,
          saved: saved ?? false,
        });
      });
    }
    if (saved === undefined) {
      showPersonalFailure({
        action: "reorder",
        reason: "failed",
        environmentId: thread.environmentId,
        dedupeKey: `reorder:${key}:failed`,
      });
    }
    return saved ?? false;
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
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Publish committed drop handles for Sidebar's single pointer gesture owner.
  useLayoutEffect(() => {
    if (externalDropRef)
      externalDropRef.current = {
        locateExternal: (point, thread, options) => {
          const key = personalThreadKey(thread);
          const sidebarKey = scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id));
          const source = [
            ...document.querySelectorAll<HTMLElement>("[data-thread-item], [data-personal-thread]"),
          ].find(
            (row) => row.dataset.threadItem === sidebarKey || row.dataset.personalThread === key,
          );
          draggedHeight.current = source?.getBoundingClientRect().height ?? 0;
          const target = locate(point, thread, !options?.fromMember);
          const previous = externalTarget.current;
          externalTarget.current = target
            ? {
                key,
                sourceId: previous?.key === key ? previous.sourceId : groupId(thread),
                target,
              }
            : null;
          return target !== null;
        },
        dropExternal: (point, thread, options) => {
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
          const target = locate(point, thread, !options?.fromMember);
          clearDrop();
          return target ? place(thread, target, !options?.fromMember, options?.unpin) : null;
        },
        clear: () => {
          externalTarget.current = null;
          clearDrop();
        },
      };
    return () => {
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
      busy.current ||
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
        cancelOnViewportExit: () => true,
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
          const actions = pointerActions.current;
          if (target && !actions.validTarget(target)) return true;
          locateCurrent(point);
          const current = currentThread();
          // Desktop may deliver a buttonless move before pointerup. Both paths call onDrop.
          if (target && current) void actions.place(current, target);
          return true;
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
      onEnd: () => {},
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
      <WorkstreamSidebarSection
        controller={controller}
        nameIconProjects={nameIconProjects}
        newThreadAction={newThreadAction}
        drafts={drafts}
        snoozedIds={snoozedIds}
        onOpenHistoryThread={onOpenHistoryThread}
        rosterTotals={personalSidebarRosterTotals(state, rosterThreads)}
        referenceMembers={(id) =>
          workstreamReferenceMembers(state, readReferenceThreads?.() ?? rosterThreads, id)
        }
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
                    <ul>{renderThread(thread)}</ul>
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
