import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import { workstreamBindingKey } from "@t3tools/client-runtime/state/workstreams";
import {
  useLayoutEffect,
  useRef,
  useState,
  type ReactNode,
  type PointerEvent as ReactPointerEvent,
  type RefObject,
} from "react";
import type { WorkstreamListView } from "../../state/workstreams";
import type { ContextMenuItem } from "@t3tools/contracts";
import type { ThreadActionMenuId } from "../threadActionMenu.logic";
import { SIDEBAR_DRAG_DISTANCE, SidebarPointerSensor } from "../Sidebar.pointer";

import { WorkstreamSidebarSection, workstreamCommandId } from "./WorkstreamSidebarSection";
import { canEditWorkstreams } from "./nativeWorkstreamActions";
import {
  nativeWorkstreamThreadKey,
  type NativeWorkstreamThreadGrouping,
} from "./nativeThreadGrouping";

import { nativeThreadSourceAvailable } from "../../jones/workstreams/nativeThreadAssignment";
import { workstreamFailureMessage } from "../../jones/workstreams/workstreamActionSnapshot";

import { summarizeWorkstreamThreadStatuses } from "./workstreamThreadStatus";

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
  assignmentMenuRef,
  externalDropRef,
  ...props
}: {
  readonly controller: WorkstreamListView;
  readonly grouping: NativeWorkstreamThreadGrouping<EnvironmentThreadShell>;
  readonly summaryGrouping?: NativeWorkstreamThreadGrouping<EnvironmentThreadShell>;
  readonly renderThread: (
    thread: EnvironmentThreadShell,
    assignment: NativeThreadAssignmentMenu,
  ) => ReactNode;
  readonly assignmentMenuRef?: RefObject<
    ((thread: EnvironmentThreadShell) => NativeThreadAssignmentMenu) | null
  >;
  readonly externalDropRef?: RefObject<NativeThreadExternalDrop | null>;
  readonly canOperate: (thread: EnvironmentThreadShell) => boolean;
  readonly canReorder: (thread: EnvironmentThreadShell) => boolean;
  readonly reorder: (
    thread: EnvironmentThreadShell,
    neighbor: EnvironmentThreadShell,
    after: boolean,
  ) => Promise<void>;
}) {
  const { controller, grouping } = props;
  const statusGrouping = props.summaryGrouping ?? grouping;
  const threadStatusSummaries = summarizeWorkstreamThreadStatuses(statusGrouping);
  const [dragged, setDragged] = useState<EnvironmentThreadShell | null>(null);
  const [dropTarget, setDropTarget] = useState<{
    workstreamId: string | null;
    threadKey?: string;
    after?: boolean;
  } | null>(null);
  const sensorRef = useRef<SidebarPointerSensor | null>(null);
  const rootRef = useRef<HTMLDivElement | null>(null);
  const [offsetY, setOffsetY] = useState(0);
  const [busy, setBusy] = useState(false);
  const busyRef = useRef(false);
  const [error, setError] = useState<string | null>(null);
  const request = useRef<AbortController | null>(null);
  const binding = controller.data ? workstreamBindingKey(controller.data.binding) : null;
  const bindingRef = useRef(binding);
  const [stateBinding, setStateBinding] = useState(binding);
  if (stateBinding !== binding) {
    setStateBinding(binding);
    setDragged(null);
    setDropTarget(null);
    setOffsetY(0);
    setError(null);
    setBusy(false);
  }
  useLayoutEffect(() => {
    bindingRef.current = binding;
    sensorRef.current?.cancel();
    request.current?.abort();
    busyRef.current = false;
    return () => {
      bindingRef.current = null;
      sensorRef.current?.cancel();
      request.current?.abort();
    };
  }, [binding, bindingRef, busyRef, request, sensorRef]);
  const canWrite = canEditWorkstreams(controller.data) && !controller.loading && !busy;
  const inventory = new Set(
    controller.placementInventory.identities.map((item) =>
      nativeWorkstreamThreadKey(item.source_instance_id, item.native_thread_id),
    ),
  );
  const assuranceOf = (thread: EnvironmentThreadShell) => {
    const key = nativeWorkstreamThreadKey(thread.environmentId, thread.id);
    return statusGrouping.assuranceByKey?.get(key) ?? grouping.assuranceByKey?.get(key);
  };
  const conflicted = (thread: EnvironmentThreadShell) => {
    const key = nativeWorkstreamThreadKey(thread.environmentId, thread.id);
    return statusGrouping.conflictingKeys.has(key) || grouping.conflictingKeys.has(key);
  };
  // With stored assignments, a recorded member is known; Unassigned needs a complete live index.
  const assignmentChecked = (thread: EnvironmentThreadShell) => {
    const assurance = assuranceOf(thread);
    if (assurance !== undefined)
      return (
        assurance === "verified" ||
        assurance === "recorded" ||
        (assurance === "unassigned" && controller.stale !== true)
      );
    return (
      controller.placements?.readiness === "ready" &&
      controller.placements.trustedEnvironments.some(
        (entry) => entry.environmentId === thread.environmentId,
      ) &&
      inventory.has(nativeWorkstreamThreadKey(thread.environmentId, thread.id))
    );
  };
  // Another environment's thread is registered through its own server, which checks its source.
  const sourceAvailable = (thread: EnvironmentThreadShell) =>
    controller.assignmentAvailability
      ? controller.assignmentAvailability(thread).state !== "unavailable"
      : nativeThreadSourceAvailable(controller.registrationContext, thread) ||
        controller.assignsThroughOwnEnvironment?.(thread.environmentId) === true;
  // Stale snapshots block assignment changes; native ordering can use recorded membership.
  // Conflicting primaries still require re-verification before either operation.
  const canMove = (thread: EnvironmentThreadShell) =>
    canWrite &&
    props.canOperate(thread) &&
    controller.stale !== true &&
    controller.assignNativeThread !== undefined &&
    sourceAvailable(thread) &&
    !conflicted(thread);
  const canReorder = (thread: EnvironmentThreadShell) =>
    !busy &&
    props.canOperate(thread) &&
    props.canReorder(thread) &&
    assignmentChecked(thread) &&
    (controller.stale !== true || currentWorkstream(thread) !== null) &&
    !conflicted(thread);
  const canDrag = (thread: EnvironmentThreadShell) => canMove(thread) || canReorder(thread);
  const hasUnchecked = grouping.ungrouped.some((thread) => !assignmentChecked(thread));
  const hasConflict = grouping.conflictingKeys.size > 0;
  // Unassigned is a claim only a complete, live index can make for every listed row.
  const otherThreadsLabel =
    grouping.assuranceByKey !== undefined ? "Assignment unknown" : "Other threads";
  const assignmentIndex = controller.assignmentIndex ?? null;
  const run = async (operation: () => Promise<void>): Promise<boolean> => {
    if (busyRef.current || bindingRef.current !== binding) return false;
    busyRef.current = true;
    setBusy(true);
    setError(null);
    const startedBinding = binding;
    return (async () => {
      try {
        await operation();
        return bindingRef.current === startedBinding;
      } catch (cause) {
        if (bindingRef.current === startedBinding) setError(workstreamFailureMessage(cause));
        return false;
      }
    })().finally(() => {
      if (bindingRef.current === startedBinding) {
        busyRef.current = false;
        setBusy(false);
      }
    });
  };
  // Retry reconciles unresolved commands by reading their receipts before it reloads.
  const refreshWorkstreams = () =>
    run(async () => {
      if (controller.retry) await controller.retry();
      else controller.refresh();
    });
  const move = async (thread: EnvironmentThreadShell, destination: string | null) => {
    if (!canMove(thread) || !controller.assignNativeThread)
      throw new Error("This environment is unavailable for Workstream assignment.");
    request.current?.abort();
    const abort = new AbortController();
    request.current = abort;
    await controller.assignNativeThread(thread, destination, {
      signal: abort.signal,
      commandId: workstreamCommandId,
    });
  };
  const currentWorkstream = (thread: EnvironmentThreadShell) =>
    statusGrouping.groups.find((group) =>
      group.threads.some(
        (item) => item.environmentId === thread.environmentId && item.id === thread.id,
      ),
    )?.workstream.workstreamId ?? null;
  const threadsByKey = new Map(
    grouping.ordered.map((thread) => [
      nativeWorkstreamThreadKey(thread.environmentId, thread.id),
      thread,
    ]),
  );
  const locate = (
    point: { x: number; y: number },
    thread: EnvironmentThreadShell,
    external = false,
  ) => {
    setDropTarget(null);
    const element = document.elementFromPoint(point.x, point.y);
    const group = element?.closest<HTMLElement>("[data-native-workstream]");
    if (!group || !rootRef.current?.contains(group)) return null;
    if (external ? !canMove(thread) : !canDrag(thread)) return null;
    if (!external && !threadsByKey.has(nativeWorkstreamThreadKey(thread.environmentId, thread.id)))
      return null;
    const destination = JSON.parse(group.dataset.nativeWorkstream!) as string | null;
    const row = element?.closest<HTMLElement>("[data-native-workstream-thread]");
    const neighbor = row?.dataset.nativeWorkstreamThread
      ? threadsByKey.get(row.dataset.nativeWorkstreamThread)
      : undefined;
    const sameGroup = assignmentChecked(thread) && currentWorkstream(thread) === destination;
    if (
      !external &&
      sameGroup &&
      (!neighbor ||
        !canReorder(thread) ||
        !canReorder(neighbor) ||
        (neighbor.environmentId === thread.environmentId && neighbor.id === thread.id))
    )
      return null;
    if (!sameGroup && !canMove(thread)) return null;
    const bounds = row?.getBoundingClientRect();
    const after = bounds !== undefined && point.y > bounds.top + bounds.height / 2;
    setDropTarget({
      workstreamId: destination,
      ...(!external && sameGroup && neighbor
        ? { threadKey: nativeWorkstreamThreadKey(neighbor.environmentId, neighbor.id), after }
        : {}),
    });
    return { destination, neighbor, after, sameGroup };
  };
  const pointerActions = useRef({ locate, run, move, reorder: props.reorder, canReorder });
  useLayoutEffect(() => {
    pointerActions.current = { locate, run, move, reorder: props.reorder, canReorder };
  });
  useLayoutEffect(() => {
    if (!externalDropRef) return;
    externalDropRef.current = {
      locateExternal: (point, thread) => locate(point, thread, true) !== null,
      dropExternal: (point, thread) => {
        const target = locate(point, thread, true);
        setDropTarget(null);
        if (!target) return null;
        if (target.sameGroup) return Promise.resolve(false);
        return run(() => move(thread, target.destination));
      },
      clear: () => setDropTarget(null),
    };
    return () => {
      externalDropRef.current = null;
    };
  });
  const pointerDown = (event: ReactPointerEvent, thread: EnvironmentThreadShell) => {
    if (
      !event.isPrimary ||
      event.button !== 0 ||
      !canDrag(thread) ||
      (event.target as HTMLElement).closest(
        "button, a, input, textarea, select, [contenteditable=true]",
      )
    )
      return;
    sensorRef.current?.cancel();
    const originY = event.clientY;
    let target: ReturnType<typeof locate> = null;
    const locatePointer = (point: { x: number; y: number }) => {
      target = pointerActions.current.locate(point, thread);
    };
    sensorRef.current = new SidebarPointerSensor({
      active: nativeWorkstreamThreadKey(thread.environmentId, thread.id),
      event: event.nativeEvent,
      options: {
        distance: SIDEBAR_DRAG_DISTANCE,
        onAttach: () => {},
        onFinish: () => {
          sensorRef.current = null;
          setDragged(null);
          setOffsetY(0);
          setDropTarget(null);
        },
        onDrop: (point) => {
          locatePointer(point);
          return false;
        },
      },
      onPending: () => {},
      onAbort: () => {},
      onStart: () => setDragged(thread),
      onMove: (point) => {
        setOffsetY(point.y - originY);
        locatePointer(point);
      },
      onCancel: () => {
        target = null;
      },
      onEnd: () => {
        if (!target) return;
        const { destination, neighbor, after, sameGroup } = target;
        const actions = pointerActions.current;
        void actions.run(async () => {
          if (!sameGroup) await actions.move(thread, destination);
          else if (neighbor && actions.canReorder(thread) && actions.canReorder(neighbor))
            await actions.reorder(thread, neighbor, after);
        });
      },
    });
  };
  const assignmentMenu = (thread: EnvironmentThreadShell): NativeThreadAssignmentMenu => {
    const checked = assignmentChecked(thread);
    const workstreamId = checked ? currentWorkstream(thread) : null;
    const secondary =
      (checked
        ? statusGrouping.secondaryWorkstreamIdsByKey.get(
            nativeWorkstreamThreadKey(thread.environmentId, thread.id),
          )
        : undefined) ?? [];
    return {
      items: [
        ...(!sourceAvailable(thread) || conflicted(thread) || !checked
          ? [
              {
                id: "assign-workstream:status" as const,
                label: !sourceAvailable(thread)
                  ? (controller.assignmentAvailability?.(thread).reason ??
                    "Assignment unavailable for this environment")
                  : conflicted(thread)
                    ? "Conflicting assignments; changes blocked"
                    : assuranceOf(thread) === undefined
                      ? "Assignment not checked"
                      : "Assignment unknown",
                disabled: true,
              },
            ]
          : []),
        ...(controller.data?.items ?? []).map((item) => ({
          id: `assign-workstream:group:${item.workstreamId}` as const,
          label: `${item.name}${item.workstreamId === workstreamId ? " (primary)" : secondary.includes(item.workstreamId) ? " (secondary)" : ""}`,
          disabled: !canMove(thread) || item.workstreamId === workstreamId,
        })),
        {
          id: "assign-workstream:remove",
          label: "Unassigned (remove primary)",
          separatorBefore: true,
          disabled: !canMove(thread) || (checked && workstreamId === null),
        },
      ],
      onAction: (action) => {
        if (action === "assign-workstream:remove") run(() => move(thread, null));
        else {
          const destination = controller.data?.items.find(
            (item) => `assign-workstream:group:${item.workstreamId}` === action,
          );
          if (destination) run(() => move(thread, destination.workstreamId));
        }
      },
    };
  };
  useLayoutEffect(() => {
    if (!assignmentMenuRef) return;
    assignmentMenuRef.current = assignmentMenu;
    return () => {
      assignmentMenuRef.current = null;
    };
  }, [assignmentMenuRef, assignmentMenu]);
  const renderMembers = (workstreamId: string | null) => {
    const threads =
      workstreamId === null
        ? grouping.ungrouped
        : (grouping.groups.find((group) => group.workstream.workstreamId === workstreamId)
            ?.threads ?? []);
    return (
      <ul
        aria-label={
          workstreamId === null
            ? hasUnchecked
              ? otherThreadsLabel
              : "Unassigned threads"
            : "Workstream threads"
        }
        className="space-y-px px-1 pb-1"
      >
        {threads.map((thread) => {
          const key = nativeWorkstreamThreadKey(thread.environmentId, thread.id);
          const isDragging =
            dragged !== null &&
            nativeWorkstreamThreadKey(dragged.environmentId, dragged.id) === key;
          return (
            <li
              key={key}
              data-native-workstream-thread={key}
              className={`relative rounded ${canDrag(thread) ? "cursor-grab active:cursor-grabbing" : ""} ${isDragging ? "pointer-events-none z-30 bg-sidebar shadow-lg" : ""}`}
              style={isDragging ? { transform: `translateY(${offsetY}px)` } : undefined}
              onPointerDown={(event) => pointerDown(event, thread)}
              data-drop-position={
                dropTarget?.threadKey === key ? (dropTarget.after ? "after" : "before") : undefined
              }
            >
              {dropTarget?.threadKey === key ? (
                <div
                  aria-hidden
                  className={`pointer-events-none absolute inset-x-1 z-20 h-0.5 bg-primary ${dropTarget.after ? "bottom-0" : "top-0"}`}
                />
              ) : null}
              <ul>{props.renderThread(thread, assignmentMenu(thread))}</ul>
            </li>
          );
        })}
        {threads.length === 0 ? (
          <li className="px-2 py-2 text-xs text-muted-foreground">No active threads</li>
        ) : null}
      </ul>
    );
  };
  return (
    <div ref={rootRef}>
      {error ? (
        <p role="alert" className="px-2 py-1 text-xs text-destructive">
          {error}{" "}
          <button type="button" onClick={refreshWorkstreams}>
            Refresh
          </button>
        </p>
      ) : null}
      {busy ? (
        <p role="status" className="px-2 text-xs text-muted-foreground">
          Saving Workstream change…
        </p>
      ) : null}
      {controller.stale ? (
        <p role="status" className="px-2 text-xs text-muted-foreground">
          Showing the last loaded Workstreams.{" "}
          <button type="button" disabled={controller.loading || busy} onClick={refreshWorkstreams}>
            Refresh
          </button>
        </p>
      ) : assignmentIndex !== null && !assignmentIndex.complete ? (
        <p role="status" className="px-2 text-xs text-muted-foreground">
          Some thread assignments could not be loaded, so threads outside a group may still belong
          to one.
        </p>
      ) : assignmentIndex === null && controller.placements === null ? (
        <p className="px-2 text-xs text-muted-foreground">
          Thread assignments are unavailable until current placements can be verified.
        </p>
      ) : null}
      {hasConflict ? (
        <p role="alert" className="px-2 text-xs text-destructive">
          Some threads have conflicting Workstream assignments. Changes to them are blocked until
          they are verified again.
        </p>
      ) : null}
      <WorkstreamSidebarSection
        controller={controller}
        memberLabel={(reference) =>
          reference.identity.provider === "t3"
            ? threadsByKey.get(
                nativeWorkstreamThreadKey(
                  reference.identity.source_instance_id,
                  reference.identity.native_id,
                ),
              )?.title
            : undefined
        }
        renderMembers={renderMembers}
        unassignedLabel={hasUnchecked ? otherThreadsLabel : "Unassigned"}
        threadStatusSummaries={threadStatusSummaries}
        threadDropTarget={dropTarget && !dropTarget.threadKey ? dropTarget.workstreamId : undefined}
        threadActionBusy={busy}
      />
    </div>
  );
}
