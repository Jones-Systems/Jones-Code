import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import { workstreamBindingKey } from "@t3tools/client-runtime/state/workstreams";
import {
  useLayoutEffect,
  useMemo,
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

export function WorkstreamNativeSidebar(props: {
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
  readonly canReorder: (thread: EnvironmentThreadShell) => boolean;
  readonly reorder: (
    thread: EnvironmentThreadShell,
    neighbor: EnvironmentThreadShell,
    after: boolean,
  ) => Promise<void>;
}) {
  const { controller, grouping } = props;
  const statusGrouping = props.summaryGrouping ?? grouping;
  const threadStatusSummaries = useMemo(
    () => summarizeWorkstreamThreadStatuses(statusGrouping),
    [statusGrouping],
  );
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
  useLayoutEffect(() => {
    bindingRef.current = binding;
    sensorRef.current?.cancel();
    request.current?.abort();
    setDragged(null);
    setDropTarget(null);
    setError(null);
    setBusy(false);
    busyRef.current = false;
    return () => {
      bindingRef.current = null;
      sensorRef.current?.cancel();
      request.current?.abort();
    };
  }, [binding]);
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
    nativeThreadSourceAvailable(controller.registrationContext, thread) ||
    controller.assignsThroughOwnEnvironment?.(thread.environmentId) === true;
  // Stale snapshots are read-only; conflicting primaries or a known identity mismatch block
  // changes until re-verification.
  const canMove = (thread: EnvironmentThreadShell) =>
    canWrite &&
    controller.stale !== true &&
    controller.assignNativeThread !== undefined &&
    sourceAvailable(thread) &&
    !conflicted(thread);
  const hasUnchecked = grouping.ungrouped.some((thread) => !assignmentChecked(thread));
  const hasConflict = grouping.conflictingKeys.size > 0;
  // Unassigned is a claim only a complete, live index can make for every listed row.
  const otherThreadsLabel =
    grouping.assuranceByKey !== undefined ? "Assignment unknown" : "Other threads";
  const assignmentIndex = controller.assignmentIndex ?? null;
  const run = (operation: () => Promise<void>) => {
    if (busyRef.current || bindingRef.current !== binding) return;
    busyRef.current = true;
    setBusy(true);
    setError(null);
    const startedBinding = binding;
    void operation()
      .catch((cause: unknown) => {
        if (bindingRef.current === startedBinding) setError(workstreamFailureMessage(cause));
      })
      .finally(() => {
        if (bindingRef.current === startedBinding) {
          busyRef.current = false;
          setBusy(false);
        }
      });
  };
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
  const threadsByKey = useMemo(
    () =>
      new Map(
        grouping.ordered.map((thread) => [
          nativeWorkstreamThreadKey(thread.environmentId, thread.id),
          thread,
        ]),
      ),
    [grouping],
  );
  const pointerActions = useRef({
    threadsByKey,
    canMove,
    currentWorkstream,
    assignmentChecked,
    run,
    move,
    reorder: props.reorder,
    canReorder: props.canReorder,
  });
  useLayoutEffect(() => {
    pointerActions.current = {
      threadsByKey,
      canMove,
      currentWorkstream,
      assignmentChecked,
      run,
      move,
      reorder: props.reorder,
      canReorder: props.canReorder,
    };
  });
  const pointerDown = (event: ReactPointerEvent, thread: EnvironmentThreadShell) => {
    if (
      !event.isPrimary ||
      event.button !== 0 ||
      !canMove(thread) ||
      (event.target as HTMLElement).closest(
        "button, a, input, textarea, select, [contenteditable=true]",
      )
    )
      return;
    sensorRef.current?.cancel();
    const originY = event.clientY;
    let target: {
      destination: string | null;
      neighbor: EnvironmentThreadShell | undefined;
      after: boolean;
    } | null = null;
    const locate = (point: { x: number; y: number }) => {
      const element = document.elementFromPoint(point.x, point.y);
      const group = element?.closest<HTMLElement>("[data-native-workstream]");
      target = null;
      setDropTarget(null);
      if (!group || !rootRef.current?.contains(group)) return;
      const actions = pointerActions.current;
      if (
        !actions.canMove(thread) ||
        !actions.threadsByKey.has(nativeWorkstreamThreadKey(thread.environmentId, thread.id))
      )
        return;
      const destination = JSON.parse(group.dataset.nativeWorkstream!) as string | null;
      const row = element?.closest<HTMLElement>("[data-native-workstream-thread]");
      const neighbor = row?.dataset.nativeWorkstreamThread
        ? actions.threadsByKey.get(row.dataset.nativeWorkstreamThread)
        : undefined;
      const sameGroup =
        actions.assignmentChecked(thread) && actions.currentWorkstream(thread) === destination;
      if (
        sameGroup &&
        (!neighbor ||
          !actions.assignmentChecked(neighbor) ||
          (neighbor.environmentId === thread.environmentId && neighbor.id === thread.id) ||
          !actions.canReorder(thread) ||
          !actions.canReorder(neighbor))
      )
        return;
      const bounds = row?.getBoundingClientRect();
      const after = bounds !== undefined && point.y > bounds.top + bounds.height / 2;
      target = { destination, neighbor, after };
      setDropTarget({
        workstreamId: destination,
        ...(sameGroup && neighbor
          ? { threadKey: nativeWorkstreamThreadKey(neighbor.environmentId, neighbor.id), after }
          : {}),
      });
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
          locate(point);
          return false;
        },
      },
      onPending: () => {},
      onAbort: () => {},
      onStart: () => setDragged(thread),
      onMove: (point) => {
        setOffsetY(point.y - originY);
        locate(point);
      },
      onCancel: () => {
        target = null;
      },
      onEnd: () => {
        if (!target) return;
        const { destination, neighbor, after } = target;
        const actions = pointerActions.current;
        if (
          !actions.canMove(thread) ||
          !actions.threadsByKey.has(nativeWorkstreamThreadKey(thread.environmentId, thread.id))
        )
          return;
        actions.run(async () => {
          if (
            !actions.assignmentChecked(thread) ||
            actions.currentWorkstream(thread) !== destination
          )
            await actions.move(thread, destination);
          else if (
            neighbor &&
            actions.assignmentChecked(neighbor) &&
            actions.canReorder(thread) &&
            actions.canReorder(neighbor)
          )
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
                  ? "Assignment unavailable for this environment"
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
  const assignmentMenuRef = props.assignmentMenuRef;
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
              className={`relative rounded ${canMove(thread) ? "cursor-grab active:cursor-grabbing" : ""} ${isDragging ? "pointer-events-none z-30 bg-sidebar shadow-lg" : ""}`}
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
          <button
            type="button"
            onClick={() =>
              run(async () => {
                if (controller.retry) await controller.retry();
                else controller.refresh();
              })
            }
          >
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
          Showing the last loaded Workstreams. Refresh to update.
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
        renderMembers={renderMembers}
        unassignedLabel={hasUnchecked ? otherThreadsLabel : "Unassigned"}
        threadStatusSummaries={threadStatusSummaries}
        threadDropTarget={dropTarget && !dropTarget.threadKey ? dropTarget.workstreamId : undefined}
        threadActionBusy={busy}
      />
    </div>
  );
}
