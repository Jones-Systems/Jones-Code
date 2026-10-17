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
import { canEditWorkstreams, planNativeMembership } from "./nativeWorkstreamActions";
import {
  nativeWorkstreamThreadKey,
  type NativeWorkstreamThreadGrouping,
} from "./nativeThreadGrouping";

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
  const canMove = (thread: EnvironmentThreadShell) =>
    canWrite &&
    controller.placements !== null &&
    inventory.has(nativeWorkstreamThreadKey(thread.environmentId, thread.id));
  const run = (operation: () => Promise<void>) => {
    if (busyRef.current || bindingRef.current !== binding) return;
    busyRef.current = true;
    setBusy(true);
    setError(null);
    const startedBinding = binding;
    void operation()
      .catch((cause: unknown) => {
        if (bindingRef.current === startedBinding)
          setError(cause instanceof Error ? cause.message : "Workstream action failed.");
      })
      .finally(() => {
        if (bindingRef.current === startedBinding) {
          busyRef.current = false;
          setBusy(false);
        }
      });
  };
  const move = async (thread: EnvironmentThreadShell, destination: string | null) => {
    const data = controller.data;
    const placements = controller.placements;
    if (!data || !placements || !canMove(thread))
      throw new Error("Refresh thread placements before changing membership.");
    const current =
      statusGrouping.groups.find((group) =>
        group.threads.some(
          (item) => item.environmentId === thread.environmentId && item.id === thread.id,
        ),
      )?.workstream.workstreamId ?? null;
    if (current === destination) return;
    const detailId = destination ?? current;
    if (!detailId) return;
    request.current?.abort();
    const abort = new AbortController();
    request.current = abort;
    const startedBinding = binding;
    const detail = await controller.loadDetail(detailId, { signal: abort.signal });
    abort.signal.throwIfAborted();
    if (bindingRef.current !== startedBinding)
      throw new Error("Workstream access changed. Refresh before retrying.");
    const context = detail.detail.context;
    if (
      context.owner_id !== data.binding.ownerId ||
      context.server_generation !== data.binding.serverGeneration ||
      context.registry_version !== data.binding.registryVersion
    )
      throw new Error("Workstreams changed. Refresh before retrying.");
    const action = planNativeMembership({
      data,
      placements,
      references: detail.references.items,
      destinationMemberships: detail.memberships.items,
      thread,
      destination,
      now: Date.now(),
    });
    if (!action) return;
    const commandId = await workstreamCommandId();
    abort.signal.throwIfAborted();
    if (bindingRef.current !== startedBinding)
      throw new Error("Workstream access changed. Refresh before retrying.");
    const receipt = await controller.submit({
      command_id: commandId,
      expected_server_generation: data.binding.serverGeneration,
      expected_registry_version: data.binding.registryVersion,
      action,
    });
    if (receipt.state !== "committed")
      throw new Error(`Membership change ${receipt.state}. Refresh before retrying.`);
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
      const sameGroup = actions.currentWorkstream(thread) === destination;
      if (
        sameGroup &&
        (!neighbor ||
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
          if (actions.currentWorkstream(thread) !== destination)
            await actions.move(thread, destination);
          else if (neighbor && actions.canReorder(thread) && actions.canReorder(neighbor))
            await actions.reorder(thread, neighbor, after);
        });
      },
    });
  };
  const assignmentMenu = (thread: EnvironmentThreadShell): NativeThreadAssignmentMenu => {
    const workstreamId = currentWorkstream(thread);
    const secondary =
      statusGrouping.secondaryWorkstreamIdsByKey.get(
        nativeWorkstreamThreadKey(thread.environmentId, thread.id),
      ) ?? [];
    return {
      items: [
        ...(controller.data?.items ?? []).map((item) => ({
          id: `assign-workstream:group:${item.workstreamId}` as const,
          label: `${item.name}${item.workstreamId === workstreamId ? " (primary)" : secondary.includes(item.workstreamId) ? " (secondary)" : ""}`,
          disabled: !canMove(thread) || item.workstreamId === workstreamId,
        })),
        {
          id: "assign-workstream:remove",
          label: "Unassigned (remove primary)",
          separatorBefore: true,
          disabled: !canMove(thread) || workstreamId === null,
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
        aria-label={workstreamId === null ? "Unassigned threads" : "Workstream threads"}
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
          <button type="button" onClick={controller.refresh}>
            Refresh
          </button>
        </p>
      ) : null}
      {busy ? (
        <p role="status" className="px-2 text-xs text-muted-foreground">
          Saving Workstream change…
        </p>
      ) : null}
      {controller.placements === null ? (
        <p className="px-2 text-xs text-muted-foreground">
          Thread assignments are unavailable until current placements can be verified.
        </p>
      ) : null}
      <WorkstreamSidebarSection
        controller={controller}
        renderMembers={renderMembers}
        threadStatusSummaries={threadStatusSummaries}
        threadDropTarget={dropTarget && !dropTarget.threadKey ? dropTarget.workstreamId : undefined}
        threadActionBusy={busy}
      />
    </div>
  );
}
