import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import { workstreamBindingKey } from "@t3tools/client-runtime/state/workstreams";
import { GripVerticalIcon, MoreHorizontalIcon } from "lucide-react";
import { useLayoutEffect, useMemo, useRef, useState, type ReactNode, type DragEvent } from "react";
import type { WorkstreamListView } from "../../state/workstreams";
import { Button } from "../ui/button";
import { Menu, MenuItem, MenuPopup, MenuTrigger } from "../ui/menu";
import { WorkstreamSidebarSection, workstreamCommandId } from "./WorkstreamSidebarSection";
import { canEditWorkstreams, planNativeMembership } from "./nativeWorkstreamActions";
import {
  nativeWorkstreamThreadKey,
  secondaryNativeWorkstreamLabels,
  type NativeWorkstreamThreadGrouping,
} from "./nativeThreadGrouping";

import { summarizeWorkstreamThreadStatuses } from "./workstreamThreadStatus";

export function WorkstreamNativeSidebar(props: {
  readonly controller: WorkstreamListView;
  readonly grouping: NativeWorkstreamThreadGrouping<EnvironmentThreadShell>;
  readonly summaryGrouping?: NativeWorkstreamThreadGrouping<EnvironmentThreadShell>;
  readonly renderThread: (thread: EnvironmentThreadShell) => ReactNode;
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
  const [menuThread, setMenuThread] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const busyRef = useRef(false);
  const [error, setError] = useState<string | null>(null);
  const request = useRef<AbortController | null>(null);
  const binding = controller.data ? workstreamBindingKey(controller.data.binding) : null;
  const bindingRef = useRef(binding);
  useLayoutEffect(() => {
    bindingRef.current = binding;
    request.current?.abort();
    setDragged(null);
    setDropTarget(null);
    setError(null);
    setBusy(false);
    busyRef.current = false;
    return () => request.current?.abort();
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
    if (busyRef.current) return;
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
      grouping.groups.find((group) => group.threads.includes(thread))?.workstream.workstreamId ??
      null;
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
    grouping.groups.find((group) => group.threads.includes(thread))?.workstream.workstreamId ??
    null;
  const acceptsDrop = (destination: string | null, neighbor?: EnvironmentThreadShell) =>
    dragged !== null &&
    canMove(dragged) &&
    (currentWorkstream(dragged) !== destination ||
      (!!neighbor &&
        neighbor !== dragged &&
        props.canReorder(dragged) &&
        props.canReorder(neighbor)));
  const dragOver = (
    event: DragEvent,
    destination: string | null,
    neighbor?: EnvironmentThreadShell,
  ) => {
    if (!dragged) return false;
    event.stopPropagation();
    if (!acceptsDrop(destination, neighbor)) {
      event.dataTransfer.dropEffect = "none";
      setDropTarget(null);
      return true;
    }
    event.preventDefault();
    event.dataTransfer.dropEffect = "move";
    const bounds = event.currentTarget.getBoundingClientRect();
    setDropTarget({
      workstreamId: destination,
      ...(neighbor && currentWorkstream(dragged) === destination
        ? {
            threadKey: nativeWorkstreamThreadKey(neighbor.environmentId, neighbor.id),
            after: event.clientY > bounds.top + bounds.height / 2,
          }
        : {}),
    });
    return true;
  };
  const drop = (
    event: DragEvent,
    destination: string | null,
    neighbor?: EnvironmentThreadShell,
  ) => {
    if (!dragged) return false;
    event.preventDefault();
    event.stopPropagation();
    const thread = dragged;
    const bounds = event.currentTarget.getBoundingClientRect();
    const after = event.clientY > bounds.top + bounds.height / 2;
    const accepted = acceptsDrop(destination, neighbor);
    setDragged(null);
    setDropTarget(null);
    if (!accepted) return true;
    const current =
      grouping.groups.find((group) => group.threads.includes(thread))?.workstream.workstreamId ??
      null;
    run(async () => {
      if (current !== destination) await move(thread, destination);
      else if (
        neighbor &&
        neighbor !== thread &&
        props.canReorder(thread) &&
        props.canReorder(neighbor)
      )
        await props.reorder(thread, neighbor, after);
    });
    return true;
  };
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
        {threads.map((thread, index) => {
          const key = nativeWorkstreamThreadKey(thread.environmentId, thread.id);
          const secondary = secondaryNativeWorkstreamLabels(grouping, thread);
          return (
            <li
              key={key}
              className={`relative rounded ${canMove(thread) ? "cursor-grab active:cursor-grabbing" : ""} ${dragged === thread ? "opacity-50" : ""}`}
              draggable={canMove(thread)}
              data-drop-position={
                dropTarget?.threadKey === key ? (dropTarget.after ? "after" : "before") : undefined
              }
              onDragStartCapture={(event) => {
                event.stopPropagation();
                const selection = window.getSelection();
                if (
                  !canMove(thread) ||
                  (event.target as HTMLElement).closest(
                    "input, textarea, select, [contenteditable=true]",
                  ) ||
                  (selection &&
                    !selection.isCollapsed &&
                    event.currentTarget.contains(selection.anchorNode))
                ) {
                  event.preventDefault();
                  return;
                }
                event.dataTransfer.effectAllowed = "move";
                event.dataTransfer.setData("application/x-t3-workstream-thread", key);
                setDragged(thread);
                setDropTarget(null);
              }}
              onDragEndCapture={(event) => {
                event.stopPropagation();
                setDragged(null);
                setDropTarget(null);
              }}
              onDragOverCapture={(event) => dragOver(event, workstreamId, thread)}
              onDropCapture={(event) => drop(event, workstreamId, thread)}
              onDragLeave={(event) => {
                if (!event.currentTarget.contains(event.relatedTarget as Node | null))
                  setDropTarget(null);
              }}
            >
              {dropTarget?.threadKey === key ? (
                <div
                  aria-hidden
                  className={`pointer-events-none absolute inset-x-1 z-20 h-0.5 bg-primary ${dropTarget.after ? "bottom-0" : "top-0"}`}
                />
              ) : null}
              <ul>{props.renderThread(thread)}</ul>
              <div className="flex items-center gap-1 px-2 pb-1 text-xs text-muted-foreground">
                <button
                  type="button"
                  draggable={canMove(thread)}
                  disabled={!canMove(thread)}
                  aria-label={`Drag ${thread.title} to a Workstream or reorder`}
                  className="cursor-grab rounded p-1 focus-visible:ring-2 focus-visible:ring-ring disabled:opacity-40"
                >
                  <GripVerticalIcon aria-hidden className="size-3" />
                </button>
                <span className="min-w-0 flex-1 truncate">
                  {secondary.length ? `Also in ${secondary.join(", ")}` : ""}
                </span>
                <Menu onOpenChange={(open) => setMenuThread(open ? key : null)}>
                  <MenuTrigger
                    aria-label={`Workstream actions for ${thread.title}`}
                    render={<Button size="icon-micro" variant="ghost-muted" />}
                  >
                    <MoreHorizontalIcon />
                  </MenuTrigger>
                  <MenuPopup align="end">
                    {(menuThread === key ? (controller.data?.items ?? []) : []).map((item) => (
                      <MenuItem
                        key={item.workstreamId}
                        disabled={!canMove(thread) || item.workstreamId === workstreamId}
                        onClick={() => run(() => move(thread, item.workstreamId))}
                      >
                        {workstreamId === null ? "Assign to" : "Move to"} {item.name}
                      </MenuItem>
                    ))}
                    <MenuItem
                      disabled={!canMove(thread) || workstreamId === null}
                      onClick={() => run(() => move(thread, null))}
                    >
                      Remove from Workstream
                    </MenuItem>
                    <MenuItem
                      disabled={
                        !canWrite ||
                        index === 0 ||
                        !props.canReorder(thread) ||
                        !props.canReorder(threads[index - 1] ?? thread)
                      }
                      onClick={() => run(() => props.reorder(thread, threads[index - 1]!, false))}
                    >
                      Move thread up
                    </MenuItem>
                    <MenuItem
                      disabled={
                        !canWrite ||
                        index === threads.length - 1 ||
                        !props.canReorder(thread) ||
                        !props.canReorder(threads[index + 1] ?? thread)
                      }
                      onClick={() => run(() => props.reorder(thread, threads[index + 1]!, true))}
                    >
                      Move thread down
                    </MenuItem>
                  </MenuPopup>
                </Menu>
              </div>
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
    <>
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
        onThreadDrop={drop}
        onThreadDragOver={dragOver}
        threadDropTarget={dropTarget && !dropTarget.threadKey ? dropTarget.workstreamId : undefined}
        onThreadDragLeave={() => setDropTarget(null)}
        threadActionBusy={busy}
      />
    </>
  );
}
