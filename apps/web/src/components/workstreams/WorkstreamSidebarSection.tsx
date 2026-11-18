import {
  useEffect,
  useId,
  useLayoutEffect,
  useRef,
  useState,
  type DragEvent,
  type ReactNode,
} from "react";
import { useAtomValue } from "@effect/atom-react";
import {
  openPersonalWorkstreams,
  type PersonalThreadRef,
  type PersonalWorkstreamsController,
  type PersonalWorkstreamView,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import {
  ArrowDownIcon,
  ArrowUpIcon,
  EllipsisVerticalIcon,
  ChevronDownIcon,
  PaletteIcon,
  PencilIcon,
  TrashIcon,
} from "lucide-react";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import {
  Menu,
  MenuItem,
  MenuItemDetail,
  MenuPopup,
  MenuTrigger,
  MenuSeparator,
  MenuSub,
  MenuSubTrigger,
  MenuSubPopup,
  MenuRadioGroup,
  MenuRadioItem,
  MenuRadioItemIndicator,
  MenuGroup,
  MenuGroupLabel,
} from "../ui/menu";
import {
  AlertDialog,
  AlertDialogPopup,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogDescription,
  AlertDialogFooter,
} from "../ui/alert-dialog";
import { Dialog, DialogPopup, DialogHeader, DialogTitle } from "../ui/dialog";
import { ClosedWorkstreams } from "../../jones/workstreams/ClosedWorkstreams";
import { showPersonalFailure } from "../../jones/workstreams/personalNotifications";
import * as Schema from "effect/Schema";
import { useLocalStorage } from "../../hooks/useLocalStorage";
import {
  PersonalWorkstreamOptionsButton,
  PersonalWorkstreamVisibleThreads,
} from "../../jones/workstreams/PersonalWorkstreamDisplayControls";
import { PersonalSidebarGroupHeader } from "../../jones/workstreams/PersonalSidebarGroupHeader";
import {
  formatPersonalThreadCount,
  type PersonalThreadSummary,
} from "../../jones/workstreams/personalPresentation";
import { personalWorkstreamRailColor } from "../../jones/workstreams/personalRail";
import { PersonalWorkstreamTitleEditor } from "../../jones/workstreams/PersonalWorkstreamTitleEditor";
import { PersonalWorkstreamsHeader } from "../../jones/workstreams/PersonalWorkstreamsHeader";

import {
  usePersonalStatusFilter,
  useSharedPersonalStatusFilter,
} from "../../jones/workstreams/personalStatusFilter";
import {
  personalGroupCollapsedKey,
  personalNamedGroupsHiddenKey,
  setPersonalGroupsCollapsed,
} from "../../jones/workstreams/personalSidebarOrder";
import {
  usePersonalThreadWindow,
  type PersonalThreadLimit,
} from "../../jones/workstreams/personalThreadWindow";

const workstreamDragType = "application/x-jones-workstream";
const lifecycles = ["planned", "active", "paused", "completed", "deferred", "abandoned"] as const;
const lifecycleLabel = (value: string) => value[0]!.toUpperCase() + value.slice(1);

const collapsedSchema = Schema.Boolean;
const emptySummary: PersonalThreadSummary = {
  total: 0,
  running: 0,
  attention: 0,
  failed: 0,
  done: 0,
};
export function WorkstreamCreateForm({
  controller,
  open,
  onClose,
  onPendingChange,
}: {
  controller: PersonalWorkstreamsController;
  open: boolean;
  onClose: () => void;
  onPendingChange: (pending: boolean) => void;
}) {
  const [name, setName] = useState("");
  const [pending, setPending] = useState(false);
  const pendingRef = useRef(false);
  if (!open) return null;
  return (
    <form
      className="flex gap-1 p-2"
      onSubmit={async (event) => {
        event.preventDefault();
        if (pendingRef.current || !name.trim()) return;
        pendingRef.current = true;
        setPending(true);
        onPendingChange(true);
        try {
          const result = await controller.create({ name: name.trim() });
          if (result.ok) {
            setName("");
            onClose();
          } else showPersonalFailure(result.failure);
        } catch (error) {
          setPending(false);
          onPendingChange(false);
          pendingRef.current = false;
          throw error;
        }
        setPending(false);
        onPendingChange(false);
        pendingRef.current = false;
      }}
    >
      <Input
        autoFocus
        aria-label="Workstream name"
        maxLength={160}
        value={name}
        onChange={(event) => setName(event.target.value)}
      />
      <Button type="submit" disabled={pending || !name.trim()}>
        Create
      </Button>
      <Button type="button" variant="ghost" disabled={pending} onClick={onClose}>
        Cancel
      </Button>
    </form>
  );
}
function Group({
  item,
  controller,
  children,
  summary,
  rosterTotal,
  dragging,
  dropTarget,
  onDragStart,
  confirmingDelete,
  onDeleteRequested,
  memberCount,
}: {
  item: PersonalWorkstreamView;
  controller: PersonalWorkstreamsController;
  children: (window: { limit: PersonalThreadLimit; expanded: boolean }) => ReactNode;
  memberCount: number;
  summary: PersonalThreadSummary;
  rosterTotal: number;
  dragging: boolean;
  dropTarget: boolean;
  onDragStart: (event: DragEvent<HTMLDivElement>) => void;
  confirmingDelete: boolean;
  onDeleteRequested: () => void;
}) {
  const state = useAtomValue(controller.state);
  const [collapsed, setCollapsed] = useLocalStorage(
    personalGroupCollapsedKey(item.id),
    false,
    collapsedSchema,
  );
  const [editing, setEditing] = useState<"name" | "color" | null>(null);
  const threadWindow = usePersonalThreadWindow(item.id);
  const { filter: statusFilter, setFilter: setStatusFilter } = usePersonalStatusFilter(item.id);
  const overflowCount = memberCount - threadWindow.limit;
  const shownCount = threadWindow.expanded
    ? memberCount
    : Math.min(memberCount, threadWindow.limit);
  const [draft, setDraft] = useState("");
  const [pending, setPending] = useState(false);
  const pendingRef = useRef(false);
  const run = async (operation: () => ReturnType<typeof controller.remove>) => {
    if (pendingRef.current) return;
    pendingRef.current = true;
    setPending(true);
    return Promise.resolve()
      .then(operation)
      .then((result) => {
        if (!result.ok) showPersonalFailure(result.failure);
      })
      .finally(() => {
        pendingRef.current = false;
        setPending(false);
      });
  };
  const sectionRef = useRef<HTMLElement>(null);
  const finishRename = (restoreFocus = true) => {
    setEditing(null);
    if (restoreFocus)
      requestAnimationFrame(() =>
        sectionRef.current
          ?.querySelector<HTMLButtonElement>("[data-workstream-header] button")
          ?.focus(),
      );
  };
  const startRename = () => {
    if (!state.canCreate || pendingRef.current || confirmingDelete) return;
    setEditing("name");
  };
  const saveRename = (name: string) => {
    if (!state.canCreate || pendingRef.current)
      return Promise.reject(new Error("Rename is unavailable."));
    pendingRef.current = true;
    setPending(true);
    return Promise.resolve()
      .then(() => controller.rename(item.id, name))
      .then((result) => {
        if (!result.ok) {
          showPersonalFailure(result.failure);
          throw new Error("Rename could not be saved.");
        }
        // A blur save must preserve the field the user moved to, including
        // when focus changed while an Enter save was still pending.
        finishRename(sectionRef.current?.querySelector("input") === document.activeElement);
      })
      .finally(() => {
        pendingRef.current = false;
        setPending(false);
      });
  };
  const openWorkstreams = openPersonalWorkstreams(state);
  const index = openWorkstreams.findIndex((entry) => entry.id === item.id);
  return (
    <section
      ref={sectionRef}
      data-native-workstream={JSON.stringify(item.id)}
      data-thread-drop-target={dropTarget ? "" : undefined}
      className={`min-w-0 rounded-lg border-l-2 border-sidebar-border/70 ${dragging ? "opacity-50" : ""} ${dropTarget ? "ring-2 ring-inset ring-primary/70 forced-colors:outline forced-colors:outline-2 forced-colors:-outline-offset-2 forced-colors:outline-current" : ""}`}
      style={{
        borderLeftColor: personalWorkstreamRailColor(item),
        backgroundColor:
          dropTarget && collapsed
            ? "color-mix(in oklab, var(--primary) 3.25%, transparent)"
            : undefined,
      }}
    >
      <div
        data-workstream-header
        className="flex min-w-0 cursor-pointer"
        draggable={state.canCreate && !pending && !confirmingDelete && !editing}
        onDragStart={onDragStart}
      >
        <PersonalSidebarGroupHeader
          name={item.name}
          collapsed={collapsed}
          onToggle={() => {
            threadWindow.setExpanded(false);
            setCollapsed((value) => !value);
            return () => {
              setCollapsed(collapsed);
              threadWindow.setExpanded(threadWindow.expanded);
            };
          }}
          summary={summary}
          rosterTotal={rosterTotal}
          visibleCount={shownCount}
          statusFilter={statusFilter}
          onStatusFilterChange={setStatusFilter}
          {...(state.canCreate && !pending && !confirmingDelete
            ? { onRenameRequested: startRename }
            : {})}
          nameEditor={
            editing === "name" ? (
              <PersonalWorkstreamTitleEditor
                name={item.name}
                pending={pending}
                onSave={saveRename}
                onCancel={finishRename}
              />
            ) : null
          }
          actions={
            <div
              className="shrink-0"
              data-workstream-menu
              data-workstream-header-control
              draggable={false}
              onPointerDown={(event) => event.stopPropagation()}
              onClick={(event) => event.stopPropagation()}
              onDoubleClick={(event) => event.stopPropagation()}
              onDragStart={(event) => {
                event.preventDefault();
                event.stopPropagation();
              }}
            >
              <Menu>
                <MenuTrigger
                  aria-label={`Options for ${item.name}`}
                  render={<PersonalWorkstreamOptionsButton />}
                >
                  <EllipsisVerticalIcon aria-hidden className="size-[12px]" />
                </MenuTrigger>
                <MenuPopup align="end">
                  <MenuItem disabled={!state.canCreate || pending} onClick={startRename}>
                    <PencilIcon aria-hidden /> Rename
                  </MenuItem>
                  <MenuItem
                    disabled={!state.canCreate || pending}
                    onClick={() => {
                      setDraft(item.color ?? "");
                      setEditing("color");
                    }}
                  >
                    <PaletteIcon aria-hidden /> Change color
                  </MenuItem>
                  <MenuSeparator />
                  <MenuSub>
                    <MenuSubTrigger disabled={!state.canCreate || pending}>
                      Workstream state{" "}
                      <MenuItemDetail>{lifecycleLabel(item.lifecycle)}</MenuItemDetail>
                    </MenuSubTrigger>
                    <MenuSubPopup>
                      <MenuRadioGroup value={item.lifecycle} aria-label="Workstream state">
                        {lifecycles.map((lifecycle) => (
                          <MenuRadioItem
                            key={lifecycle}
                            value={lifecycle}
                            disabled={!state.canCreate || pending}
                            closeOnClick
                            onClick={() => {
                              if (item.lifecycle === lifecycle) return;
                              void run(async () => {
                                const result = await controller.setLifecycle(item.id, lifecycle);
                                return result.ok ? { ok: true, value: undefined } : result;
                              });
                            }}
                          >
                            <span className="flex items-center justify-between gap-4">
                              {lifecycleLabel(lifecycle)}
                              <MenuRadioItemIndicator />
                            </span>
                          </MenuRadioItem>
                        ))}
                      </MenuRadioGroup>
                    </MenuSubPopup>
                  </MenuSub>
                  <MenuSeparator />
                  <MenuItem onClick={() => setStatusFilter(null)}>
                    Use global status filter
                  </MenuItem>
                  <MenuSeparator />
                  <MenuGroup className="flex items-center justify-between">
                    <div className="flex min-w-0 flex-wrap items-baseline">
                      <MenuGroupLabel>Visible threads</MenuGroupLabel>
                      <span
                        className="pr-2 text-xs text-muted-foreground tabular-nums"
                        aria-live="polite"
                      >
                        {shownCount} of {memberCount} shown
                      </span>
                    </div>
                    <PersonalWorkstreamVisibleThreads
                      limit={threadWindow.limit}
                      onChange={(limit) => {
                        threadWindow.setLimit(limit);
                        threadWindow.setExpanded(false);
                      }}
                    />
                  </MenuGroup>
                  <MenuSeparator />
                  <MenuItem
                    disabled={!state.canCreate || pending || index === 0}
                    onClick={() =>
                      void run(async () => {
                        const result = await controller.reorder(item.id, {
                          afterId: openWorkstreams[index - 1]!.id,
                          beforeId: openWorkstreams[index - 2]?.id ?? null,
                        });
                        return result.ok ? { ok: true, value: undefined } : result;
                      })
                    }
                  >
                    <ArrowUpIcon aria-hidden /> Move up
                  </MenuItem>
                  <MenuItem
                    disabled={!state.canCreate || pending || index === openWorkstreams.length - 1}
                    onClick={() =>
                      void run(async () => {
                        const result = await controller.reorder(item.id, {
                          beforeId: openWorkstreams[index + 1]!.id,
                          afterId: openWorkstreams[index + 2]?.id ?? null,
                        });
                        return result.ok ? { ok: true, value: undefined } : result;
                      })
                    }
                  >
                    <ArrowDownIcon aria-hidden /> Move down
                  </MenuItem>
                  <MenuSeparator />
                  <MenuItem
                    variant="destructive"
                    disabled={!state.canCreate || pending}
                    onClick={onDeleteRequested}
                  >
                    <TrashIcon aria-hidden /> Delete Workstream…
                  </MenuItem>
                </MenuPopup>
              </Menu>
            </div>
          }
        />
      </div>
      {!collapsed ? (
        <>
          {children(threadWindow)}
          {overflowCount > 0 ? (
            <button
              data-workstream-window-footer
              type="button"
              className="flex h-4 w-full cursor-pointer items-center justify-center gap-1 rounded-sm px-2 text-2xs leading-none font-normal whitespace-nowrap text-sidebar-muted-foreground outline-hidden before:me-1 before:h-px before:min-w-0 before:flex-1 before:bg-sidebar-border/30 after:ms-1 after:h-px after:min-w-0 after:flex-1 after:bg-sidebar-border/30 hover:text-sidebar-foreground focus-visible:text-sidebar-foreground focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring pointer-coarse:min-h-6"
              aria-expanded={threadWindow.expanded}
              aria-label={
                threadWindow.expanded
                  ? `Show only ${threadWindow.limit} threads in ${item.name}`
                  : `Show ${overflowCount} more ${overflowCount === 1 ? "thread" : "threads"} in ${item.name}`
              }
              onClick={() => threadWindow.setExpanded((value) => !value)}
            >
              <ChevronDownIcon
                aria-hidden
                className={`size-2.5 shrink-0 ${threadWindow.expanded ? "rotate-180" : ""}`}
              />
              {threadWindow.expanded ? "Show fewer" : `Show ${overflowCount} more`}
            </button>
          ) : null}
        </>
      ) : null}
      {editing === "color" ? (
        <Dialog
          open
          onOpenChange={(open) => {
            if (!open && !pending) setEditing(null);
          }}
        >
          <DialogPopup>
            <DialogHeader>
              <DialogTitle>Workstream color</DialogTitle>
            </DialogHeader>
            <form
              onSubmit={async (event) => {
                event.preventDefault();
                if (pending) return;
                setPending(true);
                try {
                  const result = await controller.setColor(
                    item.id,
                    draft ? draft.toUpperCase() : null,
                  );
                  if (result.ok) setEditing(null);
                  else showPersonalFailure(result.failure);
                } catch (error) {
                  setPending(false);
                  throw error;
                }
                setPending(false);
              }}
            >
              <Input
                aria-label="Hex color"
                maxLength={7}
                value={draft}
                onChange={(event) => setDraft(event.target.value)}
              />
              {editing === "color" ? (
                <Button type="button" variant="ghost" onClick={() => setDraft("")}>
                  Automatic
                </Button>
              ) : null}
              <Button
                type="submit"
                disabled={pending || (draft !== "" && !/^#[0-9a-fA-F]{6}$/.test(draft))}
              >
                Save
              </Button>
            </form>
          </DialogPopup>
        </Dialog>
      ) : null}
    </section>
  );
}
export function WorkstreamSidebarSection({
  controller,
  renderMembers,
  onOpenHistoryThread,
  summaries,
  rosterTotals,
  unassignedSummary = emptySummary,
  memberCounts,
  dropTargetId,
}: {
  controller: PersonalWorkstreamsController;
  dropTargetId?: string | null | undefined;
  onOpenHistoryThread?: ((thread: PersonalThreadRef) => void) | undefined;
  renderMembers: (
    id: string | null,
    window?: { limit: PersonalThreadLimit; expanded: boolean },
  ) => ReactNode;
  memberCounts?: ReadonlyMap<string, number>;
  summaries?: ReadonlyMap<string, PersonalThreadSummary>;
  rosterTotals?: ReadonlyMap<string, number>;
  unassignedSummary?: PersonalThreadSummary;
}) {
  const state = useAtomValue(controller.state);
  const [unassignedCollapsed, setUnassignedCollapsed] = useLocalStorage(
    personalGroupCollapsedKey(null),
    false,
    collapsedSchema,
  );
  // Hiding is separate from each group's own collapsed state so showing again restores it exactly.
  const [namedHidden, setNamedHidden] = useLocalStorage(
    personalNamedGroupsHiddenKey,
    false,
    collapsedSchema,
  );
  const namedGroupsId = useId();
  const openWorkstreams = openPersonalWorkstreams(state);
  const namedIds = openWorkstreams.map((entry) => entry.id);
  const sharedStatus = useSharedPersonalStatusFilter(namedIds);
  const namedSummary = openWorkstreams.reduce(
    (total, entry) => {
      const summary = summaries?.get(entry.id) ?? emptySummary;
      return {
        running: total.running + summary.running,
        attention: total.attention + summary.attention,
        done: total.done + summary.done,
      };
    },
    { running: 0, attention: 0, done: 0 },
  );
  const containerRef = useRef<HTMLDivElement>(null);
  const [deleteTarget, setDeleteTarget] = useState<{ id: string; name: string } | null>(null);
  const [deletePending, setDeletePending] = useState(false);
  const [deleteFailed, setDeleteFailed] = useState(false);
  const deletePendingRef = useRef(false);
  const cancelDeleteRef = useRef<HTMLButtonElement>(null);
  const removedRef = useRef(false);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Restore failure focus after the committed DOM has enabled Cancel.
  useLayoutEffect(() => {
    if (deleteFailed && !deletePending) cancelDeleteRef.current?.focus();
  }, [deleteFailed, deletePending]);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Dismiss an idle confirmation when its live target disappears; pending optimistic removal retains ownership here.
  useEffect(() => {
    if (
      deleteTarget &&
      !deletePending &&
      !state.workstreams.some((entry) => entry.id === deleteTarget.id)
    ) {
      removedRef.current = true;
      // oxlint-disable-next-line react/set-state-in-effect -- External controller removal invalidates the independent confirmation target.
      setDeleteTarget(null);
    }
  }, [deleteTarget, deletePending, state.workstreams]);
  const draggedRef = useRef<string | null>(null);
  const [draggedId, setDraggedId] = useState<string | null>(null);
  const [insertion, setInsertion] = useState<{
    beforeId: string | null;
    afterId: string | null;
  } | null>(null);
  const reorderPending = useRef(false);
  const clearDrag = () => {
    draggedRef.current = null;
    setDraggedId(null);
    setInsertion(null);
  };
  const destination = (event: DragEvent<HTMLDivElement>, targetId: string | null) => {
    const source = draggedRef.current;
    if (
      !source ||
      !state.canCreate ||
      reorderPending.current ||
      !event.dataTransfer.types.includes(workstreamDragType)
    )
      return null;
    const sourceIndex = openWorkstreams.findIndex((entry) => entry.id === source);
    if (sourceIndex < 0) return null;
    const others = openWorkstreams.filter((entry) => entry.id !== source);
    let slot: number;
    if (targetId === null) {
      // Unassigned is a fixed tail: only its leading gap and header accept a drop.
      const header = event.currentTarget.querySelector("button");
      if (!header || event.clientY > header.getBoundingClientRect().bottom) return null;
      slot = others.length;
    } else {
      if (targetId === source) return null;
      const header = event.currentTarget.querySelector("[data-workstream-header]");
      if (!header) return null;
      // Rows can make a group very tall. Header halves must remain reachable
      // drop targets in both directions regardless of the group's member count.
      const rect = header.getBoundingClientRect();
      const index = others.findIndex((entry) => entry.id === targetId);
      if (index < 0) return null;
      slot = index + (event.clientY >= rect.top + rect.height / 2 ? 1 : 0);
    }
    if (slot === sourceIndex) return null;
    return { beforeId: others[slot - 1]?.id ?? null, afterId: others[slot]?.id ?? null };
  };
  const dragOver = (event: DragEvent<HTMLDivElement>, targetId: string | null) => {
    if (!draggedRef.current) return;
    event.stopPropagation();
    const next = destination(event, targetId);
    if (next) {
      event.preventDefault();
      event.dataTransfer.dropEffect = "move";
    }
    setInsertion((current) =>
      current?.beforeId === next?.beforeId && current?.afterId === next?.afterId ? current : next,
    );
  };
  const drop = (event: DragEvent<HTMLDivElement>, targetId: string | null) => {
    if (!draggedRef.current) return;
    event.preventDefault();
    event.stopPropagation();
    const source = draggedRef.current;
    const next = destination(event, targetId);
    clearDrag();
    if (!next) return;
    reorderPending.current = true;
    void Promise.resolve()
      .then(() => controller.reorder(source, next))
      .then((result) => {
        if (!result.ok) showPersonalFailure(result.failure);
      })
      .finally(() => {
        reorderPending.current = false;
      });
  };
  const focusAfterRemoval = (id: string) => {
    const index = openWorkstreams.findIndex((entry) => entry.id === id);
    const following = openWorkstreams.slice(index + 1).map((entry) => entry.id);
    requestAnimationFrame(() => {
      const sections = containerRef.current?.querySelectorAll<HTMLElement>(
        "[data-native-workstream]",
      );
      const target =
        following
          .map((candidate) =>
            Array.from(sections ?? []).find(
              (section) => section.dataset.nativeWorkstream === JSON.stringify(candidate),
            ),
          )
          .find(Boolean) ??
        Array.from(sections ?? []).find((section) => section.dataset.nativeWorkstream === "null");
      target?.querySelector<HTMLButtonElement>("button")?.focus();
    });
  };
  const confirmDeletion = async () => {
    if (
      !deleteTarget ||
      deletePendingRef.current ||
      !state.canCreate ||
      !state.workstreams.some((entry) => entry.id === deleteTarget.id)
    )
      return;
    deletePendingRef.current = true;
    setDeletePending(true);
    setDeleteFailed(false);
    return Promise.resolve()
      .then(() => controller.remove(deleteTarget.id))
      .then((result) => {
        if (result.ok) {
          removedRef.current = true;
          setDeleteTarget(null);
          focusAfterRemoval(deleteTarget.id);
        } else {
          showPersonalFailure(result.failure);
          setDeleteFailed(true);
        }
      })
      .finally(() => {
        deletePendingRef.current = false;
        setDeletePending(false);
      });
  };
  const insertionLine = (
    <div
      aria-hidden
      data-workstream-insertion
      className="pointer-events-none absolute inset-x-0 top-0 z-10 h-0.5 bg-primary forced-colors:bg-[Highlight]"
    />
  );
  return (
    <div
      ref={containerRef}
      aria-label="Workstreams"
      onDragEnd={clearDrag}
      onDrop={clearDrag}
      onKeyDown={(event) => {
        if (event.key === "Escape") clearDrag();
      }}
      onPointerDownCapture={(event) => {
        // Alert dialogs ignore outside presses by default; this flow allows cancel until submitted.
        const target = event.target;
        if (
          deleteTarget &&
          !deletePendingRef.current &&
          target instanceof Element &&
          target.closest(
            '[data-slot="alert-dialog-viewport"], [data-slot="alert-dialog-backdrop"]',
          ) &&
          !target.closest('[data-slot="alert-dialog-popup"]')
        )
          setDeleteTarget(null);
      }}
      onDragLeave={(event) => {
        if (!event.currentTarget.contains(event.relatedTarget as Node | null)) setInsertion(null);
      }}
    >
      {openWorkstreams.length > 0 ? (
        <PersonalWorkstreamsHeader
          summary={namedSummary}
          hidden={namedHidden}
          onHiddenChange={setNamedHidden}
          onExpandAll={() => setPersonalGroupsCollapsed(namedIds, false)}
          onCollapseAll={() => setPersonalGroupsCollapsed(namedIds, true)}
          statusFilter={sharedStatus.filter}
          onStatusFilterChange={sharedStatus.setFilter}
          controlsId={namedGroupsId}
        />
      ) : null}
      {/* The container stays mounted so the disclosure's aria-controls always resolves. */}
      <div id={namedGroupsId} hidden={namedHidden}>
        {!namedHidden
          ? openWorkstreams.map((item) => (
              <div
                key={item.id}
                data-workstream-group-slot
                className="relative py-1"
                onDragOver={(event) => dragOver(event, item.id)}
                onDrop={(event) => drop(event, item.id)}
              >
                {insertion?.afterId === item.id ? insertionLine : null}
                <Group
                  item={item}
                  controller={controller}
                  summary={summaries?.get(item.id) ?? emptySummary}
                  rosterTotal={rosterTotals?.get(item.id) ?? summaries?.get(item.id)?.total ?? 0}
                  memberCount={memberCounts?.get(item.id) ?? 0}
                  dragging={draggedId === item.id}
                  dropTarget={dropTargetId === item.id}
                  confirmingDelete={deleteTarget?.id === item.id}
                  onDeleteRequested={() => {
                    removedRef.current = false;
                    setDeleteTarget({ id: item.id, name: item.name });
                  }}
                  onDragStart={(event) => {
                    if (
                      event.target instanceof Element &&
                      event.target.closest("[data-workstream-header-control]")
                    ) {
                      event.preventDefault();
                      return;
                    }
                    if (!state.canCreate || reorderPending.current) {
                      event.preventDefault();
                      return;
                    }
                    event.dataTransfer.setData(workstreamDragType, item.id);
                    event.dataTransfer.effectAllowed = "move";
                    draggedRef.current = item.id;
                    setDraggedId(item.id);
                    setInsertion(null);
                  }}
                >
                  {(window) => renderMembers(item.id, window)}
                </Group>
              </div>
            ))
          : null}
      </div>
      <div
        data-workstream-group-slot
        className="relative py-1"
        onDragOver={(event) => dragOver(event, null)}
        onDrop={(event) => drop(event, null)}
      >
        {insertion?.afterId === null ? insertionLine : null}
        <section
          data-native-workstream="null"
          style={{
            backgroundColor:
              dropTargetId === null && unassignedCollapsed
                ? "color-mix(in oklab, var(--primary) 3.25%, transparent)"
                : undefined,
          }}
          data-thread-drop-target={dropTargetId === null ? "" : undefined}
          className={`min-w-0 rounded-md ${dropTargetId === null ? "ring-2 ring-inset ring-primary/70 forced-colors:outline forced-colors:outline-2 forced-colors:-outline-offset-2 forced-colors:outline-current" : ""}`}
        >
          <button
            type="button"
            aria-label={`${unassignedCollapsed ? "Expand" : "Collapse"} Unassigned`}
            aria-description={formatPersonalThreadCount(unassignedSummary.total)}
            aria-expanded={!unassignedCollapsed}
            onClick={() => setUnassignedCollapsed((value) => !value)}
            className="flex w-full min-w-0 cursor-pointer items-center gap-2 rounded-md px-2 py-1 text-sm text-sidebar-muted-foreground hover:bg-sidebar-row-hover focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring"
          >
            <span className="truncate">Unassigned</span>
            <span className="ml-auto shrink-0 text-xs tabular-nums">
              {formatPersonalThreadCount(unassignedSummary.total)}
            </span>
          </button>
          {!unassignedCollapsed ? renderMembers(null) : null}
        </section>
      </div>
      <ClosedWorkstreams controller={controller} state={state} onOpenThread={onOpenHistoryThread} />
      {deleteTarget ? (
        <AlertDialog
          open
          onOpenChange={(open, details) => {
            if (!open && deletePendingRef.current) details.cancel();
            else if (!open) setDeleteTarget(null);
          }}
        >
          <AlertDialogPopup
            initialFocus={cancelDeleteRef}
            finalFocus={() => {
              if (removedRef.current) return false;
              const section = Array.from(
                containerRef.current?.querySelectorAll<HTMLElement>("[data-native-workstream]") ??
                  [],
              ).find((entry) => entry.dataset.nativeWorkstream === JSON.stringify(deleteTarget.id));
              return (
                section?.querySelector<HTMLButtonElement>("[data-workstream-menu] button") ??
                section?.querySelector<HTMLButtonElement>("button") ??
                false
              );
            }}
          >
            <AlertDialogHeader>
              <AlertDialogTitle>Delete “{deleteTarget.name}”?</AlertDialogTitle>
              <AlertDialogDescription>
                Threads move to Unassigned. No threads are deleted. This cannot be undone.
              </AlertDialogDescription>
            </AlertDialogHeader>
            <AlertDialogFooter>
              <Button
                ref={cancelDeleteRef}
                variant="outline"
                disabled={deletePending}
                onClick={() => setDeleteTarget(null)}
              >
                Cancel
              </Button>
              <Button
                variant="destructive"
                disabled={
                  deletePending ||
                  !state.canCreate ||
                  !state.workstreams.some((entry) => entry.id === deleteTarget.id)
                }
                onClick={() => void confirmDeletion()}
              >
                {deletePending ? "Deleting…" : "Delete Workstream"}
              </Button>
            </AlertDialogFooter>
          </AlertDialogPopup>
        </AlertDialog>
      ) : null}
    </div>
  );
}
