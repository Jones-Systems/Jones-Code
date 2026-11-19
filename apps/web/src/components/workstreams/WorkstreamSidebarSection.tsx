import { useClientSettings } from "../../hooks/useSettings";
import "../../jones/sidebarAppearance/sidebarAppearance.workstreams.css";
import { pickSnoozePreset, resolveSnoozePresets, type SnoozePreset } from "../Sidebar.snooze";
import { requestCustomSnooze } from "../CustomSnoozeDialog";
import {
  useEffect,
  useId,
  useLayoutEffect,
  useRef,
  useState,
  type CSSProperties,
  type DragEvent,
  type ReactNode,
} from "react";
import { useAtomValue } from "@effect/atom-react";
import {
  openPersonalWorkstreams,
  type PersonalResult,
  type PersonalThreadRef,
  type PersonalWorkstreamsController,
  type PersonalWorkstreamView,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import {
  AlarmClockIcon,
  ArrowDownIcon,
  ArrowUpIcon,
  ChevronDownIcon,
  PencilIcon,
  PlusIcon,
  TrashIcon,
} from "lucide-react";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import {
  Menu,
  MenuItem,
  MenuItemDetail,
  MenuPopup,
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
import { ClosedWorkstreams } from "../../jones/workstreams/ClosedWorkstreams";
import { showPersonalFailure } from "../../jones/workstreams/personalNotifications";
import * as Schema from "effect/Schema";
import { useLocalStorage } from "../../hooks/useLocalStorage";
import { PersonalWorkstreamVisibleThreads } from "../../jones/workstreams/PersonalWorkstreamDisplayControls";
import { PersonalSidebarGroupHeader } from "../../jones/workstreams/PersonalSidebarGroupHeader";
import {
  formatPersonalThreadCount,
  type PersonalThreadSummary,
} from "../../jones/workstreams/personalPresentation";
import {
  personalWorkstreamAutomaticColors,
  personalWorkstreamGroupColors,
} from "../../jones/workstreams/personalRail";
import { PersonalWorkstreamColorMenu } from "../../jones/workstreams/PersonalWorkstreamColorMenu";
import { PersonalWorkstreamTitleEditor } from "../../jones/workstreams/PersonalWorkstreamTitleEditor";
import {
  WorkstreamNameIconMenu,
  WorkstreamNameIconPicker,
  type WorkstreamNameIconProject,
} from "../../jones/workstreams/WorkstreamNameIconPicker";
import { ProjectFavicon } from "../ProjectFavicon";
import { PersonalWorkstreamsHeader } from "../../jones/workstreams/PersonalWorkstreamsHeader";
import type { WorkstreamThreadAction } from "../../jones/workstreams/useStartWorkstreamThread";

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
import {
  finishWorkstreamReferenceDrag,
  startWorkstreamReferenceDrag,
  workstreamDragType,
  type WorkstreamReference,
} from "../../jones/workstreams/workstreamReferenceDrop";

function currentSnoozePresets(
  timestampFormat: Parameters<typeof resolveSnoozePresets>[1],
  environmentId: Parameters<typeof resolveSnoozePresets>[2],
) {
  return resolveSnoozePresets(new Date(), timestampFormat, environmentId);
}

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
  nameIconProjects = [],
  canSetNameIcon = false,
  open,
  onClose,
  onPendingChange,
}: {
  controller: PersonalWorkstreamsController;
  nameIconProjects?: readonly WorkstreamNameIconProject[] | undefined;
  canSetNameIcon?: boolean;
  open: boolean;
  onClose: () => void;
  onPendingChange: (pending: boolean) => void;
}) {
  const [name, setName] = useState("");
  const [nameIconProjectKey, setNameIconProjectKey] = useState<string | null>(null);
  const [pending, setPending] = useState(false);
  const pendingRef = useRef(false);
  if (!open) return null;
  return (
    <form
      className="grid grid-cols-[minmax(0,1fr)_auto_auto] items-start gap-1 p-2"
      onSubmit={async (event) => {
        event.preventDefault();
        if (pendingRef.current || !name.trim() || (nameIconProjectKey && !canSetNameIcon)) return;
        pendingRef.current = true;
        setPending(true);
        onPendingChange(true);
        try {
          const result = await controller.create({
            name: name.trim(),
            ...(nameIconProjectKey ? { nameIconProjectKey } : {}),
          });
          if (result.ok) {
            setName("");
            setNameIconProjectKey(null);
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
        size="compact"
        aria-label="Workstream name"
        placeholder="Workstream name"
        maxLength={160}
        value={name}
        onChange={(event) => setName(event.target.value)}
      />
      <Button
        type="submit"
        size="compact"
        disabled={pending || !name.trim() || Boolean(nameIconProjectKey && !canSetNameIcon)}
      >
        Create
      </Button>
      <Button
        type="button"
        size="compact"
        variant="ghost"
        disabled={pending}
        onClick={() => {
          setNameIconProjectKey(null);
          onClose();
        }}
      >
        Cancel
      </Button>
      {canSetNameIcon || nameIconProjectKey !== null ? (
        <div className="col-start-1 row-start-2 min-w-0">
          <WorkstreamNameIconPicker
            projects={nameIconProjects}
            value={nameIconProjectKey}
            onChange={setNameIconProjectKey}
            disabled={pending}
          />
        </div>
      ) : null}
    </form>
  );
}
function Group({
  item,
  automaticColor,
  nameIconProjects,
  newThreadAction,
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
  snoozedIds,
}: {
  item: PersonalWorkstreamView;
  automaticColor: string | undefined;
  nameIconProjects: readonly WorkstreamNameIconProject[];
  newThreadAction: WorkstreamThreadAction | undefined;
  controller: PersonalWorkstreamsController;
  children: (window: { limit: PersonalThreadLimit; expanded: boolean }) => ReactNode;
  memberCount: number;
  snoozedIds: ReadonlySet<string>;
  summary: PersonalThreadSummary;
  rosterTotal: number;
  dragging: boolean;
  dropTarget: boolean;
  onDragStart: (event: DragEvent<HTMLDivElement>) => void;
  confirmingDelete: boolean;
  onDeleteRequested: () => void;
}) {
  const state = useAtomValue(controller.state);
  const colors = personalWorkstreamGroupColors(item, automaticColor);
  const [collapsed, setCollapsed] = useLocalStorage(
    personalGroupCollapsedKey(item.id),
    false,
    collapsedSchema,
  );
  const [editing, setEditing] = useState<"name" | null>(null);
  const threadWindow = usePersonalThreadWindow(item.id);
  const { filter: statusFilter, setFilter: setStatusFilter } = usePersonalStatusFilter(item.id);
  const overflowCount = memberCount - threadWindow.limit;
  const shownCount = threadWindow.expanded
    ? memberCount
    : Math.min(memberCount, threadWindow.limit);
  const setNameIcon = controller.setNameIcon;
  const nameIconProject = nameIconProjects.find(
    (project) => project.projectKey === item.nameIcon?.projectKey,
  );
  const [pending, setPending] = useState(false);
  const pendingRef = useRef(false);
  const run = async (operation: () => Promise<PersonalResult<unknown>>) => {
    if (pendingRef.current) return;
    pendingRef.current = true;
    setPending(true);
    return Promise.resolve()
      .then(operation)
      .then((result) => {
        if (!result.ok) showPersonalFailure(result.failure);
        return result;
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
  const openWorkstreams = openPersonalWorkstreams(state).filter(
    (entry) => !snoozedIds.has(entry.id),
  );
  const timestampFormat = useClientSettings((settings) => settings.timestampFormat);
  const snoozeEnvironmentId = state.environments.find(
    (entry) => entry.status === "ready",
  )?.environmentId;
  const [menuOpen, setMenuOpen] = useState(false);
  const [menuPoint, setMenuPoint] = useState<{ x: number; y: number } | null>(null);
  const [snoozePresets, setSnoozePresets] = useState<readonly SnoozePreset[]>([]);
  const openMenu = (open: boolean) => {
    setMenuOpen(open);
    if (open && snoozeEnvironmentId)
      setSnoozePresets(currentSnoozePresets(timestampFormat, snoozeEnvironmentId));
  };
  const snooze = async (preset: SnoozePreset | "custom") => {
    if (!controller.setSnoozedUntil || !snoozeEnvironmentId) return;
    const choice =
      preset === "custom"
        ? await requestCustomSnooze("workstream")
        : pickSnoozePreset(snoozePresets, preset.id, timestampFormat, snoozeEnvironmentId);
    if (!choice) return;
    await run(async () => {
      const result = await controller.setSnoozedUntil!(item.id, choice.snoozedUntil);
      return result.ok ? { ok: true, value: undefined } : result;
    });
  };
  const index = openWorkstreams.findIndex((entry) => entry.id === item.id);
  return (
    <section
      ref={sectionRef}
      data-native-workstream={JSON.stringify(item.id)}
      data-jones-board-section="workstream"
      data-thread-drop-target={dropTarget ? "" : undefined}
      className={`min-w-0 rounded-lg ${dragging ? "opacity-50" : ""} ${dropTarget ? "ring-2 ring-inset ring-primary/70 forced-colors:outline forced-colors:outline-2 forced-colors:-outline-offset-2 forced-colors:outline-current" : ""}`}
      style={
        {
          "--jones-board-section-rail": colors.rail,
          "--jones-board-section-accent": dropTarget && collapsed ? "transparent" : colors.color,
          backgroundColor:
            dropTarget && collapsed
              ? "color-mix(in oklab, var(--primary) 3.25%, transparent)"
              : undefined,
        } as CSSProperties
      }
    >
      <div
        data-workstream-header
        className="flex min-w-0 cursor-pointer"
        draggable={!pending && !confirmingDelete && !editing}
        onDragStart={onDragStart}
        onContextMenu={(event) => {
          if (event.target instanceof HTMLElement && event.target.closest("input")) return;
          event.preventDefault();
          event.stopPropagation();
          const bounds = event.currentTarget.getBoundingClientRect();
          setMenuPoint(
            event.clientX || event.clientY
              ? { x: event.clientX, y: event.clientY }
              : { x: bounds.left, y: bounds.bottom },
          );
          openMenu(true);
        }}
        onKeyDown={(event) => {
          if (event.key !== "ContextMenu" && !(event.shiftKey && event.key === "F10")) return;
          if (event.target instanceof Element && event.target.closest("input")) return;
          event.preventDefault();
          event.stopPropagation();
          const bounds = event.currentTarget.getBoundingClientRect();
          setMenuPoint({ x: bounds.left, y: bounds.bottom });
          openMenu(true);
        }}
      >
        <PersonalSidebarGroupHeader
          name={item.name}
          hasContextMenu
          nameIcon={nameIconProject ? <ProjectFavicon project={nameIconProject} /> : null}
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
        />
      </div>
      <Menu open={menuOpen} onOpenChange={openMenu}>
        <MenuPopup
          aria-label={`Workstream actions for ${item.name}`}
          align="start"
          {...(menuPoint
            ? {
                anchor: {
                  getBoundingClientRect: () => new DOMRect(menuPoint.x, menuPoint.y, 0, 0),
                },
              }
            : {})}
        >
          {newThreadAction ? (
            <>
              <MenuItem
                disabled={"disabledReason" in newThreadAction}
                onClick={"run" in newThreadAction ? newThreadAction.run : undefined}
                aria-label="New thread"
                aria-description={
                  "disabledReason" in newThreadAction ? newThreadAction.disabledReason : undefined
                }
              >
                <PlusIcon aria-hidden />
                <span>
                  New thread
                  {"disabledReason" in newThreadAction ? (
                    <span className="block max-w-48 text-xs text-muted-foreground">
                      {newThreadAction.disabledReason}
                    </span>
                  ) : null}
                </span>
              </MenuItem>
              <MenuSeparator />
            </>
          ) : null}
          <MenuItem disabled={!state.canCreate || pending} onClick={startRename}>
            <PencilIcon aria-hidden /> Rename
          </MenuItem>
          <PersonalWorkstreamColorMenu
            color={item.color}
            automaticColor={colors.automaticColor}
            disabled={!state.canCreate || pending}
            onSelect={(color) => void run(() => controller.setColor(item.id, color))}
          />
          <MenuSub>
            <MenuSubTrigger disabled={!state.canSnooze || pending || !snoozeEnvironmentId}>
              <AlarmClockIcon aria-hidden /> Snooze
            </MenuSubTrigger>
            <MenuSubPopup>
              {snoozePresets.map((preset) => (
                <MenuItem key={preset.id} onClick={() => void snooze(preset)}>
                  {preset.label} ({preset.whenLabel})
                </MenuItem>
              ))}
              <MenuSeparator />
              <MenuItem onClick={() => void snooze("custom")}>Custom…</MenuItem>
            </MenuSubPopup>
          </MenuSub>
          <MenuSeparator />
          <MenuSub>
            <MenuSubTrigger disabled={!state.canCreate || pending}>
              Workstream state <MenuItemDetail>{lifecycleLabel(item.lifecycle)}</MenuItemDetail>
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
          {setNameIcon ? (
            <WorkstreamNameIconMenu
              projects={nameIconProjects}
              value={item.nameIcon?.projectKey ?? null}
              unavailableReason={
                pending
                  ? "A Workstream change is being saved."
                  : !state.canSetNameIcon
                    ? "A writable connection with Workstream icon support is required."
                    : undefined
              }
              onChange={(projectKey) => void run(() => setNameIcon(item.id, projectKey))}
            />
          ) : null}
          <MenuSeparator />
          <MenuGroup className="flex items-center justify-between">
            <div className="flex min-w-0 flex-wrap items-baseline">
              <MenuGroupLabel>Visible threads</MenuGroupLabel>
              <span className="pr-2 text-xs text-muted-foreground tabular-nums" aria-live="polite">
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
    </section>
  );
}
export function WorkstreamSidebarSection({
  controller,
  nameIconProjects = [],
  newThreadAction,
  renderMembers,
  onOpenHistoryThread,
  summaries,
  rosterTotals,
  unassignedSummary = emptySummary,
  memberCounts,
  referenceMembers,
  dropTargetId,
  snoozedIds = new Set<string>(),
}: {
  controller: PersonalWorkstreamsController;
  nameIconProjects?: readonly WorkstreamNameIconProject[] | undefined;
  newThreadAction?: ((workstream: PersonalWorkstreamView) => WorkstreamThreadAction) | undefined;
  dropTargetId?: string | null | undefined;
  snoozedIds?: ReadonlySet<string>;
  onOpenHistoryThread?: ((thread: PersonalThreadRef) => void) | undefined;
  renderMembers: (
    id: string | null,
    window?: { limit: PersonalThreadLimit; expanded: boolean },
  ) => ReactNode;
  memberCounts?: ReadonlyMap<string, number>;
  referenceMembers?: (id: string) => WorkstreamReference["threads"];
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
  const allOpenWorkstreams = openPersonalWorkstreams(state);
  const openWorkstreams = allOpenWorkstreams.filter((entry) => !snoozedIds.has(entry.id));
  const automaticColors = personalWorkstreamAutomaticColors(allOpenWorkstreams);
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
    finishWorkstreamReferenceDrag();
    draggedRef.current = null;
    setDraggedId(null);
    setInsertion(null);
  };
  const destination = (event: DragEvent<HTMLDivElement>, targetId: string) => {
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
    if (targetId === source) return null;
    const header = event.currentTarget.querySelector("[data-workstream-header]");
    if (!header) return null;
    // Rows can make a group very tall. Header halves must remain reachable
    // drop targets in both directions regardless of the group's member count.
    const rect = header.getBoundingClientRect();
    const index = others.findIndex((entry) => entry.id === targetId);
    if (index < 0) return null;
    const slot = index + (event.clientY >= rect.top + rect.height / 2 ? 1 : 0);
    if (slot === sourceIndex) return null;
    return { beforeId: others[slot - 1]?.id ?? null, afterId: others[slot]?.id ?? null };
  };
  const dragOver = (event: DragEvent<HTMLDivElement>, targetId: string) => {
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
  const drop = (event: DragEvent<HTMLDivElement>, targetId: string) => {
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
  const insertionLine = (atEnd = false) => (
    <div
      aria-hidden
      data-workstream-insertion
      className={`pointer-events-none absolute inset-x-0 ${atEnd ? "bottom-0" : "top-0"} z-10 h-0.5 bg-primary forced-colors:bg-[Highlight]`}
    />
  );
  return (
    <div
      ref={containerRef}
      aria-label="Workstreams"
      data-jones-thread-board
      onDragEnd={clearDrag}
      onDragOver={() => {
        if (draggedRef.current) setInsertion(null);
      }}
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
      <div data-workstream-group-slot className="relative py-1">
        <section
          data-native-workstream="null"
          data-jones-board-section="unassigned"
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
            data-jones-board-unassigned-header
            aria-label={`${unassignedCollapsed ? "Expand" : "Collapse"} Unassigned`}
            aria-description={formatPersonalThreadCount(unassignedSummary.total)}
            aria-expanded={!unassignedCollapsed}
            onClick={() => setUnassignedCollapsed((value) => !value)}
            className="flex w-full min-w-0 cursor-pointer items-center gap-2 rounded-md px-2 py-1 text-sm text-sidebar-muted-foreground hover:bg-sidebar-row-hover focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring"
          >
            <ChevronDownIcon
              aria-hidden
              className={`size-4 shrink-0 ${unassignedCollapsed ? "-rotate-90" : ""}`}
            />
            <span className="truncate">Unassigned</span>
            <span data-jones-board-count className="ml-auto shrink-0 text-xs tabular-nums">
              {formatPersonalThreadCount(unassignedSummary.total)}
            </span>
          </button>
          {!unassignedCollapsed ? renderMembers(null) : null}
        </section>
      </div>
      {/* The container stays mounted so the disclosure's aria-controls always resolves. */}
      <div id={namedGroupsId} hidden={namedHidden} className="relative">
        {!namedHidden
          ? openWorkstreams.map((item) => (
              <div
                key={item.id}
                data-workstream-group-slot
                className="relative py-1"
                onDragOver={(event) => dragOver(event, item.id)}
                onDrop={(event) => drop(event, item.id)}
              >
                {insertion?.afterId === item.id ? insertionLine() : null}
                <Group
                  item={item}
                  automaticColor={automaticColors.get(item.id)}
                  nameIconProjects={nameIconProjects}
                  newThreadAction={newThreadAction?.(item)}
                  controller={controller}
                  summary={summaries?.get(item.id) ?? emptySummary}
                  rosterTotal={rosterTotals?.get(item.id) ?? summaries?.get(item.id)?.total ?? 0}
                  memberCount={memberCounts?.get(item.id) ?? 0}
                  snoozedIds={snoozedIds}
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
                    startWorkstreamReferenceDrag(event, item.id, {
                      name: item.name,
                      threads: referenceMembers?.(item.id) ?? [],
                    });
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
        {insertion?.afterId === null ? insertionLine(true) : null}
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
              return section?.querySelector<HTMLButtonElement>("button") ?? false;
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
