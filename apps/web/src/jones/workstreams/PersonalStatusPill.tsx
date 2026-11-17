import { useRef, type Ref } from "react";
import { cn } from "../../lib/utils";
import type { PersonalThreadSummary } from "./personalPresentation";
import type { PersonalStatusFilter } from "./personalStatusFilter";

const statuses = [
  {
    status: "working",
    name: "Working",
    count: "running",
    color: "text-info-foreground",
    selected: "bg-info-foreground/15",
  },
  {
    status: "input",
    name: "Needs input",
    count: "attention",
    color: "text-waiting",
    selected: "bg-waiting/15",
  },
  {
    status: "done",
    name: "Done",
    count: "done",
    color: "text-success-foreground",
    selected: "bg-success-foreground/15",
  },
] as const;

/**
 * Status counters that double as filters, shared by each Workstream header and
 * the Workstreams header. Click selects or clears a status; a second click on
 * the same status within 250 ms excludes it.
 */
export function PersonalStatusPill({
  summary,
  statusFilter = null,
  onStatusFilterChange,
  onInteract,
  ref,
}: {
  summary: Pick<PersonalThreadSummary, "running" | "attention" | "done">;
  statusFilter?: PersonalStatusFilter | null;
  onStatusFilterChange?: (filter: PersonalStatusFilter | null) => void;
  // Lets a host discard state that a counter click supersedes, such as a pending title-click undo.
  onInteract?: () => void;
  ref?: Ref<HTMLSpanElement>;
}) {
  const pointerSequence = useRef<{ status: string; at: number; consumed: boolean } | null>(null);
  return (
    <span
      ref={ref}
      data-workstream-header-control
      data-personal-status-pill
      style={{ gap: "calc(2px * var(--personal-status-spacing, 1))" }}
      className="pointer-events-auto relative inline-flex h-6 cursor-default shrink-0 items-center rounded-full bg-muted/30 px-1 text-xs font-medium tabular-nums [-webkit-app-region:no-drag]"
      draggable={false}
      onPointerDown={(event) => event.stopPropagation()}
      onClick={(event) => event.stopPropagation()}
      onDoubleClick={(event) => event.stopPropagation()}
      onDragStart={(event) => {
        event.preventDefault();
        event.stopPropagation();
      }}
    >
      {statuses.map(({ status, name: statusName, count, color, selected }, index) => {
        const excluded = statusFilter === `hide-${status}`;
        const active = statusFilter === status;
        const state = active
          ? "selected"
          : excluded
            ? "excluded"
            : statusFilter === null
              ? "using global status filter"
              : "not selected";
        return (
          <span key={status} className="contents">
            {index > 0 ? (
              <span aria-hidden className="text-sidebar-muted-foreground/60">
                ·
              </span>
            ) : null}
            {onStatusFilterChange ? (
              <button
                type="button"
                aria-label={`${statusName}: ${summary[count]}, ${state}. Click to ${active ? "use global status filter" : "show only " + statusName.toLowerCase()}; double-click within 250 ms to exclude ${statusName.toLowerCase()}.`}
                aria-pressed={active || excluded}
                className={cn(
                  "shrink-0 cursor-pointer rounded-sm outline-hidden focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring",
                  color,
                  active && selected,
                  excluded && "line-through decoration-2",
                )}
                style={{ paddingInline: "calc(6px * var(--personal-status-spacing, 1))" }}
                onClick={(event) => {
                  event.stopPropagation();
                  onInteract?.();
                  if (event.detail === 0) {
                    pointerSequence.current = null;
                    onStatusFilterChange(active ? null : status);
                    return;
                  }
                  const previous = pointerSequence.current;
                  const rapid =
                    previous !== null &&
                    previous.status === status &&
                    event.timeStamp - previous.at <= 250;
                  pointerSequence.current = { status, at: event.timeStamp, consumed: rapid };
                  if (!rapid) onStatusFilterChange(active ? null : status);
                  else if (!previous.consumed) onStatusFilterChange(`hide-${status}`);
                }}
              >
                {summary[count]}
              </button>
            ) : (
              <span
                className={cn("shrink-0 cursor-default rounded-sm", color)}
                style={{ paddingInline: "calc(6px * var(--personal-status-spacing, 1))" }}
              >
                {summary[count]}
              </span>
            )}
          </span>
        );
      })}
    </span>
  );
}
