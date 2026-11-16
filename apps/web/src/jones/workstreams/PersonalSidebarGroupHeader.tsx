import { useId, useLayoutEffect, useRef, type ReactNode } from "react";
import { ChevronDownIcon } from "lucide-react";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import { cn } from "../../lib/utils";
import { observeResize } from "../../lib/observeResize";
import { formatPersonalThreadCount, type PersonalThreadSummary } from "./personalPresentation";
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

export function PersonalSidebarGroupHeader({
  name,
  collapsed,
  onToggle,
  summary,
  onRenameRequested,
  nameEditor,
  actions,
  statusFilter = null,
  onStatusFilterChange,
}: {
  name: string;
  collapsed: boolean;
  // Return a reversal when title double-click rename is enabled.
  onToggle: () => void | (() => void);
  summary: PersonalThreadSummary;
  onRenameRequested?: () => void;
  nameEditor?: ReactNode;
  actions?: ReactNode;
  statusFilter?: PersonalStatusFilter | null;
  onStatusFilterChange?: (filter: PersonalStatusFilter | null) => void;
}) {
  const descriptionId = useId();
  const titleLaneRef = useRef<HTMLDivElement>(null);
  const titleProbeRef = useRef<HTMLSpanElement>(null);
  const countersRef = useRef<HTMLSpanElement>(null);
  const undoTitleClickRef = useRef<(() => void) | null>(null);
  const pointerSequence = useRef<{ status: string; at: number; consumed: boolean } | null>(null);
  const description = `${formatPersonalThreadCount(summary.total)}, ${summary.running} working, ${summary.attention} need attention, ${summary.done} done, ${summary.failed} failed.`;
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Fit the DOM counter pill to measured title lines and keep resize/font subscriptions mounted.
  useLayoutEffect(() => {
    const lane = titleLaneRef.current;
    const probe = titleProbeRef.current;
    const counters = countersRef.current;
    if (!lane || !probe || !counters) return;
    let disposed = false;
    const fit = () => {
      if (disposed) return;
      const setSpacing = (factor: number) =>
        counters.style.setProperty("--personal-status-spacing", String(factor));
      setSpacing(1);
      const standardWidth = counters.getBoundingClientRect().width;
      setSpacing(0.25);
      const minimumWidth = counters.getBoundingClientRect().width;
      const titleWidth = Math.max(
        0,
        lane.getBoundingClientRect().width -
          parseFloat(getComputedStyle(lane).columnGap) -
          minimumWidth,
      );
      if (nameEditor || titleWidth === 0 || standardWidth <= minimumWidth) return;

      // Measure real wrapped lines at minimum spacing. Spare space on either
      // visible line can return to the pill without changing its visible words.
      probe.textContent = name;
      probe.style.width = `${titleWidth}px`;
      const range = document.createRange();
      range.selectNodeContents(probe);
      const lines = Array.from(range.getClientRects()).filter((rect) => rect.width > 0);
      const visibleTop = lines[0]?.top;
      const secondTop = lines.find((rect) => rect.top !== visibleTop)?.top;
      const visibleLines = lines.filter(
        (rect) => rect.top === visibleTop || rect.top === secondTop,
      );
      const widestLine = Math.max(0, ...visibleLines.map((rect) => rect.width));
      probe.style.width = "max-content";
      probe.textContent = "Mw0123456789";
      const spare = Math.max(0, titleWidth - widestLine - 0.5);
      const factor = Math.min(1, 0.25 + (0.75 * spare) / (standardWidth - minimumWidth));
      setSpacing(Math.floor(factor * 1000) / 1000);
    };
    fit();
    // The unwrapped probe observes typography independently of the fitted title;
    // all temporary measurements finish synchronously, without a frame loop.
    const stop = observeResize([lane, probe, counters], fit);
    document.fonts.addEventListener("loadingdone", fit);
    void document.fonts.ready.then(fit);
    return () => {
      disposed = true;
      stop();
      document.fonts.removeEventListener("loadingdone", fit);
    };
  }, [name, nameEditor]);
  return (
    <div
      data-personal-group-header
      className="relative grid min-w-0 flex-1 grid-cols-[auto_minmax(0,1fr)_auto] items-start gap-x-0.75 rounded-md px-2 py-1 text-left hover:bg-sidebar-row-hover"
    >
      <Tooltip>
        <TooltipTrigger
          render={
            <button
              type="button"
              aria-label={`${collapsed ? "Expand" : "Collapse"} ${name}`}
              aria-describedby={descriptionId}
              aria-expanded={!collapsed}
              onClick={() => {
                undoTitleClickRef.current = null;
                onToggle();
              }}
              className="absolute inset-0 rounded-md focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring"
            />
          }
        />
        <TooltipPopup side="right">
          <span className="block">{name}</span>
          <span className="block">{description}</span>
        </TooltipPopup>
      </Tooltip>
      <ChevronDownIcon
        aria-hidden
        className={`pointer-events-none col-start-1 row-start-1 mt-0.5 size-4 shrink-0 text-sidebar-muted-foreground ${collapsed ? "-rotate-90" : ""}`}
      />
      <div
        ref={titleLaneRef}
        className="pointer-events-none relative col-start-2 row-start-1 flex min-w-0 items-start gap-x-0.75"
      >
        <span
          aria-hidden="true"
          className="pointer-events-none invisible absolute inset-0 overflow-hidden"
        >
          <span
            ref={titleProbeRef}
            className="block w-max break-words text-sm font-semibold leading-5"
          >
            Mw0123456789
          </span>
        </span>
        <div className="relative min-w-0 flex-auto [&_input]:pointer-events-auto">
          {nameEditor ?? (
            <span
              onClick={(event) => {
                event.stopPropagation();
                if (onRenameRequested && event.detail !== 0) {
                  // Retain the first click's reversal for the browser's native
                  // double-click interval, including slower double-click settings.
                  if (event.detail === 1) undoTitleClickRef.current = onToggle() ?? null;
                } else {
                  undoTitleClickRef.current = null;
                  onToggle();
                }
              }}
              onDoubleClick={
                onRenameRequested
                  ? (event) => {
                      event.stopPropagation();
                      undoTitleClickRef.current?.();
                      undoTitleClickRef.current = null;
                      onRenameRequested();
                    }
                  : undefined
              }
              className="pointer-events-auto line-clamp-2 min-w-0 break-words text-sm font-semibold leading-5"
            >
              {name}
            </span>
          )}
        </div>
        <span
          ref={countersRef}
          data-workstream-header-control
          data-personal-status-pill
          style={{ gap: "calc(2px * var(--personal-status-spacing, 1))" }}
          className="pointer-events-auto relative inline-flex h-6 shrink-0 items-center rounded-full bg-muted/30 px-1 text-xs font-medium tabular-nums [-webkit-app-region:no-drag]"
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
                      "shrink-0 rounded-sm outline-hidden focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring",
                      color,
                      active && selected,
                      excluded && "line-through decoration-2",
                    )}
                    style={{ paddingInline: "calc(6px * var(--personal-status-spacing, 1))" }}
                    onClick={(event) => {
                      event.stopPropagation();
                      undoTitleClickRef.current = null;
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
                    className={cn("shrink-0 rounded-sm", color)}
                    style={{ paddingInline: "calc(6px * var(--personal-status-spacing, 1))" }}
                  >
                    {summary[count]}
                  </span>
                )}
              </span>
            );
          })}
        </span>
      </div>
      {actions ? (
        <div
          data-workstream-header-control
          className="relative col-start-3 row-start-1 [-webkit-app-region:no-drag]"
          draggable={false}
          onPointerDown={(event) => event.stopPropagation()}
          onClick={(event) => event.stopPropagation()}
          onDoubleClick={(event) => event.stopPropagation()}
          onDragStart={(event) => {
            event.preventDefault();
            event.stopPropagation();
          }}
        >
          {actions}
        </div>
      ) : null}
      <span className="pointer-events-none col-span-2 col-start-2 row-start-2 flex min-w-0 flex-wrap items-baseline justify-between gap-x-2 text-xs leading-4 text-sidebar-muted-foreground">
        <span className="tabular-nums">{formatPersonalThreadCount(summary.total)}</span>
        {summary.failed > 0 ? (
          <span className="ml-auto text-right text-thread-failed tabular-nums">
            {summary.failed} failed
          </span>
        ) : null}
      </span>
      <span id={descriptionId} className="sr-only">
        {description}
      </span>
    </div>
  );
}
