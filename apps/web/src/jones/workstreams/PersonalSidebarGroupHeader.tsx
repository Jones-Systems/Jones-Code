import { useId, useLayoutEffect, useRef, type ReactNode } from "react";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import { useSidebarAppearancePresentation } from "../sidebarAppearance/SidebarAppearanceContext";
import { observePersonalStatusSpacing, PersonalStatusPill } from "./PersonalStatusPill";
import { formatPersonalThreadCount, type PersonalThreadSummary } from "./personalPresentation";
import type { PersonalStatusFilter } from "./personalStatusFilter";

const nameIconClassName = "me-[0.4em] inline-block size-[1em] align-[-0.125em] [&>*]:size-full";

export function WorkstreamRailToggle({
  name,
  collapsed,
  onToggle,
}: {
  name: string;
  collapsed: boolean;
  onToggle: () => void;
}) {
  return (
    <button
      type="button"
      data-workstream-rail-toggle
      aria-label={`${collapsed ? "Expand" : "Collapse"} ${name} from the color bar`}
      aria-expanded={!collapsed}
      onClick={onToggle}
      className="absolute inset-y-0 -left-0.5 z-2 w-2 cursor-pointer rounded-l-lg hover:bg-sidebar-row-hover focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring"
    />
  );
}

export function PersonalSidebarGroupHeader({
  name,
  collapsed,
  onToggle,
  summary,
  rosterTotal,
  nameIcon,
  onRenameRequested,
  nameEditor,
  hasContextMenu,
  statusFilter = null,
  onStatusFilterChange,
}: {
  name: string;
  collapsed: boolean;
  // Return a reversal when title double-click rename is enabled.
  onToggle: () => void | (() => void);
  summary: PersonalThreadSummary;
  rosterTotal?: number;
  visibleCount?: number;
  nameIcon?: ReactNode;
  onRenameRequested?: () => void;
  nameEditor?: ReactNode;
  hasContextMenu?: boolean;
  statusFilter?: PersonalStatusFilter | null;
  onStatusFilterChange?: (filter: PersonalStatusFilter | null) => void;
}) {
  const appearance = useSidebarAppearancePresentation();
  const total = rosterTotal ?? summary.total;
  const numericCount = appearance?.countStyle === "numeric-right";
  const descriptionId = useId();
  const titleLaneRef = useRef<HTMLDivElement>(null);
  const titleProbeRef = useRef<HTMLSpanElement>(null);
  const titleProbeTextRef = useRef<HTMLSpanElement>(null);
  const titleProbeIconRef = useRef<HTMLSpanElement>(null);
  const countersRef = useRef<HTMLSpanElement>(null);
  const undoTitleClickRef = useRef<(() => void) | null>(null);
  const description = `${formatPersonalThreadCount(total)}, ${summary.running} working, ${summary.attention} need attention, ${summary.done} done, ${summary.failed} failed.`;
  const hasNameIcon = Boolean(nameIcon);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Fit the DOM counter pill to measured title lines and keep resize/font subscriptions mounted.
  useLayoutEffect(() => {
    const lane = titleLaneRef.current;
    const probe = titleProbeRef.current;
    const probeText = titleProbeTextRef.current;
    const probeIcon = hasNameIcon ? titleProbeIconRef.current : null;
    const counters = countersRef.current;
    if (!lane || !probe || !probeText || !counters) return;
    const fit = () => {
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
      probeText.textContent = name;
      probe.style.width = `${titleWidth}px`;
      const range = document.createRange();
      range.selectNodeContents(probeText);
      const lines = Array.from(range.getClientRects()).filter((rect) => rect.width > 0);
      const visibleTop = lines[0]?.top;
      const secondTop = lines.find((rect) => rect.top !== visibleTop)?.top;
      const visibleLines = lines.filter(
        (rect) => rect.top === visibleTop || rect.top === secondTop,
      );
      const probeRect = probe.getBoundingClientRect();
      const rtl = getComputedStyle(probe).direction === "rtl";
      const usedWidth = (rect: DOMRect) =>
        rtl ? probeRect.right - rect.left : rect.right - probeRect.left;
      const widestLine = Math.max(
        0,
        probeIcon ? usedWidth(probeIcon.getBoundingClientRect()) : 0,
        ...visibleLines.map(usedWidth),
      );
      probe.style.width = "max-content";
      probeText.textContent = "Mw0123456789";
      const spare = Math.max(0, titleWidth - widestLine - 0.5);
      const factor = Math.min(1, 0.25 + (0.75 * spare) / (standardWidth - minimumWidth));
      setSpacing(Math.floor(factor * 1000) / 1000);
    };
    // The unwrapped probe observes typography independently of the fitted title;
    // all temporary measurements finish synchronously, without a frame loop.
    return observePersonalStatusSpacing(counters, [lane, probe, counters], fit);
  }, [name, nameEditor, hasNameIcon]);
  return (
    <div
      data-personal-group-header
      className="relative grid min-w-0 flex-1 grid-cols-[minmax(0,1fr)] items-start gap-x-0.75 rounded-md px-2 py-1 text-left hover:bg-sidebar-row-hover"
    >
      <Tooltip>
        <TooltipTrigger
          render={
            <button
              type="button"
              aria-label={`${collapsed ? "Expand" : "Collapse"} ${name}`}
              aria-describedby={descriptionId}
              aria-expanded={!collapsed}
              aria-keyshortcuts={hasContextMenu ? "Shift+F10" : undefined}
              onClick={() => {
                undoTitleClickRef.current = null;
                onToggle();
              }}
              className="absolute inset-0 cursor-pointer rounded-md focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring"
            />
          }
        />
        <TooltipPopup side="right">
          <span className="block">{name}</span>
          <span className="block">{description}</span>
        </TooltipPopup>
      </Tooltip>
      <div
        ref={titleLaneRef}
        data-personal-group-title-lane
        className="pointer-events-none relative col-start-1 row-start-1 flex min-w-0 items-start gap-x-0.75"
      >
        <span
          aria-hidden="true"
          className="pointer-events-none invisible absolute inset-0 overflow-hidden"
        >
          <span
            data-personal-group-title
            ref={titleProbeRef}
            className="block w-max break-words text-sm font-semibold leading-5"
          >
            {hasNameIcon ? <span ref={titleProbeIconRef} className={nameIconClassName} /> : null}
            <span ref={titleProbeTextRef}>Mw0123456789</span>
          </span>
        </span>
        <div className="relative min-w-0 flex-auto [&_input]:pointer-events-auto">
          {nameEditor ?? (
            <span
              data-personal-group-title
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
              className="pointer-events-auto line-clamp-2 w-fit min-w-0 max-w-full cursor-pointer [[data-workstream-header][draggable=true]_&]:cursor-grab [[data-workstream-header][draggable=true]_&]:active:cursor-grabbing break-words text-sm font-semibold leading-5"
            >
              {hasNameIcon ? (
                <span aria-hidden="true" className={nameIconClassName}>
                  {nameIcon}
                </span>
              ) : null}
              {name}
            </span>
          )}
        </div>
        <PersonalStatusPill
          ref={countersRef}
          summary={summary}
          statusFilter={statusFilter}
          {...(onStatusFilterChange ? { onStatusFilterChange } : {})}
          onInteract={() => {
            undoTitleClickRef.current = null;
          }}
        />
      </div>
      {numericCount ? (
        <span
          data-personal-group-count
          data-jones-board-count
          aria-hidden="true"
          className="pointer-events-none col-start-1 row-start-2 mt-0.5 flex min-w-0 items-baseline justify-end whitespace-nowrap font-medium tabular-nums"
        >
          {summary.failed > 0 ? (
            <>
              <span className="text-thread-failed">{summary.failed} failed</span>
              <span
                data-jones-board-count-separator
                className="mx-[0.4em] text-muted-foreground/60"
              >
                ·
              </span>
            </>
          ) : null}
          <span className="text-muted-foreground">{total}</span>
        </span>
      ) : (
        <span
          data-jones-board-count
          className="pointer-events-none col-start-1 row-start-2 flex min-w-0 flex-wrap items-baseline justify-between gap-x-2 text-xs leading-4 text-sidebar-muted-foreground"
        >
          <span className="tabular-nums">{formatPersonalThreadCount(total)}</span>
          {summary.failed > 0 ? (
            <span className="ml-auto text-right text-thread-failed tabular-nums">
              {summary.failed} failed
            </span>
          ) : null}
        </span>
      )}
      <span id={descriptionId} className="sr-only">
        {description}
      </span>
    </div>
  );
}
