import { useLayoutEffect, useRef } from "react";
import { ChevronDownIcon, MinusIcon, PlusIcon } from "lucide-react";
import { Button } from "../../components/ui/button";
import { observeResize } from "../../lib/observeResize";
import { PersonalStatusPill } from "./PersonalStatusPill";
import type { PersonalThreadSummary } from "./personalPresentation";
import type { PersonalStatusFilter } from "./personalStatusFilter";

/**
 * Inline header above the named Workstream groups: bulk expand/collapse, the
 * overall disclosure and one status filter shared by every named group. It
 * stays visible while the groups are hidden.
 */
export function PersonalWorkstreamsHeader({
  summary,
  hidden,
  onHiddenChange,
  onExpandAll,
  onCollapseAll,
  statusFilter,
  onStatusFilterChange,
  controlsId,
}: {
  summary: Pick<PersonalThreadSummary, "running" | "attention" | "done">;
  hidden: boolean;
  onHiddenChange: (hidden: boolean) => void;
  onExpandAll: () => void;
  onCollapseAll: () => void;
  statusFilter: PersonalStatusFilter | null;
  onStatusFilterChange: (filter: PersonalStatusFilter | null) => void;
  controlsId: string;
}) {
  const rowRef = useRef<HTMLDivElement>(null);
  const labelRef = useRef<HTMLSpanElement>(null);
  const pillRef = useRef<HTMLSpanElement>(null);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Fit the DOM pill spacing to the measured row so the label stays whole while the pill can still compress.
  useLayoutEffect(() => {
    const row = rowRef.current;
    const label = labelRef.current;
    const pill = pillRef.current;
    if (!row || !label || !pill) return;
    let disposed = false;
    // Same spacing variable and range the group headers use for long titles.
    const fit = () => {
      if (disposed) return;
      const setSpacing = (factor: number) =>
        pill.style.setProperty("--personal-status-spacing", String(factor));
      setSpacing(1);
      const overflow = label.scrollWidth - label.clientWidth;
      if (overflow <= 0) return;
      const standardWidth = pill.getBoundingClientRect().width;
      setSpacing(0.25);
      const minimumWidth = pill.getBoundingClientRect().width;
      if (standardWidth <= minimumWidth) return;
      // One extra pixel absorbs the label's sub-pixel text width.
      const factor = Math.max(0.25, 1 - (0.75 * (overflow + 1)) / (standardWidth - minimumWidth));
      setSpacing(Math.floor(factor * 1000) / 1000);
    };
    fit();
    // The pill's own size changes with its count digits; each fit leaves its final width
    // stable, so observing it does not loop.
    const stop = observeResize([row, pill], fit);
    document.fonts.addEventListener("loadingdone", fit);
    void document.fonts.ready.then(fit);
    return () => {
      disposed = true;
      stop();
      document.fonts.removeEventListener("loadingdone", fit);
    };
  }, []);
  return (
    <div
      ref={rowRef}
      data-personal-workstreams-header
      className="flex h-8 min-w-0 items-center gap-0.5 px-2 text-xs font-medium text-sidebar-muted-foreground"
    >
      <span ref={labelRef} className="min-w-0 truncate">
        Workstreams
      </span>
      <Button
        variant="ghost-muted"
        size="icon-micro"
        aria-label="Expand all Workstreams"
        disabled={hidden}
        onClick={onExpandAll}
      >
        <PlusIcon aria-hidden />
      </Button>
      <Button
        variant="ghost-muted"
        size="icon-micro"
        aria-label="Collapse all Workstreams"
        disabled={hidden}
        onClick={onCollapseAll}
      >
        <MinusIcon aria-hidden />
      </Button>
      <div className="ml-auto flex shrink-0 items-center gap-0.5">
        <Button
          variant="ghost-muted"
          size="icon-micro"
          aria-label={hidden ? "Show all Workstreams" : "Hide all Workstreams"}
          aria-expanded={!hidden}
          aria-controls={controlsId}
          onClick={() => onHiddenChange(!hidden)}
        >
          <ChevronDownIcon aria-hidden className={hidden ? "-rotate-90" : ""} />
        </Button>
        <PersonalStatusPill
          ref={pillRef}
          summary={summary}
          statusFilter={statusFilter}
          onStatusFilterChange={onStatusFilterChange}
        />
      </div>
    </div>
  );
}
