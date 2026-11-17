import { ChevronDownIcon, MinusIcon, PlusIcon } from "lucide-react";
import { Button } from "../../components/ui/button";
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
  return (
    <div
      data-personal-workstreams-header
      className="flex h-8 min-w-0 items-center gap-0.5 px-2 text-xs font-medium text-sidebar-muted-foreground"
    >
      <span className="mr-1 min-w-0 truncate">Workstreams</span>
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
      <div className="ml-auto flex shrink-0 items-center gap-1">
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
          summary={summary}
          statusFilter={statusFilter}
          onStatusFilterChange={onStatusFilterChange}
        />
      </div>
    </div>
  );
}
