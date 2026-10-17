import { cn } from "../../lib/utils";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import { nextThreadStatusFilter, type ThreadStatusCounts, type ThreadStatusFilter } from "./model";

const labels = { all: "all threads", working: "working", input: "needs input", done: "done" };
const colors = {
  working: "text-sky-600 dark:text-sky-300/80",
  input: "text-indigo-600 dark:text-indigo-300/90",
  done: "text-emerald-600 dark:text-emerald-300/90",
};

export function ThreadStatusFilterPill({
  counts,
  filter,
  onCycle,
}: {
  counts: ThreadStatusCounts;
  filter: ThreadStatusFilter;
  onCycle: () => void;
}) {
  const label = `${counts.working} working, ${counts.input} needs input, ${counts.done} done. Showing ${labels[filter]}. Next: ${labels[nextThreadStatusFilter(filter)]}.`;
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <button
            type="button"
            aria-label={label}
            onClick={onCycle}
            className="relative z-10 inline-flex h-6 shrink-0 items-center gap-0.5 rounded-full bg-muted/30 px-1 text-xs font-medium outline-hidden ring-ring hover:bg-muted/60 focus-visible:ring-2 focus-visible:ring-inset [-webkit-app-region:no-drag]"
          >
            {(["working", "input", "done"] as const).map((status, index) => (
              <span key={status} className="inline-flex items-center gap-0.5" aria-hidden="true">
                {index > 0 ? <span className="text-muted-foreground/60">·</span> : null}
                <span
                  className={cn(
                    "rounded-full px-1.5 tabular-nums",
                    counts[status] > 0 ? colors[status] : "text-muted-foreground/60",
                    filter === status && "bg-current/10 ring-1 ring-current/30",
                  )}
                >
                  {counts[status]}
                </span>
              </span>
            ))}
          </button>
        }
      />
      <TooltipPopup side="bottom">{label}</TooltipPopup>
    </Tooltip>
  );
}
