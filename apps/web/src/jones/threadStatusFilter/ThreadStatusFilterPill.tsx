import { useClientSettings } from "../../hooks/useSettings";
import { cn } from "../../lib/utils";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import { DOUBLE_CLICK_WINDOW_MS, useTimedClick } from "../useTimedClick";
import { nextThreadStatusFilter, type ThreadStatusCounts, type ThreadStatusFilter } from "./model";

const labels = {
  all: "all threads",
  working: "working",
  input: "needs input",
  done: "done",
  "hide-working": "Hide working",
};
const colors = {
  working: "text-sky-600 dark:text-sky-300/80",
  input: "text-indigo-600 dark:text-indigo-300/90",
  done: "text-emerald-600 dark:text-emerald-300/90",
};

const selectedNumbers = {
  working: "bg-sky-600/30 dark:bg-sky-300/30",
  input: "bg-indigo-600/30 dark:bg-indigo-300/30",
  done: "bg-emerald-600/30 dark:bg-emerald-300/30",
};

const backgrounds = {
  all: "bg-muted/30 hover:bg-muted/60",
  "hide-working": "bg-muted/60 ring-1 ring-foreground/40",
  working: "bg-sky-600/20 ring-1 ring-sky-600/30 dark:bg-sky-300/20 dark:ring-sky-300/30",
  input: "bg-indigo-600/20 ring-1 ring-indigo-600/30 dark:bg-indigo-300/20 dark:ring-indigo-300/30",
  done: "bg-emerald-600/20 ring-1 ring-emerald-600/30 dark:bg-emerald-300/20 dark:ring-emerald-300/30",
};

export function ThreadStatusFilterPill({
  counts,
  filter,
  onCycle,
  onHideWorking,
}: {
  counts: ThreadStatusCounts;
  filter: ThreadStatusFilter;
  onCycle: () => void;
  onHideWorking?: (() => void) | undefined;
}) {
  const enabled = useClientSettings((settings) => settings.threadActivityWindowEnabled);
  const amount = useClientSettings((settings) => settings.threadActivityWindowAmount);
  const unit = useClientSettings((settings) => settings.threadActivityWindowUnit);
  const windowLabel = enabled
    ? ` Activity in the past ${amount} ${unit}; Done includes read completions. All shows every thread.`
    : "";
  const timedClick = useTimedClick();
  const label = `${counts.working} working, ${counts.input} needs input, ${counts.done} done. Showing ${labels[filter]}. Next: ${labels[nextThreadStatusFilter(filter)]}. Double-click within ${DOUBLE_CLICK_WINDOW_MS} ms to hide working threads.${windowLabel}`;
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <button
            type="button"
            aria-label={label}
            onClick={(event) => timedClick(event, onCycle, onHideWorking)}
            style={{ gap: "var(--sidebar-status-gap, 2px)" }}
            className={cn(
              "relative z-10 inline-flex h-6 shrink-0 cursor-pointer items-center rounded-full px-1 text-xs font-medium outline-hidden ring-ring focus-visible:ring-2 focus-visible:ring-inset [-webkit-app-region:no-drag]",
              backgrounds[filter],
            )}
          >
            {(["working", "input", "done"] as const).map((status, index) => (
              <span
                key={status}
                className="inline-flex items-center"
                aria-hidden="true"
                style={{ gap: "var(--sidebar-status-gap, 2px)" }}
              >
                {index > 0 ? <span className="text-muted-foreground/60">·</span> : null}
                <span
                  style={{ paddingInline: "var(--sidebar-status-number-padding, 6px)" }}
                  className={cn(
                    "rounded-sm tabular-nums",
                    filter === status && "font-semibold",
                    filter === "hide-working" &&
                      status === "working" &&
                      "line-through decoration-2",
                    filter === status && selectedNumbers[status],
                    counts[status] > 0 ? colors[status] : "text-muted-foreground/60",
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
