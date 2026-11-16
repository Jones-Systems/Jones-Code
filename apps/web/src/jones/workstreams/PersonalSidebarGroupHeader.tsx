import { useId } from "react";
import { ChevronDownIcon } from "lucide-react";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import { formatPersonalThreadCount, type PersonalThreadSummary } from "./personalPresentation";

export function PersonalSidebarGroupHeader({
  name,
  collapsed,
  onToggle,
  summary,
}: {
  name: string;
  collapsed: boolean;
  onToggle: () => void;
  summary: PersonalThreadSummary;
}) {
  const descriptionId = useId();
  const description = `${formatPersonalThreadCount(summary.total)}, ${summary.running} working, ${summary.attention} need attention, ${summary.done} done, ${summary.failed} failed.`;
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <button
            type="button"
            aria-label={`${collapsed ? "Expand" : "Collapse"} ${name}`}
            aria-describedby={descriptionId}
            aria-expanded={!collapsed}
            onClick={onToggle}
            className="grid min-w-0 flex-1 grid-cols-[auto_minmax(0,1fr)_auto] items-start gap-x-2 rounded-md px-2 py-1 text-left hover:bg-sidebar-row-hover focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring"
          >
            <ChevronDownIcon
              aria-hidden
              className={`mt-0.5 size-4 shrink-0 text-sidebar-muted-foreground ${collapsed ? "-rotate-90" : ""}`}
            />
            <span className="line-clamp-2 min-w-0 break-words text-sm font-semibold leading-5">
              {name}
            </span>
            <span aria-hidden className="flex justify-end gap-x-2 text-sm leading-5 tabular-nums">
              <span className="text-info-foreground">{summary.running}</span>
              <span className="text-sidebar-muted-foreground/60">·</span>
              <span className="text-waiting">{summary.attention}</span>
              <span className="text-sidebar-muted-foreground/60">·</span>
              <span className="text-success-foreground">{summary.done}</span>
            </span>
            <span className="col-span-2 col-start-2 flex min-w-0 flex-wrap items-baseline justify-between gap-x-2 text-xs leading-4 text-sidebar-muted-foreground">
              <span className="tabular-nums">{formatPersonalThreadCount(summary.total)}</span>
              <span className="ml-auto flex flex-wrap justify-end gap-x-2 tabular-nums">
                {summary.failed > 0 ? (
                  <span className="text-thread-failed">{summary.failed} failed</span>
                ) : null}
              </span>
            </span>
            <span id={descriptionId} className="sr-only">
              {description}
            </span>
          </button>
        }
      />
      <TooltipPopup side="right">
        <span className="block">{name}</span>
        <span className="block">{description}</span>
      </TooltipPopup>
    </Tooltip>
  );
}
