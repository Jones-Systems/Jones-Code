import { cn } from "../../lib/utils";
import { Tooltip, TooltipTrigger, TooltipPopup } from "../ui/tooltip";

export function SidebarActiveThreadsPill({
  count,
  activeOnly,
  onToggle,
}: {
  count: number;
  activeOnly: boolean;
  onToggle: () => void;
}) {
  const label = activeOnly ? "Show all threads" : "Show only active threads";
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <button
            type="button"
            aria-label={label}
            aria-pressed={activeOnly}
            onClick={onToggle}
            className={cn(
              "relative z-10 inline-flex h-6 shrink-0 items-center gap-1.5 rounded-full px-2 text-xs font-medium outline-hidden ring-ring transition-colors focus-visible:ring-2 [-webkit-app-region:no-drag]",
              count > 0 ? "text-success-foreground" : "text-muted-foreground/60",
              activeOnly ? "bg-success/20 ring-1 ring-success/30" : "bg-muted/30 hover:bg-muted/60",
            )}
          >
            <span aria-hidden="true" className="size-1.5 shrink-0 rounded-full bg-current" />
            <span>{count} active</span>
          </button>
        }
      />
      <TooltipPopup side="bottom">{label}</TooltipPopup>
    </Tooltip>
  );
}
