import { cn } from "../../lib/utils";

export function SidebarActiveThreadsPill({
  count,
  activeOnly,
  onToggle,
}: {
  count: number;
  activeOnly: boolean;
  onToggle: () => void;
}) {
  return (
    <button
      type="button"
      aria-label={activeOnly ? "Show all threads" : "Show only active threads"}
      aria-pressed={activeOnly}
      title={activeOnly ? "Show all threads" : "Show only active threads"}
      onClick={onToggle}
      className={cn(
        "relative z-10 inline-flex h-6 shrink-0 items-center gap-1.5 rounded-full px-2 text-xs font-medium outline-hidden ring-ring transition-colors focus-visible:ring-2 [-webkit-app-region:no-drag]",
        count > 0 ? "text-green-700 dark:text-green-400" : "text-muted-foreground/60",
        activeOnly ? "bg-green-500/20 ring-1 ring-green-500/30" : "bg-muted/30 hover:bg-muted/60",
      )}
    >
      <span aria-hidden="true" className="size-1.5 shrink-0 rounded-full bg-current" />
      <span>{count} active</span>
    </button>
  );
}
