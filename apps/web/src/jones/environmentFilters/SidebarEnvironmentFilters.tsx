import { useAtom } from "@effect/atom-react";
import { cn } from "../../lib/utils";
import { SIDEBAR_ENVIRONMENT_FILTERS, toggleSidebarEnvironmentFilter } from "./model";
import { sidebarEnvironmentFiltersAtom } from "./state";

export function SidebarEnvironmentFilters() {
  const [filters, setFilters] = useAtom(sidebarEnvironmentFiltersAtom);
  return (
    <div
      role="group"
      aria-label="Thread environments"
      className="flex items-center gap-2 px-2 py-1"
    >
      {SIDEBAR_ENVIRONMENT_FILTERS.map((label) => (
        <button
          key={label}
          type="button"
          aria-label={`Show ${label} threads`}
          aria-pressed={filters[label]}
          onClick={() => setFilters((current) => toggleSidebarEnvironmentFilter(current, label))}
          className={cn(
            "relative z-10 inline-flex h-6 shrink-0 items-center rounded-full px-2 text-xs font-medium outline-hidden ring-ring transition-colors focus-visible:ring-2 focus-visible:ring-inset [-webkit-app-region:no-drag]",
            filters[label]
              ? "bg-success/20 text-success-foreground ring-1 ring-success/30"
              : "bg-muted/30 text-muted-foreground hover:bg-muted/60",
          )}
        >
          {label}
        </button>
      ))}
    </div>
  );
}
