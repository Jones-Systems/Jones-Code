import { ChevronDownIcon } from "lucide-react";
import type { PersonalThreadSummary } from "./personalPresentation";

export function PersonalSidebarGroupHeader({
  name,
  collapsed,
  onToggle,
  summary,
  lifecycle,
}: {
  name: string;
  collapsed: boolean;
  onToggle: () => void;
  summary: PersonalThreadSummary;
  lifecycle?: string;
}) {
  return (
    <button
      type="button"
      aria-label={`${collapsed ? "Expand" : "Collapse"} ${name}`}
      aria-expanded={!collapsed}
      onClick={onToggle}
      className="flex min-h-16 min-w-0 flex-1 items-start gap-2 rounded-md px-2 py-2.5 text-left hover:bg-sidebar-row-hover focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring"
    >
      <ChevronDownIcon
        aria-hidden
        className={`mt-0.5 size-4 shrink-0 text-sidebar-muted-foreground ${collapsed ? "-rotate-90" : ""}`}
      />
      <span className="min-w-0 flex-1 space-y-1">
        <span className="line-clamp-2 break-words text-sm font-semibold leading-5">{name}</span>
        <span className="flex flex-wrap items-center gap-x-2 gap-y-1 text-xs leading-4 text-sidebar-muted-foreground">
          <span className="tabular-nums">
            {summary.total} {summary.total === 1 ? "thread" : "threads"}
          </span>
          {lifecycle ? <span className="capitalize">{lifecycle}</span> : null}
          {summary.running > 0 ? (
            <span className="text-info-foreground">{summary.running} running</span>
          ) : null}
          {summary.attention > 0 ? (
            <span className="text-waiting">{summary.attention} need attention</span>
          ) : null}
          {summary.failed > 0 ? (
            <span className="text-thread-failed">{summary.failed} failed</span>
          ) : null}
        </span>
      </span>
    </button>
  );
}
