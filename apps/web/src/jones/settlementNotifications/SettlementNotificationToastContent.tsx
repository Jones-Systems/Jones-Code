import { useState } from "react";
import { ChevronDownIcon, ChevronUpIcon } from "lucide-react";
import { Tooltip, TooltipTrigger, TooltipPopup } from "../../components/ui/tooltip";
import type { SettlementEntry } from "@t3tools/client-runtime/jones/settlement-notifications";

export function SettlementNotificationToastContent({
  entries,
  onOpen,
  collapsedIds,
  onCollapsedChange,
}: {
  entries: readonly SettlementEntry[];
  collapsedIds: ReadonlySet<string>;
  onCollapsedChange: (ids: Set<string>) => void;
  onOpen: (entry: SettlementEntry) => void;
}) {
  const [collapsed, setCollapsed] = useState(() => new Set(collapsedIds));
  const toggle = (id: string) => {
    const next = new Set(collapsed);
    if (next.has(id)) next.delete(id);
    else next.add(id);
    onCollapsedChange(next);
    setCollapsed(next);
  };
  return (
    <ul
      className={
        entries.length === 1
          ? "flex max-h-40 flex-col gap-2 overflow-y-auto overscroll-contain"
          : "flex flex-col gap-2"
      }
    >
      {entries.map((entry) => (
        <li
          key={entry.id}
          data-slot="settlement-row"
          className={
            entry.projectRemaining?.length
              ? "min-w-0 cursor-pointer rounded-sm hover:bg-muted/40"
              : "min-w-0"
          }
          onClick={
            entry.projectRemaining?.length
              ? () => {
                  if (!window.getSelection()?.toString()) toggle(entry.id);
                }
              : undefined
          }
        >
          <div className="flex min-w-0 items-start gap-1">
            <button
              type="button"
              className="max-w-full text-left wrap-anywhere underline underline-offset-2 hover:text-foreground"
              onClick={(event) => {
                event.stopPropagation();
                onOpen(entry);
              }}
            >
              {entry.title || "Untitled thread"}
            </button>
            {entry.projectRemaining?.length ? (
              <button
                type="button"
                className="ml-auto shrink-0 rounded-sm p-0.5 text-muted-foreground hover:text-foreground focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring"
                aria-expanded={!collapsed.has(entry.id)}
                aria-label={`${collapsed.has(entry.id) ? "Show" : "Hide"} remaining work for ${entry.title || "Untitled thread"}`}
                onClick={(event) => {
                  event.stopPropagation();
                  toggle(entry.id);
                }}
              >
                {collapsed.has(entry.id) ? (
                  <ChevronDownIcon aria-hidden className="size-3.5" />
                ) : (
                  <ChevronUpIcon aria-hidden className="size-3.5" />
                )}
              </button>
            ) : null}
          </div>
          {entry.projectRemaining?.length && !collapsed.has(entry.id) ? (
            <ul
              data-slot="settlement-remaining"
              className="mt-0.5 list-disc space-y-0.5 pl-4 text-xs text-muted-foreground"
            >
              {entry.projectRemaining.map((item, index) => (
                <li key={`${index}:${item}`} className="min-w-0">
                  <Tooltip>
                    <TooltipTrigger
                      render={
                        <span
                          data-slot="settlement-remaining-item"
                          className="block truncate outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring"
                          tabIndex={0}
                        />
                      }
                    >
                      {item}
                    </TooltipTrigger>
                    <TooltipPopup>{item}</TooltipPopup>
                  </Tooltip>
                </li>
              ))}
            </ul>
          ) : null}
          {entry.threadGone ? (
            <span className="ml-2 text-xs text-muted-foreground">Thread unavailable</span>
          ) : null}
        </li>
      ))}
    </ul>
  );
}
