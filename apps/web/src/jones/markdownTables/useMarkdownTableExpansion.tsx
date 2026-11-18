import { Maximize2Icon, Minimize2Icon } from "lucide-react";
import { useState, type ReactNode, type RefObject } from "react";
import { ScrollArea } from "../../components/ui/scroll-area";
import { getClientSettings } from "../../hooks/useSettings";
import { observeResize } from "../../lib/observeResize";
import { Button } from "../../components/ui/button";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import "./markdownTables.css";

function collapsedTableNeedsExpansion(
  container: HTMLElement,
  table: HTMLTableElement,
  viewport: HTMLElement,
): boolean {
  const { scrollLeft, scrollTop } = viewport;
  const previous = container.dataset.expanded;
  container.dataset.expanded = "false";
  try {
    if (viewport.scrollWidth > viewport.clientWidth) return true;
    for (const row of table.rows) {
      for (const cell of row.cells) {
        if (cell.scrollWidth > cell.clientWidth) return true;
      }
    }
    return false;
  } finally {
    if (previous === undefined) delete container.dataset.expanded;
    else container.dataset.expanded = previous;
    // Measuring the collapsed layout can clamp either scroll offset.
    if (viewport.scrollLeft !== scrollLeft) viewport.scrollLeft = scrollLeft;
    if (viewport.scrollTop !== scrollTop) viewport.scrollTop = scrollTop;
  }
}

export function MarkdownTableExpansion({
  containerRef,
  children,
  controls,
}: {
  readonly containerRef: RefObject<HTMLDivElement | null>;
  readonly children: ReactNode;
  readonly controls: ReactNode;
}) {
  const [expanded, setExpanded] = useState(() => getClientSettings().wordWrap);
  const [canExpand, setCanExpand] = useState(false);
  // React calls this with null when a previous attach returned no cleanup.
  const attachContainer = (container: HTMLDivElement | null) => {
    containerRef.current = container;
    const table = container?.querySelector("table");
    const viewport = table?.closest<HTMLElement>('[data-slot="scroll-area-viewport"]');
    const markdown = container?.parentElement;
    if (!container || !table || !viewport || !markdown) return;
    const measure = () => setCanExpand(collapsedTableNeedsExpansion(container, table, viewport));
    measure();
    // Streaming can change cell overflow without resizing the table. The parent
    // also tracks the available collapsed width when an expanded table is wider.
    const mutations = new MutationObserver(measure);
    mutations.observe(table, { childList: true, characterData: true, subtree: true });
    const stopResize = observeResize([table, markdown], measure);
    return () => {
      mutations.disconnect();
      stopResize();
      containerRef.current = null;
    };
  };

  const expandLabel = expanded ? "Collapse table" : "Expand table";
  return (
    <div
      ref={attachContainer}
      className="chat-markdown-table-container"
      data-expanded={expanded ? "true" : "false"}
    >
      <ScrollArea
        radius="none"
        chainVerticalScroll
        scrollFade
        className="w-full max-w-full"
        data-markdown-table-scroll="true"
      >
        {children}
      </ScrollArea>
      <div className="mt-0.5 -mx-1 flex items-center justify-between select-none">
        {canExpand ? (
          <Tooltip>
            <TooltipTrigger
              render={
                <Button
                  type="button"
                  variant={expanded ? "secondary" : "ghost-muted"}
                  size="icon-xs"
                  aria-pressed={expanded}
                  onClick={() => setExpanded((value) => !value)}
                  aria-label={expandLabel}
                />
              }
            >
              {expanded ? (
                <Minimize2Icon className="size-3" />
              ) : (
                <Maximize2Icon className="size-3" />
              )}
            </TooltipTrigger>
            <TooltipPopup side="top">{expandLabel}</TooltipPopup>
          </Tooltip>
        ) : (
          <span />
        )}
        {controls}
      </div>
    </div>
  );
}
