import { Maximize2, Minimize2 } from "lucide";
import { useState, type ReactNode, type RefObject } from "react";
import { ScrollArea } from "../../components/ui/scroll-area";
import { getClientSettings } from "../../hooks/useSettings";
import { observeResize } from "../../lib/observeResize";
import { MorphIcon } from "../../components/MorphIcon";
import { Button } from "../../components/ui/button";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import "./markdownTables.css";

function tableHasClippedCells(container: HTMLElement, table: HTMLTableElement): boolean {
  const viewport = table.closest<HTMLElement>('[data-slot="scroll-area-viewport"]');
  const scrollLeft = viewport?.scrollLeft ?? 0;
  const previous = container.dataset.expanded;
  container.dataset.expanded = "false";
  try {
    for (const row of table.rows) {
      for (const cell of row.cells) {
        if (cell.scrollWidth > cell.clientWidth) return true;
      }
    }
    return false;
  } finally {
    if (previous === undefined) delete container.dataset.expanded;
    else container.dataset.expanded = previous;
    // The unwrapped layout can be narrower and clamp a sideways scroll.
    if (viewport && scrollLeft > 0) viewport.scrollLeft = scrollLeft;
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
  const [hasClippedCells, setHasClippedCells] = useState(false);
  // React calls this with null when a previous attach returned no cleanup.
  const attachContainer = (container: HTMLDivElement | null) => {
    containerRef.current = container;
    const table = container?.querySelector("table");
    if (!container || !table) return;
    const measure = () => setHasClippedCells(tableHasClippedCells(container, table));
    measure();
    // A streamed cell can grow until it clips while the table keeps its size, and a
    // narrower column can clip a cell without this component rendering.
    const mutations = new MutationObserver(measure);
    mutations.observe(table, { childList: true, characterData: true, subtree: true });
    const stopResize = observeResize(table, measure);
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
        {hasClippedCells ? (
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
              <MorphIcon className="size-3" icon={expanded ? Minimize2 : Maximize2} />
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
