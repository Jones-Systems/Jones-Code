import { useId, useState, type ReactNode } from "react";

import { CollapsibleSectionHeader } from "../../components/ui/collapsible-section-header";

export function LinkedThreadsSection(props: {
  readonly runningCount: number;
  readonly actions?: ReactNode;
  readonly children: ReactNode;
}) {
  const [expanded, setExpanded] = useState(false);
  const headingId = useId();
  const contentId = useId();

  return (
    <section
      aria-labelledby={headingId}
      data-thread-relationships-panel
      className="border-t border-border/65 px-2 pt-2 pb-2.5"
    >
      <div className="flex min-h-8 min-w-0 items-center gap-1">
        <h3 id={headingId} className="min-w-0 flex-1">
          <CollapsibleSectionHeader
            variant="panel"
            expanded={expanded}
            aria-label="Linked threads"
            aria-controls={contentId}
            onClick={() => setExpanded((value) => !value)}
          >
            {props.runningCount > 0 ? `Linked · ${props.runningCount} running` : "Linked"}
          </CollapsibleSectionHeader>
        </h3>
        {props.actions ? <div className="flex shrink-0 items-center gap-1">{props.actions}</div> : null}
      </div>
      <div id={contentId} hidden={!expanded}>
        {expanded ? props.children : null}
      </div>
    </section>
  );
}
