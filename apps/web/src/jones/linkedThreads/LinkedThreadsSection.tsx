import { createContext, useId, useRef, useState, type ReactNode } from "react";
import { CollapsibleSectionHeader } from "../../components/ui/collapsible-section-header";
import { ThreadDetailsControl } from "../../components/chat/ThreadDetailsControl";

export const LinkedContinuationFocusContext = createContext(0);

export function LinkedThreadsSection(props: {
  readonly runningCount: number;
  readonly count: number;
  readonly incomplete?: boolean;
  readonly continued?: boolean;
  readonly onExpand?: () => void;
  readonly actions?: ReactNode;
  readonly children: ReactNode;
}) {
  const [expanded, setExpanded] = useState(false);
  const [focusRequest, setFocusRequest] = useState(0);
  const headingId = useId();
  const contentId = useId();
  const content = useRef<HTMLDivElement>(null);
  const focusContinuations = useRef(false);
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
            onClick={() => {
              if (!expanded) props.onExpand?.();
              setExpanded(!expanded);
            }}
          >
            {`Linked${props.runningCount > 0 ? ` · ${props.runningCount} running` : ""} (${props.count}${props.incomplete ? "+" : ""})`}
          </CollapsibleSectionHeader>
        </h3>
        {props.continued ? (
          <ThreadDetailsControl
            size="xs"
            variant="ghost"
            part="meta"
            onClick={() => {
              props.onExpand?.();
              focusContinuations.current = true;
              setFocusRequest((value) => value + 1);
              setExpanded(true);
              if (expanded)
                content.current
                  ?.querySelector<HTMLElement>('[data-linked-group="continuations"] button')
                  ?.focus();
            }}
          >
            Continued
          </ThreadDetailsControl>
        ) : null}
        {props.actions ? (
          <div className="flex shrink-0 items-center gap-1">{props.actions}</div>
        ) : null}
      </div>
      <div
        id={contentId}
        hidden={!expanded}
        ref={(node) => {
          content.current = node;
          if (node && expanded && focusContinuations.current) {
            focusContinuations.current = false;
            node
              .querySelector<HTMLElement>(
                '[data-linked-group="continuations"] button, [data-has-continuation] button',
              )
              ?.focus();
          }
        }}
      >
        {expanded ? (
          <LinkedContinuationFocusContext value={focusRequest}>
            {props.children}
          </LinkedContinuationFocusContext>
        ) : null}
      </div>
    </section>
  );
}
