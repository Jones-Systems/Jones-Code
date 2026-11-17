import { useEffect, useRef, type FocusEvent, type ReactNode } from "react";
import { useMediaQuery } from "../../hooks/useMediaQuery";
import { PromptRow, type PromptRowView } from "./PromptRow";

export interface PromptListGroup {
  readonly key: string;
  readonly label: string;
  readonly rows: readonly PromptRowView[];
}

export interface PromptListHandlers {
  readonly onPointerEnter: () => void;
  readonly onPointerLeave: () => void;
  readonly onFocusCapture: () => void;
  readonly onBlurCapture: (event: FocusEvent<HTMLElement>) => void;
}

/**
 * Master–detail layout shared by every stage, in the Requests geometry: compact rows on the
 * left and one detail pane on the right. Below `md` the selected row expands in place and
 * focus moves to its heading, so nothing scrolls sideways at narrow widths.
 */
export function PromptMasterDetail({
  label,
  groups,
  showGroupHeadings,
  selectedKey,
  onSelect,
  detail,
  empty,
  listHandlers,
}: {
  readonly label: string;
  readonly groups: readonly PromptListGroup[];
  readonly showGroupHeadings: boolean;
  readonly selectedKey: string | null;
  readonly onSelect: (key: string) => void;
  readonly detail: ReactNode;
  readonly empty: ReactNode;
  readonly listHandlers?: PromptListHandlers;
}) {
  const wide = useMediaQuery("md");
  const container = useRef<HTMLDivElement>(null);
  const selectedShown = groups.some((group) => group.rows.some((row) => row.key === selectedKey));
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Move keyboard focus to the expanded detail heading after a narrow-layout selection.
  useEffect(() => {
    if (wide || selectedKey === null) return;
    container.current?.querySelector<HTMLElement>("[data-prompt-detail-heading]")?.focus();
  }, [selectedKey, wide]);
  return (
    <div
      ref={container}
      className="grid min-w-0 gap-4 md:grid-cols-[minmax(14rem,2fr)_minmax(0,3fr)]"
    >
      <div role="group" aria-label={label} className="min-w-0 space-y-4" {...listHandlers}>
        {!wide && selectedKey !== null && !selectedShown ? detail : null}
        {groups.length === 0
          ? empty
          : groups.map((group) => (
              <div key={group.key} className="min-w-0 space-y-1.5">
                {showGroupHeadings ? (
                  <h3 className="text-xs font-medium text-muted-foreground">
                    {group.label} · {group.rows.length}
                  </h3>
                ) : null}
                <ul
                  className="m-0 min-w-0 list-none space-y-1.5 p-0"
                  aria-label={`${label}: ${group.label}`}
                >
                  {group.rows.map((row) => (
                    <li key={row.key} className="min-w-0">
                      <PromptRow
                        row={row}
                        selected={row.key === selectedKey}
                        onSelect={() => onSelect(row.key)}
                      />
                      {!wide && row.key === selectedKey ? (
                        <div className="mt-2">{detail}</div>
                      ) : null}
                    </li>
                  ))}
                </ul>
              </div>
            ))}
      </div>
      {wide ? <div className="min-w-0 md:sticky md:top-0 md:self-start">{detail}</div> : null}
    </div>
  );
}
