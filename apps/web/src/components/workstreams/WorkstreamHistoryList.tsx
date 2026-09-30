import { Button } from "../ui/button";
import type { WorkstreamHistoryEntry } from "./types";

export interface WorkstreamHistoryListProps {
  readonly entries: readonly WorkstreamHistoryEntry[];
  readonly hasMore: boolean;
  readonly onLoadMore: () => void;
}

export function WorkstreamHistoryList(props: WorkstreamHistoryListProps) {
  return (
    <section aria-labelledby="workstream-history-title">
      <h3 className="font-medium text-sm" id="workstream-history-title">
        Permanent membership history
      </h3>
      <ol className="mt-2 space-y-2">
        {props.entries.map((entry) => (
          <li className="border-border/70 border-s ps-3" key={entry.id}>
            <div className="flex flex-wrap items-baseline justify-between gap-2">
              <span className="text-sm">{entry.label}</span>
              <time className="text-muted-foreground text-xs" dateTime={entry.occurredAt}>
                {entry.occurredAt}
              </time>
            </div>
            {entry.detail && <p className="mt-0.5 text-muted-foreground text-xs">{entry.detail}</p>}
          </li>
        ))}
      </ol>
      {props.hasMore && (
        <Button className="mt-3" onClick={props.onLoadMore} size="xs" variant="outline">
          Load older history
        </Button>
      )}
    </section>
  );
}
