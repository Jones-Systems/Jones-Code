import type { SettlementEntry } from "@t3tools/client-runtime/jones/settlement-notifications";

export function SettlementNotificationToastContent({
  entries,
  onOpen,
}: {
  entries: readonly SettlementEntry[];
  onOpen: (entry: SettlementEntry) => void;
}) {
  return (
    <ul className="flex max-h-64 flex-col gap-2 overflow-y-auto">
      {entries.map((entry) => (
        <li key={entry.id}>
          <button
            type="button"
            className="text-left underline underline-offset-2 hover:text-foreground"
            onClick={() => onOpen(entry)}
          >
            {entry.title || "Untitled thread"}
          </button>
          {entry.threadGone ? (
            <span className="ml-2 text-xs text-muted-foreground">Thread unavailable</span>
          ) : null}
        </li>
      ))}
    </ul>
  );
}
