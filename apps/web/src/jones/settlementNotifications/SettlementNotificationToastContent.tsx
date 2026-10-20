import type { SettlementEntry } from "@t3tools/client-runtime/jones/settlement-notifications";

export function SettlementNotificationToastContent({
  entries,
  onOpen,
}: {
  entries: readonly SettlementEntry[];
  onOpen: (entry: SettlementEntry) => void;
}) {
  return (
    <ul
      className={
        entries.length === 1
          ? "flex max-h-40 flex-col gap-2 overflow-y-auto overscroll-contain"
          : "flex flex-col gap-2"
      }
    >
      {entries.map((entry) => (
        <li key={entry.id} className="min-w-0">
          <button
            type="button"
            className="max-w-full text-left wrap-anywhere underline underline-offset-2 hover:text-foreground"
            onClick={() => onOpen(entry)}
          >
            {entry.title || "Untitled thread"}
          </button>
          {entry.projectRemaining?.length ? (
            <ul className="mt-1 list-disc space-y-1 pl-4">
              {entry.projectRemaining.map((item, index) => (
                <li key={`${index}:${item}`} className="wrap-anywhere">
                  {item}
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
