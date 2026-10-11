export interface ConfirmedSentPrompt {
  id: string;
  text: string | null;
  target: { threadId: string; label: string | null };
  sentAt: number;
  confirmation: {
    status: "confirmed";
    kind: "handoff-receipt";
    receiptId: string;
    source: string;
  };
  sourceLabel: string | null;
  workstreams: readonly string[];
}

export type SentPromptsState =
  | { status: "unavailable" }
  | { status: "loading" }
  | {
      status: "ready";
      provenance: "live" | "sample";
      entries: readonly ConfirmedSentPrompt[];
    };

const sentTimeFormatter = new Intl.DateTimeFormat("en-US", {
  month: "short",
  day: "numeric",
  hour: "numeric",
  minute: "2-digit",
  timeZone: "America/New_York",
  timeZoneName: "short",
});

function hasConfirmedHandoff(entry: ConfirmedSentPrompt) {
  return (
    entry.confirmation?.status === "confirmed" &&
    entry.confirmation.kind === "handoff-receipt" &&
    typeof entry.confirmation.receiptId === "string" &&
    entry.confirmation.receiptId.trim().length > 0 &&
    typeof entry.confirmation.source === "string" &&
    entry.confirmation.source.trim().length > 0 &&
    Number.isFinite(new Date(entry.sentAt).getTime())
  );
}

export function SentPrompts({ state }: { state: SentPromptsState }) {
  if (state.status === "unavailable") {
    return (
      <p className="text-sm text-muted-foreground">
        Sent history is unavailable from this connection. Unconfirmed handoffs remain under Queued.
      </p>
    );
  }
  if (state.status === "loading") {
    return (
      <p role="status" className="text-sm text-muted-foreground">
        Loading sent prompts…
      </p>
    );
  }
  const entries = state.entries.filter(hasConfirmedHandoff);
  const omitted = entries.length !== state.entries.length;
  return (
    <section aria-label="Sent prompts" className="min-w-0 space-y-4">
      {state.provenance === "sample" ? (
        <div className="rounded-lg border border-dashed bg-muted/30 p-3">
          <p className="text-xs font-semibold tracking-wide">SAMPLE DATA</p>
          <p className="text-sm text-muted-foreground">Example sent prompts · not live history</p>
        </div>
      ) : null}
      <p className="text-sm text-muted-foreground">Sent confirms handoff, not completed work.</p>
      {omitted ? (
        <p role="status" className="text-sm text-muted-foreground">
          Some sent prompts are unavailable because delivery confirmation is missing or invalid.
        </p>
      ) : entries.length === 0 ? (
        <p className="text-sm text-muted-foreground">No sent prompts observed.</p>
      ) : null}
      {entries.map((entry) => (
        <article key={entry.id} className="min-w-0 space-y-3 rounded-lg border p-4">
          <div className="flex items-start justify-between gap-3">
            <h3 className="min-w-0 wrap-anywhere text-sm font-medium">
              {entry.target.label ?? entry.target.threadId}
            </h3>
            <span className="shrink-0 text-xs text-muted-foreground">Sent</span>
          </div>
          {entry.text === null ? (
            <p className="text-sm text-muted-foreground">Prompt text is unavailable.</p>
          ) : (
            <p className="whitespace-pre-wrap wrap-anywhere text-sm">{entry.text}</p>
          )}
          <div className="flex flex-wrap gap-x-3 gap-y-1 text-xs text-muted-foreground">
            <time dateTime={new Date(entry.sentAt).toISOString()}>
              {sentTimeFormatter.format(entry.sentAt)}
            </time>
            {entry.sourceLabel ? <span className="wrap-anywhere">{entry.sourceLabel}</span> : null}
            {entry.workstreams.length > 0 ? (
              <span className="wrap-anywhere">{entry.workstreams.join(" · ")}</span>
            ) : null}
          </div>
          <details className="text-xs text-muted-foreground">
            <summary className="cursor-pointer">Delivery details</summary>
            <dl className="mt-2 grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-1">
              <dt>Receipt</dt>
              <dd className="wrap-anywhere">{entry.confirmation.receiptId}</dd>
              <dt>Source</dt>
              <dd className="wrap-anywhere">{entry.confirmation.source}</dd>
              <dt>Thread</dt>
              <dd className="wrap-anywhere">{entry.target.threadId}</dd>
            </dl>
          </details>
        </article>
      ))}
    </section>
  );
}
