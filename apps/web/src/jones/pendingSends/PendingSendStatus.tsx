import type { EnvironmentId, MessageId, ThreadId } from "@t3tools/contracts";
import { usePendingSends } from "./hooks";
import { pendingSends } from "./store";

export function PendingSendStatus(props: {
  environmentId: EnvironmentId;
  threadId: ThreadId;
  messageId: MessageId;
}) {
  const entry = usePendingSends().find(
    (entry) =>
      entry.environmentId === props.environmentId &&
      entry.threadId === props.threadId &&
      entry.message.id === props.messageId,
  );
  if (!entry || entry.delivery === "accepted") return null;
  const label =
    entry.delivery === "sending"
      ? "Sending"
      : entry.delivery === "uncertain"
        ? "Waiting for confirmation"
        : "Couldn't send";
  return (
    <div
      className="flex items-center gap-1.5 text-xs text-muted-foreground"
      data-delivery-state={entry.delivery}
      data-thread-find-ignore
    >
      <span
        aria-hidden
        className={`size-1.5 shrink-0 rounded-full ${entry.delivery === "failed" ? "bg-destructive" : entry.delivery === "uncertain" ? "bg-warning" : "bg-info"}`}
      />
      <span role="status">{label}</span>
      {entry.delivery === "uncertain" ? <span>Kept in this window</span> : null}
      {entry.retry && entry.delivery !== "sending" ? (
        <button
          type="button"
          aria-label="Retry sending this message"
          disabled={entry.inFlight}
          className="underline"
          onClick={() => {
            void entry.retry?.();
          }}
        >
          Retry
        </button>
      ) : null}
      {entry.delivery !== "sending" ? (
        <button
          type="button"
          aria-label="Dismiss unsent message"
          className="underline"
          onClick={() => pendingSends.dismiss(entry.key)}
        >
          Dismiss
        </button>
      ) : null}
    </div>
  );
}
