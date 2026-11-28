import type { EnvironmentId, MessageId, ThreadId } from "@t3tools/contracts";
import { T, useGT } from "gt-react";
import { usePendingSends } from "./hooks";
import { pendingSends } from "./store";

export function PendingSendStatus(props: {
  environmentId: EnvironmentId;
  threadId: ThreadId;
  messageId: MessageId;
}) {
  const gt = useGT();
  const entry = usePendingSends().find(
    (entry) =>
      entry.environmentId === props.environmentId &&
      entry.threadId === props.threadId &&
      entry.message.id === props.messageId,
  );
  if (!entry || entry.delivery === "accepted") return null;
  const label =
    entry.delivery === "sending"
      ? gt("Sending")
      : entry.delivery === "uncertain"
        ? gt("Waiting for confirmation")
        : gt("Couldn't send");
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
      {entry.delivery === "uncertain" ? (
        <span>
          {entry.durable
            ? entry.retry
              ? gt("Saved on this device")
              : gt("Status saved. Retry is unavailable after reload.")
            : gt("Kept in this window")}
        </span>
      ) : null}
      {entry.retry && entry.delivery !== "sending" ? (
        <button
          type="button"
          aria-label={gt("Retry sending this message")}
          disabled={entry.inFlight}
          className="underline"
          onClick={() => {
            void entry.retry?.();
          }}
        >
          <T>Retry</T>
        </button>
      ) : null}
      {entry.delivery !== "sending" && (!entry.durable || entry.delivery === "failed") ? (
        <button
          type="button"
          aria-label={gt("Dismiss unsent message")}
          className="underline"
          onClick={() => pendingSends.dismiss(entry.key)}
        >
          <T>Dismiss</T>
        </button>
      ) : null}
    </div>
  );
}
