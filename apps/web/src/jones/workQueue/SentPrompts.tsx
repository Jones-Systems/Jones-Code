import type { ReactNode } from "react";
import { usePromptTime } from "./PromptRow";

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
  providerState?: "pending" | "running" | "completed" | "interrupted" | "error";
}

export type SentPromptsState =
  | { status: "unavailable" }
  | { status: "loading" }
  | {
      status: "ready";
      /** Confirmed handoffs shown, after entries with incomplete receipts are dropped. */
      count: number;
      omitted: boolean;
      observation?: { sampledAt: number; coverage: "complete" | "partial"; stale: boolean };
    };

/**
 * The Sent stage frame: what Sent proves, how current the sample is, and the honest empty
 * states. The confirmed handoffs render as Prompts rows supplied by the caller.
 */
export function SentPrompts({
  state,
  children,
}: {
  state: SentPromptsState;
  children?: ReactNode;
}) {
  const formatTime = usePromptTime();
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
  return (
    <section aria-label="Sent prompts" className="min-w-0 space-y-3">
      {state.observation ? (
        <p role="status" className="text-sm text-muted-foreground">
          {state.observation.stale
            ? "Previously observed confirmed handoffs"
            : "Confirmed handoffs in this sample"}
          {" · Sampled "}
          {formatTime(state.observation.sampledAt)}
          {" · Coverage: "}
          {state.observation.coverage}
          {" · This sample is not a complete sent history."}
        </p>
      ) : null}
      {state.omitted ? (
        <p role="status" className="text-sm text-muted-foreground">
          Some sent prompts are unavailable because delivery confirmation is missing or invalid.
        </p>
      ) : state.count === 0 ? (
        <p className="text-sm text-muted-foreground">
          {state.observation
            ? "No confirmed handoffs in this sample; other sent prompts may be unavailable."
            : "No sent prompts observed."}
        </p>
      ) : null}
      {children}
    </section>
  );
}
