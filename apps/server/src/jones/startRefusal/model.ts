import type {
  MessageId,
  OrchestrationV2DelegatedCompletionDelivery,
  OrchestrationV2StoredEvent,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";

export type AttemptScope = "delegated_completion" | "dependency_wake";
export type RefusalReason =
  | "restart_admission"
  | "continuity_unavailable"
  | "continuity_held"
  | "control_held";

export interface AttemptKey {
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly attemptId: RunAttemptId;
}

export interface HoldKey {
  readonly threadId: ThreadId;
  readonly parentRunId: RunId;
  readonly generation: number;
  readonly messageId: MessageId;
}

export interface HoldReservation {
  readonly threadId: ThreadId;
  readonly parentRunId: RunId;
  readonly refusedRunId: RunId;
  readonly source: OrchestrationV2DelegatedCompletionDelivery;
  readonly successor: OrchestrationV2DelegatedCompletionDelivery;
}

export interface ArmedHold extends HoldKey {
  readonly refusedRunId: RunId;
  readonly releaseEventId: string;
  readonly releaseSequence: number;
}

// Running alone includes Codex's pre-acknowledgement turn. Only the native reply
// or turn/started notification supplies its non-null reference. Keep this list
// isolated: unproved drivers retain a manual hold.
export const acceptedStartWitness = (stored: OrchestrationV2StoredEvent) => {
  const event = stored.event;
  return event.type === "provider-turn.updated" &&
    event.payload.status === "running" &&
    event.payload.nativeTurnRef != null &&
    event.payload.nativeTurnRef.driver === "codex" &&
    event.payload.runAttemptId !== null &&
    event.driver === "codex"
    ? event.payload
    : undefined;
};
