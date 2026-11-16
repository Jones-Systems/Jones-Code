import type {
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import type { ProviderAdapterV2Event } from "@t3tools/provider-core/server/ProviderAdapter";
import * as Effect from "effect/Effect";
import * as ProviderEventOrigin from "@t3tools/provider-core/server/jones/ProviderEventOrigin";

export const reasons = [
  "origin",
  "identity",
  "capture",
  "projection_turn",
  "locator",
  "effect",
  "successor",
  "replacement",
  "ineligible",
  "ordinary_rejected",
  "write_error",
] as const;
export type Reason = (typeof reasons)[number];
export interface Key {
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly providerThreadId: ProviderThreadId;
  readonly providerTurnId: ProviderTurnId;
  readonly interruptedAttemptId: RunAttemptId;
}

export function terminalKey(event: ProviderAdapterV2Event): Key | undefined {
  const origin = ProviderEventOrigin.readProviderEventOrigin(event)?.turn;
  if (origin === undefined) return;
  if (
    event.type === "provider_turn.updated"
      ? event.providerTurn.status === "running" ||
        event.providerTurn.id !== origin.providerTurnId ||
        event.providerTurn.runAttemptId !== origin.attemptId ||
        event.providerTurn.providerThreadId !== origin.binding.providerThreadId
      : event.type !== "turn.terminal" || event.providerTurnId !== origin.providerTurnId
  )
    return;
  return {
    threadId: origin.binding.threadId,
    runId: origin.runId,
    providerThreadId: origin.binding.providerThreadId,
    providerTurnId: origin.providerTurnId,
    interruptedAttemptId: origin.attemptId,
  };
}

// Each sink owns this bounded diagnostic cache. It supplies no admission or retirement proof.
export function make(capacity = 4096) {
  const limit = Number.isFinite(capacity)
    ? Math.max(1, Math.min(4096, Math.floor(capacity)))
    : 4096;
  const entries = new Map<string, Reason>();
  const keyOf = (key: Key) =>
    [key.threadId, key.runId, key.providerThreadId, key.providerTurnId, key.interruptedAttemptId]
      .map((value) => `${value.length}:${value}`)
      .join("");
  return {
    record(key: Key, reason: Reason) {
      if (!reasons.includes(reason)) return;
      const id = keyOf(key);
      entries.delete(id);
      entries.set(id, reason);
      if (entries.size > limit) entries.delete(entries.keys().next().value!);
    },
    read: (key: Key) => entries.get(keyOf(key)),
    clear: (key: Key) => {
      entries.delete(keyOf(key));
    },
  };
}

// The enclosing consumer/ingestion operation owns this per-event diagnostic span.
export const recordTerminalRejection = Effect.fnUntraced(function* (
  trace: ReturnType<typeof make> | undefined,
  event: ProviderAdapterV2Event,
  reason: Reason,
) {
  const key = terminalKey(event);
  if (trace === undefined || key === undefined) return;
  trace.record(key, reason);
  yield* Effect.logWarning("Provider terminal history was not persisted", { ...key, reason });
});
