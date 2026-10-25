import * as NodeBuffer from "node:buffer";

import type { CodexStartDispatch } from "./CodexLateStartRecovery.ts";

// Deltas can greatly outnumber the existing 32 early lifecycle facts. Keep a
// finite burst budget, including payload bytes so one large output cannot pin
// arbitrary memory while the original start RPC is outstanding.
const MAX_EVENTS = 512;
const MAX_BYTES = 1024 * 1024;

interface PendingNotification<A> {
  readonly nativeTurnId: string | undefined;
  readonly threadWideUsage: boolean;
  readonly replay: A;
}

interface PendingStart<A> {
  readonly dispatch: CodexStartDispatch;
  events: Array<PendingNotification<A>>;
  bytes: number;
  count: number;
  acknowledgedTurnId: string | undefined;
}

const contextMethods = new Set([
  "item/started",
  "item/completed",
  "item/agentMessage/delta",
  "item/commandExecution/outputDelta",
  "item/reasoning/summaryTextDelta",
  "item/reasoning/textDelta",
  "item/plan/delta",
  "turn/plan/updated",
  "thread/tokenUsage/updated",
]);
const lifecycleMethods = new Set([
  "turn/started",
  "turn/completed",
  "error",
  "thread/goal/updated",
  "thread/goal/cleared",
]);

export function makeCodexPendingStartNotifications<A>() {
  const pending = new Map<string, PendingStart<A>>();
  const byDispatch = new Map<CodexStartDispatch, PendingStart<A>>();
  const clear = (dispatch: CodexStartDispatch) => {
    byDispatch.delete(dispatch);
    if (pending.get(dispatch.nativeThreadId)?.dispatch === dispatch)
      pending.delete(dispatch.nativeThreadId);
  };
  return {
    begin(dispatch: CodexStartDispatch) {
      const entry: PendingStart<A> = {
        dispatch,
        events: [],
        bytes: 0,
        count: 0,
        acknowledgedTurnId: undefined,
      };
      pending.set(dispatch.nativeThreadId, entry);
      byDispatch.set(dispatch, entry);
    },
    capture(runtimeGeneration: string, method: string, payload: unknown, replay: A) {
      if (!contextMethods.has(method) && !lifecycleMethods.has(method)) return;
      if (typeof payload !== "object" || payload === null || !("threadId" in payload)) return;
      if (typeof payload.threadId !== "string") return;
      const entry = pending.get(payload.threadId);
      if (entry === undefined || entry.dispatch.runtimeGeneration !== runtimeGeneration) return;
      // Lifecycle handlers record pre-ACK facts for unknown-start reconciliation.
      // During replay their effects must queue behind the earlier items.
      if (entry.acknowledgedTurnId === undefined && lifecycleMethods.has(method)) return;
      const nativeTurnId =
        "turnId" in payload
          ? payload.turnId
          : "turn" in payload &&
              typeof payload.turn === "object" &&
              payload.turn !== null &&
              "id" in payload.turn
            ? payload.turn.id
            : undefined;
      if (
        entry.acknowledgedTurnId === undefined &&
        (typeof nativeTurnId !== "string" || entry.dispatch.excludedTurnIds.has(nativeTurnId))
      )
        return;
      const bytes = NodeBuffer.Buffer.byteLength(JSON.stringify(payload), "utf8");
      if (entry.count >= MAX_EVENTS || bytes > MAX_BYTES - entry.bytes) {
        clear(entry.dispatch);
        entry.events.length = 0;
        return { status: "overflow", dispatch: entry.dispatch } as const;
      }
      // Once replay begins, same-thread successors and goal updates must stay
      // behind the predecessor's terminal. Their handlers retain turn ownership.
      entry.events.push({
        nativeTurnId: typeof nativeTurnId === "string" ? nativeTurnId : undefined,
        threadWideUsage: method === "thread/tokenUsage/updated",
        replay,
      });
      entry.bytes += bytes;
      entry.count++;
      return { status: "buffered", dispatch: entry.dispatch } as const;
    },
    // Only the adapter's accepted original ACK may call this. Notifications do
    // not bind a dispatch, revive a cancelled attempt, or authorize prompt replay.
    acknowledge(dispatch: CodexStartDispatch, nativeTurnId: string) {
      const entry = byDispatch.get(dispatch);
      if (entry === undefined || dispatch.excludedTurnIds.has(nativeTurnId)) return;
      // Only pre-ACK notifications need selection. Later arrivals are already
      // ordered behind this acknowledged turn and must not be filtered as its items.
      // Usage also carries a thread-wide baseline. Its handler accounts for
      // history without creating or awaiting a turn context.
      entry.events = entry.events.filter(
        (event) => event.nativeTurnId === nativeTurnId || event.threadWideUsage,
      );
      entry.acknowledgedTurnId = nativeTurnId;
    },
    take(dispatch: CodexStartDispatch): A | undefined {
      // A terminal may admit a successor before this caller has drained the
      // old turn's tail. Keep that tail owned by its original dispatch.
      const entry = byDispatch.get(dispatch);
      if (entry === undefined || entry.acknowledgedTurnId === undefined) return;
      if (entry.events.length > 0) return entry.events.shift()!.replay;
      clear(dispatch);
    },
    clear,
  };
}
