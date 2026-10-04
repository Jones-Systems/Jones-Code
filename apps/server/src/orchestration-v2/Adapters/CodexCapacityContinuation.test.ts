import { describe, expect, it } from "vitest";

import {
  makeCodexCapacityContinuation,
  reduceCodexCapacityContinuation,
  type CodexCapacityContinuationAction,
  type CodexCapacityContinuationBinding,
  type CodexCapacityContinuationSignal,
  type CodexCapacityContinuationState,
} from "./CodexCapacityContinuation.ts";

const binding: CodexCapacityContinuationBinding = {
  runId: "run-capacity",
  attemptId: "attempt-capacity",
  providerThreadId: "provider-thread-capacity",
  nativeThreadId: "native-thread-capacity",
  runtimeGeneration: "generation-capacity",
};

function acknowledge(state: CodexCapacityContinuationState, nativeTurnId: string) {
  return reduceCodexCapacityContinuation(state, {
    type: "startAcknowledged",
    binding,
    retryOrdinal: state.retryOrdinal,
    nativeTurnId,
  });
}

function nativeError(
  state: CodexCapacityContinuationState,
  nativeTurnId: string,
  code: string | null = "serverOverloaded",
  willRetry = false,
) {
  return reduceCodexCapacityContinuation(state, {
    type: "nativeError",
    binding,
    nativeTurnId,
    code,
    willRetry,
  });
}

function complete(
  state: CodexCapacityContinuationState,
  nativeTurnId: string,
  status: "completed" | "failed" | "interrupted" = "failed",
) {
  return reduceCodexCapacityContinuation(state, {
    type: "nativeCompleted",
    binding,
    nativeTurnId,
    status,
  });
}

function waitingForRetry() {
  const started = acknowledge(makeCodexCapacityContinuation(binding), "turn-original");
  return complete(nativeError(started.state, "turn-original").state, "turn-original").state;
}

describe("Codex capacity continuation", () => {
  it("requires an acknowledged matching overload and failed completion before scheduling a retry", () => {
    const started = acknowledge(makeCodexCapacityContinuation(binding), "turn-original");
    expect(started.actions).toEqual([]);
    const overloaded = nativeError(started.state, "turn-original");
    expect(overloaded.actions).toEqual([]);
    expect(complete(overloaded.state, "other-turn").actions).toEqual([]);
    const failed = complete(overloaded.state, "turn-original");
    expect(failed.actions).toEqual([
      { type: "scheduleRetry", retryOrdinal: 1, delayMs: 10000 },
    ]);
    expect(failed.state.phase).toBe("waiting_retry");
    expect(failed.state.binding).toEqual(binding);
  });

  it("allows exactly five delayed promptless retries and one logical terminal", () => {
    let state = makeCodexCapacityContinuation(binding);
    const actions: CodexCapacityContinuationAction[] = [];
    for (let retryOrdinal = 0; retryOrdinal <= 5; retryOrdinal++) {
      const nativeTurnId = `turn-${retryOrdinal}`;
      state = acknowledge(state, nativeTurnId).state;
      state = nativeError(state, nativeTurnId).state;
      const failed = complete(state, nativeTurnId);
      state = failed.state;
      actions.push(...failed.actions);
      if (retryOrdinal < 5) {
        expect(failed.actions).toEqual([
          { type: "scheduleRetry", retryOrdinal: retryOrdinal + 1, delayMs: 10000 },
        ]);
        const ready = reduceCodexCapacityContinuation(state, {
          type: "retryReady",
          binding,
          retryOrdinal: retryOrdinal + 1,
        });
        expect(ready.actions).toEqual([
          { type: "retry", retryOrdinal: retryOrdinal + 1, promptless: true },
        ]);
        actions.push(...ready.actions);
        state = ready.state;
      }
    }
    expect(actions.filter((action) => action.type === "scheduleRetry")).toHaveLength(5);
    expect(actions.filter((action) => action.type === "retry")).toHaveLength(5);
    expect(actions.filter((action) => action.type === "terminal")).toEqual([
      { type: "terminal", status: "failed", nativeTurnId: "turn-5" },
    ]);
    expect(state.phase).toBe("terminal");
    expect(complete(state, "turn-5").actions).toEqual([]);
    expect(nativeError(state, "turn-5").actions).toEqual([]);
    expect(reduceCodexCapacityContinuation(state, {
      type: "retryReady", binding, retryOrdinal: 6,
    }).actions).toEqual([]);
  });

  it.each([
    { code: "serverOverloaded", willRetry: true },
    { code: "rateLimitExceeded", willRetry: false },
    { code: "usageLimitExceeded", willRetry: false },
    { code: null, willRetry: false },
  ])("preserves native retry ownership and other failures for $code/$willRetry", ({ code, willRetry }) => {
    let state = acknowledge(makeCodexCapacityContinuation(binding), "turn-original").state;
    state = nativeError(state, "turn-original", code, willRetry).state;
    expect(complete(state, "turn-original").actions).toEqual([
      { type: "terminal", status: "failed", nativeTurnId: "turn-original" },
    ]);
  });

  it.each(["completed", "interrupted"] as const)(
    "does not retry a %s completion even after a terminal overload error",
    (status) => {
      let state = acknowledge(makeCodexCapacityContinuation(binding), "turn-original").state;
      state = nativeError(state, "turn-original").state;
      expect(complete(state, "turn-original", status).actions).toEqual([
        { type: "terminal", status, nativeTurnId: "turn-original" },
      ]);
    },
  );

  it("does not infer capacity retry from a failed completion without the matching error", () => {
    const state = acknowledge(makeCodexCapacityContinuation(binding), "turn-original").state;
    const foreignError = nativeError(state, "other-turn");
    expect(foreignError.state).toBe(state);
    expect(complete(foreignError.state, "turn-original").actions).toEqual([
      { type: "terminal", status: "failed", nativeTurnId: "turn-original" },
    ]);
  });

  it("uses the latest matching error instead of retaining an obsolete overload", () => {
    let state = acknowledge(makeCodexCapacityContinuation(binding), "turn-original").state;
    state = nativeError(state, "turn-original").state;
    state = nativeError(state, "turn-original", "serverOverloaded", true).state;
    expect(complete(state, "turn-original").actions).toEqual([
      { type: "terminal", status: "failed", nativeTurnId: "turn-original" },
    ]);
  });

  it("correlates early error and completion only after the matching start reply", () => {
    const initial = makeCodexCapacityContinuation(binding);
    const earlyError = nativeError(initial, "turn-early");
    expect(earlyError.actions).toEqual([]);
    const earlyCompletion = complete(earlyError.state, "turn-early");
    expect(earlyCompletion.actions).toEqual([]);
    expect(earlyCompletion.state.phase).toBe("awaiting_start");
    expect(acknowledge(earlyCompletion.state, "turn-early").actions).toEqual([
      { type: "scheduleRetry", retryOrdinal: 1, delayMs: 10000 },
    ]);
    expect(initial.pendingNativeTurns).toEqual({});
    expect(initial.nativeTurnId).toBeNull();
  });

  it("retains matching pre-ACK facts when completion arrives before its error", () => {
    let state = complete(makeCodexCapacityContinuation(binding), "turn-early").state;
    state = nativeError(state, "turn-early").state;
    expect(acknowledge(state, "turn-early").actions).toEqual([
      { type: "scheduleRetry", retryOrdinal: 1, delayMs: 10000 },
    ]);
  });

  it("never combines the error and completion of different pre-ACK native turns", () => {
    let state = nativeError(makeCodexCapacityContinuation(binding), "turn-error").state;
    state = complete(state, "turn-completion").state;
    expect(acknowledge(state, "turn-completion").actions).toEqual([
      { type: "terminal", status: "failed", nativeTurnId: "turn-completion" },
    ]);
    const otherReply = acknowledge(state, "turn-error");
    expect(otherReply.actions).toEqual([]);
    expect(otherReply.state.phase).toBe("running");
  });

  it("ignores unmatched early completion when the actual reply identifies another turn", () => {
    const early = complete(makeCodexCapacityContinuation(binding), "obsolete-turn", "completed");
    const started = acknowledge(early.state, "actual-turn");
    expect(started.actions).toEqual([]);
    expect(started.state.nativeTurnId).toBe("actual-turn");
    expect(started.state.pendingNativeTurns).toEqual({});
    expect(complete(started.state, "obsolete-turn", "completed").actions).toEqual([]);
    expect(complete(started.state, "actual-turn", "completed").actions).toEqual([
      { type: "terminal", status: "completed", nativeTurnId: "actual-turn" },
    ]);
  });

  it("rejects obsolete native IDs and duplicate timer wakeups across a retry", () => {
    const waiting = waitingForRetry();
    const ready = reduceCodexCapacityContinuation(waiting, {
      type: "retryReady", binding, retryOrdinal: 1,
    });
    expect(ready.actions).toEqual([{ type: "retry", retryOrdinal: 1, promptless: true }]);
    expect(reduceCodexCapacityContinuation(ready.state, {
      type: "retryReady", binding, retryOrdinal: 1,
    }).actions).toEqual([]);
    expect(acknowledge(ready.state, "turn-original").state).toBe(ready.state);
    expect(nativeError(ready.state, "turn-original").state).toBe(ready.state);
    expect(complete(ready.state, "turn-original").state).toBe(ready.state);
    const next = acknowledge(ready.state, "turn-retry");
    expect(next.state.nativeTurnId).toBe("turn-retry");
    expect(complete(next.state, "turn-original").state).toBe(next.state);
    expect(complete(next.state, "turn-retry", "completed").actions).toEqual([
      { type: "terminal", status: "completed", nativeTurnId: "turn-retry" },
    ]);
  });

  it("rejects stale reply, unknown-start and timer tokens", () => {
    const waiting = waitingForRetry();
    expect(reduceCodexCapacityContinuation(waiting, {
      type: "retryReady", binding, retryOrdinal: 0,
    }).state).toBe(waiting);
    const current = reduceCodexCapacityContinuation(waiting, {
      type: "retryReady", binding, retryOrdinal: 1,
    }).state;
    for (const signal of [
      { type: "startAcknowledged", binding, retryOrdinal: 0, nativeTurnId: "late-reply" },
      { type: "startUnknown", binding, retryOrdinal: 0 },
    ] satisfies CodexCapacityContinuationSignal[]) {
      expect(reduceCodexCapacityContinuation(current, signal).state).toBe(current);
    }
    expect(acknowledge(current, "actual-retry").state.phase).toBe("running");
  });

  it.each(["runId", "attemptId", "providerThreadId", "nativeThreadId", "runtimeGeneration"] as const)(
    "rejects every stale %s correlation without cancelling the current request",
    (key) => {
      const state = makeCodexCapacityContinuation(binding);
      const staleBinding = { ...binding, [key]: `stale-${key}` };
      const signals: CodexCapacityContinuationSignal[] = [
        { type: "startAcknowledged", binding: staleBinding, retryOrdinal: 0, nativeTurnId: "turn" },
        { type: "nativeError", binding: staleBinding, nativeTurnId: "turn", code: "serverOverloaded", willRetry: false },
        { type: "nativeCompleted", binding: staleBinding, nativeTurnId: "turn", status: "failed" },
        { type: "retryReady", binding: staleBinding, retryOrdinal: 1 },
        { type: "cancel", binding: staleBinding, reason: "closed" },
        { type: "startUnknown", binding: staleBinding, retryOrdinal: 0 },
      ];
      for (const signal of signals) {
        const reduced = reduceCodexCapacityContinuation(state, signal);
        expect(reduced.state).toBe(state);
        expect(reduced.actions).toEqual([]);
      }
    },
  );

  it("holds a delayed dispatch when the actual runtime generation has changed", () => {
    const waiting = waitingForRetry();
    const staleWakeup = reduceCodexCapacityContinuation(waiting, {
      type: "retryReady",
      binding: { ...binding, runtimeGeneration: "replacement-generation" },
      retryOrdinal: 1,
    });
    expect(staleWakeup.actions).toEqual([]);
    expect(staleWakeup.state).toBe(waiting);
    const cancelled = reduceCodexCapacityContinuation(waiting, {
      type: "cancel", binding, reason: "runtime_changed",
    });
    expect(reduceCodexCapacityContinuation(cancelled.state, {
      type: "retryReady", binding, retryOrdinal: 1,
    }).actions).toEqual([]);
  });

  it.each(["stop", "superseded", "closed", "runtime_changed"] as const)(
    "prevents dispatch after %s during a retry delay and emits one logical disposition",
    (reason) => {
      const cancelled = reduceCodexCapacityContinuation(waitingForRetry(), {
        type: "cancel", binding, reason,
      });
      expect(cancelled.actions).toEqual([
        { type: "terminal", status: "interrupted", nativeTurnId: "turn-original", reason },
      ]);
      expect(reduceCodexCapacityContinuation(cancelled.state, {
        type: "retryReady", binding, retryOrdinal: 1,
      }).actions).toEqual([]);
      expect(reduceCodexCapacityContinuation(cancelled.state, {
        type: "cancel", binding, reason,
      }).actions).toEqual([]);
    },
  );

  it("prevents an early completion from reviving a superseded unacknowledged start", () => {
    const early = complete(nativeError(makeCodexCapacityContinuation(binding), "early-turn").state, "early-turn");
    const cancelled = reduceCodexCapacityContinuation(early.state, {
      type: "cancel", binding, reason: "superseded",
    });
    expect(cancelled.state.pendingNativeTurns).toEqual({});
    expect(acknowledge(cancelled.state, "early-turn").actions).toEqual([]);
    expect(cancelled.actions).toEqual([
      { type: "terminal", status: "interrupted", nativeTurnId: null, reason: "superseded" },
    ]);
  });

  it("keeps an unknown start terminal despite early overload evidence and late replies", () => {
    let state = nativeError(makeCodexCapacityContinuation(binding), "early-turn").state;
    state = complete(state, "early-turn").state;
    const unknown = reduceCodexCapacityContinuation(state, {
      type: "startUnknown", binding, retryOrdinal: 0,
    });
    expect(unknown.actions).toEqual([
      { type: "terminal", status: "unknown", nativeTurnId: null, reason: "start_unknown" },
    ]);
    expect(acknowledge(unknown.state, "early-turn").actions).toEqual([]);
    expect(reduceCodexCapacityContinuation(unknown.state, {
      type: "retryReady", binding, retryOrdinal: 1,
    }).actions).toEqual([]);
  });

  it("does not retry a replacement whose native start is unknown", () => {
    const ready = reduceCodexCapacityContinuation(waitingForRetry(), {
      type: "retryReady", binding, retryOrdinal: 1,
    });
    const unknown = reduceCodexCapacityContinuation(ready.state, {
      type: "startUnknown", binding, retryOrdinal: 1,
    });
    expect(unknown.actions).toEqual([
      { type: "terminal", status: "unknown", nativeTurnId: null, reason: "start_unknown" },
    ]);
    expect(acknowledge(unknown.state, "late-retry").actions).toEqual([]);
  });

  it("copies the binding and leaves prior states and early facts immutable", () => {
    const mutableBinding = { ...binding };
    const initial = makeCodexCapacityContinuation(mutableBinding);
    mutableBinding.runtimeGeneration = "replacement-generation";
    expect(initial.binding).toEqual(binding);
    expect(Object.isFrozen(initial.binding)).toBe(true);
    const early = nativeError(initial, "turn-early");
    const completed = complete(early.state, "turn-early");
    expect(initial.pendingNativeTurns).toEqual({});
    expect(early.state.pendingNativeTurns["turn-early"]).not.toHaveProperty("completion");
    expect(Object.isFrozen(completed.state)).toBe(true);
    expect(Object.isFrozen(completed.state.pendingNativeTurns)).toBe(true);
    expect(Object.isFrozen(completed.state.pendingNativeTurns["turn-early"])).toBe(true);
  });
});
