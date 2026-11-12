import { describe, expect, it } from "vite-plus/test";

import {
  CODEX_CAPACITY_RETRY_PROFILE,
  codexCapacityFailureRetryable,
  codexCapacityRetryPolicies,
  codexCapacityRetryPolicy,
  isCodexCapacityError,
  makeCodexCapacityContinuation,
  makeCodexCapacityRetryFacts,
  reduceCodexCapacityContinuation,
  type CodexCapacityContinuationBinding,
  type CodexCapacityContinuationSignal,
  type CodexCapacityContinuationState,
} from "./CodexCapacityContinuation.ts";

const CAPACITY_MESSAGE = "Selected model is at capacity. Please try a different model.";

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

describe("Codex capacity retry policy", () => {
  it("defaults to upstream's five retries with a 2 s delay doubling to 30 s", () => {
    expect(CODEX_CAPACITY_RETRY_PROFILE).toBe("upstream");
    expect(codexCapacityRetryPolicy).toBe(codexCapacityRetryPolicies.upstream);
    expect(codexCapacityRetryPolicy.maxRetries).toBe(5);
    expect([1, 2, 3, 4, 5, 6, 7].map(codexCapacityRetryPolicy.delayMs)).toEqual([
      2_000, 4_000, 8_000, 16_000, 30_000, 30_000, 30_000,
    ]);
  });

  it("keeps the earlier Jones policy selectable as twelve retries at a fixed 5 s", () => {
    const jones = codexCapacityRetryPolicies.jones;
    expect(jones.maxRetries).toBe(12);
    expect([1, 6, 12].map(jones.delayMs)).toEqual([5_000, 5_000, 5_000]);
  });
});

describe("Codex capacity classification", () => {
  it.each([null, "other", "internalServerError"])(
    "retries the exact model capacity error with generic code %s",
    (code) => {
      expect(isCodexCapacityError({ code, message: CAPACITY_MESSAGE })).toBe(true);
      expect(
        codexCapacityFailureRetryable({
          nativeError: { code, message: CAPACITY_MESSAGE, willRetry: false },
        }),
      ).toBe(true);
      expect(
        codexCapacityFailureRetryable({ completionError: { code, message: CAPACITY_MESSAGE } }),
      ).toBe(true);
    },
  );

  it("retries serverOverloaded regardless of its message", () => {
    expect(codexCapacityFailureRetryable({ completionError: { code: "serverOverloaded" } })).toBe(
      true,
    );
  });

  it.each(["usageLimitExceeded", "rateLimitExceeded", "unauthorized", "badRequest", "unknown"])(
    "does not override the explicit %s classification with capacity text",
    (code) => {
      expect(
        codexCapacityFailureRetryable({
          nativeError: { code, message: CAPACITY_MESSAGE, willRetry: false },
          completionError: { code, message: CAPACITY_MESSAGE },
        }),
      ).toBe(false);
    },
  );

  it.each(["Selected model is unavailable", "The provider is at capacity", "Unknown error"])(
    "does not infer capacity from generic text: %s",
    (message) => {
      expect(
        codexCapacityFailureRetryable({
          nativeError: { code: "other", message, willRetry: false },
          completionError: { code: "other", message },
        }),
      ).toBe(false);
    },
  );

  it("preserves native retry ownership when completion repeats capacity evidence", () => {
    expect(
      codexCapacityFailureRetryable({
        nativeError: { code: "serverOverloaded", willRetry: true },
        completionError: { code: "serverOverloaded" },
      }),
    ).toBe(false);
  });

  it("uses a noncapacity completion error instead of an earlier overload", () => {
    expect(
      codexCapacityFailureRetryable({
        nativeError: { code: "serverOverloaded", willRetry: false },
        completionError: { code: "usageLimitExceeded", message: "Quota exhausted" },
      }),
    ).toBe(false);
  });

  it("does not infer a capacity retry from a failure without error evidence", () => {
    expect(codexCapacityFailureRetryable({})).toBe(false);
  });

  it("uses the latest native error per turn instead of an obsolete overload", () => {
    const facts = makeCodexCapacityRetryFacts<object>();
    const turn = {};
    const other = {};
    facts.observeNativeError(turn, { code: "serverOverloaded", willRetry: false });
    expect(facts.retryable(turn, { code: "serverOverloaded" })).toBe(true);
    facts.observeNativeError(turn, { code: "serverOverloaded", willRetry: true });
    expect(facts.retryable(turn, { code: "serverOverloaded" })).toBe(false);
    // Another turn's notification is not evidence for this one.
    facts.observeNativeError(other, { code: "serverOverloaded", willRetry: true });
    const fresh = {};
    expect(facts.retryable(fresh, { code: "serverOverloaded" })).toBe(true);
  });

  it("reports one exhaustion per turn so failover is raised at most once", () => {
    const facts = makeCodexCapacityRetryFacts<object>();
    const turn = {};
    expect(facts.takeExhausted(turn)).toBe(false);
    facts.markExhausted(turn);
    facts.markExhausted(turn);
    expect(facts.takeExhausted(turn)).toBe(true);
    expect(facts.takeExhausted(turn)).toBe(false);
    expect(facts.takeExhausted({})).toBe(false);
  });
});

describe("Codex capacity start owner", () => {
  it("binds exactly one acknowledged native turn per start", () => {
    const started = acknowledge(makeCodexCapacityContinuation(binding), "turn-original");
    expect(started.phase).toBe("running");
    expect(started.nativeTurnId).toBe("turn-original");
    expect(acknowledge(started, "turn-other")).toBe(started);
  });

  it("starts a promptless continuation that never binds retired turns or stale replies", () => {
    const continuation = makeCodexCapacityContinuation(binding, {
      retryOrdinal: 2,
      retiredNativeTurnIds: ["turn-0", "turn-1"],
    });
    expect(continuation.phase).toBe("awaiting_start");
    expect(continuation.retryOrdinal).toBe(2);
    expect(acknowledge(continuation, "turn-1")).toBe(continuation);
    for (const signal of [
      { type: "startAcknowledged", binding, retryOrdinal: 1, nativeTurnId: "late-reply" },
      { type: "startUnknown", binding, retryOrdinal: 1 },
    ] satisfies CodexCapacityContinuationSignal[]) {
      expect(reduceCodexCapacityContinuation(continuation, signal)).toBe(continuation);
    }
    expect(acknowledge(continuation, "turn-2").phase).toBe("running");
  });

  it.each([
    "runId",
    "attemptId",
    "providerThreadId",
    "nativeThreadId",
    "runtimeGeneration",
  ] as const)("rejects every stale %s correlation without changing the request", (key) => {
    const state = makeCodexCapacityContinuation(binding);
    const staleBinding = { ...binding, [key]: `stale-${key}` };
    const signals: CodexCapacityContinuationSignal[] = [
      { type: "startAcknowledged", binding: staleBinding, retryOrdinal: 0, nativeTurnId: "turn" },
      { type: "cancel", binding: staleBinding, reason: "closed" },
      { type: "startUnknown", binding: staleBinding, retryOrdinal: 0 },
    ];
    for (const signal of signals) {
      expect(reduceCodexCapacityContinuation(state, signal)).toBe(state);
    }
  });

  it.each(["stop", "superseded", "closed", "runtime_changed"] as const)(
    "records the %s cancellation reason once and ignores late replies",
    (reason) => {
      const cancelled = reduceCodexCapacityContinuation(makeCodexCapacityContinuation(binding), {
        type: "cancel",
        binding,
        reason,
      });
      expect(cancelled.phase).toBe("terminal");
      expect(cancelled.terminalReason).toBe(reason);
      expect(acknowledge(cancelled, "late-turn")).toBe(cancelled);
      expect(
        reduceCodexCapacityContinuation(cancelled, { type: "cancel", binding, reason: "stop" }),
      ).toBe(cancelled);
    },
  );

  it("keeps an unknown start terminal so it is never replayed", () => {
    const unknown = reduceCodexCapacityContinuation(
      makeCodexCapacityContinuation(binding, { retryOrdinal: 1, retiredNativeTurnIds: ["t0"] }),
      { type: "startUnknown", binding, retryOrdinal: 1 },
    );
    expect(unknown.phase).toBe("terminal");
    expect(unknown.terminalReason).toBe("start_unknown");
    expect(acknowledge(unknown, "late-retry")).toBe(unknown);
  });

  it("does not mark an acknowledged start unknown", () => {
    const started = acknowledge(makeCodexCapacityContinuation(binding), "turn");
    expect(
      reduceCodexCapacityContinuation(started, { type: "startUnknown", binding, retryOrdinal: 0 }),
    ).toBe(started);
  });

  it("copies the binding and leaves prior states immutable", () => {
    const mutableBinding = { ...binding };
    const retired = ["turn-0"];
    const initial = makeCodexCapacityContinuation(mutableBinding, {
      retryOrdinal: 1,
      retiredNativeTurnIds: retired,
    });
    mutableBinding.runtimeGeneration = "replacement-generation";
    retired.push("turn-1");
    expect(initial.binding).toEqual(binding);
    expect(initial.retiredNativeTurnIds).toEqual(["turn-0"]);
    expect(Object.isFrozen(initial)).toBe(true);
    expect(Object.isFrozen(initial.binding)).toBe(true);
    expect(Object.isFrozen(initial.retiredNativeTurnIds)).toBe(true);
    const started = acknowledge(initial, "turn-1b");
    expect(initial.phase).toBe("awaiting_start");
    expect(Object.isFrozen(started)).toBe(true);
  });
});
