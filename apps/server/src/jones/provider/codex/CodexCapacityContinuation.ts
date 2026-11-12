/**
 * Jones hooks for `CodexAdapterV2.retryCapacityFailure`, the only capacity
 * retry chain: retry policy, classifier, native retry ownership, failover
 * exhaustion signal and the start owner that fences each native turn/start.
 */

export type CodexCapacityRetryProfile = "upstream" | "jones";

/**
 * Selects the capacity retry policy. "upstream" keeps T3 Code's 5 retries
 * with a 2 s delay doubling to 30 s. "jones" restores the earlier Jones
 * policy of 12 retries at a fixed 5 s. Switching this constant changes only
 * the count and delay; the lifecycle stays upstream's.
 */
export const CODEX_CAPACITY_RETRY_PROFILE: CodexCapacityRetryProfile = "upstream";

export interface CodexCapacityRetryPolicy {
  readonly maxRetries: number;
  /** Delay before the given 1-based retry attempt. */
  readonly delayMs: (attempt: number) => number;
}

export const codexCapacityRetryPolicies: Readonly<
  Record<CodexCapacityRetryProfile, CodexCapacityRetryPolicy>
> = Object.freeze({
  upstream: Object.freeze({
    maxRetries: 5,
    delayMs: (attempt: number) => Math.min(2_000 * 2 ** (attempt - 1), 30_000),
  }),
  jones: Object.freeze({ maxRetries: 12, delayMs: () => 5_000 }),
});

export const codexCapacityRetryPolicy: CodexCapacityRetryPolicy =
  codexCapacityRetryPolicies[CODEX_CAPACITY_RETRY_PROFILE];

export interface CodexCapacityErrorFacts {
  readonly code: string | null;
  readonly message?: string | undefined;
}

export interface CodexCapacityNativeError extends CodexCapacityErrorFacts {
  readonly willRetry: boolean;
}

const CODEX_CAPACITY_MESSAGE = "Selected model is at capacity. Please try a different model.";

/**
 * Codex reports overload as `serverOverloaded`, but some app-server builds
 * report the same condition with a null, `other` or `internalServerError`
 * code and only the exact capacity message.
 */
export function isCodexCapacityError(error: CodexCapacityErrorFacts): boolean {
  return (
    error.code === "serverOverloaded" ||
    ((error.code === null || error.code === "other" || error.code === "internalServerError") &&
      error.message?.trim() === CODEX_CAPACITY_MESSAGE)
  );
}

/**
 * True when the adapter, not Codex, must retry this overload. A native
 * `willRetry` keeps recovery with Codex; a completion error only fills missing
 * evidence or replaces an earlier overload with a different failure.
 */
export function codexCapacityFailureRetryable(input: {
  readonly nativeError?: CodexCapacityNativeError | undefined;
  readonly completionError?: CodexCapacityErrorFacts | undefined;
}): boolean {
  const error =
    input.completionError !== undefined &&
    (input.nativeError === undefined || !isCodexCapacityError(input.completionError))
      ? { ...input.completionError, willRetry: false }
      : input.nativeError;
  return error !== undefined && !error.willRetry && isCodexCapacityError(error);
}

/**
 * Per-turn facts the adapter keeps beside upstream's lifecycle. Keys are the
 * adapter's turn contexts, so entries disappear with the turn.
 */
export function makeCodexCapacityRetryFacts<Turn extends object>() {
  const nativeErrors = new WeakMap<Turn, CodexCapacityNativeError>();
  const exhausted = new WeakSet<Turn>();
  return {
    observeNativeError: (turn: Turn, error: CodexCapacityNativeError) => {
      nativeErrors.set(turn, Object.freeze({ ...error }));
    },
    retryable: (turn: Turn, completionError: CodexCapacityErrorFacts | undefined) =>
      codexCapacityFailureRetryable({ nativeError: nativeErrors.get(turn), completionError }),
    /** Marks a retryable overload that reached the retry limit. */
    markExhausted: (turn: Turn) => {
      exhausted.add(turn);
    },
    /** Returns the exhaustion mark once, so failover is raised at most once per turn. */
    takeExhausted: (turn: Turn) => exhausted.delete(turn),
  };
}

/** Start-owner state for one native turn/start dispatch. */
export interface CodexCapacityContinuationBinding {
  readonly runId: string;
  readonly attemptId: string;
  readonly providerThreadId: string;
  readonly nativeThreadId: string;
  readonly runtimeGeneration: string;
}

export type CodexCapacityCancellationReason = "stop" | "superseded" | "closed" | "runtime_changed";

export interface CodexCapacityContinuationState {
  readonly binding: CodexCapacityContinuationBinding;
  readonly phase: "awaiting_start" | "running" | "terminal";
  /** 0 for the prompt; the retry attempt for a promptless continuation. */
  readonly retryOrdinal: number;
  readonly nativeTurnId: string | null;
  /** Native turns of earlier attempts. Their replies and notifications never bind this start. */
  readonly retiredNativeTurnIds: ReadonlyArray<string>;
  readonly terminalReason?: CodexCapacityCancellationReason | "start_unknown";
}

export type CodexCapacityContinuationSignal = {
  readonly binding: CodexCapacityContinuationBinding;
} & (
  | {
      readonly type: "startAcknowledged";
      readonly retryOrdinal: number;
      readonly nativeTurnId: string;
    }
  | { readonly type: "cancel"; readonly reason: CodexCapacityCancellationReason }
  | { readonly type: "startUnknown"; readonly retryOrdinal: number }
);

export function makeCodexCapacityContinuation(
  binding: CodexCapacityContinuationBinding,
  continuation?: {
    readonly retryOrdinal: number;
    readonly retiredNativeTurnIds: ReadonlyArray<string>;
  },
): CodexCapacityContinuationState {
  return Object.freeze<CodexCapacityContinuationState>({
    binding: Object.freeze({ ...binding }),
    phase: "awaiting_start",
    retryOrdinal: continuation?.retryOrdinal ?? 0,
    nativeTurnId: null,
    retiredNativeTurnIds: Object.freeze([...(continuation?.retiredNativeTurnIds ?? [])]),
  });
}

function matchesBinding(
  expected: CodexCapacityContinuationBinding,
  actual: CodexCapacityContinuationBinding,
): boolean {
  return (
    expected.runId === actual.runId &&
    expected.attemptId === actual.attemptId &&
    expected.providerThreadId === actual.providerThreadId &&
    expected.nativeThreadId === actual.nativeThreadId &&
    expected.runtimeGeneration === actual.runtimeGeneration
  );
}

// Wakeups carry the freshly checked current binding. Replaying the request's
// frozen binding alone cannot establish that its delayed target is still current.
export function reduceCodexCapacityContinuation(
  state: CodexCapacityContinuationState,
  signal: CodexCapacityContinuationSignal,
): CodexCapacityContinuationState {
  if (!matchesBinding(state.binding, signal.binding) || state.phase === "terminal") return state;
  switch (signal.type) {
    case "cancel":
      return Object.freeze<CodexCapacityContinuationState>({
        ...state,
        phase: "terminal",
        terminalReason: signal.reason,
      });
    case "startUnknown":
      return state.phase === "awaiting_start" && signal.retryOrdinal === state.retryOrdinal
        ? Object.freeze<CodexCapacityContinuationState>({
            ...state,
            phase: "terminal",
            terminalReason: "start_unknown",
          })
        : state;
    case "startAcknowledged":
      if (
        state.phase !== "awaiting_start" ||
        signal.retryOrdinal !== state.retryOrdinal ||
        state.retiredNativeTurnIds.includes(signal.nativeTurnId)
      )
        return state;
      return Object.freeze<CodexCapacityContinuationState>({
        ...state,
        phase: "running",
        nativeTurnId: signal.nativeTurnId,
      });
  }
}
