export interface CodexCapacityContinuationBinding {
  readonly runId: string;
  readonly attemptId: string;
  readonly providerThreadId: string;
  readonly nativeThreadId: string;
  readonly runtimeGeneration: string;
}

export type CodexCapacityTerminalStatus = "completed" | "failed" | "interrupted";
export type CodexCapacityCancellationReason = "stop" | "superseded" | "closed" | "runtime_changed";

interface CodexCapacityNativeFacts {
  readonly error?: {
    readonly code: string | null;
    readonly willRetry: boolean;
  };
  readonly completion?: CodexCapacityTerminalStatus;
}

export interface CodexCapacityContinuationState {
  readonly binding: CodexCapacityContinuationBinding;
  readonly phase: "awaiting_start" | "running" | "waiting_retry" | "terminal";
  readonly retryOrdinal: number;
  readonly nativeTurnId: string | null;
  readonly nativeFacts: CodexCapacityNativeFacts;
  readonly pendingNativeTurns: Readonly<Record<string, CodexCapacityNativeFacts>>;
  readonly retiredNativeTurnIds: ReadonlyArray<string>;
}

export type CodexCapacityContinuationSignal = {
  readonly binding: CodexCapacityContinuationBinding;
} & (
  | {
      readonly type: "startAcknowledged";
      readonly retryOrdinal: number;
      readonly nativeTurnId: string;
    }
  | {
      readonly type: "nativeError";
      readonly nativeTurnId: string;
      readonly code: string | null;
      readonly willRetry: boolean;
    }
  | {
      readonly type: "nativeCompleted";
      readonly nativeTurnId: string;
      readonly status: CodexCapacityTerminalStatus;
    }
  | { readonly type: "retryReady"; readonly retryOrdinal: number }
  | { readonly type: "cancel"; readonly reason: CodexCapacityCancellationReason }
  | { readonly type: "startUnknown"; readonly retryOrdinal: number }
);

// These actions describe the logical request. Cancellation and unknown starts
// do not confirm native interruption; the adapter owns native operations.
export type CodexCapacityContinuationAction =
  | { readonly type: "scheduleRetry"; readonly retryOrdinal: number; readonly delayMs: 10000 }
  | { readonly type: "retry"; readonly retryOrdinal: number; readonly promptless: true }
  | {
      readonly type: "terminal";
      readonly status: CodexCapacityTerminalStatus | "unknown";
      readonly nativeTurnId: string | null;
      readonly reason?: CodexCapacityCancellationReason | "start_unknown";
    };

export interface CodexCapacityContinuationReduction {
  readonly state: CodexCapacityContinuationState;
  readonly actions: ReadonlyArray<CodexCapacityContinuationAction>;
}

const emptyFacts: CodexCapacityNativeFacts = Object.freeze({});
const emptyPending: Readonly<Record<string, CodexCapacityNativeFacts>> = Object.freeze({});
const noActions: ReadonlyArray<CodexCapacityContinuationAction> = Object.freeze([]);
const maxRetries = 5;

// The adapter retains the effective settings for this logical request; retries
// only request another promptless send and never resolve settings themselves.
export function makeCodexCapacityContinuation(
  binding: CodexCapacityContinuationBinding,
): CodexCapacityContinuationState {
  return Object.freeze<CodexCapacityContinuationState>({
    binding: Object.freeze({ ...binding }),
    phase: "awaiting_start",
    retryOrdinal: 0,
    nativeTurnId: null,
    nativeFacts: emptyFacts,
    pendingNativeTurns: emptyPending,
    retiredNativeTurnIds: Object.freeze([]),
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

function unchanged(state: CodexCapacityContinuationState): CodexCapacityContinuationReduction {
  return { state, actions: noActions };
}

function terminal(
  state: CodexCapacityContinuationState,
  status: CodexCapacityTerminalStatus | "unknown",
  reason?: CodexCapacityCancellationReason | "start_unknown",
): CodexCapacityContinuationReduction {
  return {
    state: Object.freeze<CodexCapacityContinuationState>({
      ...state,
      phase: "terminal",
      pendingNativeTurns: emptyPending,
    }),
    actions: Object.freeze([
      Object.freeze<CodexCapacityContinuationAction>({
        type: "terminal",
        status,
        nativeTurnId: state.nativeTurnId,
        ...(reason === undefined ? {} : { reason }),
      }),
    ]),
  };
}

function reduceCompletion(
  state: CodexCapacityContinuationState,
): CodexCapacityContinuationReduction {
  const status = state.nativeFacts.completion;
  if (status === undefined) return unchanged(state);
  if (
    status === "failed" &&
    state.nativeFacts.error?.code === "serverOverloaded" &&
    state.nativeFacts.error.willRetry === false &&
    state.retryOrdinal < maxRetries
  ) {
    const retryOrdinal = state.retryOrdinal + 1;
    return {
      state: Object.freeze<CodexCapacityContinuationState>({
        ...state,
        phase: "waiting_retry",
        retryOrdinal,
        retiredNativeTurnIds: Object.freeze([...state.retiredNativeTurnIds, state.nativeTurnId!]),
      }),
      actions: Object.freeze([
        Object.freeze<CodexCapacityContinuationAction>({
          type: "scheduleRetry",
          retryOrdinal,
          delayMs: 10000,
        }),
      ]),
    };
  }
  return terminal(state, status);
}

function updateFacts(
  facts: CodexCapacityNativeFacts,
  signal: Extract<CodexCapacityContinuationSignal, { type: "nativeError" | "nativeCompleted" }>,
): CodexCapacityNativeFacts {
  return signal.type === "nativeError"
    ? Object.freeze({
        ...facts,
        error: Object.freeze({ code: signal.code, willRetry: signal.willRetry }),
      })
    : Object.freeze({ ...facts, completion: facts.completion ?? signal.status });
}

// Wakeups carry the freshly checked current binding. Replaying the request's
// frozen binding alone cannot establish that its delayed target is still current.
export function reduceCodexCapacityContinuation(
  state: CodexCapacityContinuationState,
  signal: CodexCapacityContinuationSignal,
): CodexCapacityContinuationReduction {
  if (!matchesBinding(state.binding, signal.binding) || state.phase === "terminal") {
    return unchanged(state);
  }
  switch (signal.type) {
    case "cancel":
      return terminal(state, "interrupted", signal.reason);
    case "startUnknown":
      return state.phase === "awaiting_start" && signal.retryOrdinal === state.retryOrdinal
        ? terminal(state, "unknown", "start_unknown")
        : unchanged(state);
    case "retryReady":
      if (state.phase !== "waiting_retry" || signal.retryOrdinal !== state.retryOrdinal) {
        return unchanged(state);
      }
      return {
        state: Object.freeze<CodexCapacityContinuationState>({
          ...state,
          phase: "awaiting_start",
          nativeTurnId: null,
          nativeFacts: emptyFacts,
          pendingNativeTurns: emptyPending,
        }),
        actions: Object.freeze([
          Object.freeze<CodexCapacityContinuationAction>({
            type: "retry",
            retryOrdinal: state.retryOrdinal,
            promptless: true,
          }),
        ]),
      };
    case "startAcknowledged": {
      if (
        state.phase !== "awaiting_start" ||
        signal.retryOrdinal !== state.retryOrdinal ||
        state.retiredNativeTurnIds.includes(signal.nativeTurnId)
      ) {
        return unchanged(state);
      }
      const nativeFacts = Object.hasOwn(state.pendingNativeTurns, signal.nativeTurnId)
        ? state.pendingNativeTurns[signal.nativeTurnId]!
        : emptyFacts;
      return reduceCompletion(
        Object.freeze<CodexCapacityContinuationState>({
          ...state,
          phase: "running",
          nativeTurnId: signal.nativeTurnId,
          nativeFacts,
          pendingNativeTurns: emptyPending,
        }),
      );
    }
    case "nativeError":
    case "nativeCompleted": {
      if (
        state.phase === "waiting_retry" ||
        state.retiredNativeTurnIds.includes(signal.nativeTurnId)
      ) {
        return unchanged(state);
      }
      if (state.phase === "awaiting_start") {
        // Early notifications are facts, not a start acknowledgement. Only the
        // matching reply selects which native turn may advance this request.
        const previous = Object.hasOwn(state.pendingNativeTurns, signal.nativeTurnId)
          ? state.pendingNativeTurns[signal.nativeTurnId]!
          : emptyFacts;
        return {
          state: Object.freeze<CodexCapacityContinuationState>({
            ...state,
            pendingNativeTurns: Object.freeze({
              ...state.pendingNativeTurns,
              [signal.nativeTurnId]: updateFacts(previous, signal),
            }),
          }),
          actions: noActions,
        };
      }
      if (signal.nativeTurnId !== state.nativeTurnId) return unchanged(state);
      const next = Object.freeze({ ...state, nativeFacts: updateFacts(state.nativeFacts, signal) });
      return signal.type === "nativeCompleted" ? reduceCompletion(next) : unchanged(next);
    }
  }
}
