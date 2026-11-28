import type { CodexAppServerClientOptions } from "effect-codex-app-server/client";
import type * as CodexErrors from "effect-codex-app-server/errors";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import type * as DateTime from "effect/DateTime";
import * as Predicate from "effect/Predicate";
import { isNonApplyingStartupRejection } from "./CodexStartup.ts";

// A slow live producer can acknowledge the original start well after 30 seconds.
// Bound a wedged wait without treating the deadline as proof of native cancellation.
export const CODEX_START_ACK_TIMEOUT_MS = 10 * 60 * 1_000;
const INVALID_PARAMS = -32602;
const RECENT_ACKNOWLEDGED_TURN_LIMIT = 128;
const RELEASED_START_LIMIT = 128;

// Observe frames on the reader before it wakes a response waiter or forks a request handler.
export function codexStartReplyObservationOptions(
  observeFrame: ((frame: unknown) => void) | undefined,
  protocolLogger?: CodexAppServerClientOptions["logger"],
): CodexAppServerClientOptions {
  if (observeFrame === undefined && protocolLogger === undefined) return {};
  return {
    logIncoming: true,
    logOutgoing: protocolLogger !== undefined,
    logger: (event) => {
      if (event.direction === "incoming" && event.stage === "decoded")
        observeFrame?.(event.payload);
      return protocolLogger?.(event) ?? Effect.void;
    },
  };
}

interface NativeBinding {
  readonly runtimeGeneration: string;
  readonly nativeThreadId: string;
}

export interface CodexStartTerminal {
  readonly nativeTurnId: string;
  readonly status: "completed" | "failed" | "interrupted";
  readonly startedAt: DateTime.Utc;
  readonly completedAt: DateTime.Utc;
  readonly inputObserved?: true;
}

export const CodexPendingStartStop = Context.Reference<Effect.Effect<boolean>>(
  "jones/CodexPendingStartStop",
  { defaultValue: () => Effect.succeed(false) },
);

export const CodexQueuedStartStop = Context.Reference<Effect.Effect<boolean> | undefined>(
  "jones/CodexQueuedStartStop",
  { defaultValue: () => undefined },
);

export interface CodexStartDispatch extends NativeBinding {
  readonly ordinal: number;
  readonly excludedTurnIds: ReadonlySet<string>;
  readonly startedTurnIds: Set<string>;
  readonly completions: Map<string, CodexStartTerminal | undefined>;
  readonly onLateTerminal?: (terminal: CodexStartTerminal) => void;
  onStartOrTerminal?: () => void;
  onRetired?: () => void;
  nativeTurnId: string | undefined;
  turnActivityObserved: boolean;
  observedTurnId?: string;
  observedInputTurnId?: string;
  conflictingTurnActivity?: true;
  retainForStop?: true;
  unknown: boolean;
  overflow: boolean;
}

const matchesNativeBinding = (left: NativeBinding, right: NativeBinding) =>
  left.runtimeGeneration === right.runtimeGeneration &&
  left.nativeThreadId === right.nativeThreadId;

export interface CodexStartRejectionProof {
  readonly kind: "not_applied";
  readonly canReconcileLate: boolean;
  readonly isCurrent: () => boolean;
  readonly release: () => boolean;
}

type StartRejection = { readonly kind: "other_error" | "unverified" } | CodexStartRejectionProof;

interface MaintenanceActivity {
  readonly blocked: boolean;
  readonly observeActivity: (
    binding: NativeBinding,
    nativeTurnId: string,
    method: string,
    payload: object,
  ) => boolean;
}

// The caller's deadline leaves the native effect unresolved. The original RPC
// can reconcile it with an exact non-applying rejection, or with an ACK and the
// acknowledged turn's terminal.
export function makeCodexLateStartRecovery<AdditionalContext = never>(
  maintenance?: MaintenanceActivity,
) {
  const dispatches = new Set<CodexStartDispatch>();
  const releasedStarts = new Map<
    string,
    NativeBinding & { readonly excludedTurnIds: ReadonlySet<string>; conflicted: boolean }
  >();
  const acknowledgedTurns = new Map<string, NativeBinding>();
  const rejectionReplies = new WeakMap<object, { runtimeGeneration: string; requestId: string }>();
  const acceptedContexts = new Map<
    string,
    { readonly ordinal: number; readonly context: AdditionalContext | undefined }
  >();
  let nextOrdinal = 0;
  const settled = (dispatch: CodexStartDispatch) =>
    !dispatch.overflow &&
    dispatch.nativeTurnId !== undefined &&
    dispatch.completions.has(dispatch.nativeTurnId);
  const lateTerminal = (dispatch: CodexStartDispatch) => {
    if (
      !dispatches.has(dispatch) ||
      !dispatch.unknown ||
      !settled(dispatch) ||
      dispatch.conflictingTurnActivity ||
      (dispatch.observedTurnId !== undefined && dispatch.observedTurnId !== dispatch.nativeTurnId)
    )
      return undefined;
    const terminal = dispatch.completions.get(dispatch.nativeTurnId!);
    return terminal?.status === "interrupted" &&
      terminal.inputObserved !== true &&
      dispatch.observedInputTurnId !== terminal.nativeTurnId
      ? undefined
      : terminal;
  };
  const releaseSettled = (dispatch: CodexStartDispatch) => {
    if (!dispatch.unknown || !settled(dispatch)) return;
    if (dispatch.onLateTerminal === undefined) {
      dispatches.delete(dispatch);
      return;
    }
    const terminal = lateTerminal(dispatch);
    if (terminal !== undefined) dispatch.onLateTerminal(terminal);
  };
  const releasedStart = (binding: NativeBinding) => {
    const record = releasedStarts.get(binding.nativeThreadId);
    return record !== undefined && matchesNativeBinding(record, binding) ? record : undefined;
  };
  const releaseWithFence = (dispatch: CodexStartDispatch) => {
    for (const [id, record] of releasedStarts) {
      if (record.runtimeGeneration !== dispatch.runtimeGeneration) releasedStarts.delete(id);
    }
    // Keep observing the binding until the next prompt is admitted to the queue.
    if (releasedStarts.size >= RELEASED_START_LIMIT || releasedStart(dispatch)?.conflicted)
      return false;
    const excludedTurnIds = new Set(
      [...dispatch.excludedTurnIds].slice(-RECENT_ACKNOWLEDGED_TURN_LIMIT),
    );
    if (dispatch.nativeTurnId !== undefined) excludedTurnIds.add(dispatch.nativeTurnId);
    releasedStarts.set(dispatch.nativeThreadId, {
      runtimeGeneration: dispatch.runtimeGeneration,
      nativeThreadId: dispatch.nativeThreadId,
      excludedTurnIds,
      conflicted: false,
    });
    return dispatches.delete(dispatch);
  };
  const observing = () => dispatches.size > 0 || releasedStarts.size > 0 || maintenance?.blocked;
  const observeTurnActivity = (runtimeGeneration: string, payload: unknown, method?: string) => {
    if (!observing() || !Predicate.isObject(payload)) return;
    const nativeThreadId = payload.threadId;
    const nativeTurnId =
      typeof payload.turnId === "string"
        ? payload.turnId
        : Predicate.isObject(payload.turn)
          ? payload.turn.id
          : undefined;
    if (typeof nativeThreadId !== "string" || typeof nativeTurnId !== "string" || !nativeTurnId)
      return;
    const binding = { nativeThreadId, runtimeGeneration };
    const released = releasedStart(binding);
    const knownOwner = acknowledgedTurns.get(nativeTurnId);
    const knownTurn = knownOwner !== undefined && matchesNativeBinding(knownOwner, binding);
    if (
      !knownTurn &&
      !released?.excludedTurnIds.has(nativeTurnId) &&
      method !== undefined &&
      maintenance?.observeActivity(binding, nativeTurnId, method, payload)
    )
      return;
    if (released !== undefined && !released.excludedTurnIds.has(nativeTurnId) && !knownTurn)
      released.conflicted = true;
    for (const dispatch of dispatches) {
      if (!matchesNativeBinding(dispatch, binding) || dispatch.excludedTurnIds.has(nativeTurnId))
        continue;
      dispatch.turnActivityObserved = true;
      if (dispatch.onLateTerminal !== undefined) {
        dispatch.observedTurnId ??= nativeTurnId;
        if (dispatch.observedTurnId !== nativeTurnId) dispatch.conflictingTurnActivity = true;
      }
    }
  };
  const rememberTurn = (binding: NativeBinding, nativeTurnId: string) => {
    acknowledgedTurns.set(nativeTurnId, binding);
    if (acknowledgedTurns.size > RECENT_ACKNOWLEDGED_TURN_LIMIT) {
      const oldest = acknowledgedTurns.keys().next();
      if (!oldest.done) acknowledgedTurns.delete(oldest.value);
    }
  };
  return {
    begin(
      binding: NativeBinding,
      excludedTurnIds: ReadonlyArray<string>,
      onLateTerminal?: CodexStartDispatch["onLateTerminal"],
    ): CodexStartDispatch {
      // Terminal turns can already be gone from the adapter's active and settled maps.
      const knownTurns = new Set(excludedTurnIds);
      for (const [turnId, owner] of acknowledgedTurns) {
        if (matchesNativeBinding(owner, binding)) knownTurns.add(turnId);
      }
      const dispatch: CodexStartDispatch = {
        ...binding,
        ordinal: ++nextOrdinal,
        excludedTurnIds: knownTurns,
        startedTurnIds: new Set(),
        completions: new Map(),
        ...(onLateTerminal === undefined ? {} : { onLateTerminal }),
        nativeTurnId: undefined,
        turnActivityObserved: false,
        unknown: false,
        overflow: false,
      };
      dispatches.add(dispatch);
      return dispatch;
    },
    blocksStart(binding: NativeBinding) {
      return releasedStart(binding)?.conflicted === true;
    },
    consumeReleasedStart(binding: NativeBinding) {
      const released = releasedStart(binding);
      if (released !== undefined) releasedStarts.delete(binding.nativeThreadId);
    },
    ownsDispatch(dispatch: CodexStartDispatch) {
      return dispatches.has(dispatch);
    },
    markUnknown(dispatch: CodexStartDispatch, options?: { readonly retainForStop: true }) {
      if (!dispatches.has(dispatch)) return;
      if (options?.retainForStop) dispatch.retainForStop = true;
      dispatch.unknown = true;
      releaseSettled(dispatch);
    },
    hasPendingStop(runtimeGeneration: string, nativeThreadId?: string) {
      for (const dispatch of dispatches) {
        if (
          dispatch.retainForStop &&
          dispatch.runtimeGeneration === runtimeGeneration &&
          (nativeThreadId === undefined || dispatch.nativeThreadId === nativeThreadId) &&
          (dispatch.nativeTurnId === undefined || !dispatch.completions.has(dispatch.nativeTurnId))
        )
          return true;
      }
      return false;
    },
    retireThread(binding: NativeBinding) {
      // Verified native closure ends live custody without proving prompt delivery.
      for (const dispatch of dispatches) {
        if (!matchesNativeBinding(dispatch, binding)) continue;
        dispatches.delete(dispatch);
        dispatch.onStartOrTerminal?.();
        dispatch.onRetired?.();
      }
      if (releasedStart(binding) !== undefined) releasedStarts.delete(binding.nativeThreadId);
    },
    observeFrame(runtimeGeneration: string, frame: unknown) {
      if (!observing() || !Predicate.isObject(frame)) return;
      if (
        typeof frame.method === "string" &&
        (frame.id === undefined ||
          typeof frame.id === "string" ||
          Number.isSafeInteger(frame.id)) &&
        !("result" in frame) &&
        !("error" in frame)
      ) {
        // Record activity before handlers can yield or be forked off the reader.
        observeTurnActivity(runtimeGeneration, frame.params, frame.method);
        return;
      }
      // The envelope schema permits both result and error. The client sends
      // integer IDs and Codex stdio may omit the JSON-RPC version.
      if (
        typeof frame.id !== "number" ||
        !Number.isSafeInteger(frame.id) ||
        frame.id < 1 ||
        ("jsonrpc" in frame && frame.jsonrpc !== "2.0") ||
        !Predicate.isObject(frame.error) ||
        (frame.error.code !== -32600 &&
          frame.error.code !== -32601 &&
          frame.error.code !== INVALID_PARAMS) ||
        typeof frame.error.message !== "string" ||
        Object.keys(frame).some((key) => key !== "id" && key !== "error" && key !== "jsonrpc") ||
        Object.keys(frame.error).some(
          (key) => key !== "code" && key !== "message" && key !== "data",
        )
      )
        return;
      rejectionReplies.set(frame.error, { runtimeGeneration, requestId: String(frame.id) });
    },
    reject(
      dispatch: CodexStartDispatch,
      currentGeneration: () => string | undefined,
      error: CodexErrors.CodexAppServerError,
    ): StartRejection {
      if (
        error._tag !== "CodexAppServerRequestError" ||
        !isNonApplyingStartupRejection(error, "turn/start")
      )
        return { kind: "other_error" };
      if (!Predicate.isObject(error.cause)) return { kind: "unverified" };
      // The original waiter binds the request method and ID to this dispatch.
      // Object identity ties that failure to the exact envelope on the wire.
      const reply = rejectionReplies.get(error.cause);
      const qualifies = () =>
        dispatches.has(dispatch) &&
        dispatch.runtimeGeneration === currentGeneration() &&
        dispatch.nativeTurnId === undefined &&
        !dispatch.turnActivityObserved &&
        dispatch.completions.size === 0 &&
        !dispatch.overflow;
      if (
        reply?.runtimeGeneration !== dispatch.runtimeGeneration ||
        reply.requestId !== error.requestId ||
        !qualifies()
      )
        return { kind: "unverified" };
      rejectionReplies.delete(error.cause);
      const canReconcileLate = error.code === INVALID_PARAMS;
      const isCurrent = () => (canReconcileLate || !dispatch.unknown) && qualifies();
      return {
        kind: "not_applied",
        canReconcileLate,
        isCurrent,
        release: () => isCurrent() && releaseWithFence(dispatch),
      };
    },
    observeTurnActivity,
    observeUserInput(binding: NativeBinding, nativeTurnId: string) {
      for (const dispatch of dispatches) {
        if (
          dispatch.onLateTerminal === undefined ||
          !matchesNativeBinding(dispatch, binding) ||
          dispatch.observedTurnId !== nativeTurnId ||
          dispatch.conflictingTurnActivity
        )
          continue;
        dispatch.observedInputTurnId = nativeTurnId;
        releaseSettled(dispatch);
      }
    },
    acknowledge(
      dispatch: CodexStartDispatch,
      runtimeGeneration: string,
      nativeTurnId: string,
      context?: AdditionalContext,
      onAcknowledged?: () => void,
    ) {
      if (!dispatches.has(dispatch) || dispatch.runtimeGeneration !== runtimeGeneration) return;
      if (!nativeTurnId || dispatch.excludedTurnIds.has(nativeTurnId)) return;
      if (dispatch.nativeTurnId !== undefined) {
        if (dispatch.nativeTurnId !== nativeTurnId) dispatch.overflow = true;
        return;
      }
      if (acknowledgedTurns.get(nativeTurnId)?.runtimeGeneration === runtimeGeneration) return;
      dispatch.nativeTurnId = nativeTurnId;
      rememberTurn(
        {
          runtimeGeneration,
          nativeThreadId: dispatch.nativeThreadId,
        },
        nativeTurnId,
      );
      // A prompt's accepted context survives caller failure. Keep the ordinal
      // even when this ACK clears context, so an older ACK cannot restore it.
      const accepted = acceptedContexts.get(dispatch.nativeThreadId);
      if (accepted === undefined || accepted.ordinal < dispatch.ordinal)
        acceptedContexts.set(dispatch.nativeThreadId, { ordinal: dispatch.ordinal, context });
      onAcknowledged?.();
      releaseSettled(dispatch);
    },
    acceptedContext(nativeThreadId: string) {
      return acceptedContexts.get(nativeThreadId)?.context;
    },
    // Reserve the order at steer dispatch so a delayed ACK cannot overwrite a newer start.
    prepareContextUpdate(
      nativeThreadId: string,
      update: (context: AdditionalContext | undefined) => AdditionalContext,
    ) {
      const ordinal = ++nextOrdinal;
      return () => {
        const accepted = acceptedContexts.get(nativeThreadId);
        if (accepted === undefined || accepted.ordinal < ordinal)
          acceptedContexts.set(nativeThreadId, { ordinal, context: update(accepted?.context) });
      };
    },
    recordNativeStart(binding: NativeBinding, nativeTurnId: string) {
      for (const dispatch of dispatches) {
        if (
          !matchesNativeBinding(dispatch, binding) ||
          dispatch.excludedTurnIds.has(nativeTurnId) ||
          (dispatch.nativeTurnId !== undefined && dispatch.nativeTurnId !== nativeTurnId) ||
          dispatch.startedTurnIds.has(nativeTurnId)
        )
          continue;
        if (dispatch.startedTurnIds.size >= 32) dispatch.overflow = true;
        else dispatch.startedTurnIds.add(nativeTurnId);
        if (dispatch.nativeTurnId === nativeTurnId) dispatch.onStartOrTerminal?.();
      }
    },
    complete(
      binding: NativeBinding,
      nativeTurnId: string,
      readTerminal?: () => CodexStartTerminal | undefined,
    ) {
      for (const dispatch of dispatches) {
        if (
          !matchesNativeBinding(dispatch, binding) ||
          dispatch.excludedTurnIds.has(nativeTurnId) ||
          (dispatch.nativeTurnId !== undefined && dispatch.nativeTurnId !== nativeTurnId)
        )
          continue;
        if (dispatch.completions.has(nativeTurnId)) continue;
        if (dispatch.completions.size >= 32) dispatch.overflow = true;
        else
          dispatch.completions.set(
            nativeTurnId,
            dispatch.onLateTerminal !== undefined ? readTerminal?.() : undefined,
          );
        if (dispatch.nativeTurnId === nativeTurnId) dispatch.onStartOrTerminal?.();
        releaseSettled(dispatch);
      }
    },
    ownsLateTerminal(dispatch: CodexStartDispatch, terminal: CodexStartTerminal) {
      return lateTerminal(dispatch) === terminal;
    },
    releaseLateTerminal(dispatch: CodexStartDispatch, terminal: CodexStartTerminal) {
      if (lateTerminal(dispatch) === terminal) releaseWithFence(dispatch);
    },
    release(dispatch: CodexStartDispatch) {
      if (!dispatch.unknown) dispatches.delete(dispatch);
    },
    get blocked() {
      return Array.from(dispatches).some((dispatch) => dispatch.unknown);
    },
  };
}
