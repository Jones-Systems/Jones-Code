import type { CodexAppServerClientOptions } from "effect-codex-app-server/client";
import type * as CodexErrors from "effect-codex-app-server/errors";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import { isNonApplyingStartupRejection } from "./CodexStartup.ts";

// A slow live producer can acknowledge the original start well after 30 seconds.
// Bound a wedged wait without treating the deadline as proof of native cancellation.
export const CODEX_START_ACK_TIMEOUT_MS = 10 * 60 * 1_000;
const INVALID_PARAMS = -32602;

// Observe the decoded envelope before the protocol reduces it to a request error.
// Evidence collection also runs with native event logging disabled.
export function codexStartReplyObservationOptions(
  observeReply: ((reply: unknown) => void) | undefined,
  protocolLogger?: CodexAppServerClientOptions["logger"],
): CodexAppServerClientOptions {
  if (observeReply === undefined && protocolLogger === undefined) return {};
  return {
    logIncoming: true,
    logOutgoing: protocolLogger !== undefined,
    logger: (event) => {
      if (event.direction === "incoming" && event.stage === "decoded") observeReply?.(event.payload);
      return protocolLogger?.(event) ?? Effect.void;
    },
  };
}

interface NativeBinding {
  readonly runtimeGeneration: string;
  readonly nativeThreadId: string;
}

export interface CodexStartDispatch extends NativeBinding {
  readonly excludedTurnIds: ReadonlySet<string>;
  readonly completions: Set<string>;
  nativeTurnId: string | undefined;
  turnActivityObserved: boolean;
  unknown: boolean;
  overflow: boolean;
}

const matchesNativeBinding = (dispatch: CodexStartDispatch, binding: NativeBinding) =>
  dispatch.runtimeGeneration === binding.runtimeGeneration &&
  dispatch.nativeThreadId === binding.nativeThreadId;

// The caller's deadline leaves the native effect unresolved. The original RPC
// can reconcile it with an exact non-applying rejection, or with an ACK and the
// acknowledged turn's terminal. Reconciliation only lifts this admission hold.
export function makeCodexLateStartRecovery() {
  const dispatches = new Set<CodexStartDispatch>();
  const rejectionReplies = new WeakMap<object, { runtimeGeneration: string; requestId: string }>();
  const settled = (dispatch: CodexStartDispatch) =>
    !dispatch.overflow &&
    dispatch.nativeTurnId !== undefined &&
    dispatch.completions.has(dispatch.nativeTurnId);
  const releaseSettled = (dispatch: CodexStartDispatch) => {
    if (dispatch.unknown && settled(dispatch)) dispatches.delete(dispatch);
  };
  return {
    begin(binding: NativeBinding, excludedTurnIds: ReadonlyArray<string>): CodexStartDispatch {
      const dispatch: CodexStartDispatch = {
        ...binding,
        excludedTurnIds: new Set(excludedTurnIds),
        completions: new Set(),
        nativeTurnId: undefined,
        turnActivityObserved: false,
        unknown: false,
        overflow: false,
      };
      dispatches.add(dispatch);
      return dispatch;
    },
    markUnknown(dispatch: CodexStartDispatch) {
      if (!dispatches.has(dispatch)) return;
      dispatch.unknown = true;
      releaseSettled(dispatch);
    },
    observeReply(runtimeGeneration: string, reply: unknown) {
      // The envelope schema permits both result and error. The client sends
      // integer IDs and Codex stdio may omit the JSON-RPC version.
      if (
        dispatches.size === 0 ||
        !Predicate.isObject(reply) ||
        typeof reply.id !== "number" ||
        !Number.isSafeInteger(reply.id) ||
        reply.id < 1 ||
        ("jsonrpc" in reply && reply.jsonrpc !== "2.0") ||
        !Predicate.isObject(reply.error) ||
        reply.error.code !== INVALID_PARAMS ||
        typeof reply.error.message !== "string" ||
        Object.keys(reply).some((key) => key !== "id" && key !== "error" && key !== "jsonrpc") ||
        Object.keys(reply.error).some((key) => key !== "code" && key !== "message" && key !== "data")
      )
        return;
      rejectionReplies.set(reply.error, { runtimeGeneration, requestId: String(reply.id) });
    },
    reject(
      dispatch: CodexStartDispatch,
      currentGeneration: string | undefined,
      error: CodexErrors.CodexAppServerError,
    ) {
      if (
        error._tag !== "CodexAppServerRequestError" ||
        !isNonApplyingStartupRejection(error, "turn/start")
      )
        return "other_error";
      if (error.code !== INVALID_PARAMS || !Predicate.isObject(error.cause)) return "unverified";
      // The original waiter binds the request method and ID to this dispatch.
      // Object identity ties that failure to the exact envelope on the wire.
      const reply = rejectionReplies.get(error.cause);
      if (
        !dispatches.has(dispatch) ||
        dispatch.runtimeGeneration !== currentGeneration ||
        reply === undefined ||
        reply.runtimeGeneration !== currentGeneration ||
        reply.requestId !== error.requestId ||
        dispatch.nativeTurnId !== undefined ||
        dispatch.turnActivityObserved ||
        dispatch.completions.size > 0 ||
        dispatch.overflow
      )
        return "unverified";
      rejectionReplies.delete(error.cause);
      dispatches.delete(dispatch);
      return "not_applied";
    },
    observeTurnActivity(runtimeGeneration: string, payload: unknown) {
      if (dispatches.size === 0 || !Predicate.isObject(payload)) return;
      const nativeThreadId = payload.threadId;
      const nativeTurnId =
        typeof payload.turnId === "string"
          ? payload.turnId
          : Predicate.isObject(payload.turn)
            ? payload.turn.id
            : undefined;
      if (typeof nativeThreadId !== "string" || typeof nativeTurnId !== "string") return;
      const binding = { nativeThreadId, runtimeGeneration };
      for (const dispatch of dispatches) {
        if (matchesNativeBinding(dispatch, binding) && !dispatch.excludedTurnIds.has(nativeTurnId))
          dispatch.turnActivityObserved = true;
      }
    },
    acknowledge(dispatch: CodexStartDispatch, runtimeGeneration: string, nativeTurnId: string) {
      if (!dispatches.has(dispatch) || dispatch.runtimeGeneration !== runtimeGeneration) return;
      if (!nativeTurnId || dispatch.excludedTurnIds.has(nativeTurnId)) return;
      if (dispatch.nativeTurnId !== undefined && dispatch.nativeTurnId !== nativeTurnId) {
        dispatch.overflow = true;
        return;
      }
      dispatch.nativeTurnId = nativeTurnId;
      releaseSettled(dispatch);
    },
    complete(binding: NativeBinding, nativeTurnId: string) {
      for (const dispatch of dispatches) {
        if (
          !matchesNativeBinding(dispatch, binding) ||
          dispatch.excludedTurnIds.has(nativeTurnId) ||
          (dispatch.nativeTurnId !== undefined && dispatch.nativeTurnId !== nativeTurnId)
        )
          continue;
        if (dispatch.completions.has(nativeTurnId)) continue;
        if (dispatch.completions.size >= 32) dispatch.overflow = true;
        else dispatch.completions.add(nativeTurnId);
        releaseSettled(dispatch);
      }
    },
    release(dispatch: CodexStartDispatch) {
      if (!dispatch.unknown) dispatches.delete(dispatch);
    },
    get blocked() {
      return Array.from(dispatches).some((dispatch) => dispatch.unknown);
    },
  };
}
