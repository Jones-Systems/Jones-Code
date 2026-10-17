interface NativeBinding {
  readonly runtimeGeneration: string;
  readonly nativeThreadId: string;
}

export interface CodexStartDispatch extends NativeBinding {
  readonly excludedTurnIds: ReadonlySet<string>;
  readonly completions: Set<string>;
  nativeTurnId: string | undefined;
  unknown: boolean;
  overflow: boolean;
}

// A local timeout ends the caller's attempt, not the native effect. Only the
// original RPC acknowledgement and that exact native turn's terminal can lift
// this admission hold; this ledger never authorizes replay or revives an attempt.
export function makeCodexLateStartRecovery() {
  const dispatches = new Set<CodexStartDispatch>();
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
          dispatch.runtimeGeneration !== binding.runtimeGeneration ||
          dispatch.nativeThreadId !== binding.nativeThreadId ||
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
