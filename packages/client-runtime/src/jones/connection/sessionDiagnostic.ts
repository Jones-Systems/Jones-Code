import type { SupervisorConnectionState } from "../../connection/model.ts";

/**
 * The supervisor retains readiness, not an authorization transcript. A cached
 * token or a socket upgrade cannot prove bootstrap, token exchange, or HTTP reads.
 */
export function connectionSessionFacts(
  state: SupervisorConnectionState,
): ReadonlyArray<readonly [string, string]> {
  const ready = state.desired && state.phase === "connected" && state.lastFailure === null;
  return [
    ["Connection state", state.phase],
    ["Connection generation", String(state.generation)],
    ...(state.stage === null ? [] : [["Current stage", state.stage] as const]),
    ["Relay authorization / bootstrap", "not checked (no per-stage evidence retained)"],
    ["Descriptor identity", "not checked (no per-stage evidence retained)"],
    ["Token exchange", "not checked (a credential may have been cached)"],
    ["Authenticated HTTP read", "not checked (not recorded by connection state)"],
    [
      "RPC readiness",
      ready
        ? "ready (current connection synchronized its initial configuration)"
        : state.phase === "connecting" && state.stage === "synchronizing"
          ? "not ready (waiting for initial configuration)"
          : "not checked for a current connected session",
    ],
    [
      "Host probe interpretation",
      "separate observation; HTTP 403 does not establish failure of this device's authenticated session",
    ],
  ];
}
