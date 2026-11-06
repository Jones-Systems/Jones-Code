import * as Schema from "effect/Schema";
import {
  DESKTOP_THREAD_MONITOR_PROTOCOL,
  decodeThreadMonitorFrame,
  ThreadMonitorNativeState,
  type ThreadMonitorPeerIntent,
  type ThreadMonitorPublisherFrame,
} from "@t3tools/contracts/jones/desktopThreadMonitor";

const publisherTypes = new Set([
  "view",
  "viewState",
  "invalidateEnvironment",
  "theme",
  "detail",
  "draft",
  "actionResult",
  "reset",
]);
const peerTypes = new Set([
  "send",
  "reconcile",
  "settle",
  "restore",
  "open",
  "respondToApproval",
  "respondToUserInput",
  "usePlan",
  "environmentInvalidated",
  "editDraft",
  "selectDetail",
  "releaseDetail",
  "setExpanded",
  "setViewState",
  "hide",
  "requestMore",
  "requestSnapshot",
  "ready",
]);
export const envelope = <T>(payload: T) => ({
  schemaVersion: DESKTOP_THREAD_MONITOR_PROTOCOL,
  payload,
});

export function publisherPayload(raw: unknown): ThreadMonitorPublisherFrame {
  const frame = decodeThreadMonitorFrame(raw).payload;
  if (!publisherTypes.has(frame.type)) throw new Error("Invalid publisher direction");
  return frame as ThreadMonitorPublisherFrame;
}
export function peerPayload(raw: unknown): ThreadMonitorPeerIntent {
  const frame = decodeThreadMonitorFrame(raw).payload;
  if (!peerTypes.has(frame.type)) throw new Error("Invalid peer direction");
  return frame as ThreadMonitorPeerIntent;
}

const StateEnvelope = Schema.Struct({
  schemaVersion: Schema.Literal(DESKTOP_THREAD_MONITOR_PROTOCOL),
  payload: ThreadMonitorNativeState,
});
const EnabledEnvelope = Schema.Struct({
  schemaVersion: Schema.Literal(DESKTOP_THREAD_MONITOR_PROTOCOL),
  payload: Schema.Boolean,
});
const EmptyEnvelope = Schema.Struct({
  schemaVersion: Schema.Literal(DESKTOP_THREAD_MONITOR_PROTOCOL),
  payload: Schema.Null,
});
export const statePayload = (raw: unknown) =>
  Schema.decodeUnknownSync(StateEnvelope, { onExcessProperty: "error" })(raw).payload;
export const enabledPayload = (raw: unknown) =>
  Schema.decodeUnknownSync(EnabledEnvelope, { onExcessProperty: "error" })(raw).payload;
export const emptyPayload = (raw: unknown) =>
  Schema.decodeUnknownSync(EmptyEnvelope, { onExcessProperty: "error" })(raw).payload;
