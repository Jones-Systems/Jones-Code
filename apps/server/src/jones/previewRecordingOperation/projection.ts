import {
  PreviewRecordingReportedStart,
  type PreviewAutomationOperation,
  type PreviewAutomationResponse,
  type PreviewAutomationRuntimeIdentity,
  type PreviewRecordingOperationInput,
  type PreviewRecordingOperationResult,
  type PreviewTabId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as DateTime from "effect/DateTime";

interface RecordingScope {
  readonly environmentId: string;
  readonly thread: {
    readonly threadId: string;
    readonly providerSessionId: string;
    readonly providerInstanceId: string;
  };
}
interface RequestContext {
  readonly operation: PreviewAutomationOperation;
  readonly environmentId: string;
  readonly threadId: string;
  readonly providerSessionId: string;
  readonly providerInstanceId: string;
  readonly requestId: string;
  readonly clientId: string;
  readonly connectionId: string;
  readonly tabId?: PreviewTabId;
}
interface RetainedRequest {
  readonly context: RequestContext;
  readonly dispatched: boolean;
  readonly runtimeIdentity?: PreviewAutomationRuntimeIdentity | undefined;
  readonly runtimeEvidence?: {
    readonly runtimeIdentity: PreviewAutomationRuntimeIdentity | null;
    readonly attachmentGeneration: number;
  } | null;
}
type Result = PreviewRecordingOperationResult["result"];
type MatchedResult = Extract<Result, { status: "pending" | "unknown" | "settled" }>;
type SettledResult = Extract<Result, { status: "settled" }>;
export interface CompletedRecordingOperation {
  readonly context: RequestContext;
  readonly result: SettledResult;
}

const isRecording = (operation: PreviewAutomationOperation) =>
  operation === "recordingStart" || operation === "recordingStop";
const handleFrom = (context: RequestContext) => ({
  requestId: context.requestId,
  clientId: context.clientId,
  connectionId: context.connectionId,
});
const dispatchEvidence = (entry: RetainedRequest) => {
  // Evidence is copied from dispatch, never filled from today's host or attachment.
  const identity = entry.runtimeIdentity;
  return {
    basis: "dispatch_time" as const,
    runtimeIdentity:
      identity === undefined
        ? null
        : {
            schemaVersion: identity.schemaVersion,
            runtimeKind: identity.runtimeKind,
            runtimeInstanceId: identity.runtimeInstanceId,
            appVersion: identity.appVersion,
            buildCommit: identity.buildCommit,
          },
    attachmentGeneration: entry.runtimeEvidence?.attachmentGeneration ?? null,
  };
};
const evidenceFrom = (entry: RetainedRequest) =>
  entry.dispatched ? { dispatchEvidence: dispatchEvidence(entry) } : {};
const matches = (
  context: RequestContext,
  scope: RecordingScope,
  input: PreviewRecordingOperationInput,
) =>
  isRecording(context.operation) &&
  context.environmentId === scope.environmentId &&
  context.threadId === scope.thread.threadId &&
  context.providerSessionId === scope.thread.providerSessionId &&
  context.providerInstanceId === scope.thread.providerInstanceId &&
  context.tabId !== undefined &&
  context.tabId === input.tabId &&
  (input.handle === undefined ||
    (context.requestId === input.handle.requestId &&
      context.clientId === input.handle.clientId &&
      context.connectionId === input.handle.connectionId));

const isReportedStart = Schema.is(PreviewRecordingReportedStart);

// Called only at the existing accepted response transition, with its pre-transition quarantine entry.
export const retainRecordingSettlement = (
  completed: ReadonlyMap<string, CompletedRecordingOperation>,
  quarantined: RetainedRequest | undefined,
  response: PreviewAutomationResponse,
  now: number,
): ReadonlyMap<string, CompletedRecordingOperation> => {
  if (
    quarantined === undefined ||
    !isRecording(quarantined.context.operation) ||
    completed.has(response.requestId)
  )
    return completed;
  const context = quarantined.context;
  const tabId = context.tabId;
  if (tabId === undefined) return completed;
  const operation = context.operation as "recordingStart" | "recordingStop";
  const reportedOutcome = response.ok
    ? response.error === undefined
      ? "success"
      : "unclassifiable"
    : response.error?.outcome === "not_started"
      ? "not_started"
      : "failure";
  let metadata: SettledResult["metadata"] = { status: "not_projected" };
  if (reportedOutcome === "unclassifiable") metadata = { status: "invalid" };
  else if (reportedOutcome === "success" && operation === "recordingStart") {
    const value = response.result;
    metadata =
      isReportedStart(value) && value.tabId === tabId
        ? {
            status: "valid",
            reportedStart: {
              tabId: value.tabId,
              recording: value.recording,
              startedAt: value.startedAt,
            },
          }
        : { status: "invalid" };
  }
  const result: SettledResult = {
    status: "settled",
    operation,
    handle: handleFrom(context),
    ...evidenceFrom(quarantined),
    reportedOutcome,
    metadata,
    settledAt: DateTime.formatIso(DateTime.makeUnsafe(now)),
  };
  const next = new Map(completed);
  // Deliberately copy only identity and the reduced projection, never PendingRequest resources.
  next.set(response.requestId, {
    context: {
      operation,
      environmentId: context.environmentId,
      threadId: context.threadId,
      providerSessionId: context.providerSessionId,
      providerInstanceId: context.providerInstanceId,
      requestId: context.requestId,
      clientId: context.clientId,
      connectionId: context.connectionId,
      tabId,
    },
    result,
  });
  while (next.size > 256) next.delete(next.keys().next().value!);
  return next;
};

export const selectRecordingOperation = (
  snapshot: {
    readonly pending: ReadonlyMap<string, RetainedRequest>;
    readonly quarantine: ReadonlyMap<string, RetainedRequest>;
    readonly completedRecordingOperations: ReadonlyMap<string, CompletedRecordingOperation>;
  },
  scope: RecordingScope,
  input: PreviewRecordingOperationInput,
): PreviewRecordingOperationResult => {
  const candidates: Array<Extract<Result, { status: "ambiguous" }>["candidates"][number]> = [];
  let unique: MatchedResult | undefined;
  let truncated = false;
  const visit = (context: RequestContext, result: () => MatchedResult) => {
    if (!matches(context, scope, input)) return;
    if (candidates.length === 8) {
      truncated = true;
      return;
    }
    const value = result();
    if (unique === undefined) unique = value;
    candidates.push({
      handle: value.handle,
      operation: value.operation,
      status: value.status,
    });
  };
  // Quarantine wins an inconsistent duplicate, then pending, then completed.
  // Deduplication uses keyed snapshot lookups rather than an unbounded mirror/set.
  const duplicate = (
    context: RequestContext,
    entry: { readonly context: RequestContext } | undefined,
  ) =>
    entry !== undefined &&
    entry.context.requestId === context.requestId &&
    entry.context.clientId === context.clientId &&
    entry.context.connectionId === context.connectionId;
  for (const entry of snapshot.quarantine.values()) {
    visit(entry.context, () => ({
      status: "unknown",
      operation: entry.context.operation as "recordingStart" | "recordingStop",
      handle: handleFrom(entry.context),
      ...evidenceFrom(entry),
    }));
  }
  for (const entry of snapshot.pending.values()) {
    if (duplicate(entry.context, snapshot.quarantine.get(entry.context.requestId))) continue;
    visit(entry.context, () => ({
      status: "pending",
      operation: entry.context.operation as "recordingStart" | "recordingStop",
      handle: handleFrom(entry.context),
      ...(entry.dispatched
        ? { dispatched: true, dispatchEvidence: dispatchEvidence(entry) }
        : { dispatched: false }),
    }));
  }
  for (const entry of snapshot.completedRecordingOperations.values()) {
    if (
      duplicate(entry.context, snapshot.quarantine.get(entry.context.requestId)) ||
      duplicate(entry.context, snapshot.pending.get(entry.context.requestId))
    )
      continue;
    visit(entry.context, () => entry.result);
  }
  return {
    schemaVersion: 1,
    tabId: input.tabId,
    result:
      candidates.length > 1
        ? { status: "ambiguous", candidates, truncated }
        : (unique ?? { status: "unavailable", reason: "not_retained" }),
  };
};
