import { runRanAfter } from "@t3tools/shared/orchestrationV2ThreadError";
import { RunId, ThreadId, type OrchestrationV2ThreadProjection } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as DateTime from "effect/DateTime";
import { threadShellFromProjection } from "../../orchestration-v2/ProjectionStore.ts";
import { restartContinuationRun } from "../../orchestration-v2/RestartContinuation.ts";
import { workModeCandidate, workModeCommand, workModeContext } from "../workMode/Policy.ts";

export const PlannedThreadSnapshot = Schema.Struct({
  threadId: ThreadId,
  sourceRunId: RunId,
  threadIdentity: Schema.String,
  explicitContinuation: Schema.Boolean,
  receiptRowId: Schema.Number,
  receiptCommandId: Schema.String,
  queue: Schema.NullOr(Schema.String),
  workGeneration: Schema.NullOr(Schema.String),
  workOwner: Schema.NullOr(Schema.String),
});
export type PlannedThreadSnapshot = typeof PlannedThreadSnapshot.Type;

function threadIdentity(projection: OrchestrationV2ThreadProjection): string {
  const thread = projection.thread;
  return JSON.stringify([
    thread.projectId,
    thread.modelSelection,
    thread.runtimeMode,
    thread.interactionMode,
    thread.worktreePath,
    thread.branch,
    thread.activeProviderThreadId,
    thread.lineage,
  ]);
}

export function workOwnerIdentity(owner: NonNullable<ReturnType<typeof workModeContext>>): string {
  return JSON.stringify([
    owner.id,
    owner.driver,
    owner.providerInstanceId,
    owner.providerSessionId,
    owner.appThreadId,
    owner.ownerNodeId,
    owner.nativeThreadRef,
  ]);
}

function queuedIdentity(projection: OrchestrationV2ThreadProjection): string | null {
  const queued = projection.runs
    .filter((run) => run.status === "queued")
    .toSorted((a, b) => (a.queuePosition ?? a.ordinal) - (b.queuePosition ?? b.ordinal));
  if (queued.length === 0) return null;
  const entries = queued.map((run) => {
    const message = projection.messages.find((candidate) => candidate.id === run.userMessageId);
    if (message === undefined) return null;
    return [
      run.id,
      run.ordinal,
      run.queuePosition,
      run.userMessageId,
      run.modelSelection,
      run.providerThreadId,
      run.activeAttemptId,
      run.rootNodeId,
      DateTime.formatIso(run.requestedAt),
      message.text,
      message.attachments,
      message.context,
      DateTime.formatIso(message.updatedAt),
    ];
  });
  return entries.some((entry) => entry === null) ? null : JSON.stringify(entries);
}

export function captureThreadContinuity(input: {
  readonly projection: OrchestrationV2ThreadProjection;
  readonly explicitContinuation: boolean;
  readonly workModeEnabled: boolean;
  readonly liveWorkOwner: boolean;
  readonly nowMs: number;
  readonly receiptRowId: number;
  readonly receiptCommandId: string;
}): PlannedThreadSnapshot | undefined {
  const { projection } = input;
  const shell = threadShellFromProjection(projection);
  if (
    shell.archivedAt !== null ||
    shell.deletedAt !== null ||
    shell.settledAt !== null ||
    shell.settledOverride === "settled" ||
    shell.threadMessagesBlocked === true ||
    shell.pendingRuntimeRequest !== null ||
    shell.lastErrorClass === "usage_limit" ||
    (shell.snoozedUntil != null && DateTime.toEpochMillis(shell.snoozedUntil) > input.nowMs) ||
    projection.runtimeRequests.some((request) => request.status === "pending")
  )
    return undefined;
  const latest = projection.runs
    .filter((run) => run.status !== "queued")
    .reduce<(typeof projection.runs)[number] | undefined>(
      (previous, run) => (previous === undefined || runRanAfter(run, previous) ? run : previous),
      undefined,
    );
  if (
    latest === undefined ||
    (latest.status !== "completed" && restartContinuationRun(projection)?.id !== latest.id)
  )
    return undefined;
  if (
    projection.turnItems.some(
      (item) => item.runId === latest.id && item.type === "run_interrupt_request",
    )
  )
    return undefined;
  const queue = projection.runs.some((run) => run.status === "queued" && run.queueHeld === true)
    ? null
    : queuedIdentity(projection);
  const candidate =
    input.workModeEnabled && input.liveWorkOwner
      ? workModeCandidate(shell, input.nowMs, { ignoreInterval: true })
      : null;
  const owner =
    candidate === null ? null : workModeContext(projection, input.nowMs, { ignoreInterval: true });
  if (queue === null && owner === null) return undefined;
  return {
    threadId: shell.id,
    sourceRunId: latest.id,
    threadIdentity: threadIdentity(projection),
    explicitContinuation: input.explicitContinuation,
    receiptRowId: input.receiptRowId,
    receiptCommandId: input.receiptCommandId,
    queue,
    workGeneration: owner === null ? null : candidate!.generation,
    workOwner: owner === null ? null : workOwnerIdentity(owner),
  };
}

export function samePlannedThread(
  snapshot: PlannedThreadSnapshot,
  projection: OrchestrationV2ThreadProjection,
): boolean {
  const shell = threadShellFromProjection(projection);
  return (
    snapshot.threadId === shell.id &&
    snapshot.threadIdentity === threadIdentity(projection) &&
    shell.archivedAt === null &&
    shell.deletedAt === null &&
    shell.settledAt === null &&
    shell.settledOverride !== "settled" &&
    shell.threadMessagesBlocked !== true &&
    shell.pendingRuntimeRequest === null &&
    shell.lastErrorClass !== "usage_limit" &&
    !projection.runtimeRequests.some((request) => request.status === "pending")
  );
}

export function continuationRunIds(
  snapshot: PlannedThreadSnapshot,
  projection: OrchestrationV2ThreadProjection,
): Set<string> {
  const ids = new Set<string>([snapshot.sourceRunId]);
  for (const run of projection.runs.toSorted((a, b) => a.ordinal - b.ordinal)) {
    if (run.restartContinuationOfRunId !== undefined && ids.has(run.restartContinuationOfRunId))
      ids.add(run.id);
  }
  return ids;
}

export function canReleasePlannedQueue(
  snapshot: PlannedThreadSnapshot,
  projection: OrchestrationV2ThreadProjection,
): boolean {
  if (
    snapshot.queue === null ||
    !samePlannedThread(snapshot, projection) ||
    queuedIdentity(projection) !== snapshot.queue ||
    projection.runs.some((run) =>
      ["preparing", "starting", "running", "waiting"].includes(run.status),
    )
  )
    return false;
  const ids = continuationRunIds(snapshot, projection);
  const latest = projection.runs
    .filter((run) => run.status !== "queued")
    .reduce<(typeof projection.runs)[number] | undefined>(
      (previous, run) => (previous === undefined || runRanAfter(run, previous) ? run : previous),
      undefined,
    );
  return latest !== undefined && latest.status === "completed" && ids.has(latest.id);
}

const cosmeticCommands = new Set([
  "thread.visit",
  "thread.mark-unread",
  "thread.pin",
  "thread.unpin",
  "thread.pin.reorder",
  "thread.active.reorder",
  "thread.pull-request.sync",
  "thread.pull-request-link.sync",
  "thread.pull-request-watch.sync",
  "thread.title.regeneration.complete",
]);

export function changesPlannedControl(
  receipt: { readonly command_id: string; readonly command_type: string; readonly accepted_at?: string | undefined },
  snapshot: PlannedThreadSnapshot,
  continuationIds: ReadonlySet<string>,
): boolean {
  if (cosmeticCommands.has(receipt.command_type)) return false;
  // Only ProviderRuntimeRecoveryService emits this internal receipt. Its exact identity
  // binds the lifecycle trigger, captured thread, and committed acceptance timestamp.
  // The caller still checks every intervening user receipt and the current projection.
  if (
    receipt.command_type === "provider-runtime.reconcile" &&
    receipt.accepted_at !== undefined &&
    /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(receipt.accepted_at) &&
    Number.isFinite(Date.parse(receipt.accepted_at)) &&
    new Date(receipt.accepted_at).toISOString() === receipt.accepted_at &&
    ["startup", "shutdown"].some((trigger) =>
      receipt.command_id === `command:runtime-reconcile:${trigger}:${snapshot.threadId}:${receipt.accepted_at}`,
    )
  ) return false;
  if (
    receipt.command_type === "checkpoint.capture" &&
    [...continuationIds].some((id) => receipt.command_id === `command:effect:checkpoint.capture:${id}`)
  ) return false;
  if (
    receipt.command_type === "message.dispatch" &&
    snapshot.workGeneration !== null &&
    receipt.command_id ===
      workModeCommand({
        threadId: snapshot.threadId,
        generation: snapshot.workGeneration,
      }).commandId
  )
    return false;
  return !(
    receipt.command_type === "message.dispatch" &&
    [...continuationIds].some((id) => receipt.command_id === `command:restart-continuation:${id}`)
  );
}
