import {
  CommandId,
  MessageId,
  type OrchestrationV2Command,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2ThreadShell,
  type ModelSelection,
  type ThreadId,
} from "@t3tools/contracts";
import { WORK_MODE_INTERVAL_MS, WORK_MODE_SENTINEL } from "@t3tools/shared/jones/workMode";
import { backgroundWorkHoldsCompletion } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import * as DateTime from "effect/DateTime";

export interface WorkModeCandidate {
  readonly threadId: ThreadId;
  readonly generation: string;
}

export type WorkModeAdmissionResult = "dispatched" | "acknowledged" | "skipped";

function activityMillis(thread: OrchestrationV2ThreadShell): number | null {
  const dates = [
    thread.latestUserAuthoredMessageAt,
    thread.latestUserMessageAt,
    thread.latestVisibleMessage?.updatedAt,
    thread.latestRunRequestedAt,
    thread.latestRunStartedAt,
    thread.latestRunCompletedAt,
  ].filter((value) => value != null);
  return dates.length === 0 ? null : Math.max(...dates.map(DateTime.toEpochMillis));
}

export function workModeCandidate(
  thread: OrchestrationV2ThreadShell,
  nowMs: number,
  options?: { readonly ignoreInterval?: boolean },
): WorkModeCandidate | null {
  if (
    thread.lineage.parentThreadId !== null ||
    thread.lineage.relationshipToParent !== null ||
    thread.archivedAt !== null ||
    thread.deletedAt !== null ||
    thread.settledAt !== null ||
    thread.settledOverride === "settled" ||
    thread.threadMessagesBlocked === true ||
    thread.status !== "completed" ||
    thread.latestRunId === null ||
    thread.latestRunCompletedAt == null ||
    thread.activeProviderThreadId === null ||
    thread.activeRunId !== null ||
    thread.activityRunStatus != null ||
    thread.pendingRuntimeRequest !== null ||
    thread.limitRecovery != null ||
    thread.lastErrorClass === "usage_limit" ||
    (thread.snoozedUntil != null && DateTime.toEpochMillis(thread.snoozedUntil) > nowMs) ||
    backgroundWorkHoldsCompletion(thread.pendingBackgroundTasks ?? [])
  )
    return null;
  const anchor = activityMillis(thread);
  if (
    anchor === null ||
    !Number.isFinite(nowMs) ||
    (options?.ignoreInterval !== true && nowMs - anchor < WORK_MODE_INTERVAL_MS)
  )
    return null;
  return { threadId: thread.id, generation: `${thread.latestRunId}:${anchor}` };
}

export function workModeContext(
  projection: OrchestrationV2ThreadProjection,
  nowMs: number,
  options?: { readonly ignoreInterval?: boolean; readonly allowStoppedSession?: boolean },
) {
  if (
    projection.thread.selfSettlement != null ||
    projection.runs.some((run) =>
      ["queued", "preparing", "starting", "running", "waiting"].includes(run.status),
    ) ||
    projection.runtimeRequests.some((request) => request.status === "pending") ||
    projection.providerTurns.some((turn) => turn.status === "pending" || turn.status === "running")
  )
    return null;
  const latestRun = projection.runs.toSorted((a, b) => b.ordinal - a.ordinal)[0];
  const owner = projection.providerThreads.find(
    (thread) => thread.id === projection.thread.activeProviderThreadId,
  );
  if (
    latestRun?.status !== "completed" ||
    latestRun.startedAt === null ||
    latestRun.completedAt === null ||
    latestRun.providerThreadId !== owner?.id ||
    owner?.providerSessionId == null ||
    owner.status !== "idle" ||
    owner.nativeThreadRef == null ||
    owner.ownerNodeId !== null ||
    owner.appThreadId !== projection.thread.id ||
    owner.providerInstanceId !== projection.thread.modelSelection.instanceId ||
    backgroundWorkHoldsCompletion(owner.pendingBackgroundTasks ?? []) ||
    !projection.providerTurns.some(
      (turn) =>
        turn.providerThreadId === owner.id &&
        turn.runAttemptId === latestRun.activeAttemptId &&
        turn.status === "completed",
    )
  )
    return null;
  const session = projection.providerSessions.find((row) => row.id === owner.providerSessionId);
  if (
    session === undefined ||
    session.lastError !== null ||
    (session.status !== "ready" &&
      !(options?.allowStoppedSession === true && session.status === "stopped"))
  )
    return null;
  const lastMessageAt = projection.messages
    .filter((message) => message.role !== "system")
    .reduce(
      (latest, message) =>
        Math.max(
          latest,
          DateTime.toEpochMillis(message.createdAt),
          DateTime.toEpochMillis(message.updatedAt),
        ),
      Number.NEGATIVE_INFINITY,
    );
  const lastRunAt = projection.runs.reduce(
    (latest, run) =>
      Math.max(
        latest,
        DateTime.toEpochMillis(run.requestedAt),
        run.startedAt === null ? Number.NEGATIVE_INFINITY : DateTime.toEpochMillis(run.startedAt),
        run.completedAt === null
          ? Number.NEGATIVE_INFINITY
          : DateTime.toEpochMillis(run.completedAt),
      ),
    Number.NEGATIVE_INFINITY,
  );
  if (
    options?.ignoreInterval !== true &&
    nowMs - Math.max(lastMessageAt, lastRunAt) < WORK_MODE_INTERVAL_MS
  )
    return null;
  return owner;
}

export function workModeCommand(candidate: WorkModeCandidate, modelSelection?: ModelSelection) {
  const identity = `work-mode:${encodeURIComponent(candidate.threadId)}:${encodeURIComponent(candidate.generation)}`;
  return {
    type: "message.dispatch",
    commandId: CommandId.make(identity),
    messageId: MessageId.make(identity),
    threadId: candidate.threadId,
    text: WORK_MODE_SENTINEL,
    attachments: [],
    createdBy: "system",
    creationSource: "server",
    dispatchMode: { type: "start_immediately" },
    ...(modelSelection === undefined ? {} : { modelSelection }),
  } satisfies Extract<OrchestrationV2Command, { readonly type: "message.dispatch" }>;
}
