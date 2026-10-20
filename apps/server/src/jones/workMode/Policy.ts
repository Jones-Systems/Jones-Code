import {
  CommandId,
  MessageId,
  type OrchestrationV2Command,
  type OrchestrationV2ProviderGoal,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2ThreadShell,
  type ModelSelection,
  type ProviderThreadId,
  type ThreadId,
} from "@t3tools/contracts";
import {
  WORK_MODE_INITIAL_MAX_IDLE_MS,
  WORK_MODE_INTERVAL_MS,
  WORK_MODE_SENTINEL,
} from "@t3tools/shared/jones/workMode";
import { backgroundWorkHoldsCompletion } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import * as DateTime from "effect/DateTime";
import { isWorkModeKeepWarm } from "../provider/workModePrompt.ts";

export interface WorkModeCandidate {
  readonly threadId: ThreadId;
  readonly generation: string;
}

export type WorkModeAdmissionResult = "dispatched" | "acknowledged" | "skipped";

export interface WorkModeTiming {
  readonly lastActivityMs: number;
  readonly dueAtMs: number;
  readonly expiresAtMs: number | null;
}

export function workModeTiming(
  records: Pick<OrchestrationV2ThreadProjection, "providerTurns" | "runs" | "messages">,
  ownerId: ProviderThreadId,
): WorkModeTiming | null {
  const runs = new Map(
    records.runs
      .filter((run) => run.providerThreadId === ownerId && run.activeAttemptId !== null)
      .map((run) => [run.activeAttemptId, run]),
  );
  const refreshMessages = new Set(
    records.messages.filter(isWorkModeKeepWarm).map((message) => message.id),
  );
  let lastActivityMs = Number.NEGATIVE_INFINITY;
  let established = false;
  for (const turn of records.providerTurns) {
    if (turn.providerThreadId !== ownerId || turn.nativeTurnRef === null) continue;
    const run = runs.get(turn.runAttemptId);
    if (run === undefined || run.rootNodeId !== turn.nodeId) continue;
    const usage = turn.tokenUsage;
    // Compaction and synthetic replies can update context size without model input/output.
    if (
      usage === undefined ||
      (usage.inputTokens ?? 0) + (usage.cachedInputTokens ?? 0) + (usage.outputTokens ?? 0) <= 0
    )
      continue;
    const at = Date.parse(usage.updatedAt);
    if (!Number.isFinite(at)) continue;
    lastActivityMs = Math.max(lastActivityMs, at);
    if (
      turn.status === "completed" &&
      run.status === "completed" &&
      refreshMessages.has(run.userMessageId)
    )
      established = true;
  }
  if (!Number.isFinite(lastActivityMs)) return null;
  let lastRefreshStartMs = Number.NEGATIVE_INFINITY;
  for (const run of runs.values()) {
    if (!refreshMessages.has(run.userMessageId)) continue;
    lastRefreshStartMs = Math.max(
      lastRefreshStartMs,
      DateTime.toEpochMillis(run.startedAt ?? run.requestedAt),
    );
  }
  return {
    lastActivityMs,
    // A completed refresh missing usage must not cause another request on every sweep.
    dueAtMs: Math.max(lastActivityMs, lastRefreshStartMs) + WORK_MODE_INTERVAL_MS,
    expiresAtMs: established ? null : lastActivityMs + WORK_MODE_INITIAL_MAX_IDLE_MS,
  };
}

// An unfinished goal resumes after the next completed turn: Codex starts its next
// goal turn, which the adapter adopts into that run, and Claude re-checks the goal
// at the end of every message. A refresh must never be what resumes goal work;
// only paused or complete goals are known not to continue.
function goalMayContinue(goal: OrchestrationV2ProviderGoal | null | undefined): boolean {
  return goal != null && goal.status !== "paused" && goal.status !== "complete";
}

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
  _options?: { readonly ignoreInterval?: boolean },
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
    backgroundWorkHoldsCompletion(thread.pendingBackgroundTasks ?? []) ||
    goalMayContinue(thread.goal)
  )
    return null;
  // Shell activity identifies dispatch races; only provider usage sets the refresh clock.
  const anchor = activityMillis(thread);
  if (anchor === null || !Number.isFinite(nowMs)) return null;
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
    goalMayContinue(owner.goal) ||
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
  const timing = workModeTiming(projection, owner.id);
  if (
    timing === null ||
    !Number.isFinite(nowMs) ||
    nowMs < timing.lastActivityMs ||
    (timing.expiresAtMs !== null && nowMs > timing.expiresAtMs) ||
    (options?.ignoreInterval !== true && nowMs < timing.dueAtMs)
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
