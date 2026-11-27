import { DateTime } from "effect";
import { isListedThreadLineage } from "@t3tools/contracts";
import {
  threadRuntimeIsActive,
  type EnvironmentThreadShell,
} from "@t3tools/client-runtime/state/shell";
import { hasQueuedTurnStart } from "@t3tools/client-runtime/state/thread-settled";

export const FINISHED_WINDOW_MS = 60 * 60 * 1000;
export type OrchestratorQueue = "done" | "input";
export type OrchestratorThreadRef = {
  readonly environmentId: string;
  readonly threadId: string;
};

export function orchestratorThreadRef(thread: EnvironmentThreadShell): OrchestratorThreadRef {
  return { environmentId: thread.environmentId, threadId: thread.id };
}

export function sameOrchestratorThread(
  left: OrchestratorThreadRef,
  right: OrchestratorThreadRef,
): boolean {
  return left.environmentId === right.environmentId && left.threadId === right.threadId;
}

export function isOrchestratorConversation(thread: EnvironmentThreadShell): boolean {
  return (
    thread.archivedAt === null &&
    thread.deletedAt === null &&
    isListedThreadLineage(thread.lineage) &&
    thread.delegatedFrom === null &&
    thread.workflowRun === null
  );
}

export function orchestratorNeedsInput(thread: EnvironmentThreadShell): boolean {
  return (
    thread.hasPendingApprovals ||
    thread.hasPendingUserInput ||
    thread.pendingSubagentRequest != null
  );
}

export function orchestratorNeedsApproval(thread: EnvironmentThreadShell): boolean {
  return thread.hasPendingApprovals || thread.pendingSubagentRequest === "approval";
}

export function finishedAt(thread: EnvironmentThreadShell): number | null {
  const completedAt = thread.source.latestRunCompletedAt;
  return thread.source.latestRunId != null && completedAt != null
    ? DateTime.toEpochMillis(completedAt)
    : null;
}

export function nextFinishedExpiry(threads: readonly EnvironmentThreadShell[], now: number) {
  let next = Infinity;
  for (const thread of threads) {
    const at = finishedAt(thread);
    if (at === null || !isOrchestratorConversation(thread)) continue;
    const expiry = at + FINISHED_WINDOW_MS;
    if (expiry > now && expiry < next) next = expiry;
  }
  return Number.isFinite(next) ? next : null;
}

export function orchestratorQueues(
  threads: readonly EnvironmentThreadShell[],
  now: number,
  hasQueuedMessages: (thread: EnvironmentThreadShell) => boolean,
) {
  const input: Array<{ thread: EnvironmentThreadShell; at: number }> = [];
  const done: Array<{ thread: EnvironmentThreadShell; at: number }> = [];
  const nowIso = new Date(now).toISOString();
  for (const thread of threads) {
    if (!isOrchestratorConversation(thread)) continue;
    if (orchestratorNeedsInput(thread)) {
      const request = thread.source.pendingRuntimeRequest;
      const at = request ? DateTime.toEpochMillis(request.createdAt) : Date.parse(thread.updatedAt);
      input.push({ thread, at });
      continue;
    }
    if (
      thread.settledOverride === "settled" ||
      threadRuntimeIsActive(thread.runtime) ||
      thread.source.activeRunId !== null ||
      thread.blockedOnThreadWait === true ||
      hasQueuedMessages(thread) ||
      hasQueuedTurnStart(thread, { now: nowIso })
    )
      continue;
    const at = finishedAt(thread);
    if (at !== null && at > now - FINISHED_WINDOW_MS && at <= now) done.push({ thread, at });
  }
  const newestFirst = (rows: typeof input) =>
    rows
      .sort(
        (a, b) =>
          b.at - a.at ||
          a.thread.environmentId.localeCompare(b.thread.environmentId) ||
          a.thread.id.localeCompare(b.thread.id),
      )
      .map(({ thread }) => thread);
  return { input: newestFirst(input), done: newestFirst(done) };
}

/** Follow the live queue. If the current row left it, use its former neighbors as anchors. */
export function adjacentOrchestratorThread(
  current: OrchestratorThreadRef,
  live: readonly OrchestratorThreadRef[],
  entryOrder: readonly OrchestratorThreadRef[],
  direction: 1 | -1,
): OrchestratorThreadRef | undefined {
  const liveIndex = live.findIndex((ref) => sameOrchestratorThread(ref, current));
  if (liveIndex !== -1) return live[liveIndex + direction];
  const entryIndex = entryOrder.findIndex((ref) => sameOrchestratorThread(ref, current));
  if (entryIndex === -1) return undefined;
  for (
    let index = entryIndex + direction;
    index >= 0 && index < entryOrder.length;
    index += direction
  ) {
    const candidate = entryOrder[index]!;
    if (live.some((ref) => sameOrchestratorThread(ref, candidate))) return candidate;
  }
  return undefined;
}
