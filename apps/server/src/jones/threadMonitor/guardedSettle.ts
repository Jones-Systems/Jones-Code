import type { OrchestrationV2TurnItem, ThreadId } from "@t3tools/contracts";
import { isOrchestrationV2WorkActive } from "@t3tools/contracts";
import { threadPullRequestsOf } from "@t3tools/shared/threadPullRequests";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import type { ProjectionStoreV2Shape } from "../../orchestration-v2/ProjectionStore.ts";
import type { RecordedThreadProjection } from "../../orchestration-v2/RecordedTypes.ts";

const fields = [
  "runs",
  "runtimeRequests",
  "plans",
  "providerThreads",
  "providerSessions",
  "subagents",
  "turnItems",
] as const;
export type GuardedSettlementRecords = Pick<
  RecordedThreadProjection,
  "thread" | (typeof fields)[number]
>;
export type MonitorSettlementBlocker =
  | "stale"
  | "request"
  | "run"
  | "plan"
  | "goal"
  | "background"
  | "child"
  | "unavailable";
const executingItems = new Set<OrchestrationV2TurnItem["type"]>([
  "command_execution",
  "dynamic_tool",
  "subagent",
  "file_change",
  "file_search",
  "web_search",
  "compaction",
]);
const terminalTask = (status: string) =>
  ["completed", "failed", "cancelled", "interrupted"].includes(status);

export function monitorSettlementBlocker(
  records: GuardedSettlementRecords,
  expectedUpdatedAt?: string,
): MonitorSettlementBlocker | null {
  if (
    expectedUpdatedAt !== undefined &&
    DateTime.toEpochMillis(records.thread.updatedAt) !== Date.parse(expectedUpdatedAt)
  )
    return "stale";
  if (records.runtimeRequests.some((request) => request.status === "pending")) return "request";
  if (
    records.runs.some((run) =>
      ["preparing", "queued", "starting", "running", "waiting"].includes(run.status),
    )
  )
    return "run";
  if (records.plans.some((plan) => plan.kind === "proposed_plan" && plan.status === "active"))
    return "plan";
  if (
    records.providerThreads.some(
      (thread) => thread.goal != null && thread.goal.status !== "complete",
    )
  )
    return "goal";
  if (
    records.providerThreads.some((thread) => (thread.pendingBackgroundTasks?.length ?? 0) > 0) ||
    records.providerSessions.some(
      (session) =>
        session.status === "starting" ||
        session.status === "running" ||
        session.status === "waiting",
    ) ||
    threadPullRequestsOf(records.thread).some((link) => link.watch !== undefined) ||
    records.turnItems.some(
      (item) => executingItems.has(item.type) && isOrchestrationV2WorkActive(item.status),
    )
  )
    return "background";
  if (
    records.subagents.some(
      (task) =>
        !terminalTask(task.status) ||
        task.workflow?.agents.some(
          (agent) => agent.state === "queued" || agent.state === "running",
        ) ||
        task.completionDelivery?.state === "pending" ||
        task.completionDelivery?.state === "claimed",
    )
  )
    return "child";
  return null;
}

/** Called only inside the orchestrator's existing thread dispatch lock, before settle effects. */
export const readMonitorSettlementBlocker = Effect.fn("ThreadMonitor.readSettlementBlocker")(
  function* (
    store: Pick<ProjectionStoreV2Shape, "getThreadRecords">,
    threadId: ThreadId,
    expectedUpdatedAt: string,
  ) {
    const records = yield* store.getThreadRecords(threadId, fields, {
      turnItemTypes: [...executingItems],
      turnItemStatuses: ["pending", "running", "waiting"],
    });
    const blocker = monitorSettlementBlocker(records, expectedUpdatedAt);
    if (blocker !== null) return blocker;
    const seen = new Set<ThreadId>([threadId]);
    const pending = records.subagents.map((task) => ({
      task,
      ancestors: new Set<ThreadId>([threadId]),
    }));
    // Bounded descendant coverage: incomplete or cross-environment evidence never permits settle.
    while (pending.length > 0) {
      const { task, ancestors } = pending.pop()!;
      if (task.remoteChild !== undefined && task.remoteChild.stoppedThere !== true)
        return "unavailable" as const;
      if (task.childThreadId === null) continue;
      if (ancestors.has(task.childThreadId)) return "unavailable" as const;
      if (seen.has(task.childThreadId)) continue;
      if (seen.size >= 64) return "unavailable" as const;
      seen.add(task.childThreadId);
      const child = yield* store.getThreadRecords(task.childThreadId, fields, {
        turnItemTypes: [...executingItems],
        turnItemStatuses: ["pending", "running", "waiting"],
      });
      if (
        child.thread.lineage.relationshipToParent !== "subagent" ||
        child.thread.lineage.parentThreadId !== task.threadId
      )
        return "unavailable" as const;
      if (monitorSettlementBlocker(child) !== null) return "child" as const;
      // Unsettled children can receive independent automatic work outside the parent's lock.
      // Require their settled lifecycle boundary rather than promising a cross-thread transaction.
      if (
        child.thread.deletedAt === null &&
        child.thread.archivedAt === null &&
        child.thread.settledAt === null
      )
        return "unavailable" as const;
      const childAncestors = new Set(ancestors).add(task.childThreadId);
      pending.push(
        ...child.subagents.map((descendant) => ({ task: descendant, ancestors: childAncestors })),
      );
    }
    return null;
  },
);

export const monitorSettlementBlockerMessage: Record<MonitorSettlementBlocker, string> = {
  stale: "Thread changed after the monitor snapshot. Refresh before settling.",
  request: "Resolve every pending request before settling.",
  run: "Thread has active or queued work, including automatic work.",
  plan: "Thread has an actionable plan.",
  goal: "Thread has an unfinished goal.",
  background: "Thread has background work or a pull request watch.",
  child: "Thread has unfinished child or delegated work.",
  unavailable: "Complete child-work coverage is unavailable. Manage this thread in Jones Code.",
};
