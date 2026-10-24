import type {
  OrchestrationV2ProviderThread,
  OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";

export type ProviderSwitchBeforeStartEvidence = Pick<
  OrchestrationV2ThreadProjection,
  "runs" | "attempts" | "nodes" | "providerThreads" | "providerTurns" | "turnItems"
>;

export function interruptedPendingProviderThread(
  projection: Pick<OrchestrationV2ThreadProjection, "thread" | "providerSessions">,
  evidence: ProviderSwitchBeforeStartEvidence | undefined,
): OrchestrationV2ProviderThread | undefined {
  if (evidence === undefined || projection.providerSessions.length !== 0) return;
  const pending = evidence.providerThreads[0];
  if (
    evidence.providerThreads.length !== 1 ||
    pending === undefined ||
    pending.id !== projection.thread.activeProviderThreadId ||
    pending.appThreadId !== projection.thread.id ||
    pending.ownerNodeId !== null ||
    pending.providerInstanceId !== projection.thread.modelSelection.instanceId ||
    pending.status !== "not_loaded" ||
    pending.driver !== "codex" ||
    projection.thread.providerInstanceId !== projection.thread.modelSelection.instanceId ||
    pending.nativeThreadRef !== null ||
    pending.nativeConversationHeadRef !== null ||
    pending.runtimeIdentity !== undefined ||
    pending.forkedFrom !== null ||
    pending.handoffIds.length !== 0 ||
    pending.goal != null ||
    (pending.pendingBackgroundTasks?.length ?? 0) !== 0 ||
    pending.nativeMetadata != null ||
    pending.contextUsage != null ||
    evidence.providerTurns.length !== 0 ||
    evidence.runs.length === 0 ||
    evidence.attempts.length !== evidence.runs.length ||
    evidence.nodes.length !== evidence.runs.length ||
    evidence.turnItems.some((item) => item.nativeItemRef !== null || item.providerTurnId !== null)
  )
    return;

  // Null cursors alone are unknown: each run needs the app's recorded prestart interruption.
  const allInterruptedBeforeStart = evidence.runs.every((run) => {
    const attempt = evidence.attempts.find((candidate) => candidate.id === run.activeAttemptId);
    const node = evidence.nodes.find((candidate) => candidate.id === run.rootNodeId);
    return (
      run.threadId === projection.thread.id &&
      run.providerInstanceId === pending.providerInstanceId &&
      run.providerThreadId === pending.id &&
      run.status === "interrupted" &&
      run.startedAt === null &&
      run.completedAt !== null &&
      attempt !== undefined &&
      attempt.runId === run.id &&
      attempt.providerInstanceId === pending.providerInstanceId &&
      attempt.providerThreadId === pending.id &&
      attempt.rootNodeId === run.rootNodeId &&
      attempt.reason === "initial" &&
      attempt.attemptOrdinal === 1 &&
      attempt.status === "interrupted" &&
      attempt.startedAt === null &&
      attempt.completedAt !== null &&
      attempt.providerTurnId === null &&
      attempt.nativeThreadId === undefined &&
      attempt.providerSettlement == null &&
      node !== undefined &&
      node.runId === run.id &&
      node.kind === "root_turn" &&
      node.parentNodeId === null &&
      node.providerThreadId === pending.id &&
      node.status === "interrupted" &&
      node.startedAt === null &&
      node.completedAt !== null &&
      node.threadId === run.threadId &&
      node.rootNodeId === node.id &&
      node.providerTurnId === null &&
      node.nativeItemRef === null &&
      evidence.turnItems.some(
        (item) =>
          item.type === "run_interrupt_result" &&
          item.threadId === run.threadId &&
          item.runId === run.id &&
          item.nodeId === run.rootNodeId &&
          item.providerThreadId === pending.id &&
          item.status === "interrupted" &&
          item.completedAt !== null &&
          item.message === "Run interrupted before provider start",
      )
    );
  });
  return allInterruptedBeforeStart ? pending : undefined;
}
