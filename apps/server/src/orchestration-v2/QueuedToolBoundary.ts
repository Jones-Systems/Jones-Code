import type { OrchestrationV2ExecutionNode } from "@t3tools/contracts";
import { modelSelectionsEqual } from "@t3tools/shared/model";
import type { ProjectionRuntimeRecoveryState } from "./ProjectionStore.ts";
import { queuedRunsInDeliveryOrder } from "./QueuedRunOrder.ts";

export function queuedToolBoundaryTarget(
  projection: ProjectionRuntimeRecoveryState,
  boundary: OrchestrationV2ExecutionNode,
) {
  const activeRun = projection.runs.find((run) => run.id === boundary.runId);
  const providerTurn = projection.providerTurns.find((turn) => turn.id === boundary.providerTurnId);
  const root = projection.nodes.find((node) => node.id === activeRun?.rootNodeId);
  const current = projection.nodes.find((node) => node.id === boundary.id);
  if (
    projection.thread.archivedAt !== null ||
    projection.thread.deletedAt !== null ||
    activeRun?.status !== "running" ||
    root?.kind !== "root_turn" ||
    root.parentNodeId !== null ||
    root.id !== boundary.rootNodeId ||
    activeRun.providerThreadId !== boundary.providerThreadId ||
    providerTurn?.status !== "running" ||
    providerTurn.nodeId !== root.id ||
    providerTurn.providerThreadId !== activeRun.providerThreadId ||
    providerTurn.runAttemptId !== activeRun.activeAttemptId ||
    projection.thread.activeProviderThreadId !== activeRun.providerThreadId ||
    boundary.kind !== "tool_call" ||
    boundary.status !== "completed" ||
    boundary.nativeItemRef?.nativeId == null ||
    boundary.nativeItemRef.strength !== "strong" ||
    current?.status !== "completed" ||
    current.providerTurnId !== providerTurn.id ||
    current.runId !== activeRun.id ||
    projection.runtimeRequests.some((request) => request.status === "pending") ||
    projection.runs.some((run) => run.status === "queued" && run.queueHeld === true)
  )
    return null;

  const belongsToRoot = (node: OrchestrationV2ExecutionNode): boolean | undefined => {
    if (
      node.runId !== activeRun.id ||
      node.rootNodeId !== root.id ||
      node.providerThreadId !== activeRun.providerThreadId ||
      node.providerTurnId !== providerTurn.id
    )
      return false;
    const seen = new Set([node.id]);
    let parentId = node.parentNodeId;
    while (parentId !== null) {
      if (parentId === root.id) return true;
      if (seen.has(parentId)) return undefined;
      seen.add(parentId);
      const parent = projection.nodes.find((candidate) => candidate.id === parentId);
      if (parent === undefined) return undefined;
      if (parent.kind === "subagent" || parent.kind === "root_turn") return false;
      parentId = parent.parentNodeId;
    }
    return undefined;
  };
  if (
    belongsToRoot(boundary) !== true ||
    projection.nodes.some(
      (node) =>
        node.kind === "tool_call" &&
        ["pending", "running", "waiting"].includes(node.status) &&
        belongsToRoot(node) !== false,
    )
  )
    return null;
  const queuedRun = queuedRunsInDeliveryOrder(projection)[0];
  const message = projection.messages.find(
    (candidate) => candidate.id === queuedRun?.userMessageId,
  );
  const activeMessage = projection.messages.find(
    (candidate) => candidate.id === activeRun.userMessageId,
  );
  const maintenance = (candidate: typeof message) =>
    candidate !== undefined &&
    candidate.attachments.length === 0 &&
    ["/compact", "/logout"].includes(candidate.text.trim().toLowerCase());
  const provider = projection.providerThreads.find(
    (candidate) => candidate.id === activeRun.providerThreadId,
  );
  const session = projection.providerSessions.find(
    (candidate) => candidate.id === provider?.providerSessionId,
  );
  if (
    queuedRun === undefined ||
    message?.createdBy !== "user" ||
    message.notification !== undefined ||
    message.delegatedCompletion !== undefined ||
    message.scheduledTaskId !== undefined ||
    message.senderThreadId !== undefined ||
    queuedRun.sourcePlanRef !== undefined ||
    queuedRun.restartContinuationOfRunId !== undefined ||
    maintenance(message) ||
    maintenance(activeMessage) ||
    queuedRun.providerInstanceId !== activeRun.providerInstanceId ||
    queuedRun.providerThreadId !== activeRun.providerThreadId ||
    projection.thread.providerInstanceId !== activeRun.providerInstanceId ||
    !modelSelectionsEqual(queuedRun.modelSelection, activeRun.modelSelection) ||
    !modelSelectionsEqual(projection.thread.modelSelection, activeRun.modelSelection) ||
    session?.capabilities.turns.supportsActiveSteering !== true ||
    (session.status !== "ready" && session.status !== "running")
  )
    return null;
  return { queuedRun, activeRun, providerTurn };
}
