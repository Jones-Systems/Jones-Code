import type {
  MessageId,
  OrchestrationV2ConversationMessage,
  OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import { formatThreadTitleContext } from "@t3tools/provider-core/server/threadTitleContext";
import { isNativeMaintenanceCommand } from "../../orchestration-v2/CommandPolicy.ts";

type EvidenceProjection = Pick<OrchestrationV2ThreadProjection, "messages" | "runs">;

export function eligibleOwnerMessages(projection: EvidenceProjection) {
  const rolledBack = new Set(
    projection.runs.filter((run) => run.status === "rolled_back").map((run) => run.id),
  );
  const seen = new Set<MessageId>();
  return projection.messages.filter((message) => {
    if (seen.has(message.id)) return false;
    seen.add(message.id);
    return (
      message.role === "user" &&
      message.createdBy === "user" &&
      message.creationSource !== "server" &&
      message.creationSource !== "provider" &&
      message.scheduledTaskId === undefined &&
      message.notification === undefined &&
      message.delegatedCompletion === undefined &&
      message.workflowScript !== true &&
      message.senderThreadId === undefined &&
      !isNativeMaintenanceCommand(message) &&
      (message.runId === null || !rolledBack.has(message.runId))
    );
  });
}

type ContextProjection = Pick<
  OrchestrationV2ThreadProjection,
  "messages" | "runs" | "nodes" | "turnItems"
>;

function latestRootReply(
  projection: ContextProjection,
): OrchestrationV2ConversationMessage | undefined {
  const run = projection.runs
    .filter((run) => run.status === "completed")
    .toSorted((a, b) => b.ordinal - a.ordinal)[0];
  if (!run?.rootNodeId || !run.providerThreadId) return undefined;
  const nodes = new Map(projection.nodes.map((node) => [node.id, node]));
  const messages = new Map(projection.messages.map((message) => [message.id, message]));
  // Messages have their own nodes in native Codex/Claude turns. Root equality alone
  // misses those; walk ancestry and reject delegated/provider-child branches.
  for (const item of projection.turnItems.toSorted((a, b) => b.ordinal - a.ordinal)) {
    if (
      item.type !== "assistant_message" ||
      item.runId !== run.id ||
      item.providerThreadId !== run.providerThreadId
    )
      continue;
    const message = messages.get(item.messageId);
    if (!message || message.streaming || message.role !== "assistant" || message.runId !== run.id)
      continue;
    let id = message.nodeId;
    const seen = new Set();
    while (id !== null && !seen.has(id)) {
      seen.add(id);
      const node = nodes.get(id);
      if (
        !node ||
        node.runId !== run.id ||
        node.kind === "subagent" ||
        (node.providerThreadId !== null && node.providerThreadId !== run.providerThreadId)
      )
        break;
      if (node.id === run.rootNodeId && node.kind === "root_turn") return message;
      id = node.parentNodeId;
    }
  }
  return undefined;
}

export function automaticTitleContext(
  projection: ContextProjection,
  evidence: ReadonlyArray<MessageId>,
) {
  const ids = new Set(evidence.slice(0, 3));
  const eligible = new Map(
    eligibleOwnerMessages(projection).map((message) => [message.id, message]),
  );
  const owners = [...ids].flatMap((id) => {
    const message = eligible.get(id);
    return message ? [message] : [];
  });
  const reply = latestRootReply(projection);
  // Nonstreaming is only a bounded fallback from a completed root run, not a
  // claim that the provider emitted a final-channel answer. Never wait for one.
  return formatThreadTitleContext([...owners, ...(reply ? [reply] : [])]);
}
