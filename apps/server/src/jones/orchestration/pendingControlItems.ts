import type { OrchestrationV2ThreadProjection } from "@t3tools/contracts";

export function pendingControlRequests(
  projection: Pick<OrchestrationV2ThreadProjection, "runtimeRequests">,
) {
  return projection.runtimeRequests.flatMap((request) =>
    request.status !== "pending" ||
    request.kind === "auth_refresh" ||
    request.kind === "dynamic_tool_call"
      ? []
      : [
          {
            id: request.id,
            nodeId: request.nodeId,
            type: request.kind === "user_input" ? "user_input_request" : "approval_request",
          },
        ],
  );
}

export function pendingControlTurnItems(
  projection: Pick<OrchestrationV2ThreadProjection, "thread" | "runtimeRequests" | "turnItems">,
) {
  const requests = new Map(
    pendingControlRequests(projection).map((request) => [request.id, request]),
  );
  // Async question items can be completed while their runtime request is still pending.
  return projection.turnItems.filter((item) => {
    if (item.type !== "user_input_request" && item.type !== "approval_request") return false;
    const request = requests.get(item.requestId);
    return (
      item.threadId === projection.thread.id &&
      request?.type === item.type &&
      request.nodeId === item.nodeId
    );
  });
}
