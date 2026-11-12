import type { OrchestrationV2ThreadProjection, RemoteBlockingQuestions } from "@t3tools/contracts";

/** Only live user-input requests can suspend a delegated task. Foreign IDs stay remote. */
export const blockingQuestions = (
  records: Pick<OrchestrationV2ThreadProjection, "runtimeRequests" | "nodes" | "runs">,
): RemoteBlockingQuestions | undefined => {
  const requests: Array<RemoteBlockingQuestions["requests"][number]> = [];
  for (const request of records.runtimeRequests) {
    if (
      request.kind !== "user_input" ||
      request.status !== "pending" ||
      request.responseCapability.type !== "live"
    )
      continue;
    const runId =
      records.nodes?.find((node) => node.id === request.nodeId)?.runId ??
      records.runs.find((run) => run.rootNodeId === request.nodeId)?.id;
    if (runId === undefined || requests.length >= 64) return undefined;
    requests.push({ requestId: request.id, runId });
  }
  requests.sort((a, b) => a.requestId.localeCompare(b.requestId));
  const resumeToken = JSON.stringify(requests);
  if (resumeToken.length > 16_384) return undefined;
  return { resumeToken, requests };
};

export const questionsChanged = (
  questions: RemoteBlockingQuestions,
  resumeToken: string | undefined,
) =>
  resumeToken === undefined ? questions.requests.length > 0 : questions.resumeToken !== resumeToken;
