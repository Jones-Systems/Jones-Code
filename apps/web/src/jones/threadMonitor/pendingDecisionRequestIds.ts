import type { OrchestrationV2ThreadProjection, RuntimeRequestId } from "@t3tools/contracts";

export function pendingDecisionRequestIds(
  projection: Pick<OrchestrationV2ThreadProjection, "runtimeRequests">,
): ReadonlySet<RuntimeRequestId> {
  return new Set(
    projection.runtimeRequests
      .filter((request) => request.status === "pending")
      .map((request) => request.id),
  );
}
