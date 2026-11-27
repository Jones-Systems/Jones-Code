import type { OrchestratorThreadRef } from "./queues";

export const ORCHESTRATOR_TEXT_DRAFT_KEY = "mobile-orchestrator:draft";

export function orchestratorOptionsKey(target: OrchestratorThreadRef): string {
  return JSON.stringify(["mobile-orchestrator-options", target.environmentId, target.threadId]);
}

export function orchestratorRequest(text: string): string {
  return (
    "Handle the following owner request as the Jones Code Orchestrator. " +
    "Use your native thread and Workstream tools to resolve the intended destination from current evidence. " +
    "Reuse the appropriate conversation when possible and choose the appropriate project, environment, " +
    "and Workstream for requested new work. Observe the usual authority and approval rules. " +
    "If the intended destination is materially ambiguous, request input here. " +
    "Report where the request went or what prevented routing.\n\n" +
    text
  );
}
