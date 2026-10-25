import {
  type EnvironmentId,
  OrchestratorMcpCapabilitiesInput,
  OrchestratorMcpCapabilitiesResult,
  OrchestratorMcpFailure,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { Tool } from "effect/ai";
import type { McpThreadInvocationScope } from "../../mcp/McpInvocationContext.ts";
import type { PeerForwarding } from "../../peer/PeerForwarding.ts";

export const version = "t3.remote-task-observation/v1";

const capabilities = Tool.make("orchestrator_capabilities", {
  parameters: OrchestratorMcpCapabilitiesInput,
  success: OrchestratorMcpCapabilitiesResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
});

export const supported = (
  forwarding: Pick<PeerForwarding["Service"], "call">,
  scope: McpThreadInvocationScope,
  environmentId: EnvironmentId,
) =>
  forwarding
    .call(scope, capabilities, environmentId, {})
    .pipe(Effect.map((result) => result.features.remoteTaskObservationVersion === version));

export const unsupported = () =>
  new OrchestratorMcpFailure({
    code: "capability_denied",
    message:
      "Remote task observation is unsupported by this linked environment. Upgrade the receiving server before delegating new work.",
  });
