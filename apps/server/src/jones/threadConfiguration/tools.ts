import * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";
import {
  ThreadSelfConfigurationInput,
  ThreadSelfConfigurationResult,
  OrchestratorMcpFailure,
} from "@t3tools/contracts";
import { Tool, Toolkit } from "effect/ai";
import * as Effect from "effect/Effect";
import * as McpToolAccess from "../../mcp/McpToolAccess.ts";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import * as Service from "./ThreadSelfConfiguration.ts";

const tool = Tool.make("t3_thread_self_configure", {
  description:
    "Save this calling thread's provider, model and options without changing its current run. First read t3_thread_configuration and pass its configuration revision and execution precondition. Options replace all explicit overrides; omitted options use provider defaults. Applies to subsequently admitted runs that inherit this selection; this operation does not alter the current run or already captured runs. Retry the identical request with the same clientRequestId.",
  parameters: ThreadSelfConfigurationInput,
  success: ThreadSelfConfigurationResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies: [
    Invocation.McpInvocationContext,
    Service.ThreadSelfConfiguration,
    ThreadManagement.ThreadManagementService,
  ],
}).annotate(Tool.Destructive, false);
export const ThreadSelfConfigurationToolkit = Toolkit.make(tool);
export const layer = McpToolAccess.toLayer(ThreadSelfConfigurationToolkit, {
  t3_thread_self_configure: McpToolAccess.actsAsCallerWithReceipt((input) =>
    Effect.gen(function* () {
      const scope = yield* Invocation.McpInvocationContext;
      const service = yield* Service.ThreadSelfConfiguration;
      return yield* service.save(input, scope).pipe(Effect.mapError(toMcpFailure));
    }),
  ),
});
export const toMcpFailure = (error: Service.ThreadSelfConfigurationError) =>
  new OrchestratorMcpFailure({
    code: "invalid_request",
    message: `Self-configuration refused: ${error.reason}. Read t3_thread_configuration before a new request; retry an accepted request with its original inputs.`,
  });
