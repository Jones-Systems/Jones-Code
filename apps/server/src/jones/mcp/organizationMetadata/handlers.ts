import * as Effect from "effect/Effect";
import { requireMcpCapability, requireThreadScope } from "../../../mcp/McpInvocationContext.ts";
import * as McpToolAccess from "../../../mcp/McpToolAccess.ts";
import * as OrganizationMetadata from "./OrganizationMetadataMcpService.ts";
import { OrganizationMetadataToolkit } from "./tools.ts";

export const OrganizationMetadataHandlersLive = McpToolAccess.toLayer(OrganizationMetadataToolkit, {
  get_invocation_context: McpToolAccess.readsAsCaller(() =>
    Effect.gen(function* () {
      const caller = yield* requireMcpCapability("orchestration");
      const scope = yield* requireThreadScope(caller, "get_invocation_context");
      const service = yield* OrganizationMetadata.OrganizationMetadataMcpService;
      return yield* service.getInvocationContext({
        environmentId: scope.environmentId,
        threadId: scope.thread.threadId,
      });
    }),
  ),
  list_organization_thread_metadata: McpToolAccess.reads((input) =>
    Effect.gen(function* () {
      const caller = yield* requireMcpCapability("orchestration");
      const service = yield* OrganizationMetadata.OrganizationMetadataMcpService;
      return yield* service.listThreadMetadata(caller.environmentId, input);
    }),
  ),
});
