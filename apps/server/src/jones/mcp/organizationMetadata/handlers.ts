import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { McpServer } from "effect/unstable/ai";
import { requireMcpCapability } from "../../../mcp/McpInvocationContext.ts";
import * as OrganizationMetadata from "./OrganizationMetadataMcpService.ts";
import { OrganizationMetadataToolkit } from "./tools.ts";

export const OrganizationMetadataHandlersLive = OrganizationMetadataToolkit.toLayer({
  get_invocation_context: () =>
    Effect.gen(function* () {
      const caller = yield* requireMcpCapability("orchestration");
      const service = yield* OrganizationMetadata.OrganizationMetadataMcpService;
      return yield* service.getInvocationContext(caller);
    }),
  list_organization_thread_metadata: (input) =>
    Effect.gen(function* () {
      const caller = yield* requireMcpCapability("orchestration");
      const service = yield* OrganizationMetadata.OrganizationMetadataMcpService;
      return yield* service.listThreadMetadata(caller.environmentId, input);
    }),
});
export const OrganizationMetadataRegistrationLive = McpServer.toolkit(
  OrganizationMetadataToolkit,
).pipe(Layer.provide(OrganizationMetadataHandlersLive), Layer.provide(OrganizationMetadata.layer));
