import * as Effect from "effect/Effect";
import { requireMcpCapability, requireThreadScope } from "../../../mcp/McpInvocationContext.ts";
import * as McpToolAccess from "../../../mcp/McpToolAccess.ts";
import * as UserResponseRequest from "./UserResponseRequestMcpService.ts";
import { UserResponseRequestToolkit } from "./tools.ts";

export const handlers = McpToolAccess.toLayer(UserResponseRequestToolkit, {
  t3_request_user_response: McpToolAccess.actsAsCaller((input) =>
    Effect.gen(function* () {
      const caller = yield* requireMcpCapability("orchestration");
      const scope = yield* requireThreadScope(caller, "t3_request_user_response");
      const service = yield* UserResponseRequest.UserResponseRequestMcpService;
      return yield* service.request(scope, input);
    }),
  ),
});
