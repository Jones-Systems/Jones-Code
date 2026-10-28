import { EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as McpSessionRegistry from "./McpSessionRegistry.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";

export const layer = Layer.merge(
  Layer.succeed(
    McpSessionRegistry.McpSessionRegistry,
    McpSessionRegistry.McpSessionRegistry.of({
      issue: ({ threadId, providerInstanceId }) =>
        Effect.succeed({
          config: {
            environmentId: EnvironmentId.make("environment:mcp-test"),
            threadId,
            providerSessionId: `mcp-test:${threadId}`,
            providerInstanceId,
            endpoint: "http://127.0.0.1/mcp",
            authorizationHeader: `Bearer mcp-test:${threadId}`,
            browserToolsAvailable: true,
          },
        }),
      // @effect-diagnostics-next-line effectSucceedWithVoid:off - This optional result requires undefined, not the wider void type.
      resolve: () => Effect.succeed(undefined),
      touch: () => Effect.void,
      revokeProviderSession: () => Effect.void,
      revokeThread: () => Effect.void,
      revokeAll: Effect.void,
    }),
  ),
  Layer.succeed(ServerEnvironment.ServerEnvironmentIdentity, {
    getEnvironmentId: Effect.succeed(EnvironmentId.make("environment:mcp-test")),
  }),
);
