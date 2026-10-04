import { EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as McpSessionRegistry from "./McpSessionRegistry.ts";

export const layer = Layer.succeed(
  McpSessionRegistry.McpSessionRegistry,
  McpSessionRegistry.McpSessionRegistry.of({
    issue: ({ threadId, providerInstanceId, capabilities, browserToolsAvailable }) =>
      Effect.succeed({
        config: {
          environmentId: EnvironmentId.make("environment:mcp-test"),
          threadId,
          providerSessionId: `mcp-test:${threadId}`,
          providerInstanceId,
          endpoint: "http://127.0.0.1/mcp",
          authorizationHeader: `Bearer mcp-test:${threadId}`,
          capabilities:
            capabilities ??
            new Set([
              "orchestration",
              "worktree",
              "pull-requests",
              ...(browserToolsAvailable === false ? [] : ["preview"]),
            ]),
          browserToolsAvailable: capabilities?.has("preview") ?? browserToolsAvailable ?? true,
        },
      }),
    resolve: () => Effect.succeed(undefined),
    touch: () => Effect.void,
    revokeProviderSession: () => Effect.void,
    revokeThread: () => Effect.void,
    revokeAll: Effect.void,
  }),
);
