import * as InProcessMcp from "../../../mcp/McpHttpServer.ts";
import { expect, it } from "@effect/vitest";
import { EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { McpSchema, McpServer } from "effect/ai";
import * as McpHttpServer from "../../../mcp/McpHttpServer.ts";
import * as Invocation from "../../../mcp/McpInvocationContext.ts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";

import * as Personal from "../../workstreams/personal/service.ts";
import * as Agent from "../../workstreams/personal/agentService.ts";
const invocation: Invocation.McpInvocationScope = {
  environmentId: EnvironmentId.make("env-test"),
  thread: undefined,
  client: { sessionId: "workstreams-test", label: "test", access: "full-access" },
  capabilities: new Set(["orchestration"]),
  issuedAt: 0,
  requestNamespace: "workstreams-test",
};
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  clientCapabilities: {},
  clientInfo: { name: "workstreams-test", version: "1.0.0" },
  protocolVersion: "2025-06-18",
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "workstreams-test", version: "1.0.0" },
  },
  getClient: Effect.die("unused"),
});

it.effect(
  "production MCP registration uses the personal application read port without a UI",
  () => {
    const snapshot = {
      environmentId: EnvironmentId.make("env-test"),
      sequence: 3,
      workstreams: [],
      assignments: [],
      importIssue: null,
    };
    let reads = 0;
    const registration = McpHttpServer.layerWorkstreamToolkit.pipe(
      Layer.provideMerge(InProcessMcp.layerInProcessMcpServer),
      Layer.provide(
        Layer.mock(Personal.PersonalWorkstreams)({
          snapshot: () =>
            Effect.sync(() => {
              reads++;
              return snapshot;
            }),
        }),
      ),
      Layer.provide(Layer.mock(Agent.AgentWorkstreams)({})),
    );
    return Effect.gen(function* () {
      const server = yield* McpServer.McpServer;
      expect(server.tools.map(({ tool }) => tool.name).sort()).toEqual([
        "t3_thread_workstream_read",
        "t3_thread_workstream_set",
        "t3_workstream_create",
        "t3_workstream_update",
        "t3_workstreams_list",
      ]);
      const listed = yield* server
        .callTool({ name: "t3_workstreams_list", arguments: {} })
        .pipe(
          Effect.provideService(Invocation.McpInvocationContext, invocation),
          Effect.provideService(McpSchema.McpServerClient, client),
        );
      expect(listed.isError).toBe(false);
      expect(listed.structuredContent).toEqual({
        environmentId: snapshot.environmentId,
        sequence: 3,
        workstreams: [],
      });
      expect(reads).toBe(1);
    }).pipe(
      Effect.provide(
        registration.pipe(
          Layer.provideMerge(
            Layer.mock(ThreadManagementService)({ getThreadShell: () => Effect.succeed(null) }),
          ),
        ),
      ),
    );
  },
);
