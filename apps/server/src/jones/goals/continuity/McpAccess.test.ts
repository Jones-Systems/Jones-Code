import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  OrchestratorMcpFailure,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { McpSchema, McpServer, Tool, Toolkit } from "effect/ai";
import * as McpHttpServer from "../../../mcp/McpHttpServer.ts";
import * as Invocation from "../../../mcp/McpInvocationContext.ts";
import * as Access from "../../../mcp/McpToolAccess.ts";
import { liveThreadShell } from "../../../mcp/McpToolAccess.testkit.ts";
import * as Threads from "../../../orchestration-v2/ThreadManagementService.ts";

const threadId = ThreadId.make("thread:continuity-access");
const shell = liveThreadShell(threadId, { activeRunId: null });
const Probe = Toolkit.make(
  Tool.make("continuity_access_probe", {
    parameters: Schema.Struct({
      action: Schema.Literals(["read", "checkpoint"]),
      threadId: Schema.optional(ThreadId),
    }),
    success: Schema.Struct({ ran: Schema.String }),
    failure: OrchestratorMcpFailure,
    failureMode: "return",
    dependencies: [Invocation.McpInvocationContext, Threads.ThreadManagementService],
  }),
);
const handlers = Access.toLayer(Probe, {
  continuity_access_probe: Access.readsOrWritesThreads(
    (input) => input.action === "read",
    (input) => [input.threadId],
    (input) => Effect.succeed({ ran: input.action }),
  ),
});
const Server = McpHttpServer.toolkitRegistration(Probe, handlers).pipe(
  Layer.provideMerge(McpServer.McpServer.layer),
  Layer.provideMerge(
    Layer.mock(Threads.ThreadManagementService)({
      getThreadShell: () => Effect.succeed(shell),
    }),
  ),
);
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "continuity-access-test", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "continuity-access-test", version: "1" },
  },
  getClient: Effect.die("unused"),
});
const scope = (access: "read-only" | "full-access"): Invocation.McpInvocationScope => ({
  environmentId: EnvironmentId.make("synthetic-environment"),
  requestNamespace: "client:continuity-access",
  issuedAt: 0,
  capabilities: new Set(["orchestration"]),
  thread: undefined,
  client: { sessionId: "synthetic-client", label: "Owner", access },
});
const endedAgent: Invocation.McpInvocationScope = {
  ...scope("full-access"),
  client: undefined,
  thread: {
    threadId,
    providerSessionId: "synthetic-attachment",
    providerInstanceId: ProviderInstanceId.make("codex"),
  },
};
const decode = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Union([Schema.Struct({ ran: Schema.String }), Schema.Struct({ code: Schema.String })]),
  ),
);
const call = (caller: Invocation.McpInvocationScope, action: "read" | "checkpoint") =>
  McpServer.McpServer.pipe(
    Effect.flatMap((server) =>
      server.callTool({ name: "continuity_access_probe", arguments: { action, threadId } }),
    ),
    Effect.flatMap(({ content }) =>
      decode(content.map((part) => (part.type === "text" ? part.text : "")).join("")),
    ),
    Effect.map((result) => ("code" in result ? result.code : result.ran)),
    Effect.provideService(Invocation.McpInvocationContext, caller),
    Effect.provideService(McpSchema.McpServerClient, client),
    Effect.provide(Server),
  );

it.effect.each([
  [scope("read-only"), "read", "read"],
  [scope("read-only"), "checkpoint", "capability_denied"],
  [scope("full-access"), "checkpoint", "checkpoint"],
  [endedAgent, "read", "read"],
  [endedAgent, "checkpoint", "parent_not_active"],
  [
    {
      ...scope("full-access"),
      client: {
        ...scope("full-access").client!,
        narrowedTo: { runtimeMode: "approval-required", interactionMode: "default" },
      },
    },
    "checkpoint",
    "runtime_mode_escalation_denied",
  ],
] as const)("operation-aware access %o %s: %s", ([caller, action, expected]) =>
  call(caller, action).pipe(Effect.map((result) => expect(result).toBe(expected))),
);
