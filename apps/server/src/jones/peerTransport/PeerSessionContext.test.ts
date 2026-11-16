import { NodeHttpServer } from "@effect/platform-node";
import { OrchestratorMcpFailure } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/ai";

import * as McpHttpServer from "../../mcp/McpHttpServer.ts";
import * as McpInvocationContext from "../../mcp/McpInvocationContext.ts";
import * as McpToolAccess from "../../mcp/McpToolAccess.ts";
import * as PeerLinks from "../../peer/PeerLinks.ts";
import {
  descriptorOf,
  layerLinkingEnvironment,
  linkTo,
  servePeer,
} from "../../peer/PeerLinks.testkit.ts";
import * as PeerMcpClient from "../../peer/PeerMcpClient.ts";

const Observed = Schema.Struct({ modes: Schema.String, origin: Schema.String });
const toolkit = Toolkit.make(
  Tool.make("context_probe", {
    description: "Reports the authenticated request context.",
    success: Observed,
    failure: OrchestratorMcpFailure,
    failureMode: "return",
    dependencies: [McpInvocationContext.McpInvocationContext],
  }),
);
const handlers = McpToolAccess.toLayer(toolkit, {
  context_probe: McpToolAccess.reads(() =>
    McpInvocationContext.McpInvocationContext.pipe(
      Effect.map((scope) => {
        const modes = McpInvocationContext.clientModeCeiling(scope.client);
        return {
          modes: `${modes.runtimeMode}/${modes.interactionMode}`,
          origin: scope.client?.remoteOrigin?.thread?.threadId ?? "absent",
        };
      }),
    ),
  ),
});

it.effect("one actual MCP session enforces each call's limits and origin independently", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const target = descriptorOf("context-target", "Target");
      const peer = yield* servePeer(target, McpHttpServer.toolkitRegistration(toolkit, handlers));
      const context = yield* layerLinkingEnvironment(descriptorOf("context-source", "Source")).pipe(
        Layer.build,
      );
      yield* linkTo(Context.get(context, PeerLinks.PeerLinks), peer, "full-access");
      const client = Context.get(context, PeerMcpClient.PeerMcpClient);
      const call = (runtimeMode: "full-access" | "approval-required", threadId?: string) =>
        client.call({
          environmentId: target.environmentId,
          tool: "context_probe",
          arguments: {},
          limits: {
            runtimeMode,
            interactionMode: runtimeMode === "full-access" ? "default" : "plan",
          },
          ...(threadId === undefined
            ? {}
            : {
                origin: {
                  schemaVersion: "t3.linked-message-origin/v1" as const,
                  environmentId: "context-source",
                  thread: { threadId },
                },
              }),
          success: Observed,
        });
      // Start narrow, then widen on the same session: initialization must not bind the ceiling.
      expect(yield* call("approval-required", "first")).toEqual({
        modes: "approval-required/plan",
        origin: "first",
      });
      expect(yield* call("full-access", "second")).toEqual({
        modes: "full-access/default",
        origin: "second",
      });
      expect(yield* call("approval-required")).toEqual({
        modes: "approval-required/plan",
        origin: "absent",
      });
      expect(yield* Ref.get(peer.mcpOpens)).toBe(1);
      const sessions = yield* Ref.get(peer.mcpSessions);
      expect(sessions).toHaveLength(4); // initialized notification and three tool calls
      expect(new Set(sessions).size).toBe(1);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);
