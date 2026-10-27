import { NodeHttpServer } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { OrchestratorMcpFailure, type LinkedRequestContext } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/ai";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServerRequest from "effect/http/HttpServerRequest";

import * as McpHttpServer from "../../mcp/McpHttpServer.ts";
import * as McpInvocationContext from "../../mcp/McpInvocationContext.ts";
import * as McpToolAccess from "../../mcp/McpToolAccess.ts";
import * as PeerLinks from "../../peer/PeerLinks.ts";
import * as PeerMcpClient from "../../peer/PeerMcpClient.ts";
import {
  descriptorOf,
  layerLinkingEnvironment,
  linkTo,
  servePeer,
} from "../../peer/PeerLinks.testkit.ts";
import * as RemoteAdmissionIntake from "../remoteAdmissionIntegration/RemoteAdmissionIntake.ts";
import { LINKED_MCP_PROBE_TOOL } from "./LinkedMcpProbe.testkit.ts";
import {
  type LinkedMessageOrigin,
  encodeLinkedMessageOrigin,
  LINKED_MESSAGE_ORIGIN_HEADER,
} from "./LinkedMessageOrigin.ts";
import {
  type BoundLinkedRequestContext,
  LINKED_REQUEST_CONTEXT_HEADER,
  PEER_REQUEST_HEADER_BUDGET,
  REQUEST_CONTEXT_MAX_SENT_CHARS,
  UNCONTROLLED_HEADER_ALLOWANCE,
  readBoundLinkedRequestContext,
  requestHeaderBytes,
} from "./LinkedRequestContext.ts";

// The frozen probe's server factory fixes its authenticator. Register its probe
// name here so this qualification uses servePeer's real pairing/OAuth instead.
const ProbeToolkit = Toolkit.make(
  Tool.make(LINKED_MCP_PROBE_TOOL, {
    description: "Synthetic real-link request-context qualification.",
    parameters: Schema.Record(Schema.String, Schema.Unknown),
    success: Schema.Unknown,
    failure: OrchestratorMcpFailure,
    failureMode: "return",
    dependencies: [McpInvocationContext.McpInvocationContext],
  }),
);
const success = Schema.Struct({ ok: Schema.Boolean });

it.effect(
  "A1-Q: a real paired link binds context, link session and unchanged v1 origin at intake",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sender = descriptorOf("sender", "Synthetic sender");
        const receiver = descriptorOf("receiver", "Synthetic receiver");
        const seen: Array<{
          readonly bound: BoundLinkedRequestContext | undefined;
          readonly invocation: McpInvocationContext.McpInvocationScope;
        }> = [];
        const headers: Array<Readonly<Record<string, string>>> = [];
        const registrations: Array<string> = [];
        const intake = Layer.mock(RemoteAdmissionIntake.RemoteAdmissionIntake)({
          register: (operation) => {
            registrations.push(operation);
          },
          invoke: (_operation, _params, invocation) =>
            Effect.gen(function* () {
              seen.push({ bound: yield* readBoundLinkedRequestContext(invocation), invocation });
              return { ok: true };
            }),
        });
        const capture = HttpRouter.middleware(
          (httpEffect) =>
            Effect.gen(function* () {
              const request = yield* HttpServerRequest.HttpServerRequest;
              if (request.url === "/mcp") headers.push(request.headers);
              return yield* httpEffect;
            }),
          { global: true },
        );
        const tools = McpHttpServer.toolkitRegistration(
          ProbeToolkit,
          McpToolAccess.toLayer(ProbeToolkit, {
            [LINKED_MCP_PROBE_TOOL]: McpToolAccess.reads(() => Effect.succeed({ ok: true })),
          }),
        ).pipe(Layer.provide(intake), Layer.provide(capture));
        const peer = yield* servePeer(receiver, tools);
        const linking = yield* layerLinkingEnvironment(sender).pipe(Layer.build);
        const links = Context.get(linking, PeerLinks.PeerLinks);
        const client = Context.get(linking, PeerMcpClient.PeerMcpClient);
        yield* linkTo(links, peer, "auto");
        const linkedSession = (yield* peer.linkedSessions)[0]!;
        const origin: LinkedMessageOrigin = {
          schemaVersion: "t3.linked-message-origin/v1",
          environmentId: sender.environmentId,
          thread: { threadId: "source-thread", title: "界".repeat(512) },
        };
        const context: LinkedRequestContext = {
          schemaVersion: "t3.linked-request-context/v1",
          sentAt: "2026-10-26T12:34:56.789Z",
          thread: { threadId: "source-thread", title: "Sender thread" },
          parent: { threadId: "parent-thread", title: "Sender parent" },
          project: {
            projectId: "source-project",
            title: "Sender project",
            icon: { kind: "emoji", emoji: "🚀" },
          },
        };
        const call = {
          environmentId: receiver.environmentId,
          tool: LINKED_MCP_PROBE_TOOL,
          arguments: {},
          limits: { runtimeMode: "auto", interactionMode: "default" } as const,
          success,
        };
        expect(registrations).toEqual([LINKED_MCP_PROBE_TOOL]);
        expect(yield* client.call({ ...call, origin, context })).toEqual({ ok: true });
        expect(seen[0]?.bound).toEqual({ context, linkSessionId: linkedSession.sessionId });
        expect(seen[0]?.invocation.client?.sessionId).toBe(linkedSession.sessionId);
        expect(seen[0]?.invocation.client?.remoteOrigin).toEqual({
          ...origin,
          linkSessionId: linkedSession.sessionId,
        });
        const wire = headers.find((value) => value[LINKED_REQUEST_CONTEXT_HEADER] !== undefined)!;
        expect(wire[LINKED_MESSAGE_ORIGIN_HEADER]).toBe(encodeLinkedMessageOrigin(origin));
        expect(wire[LINKED_REQUEST_CONTEXT_HEADER]!.length).toBeLessThanOrEqual(
          REQUEST_CONTEXT_MAX_SENT_CHARS,
        );
        expect(requestHeaderBytes(wire) + UNCONTROLLED_HEADER_ALLOWANCE).toBeLessThanOrEqual(
          PEER_REQUEST_HEADER_BUDGET,
        );
        expect(
          headers.filter((value) => value[LINKED_REQUEST_CONTEXT_HEADER] !== undefined),
        ).toHaveLength(1);
        yield* client.call(call);
        expect(seen).toHaveLength(2);
        expect(seen[1]?.bound).toBeUndefined();
        expect(seen[1]?.invocation.client?.remoteOrigin).toBeUndefined();
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);
