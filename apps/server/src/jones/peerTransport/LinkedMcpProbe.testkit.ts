import { NodeHttpServer } from "@effect/platform-node";
import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/ai";
import { HttpBody, HttpClient, HttpRouter, HttpServer } from "effect/http";

import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import * as McpInvocationContext from "../../mcp/McpInvocationContext.ts";
import * as McpHttpServer from "../../mcp/McpHttpServer.ts";
import * as McpSessionRegistry from "../../mcp/McpSessionRegistry.ts";
import * as McpToolAccess from "../../mcp/McpToolAccess.ts";
import * as RemoteAdmissionIntake from "../remoteAdmissionIntegration/RemoteAdmissionIntake.ts";

export const LINKED_MCP_PROBE_TOOL = "linked_context_probe";
const protocolVersion = "2025-06-18";
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const ProbeToolkit = Toolkit.make(
  Tool.make(LINKED_MCP_PROBE_TOOL, {
    description: "Synthetic mutation routed through the real admission wrapper.",
    parameters: Schema.Record(Schema.String, Schema.Unknown),
    success: Schema.Unknown,
    failure: OrchestratorMcpFailure,
    failureMode: "return",
    dependencies: [McpInvocationContext.McpInvocationContext],
  }),
);

/** Owns an ephemeral Node server in the caller's Scope; accepts either recording or real intake. */
export const serveLinkedMcpProbe = <E, R>(
  intakeLayer: Layer.Layer<RemoteAdmissionIntake.RemoteAdmissionIntake, E, R>,
  scopes: Readonly<Record<string, McpInvocationScope>>,
) =>
  Effect.gen(function* () {
    const node = yield* NodeHttpServer.layerTest.pipe(Layer.fresh, Layer.build);
    const nodeLayer = Layer.succeedContext(node);
    const routes = McpHttpServer.toolkitRegistration(
      ProbeToolkit,
      McpToolAccess.toLayer(ProbeToolkit, {
        [LINKED_MCP_PROBE_TOOL]: McpToolAccess.reads(() => Effect.succeed({ ok: true })),
      }),
    ).pipe(
      Layer.provideMerge(McpHttpServer.layerMcpTransport),
      Layer.provide(intakeLayer),
      Layer.provide(
        Layer.mock(McpSessionRegistry.McpSessionRegistry)({
          // @effect-diagnostics-next-line effectSucceedWithVoid:off - This optional result requires undefined, not the wider void type.
          resolve: () => Effect.succeed(undefined),
        }),
      ),
      Layer.provide(
        Layer.succeed(McpHttpServer.McpClientAuthenticator, {
          authenticate: (request) =>
            Effect.succeed(scopes[request.headers.authorization?.replace(/^Bearer /, "") ?? ""]),
        }),
      ),
    );
    yield* HttpRouter.serve(routes, { disableListenLog: true, disableLogger: true }).pipe(
      Layer.provide(nodeLayer),
      Layer.fresh,
      Layer.build,
    );
    const address = Context.get(node, HttpServer.HttpServer).address;
    if (address._tag === "UnixPathAddress")
      return yield* Effect.die("Expected an owned TCP probe server");
    const url = `http://127.0.0.1:${address.port}/mcp`;
    const client = Context.get(node, HttpClient.HttpClient);
    let nextId = 0;
    const callHeaders = (
      session: { readonly token: string; readonly sessionId: string },
      extra: Readonly<Record<string, string>> = {},
    ): Record<string, string> => ({
      accept: "application/json, text/event-stream",
      authorization: `Bearer ${session.token}`,
      "content-type": "application/json",
      "mcp-session-id": session.sessionId,
      "mcp-protocol-version": protocolVersion,
      ...extra,
    });
    const callBody = (params: Readonly<Record<string, unknown>> = {}) =>
      encodeJson({
        jsonrpc: "2.0",
        id: ++nextId,
        method: "tools/call",
        params: { name: LINKED_MCP_PROBE_TOOL, arguments: params },
      });
    const open = (token: string) =>
      Effect.gen(function* () {
        const response = yield* client.post("/mcp", {
          headers: {
            accept: "application/json, text/event-stream",
            authorization: `Bearer ${token}`,
          },
          body: HttpBody.text(
            encodeJson({
              jsonrpc: "2.0",
              id: ++nextId,
              method: "initialize",
              params: {
                protocolVersion,
                capabilities: {},
                clientInfo: { name: "linked-probe", version: "1" },
              },
            }),
            "application/json",
          ),
        });
        yield* response.text;
        const sessionId = response.headers["mcp-session-id"];
        if (response.status !== 200 || sessionId === undefined) {
          return yield* Effect.die(`Probe initialization failed: HTTP ${response.status}`);
        }
        return { token, sessionId };
      });
    const call = (
      session: { readonly token: string; readonly sessionId: string },
      input: {
        readonly headers?: Readonly<Record<string, string>>;
        readonly params?: Readonly<Record<string, unknown>>;
      } = {},
    ) =>
      Effect.gen(function* () {
        const response = yield* client.post("/mcp", {
          headers: callHeaders(session, input.headers),
          body: HttpBody.text(callBody(input.params), "application/json"),
        });
        return { status: response.status, text: yield* response.text };
      });
    return { url, open, call, callHeaders, callBody };
  });
