import { NodeHttpServer } from "@effect/platform-node";
import * as NodePath from "@effect/platform-node/NodePath";
import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ProviderInstanceId,
  ThreadId,
  type McpCapabilityUnavailableError,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import type * as Types from "effect/Types";
import { McpProtocol, McpSchema, McpServer, Tool } from "effect/ai";
import {
  HttpBody,
  HttpClient,
  HttpRouter,
  HttpServer,
  HttpServerRequest,
  HttpServerResponse,
} from "effect/http";
import * as NetAddress from "effect/net/NetAddress";
import { ServerConfig, deriveServerPaths } from "../../../config.ts";
import { DEFAULT_SIGNAL_EXPORT } from "@t3tools/shared/observability";
import * as OtelEnvironment from "@t3tools/shared/otelEnvironment";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";
import * as Invocation from "../../../mcp/McpInvocationContext.ts";
import { APP_IDENTITY } from "../../runtimeIdentity/AppIdentity.ts";
import * as Metadata from "./OrganizationMetadataMcpService.ts";
import { OrganizationMetadataHandlersLive } from "./handlers.ts";
import { OrganizationMetadataRegistrationLive } from "../../../mcp/McpHttpServer.ts";
import * as McpToolAccess from "../../../mcp/McpToolAccess.ts";
import { OrganizationMetadataToolkit } from "./tools.ts";

const encodeJsonText = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const configLayer = Layer.effect(
  ServerConfig,
  deriveServerPaths("/synthetic/effective-home", undefined).pipe(
    Effect.map((derivedPaths) =>
      ServerConfig.of({
        logLevel: "Error",
        traceMinLevel: "Info",
        traceTimingEnabled: true,
        traceBatchWindowMs: 200,
        traceMaxBytes: 10 * 1024 * 1024,
        traceMaxFiles: 10,
        otlpTracesUrl: undefined,
        otlpMetricsUrl: undefined,
        otlpLogsUrl: undefined,
        otlpTracesExport: DEFAULT_SIGNAL_EXPORT,
        otlpMetricsExport: DEFAULT_SIGNAL_EXPORT,
        otlpLogsExport: DEFAULT_SIGNAL_EXPORT,
        otelEnvironment: OtelEnvironment.none,
        cwd: "/synthetic/workspace",
        baseDir: "/synthetic/effective-home",
        ...derivedPaths,
        mode: "web",
        autoBootstrapProjectFromCwd: false,
        logWebSocketEvents: false,
        tailscaleServeEnabled: false,
        tailscaleServePort: 443,
        port: 0,
        host: undefined,
        desktopBootstrapToken: undefined,
        desktopTelemetryFd: undefined,
        desktopTelemetryControlFd: undefined,
        resourceMonitorPath: undefined,
        staticDir: undefined,
        devUrl: undefined,
        devAllowedOrigins: [],
        noBrowser: false,
        startupPresentation: "browser",
      }),
    ),
    Effect.provide(NodePath.layer),
  ),
);

const invocation: Invocation.McpInvocationScope = {
  environmentId: EnvironmentId.make("authenticated-environment"),
  requestNamespace: "private-session",
  thread: {
    threadId: ThreadId.make("authenticated-thread"),
    providerSessionId: "private-session",
    providerInstanceId: ProviderInstanceId.make("codex"),
  },
  client: undefined,
  capabilities: new Set(["orchestration"]),
  issuedAt: 1,
};
const dependencies = Layer.mergeAll(
  configLayer,
  Layer.mock(HttpServer.HttpServer)({
    address: NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 43123),
  }),
  Layer.mock(ThreadManagement.ThreadManagementService)({
    getShellSnapshot: () =>
      Effect.succeed({ schemaVersion: 1, snapshotSequence: 7, threads: [], archivedThreads: [] }),
  }),
);
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  clientCapabilities: {},
  clientInfo: { name: "synthetic-test", version: "1.0.0" },
  protocolVersion: "2025-06-18",
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "synthetic-test", version: "1.0.0" },
  },
  getClient: Effect.die("unused"),
});

it("accepts only empty invocation input with a reference-free object-root schema", () => {
  const tool = OrganizationMetadataToolkit.tools.get_invocation_context;
  expect(Tool.getJsonSchema(tool)).toMatchObject({ type: "object", maxProperties: 0 });
  const decode = Schema.decodeUnknownSync(tool.parametersSchema);
  expect(decode({})).toEqual({});
  for (const input of [
    null,
    [],
    "",
    1,
    { environmentId: "forged" },
    { threadId: "forged" },
    { authorizationHeader: "private" },
  ]) {
    expect(() => decode(input)).toThrow();
  }
  for (const tool of Object.values(OrganizationMetadataToolkit.tools)) {
    expect(Tool.getJsonSchema(tool)).toMatchObject({ type: "object" });
    expect(encodeJsonText(Tool.getJsonSchema(tool))).not.toContain('"$ref"');
  }
});

it("bounds metadata pagination input", () => {
  const decode = Schema.decodeUnknownSync(
    OrganizationMetadataToolkit.tools.list_organization_thread_metadata.parametersSchema,
  );
  expect(decode({ limit: 1, offset: 0, expectedSnapshotSequence: 7 })).toEqual({
    limit: 1,
    offset: 0,
    expectedSnapshotSequence: 7,
  });
  expect(decode({ limit: 100 })).toEqual({ limit: 100 });
  for (const input of [
    { limit: 0 },
    { limit: 101 },
    { limit: 1.5 },
    { offset: -1 },
    { offset: Number.MAX_SAFE_INTEGER + 1 },
    { expectedSnapshotSequence: -1 },
  ])
    expect(() => decode(input)).toThrow();
});

it.effect(
  "uses authenticated caller identities across callers sharing one toolkit without exposing credentials",
  () =>
    Effect.gen(function* () {
      const toolkit = yield* OrganizationMetadataToolkit.pipe(
        Effect.provide(
          McpToolAccess.HandlersLayer.layer(OrganizationMetadataHandlersLive).pipe(
            Layer.provide(Metadata.layer),
            Layer.provide(dependencies),
          ),
        ),
      );
      for (const caller of [
        invocation,
        {
          ...invocation,
          environmentId: EnvironmentId.make("alternate-environment"),
          thread: { ...invocation.thread!, threadId: ThreadId.make("alternate-thread") },
        },
        invocation,
      ]) {
        const results = yield* toolkit
          .handle("get_invocation_context", {})
          .pipe(
            Stream.unwrap,
            Stream.runCollect,
            Effect.provideService(Invocation.McpInvocationContext, caller),
            Effect.provide(Metadata.layer.pipe(Layer.provide(dependencies))),
          );
        const result = results.at(-1)?.result;
        expect(result).toMatchObject({
          environmentId: caller.environmentId,
          threadId: caller.thread?.threadId,
          appIdentity: APP_IDENTITY,
        });
        expect(encodeJsonText(result)).not.toContain(caller.thread?.providerSessionId);
        expect(encodeJsonText(result)).not.toContain("authorizationHeader");
      }
    }),
);

it.effect(
  "denies both tools before calling the metadata service when orchestration is absent",
  () =>
    Effect.gen(function* () {
      let reads = 0;
      const service = Layer.succeed(Metadata.OrganizationMetadataMcpService, {
        getInvocationContext: () => {
          reads++;
          return Effect.die("unexpected context read");
        },
        listThreadMetadata: () => {
          reads++;
          return Effect.die("unexpected metadata read");
        },
      });
      const toolkit = yield* OrganizationMetadataToolkit.pipe(
        Effect.provide(
          McpToolAccess.HandlersLayer.layer(OrganizationMetadataHandlersLive).pipe(
            Layer.provide(service),
          ),
        ),
      );
      for (const name of ["get_invocation_context", "list_organization_thread_metadata"] as const) {
        const results = yield* toolkit.handle(name, {}).pipe(
          Stream.unwrap,
          Stream.runCollect,
          Effect.provideService(Invocation.McpInvocationContext, {
            ...invocation,
            capabilities: new Set<Invocation.McpCapability>(),
          }),
          Effect.provide(service),
        );
        expect(results.at(-1)?.result as McpCapabilityUnavailableError).toMatchObject({
          _tag: "McpCapabilityUnavailableError",
          capability: "orchestration",
        });
      }
      expect(reads).toBe(0);
    }),
);

it.effect(
  "denies caller invocation context to an external OAuth client before reading metadata",
  () =>
    Effect.gen(function* () {
      let reads = 0;
      const service = Layer.succeed(Metadata.OrganizationMetadataMcpService, {
        getInvocationContext: () => {
          reads++;
          return Effect.die("unexpected context read");
        },
        listThreadMetadata: () => Effect.die("unused"),
      });
      const toolkit = yield* OrganizationMetadataToolkit.pipe(
        Effect.provide(
          McpToolAccess.HandlersLayer.layer(OrganizationMetadataHandlersLive).pipe(
            Layer.provide(service),
          ),
        ),
      );
      const results = yield* toolkit.handle("get_invocation_context", {}).pipe(
        Stream.unwrap,
        Stream.runCollect,
        Effect.provideService(Invocation.McpInvocationContext, {
          ...invocation,
          requestNamespace: "client:synthetic-session",
          thread: undefined,
          client: { sessionId: "synthetic-session", label: "synthetic", access: "read-only" },
        }),
        Effect.provide(service),
      );
      expect(results.at(-1)?.result).toMatchObject({
        _tag: "OrchestratorMcpFailure",
        code: "thread_credential_required",
      });
      expect(reads).toBe(0);
    }),
);

it.effect(
  "registers exactly the two read-only tools and dispatches with request-scoped identities",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        expect(server.tools.map(({ tool }) => tool.name).toSorted()).toEqual([
          "get_invocation_context",
          "list_organization_thread_metadata",
        ]);
        for (const { tool } of server.tools)
          expect(tool.annotations).toMatchObject({
            readOnlyHint: true,
            destructiveHint: false,
            idempotentHint: true,
            openWorldHint: false,
          });
        const context = yield* server
          .callTool({ name: "get_invocation_context", arguments: {} })
          .pipe(
            Effect.provideService(Invocation.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );
        expect(context.isError).toBe(false);
        expect(context.structuredContent).toMatchObject({
          environmentId: invocation.environmentId,
          threadId: invocation.thread?.threadId,
          loopbackOrigin: "http://127.0.0.1:43123",
        });
        const page = yield* server
          .callTool({ name: "list_organization_thread_metadata", arguments: {} })
          .pipe(
            Effect.provideService(Invocation.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );
        expect(page.structuredContent).toMatchObject({
          environmentId: invocation.environmentId,
          snapshotSequence: 7,
          threads: [],
          nextOffset: null,
        });
      }),
    ).pipe(
      Effect.provide(
        OrganizationMetadataRegistrationLive.pipe(
          Layer.provideMerge(McpServer.McpServer.layer),
          Layer.provide(dependencies),
        ),
      ),
    ),
);

it.effect(
  "publishes metadata tools through an HTTP MCP session with synthetic authentication",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const auth = HttpRouter.middleware<{ provides: Invocation.McpInvocationContext }>()(
          Effect.succeed(
            (
              httpEffect: Effect.Effect<
                HttpServerResponse.HttpServerResponse,
                Types.unhandled,
                Invocation.McpInvocationContext
              >,
            ) =>
              Effect.gen(function* () {
                const request = yield* HttpServerRequest.HttpServerRequest;
                if (request.headers.authorization !== "Bearer synthetic-test")
                  return HttpServerResponse.empty({ status: 401 });
                return yield* httpEffect.pipe(
                  Effect.provideService(Invocation.McpInvocationContext, invocation),
                );
              }),
          ),
        ).layer;
        const transport = McpServer.layerHttp({
          name: "Metadata test",
          version: "1.0.0",
          path: "/mcp",
          protocols: [McpProtocol.v2025_06_18],
        }).pipe(Layer.provide(auth));
        const registration = OrganizationMetadataRegistrationLive.pipe(Layer.provide(dependencies));
        yield* HttpRouter.serve(registration.pipe(Layer.provideMerge(transport)), {
          disableListenLog: true,
          disableLogger: true,
        }).pipe(Layer.build);
        const httpClient = yield* HttpClient.HttpClient;
        const initialize = yield* httpClient.post("/mcp", {
          headers: {
            accept: "application/json, text/event-stream",
            authorization: "Bearer synthetic-test",
          },
          body: HttpBody.text(
            encodeJsonText({
              jsonrpc: "2.0",
              id: 1,
              method: "initialize",
              params: {
                protocolVersion: "2025-06-18",
                capabilities: {},
                clientInfo: { name: "synthetic", version: "1.0.0" },
              },
            }),
            "application/json",
          ),
        });
        expect(initialize.status).toBe(200);
        const session = initialize.headers["mcp-session-id"];
        expect(session).toBeDefined();
        const headers = {
          accept: "application/json, text/event-stream",
          authorization: "Bearer synthetic-test",
          "mcp-session-id": session!,
          "mcp-protocol-version": "2025-06-18",
        };
        const listing = yield* httpClient.post("/mcp", {
          headers,
          body: HttpBody.text(
            encodeJsonText({ jsonrpc: "2.0", id: 2, method: "tools/list", params: {} }),
            "application/json",
          ),
        });
        expect(listing.status).toBe(200);
        const listed = yield* listing.text;
        expect(listed).toContain('"name":"get_invocation_context"');
        expect(listed).toContain('"name":"list_organization_thread_metadata"');
        const context = yield* httpClient.post("/mcp", {
          headers,
          body: HttpBody.text(
            encodeJsonText({
              jsonrpc: "2.0",
              id: 3,
              method: "tools/call",
              params: { name: "get_invocation_context", arguments: {} },
            }),
            "application/json",
          ),
        });
        expect(context.status).toBe(200);
        const body = yield* context.text;
        expect(body).toContain('"threadId":"authenticated-thread"');
        expect(body).toContain('"environmentId":"authenticated-environment"');
        expect(body).not.toContain("private-session");
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);
