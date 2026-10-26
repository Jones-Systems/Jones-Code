import { expect, it } from "@effect/vitest";
import { EnvironmentId } from "@t3tools/contracts";
import {
  WORKSTREAMS_HOME_READ_RESULT_CONTRACT,
  type WorkstreamsHomeReadResult,
  type WorkstreamsHomeReadResultRequest,
} from "@t3tools/contracts/jones/workstreamsHomeReadResult";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { McpSchema, McpServer, Tool } from "effect/ai";
import * as McpHttpServer from "../../../mcp/McpHttpServer.ts";
import * as Invocation from "../../../mcp/McpInvocationContext.ts";
import * as Access from "../../../mcp/McpToolAccess.ts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";
import * as Protocol from "../../workstreams/native/forwarding/homeReadProtocol.ts";
import { RegistryError } from "../../workstreams/native/registry/session.ts";
import * as NativeReads from "./nativeReads.ts";

// This transport fixture supplies an external protocol port. Domain/peer admission is tested by
// homeReadProtocol and the qualified peer adapter suites, not simulated as authenticated here.
const scope: Invocation.McpInvocationScope = {
  environmentId: EnvironmentId.make("home:test"),
  thread: undefined,
  client: { sessionId: "peer:test", label: "test peer", access: "read-only", linked: true },
  capabilities: new Set(["orchestration"]),
  issuedAt: 0,
  requestNamespace: "test",
};
const request: WorkstreamsHomeReadResultRequest = {
  contract: WORKSTREAMS_HOME_READ_RESULT_CONTRACT,
  request: { route: "counts" as const, params: {} },
};
const context = { owner_id: "owner:test", server_generation: 1, registry_version: 3 };
const result: WorkstreamsHomeReadResult = {
  contract: WORKSTREAMS_HOME_READ_RESULT_CONTRACT,
  route: "counts",
  authorization_revision: 1,
  envelope: {
    contract: "workstreams-read/1.0.0",
    registry: {
      ...context,
      registry_instance_id: "11111111-1111-4111-8111-111111111111",
      canonical_origin: "https://native.workstreams.invalid",
    },
    provenance: {
      authority: "native-home",
      home_environment_id: "home:test",
      served_by_environment_id: "home:test",
      source: "live",
      observed_at: "2026-01-01T00:00:00Z",
      stale: false,
    },
    coverage: { registry: "complete", sources: [] },
  },
  value: {
    context,
    principal_id: "principal:test",
    observed_at: "2026-01-01T00:00:00Z",
    authority_effect: "none",
    active: 1,
    total: 1,
    unknown_lifecycle: 0,
  },
};
const threads = Layer.mock(ThreadManagementService)({ getThreadShell: () => Effect.succeed(null) });
const build = (read: Protocol.HomeReadProtocol["Service"]["read"]) =>
  NativeReads.toolkit.pipe(
    Effect.provide(
      Access.HandlersLayer.layer(NativeReads.handlers).pipe(
        Layer.provide(Layer.succeed(Protocol.HomeReadProtocol, { read })),
      ),
    ),
  );

const decodeRequest = Schema.decodeUnknownSync(
  NativeReads.toolkit.tools.jones_workstreams_home_read_result.parametersSchema,
);

it("advertises a closed, read-only, single-hop versioned protocol", () => {
  const tool = NativeReads.toolkit.tools.jones_workstreams_home_read_result;
  expect(Tool.getJsonSchema(tool)).toMatchObject({ type: "object" });
  expect(decodeRequest(request)).toEqual(request);
  for (const invalid of [
    { ...request, environment: "another-hop" },
    { ...request, contract: "unsupported" },
    { ...request, request: { route: "export", params: {} } },
    { ...request, placement_contract: "unsupported" },
  ])
    expect(() => decodeRequest(invalid)).toThrow();
});

it.effect("retains the coherent result and calls the application protocol once", () =>
  Effect.gen(function* () {
    const calls: unknown[] = [];
    const toolkit = yield* build((input) =>
      Effect.sync(() => {
        calls.push(input);
        return result;
      }),
    );
    const replies = yield* toolkit
      .handle("jones_workstreams_home_read_result", request)
      .pipe(Stream.unwrap, Stream.runCollect);
    expect(calls).toEqual([request]);
    expect(replies.at(-1)?.result).toEqual(result);
  }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope), Effect.provide(threads)),
);

it.effect("refuses missing orchestration capability before the protocol is called", () =>
  Effect.gen(function* () {
    let calls = 0;
    const toolkit = yield* build(() =>
      Effect.sync(() => {
        calls++;
        return result;
      }),
    );
    const replies = yield* toolkit
      .handle("jones_workstreams_home_read_result", request)
      .pipe(Stream.unwrap, Stream.runCollect);
    expect(calls).toBe(0);
    expect(JSON.stringify(replies)).toContain("McpCapabilityUnavailableError");
  }).pipe(
    Effect.provideService(Invocation.McpInvocationContext, {
      ...scope,
      capabilities: new Set<never>(),
    }),
    Effect.provide(threads),
  ),
);

it.effect("preserves a bounded domain refusal without leaking its underlying cause", () =>
  Effect.gen(function* () {
    const toolkit = yield* build(() =>
      Effect.fail(
        new RegistryError({ code: "not_granted", cause: new Error("protected-sentinel") }),
      ),
    );
    const replies = yield* toolkit
      .handle("jones_workstreams_home_read_result", request)
      .pipe(Stream.unwrap, Stream.runCollect);
    expect(JSON.stringify(replies)).toContain("not_granted");
    expect(JSON.stringify(replies)).not.toContain("protected-sentinel");
  }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope), Effect.provide(threads)),
);

it.effect("does not advertise the native peer tool without an application protocol", () =>
  Effect.gen(function* () {
    const server = yield* McpServer.McpServer;
    expect(server.tools.map(({ tool }) => tool.name)).toEqual([]);
  }).pipe(
    Effect.provide(
      McpHttpServer.layerNativeHomeReadToolkit.pipe(
        Layer.provideMerge(McpServer.McpServer.layer),
        Layer.provide(threads),
      ),
    ),
  ),
);

it.effect(
  "production registration preserves the application-owned protocol and coherent wire result",
  () => {
    let calls = 0;
    const registration = McpHttpServer.layerNativeHomeReadToolkit.pipe(
      Layer.provideMerge(McpServer.McpServer.layer),
      Layer.provide(
        Layer.succeed(Protocol.HomeReadProtocol, {
          read: (input) =>
            Effect.sync(() => {
              calls++;
              expect(input).toEqual(request);
              return result;
            }),
        }),
      ),
      Layer.provide(threads),
    );
    const client = McpSchema.McpServerClient.of({
      clientId: 1,
      clientCapabilities: {},
      clientInfo: { name: "native-peer-test", version: "1.0.0" },
      protocolVersion: "2025-06-18",
      initializePayload: {
        protocolVersion: "2025-06-18",
        capabilities: {},
        clientInfo: { name: "native-peer-test", version: "1.0.0" },
      },
      getClient: Effect.die("unused"),
    });
    return Effect.gen(function* () {
      const server = yield* McpServer.McpServer;
      expect(server.tools.map(({ tool }) => tool.name)).toEqual([
        "jones_workstreams_home_read_result",
      ]);
      const reply = yield* server
        .callTool({
          name: "jones_workstreams_home_read_result",
          arguments: request,
        })
        .pipe(
          Effect.provideService(Invocation.McpInvocationContext, scope),
          Effect.provideService(McpSchema.McpServerClient, client),
        );
      expect(reply.isError).toBe(false);
      expect(reply.structuredContent).toEqual(result);
      expect(calls).toBe(1);
    }).pipe(Effect.provide(registration));
  },
);
