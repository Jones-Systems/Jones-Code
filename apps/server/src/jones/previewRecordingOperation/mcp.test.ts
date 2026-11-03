import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  EnvironmentId,
  PreviewTabId,
  ProviderInstanceId,
  ThreadId,
  type PreviewAutomationRequest,
  type PreviewAutomationStreamEvent,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { McpSchema, McpServer, Tool } from "effect/ai";

import * as ServerConfig from "../../config.ts";
import * as McpHttpServer from "../../mcp/McpHttpServer.ts";
import * as McpInvocationContext from "../../mcp/McpInvocationContext.ts";
import * as McpToolAccessTestkit from "../../mcp/McpToolAccess.testkit.ts";
import * as PreviewAutomationBroker from "../../mcp/PreviewAutomationBroker.ts";
import { PreviewStandardToolkit, PreviewToolkit } from "../../mcp/toolkits/preview/tools.ts";

const tabId = PreviewTabId.make("recording-tab");
const invocation: McpInvocationContext.McpThreadInvocationScope = {
  environmentId: EnvironmentId.make("recording-environment"),
  requestNamespace: "recording-session",
  thread: {
    threadId: ThreadId.make("recording-thread"),
    providerSessionId: "recording-session",
    providerInstanceId: ProviderInstanceId.make("codex"),
  },
  client: undefined,
  capabilities: new Set(["preview"]),
  issuedAt: 1,
};
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  clientCapabilities: {},
  clientInfo: { name: "recording-operation-test", version: "1" },
  protocolVersion: "2025-06-18",
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "recording-operation-test", version: "1" },
  },
  getClient: Effect.die("unused"),
});

// Registration needs the stop tool's services; this read must never use them.
const layer = McpHttpServer.layerPreviewToolkit.pipe(
  Layer.provideMerge(McpServer.McpServer.layer),
  Layer.provideMerge(McpToolAccessTestkit.liveThreadsLayer),
  Layer.provideMerge(PreviewAutomationBroker.layer),
  Layer.provideMerge(
    ServerConfig.layerTest(process.cwd(), { prefix: "t3-recording-operation-test-" }),
  ),
  Layer.provideMerge(NodeServices.layer),
);
const call = (
  args: Record<string, unknown>,
  scope: McpInvocationContext.McpInvocationScope = invocation,
) =>
  Effect.gen(function* () {
    const server = yield* McpServer.McpServer;
    return yield* server
      .callTool({ name: "preview_recording_operation", arguments: args })
      .pipe(
        Effect.provideService(McpInvocationContext.McpInvocationContext, scope),
        Effect.provideService(McpSchema.McpServerClient, client),
      );
  });

it("advertises the recording operation read in both actual preview toolkits", () => {
  for (const toolkit of [PreviewToolkit, PreviewStandardToolkit]) {
    const tool = Object.values(toolkit.tools).find(
      (entry) => entry.name === "preview_recording_operation",
    );
    expect(tool).toBeDefined();
    if (tool === undefined) return;
    expect(Tool.getJsonSchema(tool)).toMatchObject({ type: "object", required: ["tabId"] });
  }
});

it.effect("registers a closed-world read with structured unavailable content and no host", () =>
  Effect.gen(function* () {
    const server = yield* McpServer.McpServer;
    const tool = server.tools.find(({ tool }) => tool.name === "preview_recording_operation");
    expect(tool).toBeDefined();
    expect(tool?.tool.annotations).toMatchObject({
      readOnlyHint: true,
      destructiveHint: false,
      idempotentHint: true,
      openWorldHint: false,
    });
    const result = yield* call({ tabId });
    expect(result.isError).toBe(false);
    expect(result.structuredContent).toEqual({
      schemaVersion: 1,
      tabId,
      result: { status: "unavailable", reason: "not_retained" },
    });
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "reads a timed-out recording through public MCP without contacting the host or clearing its fence",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const broker = yield* PreviewAutomationBroker.PreviewAutomationBroker;
        const connected = yield* Deferred.make<void>();
        const received = yield* Deferred.make<
          PreviewAutomationRequest & { connectionId: string }
        >();
        let hostRequests = 0;
        let evidenceCalls = 0;
        yield* Stream.runForEach(
          yield* broker.connect(
            {
              clientId: "recording-host",
              environmentId: invocation.environmentId,
            },
            {
              resolveRuntimeEvidence: () =>
                Effect.sync(() => {
                  evidenceCalls++;
                  return null;
                }),
            },
          ),
          (event: PreviewAutomationStreamEvent) => {
            if (event.type === "connected") return Deferred.succeed(connected, undefined);
            hostRequests++;
            return Deferred.succeed(received, {
              ...event.request,
              connectionId: event.connectionId,
            });
          },
        ).pipe(Effect.forkScoped);
        yield* Deferred.await(connected);
        const caller = yield* broker
          .invoke({
            scope: invocation,
            operation: "recordingStart",
            input: {},
            tabId,
            timeoutMs: 100,
            failurePolicy: "request_only",
          })
          .pipe(Effect.flip, Effect.orDie, Effect.forkScoped);
        const original = yield* Deferred.await(received);
        const handle = {
          requestId: original.requestId,
          clientId: "recording-host",
          connectionId: original.connectionId,
        };
        const before = { hostRequests, evidenceCalls };
        expect((yield* call({ tabId })).structuredContent).toMatchObject({
          result: { status: "pending", operation: "recordingStart", handle, dispatched: true },
        });
        yield* TestClock.adjust(1100);
        const timeout = yield* Fiber.join(caller);
        expect(timeout).toMatchObject({
          _tag: "PreviewAutomationTimeoutError",
          outcome: "unknown",
          ...handle,
        });
        for (let i = 0; i < 2; i++) {
          expect((yield* call({ tabId, handle })).structuredContent).toMatchObject({
            result: { status: "unknown", handle },
          });
        }
        expect({ hostRequests, evidenceCalls }).toEqual(before);
        expect(
          yield* broker
            .invoke({ scope: invocation, operation: "click", tabId, input: {} })
            .pipe(Effect.flip, Effect.orDie),
        ).toMatchObject({
          outcome: "not_started",
          unreconciled: true,
        });
        yield* TestClock.adjust(60_001);
        expect((yield* call({ tabId, handle })).structuredContent).toMatchObject({
          result: { status: "unknown" },
        });
        yield* broker.respond({
          ...handle,
          ok: true,
          result: { tabId, recording: true, startedAt: null },
        });
        const result = yield* call({ tabId, handle });
        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          result: {
            status: "settled",
            handle,
            reportedOutcome: "success",
            metadata: {
              status: "valid",
              reportedStart: { tabId, recording: true, startedAt: null },
            },
          },
        });
        expect({ hostRequests, evidenceCalls }).toEqual(before);
        expect(timeout).toMatchObject({
          _tag: "PreviewAutomationTimeoutError",
          outcome: "unknown",
        });
      }),
    ).pipe(Effect.provide(layer)),
);

it.effect("validates selectors and enforces caller access before the broker read", () =>
  Effect.gen(function* () {
    for (const args of [
      {},
      { tabId, handle: {} },
      { tabId, handle: { requestId: "one" } },
      { tabId, handle: null },
      { tabId, handle: { requestId: "", clientId: "host", connectionId: "connection" } },
      {
        tabId,
        handle: { requestId: "x".repeat(129), clientId: "host", connectionId: "connection" },
      },
      { tabId, providerSessionId: "override" },
      {
        tabId,
        handle: {
          requestId: "preview-1",
          clientId: "host",
          connectionId: "connection",
          extra: true,
        },
      },
    ]) {
      expect(yield* call(args).pipe(Effect.flip)).toMatchObject({ _tag: "InvalidParams" });
    }
    for (const scope of [
      { ...invocation, capabilities: new Set<McpInvocationContext.McpCapability>() },
      { ...invocation, thread: undefined },
    ]) {
      const result = yield* call({ tabId }, scope);
      expect(result.isError).toBe(true);
      expect(JSON.stringify(result)).not.toContain("not_retained");
    }
    const readonly = yield* call(
      { tabId },
      {
        ...invocation,
        client: { sessionId: "read-client", label: "test", access: "read-only" },
      },
    );
    expect(readonly.isError).toBe(false);
  }).pipe(Effect.provide(layer)),
);
