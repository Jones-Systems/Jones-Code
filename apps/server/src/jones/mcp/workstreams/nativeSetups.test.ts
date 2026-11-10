import { expect, it } from "@effect/vitest";
import { EnvironmentId } from "@t3tools/contracts";
import * as W from "@t3tools/contracts/jones/workstreamsPeerSetup";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import { McpServer } from "effect/ai";
import * as Server from "../../../mcp/McpHttpServer.ts";
import * as Invocation from "../../../mcp/McpInvocationContext.ts";
import * as Access from "../../../mcp/McpToolAccess.ts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";
import * as Setup from "../../workstreams/native/runtime/peerSetup.ts";
import { PeerSessionError } from "../../workstreams/native/runtime/peerSessionPins.ts";
import * as Tools from "./nativeSetups.ts";
const scope: Invocation.McpInvocationScope = {
  environmentId: EnvironmentId.make("home:test"),
  thread: undefined,
  capabilities: new Set(["orchestration"]),
  issuedAt: 0,
  requestNamespace: "test",
  client: { sessionId: "peer:test", label: "no authority", access: "full-access", linked: true },
};
const request: W.WorkstreamsPeerSetupReadRequest = {
  contract: W.WORKSTREAMS_PEER_SETUP_CONTRACT,
  operation: "context",
  setup_id: "setup:test",
  expected_home: {
    registry_instance_id: "11111111-1111-4111-8111-111111111111",
    owner_id: "owner:test",
    home_environment_id: "home:test",
    server_generation: 1,
  },
  source_environment_id: "member:test",
  receiver_environment_id: "home:test",
};
const threads = Layer.mock(ThreadManagementService)({ getThreadShell: () => Effect.succeed(null) });
it("advertises only two fixed closed setup tools", () => {
  expect(Object.keys(Tools.toolkit.tools)).toEqual([
    "jones_workstreams_peer_setup_read",
    "jones_workstreams_peer_setup_apply",
  ]);
  const decode = Schema.decodeUnknownSync(
    Tools.toolkit.tools.jones_workstreams_peer_setup_read.parametersSchema,
  );
  expect(decode(request)).toEqual(request);
  const intent = { ...request, operation: "source_intent", request_sha256: "a".repeat(64) };
  expect(decode(intent)).toEqual(intent);
  for (const invalid of [
    { ...request, tool: "arbitrary" },
    { ...request, request_sha256: intent.request_sha256 },
    { ...request, operation: "source_intent" },
    { ...intent, request_sha256: "invalid" },
    { ...intent, tool: "arbitrary" },
  ])
    expect(() => decode(invalid)).toThrow();
});
it.effect("actual handler passes exact invocation; error mapping excludes protected causes", () =>
  Effect.gen(function* () {
    let calls = 0;
    const service = Layer.succeed(Setup.PeerSetup, {
      read: (r, s) =>
        Effect.suspend(() => {
          calls++;
          expect(r).toEqual(request);
          expect(s).toBe(scope);
          return Effect.fail(
            new PeerSessionError({
              code: "owner_unqualified",
              cause: new Error("protected-sentinel"),
            }),
          );
        }),
      prepare: () => Effect.die("unused"),
      apply: () => Effect.die("unused"),
      ownerOperation: () => Effect.die("unused"),
      homeBind: {
        setupReadback: () => Effect.die("unused"),
        bind: () => Effect.die("unused"),
        readback: () => Effect.die("unused"),
      },
    });
    const toolkit = yield* Tools.toolkit.pipe(
      Effect.provide(Access.HandlersLayer.layer(Tools.handlers).pipe(Layer.provide(service))),
    );
    const replies = yield* toolkit
      .handle("jones_workstreams_peer_setup_read", request)
      .pipe(Stream.unwrap, Stream.runCollect);
    expect(calls).toBe(1);
    expect(JSON.stringify(replies)).toContain("owner_unqualified");
    expect(JSON.stringify(replies)).not.toContain("protected-sentinel");
  }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope), Effect.provide(threads)),
);

it.effect("registers the setup tools through the actual MCP server", () =>
  Effect.gen(function* () {
    const registered = (yield* McpServer.McpServer).tools;
    expect(registered.map(({ tool }) => tool.name)).toEqual(Object.keys(Tools.toolkit.tools));
    for (const { tool } of registered) expect(tool.inputSchema.type).toBe("object");
  }).pipe(
    Effect.provide(
      Server.toolkitRegistration(Tools.toolkit, Tools.handlers).pipe(
        Layer.provide(
          Layer.succeed(Setup.PeerSetup, {
            read: () => Effect.die("registration must not invoke setup"),
            prepare: () => Effect.die("registration must not invoke setup"),
            apply: () => Effect.die("registration must not invoke setup"),
            ownerOperation: () => Effect.die("registration must not invoke setup"),
            homeBind: {
              setupReadback: () => Effect.die("registration must not invoke setup"),
              bind: () => Effect.die("registration must not invoke setup"),
              readback: () => Effect.die("registration must not invoke setup"),
            },
          }),
        ),
        Layer.provideMerge(McpServer.McpServer.layer),
        Layer.provide(threads),
      ),
    ),
  ),
);
