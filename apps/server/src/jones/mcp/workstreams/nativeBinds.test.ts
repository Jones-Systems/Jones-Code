import { expect, it } from "@effect/vitest";
import { AuthSessionId, EnvironmentId } from "@t3tools/contracts";
import * as Wire from "@t3tools/contracts/jones/workstreamsHomeBind";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { McpServer } from "effect/ai";
import * as Server from "../../../mcp/McpHttpServer.ts";
import * as Invocation from "../../../mcp/McpInvocationContext.ts";
import * as Access from "../../../mcp/McpToolAccess.ts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";
import * as Home from "../../workstreams/native/runtime/homeBind.ts";
import { PeerSessionError } from "../../workstreams/native/runtime/peerSessionPins.ts";
import * as Binds from "./nativeBinds.ts";

// Transport-only fixture: HomeBind is an external port here. Genuine H/M qualification is separate.
const scope: Invocation.McpInvocationScope = {
  environmentId: EnvironmentId.make("home:test"),
  thread: undefined,
  client: { sessionId: "peer:test", label: "test peer", access: "read-only", linked: true },
  capabilities: new Set(["orchestration"]),
  issuedAt: 0,
  requestNamespace: "test",
};
const request: Wire.WorkstreamsHomeBindReadbackRequestV2 = {
  contract: Wire.WORKSTREAMS_HOME_BIND_CONTRACT,
  expected_home: {
    registry_instance_id: "11111111-1111-4111-8111-111111111111",
    owner_id: "owner:test",
    home_environment_id: "home:test",
    server_generation: 1,
  },
  bind_id: "bind:test",
  request_sha256: "a".repeat(64),
};
const result: Wire.WorkstreamsHomeBindReadbackV2 = {
  contract: Wire.WORKSTREAMS_HOME_BIND_CONTRACT,
  request,
  kind: "unknown",
  reason: "coverage_incomplete",
};
const threads = Layer.mock(ThreadManagementService)({ getThreadShell: () => Effect.succeed(null) });
const services = (readback: Home.HomeBind["Service"]["readback"]) =>
  Layer.succeed(Home.HomeBind, {
    readback,
    setupReadback: () => Effect.fail(new PeerSessionError({ code: "owner_unqualified" })),
    bind: () => Effect.fail(new PeerSessionError({ code: "owner_unqualified" })),
  });
const build = (readback: Home.HomeBind["Service"]["readback"]) =>
  Binds.toolkit.pipe(
    Effect.provide(
      Access.HandlersLayer.layer(Binds.handlers).pipe(Layer.provide(services(readback))),
    ),
  );
const decode = Schema.decodeUnknownSync(
  Binds.toolkit.tools.jones_workstreams_home_bind_readback.parametersSchema,
);

it("advertises only the three fixed closed bind operations", () => {
  expect(Object.keys(Binds.toolkit.tools)).toEqual([
    "jones_workstreams_home_bind_setup",
    "jones_workstreams_home_bind",
    "jones_workstreams_home_bind_readback",
  ]);
  expect(decode(request)).toEqual(request);
  for (const invalid of [
    { ...request, environmentId: "relay" },
    { ...request, origin: "forged" },
    { ...request, contract: "unsupported" },
    { ...request, request_sha256: "bad" },
  ])
    expect(() => decode(invalid)).toThrow();
});

it.effect(
  "passes the exact request and registered invocation once, retaining unknown outcomes",
  () =>
    Effect.gen(function* () {
      let calls = 0;
      const toolkit = yield* build((input, invocation) =>
        Effect.sync(() => {
          calls++;
          expect(input).toEqual(request);
          expect(invocation).toBe(scope);
          return result;
        }),
      );
      const replies = yield* toolkit
        .handle("jones_workstreams_home_bind_readback", request)
        .pipe(Stream.unwrap, Stream.runCollect);
      expect(replies.at(-1)?.result).toEqual(result);
      expect(calls).toBe(1);
    }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope), Effect.provide(threads)),
);

it.effect("checks orchestration capability before domain lookup", () =>
  Effect.gen(function* () {
    let calls = 0;
    const toolkit = yield* build(() =>
      Effect.sync(() => {
        calls++;
        return result;
      }),
    );
    const replies = yield* toolkit
      .handle("jones_workstreams_home_bind_readback", request)
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

it.effect("suppresses protected causes in domain refusals", () =>
  Effect.gen(function* () {
    const toolkit = yield* build(() =>
      Effect.fail(
        new PeerSessionError({ code: "owner_unqualified", cause: new Error("protected-sentinel") }),
      ),
    );
    const replies = yield* toolkit
      .handle("jones_workstreams_home_bind_readback", request)
      .pipe(Stream.unwrap, Stream.runCollect);
    expect(JSON.stringify(replies)).toContain("owner_unqualified");
    expect(JSON.stringify(replies)).not.toContain("protected-sentinel");
  }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope), Effect.provide(threads)),
);

it.effect("does not register bind tools without an application-owned producer", () =>
  Effect.gen(function* () {
    expect((yield* McpServer.McpServer).tools).toEqual([]);
  }).pipe(
    Effect.provide(
      Server.layerNativeHomeBindToolkit.pipe(
        Layer.provideMerge(McpServer.McpServer.layer),
        Layer.provide(threads),
      ),
    ),
  ),
);

it.effect("registers exactly the fixed tools when the application supplies its producer", () =>
  Effect.gen(function* () {
    expect((yield* McpServer.McpServer).tools.map(({ tool }) => tool.name)).toEqual(
      Object.keys(Binds.toolkit.tools),
    );
  }).pipe(
    Effect.provide(
      Server.layerNativeHomeBindToolkit.pipe(
        Layer.provideMerge(McpServer.McpServer.layer),
        Layer.provide(services(() => Effect.succeed(result))),
        Layer.provide(threads),
      ),
    ),
  ),
);

const bindRequest: Wire.WorkstreamsHomeBindRequestV2 = {
  contract: Wire.WORKSTREAMS_HOME_BIND_CONTRACT,
  request_body: Wire.canonicalWorkstreamsHomeBindBody({
    bind_id: "bind:test",
    expected_home: request.expected_home,
    expected_canonical_origin: "https://home.invalid",
    expected_home_authority: {
      environment_id: "home:test",
      authority_namespace: "home-store",
      store_generation: 1,
    },
    source: {
      authority: {
        environment_id: "member:test",
        authority_namespace: "source-store",
        store_generation: 1,
      },
      enrollment: {
        schema: "jones-code.workstreams-native-enrollment/v1",
        enrollment_id: "enrollment:test",
        registry_origin: "https://home.invalid",
        context: {
          owner_id: "owner:test",
          principal_id: "principal:test",
          source_instance_id: "member:test",
          authority_namespace: "source-store",
          store_generation: 1,
          enrollment_id: "enrollment:test",
          protocol: "workstreams-t3-provider/1.0.0",
          build: {
            repository: "Jones-Systems/Jones-Code",
            sha: "a".repeat(40),
            tree: "b".repeat(40),
          },
        },
        session: {
          session_id: AuthSessionId.make("provider:test"),
          issued_at: "2026-10-27T00:00:00.000Z",
          expires_at: "2026-10-28T00:00:00.000Z",
        },
      },
    },
    home_setup: {
      pin_id: "home-pin",
      prepared_revision: 1,
      setup_request_id: "home-setup",
      setup_request_sha256: "b".repeat(64),
    },
    member_setup: {
      pin_id: "member-pin",
      prepared_revision: 1,
      setup_request_id: "member-setup",
      outbound_link_generation: "opaque-link",
      expected_singleton_version: null,
      singleton_binding_version: 1,
    },
    expected: {
      principal_id: "principal:test",
      registry_version: 1,
      grant_id: "grant:test",
      grant_version: 1,
      source_binding: null,
    },
    rebind: false,
  }),
};

it.effect.each(["read-only", "full-access"] as const)(
  "bind applies the MCP %s ceiling before the domain's qualified write boundary",
  (access) =>
    Effect.gen(function* () {
      let calls = 0;
      const invocation: Invocation.McpInvocationScope = {
        ...scope,
        client: { ...scope.client!, access },
      };
      const base = {
        readback: () => Effect.succeed(result),
        setupReadback: () => Effect.fail(new PeerSessionError({ code: "owner_unqualified" })),
        bind: (input: Wire.WorkstreamsHomeBindRequestV2, actual: Invocation.McpInvocationScope) =>
          Effect.gen(function* () {
            calls++;
            expect(actual).toBe(invocation);
            expect(input.request_body).toBe(bindRequest.request_body);
            return yield* new PeerSessionError({ code: "owner_unqualified" });
          }),
      };
      const toolkit = yield* Binds.toolkit.pipe(
        Effect.provide(
          Access.HandlersLayer.layer(Binds.handlers).pipe(
            Layer.provide(Layer.succeed(Home.HomeBind, base)),
          ),
        ),
      );
      const replies = yield* toolkit
        .handle("jones_workstreams_home_bind", bindRequest)
        .pipe(
          Stream.unwrap,
          Stream.runCollect,
          Effect.provideService(Invocation.McpInvocationContext, invocation),
        );
      expect(calls).toBe(access === "read-only" ? 0 : 1);
      expect(JSON.stringify(replies)).toContain(
        access === "read-only" ? "capability_denied" : "owner_unqualified",
      );
    }).pipe(Effect.provide(threads)),
);
