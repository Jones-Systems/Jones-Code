import { expect, it } from "@effect/vitest";
import { EnvironmentId } from "@t3tools/contracts";
import * as Forward from "@t3tools/contracts/jones/workstreamsForwarding";
import * as Parent from "@t3tools/contracts/jones/workstreamsForwardQualification";
import * as Raw from "@t3tools/contracts/jones/workstreamsForwardReadmission";
import * as RetainedWire from "@t3tools/contracts/jones/workstreamsRetainedSourceEffect";
import * as Retained from "../../workstreams/native/forwarding/retainedSourceEffectPort.ts";
import { request as providerRequest } from "../../workstreams/nativeProvider/testFixtures.ts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { McpServer } from "effect/ai";
import * as Server from "../../../mcp/McpHttpServer.ts";
import * as Invocation from "../../../mcp/McpInvocationContext.ts";
import * as Access from "../../../mcp/McpToolAccess.ts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";
import * as Home from "../../workstreams/native/forwarding/homeForwardPort.ts";
import * as Source from "../../workstreams/native/forwarding/sourceReadmissionPort.ts";
import { RegistryError } from "../../workstreams/native/registry/session.ts";
import * as ForwardMcp from "./nativeForwards.ts";

// Transport-only ports; genuine peer, caller and encrypted ledger qualification is separate.
const invocation: Invocation.McpInvocationScope = {
  environmentId: EnvironmentId.make("home:test"),
  thread: undefined,
  client: { sessionId: "peer:test", label: "test peer", access: "full-access", linked: true },
  capabilities: new Set(["orchestration"]),
  issuedAt: 0,
  requestNamespace: "test",
};
const home = {
  registry_instance_id: "11111111-1111-4111-8111-111111111111",
  owner_id: "owner:test",
  home_environment_id: "home:test",
  server_generation: 1,
};
const appearance = {
  command_id: "appearance:mcp-0001",
  workstream_id: "workstream:test",
  expected_server_generation: 1,
  expected_version: 2,
  border_color: "#112233",
};
const body =
  '{"command_id":"appearance:mcp-0001","workstream_id":"workstream:test","expected_server_generation":1,"expected_version":2,"border_color":"#112233"}';
const digest = "897c77e694a054a74a5f359b1ec89d2218732741297c8e79190dc289f62c1708";
const identity: Forward.WorkstreamsForwardIdentity = {
  forward_id: "jwf-mcp-forward-0001",
  source_operation_id: appearance.command_id,
  parent_operation_id: null,
  command_ids: [appearance.command_id],
  request_sha256: digest,
  recorded_caller_sha256: "b".repeat(64),
  expected_home: home,
  canonical_origin: "https://home.invalid",
  source_authority: {
    environment_id: "member:test",
    authority_namespace: "member-store",
    store_generation: 1,
  },
  source_enrollment_id: "enrollment:test",
  source_enrollment_session_id: "enrollment-session:test",
  principal_id: "principal:test",
  singleton_binding_version: 1,
  source_binding_version: 3,
  member_pin_id: "member-pin:test",
  member_pin_bound_revision: 2,
  outbound_link_generation: "member-to-home:opaque",
};
const forward: Forward.WorkstreamsForwardRequest = {
  contract: Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
  kind: "appearance",
  identity,
  request: {
    expected_home: home,
    relay: {
      source_environment_id: "member:test",
      origin: { kind: "cli" },
      linked_origin_environment_id: null,
      hop: 1,
    },
    request: appearance,
    request_body: body,
    request_sha256: digest,
  },
};
const result: Forward.WorkstreamsForwardResult = {
  contract: forward.contract,
  kind: "appearance",
  identity,
  home: {
    registry: { ...home, canonical_origin: identity.canonical_origin, registry_version: 22 },
    principal_id: identity.principal_id,
    authorization_revision: 9,
    source_binding_version: identity.source_binding_version,
    incoming_peer_pin_id: "home-pin:test",
    incoming_peer_pin_revision: 4,
  },
  result: { workstream_id: appearance.workstream_id, border_color: "#112233", version: 3 },
};
const readback: Forward.WorkstreamsForwardReadbackRequest = {
  contract: forward.contract,
  identity,
};
const unknown: Forward.WorkstreamsForwardReadbackResult = {
  ...readback,
  state: "unknown",
  reason: "not_found",
};
const raw: Raw.WorkstreamsSourceReadmissionRequestV2 = {
  contract: Raw.WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V2,
  accepted_forward: identity,
  command_id: appearance.command_id,
  command_request_sha256: digest,
};
const parent: Parent.WorkstreamsSourceParentReadmissionRequestV3 = {
  contract: Parent.WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V3,
  accepted_forward: identity,
  receiving_home_pin_id: "home-pin:test",
  receiving_home_pin_revision: 4,
};
const rawUnavailable: Raw.WorkstreamsSourceReadmissionResponseV2 = {
  contract: raw.contract,
  request: raw,
  state: "unavailable",
  reason: "authority_unavailable",
};
const parentDenied: Parent.WorkstreamsSourceParentReadmissionResponseV3 = {
  contract: parent.contract,
  request: parent,
  state: "denied",
  reason: "not_found",
};
const threads = Layer.mock(ThreadManagementService)({ getThreadShell: () => Effect.succeed(null) });
const homeService = (
  overrides: Partial<Home.HomeTools["Service"]> = {},
): Home.HomeTools["Service"] => ({
  forward: () => Effect.succeed(result),
  readback: () => Effect.succeed(unknown),
  ...overrides,
});
const sourceService = (
  overrides: Partial<Source.SourceReadmission["Service"]> = {},
): Source.SourceReadmission["Service"] => ({
  lookupRaw: () => Effect.succeed(rawUnavailable),
  lookupParent: () => Effect.succeed(parentDenied),
  ...overrides,
});
const buildHome = (service = homeService()) =>
  ForwardMcp.homeToolkit.pipe(
    Effect.provide(
      Access.HandlersLayer.layer(ForwardMcp.homeHandlers).pipe(
        Layer.provide(Layer.succeed(Home.HomeTools, service)),
      ),
    ),
  );
const buildSource = (service = sourceService()) =>
  ForwardMcp.sourceToolkit.pipe(
    Effect.provide(
      Access.HandlersLayer.layer(ForwardMcp.sourceHandlers).pipe(
        Layer.provide(Layer.succeed(Source.SourceReadmission, service)),
      ),
    ),
  );
const decodeForward = Schema.decodeUnknownSync(
  ForwardMcp.homeToolkit.tools.jones_workstreams_home_forward.parametersSchema,
);
const decodeReadback = Schema.decodeUnknownSync(
  ForwardMcp.homeToolkit.tools.jones_workstreams_home_forward_readback.parametersSchema,
);
const decodeSource = Schema.decodeUnknownSync(
  ForwardMcp.sourceToolkit.tools.jones_workstreams_source_readmission.parametersSchema,
);

it("advertises only fixed closed home tools and the explicit V2 or V3 source lookup", () => {
  expect(Object.keys(ForwardMcp.homeToolkit.tools)).toEqual([
    Home.HOME_FORWARD_TOOL,
    Home.HOME_FORWARD_READBACK_TOOL,
  ]);
  expect(Object.keys(ForwardMcp.sourceToolkit.tools)).toEqual([Source.SOURCE_READMISSION_TOOL]);
  expect(decodeForward(forward)).toEqual(forward);
  expect(() => decodeForward({ ...forward, kind: "command" })).toThrow();
  expect(() => decodeForward({ ...forward, request_body: body })).toThrow();
  expect(decodeReadback(readback)).toEqual(readback);
  expect(decodeSource(raw)).toEqual(raw);
  expect(decodeSource(parent)).toEqual(parent);
  for (const extra of [{ environmentId: "relay" }, { origin: "forged" }, { tool: "arbitrary" }]) {
    expect(() => decodeForward({ ...forward, ...extra })).toThrow();
    expect(() => decodeReadback({ ...readback, ...extra })).toThrow();
    expect(() => decodeSource({ ...parent, ...extra })).toThrow();
  }
  expect(() =>
    decodeSource({ ...parent, contract: "workstreams-source-readmission/1.0.0" }),
  ).toThrow();
  expect(() => decodeSource({ ...parent, command_id: raw.command_id })).toThrow();
  expect(() =>
    decodeSource({ ...raw, receiving_home_pin_id: parent.receiving_home_pin_id }),
  ).toThrow();
});

it.effect("passes exact retained bytes and the original invocation once for each home call", () =>
  Effect.gen(function* () {
    const calls: string[] = [];
    const toolkit = yield* buildHome(
      homeService({
        forward: (request, actual) =>
          Effect.sync(() => {
            calls.push("forward");
            expect(request).toEqual(forward);
            expect(actual).toBe(invocation);
            return result;
          }),
        readback: (request, actual) =>
          Effect.sync(() => {
            calls.push("readback");
            expect(request).toEqual(readback);
            expect(actual).toBe(invocation);
            return unknown;
          }),
      }),
    );
    const written = yield* toolkit
      .handle(Home.HOME_FORWARD_TOOL, forward)
      .pipe(Stream.unwrap, Stream.runCollect);
    const retained = yield* toolkit
      .handle(Home.HOME_FORWARD_READBACK_TOOL, readback)
      .pipe(Stream.unwrap, Stream.runCollect);
    expect(written.at(-1)?.result).toEqual(result);
    expect(retained.at(-1)?.result).toEqual(unknown);
    expect(calls).toEqual(["forward", "readback"]);
  }).pipe(
    Effect.provideService(Invocation.McpInvocationContext, invocation),
    Effect.provide(threads),
  ),
);

it.effect.each([
  ["read-only", "default", false],
  ["full-access", "plan", false],
  ["full-access", "default", true],
] as const)(
  "write ceiling %s/%s is enforced before home authority",
  ([access, interactionMode, allowed]) =>
    Effect.gen(function* () {
      let calls = 0;
      const scope: Invocation.McpInvocationScope = {
        ...invocation,
        client: {
          ...invocation.client!,
          access,
          narrowedTo: { runtimeMode: "full-access", interactionMode },
        },
      };
      const toolkit = yield* buildHome(
        homeService({
          forward: () =>
            Effect.sync(() => {
              calls++;
              return result;
            }),
        }),
      );
      const replies = yield* toolkit
        .handle(Home.HOME_FORWARD_TOOL, forward)
        .pipe(
          Stream.unwrap,
          Stream.runCollect,
          Effect.provideService(Invocation.McpInvocationContext, scope),
        );
      expect(calls).toBe(allowed ? 1 : 0);
      expect(allowed ? replies.at(-1)?.result : JSON.stringify(replies)).toEqual(
        allowed ? result : expect.stringContaining("capability_denied"),
      );
    }).pipe(Effect.provide(threads)),
);

it.effect("routes V2 and V3 exactly once, retaining denial and unavailable observations", () =>
  Effect.gen(function* () {
    const calls: string[] = [];
    const toolkit = yield* buildSource(
      sourceService({
        lookupRaw: (request, actual) =>
          Effect.sync(() => {
            calls.push("V2");
            expect(request).toEqual(raw);
            expect(actual).toBe(invocation);
            return rawUnavailable;
          }),
        lookupParent: (request, actual) =>
          Effect.sync(() => {
            calls.push("V3");
            expect(request).toEqual(parent);
            expect(actual).toBe(invocation);
            return parentDenied;
          }),
      }),
    );
    const rawReplies = yield* toolkit
      .handle(Source.SOURCE_READMISSION_TOOL, raw)
      .pipe(Stream.unwrap, Stream.runCollect);
    const parentReplies = yield* toolkit
      .handle(Source.SOURCE_READMISSION_TOOL, parent)
      .pipe(Stream.unwrap, Stream.runCollect);
    expect(rawReplies.at(-1)?.result).toEqual(rawUnavailable);
    expect(parentReplies.at(-1)?.result).toEqual(parentDenied);
    expect(calls).toEqual(["V2", "V3"]);
  }).pipe(
    Effect.provideService(Invocation.McpInvocationContext, invocation),
    Effect.provide(threads),
  ),
);

it.effect("a V3 refusal never falls back to V2", () =>
  Effect.gen(function* () {
    let rawCalls = 0;
    const toolkit = yield* buildSource(
      sourceService({
        lookupParent: () => Effect.fail(new RegistryError({ code: "unsupported_protocol" })),
        lookupRaw: () =>
          Effect.sync(() => {
            rawCalls++;
            return rawUnavailable;
          }),
      }),
    );
    const replies = yield* toolkit
      .handle(Source.SOURCE_READMISSION_TOOL, parent)
      .pipe(Stream.unwrap, Stream.runCollect);
    expect(rawCalls).toBe(0);
    expect(JSON.stringify(replies)).toContain("unsupported_protocol");
  }).pipe(
    Effect.provideService(Invocation.McpInvocationContext, invocation),
    Effect.provide(threads),
  ),
);

it.effect("requires orchestration capability before every domain call", () =>
  Effect.gen(function* () {
    let calls = 0;
    const homeTools = yield* buildHome(
      homeService({
        forward: () =>
          Effect.sync(() => {
            calls++;
            return result;
          }),
        readback: () =>
          Effect.sync(() => {
            calls++;
            return unknown;
          }),
      }),
    );
    const sourceTools = yield* buildSource(
      sourceService({
        lookupParent: () =>
          Effect.sync(() => {
            calls++;
            return parentDenied;
          }),
      }),
    );
    const replies = yield* Effect.all([
      homeTools.handle(Home.HOME_FORWARD_TOOL, forward).pipe(Stream.unwrap, Stream.runCollect),
      homeTools
        .handle(Home.HOME_FORWARD_READBACK_TOOL, readback)
        .pipe(Stream.unwrap, Stream.runCollect),
      sourceTools
        .handle(Source.SOURCE_READMISSION_TOOL, parent)
        .pipe(Stream.unwrap, Stream.runCollect),
    ]);
    expect(calls).toBe(0);
    expect(JSON.stringify(replies[0])).toContain("capability_denied");
    expect(JSON.stringify(replies[1])).toContain("McpCapabilityUnavailableError");
    expect(JSON.stringify(replies[2])).toContain("McpCapabilityUnavailableError");
  }).pipe(
    Effect.provideService(Invocation.McpInvocationContext, {
      ...invocation,
      capabilities: new Set<never>(),
    }),
    Effect.provide(threads),
  ),
);

it.effect("returns bounded error codes without protected domain causes", () =>
  Effect.gen(function* () {
    const refused = () =>
      Effect.fail(
        new RegistryError({ code: "not_granted", cause: new Error("protected-forward-sentinel") }),
      );
    const homeTools = yield* buildHome(homeService({ forward: refused, readback: refused }));
    const sourceTools = yield* buildSource(sourceService({ lookupParent: refused }));
    const replies = yield* Effect.all([
      homeTools.handle(Home.HOME_FORWARD_TOOL, forward).pipe(Stream.unwrap, Stream.runCollect),
      homeTools
        .handle(Home.HOME_FORWARD_READBACK_TOOL, readback)
        .pipe(Stream.unwrap, Stream.runCollect),
      sourceTools
        .handle(Source.SOURCE_READMISSION_TOOL, parent)
        .pipe(Stream.unwrap, Stream.runCollect),
    ]);
    for (const reply of replies) {
      expect(JSON.stringify(reply)).toContain("not_granted");
      expect(JSON.stringify(reply)).not.toContain("protected-forward-sentinel");
    }
  }).pipe(
    Effect.provideService(Invocation.McpInvocationContext, invocation),
    Effect.provide(threads),
  ),
);

it.effect.each([
  [false, false, false],
  [true, false, false],
  [false, true, false],
  [true, true, false],
  [false, false, true],
  [true, false, true],
  [false, true, true],
  [true, true, true],
] as const)(
  "registers only supplied home=%s source=%s retained=%s domain producers",
  ([hasHome, hasSource, hasRetained]) =>
    Effect.gen(function* () {
      const expected = [
        ...(hasHome ? Object.keys(ForwardMcp.homeToolkit.tools) : []),
        ...(hasSource ? Object.keys(ForwardMcp.sourceToolkit.tools) : []),
        ...(hasRetained ? Object.keys(ForwardMcp.retainedSourceToolkit.tools) : []),
      ];
      expect((yield* McpServer.McpServer).tools.map(({ tool }) => tool.name).sort()).toEqual(
        expected.sort(),
      );
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          Server.layerNativeHomeForwardToolkit,
          Server.layerNativeSourceReadmissionToolkit,
          Server.layerNativeRetainedSourceToolkit,
        ).pipe(
          Layer.provide(
            Layer.mergeAll(
              hasHome ? Layer.succeed(Home.HomeTools, homeService()) : Layer.empty,
              hasSource ? Layer.succeed(Source.SourceReadmission, sourceService()) : Layer.empty,
              hasRetained
                ? Layer.succeed(Retained.RetainedSourceEffects, {
                    invoke: () => Effect.succeed(retainedUnknown),
                  })
                : Layer.empty,
            ),
          ),
          Layer.provideMerge(McpServer.McpServer.layer),
          Layer.provide(threads),
        ),
      ),
    ),
);

const decodeRetainedCarrier = Schema.decodeUnknownSync(
  ForwardMcp.retainedSourceToolkit.tools.jones_workstreams_source_retained_effect.parametersSchema,
);
const retainedCarrier: RetainedWire.WorkstreamsRetainedSourceEffectRequestV1 = {
  contract: RetainedWire.WORKSTREAMS_SOURCE_RETAINED_EFFECT_CONTRACT_V1,
  source_readmission: raw,
  receiving_home_pin_id: "home-pin:test",
  receiving_home_pin_revision: 4,
  kind: "settle_lookup",
  request: providerRequest,
};
const retainedUnknown: RetainedWire.WorkstreamsRetainedSourceEffectResultV1 = {
  contract: retainedCarrier.contract,
  request: retainedCarrier,
  state: "unavailable",
  reason: "authority_unavailable",
};

it.effect("registers one closed retained-effect carrier and passes the original invocation", () =>
  Effect.gen(function* () {
    let calls = 0;
    expect(Object.keys(ForwardMcp.retainedSourceToolkit.tools)).toEqual([
      Retained.SOURCE_RETAINED_EFFECT_TOOL,
    ]);
    const decode = decodeRetainedCarrier;
    expect(decode(retainedCarrier)).toEqual(retainedCarrier);
    for (const extra of [{ tool: "other" }, { environmentId: "redirect" }, { caller: {} }])
      expect(() => decode({ ...retainedCarrier, ...extra })).toThrow();
    const toolkit = yield* ForwardMcp.retainedSourceToolkit.pipe(
      Effect.provide(
        Access.HandlersLayer.layer(ForwardMcp.retainedSourceHandlers).pipe(
          Layer.provide(
            Layer.succeed(Retained.RetainedSourceEffects, {
              invoke: (request, scope) =>
                Effect.sync(() => {
                  calls++;
                  expect(request).toEqual(retainedCarrier);
                  expect(scope).toBe(invocation);
                  return retainedUnknown;
                }),
            }),
          ),
        ),
      ),
    );
    const replies = yield* toolkit
      .handle(Retained.SOURCE_RETAINED_EFFECT_TOOL, retainedCarrier)
      .pipe(Stream.unwrap, Stream.runCollect);
    expect(replies.at(-1)?.result).toEqual(retainedUnknown);
    expect(calls).toBe(1);
  }).pipe(
    Effect.provide(threads),
    Effect.provideService(Invocation.McpInvocationContext, invocation),
  ),
);

it.effect.each([
  ["read-only", "default"],
  ["full-access", "plan"],
] as const)(
  "retained effect refuses %s/%s before reaching the producer",
  ([access, interactionMode]) =>
    Effect.gen(function* () {
      let calls = 0;
      const toolkit = yield* ForwardMcp.retainedSourceToolkit.pipe(
        Effect.provide(
          Access.HandlersLayer.layer(ForwardMcp.retainedSourceHandlers).pipe(
            Layer.provide(
              Layer.succeed(Retained.RetainedSourceEffects, {
                invoke: () =>
                  Effect.sync(() => {
                    calls++;
                    return retainedUnknown;
                  }),
              }),
            ),
          ),
        ),
      );
      const scope = {
        ...invocation,
        client: {
          ...invocation.client!,
          access,
          narrowedTo: { runtimeMode: "full-access" as const, interactionMode },
        },
      };
      const result = yield* toolkit
        .handle(Retained.SOURCE_RETAINED_EFFECT_TOOL, retainedCarrier)
        .pipe(
          Stream.unwrap,
          Stream.runCollect,
          Effect.provideService(Invocation.McpInvocationContext, scope),
        );
      expect(result.at(-1)?.isFailure).toBe(true);
      expect(calls).toBe(0);
    }).pipe(Effect.provide(threads)),
);
