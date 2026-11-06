// @effect-diagnostics nodeBuiltinImport:off -- Match the unchanged canonical public request digest.
import * as NodeCrypto from "node:crypto";
import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ThreadId,
  ProviderInstanceId,
  t3PlacementInventoryJson,
} from "@t3tools/contracts";
import * as Wire from "@t3tools/contracts/jones/workstreamOperations";
import * as Access from "@t3tools/contracts/jones/workstreamsHomeAccess";
import type { NativeReference, Workstream } from "@t3tools/contracts";
import * as Native from "@t3tools/contracts/jones/workstreamsNativePlacement";
import type { WorkstreamsNativeHomeRead } from "@t3tools/contracts/jones/workstreamsNativeRead";
import * as Effect from "effect/Effect";
import * as TestClock from "effect/testing/TestClock";
import * as Authority from "../../../../environment/NativeStoreAuthority.ts";
import type { ReadResult } from "../read/routes.ts";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Environment from "../../../../environment/ServerEnvironment.ts";
import * as Public from "../../operations/service.ts";
import { canonicalJson } from "../../operations/encoding.ts";
import * as App from "../runtime/memberApplication.ts";
import { RegistryError } from "../registry/session.ts";
import { workstreamCapabilities } from "../registry/capabilities.ts";
import type { PublicOperation } from "./memberOperationRead.ts";
import { projectOperation } from "./memberOperations.ts";
import * as Member from "./memberPublicOperations.ts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";

const isSetupStatus = Schema.is(Wire.WorkstreamSetupStatus);
const at = "2026-01-01T00:00:00.000Z";
const home = {
  registry_instance_id: "11111111-1111-4111-8111-111111111111",
  owner_id: "owner:test",
  canonical_origin: "https://home.invalid",
  home_environment_id: "home:test",
  server_generation: 1,
  registry_version: 9,
};
const observation = (): Access.WorkstreamsHomeAccessResult => ({
  contract: Access.WORKSTREAMS_HOME_ACCESS_CONTRACT,
  home,
  principal_id: "principal:test",
  authorization_revision: 4,
  observed_at: at,
  capabilities: workstreamCapabilities(
    { owner_id: home.owner_id, server_generation: 1, registry_version: 9 },
    ["workstreams:read", "workstreams:write"],
  ),
});
const request = Wire.parseWorkstreamOperationRequest({
  contract: Wire.WORKSTREAM_OPERATIONS_CONTRACT,
  operation_id: "operation:public-member-01",
  kind: "create_workstream",
  origin: { kind: "app" },
  registry: { registry_id: home.registry_instance_id, owner_id: home.owner_id },
  workstream: { name: "Test", lifecycle: "active", progress: { state: "unknown" }, sort_order: 0 },
});
const pending = (input = request): Wire.WorkstreamOperationResult =>
  Wire.parseWorkstreamOperationResult({
    contract: Wire.WORKSTREAM_OPERATIONS_CONTRACT,
    operation_id: input.operation_id,
    kind: input.kind,
    origin: input.origin,
    request_sha256: NodeCrypto.createHash("sha256").update(canonicalJson(input)).digest("hex"),
    observed: null,
    children: [],
    accepted_at: at,
    updated_at: at,
    result: null,
    detail: null,
    state: "reconciling",
    reason:
      input.kind === "create_workstream" ? "create_effect_unknown" : "membership_effect_unknown",
    poll_after_ms: 30_000,
  });
const readContext = { owner_id: home.owner_id, server_generation: 1, registry_version: 9 };
const sourceTrust = {
  environmentId: "member:test",
  authorityNamespace: "authority:local",
  storeGeneration: 1,
};
const primaryPlacement = (): Native.WorkstreamsNativeThreadPlacement => ({
  membership_id: "membership:one",
  workstream_id: "workstream:one",
  native_reference_id: "reference:one",
  kind: "primary",
  source_instance_id: "member:test",
  native_thread_id: "thread:test",
  attestation_version: 1,
  attested_at: at,
  expires_at: "2026-01-01T00:05:00.000Z",
  evidence_sha256: "a".repeat(64),
  source_binding_version: 1,
  authority_namespace: sourceTrust.authorityNamespace,
  store_generation: sourceTrust.storeGeneration,
  attestation_source: "native",
  evidence_contract: "workstreams-t3-provider/2.0.0",
});
const reference = (): NativeReference => ({
  native_reference_id: "reference:one",
  owner_id: home.owner_id,
  identity: {
    provider: "t3",
    source_instance_id: "member:test",
    resource_kind: "thread",
    id_kind: "internal",
    native_id: "thread:test",
    account_provenance: { kind: "not_account_scoped" },
  },
  pr_locator: null,
  registration: {
    state: "attested",
    attestation_version: 1,
    attested_at: at,
    expires_at: "2026-01-01T00:05:00.000Z",
    evidence: {
      provider: "t3",
      source_instance_id: "member:test",
      authority_namespace: sourceTrust.authorityNamespace,
      native_id: "thread:test",
      store_generation: 1,
      evidence_sha256: "a".repeat(64),
    },
  },
  created_at: at,
  created_by: { principal_id: "principal:test" },
  created_registry_version: 1,
});
const workstream: Workstream = {
  workstream_id: "workstream:one",
  owner_id: home.owner_id,
  name: "Current home name",
  lifecycle: "active",
  progress: { state: "unknown" },
  delivery: "unknown",
  freshness: "current",
  sort_order: 0,
  version: 1,
  created_at: at,
  updated_at: at,
  created_by: { principal_id: "principal:test" },
  updated_by: { principal_id: "principal:test" },
};
const readResult = (route: ReadResult["route"], value: unknown): ReadResult => ({
  route,
  authorization_revision: 4,
  envelope: {
    contract: "workstreams-read/1.0.0",
    registry: {
      registry_instance_id: home.registry_instance_id,
      owner_id: home.owner_id,
      canonical_origin: home.canonical_origin,
      server_generation: 1,
      registry_version: 9,
    },
    provenance: {
      authority: "native-home",
      home_environment_id: home.home_environment_id,
      served_by_environment_id: home.home_environment_id,
      source: "live",
      observed_at: at,
      stale: false,
    },
    coverage: { registry: "complete", sources: [] },
  },
  value,
});
const fixture = () => {
  const state = {
    accesses: 0,
    submissions: [] as Wire.WorkstreamOperationSubmitRequest[],
    reads: [] as string[],
    openCalls: 0,
    value: { request, result: pending() } as PublicOperation | null,
    open: [] as readonly PublicOperation[],
    result: pending(),
    error: null as RegistryError | null,
    alter: (value: Access.WorkstreamsHomeAccessResult, _call: number) => value,
    readQueries: [] as WorkstreamsNativeHomeRead[],
    placementItems: [] as Native.WorkstreamsNativeThreadPlacement[],
    referenceItems: [] as NativeReference[],
    trust: { readiness: "ready", trustedEnvironments: [] } as ReturnType<
      Authority.NativeStoreAuthority["Service"]["trustProvider"]["readTrustSnapshot"]
    >,
    coverage: [] as ReadResult["envelope"]["coverage"]["sources"],
    alterRead: (value: ReadResult, _call: number): ReadResult => value,
    afterAccess: (_call: number): Effect.Effect<void> => Effect.void,
  };
  // Application resources are explicit synthetic ports; combined H/M proves actual auth, storage and transport.
  const ports = Layer.mock(App.MemberApplication)({
    access: () =>
      Effect.gen(function* () {
        const call = ++state.accesses;
        yield* state.afterAccess(call);
        return state.alter(observation(), call);
      }),
    read: (query) =>
      Effect.gen(function* () {
        state.readQueries.push(query);
        let result: ReadResult;
        if (query.route === "placements")
          result = {
            ...readResult("placements", {
              inventory_sha256: NodeCrypto.createHash("sha256")
                .update(t3PlacementInventoryJson(query.params.identities))
                .digest("hex"),
              context: {
                ...readContext,
                principal_id: "principal:test",
                authorization_revision: 4,
              },
              provenance: {
                authority: "native-home",
                registry_instance_id: home.registry_instance_id,
                home_environment_id: home.home_environment_id,
                home_generation: 1,
                served_by_environment_id: home.home_environment_id,
                observed_at: at,
              },
              items: state.placementItems,
              next_cursor: null,
            }),
            placement_contract: "workstreams-t3-placement/2.0.0",
          };
        else if (query.route === "workstream")
          result = readResult("workstream", { context: readContext, workstream });
        else if (query.route === "references")
          result = readResult("references", {
            context: readContext,
            items: state.referenceItems,
            next_cursor: null,
          });
        else return yield* new RegistryError({ code: "invalid_request" });
        return state.alterRead(
          {
            ...result,
            envelope: {
              ...result.envelope,
              coverage: { registry: "complete", sources: state.coverage },
            },
          },
          state.readQueries.length,
        );
      }),
    submitOperation: (input) =>
      Effect.suspend(() => {
        state.submissions.push(input);
        return state.error ? Effect.fail(state.error) : Effect.succeed(state.result);
      }),
    readPublicOperation: (id) =>
      Effect.sync(() => {
        state.reads.push(id);
        return state.error ? null : state.value;
      }),
    openPublicOperations: () =>
      Effect.suspend(() => {
        state.openCalls++;
        return state.error ? Effect.fail(state.error) : Effect.succeed(state.open);
      }),
  });
  const layer = Member.layer.pipe(
    Layer.provide(ports),
    Layer.provide(
      Layer.mock(Authority.NativeStoreAuthority)({
        trustProvider: { readTrustSnapshot: () => state.trust },
      }),
    ),
    Layer.provide(
      Layer.succeed(Environment.ServerEnvironmentIdentity, {
        getEnvironmentId: Effect.succeed(EnvironmentId.make("member:test")),
      }),
    ),
  );
  return { state, layer };
};
it.effect(
  "preserves public request bytes/digest while allowing its own registry version advance",
  () => {
    const f = fixture();
    f.state.alter = (value, call) =>
      call === 1
        ? value
        : {
            ...value,
            home: { ...value.home, registry_version: 10 },
            capabilities: {
              ...value.capabilities,
              context: { ...value.capabilities.context, registry_version: 10 },
            },
          };
    return Effect.gen(function* () {
      const service = yield* Public.WorkstreamOperations;
      expect(yield* service.submit({ request, wait_ms: 0 })).toEqual(pending());
      expect(f.state.submissions).toEqual([{ request, wait_ms: 0 }]);
      expect(f.state.accesses).toBe(2);
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect("refuses a URL registry identity before forwarding", () => {
  const f = fixture();
  const input = {
    ...request,
    registry: { registry_id: home.canonical_origin, owner_id: home.owner_id },
  };
  return Effect.gen(function* () {
    expect(
      yield* (yield* Public.WorkstreamOperations)
        .submit({ request: input, wait_ms: 0 })
        .pipe(Effect.flip),
    ).toBeInstanceOf(Public.OperationInternalError);
    expect(f.state.submissions).toEqual([]);
  }).pipe(Effect.provide(f.layer));
});

it.effect.each(["registry", "owner", "read-only", "extra", "wait"] as const)(
  "refuses %s before accepting an operation",
  (problem) => {
    const f = fixture();
    const input = {
      request: {
        ...request,
        ...(problem === "registry"
          ? {
              registry: {
                registry_id: "22222222-2222-4222-8222-222222222222",
                owner_id: home.owner_id,
              },
            }
          : {}),
        ...(problem === "owner"
          ? { registry: { registry_id: home.registry_instance_id, owner_id: "foreign:owner" } }
          : {}),
      },
      wait_ms: problem === "wait" ? 3001 : 0,
      ...(problem === "extra" ? { route: "https://foreign.invalid" } : {}),
    };
    if (problem === "read-only")
      f.state.alter = (value) => ({
        ...value,
        capabilities: { ...value.capabilities, permissions: ["workstreams:read"] },
      });
    return Effect.gen(function* () {
      expect(
        yield* (yield* Public.WorkstreamOperations).submit(input).pipe(Effect.flip),
      ).toBeInstanceOf(Public.OperationInternalError);
      expect(f.state.submissions).toEqual([]);
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect.each(["operation_id_conflict", "thread_operation_in_progress"] as const)(
  "preserves the closed %s conflict without native causes",
  (code) => {
    const f = fixture();
    f.state.error = new RegistryError({ code, cause: "PRIVATE_SENTINEL" });
    return Effect.gen(function* () {
      const result = yield* (yield* Public.WorkstreamOperations)
        .submit({ request, wait_ms: 0 })
        .pipe(Effect.flip);
      expect(result).toEqual(new Public.OperationConflict({ code }));
      expect(String(result)).not.toContain("PRIVATE_SENTINEL");
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect.each(["principal", "grant", "owner", "generation", "ceiling", "regression"] as const)(
  "fences %s changes before returning a write result",
  (problem) => {
    const f = fixture();
    f.state.alter = (value, call) =>
      call === 1
        ? value
        : {
            ...value,
            ...(problem === "principal" ? { principal_id: "principal:replacement" } : {}),
            ...(problem === "grant" ? { authorization_revision: 5 } : {}),
            home: {
              ...value.home,
              ...(problem === "owner" ? { owner_id: "owner:replacement" } : {}),
              ...(problem === "generation" ? { server_generation: 2 } : {}),
              ...(problem === "regression" ? { registry_version: 8 } : {}),
            },
            capabilities: {
              ...value.capabilities,
              ...(problem === "ceiling" ? { permissions: ["workstreams:read"] } : {}),
              context: {
                ...value.capabilities.context,
                ...(problem === "owner" ? { owner_id: "owner:replacement" } : {}),
                ...(problem === "generation" ? { server_generation: 2 } : {}),
                ...(problem === "regression" ? { registry_version: 8 } : {}),
              },
            },
          };
    return Effect.gen(function* () {
      expect(
        yield* (yield* Public.WorkstreamOperations)
          .submit({ request, wait_ms: 0 })
          .pipe(Effect.flip),
      ).toBeInstanceOf(Public.OperationInternalError);
      expect(f.state.submissions).toHaveLength(1);
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect.each(["id", "digest", "kind"] as const)(
  "rejects substituted public result %s",
  (problem) => {
    const f = fixture();
    f.state.result = Wire.parseWorkstreamOperationResult({
      ...pending(),
      ...(problem === "id" ? { operation_id: "operation:replacement" } : {}),
      ...(problem === "digest" ? { request_sha256: "a".repeat(64) } : {}),
      ...(problem === "kind" ? { kind: "set_primary", reason: "membership_effect_unknown" } : {}),
    });
    return Effect.gen(function* () {
      expect(
        yield* (yield* Public.WorkstreamOperations)
          .submit({ request, wait_ms: 0 })
          .pipe(Effect.flip),
      ).toBeInstanceOf(Public.OperationInternalError);
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect("reads pending intent and unknown locally without dispatch or recovery", () => {
  const f = fixture();
  f.state.open = [{ request, result: pending() }];
  return Effect.gen(function* () {
    const service = yield* Public.WorkstreamOperations;
    expect(yield* service.read({ operation_id: request.operation_id })).toEqual({
      operation: pending(),
    });
    expect(yield* service.read({ open: true })).toEqual({ operations: [pending()] });
    f.state.value = null;
    expect(yield* service.read({ operation_id: "operation:unknown" })).toEqual({ operation: null });
    expect(f.state.submissions).toEqual([]);
    expect(yield* service.resumeRecorded(request.operation_id).pipe(Effect.flip)).toMatchObject({
      code: "missing",
    });
  }).pipe(Effect.provide(f.layer));
});
it.effect("does not release reads after home revision drift or retain a result cache", () => {
  const f = fixture();
  f.state.alter = (value, call) => (call === 2 ? { ...value, authorization_revision: 5 } : value);
  return Effect.gen(function* () {
    const service = yield* Public.WorkstreamOperations;
    expect(yield* service.read({ operation_id: request.operation_id })).toEqual({
      operation: null,
    });
    expect(yield* service.read({ operation_id: request.operation_id })).toEqual({
      operation: pending(),
    });
    expect(f.state.reads).toHaveLength(2);
  }).pipe(Effect.provide(f.layer));
});
it.effect(
  "thread reads retain unverified assignment and only their matching pending operation",
  () => {
    const f = fixture();
    const primary = Wire.parseWorkstreamOperationRequest({
      contract: Wire.WORKSTREAM_OPERATIONS_CONTRACT,
      operation_id: "operation:primary-01",
      origin: { kind: "app" },
      kind: "set_primary",
      thread: { environment_id: "member:test", thread_id: "thread:test" },
      target: {
        kind: "unassigned",
        registry_id: home.registry_instance_id,
        owner_id: home.owner_id,
      },
      expected: { kind: "any" },
    });
    f.state.open = [
      { request: primary, result: pending(primary) },
      { request, result: pending() },
    ];
    return Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse(at));
      const service = yield* Public.WorkstreamOperations;
      expect(
        yield* service.read({
          thread: { environment_id: "member:test", thread_id: "thread:test" },
        }),
      ).toEqual({ assignment: { kind: "unverified" }, open_operation: pending(primary) });
      expect(
        yield* service.read({
          thread: { environment_id: "member:test", thread_id: "thread:other" },
        }),
      ).toEqual({ assignment: { kind: "unverified" }, open_operation: null });
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect(
  "setup observes current access without legacy activation instructions or placement claims",
  () => {
    const f = fixture();
    return Effect.gen(function* () {
      const service = yield* Public.WorkstreamOperations;
      const status = yield* service.setupStatus();
      expect(isSetupStatus(status)).toBe(true);
      expect(status).toMatchObject({
        activation: { state: "enabled", missing_keys: [] },
        registry: { registry_id: home.registry_instance_id, can_write: true },
        t3_source: { state: "ready" },
        placements: { readiness: "trust-provider-required" },
        authority_effect: "none",
        next_steps: [],
      });
      f.state.error = new RegistryError({ code: "forbidden", cause: "PRIVATE_SENTINEL" });
      expect(yield* service.setupStatus()).toMatchObject({
        activation: { state: "invalid" },
        registry: { state: "unavailable", registry_id: null, can_write: false },
      });
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect(
  "pending projection requires an accepted matching intent and never invents terminal outcomes",
  () =>
    Effect.gen(function* () {
      const view = {
        operation_id: request.operation_id,
        state: "accepted" as const,
        next_poll_at: null,
        accepted_at: at,
        updated_at: at,
        result: null,
      };
      expect(yield* projectOperation(request, view)).toEqual(pending());
      expect(
        (yield* Effect.result(
          projectOperation(request, { ...view, operation_id: "operation:other" }),
        ))._tag,
      ).toBe("Failure");
      expect(
        (yield* Effect.result(projectOperation(request, { ...view, state: "committed" })))._tag,
      ).toBe("Failure");
    }),
);

// Synthetic application port proves only adapter selection; actual peer qualification lives in the combined harness.
it.effect.each(["native-client", "own-agent"] as const)(
  "linked placement uses the target-qualified application path without generic access bootstrap: %s",
  (callerKind) => {
    const f = fixture();
    const placement = Wire.parseWorkstreamOperationRequest({
      contract: Wire.WORKSTREAM_OPERATIONS_CONTRACT,
      operation_id: "operation:linked-adapter-01",
      kind: "set_primary",
      origin: { kind: "agent" },
      thread: { environment_id: "member:test", thread_id: "thread:member" },
      target: {
        kind: "unassigned",
        registry_id: home.registry_instance_id,
        owner_id: home.owner_id,
      },
      expected: { kind: "any" },
    });
    f.state.result = pending(placement);
    const invocation: Mcp.McpInvocationScope = {
      environmentId: EnvironmentId.make("member:test"),
      requestNamespace: "link-original",
      issuedAt: 1,
      capabilities: new Set(["orchestration"]),
      thread:
        callerKind === "own-agent"
          ? {
              threadId: ThreadId.make("thread:member"),
              providerSessionId: "original-credential",
              providerInstanceId: ProviderInstanceId.make("codex"),
            }
          : undefined,
      client:
        callerKind === "native-client"
          ? { sessionId: "link:home", label: "fixture", access: "full-access", linked: true }
          : undefined,
    };
    return Effect.gen(function* () {
      const publicOperations = yield* Public.WorkstreamOperations;
      expect(yield* publicOperations.submit({ request: placement, wait_ms: 0 })).toEqual(
        pending(placement),
      );
      expect(f.state.accesses).toBe(0);
      expect(f.state.submissions).toEqual([{ request: placement, wait_ms: 0 }]);
      f.state.error = new RegistryError({ code: "not_granted" });
      expect(
        yield* publicOperations.submit({ request: placement, wait_ms: 0 }).pipe(Effect.flip),
      ).toBeInstanceOf(Public.OperationInternalError);
    }).pipe(Effect.provide(f.layer), Effect.provideService(Mcp.McpInvocationContext, invocation));
  },
);

const verifiedFixture = () => {
  const f = fixture();
  f.state.trust = { readiness: "ready", trustedEnvironments: [sourceTrust] };
  f.state.coverage = [
    {
      source_instance_id: "member:test",
      mapping: "mapped",
      attestation: "current",
      observed_at: at,
    },
  ];
  f.state.placementItems = [primaryPlacement()];
  f.state.referenceItems = [reference()];
  return f;
};
const threadRead = { thread: { environment_id: "member:test", thread_id: "thread:test" } };
it.effect(
  "thread assignment comes from fresh native placement and home detail, independently of retained outcomes",
  () => {
    const f = verifiedFixture();
    f.state.value = null;
    return Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse(at));
      const service = yield* Public.WorkstreamOperations;
      expect(yield* service.read(threadRead)).toEqual({
        assignment: { kind: "primary", workstream_id: "workstream:one", name: "Current home name" },
        open_operation: null,
      });
      expect(f.state.readQueries.map((q) => q.route)).toEqual(["placements", "workstream"]);
      expect(f.state.submissions).toEqual([]);
      expect(f.state.reads).toEqual([]);
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect(
  "empty placements require a current independently trusted exact reference before reporting unassigned",
  () => {
    const f = verifiedFixture();
    f.state.placementItems = [];
    return Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse(at));
      const service = yield* Public.WorkstreamOperations;
      expect(yield* service.read(threadRead)).toEqual({
        assignment: { kind: "unassigned" },
        open_operation: null,
      });
      f.state.referenceItems = [];
      expect(yield* service.read(threadRead)).toEqual({
        assignment: { kind: "unverified" },
        open_operation: null,
      });
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect.each([
  "no-trust",
  "not-ready",
  "source-unknown",
  "source-expired",
  "source-unmapped",
  "source-future",
  "namespace",
  "generation",
] as const)("thread assignment remains unverified with %s evidence", (fault) => {
  const f = verifiedFixture();
  if (fault === "no-trust") f.state.trust = { readiness: "ready", trustedEnvironments: [] };
  if (fault === "not-ready")
    f.state.trust = { readiness: "trust-provider-required", trustedEnvironments: [] };
  if (fault === "source-unknown")
    f.state.coverage = [
      {
        source_instance_id: "member:test",
        mapping: "mapped",
        attestation: "unknown",
        observed_at: null,
      },
    ];
  if (fault === "source-expired")
    f.state.coverage = [
      {
        source_instance_id: "member:test",
        mapping: "mapped",
        attestation: "expired",
        observed_at: null,
      },
    ];
  if (fault === "source-unmapped")
    f.state.coverage = [
      {
        source_instance_id: "member:test",
        mapping: "unmapped",
        attestation: "unknown",
        observed_at: null,
      },
    ];
  if (fault === "source-future")
    f.state.coverage = [
      {
        source_instance_id: "member:test",
        mapping: "mapped",
        attestation: "current",
        observed_at: "2026-01-01T00:00:00.000001Z",
      },
    ];
  if (fault === "namespace")
    f.state.placementItems = [
      { ...primaryPlacement(), authority_namespace: "authority:replacement" },
    ];
  if (fault === "generation")
    f.state.placementItems = [{ ...primaryPlacement(), store_generation: 2 }];
  return Effect.gen(function* () {
    yield* TestClock.setTime(Date.parse(at));
    expect(yield* (yield* Public.WorkstreamOperations).read(threadRead)).toEqual({
      assignment: { kind: "unverified" },
      open_operation: null,
    });
    expect(f.state.readQueries.map((q) => q.route)).toEqual(["placements"]);
    expect(f.state.submissions).toEqual([]);
  }).pipe(Effect.provide(f.layer));
});
it.effect.each([
  "expired",
  "future",
  "namespace",
  "generation",
  "foreign-id",
  "quarantined",
] as const)("empty placement cannot promote %s reference into unassigned", (fault) => {
  const f = verifiedFixture();
  f.state.placementItems = [];
  const r = reference();
  if (fault === "expired")
    f.state.referenceItems = [{ ...r, registration: { ...r.registration, expires_at: at } }];
  if (fault === "future")
    f.state.referenceItems = [
      { ...r, registration: { ...r.registration, attested_at: "2026-01-01T00:00:00.000001Z" } },
    ];
  if (fault === "namespace" || fault === "generation")
    f.state.referenceItems = [
      {
        ...r,
        registration: {
          ...r.registration,
          evidence: {
            ...r.registration.evidence!,
            ...(fault === "namespace"
              ? { authority_namespace: "authority:foreign" }
              : { store_generation: 2 }),
          },
        },
      },
    ];
  if (fault === "foreign-id")
    f.state.referenceItems = [{ ...r, identity: { ...r.identity, native_id: "thread:other" } }];
  if (fault === "quarantined")
    f.state.referenceItems = [
      {
        ...r,
        registration: {
          state: "quarantined",
          attestation_version: 0,
          attested_at: null,
          expires_at: null,
          evidence: null,
        },
      },
    ];
  return Effect.gen(function* () {
    yield* TestClock.setTime(Date.parse(at));
    expect(yield* (yield* Public.WorkstreamOperations).read(threadRead)).toEqual({
      assignment: { kind: "unverified" },
      open_operation: null,
    });
  }).pipe(Effect.provide(f.layer));
});
it.effect(
  "unassigned lookup completes coherent reference paging instead of interpreting its first empty page as absence",
  () => {
    const f = verifiedFixture();
    f.state.placementItems = [];
    f.state.alterRead = (value) => {
      if (value.route !== "references") return value;
      const query = f.state.readQueries.at(-1),
        continued = query?.route === "references" && query.params.cursor !== undefined;
      return {
        ...value,
        value: {
          context: readContext,
          items: continued ? [reference()] : [],
          next_cursor: continued ? null : "next_reference",
        },
      };
    };
    return Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse(at));
      expect(yield* (yield* Public.WorkstreamOperations).read(threadRead)).toEqual({
        assignment: { kind: "unassigned" },
        open_operation: null,
      });
      expect(f.state.readQueries).toHaveLength(3);
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect.each(["home", "detail-id", "grant", "cursor-cycle", "reference-snapshot"] as const)(
  "assignment read fails closed on %s substitution",
  (fault) => {
    const f = verifiedFixture();
    if (fault === "grant")
      f.state.alter = (value, call) =>
        call === 4 ? { ...value, authorization_revision: 5 } : value;
    if (fault === "home")
      f.state.alterRead = (value) => ({
        ...value,
        envelope: {
          ...value.envelope,
          registry: { ...value.envelope.registry, canonical_origin: "https://foreign.invalid" },
        },
      });
    if (fault === "detail-id")
      f.state.alterRead = (value) =>
        value.route !== "workstream"
          ? value
          : {
              ...value,
              value: {
                context: readContext,
                workstream: { ...workstream, workstream_id: "workstream:other" },
              },
            };
    if (fault === "cursor-cycle" || fault === "reference-snapshot") {
      f.state.placementItems = [];
      f.state.alterRead = (value, call) =>
        value.route !== "references"
          ? value
          : {
              ...value,
              envelope: {
                ...value.envelope,
                provenance: {
                  ...value.envelope.provenance,
                  observed_at:
                    fault === "reference-snapshot" && call > 2 ? "2026-01-01T00:00:00.000001Z" : at,
                },
              },
              value: { context: readContext, items: [], next_cursor: "same_cursor" },
            };
    }
    return Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse("2026-01-01T00:00:01.000Z"));
      expect(yield* (yield* Public.WorkstreamOperations).read(threadRead)).toEqual({
        assignment: { kind: "unavailable", reason: "offline" },
        open_operation: null,
      });
      expect(f.state.submissions).toEqual([]);
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect.each(["expires", "trust-change"] as const)(
  "assignment is withheld when %s occurs during final authorization",
  (fault) => {
    const f = verifiedFixture();
    const entries = [{ ...sourceTrust }];
    f.state.trust = { readiness: "ready", trustedEnvironments: entries };
    f.state.afterAccess = (call) =>
      call !== 4
        ? Effect.void
        : fault === "expires"
          ? TestClock.setTime(Date.parse("2026-01-01T00:05:00.000Z"))
          : Effect.sync(() => {
              // Mutate the same array to ensure the captured trust comparison cannot alias live provider data.
              entries.splice(0);
            });
    return Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse(at));
      expect(yield* (yield* Public.WorkstreamOperations).read(threadRead)).toEqual({
        assignment: { kind: "unverified" },
        open_operation: null,
      });
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect("foreign environment assignment reads never select a home route or source", () => {
  const f = verifiedFixture();
  return Effect.gen(function* () {
    yield* TestClock.setTime(Date.parse(at));
    expect(
      yield* (yield* Public.WorkstreamOperations).read({
        thread: { environment_id: "foreign:test", thread_id: "thread:test" },
      }),
    ).toEqual({ assignment: { kind: "unavailable", reason: "offline" }, open_operation: null });
    expect(f.state.readQueries).toEqual([]);
  }).pipe(Effect.provide(f.layer));
});
