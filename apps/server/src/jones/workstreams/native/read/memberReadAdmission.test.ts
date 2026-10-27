import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Admission from "../runtime/memberAdmission.ts";
import * as Current from "./memberReadAdmission.ts";
import * as Member from "./memberRead.ts";
import * as Protocol from "../forwarding/homeReadProtocol.ts";
import {
  EnvironmentId,
  ThreadId,
  ProviderInstanceId,
  t3PlacementInventoryJson,
} from "@t3tools/contracts";
import { sha256Bytes } from "../../nativeProvider/service.ts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import { RegistryError } from "../registry/session.ts";
const home = {
  registry_instance_id: "11111111-1111-4111-8111-111111111111",
  owner_id: "owner:test",
  home_environment_id: "home:test",
  server_generation: 1,
};
const record: Admission.MemberRecordedCallerV1 = {
  schema: "jones-workstreams-member-recorded-caller/1",
  local: {
    schema: "jones-workstreams-member-local-caller/1",
    auth: { kind: "bearer", session_id: "local", subject: "owner", method: "bearer-access-token" },
    origin: { kind: "app" },
    ceiling: {
      permissions: ["workstreams:read"],
      scopes: ["orchestration:read"],
      capabilities: [],
      limits: { runtimeMode: "approval-required", interactionMode: "default" },
    },
    session: { issued_at: 0, expires_at: 10000 },
    environment_id: "member:test",
    request_namespace: null,
    invocation_issued_at: null,
    link_origin: null,
    link_session: null,
    captured_at: 0,
  },
  association: {
    enrollment_id: "enrollment",
    enrollment_session_id: "session",
    intent_sha256: "a".repeat(64),
    home,
    canonical_origin: "https://home.invalid",
    principal_id: "principal",
    singleton_binding_version: 1,
    pin_bound_revision: 2,
    source_binding_version: 1,
    source: {
      environment_id: "member:test",
      authority_namespace: "namespace",
      store_generation: 1,
    },
    link_generation: "opaque-original",
  },
};
const envelope = {
  contract: "workstreams-read/1.0.0",
  registry: {
    registry_instance_id: home.registry_instance_id,
    owner_id: home.owner_id,
    server_generation: home.server_generation,
    canonical_origin: "https://home.invalid",
    registry_version: 10,
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
};
const result = () => ({
  contract: "workstreams-native-home-read-result/1.0.0",
  route: "counts",
  envelope: structuredClone(envelope),
  authorization_revision: 3,
  value: {
    context: { owner_id: "owner:test", server_generation: 1, registry_version: 10 },
    principal_id: "principal",
    observed_at: envelope.provenance.observed_at,
    authority_effect: "none",
    total: 1,
    active: 1,
    unknown_lifecycle: 0,
  },
});
// Synthetic remote results and local admission, with real strict protocol/permission/MemberReads layers.
const fixture = (alter: (r: ReturnType<typeof result>, call: number) => void = () => {}) => {
  let calls = 0,
    revoked = false;
  const routes: string[] = [];
  const ports = Layer.mergeAll(
    Layer.mock(Admission.MemberAdmission)({
      current: () =>
        Effect.succeed({
          record,
          limits: record.local.ceiling.limits!,
          checkCurrent: () =>
            revoked
              ? Effect.fail(new RegistryError({ code: "forbidden" }))
              : Effect.succeed(record.local.ceiling.limits!),
        }),
    }),
    Layer.succeed(Protocol.HomeReadResultClient, {
      read: (_home, request) =>
        Effect.gen(function* () {
          const original = yield* Effect.serviceOption(Admission.RetainedMemberCaller);
          expect(Option.isSome(original) ? original.value : null).toEqual(record);
          return yield* Effect.sync(() => {
            routes.push(request.request.route);
            const value = result();
            alter(value, ++calls);
            return value;
          });
        }),
    }),
  );
  const permission = Current.layer.pipe(
    Layer.provide(Protocol.homeReadPortLayer.pipe(Layer.provide(ports))),
    Layer.provide(ports),
  );
  const request = Layer.mergeAll(Current.admissionLayer, Current.portLayer).pipe(
    Layer.provideMerge(permission),
  );
  return {
    request,
    member: Member.layer.pipe(Layer.provideMerge(request)),
    calls: () => calls,
    routes,
    revoke: () => {
      revoked = true;
    },
  };
};
it.effect(
  "acquires one fixed coherent counts permission before MemberReads without recursion",
  () => {
    const f = fixture();
    return Effect.gen(function* () {
      const admission = yield* Current.CurrentMemberRead;
      expect(f.calls()).toBe(1);
      expect(admission.admission.authorization_revision).toBe(3);
      const read = yield* (yield* Member.MemberReads).read({ route: "counts", params: {} });
      expect(read.route).toBe("counts");
      expect(f.routes).toEqual(["counts", "counts"]);
      expect(f.calls()).toBe(2);
    }).pipe(Effect.provide(f.member));
  },
);
it.effect.each([
  "home",
  "owner",
  "origin",
  "serving",
  "generation",
  "principal",
  "incomplete",
  "stale",
  "context",
  "observed",
] as const)("refuses wrong initial %s context", (change) => {
  const f = fixture((r) => {
    if (change === "home") r.envelope.provenance.home_environment_id = "other";
    if (change === "owner") r.envelope.registry.owner_id = "other";
    if (change === "origin") r.envelope.registry.canonical_origin = "https://other.invalid";
    if (change === "serving") r.envelope.provenance.served_by_environment_id = "other";
    if (change === "generation") r.envelope.registry.server_generation = 2;
    if (change === "principal") r.value.principal_id = "other";
    if (change === "incomplete") r.envelope.coverage.registry = "unavailable";
    if (change === "stale") r.envelope.provenance.stale = true;
    if (change === "context") r.value.context.registry_version++;
    if (change === "observed") r.value.observed_at = "2026-01-01T00:00:01Z";
  });
  return Effect.gen(function* () {
    yield* Current.CurrentMemberRead;
  }).pipe(
    Effect.provide(f.request),
    Effect.result,
    Effect.map((r) => {
      expect(r._tag).toBe("Failure");
      expect(f.calls()).toBe(1);
    }),
  );
});
it.effect.each(["revision", "origin", "principal", "revocation"] as const)(
  "fences %s changes during subsequent member read",
  (change) => {
    let revoke = () => {};
    const f = fixture((r, n) => {
      if (n > 1 && change === "revision") r.authorization_revision++;
      if (n > 1 && change === "origin")
        r.envelope.registry.canonical_origin = "https://other.invalid";
      if (n > 1 && change === "principal") r.value.principal_id = "other";
      if (n > 1 && change === "revocation") revoke();
    });
    revoke = f.revoke;
    return Effect.gen(function* () {
      expect(
        yield* (yield* Member.MemberReads)
          .read({ route: "counts", params: {} })
          .pipe(Effect.result),
      ).toMatchObject({ _tag: "Failure" });
      expect(f.calls()).toBe(2);
    }).pipe(Effect.provide(f.member));
  },
);

it.effect.each([
  "own",
  "other-thread",
  "context",
  "principal",
  "origin",
  "generic",
  "cloned-proof",
] as const)("private own-thread placement proof rejects %s widening", (change) => {
  const original: Admission.MemberRecordedCallerV1 = {
    ...record,
    local: {
      ...record.local,
      auth: {
        kind: "agent",
        calling_thread_id: "own-thread",
        provider_instance_id: "codex",
        mcp_credential_id: "credential",
        run_id: "original-run",
        provider_thread_id: "original-provider-thread",
        provider_session_id: "original-session",
      },
      session: null,
      link_origin: "opaque-original-link",
    },
  };
  const scope: Mcp.McpInvocationScope = {
    environmentId: EnvironmentId.make("member:test"),
    capabilities: new Set(["orchestration"]),
    issuedAt: 1,
    requestNamespace: "own",
    client: undefined,
    thread: {
      threadId: ThreadId.make("own-thread"),
      providerSessionId: "credential",
      providerInstanceId: ProviderInstanceId.make("codex"),
    },
  };
  let calls = 0;
  const ports = Layer.mergeAll(
    Layer.mock(Admission.MemberAdmission)({
      captureOwnPlacement: () => Effect.succeed(original),
      readmitOwnPlacement: () =>
        Effect.succeed({
          record: original,
          limits: record.local.ceiling.limits!,
          checkCurrent: () => Effect.succeed(record.local.ceiling.limits!),
        }),
    }),
    Layer.succeed(Protocol.HomeReadResultClient, {
      read: (home, request) =>
        Effect.gen(function* () {
          calls++;
          expect(request.request).toEqual({
            route: "placements",
            params: {
              identities: [{ source_instance_id: "member:test", native_thread_id: "own-thread" }],
            },
          });
          const tested =
            change === "generic"
              ? { ...request, request: { route: "counts" as const, params: {} } }
              : change === "other-thread"
                ? {
                    ...request,
                    request: {
                      route: "placements" as const,
                      params: {
                        identities: [
                          { source_instance_id: "member:test", native_thread_id: "other-thread" },
                        ],
                      },
                    },
                  }
                : request;
          const proof = yield* Current.checkOwnPlacementForwarding(home, tested).pipe(
            change === "cloned-proof"
              ? Effect.provideService(Mcp.McpInvocationContext, { ...scope })
              : (effect) => effect,
            Effect.result,
          );
          if (["generic", "other-thread", "cloned-proof"].includes(change)) {
            expect(proof._tag).toBe("Failure");
            return yield* new RegistryError({ code: "forbidden" });
          }
          expect(proof._tag).toBe("Success");
          const ids = [{ source_instance_id: "member:test", native_thread_id: "own-thread" }];
          return {
            contract: "workstreams-native-home-read-result/1.0.0",
            route: "placements",
            placement_contract: "workstreams-t3-placement/2.0.0",
            envelope,
            authorization_revision: 3,
            value: {
              inventory_sha256: sha256Bytes(t3PlacementInventoryJson(ids)),
              context: {
                owner_id: "owner:test",
                server_generation: 1,
                registry_version: change === "context" ? 11 : 10,
                principal_id: change === "principal" ? "other" : "principal",
                authorization_revision: 3,
              },
              provenance: {
                authority: "native-home",
                registry_instance_id: home.registry_instance_id,
                home_environment_id: "home:test",
                home_generation: 1,
                served_by_environment_id: "home:test",
                observed_at: envelope.provenance.observed_at,
              },
              items: [],
              next_cursor: null,
            },
            ...(change === "origin"
              ? {
                  envelope: {
                    ...envelope,
                    registry: { ...envelope.registry, canonical_origin: "https://other.invalid" },
                  },
                }
              : {}),
          };
        }),
    }),
  );
  return Effect.gen(function* () {
    expect(
      yield* Current.checkOwnPlacementForwarding(home, {
        contract: "workstreams-native-home-read-result/1.0.0",
        request: { route: "counts", params: {} },
      }).pipe(Effect.result),
    ).toMatchObject({ _tag: "Failure" });
    const result = yield* (yield* Current.MemberOwnThreadPlacements).read().pipe(Effect.result);
    expect(result._tag).toBe(change === "own" ? "Success" : "Failure");
    expect(calls).toBe(1);
  }).pipe(
    Effect.provide(Current.ownPlacementLayer.pipe(Layer.provide(ports))),
    Effect.provideService(Mcp.McpInvocationContext, scope),
  );
});
