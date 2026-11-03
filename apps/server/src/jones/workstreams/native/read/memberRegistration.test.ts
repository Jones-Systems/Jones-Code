import { expect, it } from "@effect/vitest";
import { AuthSessionId, EnvironmentId, OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Wire from "@t3tools/contracts/jones/workstreamsHomeRegistration";
import * as Access from "@t3tools/contracts/jones/workstreamsHomeAccess";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Peer from "../../../../peer/PeerMcpClient.ts";
import * as Admission from "../runtime/memberAdmission.ts";
import * as Members from "../runtime/memberEnrollment.ts";
import * as Startup from "../runtime/startup.ts";
import * as Provider from "../providers/localAttestation.ts";
import * as ProviderTest from "../../nativeProvider/testFixtures.ts";
import { makeWorkstreamsNativeProvider } from "../../nativeProvider/service.ts";
import { workstreamCapabilities } from "../registry/capabilities.ts";
import { RegistryError } from "../registry/session.ts";
import { HOME_REGISTRATION_TOOL } from "../forwarding/homeForwardPort.ts";
import * as Registration from "./memberRegistration.ts";

// External admission and transport are synthetic here. The combined H/M test supplies genuine owners.
const binding = ProviderTest.binding;
const home = {
  registry_instance_id: "11111111-1111-4111-8111-111111111111",
  owner_id: binding.owner_id,
  home_environment_id: "home:test",
  server_generation: 1,
};
const limits = { runtimeMode: "approval-required" as const, interactionMode: "default" as const };
const record: Admission.MemberRecordedCallerV1 = {
  schema: "jones-workstreams-member-recorded-caller/1",
  local: {
    schema: "jones-workstreams-member-local-caller/1",
    auth: {
      kind: "bearer",
      session_id: "original",
      subject: "owner",
      method: "bearer-access-token",
    },
    origin: { kind: "app" },
    ceiling: {
      permissions: ["workstreams:read"],
      scopes: ["orchestration:read"],
      capabilities: [],
      limits,
    },
    session: { issued_at: 0, expires_at: 10000 },
    environment_id: binding.source_instance_id,
    request_namespace: null,
    invocation_issued_at: null,
    link_origin: null,
    link_session: null,
    captured_at: 0,
  },
  association: {
    home,
    canonical_origin: binding.registry_origin,
    principal_id: binding.principal_id,
    enrollment_id: binding.enrollment_id,
    enrollment_session_id: binding.session_id,
    intent_sha256: "a".repeat(64),
    singleton_binding_version: 1,
    pin_bound_revision: 2,
    source_binding_version: 1,
    source: {
      environment_id: binding.source_instance_id,
      authority_namespace: binding.authority_namespace,
      store_generation: binding.store_generation,
    },
    link_generation: "opaque:member-to-home",
  },
};
const observedBuild = { ...binding.build, sha: "c".repeat(40), tree: "d".repeat(40) };
const response = (): Wire.WorkstreamsHomeRegistrationResult => ({
  contract: Wire.WORKSTREAMS_HOME_REGISTRATION_CONTRACT,
  grant_id: "actual-home-grant",
  access: {
    contract: Access.WORKSTREAMS_HOME_ACCESS_CONTRACT,
    home: { ...home, canonical_origin: binding.registry_origin, registry_version: 7 },
    principal_id: binding.principal_id,
    authorization_revision: 4,
    observed_at: ProviderTest.now,
    capabilities: workstreamCapabilities(
      { owner_id: home.owner_id, server_generation: 1, registry_version: 7 },
      ["workstreams:read"],
    ),
  },
});
const fixture = (
  options: {
    alterHome?: (r: Wire.WorkstreamsHomeRegistrationResult, call: number) => unknown;
    bindingFault?: "missing" | "owner" | "session" | "source" | "origin";
    sourceFault?: "unavailable" | "rejected" | "foreign" | "short-build";
    currentFault?: "before" | "after-source" | "limits";
  } = {},
) => {
  const calls: Array<Parameters<Peer.PeerMcpClient["Service"]["call"]>[0]> = [];
  let captures = 0,
    contexts = 0,
    checks = 0;
  const native = makeWorkstreamsNativeProvider(
    {
      ...ProviderTest.makeProviderFixture().ports,
      build: Effect.succeed(Option.some(observedBuild)),
    },
    "v2",
  );
  const enrolled =
    options.bindingFault === "owner"
      ? { ...binding, owner_id: "other" }
      : options.bindingFault === "session"
        ? { ...binding, session_id: "replacement" }
        : options.bindingFault === "source"
          ? { ...binding, source_instance_id: "other" }
          : options.bindingFault === "origin"
            ? { ...binding, registry_origin: "https://other.invalid" }
            : binding;
  const source = Effect.fn("RegistrationTest.context")(function* (input: typeof binding) {
    contexts++;
    const result = yield* native.context(input);
    if (options.sourceFault === "unavailable")
      return {
        protocol: "workstreams-t3-provider/2.0.0",
        state: "unavailable",
        reason: "build_unavailable",
      } as const;
    if (options.sourceFault === "rejected")
      return {
        protocol: "workstreams-t3-provider/2.0.0",
        state: "rejected",
        reason: "caller_mismatch",
      } as const;
    if (result.state !== "ready") return result;
    if (options.sourceFault === "foreign")
      return { ...result, enrollment: { ...result.enrollment, principal_id: "other" } };
    return options.sourceFault === "short-build"
      ? { ...result, observed_build: { ...result.observed_build, sha: "short" } }
      : result;
  });
  const ports = Layer.mergeAll(
    Layer.succeed(Startup.Startup, {
      mode: "native-member",
      problem: null,
      config: null,
      home: null,
      sourceSessionId: binding.session_id,
    }),
    Layer.mock(Members.MemberEnrollments)({
      getBySessionId: (id) =>
        Effect.succeed(
          id === binding.session_id && options.bindingFault !== "missing"
            ? Option.some(enrolled)
            : Option.none(),
        ),
    }),
    Layer.mock(Admission.MemberAdmission)({
      capture: () =>
        Effect.sync(() => {
          captures++;
          return record;
        }),
      readmit: () =>
        Effect.succeed({
          record,
          limits,
          checkCurrent: () =>
            Effect.suspend(() => {
              checks++;
              if (
                options.currentFault === "before" ||
                (options.currentFault === "after-source" && contexts > 0)
              )
                return Effect.fail(new RegistryError({ code: "forbidden" }));
              return Effect.succeed(
                options.currentFault === "limits" && checks > 1
                  ? { ...limits, interactionMode: "plan" as const }
                  : limits,
              );
            }),
        }),
    }),
    Layer.succeed(Provider.NativeProvider, { ...native, context: source }),
    Layer.succeed(Peer.PeerMcpClient, {
      call: (input) => {
        calls.push(input);
        // oxlint-disable-next-line t3code/no-inline-schema-compile -- The external peer applies the exact fixed client's supplied decoder.
        return Schema.decodeUnknownEffect(input.success)(
          options.alterHome ? options.alterHome(response(), calls.length) : response(),
        ).pipe(
          Effect.mapError(
            () =>
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "Synthetic protocol refusal.",
              }),
          ),
        );
      },
    }),
  );
  return {
    layer: Registration.layer.pipe(Layer.provide(ports)),
    calls,
    contexts: () => contexts,
    captures: () => captures,
  };
};
it.effect("projects the real home grant and actual source build on every fresh request", () => {
  const f = fixture();
  return Effect.gen(function* () {
    const service = yield* Registration.MemberRegistration;
    for (let i = 0; i < 2; i++)
      expect(yield* service.read()).toMatchObject({
        context: {
          protocol: "workstreams-registration-context/2.0.0",
          grant_id: "actual-home-grant",
          owner_id: home.owner_id,
          principal_id: binding.principal_id,
          authorization_revision: 4,
          sources: [
            {
              source_instance_id: binding.source_instance_id,
              build: observedBuild,
              native_protocol: binding.protocol,
              account_provenance: { kind: "not_account_scoped" },
            },
          ],
        },
        t3Availability: { state: "ready" },
      });
    expect(f.contexts()).toBe(2);
    expect(f.captures()).toBe(2);
    expect(f.calls).toHaveLength(4);
    for (const call of f.calls) {
      expect(call).toMatchObject({
        tool: HOME_REGISTRATION_TOOL,
        environmentId: "home:test",
        expectedGeneration: record.association.link_generation,
        limits,
        arguments: { contract: Wire.WORKSTREAMS_HOME_REGISTRATION_CONTRACT, expected_home: home },
      });
      expect(call).not.toHaveProperty("origin");
    }
  }).pipe(Effect.provide(f.layer));
});
it.effect.each(["missing", "owner", "session", "source", "origin"] as const)(
  "refuses %s enrollment without sending a home call",
  (bindingFault) => {
    const f = fixture({ bindingFault });
    return Effect.gen(function* () {
      expect(
        (yield* (yield* Registration.MemberRegistration).read().pipe(Effect.result))._tag,
      ).toBe("Failure");
      expect(f.calls).toHaveLength(0);
      expect(f.contexts()).toBe(0);
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect.each(["before", "after-source", "limits"] as const)(
  "refuses %s local caller currentness drift",
  (currentFault) => {
    const f = fixture({ currentFault });
    return Effect.gen(function* () {
      expect(
        (yield* (yield* Registration.MemberRegistration).read().pipe(Effect.result))._tag,
      ).toBe("Failure");
      expect(f.calls.length).toBeLessThan(2);
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect.each([
  "principal",
  "owner",
  "registry",
  "origin",
  "generation",
  "revision",
  "grant",
  "extra",
  "legacy",
] as const)("refuses %s home substitution or drift without fallback", (fault) => {
  const f = fixture({
    alterHome: (r, n) => {
      if (fault === "revision")
        return { ...r, access: { ...r.access, authorization_revision: n === 2 ? 5 : 4 } };
      if (fault === "grant") return { ...r, grant_id: n === 2 ? "replacement" : r.grant_id };
      if (fault === "extra") return { ...r, trusted: true };
      if (fault === "legacy") return r.access;
      const a = r.access;
      if (fault === "principal") return { ...r, access: { ...a, principal_id: "other" } };
      const key =
        fault === "registry"
          ? "registry_instance_id"
          : fault === "origin"
            ? "canonical_origin"
            : fault === "generation"
              ? "server_generation"
              : "owner_id";
      const value =
        fault === "generation"
          ? 2
          : fault === "origin"
            ? "https://other.invalid"
            : fault === "registry"
              ? "22222222-2222-4222-8222-222222222222"
              : "other";
      const context =
        fault === "owner"
          ? { ...a.capabilities.context, owner_id: value }
          : fault === "generation"
            ? { ...a.capabilities.context, server_generation: value }
            : a.capabilities.context;
      return {
        ...r,
        access: {
          ...a,
          home: { ...a.home, [key]: value },
          capabilities: { ...a.capabilities, context },
        },
      };
    },
  });
  return Effect.gen(function* () {
    expect((yield* (yield* Registration.MemberRegistration).read().pipe(Effect.result))._tag).toBe(
      "Failure",
    );
    expect(f.calls).toHaveLength(fault === "revision" || fault === "grant" ? 2 : 1);
  }).pipe(Effect.provide(f.layer));
});
it.effect.each(["rejected", "foreign", "short-build"] as const)(
  "refuses %s source context without manufacturing a descriptor",
  (sourceFault) => {
    const f = fixture({ sourceFault });
    return Effect.gen(function* () {
      expect(
        (yield* (yield* Registration.MemberRegistration).read().pipe(Effect.result))._tag,
      ).toBe("Failure");
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect(
  "reports an actual unavailable source without relabeling historical build as current",
  () => {
    const f = fixture({ sourceFault: "unavailable" });
    return Effect.gen(function* () {
      expect(yield* (yield* Registration.MemberRegistration).read()).toMatchObject({
        context: { grant_id: "actual-home-grant", sources: [] },
        t3Availability: { state: "unavailable", reason: "build_unavailable" },
      });
      expect(f.calls).toHaveLength(2);
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect("linked metadata recursion refuses before capture, source or transport", () => {
  const f = fixture();
  return Effect.gen(function* () {
    expect(yield* (yield* Registration.MemberRegistration).read().pipe(Effect.flip)).toMatchObject({
      code: "home_route_required",
    });
    expect(f.calls).toHaveLength(0);
    expect(f.captures()).toBe(0);
    expect(f.contexts()).toBe(0);
  }).pipe(
    Effect.provide(f.layer),
    Effect.provideService(Mcp.McpInvocationContext, {
      environmentId: EnvironmentId.make("member:test"),
      issuedAt: 0,
      requestNamespace: "linked",
      capabilities: new Set(["orchestration" as const]),
      thread: undefined,
      client: {
        sessionId: AuthSessionId.make("linked"),
        label: "linked",
        access: "full-access",
        linked: true,
      },
    }),
  );
});
