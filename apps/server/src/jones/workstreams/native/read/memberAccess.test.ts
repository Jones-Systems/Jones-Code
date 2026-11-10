import { expect, it } from "@effect/vitest";
import { AuthSessionId, EnvironmentId, OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Wire from "@t3tools/contracts/jones/workstreamsHomeAccess";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Admission from "../runtime/memberAdmission.ts";
import * as Peer from "../../../../peer/PeerMcpClient.ts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import { RegistryError } from "../registry/session.ts";
import { workstreamCapabilities } from "../registry/capabilities.ts";
import { HOME_ACCESS_TOOL } from "../forwarding/homeForwardPort.ts";
import * as Access from "./memberAccess.ts";

// Admission and peer transport are external ports here; the combined H/M test supplies their real owners.
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

const limits = record.local.ceiling.limits!;
const permissions = ["workstreams:read", "workstreams:write"] as const;
const writeRecord: Admission.MemberRecordedWriteCallerV2 = {
  schema: "jones-workstreams-member-recorded-write-caller/2",
  local: {
    ...record.local,
    schema: "jones-workstreams-member-local-write-caller/2",
    target: { kind: "metadata" },
    ceiling: { ...record.local.ceiling, permissions },
  },
  association: {
    ...record.association,
    member_pin_id: "member-pin",
    home_pin_id: "home-pin",
    home_pin_bound_revision: 2,
  },
  original_outbound_ceiling: {
    access: "full-access",
    runtime_mode: "full-access",
    interaction_mode: "default",
    scopes: ["orchestration:read", "orchestration:operate"],
    permissions,
  },
};
const response = (): Wire.WorkstreamsHomeAccessResult => ({
  contract: Wire.WORKSTREAMS_HOME_ACCESS_CONTRACT,
  home: { ...home, canonical_origin: record.association.canonical_origin, registry_version: 9 },
  principal_id: record.association.principal_id,
  authorization_revision: 4,
  observed_at: "2026-01-01T00:00:00.000Z",
  capabilities: workstreamCapabilities(
    { owner_id: home.owner_id, server_generation: 1, registry_version: 9 },
    permissions,
  ),
});
const fixture = (
  options: {
    write?: "allowed" | "denied" | "revoked" | "foreign" | "other-caller";
    changeRead?: "before" | "after" | "limits";
    alter?: (value: Wire.WorkstreamsHomeAccessResult) => unknown;
    missingTool?: boolean;
  } = {},
) => {
  const calls: Array<Parameters<Peer.PeerMcpClient["Service"]["call"]>[0]> = [];
  let captures = 0,
    checks = 0;
  const selectedWrite =
    options.write === "foreign"
      ? { ...writeRecord, association: { ...writeRecord.association, enrollment_id: "other" } }
      : options.write === "other-caller"
        ? {
            ...writeRecord,
            local: {
              ...writeRecord.local,
              auth: {
                kind: "bearer" as const,
                session_id: "other",
                subject: "owner",
                method: "bearer-access-token" as const,
              },
            },
          }
        : writeRecord;
  const ports = Layer.mergeAll(
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
                options.changeRead === "before" ||
                (options.changeRead === "after" && checks >= 2)
              )
                return Effect.fail(new RegistryError({ code: "forbidden" }));
              return Effect.succeed(
                options.changeRead === "limits" && checks >= 2
                  ? { ...limits, interactionMode: "plan" as const }
                  : limits,
              );
            }),
        }),
      captureWrite: () =>
        options.write === "denied"
          ? Effect.fail(new RegistryError({ code: "forbidden" }))
          : Effect.succeed(selectedWrite),
      readmitWrite: () =>
        Effect.succeed({
          record: selectedWrite,
          limits,
          ceiling: selectedWrite.local.ceiling,
          checkCurrent: () =>
            options.write === "revoked"
              ? Effect.fail(new RegistryError({ code: "forbidden" }))
              : Effect.succeed(limits),
        }),
    }),
    Layer.succeed(Peer.PeerMcpClient, {
      call: (input) => {
        calls.push(input);
        if (options.missingTool)
          return Effect.fail(
            new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "Synthetic missing tool",
            }),
          );
        const value = response();
        // oxlint-disable-next-line t3code/no-inline-schema-compile -- The synthetic peer applies the fixed client's supplied decoder.
        return Schema.decodeUnknownEffect(input.success)(
          options.alter ? options.alter(value) : value,
        ).pipe(
          Effect.mapError(
            () =>
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "Synthetic protocol refusal",
              }),
          ),
        );
      },
    }),
  );
  return { layer: Access.layer.pipe(Layer.provide(ports)), calls, captures: () => captures };
};
it.effect(
  "uses one fixed generation-bound query per fresh caller and never caches a session",
  () => {
    const f = fixture();
    return Effect.gen(function* () {
      const access = yield* Access.MemberAccess;
      expect(yield* access.current()).toEqual(response());
      expect(yield* access.current()).toEqual(response());
      expect(f.captures()).toBe(2);
      expect(f.calls).toHaveLength(2);
      expect(f.calls[0]).toMatchObject({
        environmentId: home.home_environment_id,
        tool: HOME_ACCESS_TOOL,
        expectedGeneration: "opaque-original",
        limits,
        arguments: { contract: Wire.WORKSTREAMS_HOME_ACCESS_CONTRACT, expected_home: home },
      });
      expect(f.calls[0]).not.toHaveProperty("origin");
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect.each(["denied", "revoked", "foreign", "other-caller"] as const)(
  "narrows home write permission when source write admission is %s",
  (write) => {
    const f = fixture({ write });
    return Effect.gen(function* () {
      const result = yield* (yield* Access.MemberAccess).current();
      expect(result.capabilities.permissions).toEqual(["workstreams:read"]);
      expect(f.calls).toHaveLength(1);
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect.each(["before", "after", "limits"] as const)(
  "refuses %s read-currentness drift without fallback or retry",
  (changeRead) => {
    const f = fixture({ changeRead });
    return Effect.gen(function* () {
      expect((yield* (yield* Access.MemberAccess).current().pipe(Effect.result))._tag).toBe(
        "Failure",
      );
      expect(f.calls).toHaveLength(changeRead === "before" ? 0 : 1);
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect.each([
  "principal",
  "owner",
  "generation",
  "origin",
  "environment",
  "registry",
  "extra",
  "bare",
] as const)("refuses mismatched or non-closed %s home results", (fault) => {
  const f = fixture({
    alter: (r) => {
      if (fault === "principal") return { ...r, principal_id: "other" };
      if (fault === "owner")
        return {
          ...r,
          home: { ...r.home, owner_id: "other" },
          capabilities: {
            ...r.capabilities,
            context: { ...r.capabilities.context, owner_id: "other" },
          },
        };
      if (fault === "generation")
        return {
          ...r,
          home: { ...r.home, server_generation: 2 },
          capabilities: {
            ...r.capabilities,
            context: { ...r.capabilities.context, server_generation: 2 },
          },
        };
      if (fault === "origin")
        return { ...r, home: { ...r.home, canonical_origin: "https://other.invalid" } };
      if (fault === "environment")
        return { ...r, home: { ...r.home, home_environment_id: "other" } };
      if (fault === "registry")
        return {
          ...r,
          home: { ...r.home, registry_instance_id: "22222222-2222-4222-8222-222222222222" },
        };
      return fault === "extra" ? { ...r, caller: {} } : r.capabilities;
    },
  });
  return Effect.gen(function* () {
    expect((yield* (yield* Access.MemberAccess).current().pipe(Effect.result))._tag).toBe(
      "Failure",
    );
    expect(f.calls).toHaveLength(1);
  }).pipe(Effect.provide(f.layer));
});
it.effect("missing access support refuses without legacy negotiation or replay", () => {
  const f = fixture({ missingTool: true });
  return Effect.gen(function* () {
    expect(yield* (yield* Access.MemberAccess).current().pipe(Effect.flip)).toMatchObject({
      code: "orchestration_error",
    });
    expect(f.calls).toHaveLength(1);
  }).pipe(Effect.provide(f.layer));
});
it.effect("linked metadata recursion refuses before capture or transport", () => {
  const f = fixture();
  return Effect.gen(function* () {
    expect(yield* (yield* Access.MemberAccess).current().pipe(Effect.flip)).toMatchObject({
      code: "home_route_required",
    });
    expect(f.captures()).toBe(0);
    expect(f.calls).toHaveLength(0);
  }).pipe(
    Effect.provide(f.layer),
    Effect.provideService(Mcp.McpInvocationContext, {
      environmentId: EnvironmentId.make("member:test"),
      issuedAt: 0,
      requestNamespace: "linked:test",
      capabilities: new Set(["orchestration" as const]),
      thread: undefined,
      client: {
        sessionId: AuthSessionId.make("linked:test"),
        label: "linked",
        access: "full-access",
        linked: true,
      },
    }),
  );
});
