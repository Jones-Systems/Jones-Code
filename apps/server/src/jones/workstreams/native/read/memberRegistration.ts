import {
  EnvironmentId,
  WorkstreamsNativeContextResponseV2,
  WorkstreamsRegistrationContextV2Response,
  WORKSTREAMS_REGISTRATION_CONTEXT_V2_PROTOCOL,
} from "@t3tools/contracts";
import * as Wire from "@t3tools/contracts/jones/workstreamsHomeRegistration";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Peer from "../../../../peer/PeerMcpClient.ts";
import { canonicalJson } from "../../operations/encoding.ts";
import type { QualifiedRegistrationContext } from "../../registrationContext/service.ts";
import * as Admission from "../runtime/memberAdmission.ts";
import * as Members from "../runtime/memberEnrollment.ts";
import * as Startup from "../runtime/startup.ts";
import * as Provider from "../providers/localAttestation.ts";
import { HOME_REGISTRATION_TOOL } from "../forwarding/homeForwardPort.ts";
import { RegistryError } from "../registry/session.ts";

export class MemberRegistration extends Context.Service<
  MemberRegistration,
  {
    readonly read: () => Effect.Effect<QualifiedRegistrationContext, RegistryError>;
  }
>()("t3/jones/workstreams/native/read/memberRegistration") {}

const decodeEnvironment = Schema.decodeUnknownEffect(EnvironmentId);
const decodeHome = Schema.decodeUnknownEffect(Wire.WorkstreamsHomeRegistrationResult);
const decodeSource = Schema.decodeUnknownEffect(WorkstreamsNativeContextResponseV2);
const decodePublic = Schema.decodeUnknownEffect(WorkstreamsRegistrationContextV2Response);
const refused = () => new RegistryError({ code: "authorization_not_current" });
const observation = (r: Wire.WorkstreamsHomeRegistrationResult) =>
  canonicalJson({
    ...r,
    access: { ...r.access, observed_at: undefined },
  });

const make = Effect.gen(function* () {
  const admission = yield* Admission.MemberAdmission;
  const members = yield* Members.MemberEnrollments;
  const selected = yield* Startup.Startup;
  const provider = yield* Provider.NativeProvider;
  const peer = yield* Peer.PeerMcpClient;
  return MemberRegistration.of({
    read: Effect.fn("MemberRegistration.read")(function* () {
      const invocation = yield* Effect.serviceOption(Mcp.McpInvocationContext);
      if (
        Option.isSome(invocation) &&
        (invocation.value.client?.linked === true ||
          invocation.value.client?.remoteOrigin !== undefined)
      )
        return yield* new RegistryError({ code: "home_route_required" });
      if (
        selected.mode !== "native-member" ||
        selected.problem !== null ||
        !selected.sourceSessionId ||
        provider.profile !== "v2"
      )
        return yield* new RegistryError({ code: "provider_unconfigured" });
      // Only fresh request admission may choose the current local caller.
      const admitted = yield* admission.readmit(yield* admission.capture());
      const a = admitted.record.association;
      if (!a.link_generation.trim())
        return yield* new RegistryError({ code: "link_generation_required" });
      const environmentId = yield* decodeEnvironment(a.home.home_environment_id).pipe(
        Effect.mapError(refused),
      );
      const binding = yield* members
        .getBySessionId(selected.sourceSessionId)
        .pipe(Effect.mapError(refused));
      if (Option.isNone(binding)) return yield* refused();
      const enrolled = binding.value;
      if (
        enrolled.owner_id !== a.home.owner_id ||
        enrolled.principal_id !== a.principal_id ||
        enrolled.source_instance_id !== a.source.environment_id ||
        enrolled.authority_namespace !== a.source.authority_namespace ||
        enrolled.store_generation !== a.source.store_generation ||
        enrolled.enrollment_id !== a.enrollment_id ||
        enrolled.session_id !== a.enrollment_session_id ||
        enrolled.registry_origin !== a.canonical_origin ||
        enrolled.session_id !== selected.sourceSessionId
      )
        return yield* refused();
      const check = Effect.fn("MemberRegistration.checkCurrent")(function* () {
        if (canonicalJson(yield* admitted.checkCurrent()) !== canonicalJson(admitted.limits))
          return yield* refused();
      });
      const home = Effect.fn("MemberRegistration.home")(function* () {
        yield* check();
        const raw = yield* peer
          .call({
            environmentId,
            tool: HOME_REGISTRATION_TOOL,
            arguments: {
              contract: Wire.WORKSTREAMS_HOME_REGISTRATION_CONTRACT,
              expected_home: a.home,
            },
            limits: admitted.limits,
            expectedGeneration: a.link_generation,
            success: Wire.WorkstreamsHomeRegistrationResult,
          })
          .pipe(Effect.mapError((cause) => new RegistryError({ code: cause.code })));
        const result = yield* decodeHome(raw).pipe(
          Effect.mapError(() => new RegistryError({ code: "invalid_response" })),
        );
        const r = result.access;
        if (
          r.home.registry_instance_id !== a.home.registry_instance_id ||
          r.home.owner_id !== a.home.owner_id ||
          r.home.home_environment_id !== a.home.home_environment_id ||
          r.home.server_generation !== a.home.server_generation ||
          r.home.canonical_origin !== a.canonical_origin ||
          r.principal_id !== a.principal_id
        )
          return yield* new RegistryError({ code: "registry_mismatch" });
        yield* check();
        return result;
      });
      const before = yield* home();
      // The real provider-v2 context observes current authority/build while retaining the original enrollment.
      const source = yield* decodeSource(yield* provider.context(enrolled)).pipe(
        Effect.mapError(() => new RegistryError({ code: "invalid_response" })),
      );
      if (source.state === "rejected") return yield* refused();
      if (
        source.state === "ready" &&
        canonicalJson(source.enrollment) !==
          canonicalJson({
            owner_id: enrolled.owner_id,
            principal_id: enrolled.principal_id,
            source_instance_id: enrolled.source_instance_id,
            authority_namespace: enrolled.authority_namespace,
            store_generation: enrolled.store_generation,
            enrollment_id: enrolled.enrollment_id,
            protocol: enrolled.protocol,
            build: enrolled.build,
          })
      )
        return yield* new RegistryError({ code: "registry_mismatch" });
      const after = yield* home();
      if (observation(before) !== observation(after)) return yield* refused();
      const context = yield* decodePublic({
        protocol: WORKSTREAMS_REGISTRATION_CONTEXT_V2_PROTOCOL,
        state: "ready",
        owner_id: after.access.home.owner_id,
        principal_id: after.access.principal_id,
        grant_id: after.grant_id,
        authorization_revision: after.access.authorization_revision,
        server_generation: after.access.home.server_generation,
        registry_version: after.access.home.registry_version,
        sources:
          source.state === "ready"
            ? [
                {
                  provider: "t3",
                  source_instance_id: source.enrollment.source_instance_id,
                  authority_namespace: source.enrollment.authority_namespace,
                  store_generation: source.enrollment.store_generation,
                  resource_kind: "thread",
                  id_kind: "internal",
                  account_provenance: { kind: "not_account_scoped" },
                  native_protocol: source.enrollment.protocol,
                  build: source.observed_build,
                },
              ]
            : [],
      }).pipe(Effect.mapError(() => new RegistryError({ code: "invalid_response" })));
      yield* check();
      return {
        context,
        t3Availability:
          source.state === "ready"
            ? { state: "ready" as const }
            : {
                state: "unavailable" as const,
                reason:
                  source.reason === "build_unavailable"
                    ? ("build_unavailable" as const)
                    : source.reason === "provider_unconfigured"
                      ? ("descriptor_absent" as const)
                      : ("authority_unavailable" as const),
              },
      };
    }),
  });
});
export const layer = Layer.effect(MemberRegistration, make);
