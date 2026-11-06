import { EnvironmentId } from "@t3tools/contracts";
import * as Wire from "@t3tools/contracts/jones/workstreamsHomeAccess";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Peer from "../../../../peer/PeerMcpClient.ts";
import { canonicalJson } from "../../operations/encoding.ts";
import * as Admission from "../runtime/memberAdmission.ts";
import { RegistryError } from "../registry/session.ts";
import { HOME_ACCESS_TOOL } from "../forwarding/homeForwardPort.ts";

export class MemberAccess extends Context.Service<
  MemberAccess,
  {
    readonly current: () => Effect.Effect<Wire.WorkstreamsHomeAccessResult, RegistryError>;
  }
>()("t3/jones/workstreams/native/read/memberAccess") {}

const decodeEnvironment = Schema.decodeUnknownEffect(EnvironmentId);
const decodeResult = Schema.decodeUnknownEffect(Wire.WorkstreamsHomeAccessResult);
const refused = () => new RegistryError({ code: "authorization_not_current" });
const make = Effect.gen(function* () {
  const admission = yield* Admission.MemberAdmission;
  const peer = yield* Peer.PeerMcpClient;
  return MemberAccess.of({
    current: Effect.fn("MemberAccess.current")(function* () {
      const invocation = yield* Effect.serviceOption(Mcp.McpInvocationContext);
      if (
        Option.isSome(invocation) &&
        (invocation.value.client?.linked === true ||
          invocation.value.client?.remoteOrigin !== undefined)
      )
        return yield* new RegistryError({ code: "home_route_required" });
      // Capture fresh local authority; retained recovery records cannot select a public session.
      const admitted = yield* admission.readmit(yield* admission.capture());
      const association = admitted.record.association;
      if (association.link_generation.trim().length === 0)
        return yield* new RegistryError({ code: "link_generation_required" });
      const environmentId = yield* decodeEnvironment(association.home.home_environment_id).pipe(
        Effect.mapError(() => new RegistryError({ code: "invalid_home_environment" })),
      );
      const write = yield* admission.captureWrite({ kind: "metadata" }).pipe(
        Effect.flatMap((record) => admission.readmitWrite(record)),
        Effect.flatMap((candidate) => {
          const sameAssociation = Object.entries(association).every(
            ([key, value]) =>
              canonicalJson(value) ===
              canonicalJson(Reflect.get(candidate.record.association, key)),
          );
          return sameAssociation &&
            canonicalJson(candidate.record.local.auth) ===
              canonicalJson(admitted.record.local.auth) &&
            candidate.record.local.request_namespace === admitted.record.local.request_namespace &&
            candidate.record.local.invocation_issued_at ===
              admitted.record.local.invocation_issued_at &&
            canonicalJson(candidate.limits) === canonicalJson(admitted.limits)
            ? Effect.succeed(candidate)
            : Effect.fail(refused());
        }),
        Effect.option,
      );
      const checkRead = Effect.fn("MemberAccess.checkRead")(function* () {
        const current = yield* admitted.checkCurrent();
        if (canonicalJson(current) !== canonicalJson(admitted.limits)) return yield* refused();
      });
      yield* checkRead();
      const raw = yield* peer
        .call({
          environmentId,
          tool: HOME_ACCESS_TOOL,
          arguments: {
            contract: Wire.WORKSTREAMS_HOME_ACCESS_CONTRACT,
            expected_home: association.home,
          },
          limits: admitted.limits,
          expectedGeneration: association.link_generation,
          success: Wire.WorkstreamsHomeAccessResult,
        })
        .pipe(Effect.mapError((cause) => new RegistryError({ code: cause.code, cause })));
      const result = yield* decodeResult(raw).pipe(
        Effect.mapError(() => new RegistryError({ code: "invalid_response" })),
      );
      const home = association.home;
      if (
        result.home.registry_instance_id !== home.registry_instance_id ||
        result.home.owner_id !== home.owner_id ||
        result.home.home_environment_id !== home.home_environment_id ||
        result.home.canonical_origin !== association.canonical_origin ||
        result.principal_id !== association.principal_id
      )
        return yield* new RegistryError({ code: "registry_mismatch" });
      if (result.home.server_generation !== home.server_generation)
        return yield* new RegistryError({ code: "generation_mismatch" });
      const currentWrite = Option.isSome(write)
        ? yield* write.value.checkCurrent().pipe(
            Effect.map((limits) => canonicalJson(limits) === canonicalJson(admitted.limits)),
            Effect.orElseSucceed(() => false),
          )
        : false;
      yield* checkRead();
      return {
        ...result,
        capabilities: {
          ...result.capabilities,
          permissions: result.capabilities.permissions.filter(
            (permission) => permission !== "workstreams:write" || currentWrite,
          ),
        },
      };
    }),
  });
});

export const layer = Layer.effect(MemberAccess, make);
