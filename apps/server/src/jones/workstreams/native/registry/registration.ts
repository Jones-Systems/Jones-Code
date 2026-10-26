import * as Wire from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as NativeAuthority from "../../../../environment/NativeStoreAuthority.ts";
import * as Legacy from "../../registrationContext/service.ts";
import * as Attestation from "../providers/localAttestation.ts";
import * as Registry from "./service.ts";
import { NativeHome, type AdmittedCaller } from "../mode/authorityMode.ts";

export class Registration extends Context.Service<
  Registration,
  {
    readonly forCaller: (
      caller: AdmittedCaller,
    ) => Legacy.WorkstreamsRegistrationContext["Service"];
  }
>()("t3/jones/workstreams/native/registry/registration") {}
export const layer = Layer.effect(
  Registration,
  Effect.gen(function* () {
    const home = yield* NativeHome,
      registry = yield* Registry.Registry,
      source = yield* Attestation.SourceAttestation,
      authority = yield* NativeAuthority.NativeStoreAuthority;
    return Registration.of({
      forCaller: (caller) => ({
        read: Effect.fn("NativeRegistration.read")(function* (): Effect.fn.Return<
          Legacy.QualifiedRegistrationContext,
          Legacy.RegistrationContextError
        > {
          const s = yield* registry
            .snapshot(caller)
            .pipe(
              Effect.mapError(
                () => new Legacy.RegistrationContextError({ reason: "registry_unavailable" }),
              ),
            );
          const base = {
            protocol:
              Wire.WORKSTREAMS_REGISTRATION_CONTEXT_V2_PROTOCOL as typeof Wire.WORKSTREAMS_REGISTRATION_CONTEXT_V2_PROTOCOL,
            state: "ready" as const,
            owner_id: home.owner_id,
            principal_id: caller.principal_id,
            grant_id: s.authorization.grant.grant_id,
            authorization_revision: s.authorization.grant.grant_version,
            server_generation: s.context.server_generation,
            registry_version: s.context.registry_version,
          };
          const unavailable = (
            reason: "authority_unavailable" | "authority_fenced" | "generation_mismatch",
          ): Legacy.QualifiedRegistrationContext => ({
            context: { ...base, sources: [] },
            t3Availability: { state: "unavailable", reason },
          });
          const current = yield* authority.readCurrent.pipe(Effect.result);
          if (current._tag === "Failure")
            return unavailable(
              current.failure.code === "fenced" ? "authority_fenced" : "authority_unavailable",
            );
          const value = yield* source
            .context({
              expected_home: {
                registry_instance_id: home.registry_instance_id,
                owner_id: home.owner_id,
                home_environment_id: home.home_environment_id,
                server_generation: home.server_generation,
              },
            })
            .pipe(Effect.result);
          if (value._tag === "Failure" || value.success.state !== "ready")
            return unavailable("authority_unavailable");
          const observed = value.success.enrollment,
            tuple = current.success,
            binding = s.state.source_bindings.get(tuple.environmentId);
          if (
            observed.owner_id !== home.owner_id ||
            observed.principal_id !== caller.principal_id ||
            observed.source_instance_id !== tuple.environmentId ||
            tuple.environmentId !== caller.source_environment_id ||
            observed.authority_namespace !== tuple.authorityNamespace ||
            observed.store_generation !== tuple.storeGeneration ||
            binding?.state !== "active" ||
            binding.authority_namespace !== tuple.authorityNamespace ||
            binding.store_generation !== tuple.storeGeneration
          )
            return unavailable("generation_mismatch");
          // Project the actual source-issued context; never manufacture enrollment or build evidence.
          return {
            context: {
              ...base,
              sources: [
                {
                  provider: "t3",
                  source_instance_id: observed.source_instance_id,
                  authority_namespace: observed.authority_namespace,
                  store_generation: observed.store_generation,
                  resource_kind: "thread",
                  id_kind: "internal",
                  account_provenance: { kind: "not_account_scoped" },
                  native_protocol: observed.protocol,
                  build: value.success.observed_build,
                },
              ],
            },
            t3Availability: { state: "ready" },
          };
        }),
      }),
    });
  }),
);
