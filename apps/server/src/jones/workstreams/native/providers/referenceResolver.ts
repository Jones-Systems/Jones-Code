import {
  type NativeReference,
  type WorkstreamNativeIdentity,
  type WorkstreamPrLocator,
  WORKSTREAMS_T3_PROVIDER_ATTESTATION_TTL_MS,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { canonicalJson } from "../../operations/encoding.ts";
import { sha256Bytes } from "../../nativeProvider/service.ts";
import {
  ProviderError,
  type SourceCall,
  type SourceAttestation,
  validateAttestation,
} from "./localAttestation.ts";
import * as GitHub from "./githubPrObserver.ts";

export interface ReferenceResolutionRequest {
  readonly owner_id: string;
  readonly principal_id: string;
  readonly identity: typeof WorkstreamNativeIdentity.Type;
  readonly pr_locator: WorkstreamPrLocator | null;
}
export type ReferenceResolution =
  | {
      readonly state: "attested";
      readonly attestation: NonNullable<NativeReference["registration"]["evidence"]>;
      readonly attested_at: string;
      readonly expires_at: string;
    }
  | { readonly state: "verification-pending" | "verification-failed" };

export interface AttestationRoute extends SourceCall {
  readonly authority_namespace: string;
  readonly store_generation: number;
  readonly source: SourceAttestation["Service"];
}
export interface GitHubReferenceSource {
  readonly owner_id: string;
  readonly principal_id: string;
  readonly source_instance_id: string;
  readonly authority_namespace: string;
  readonly store_generation: number;
}

// Routing metadata only. Thread admission and generation remain at the selected source.
export class ReferenceSources extends Context.Service<
  ReferenceSources,
  {
    readonly selectT3: (
      input: ReferenceResolutionRequest,
    ) => Effect.Effect<AttestationRoute | null, ProviderError>;
    readonly github: GitHubReferenceSource | null;
  }
>()("t3/jones/workstreams/native/providers/referenceResolver/ReferenceSources") {}

export class ReferenceResolver extends Context.Service<
  ReferenceResolver,
  {
    readonly resolve: (
      input: ReferenceResolutionRequest,
    ) => Effect.Effect<ReferenceResolution, ProviderError>;
  }
>()("t3/jones/workstreams/native/providers/referenceResolver") {}

const make = Effect.gen(function* () {
  const sources = yield* ReferenceSources;
  const github = yield* GitHub.LinkedPrObserver;
  return ReferenceResolver.of({
    resolve: Effect.fn("ReferenceResolver.resolve")(function* (input): Effect.fn.Return<
      ReferenceResolution,
      ProviderError
    > {
      const failed = { state: "verification-failed" } as const;
      const identity = input.identity;
      if (identity.account_provenance.kind !== "not_account_scoped") return failed;
      if (identity.provider === "t3") {
        if (
          identity.resource_kind !== "thread" ||
          identity.id_kind !== "internal" ||
          input.pr_locator !== null
        )
          return failed;
        const route = yield* sources.selectT3(input);
        if (route === null) return yield* new ProviderError({ code: "provider_unconfigured" });
        const request = {
          owner_id: input.owner_id,
          principal_id: input.principal_id,
          identity: {
            ...identity,
            provider: "t3" as const,
            resource_kind: "thread" as const,
            id_kind: "internal" as const,
            account_provenance: { kind: "not_account_scoped" as const },
          },
          expected_authority_namespace: route.authority_namespace,
          expected_store_generation: route.store_generation,
        };
        const response = yield* validateAttestation(
          request,
          yield* route.source.attest({ expected_home: route.expected_home, request }),
        );
        if (response.state === "unknown") return { state: "verification-pending" };
        if (response.state !== "attested") return failed;
        return {
          state: "attested",
          attestation: response.attestation,
          attested_at: response.attested_at,
          expires_at: response.expires_at,
        };
      }
      if (
        identity.provider !== "github" ||
        identity.resource_kind !== "pull_request" ||
        identity.id_kind !== "external" ||
        input.pr_locator === null
      )
        return failed;
      const config = sources.github;
      if (config === null) return yield* new ProviderError({ code: "provider_unconfigured" });
      const locator = input.pr_locator;
      if (
        config.owner_id !== input.owner_id ||
        config.principal_id !== input.principal_id ||
        config.source_instance_id !== identity.source_instance_id ||
        identity.native_id !==
          `${locator.repository_owner}/${locator.repository_name}#${locator.number}`
      )
        return failed;
      const observation = yield* github.observe({
        source_instance_id: identity.source_instance_id,
        locator,
        previous_success: null,
        etag: null,
      });
      if (observation.outcome !== "observed" || observation.last_success === null)
        return {
          state:
            observation.outcome === "rate_limited" || observation.outcome === "unavailable"
              ? "verification-pending"
              : "verification-failed",
        };
      const at = observation.observed_at;
      const expires = DateTime.formatIso(
        DateTime.addDuration(DateTime.makeUnsafe(at), WORKSTREAMS_T3_PROVIDER_ATTESTATION_TTL_MS),
      );
      return {
        state: "attested",
        attested_at: at,
        expires_at: expires,
        attestation: {
          provider: "github",
          source_instance_id: config.source_instance_id,
          authority_namespace: config.authority_namespace,
          store_generation: config.store_generation,
          native_id: identity.native_id,
          evidence_sha256: sha256Bytes(
            canonicalJson({
              owner_id: input.owner_id,
              principal_id: input.principal_id,
              identity,
              pr_locator: locator,
              authority_namespace: config.authority_namespace,
              store_generation: config.store_generation,
              observation,
            }),
          ),
        },
      };
    }),
  });
});

export const layer = Layer.effect(ReferenceResolver, make);
export const unconfiguredLayer = Layer.succeed(ReferenceResolver, {
  resolve: () => Effect.fail(new ProviderError({ code: "provider_unconfigured" })),
});
