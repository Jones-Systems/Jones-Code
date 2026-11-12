import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { makeWorkstreamsNativeProvider } from "../../nativeProvider/service.ts";
import {
  attestationRequest,
  binding,
  makeProviderFixture,
} from "../../nativeProvider/testFixtures.ts";
import * as Attestation from "./localAttestation.ts";
import * as GitHub from "./githubPrObserver.ts";
import * as Resolver from "./referenceResolver.ts";
import { syntheticGitHubLayer } from "./synthetic.ts";

const home = {
  registry_instance_id: "11111111-1111-4111-8111-111111111111",
  owner_id: binding.owner_id,
  home_environment_id: "home-synthetic",
  server_generation: 1,
};
const request: Resolver.ReferenceResolutionRequest = {
  owner_id: binding.owner_id,
  principal_id: binding.principal_id,
  identity: attestationRequest.identity,
  pr_locator: null,
};

it.effect(
  "T3 resolution validates source v2 evidence and never invents an attestation for unavailable sources",
  () =>
    Effect.gen(function* () {
      const fixture = makeProviderFixture();
      const source = yield* Attestation.SourceAttestation.pipe(
        Effect.provide(
          Attestation.layer.pipe(
            Layer.provide(
              Layer.merge(
                Layer.succeed(
                  Attestation.NativeProvider,
                  makeWorkstreamsNativeProvider(fixture.ports, "v2"),
                ),
                Layer.succeed(Attestation.SourceAdmission, {
                  authorize: () => Effect.succeed(binding),
                }),
              ),
            ),
          ),
        ),
      );
      const configured = Resolver.layer.pipe(
        Layer.provide(
          Layer.merge(
            Layer.succeed(Resolver.ReferenceSources, {
              selectT3: () =>
                Effect.succeed({
                  expected_home: home,
                  authority_namespace: binding.authority_namespace,
                  store_generation: binding.store_generation,
                  source,
                }),
              github: null,
            }),
            GitHub.unconfiguredLayer,
          ),
        ),
      );
      yield* Effect.gen(function* () {
        const resolver = yield* Resolver.ReferenceResolver;
        const result = yield* resolver.resolve(request);
        assert.strictEqual(result.state, "attested");
        if (result.state === "attested") assert.strictEqual(result.attestation.provider, "t3");
        assert.deepEqual(
          yield* resolver.resolve({
            ...request,
            identity: { ...request.identity, source_instance_id: "foreign" },
          }),
          { state: "verification-failed" },
        );
        assert.deepEqual(
          yield* resolver.resolve({
            ...request,
            identity: { ...request.identity, id_kind: "external" },
          }),
          { state: "verification-failed" },
        );
        assert.strictEqual(fixture.calls.length, 0);
      }).pipe(Effect.provide(configured));
    }),
);

it.effect(
  "GitHub verification uses the existing identity and genuine observation; rate-limited is pending",
  () =>
    Effect.gen(function* () {
      const locator = {
        host: "github.com" as const,
        repository_owner: "jones-systems",
        repository_name: "jones-code",
        number: 660,
      };
      const input = {
        ...request,
        identity: {
          provider: "github",
          source_instance_id: "github",
          resource_kind: "pull_request",
          id_kind: "external" as const,
          native_id: "jones-systems/jones-code#660",
          account_provenance: { kind: "not_account_scoped" as const },
        },
        pr_locator: locator,
      };
      for (const outcome of ["observed", "rate_limited", "inaccessible"] as const) {
        const configured = Resolver.layer.pipe(
          Layer.provide(
            Layer.merge(
              Layer.succeed(Resolver.ReferenceSources, {
                selectT3: () => Effect.succeed(null),
                github: {
                  owner_id: binding.owner_id,
                  principal_id: binding.principal_id,
                  source_instance_id: "github",
                  authority_namespace: "github-native",
                  store_generation: 1,
                },
              }),
              syntheticGitHubLayer([
                {
                  source_instance_id: "github",
                  locator,
                  result: {
                    outcome,
                    observed_at: "2026-01-01T00:00:00Z",
                    retry_after_seconds: outcome === "rate_limited" ? 60 : null,
                    last_success:
                      outcome === "observed"
                        ? {
                            state: "open",
                            draft: false,
                            observed_at: "2026-01-01T00:00:00Z",
                            provider_updated_at: null,
                          }
                        : null,
                    etag: null,
                  },
                },
              ]),
            ),
          ),
        );
        const result = yield* Effect.flatMap(Resolver.ReferenceResolver, (resolver) =>
          resolver.resolve(input),
        ).pipe(Effect.provide(configured));
        assert.strictEqual(
          result.state,
          outcome === "observed"
            ? "attested"
            : outcome === "rate_limited"
              ? "verification-pending"
              : "verification-failed",
        );
        if (result.state === "attested") {
          assert.strictEqual(result.attestation.provider, "github");
          assert.strictEqual(
            Date.parse(result.expires_at) - Date.parse(result.attested_at),
            300_000,
          );
        }
        const wrong = yield* Effect.flatMap(Resolver.ReferenceResolver, (resolver) =>
          resolver.resolve({
            ...input,
            identity: { ...input.identity, native_id: "wrong#1" },
          }),
        ).pipe(Effect.provide(configured));
        assert.deepEqual(wrong, { state: "verification-failed" });
      }
    }),
);

it.effect("missing resolver and source configuration are explicit", () =>
  Effect.gen(function* () {
    const configured = Resolver.layer.pipe(
      Layer.provide(
        Layer.merge(
          Layer.succeed(Resolver.ReferenceSources, {
            selectT3: () => Effect.succeed(null),
            github: null,
          }),
          GitHub.unconfiguredLayer,
        ),
      ),
    );
    for (const layer of [configured, Resolver.unconfiguredLayer]) {
      const missing = yield* Effect.flatMap(Resolver.ReferenceResolver, (resolver) =>
        resolver.resolve(request),
      ).pipe(Effect.provide(layer), Effect.flip);
      assert.strictEqual(missing.code, "provider_unconfigured");
    }
  }),
);
