import { assert, it } from "@effect/vitest";
import {
  WorkstreamsNativeAttestationResponseV2,
  WorkstreamsNativeContextResponseV2,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { makeWorkstreamsNativeProvider } from "../../nativeProvider/service.ts";
import {
  attestationRequest,
  binding,
  makeProviderFixture,
} from "../../nativeProvider/testFixtures.ts";
import * as Attestation from "./localAttestation.ts";

const decodeWorkstreamsNativeContextResponseV2 = Schema.decodeEffect(
  WorkstreamsNativeContextResponseV2,
);
const decodeWorkstreamsNativeAttestationResponseV2 = Schema.decodeEffect(
  WorkstreamsNativeAttestationResponseV2,
);

const home = {
  registry_instance_id: "11111111-1111-4111-8111-111111111111",
  owner_id: binding.owner_id,
  home_environment_id: "home-synthetic",
  server_generation: 1,
};

it.effect(
  "v2 identity and bounded evidence come from the source, with admission on every call",
  () =>
    Effect.gen(function* () {
      const fixture = makeProviderFixture();
      const admitted: string[] = [];
      const layer = Attestation.layer.pipe(
        Layer.provide(
          Layer.merge(
            Layer.succeed(
              Attestation.NativeProvider,
              makeWorkstreamsNativeProvider(fixture.ports, "v2"),
            ),
            Layer.succeed(Attestation.SourceAdmission, {
              authorize: (input) =>
                Effect.sync(() => {
                  assert.deepEqual(input.expected_home, home);
                  admitted.push(input.operation);
                  return binding;
                }),
            }),
          ),
        ),
      );
      yield* Effect.gen(function* () {
        const source = yield* Attestation.SourceAttestation;
        const context = yield* source.context({ expected_home: home });
        yield* decodeWorkstreamsNativeContextResponseV2(context);
        const result = yield* source.attest({ expected_home: home, request: attestationRequest });
        yield* decodeWorkstreamsNativeAttestationResponseV2(result);
        assert.strictEqual(result.state, "attested");
        assert.deepEqual(admitted, ["context", "attest"]);
        assert.strictEqual(fixture.calls.length, 0);
        if (result.state !== "attested") return;
        for (const invalid of [
          {
            ...result,
            attestation: { ...result.attestation, store_generation: binding.store_generation + 1 },
          },
          { ...result, attestation: { ...result.attestation, evidence_sha256: "f".repeat(64) } },
          { ...result, expires_at: "2030-01-01T00:00:00.000Z" },
          { ...result, request: { ...result.request, principal_id: "foreign" } },
        ]) {
          const failed = yield* Attestation.validateAttestation(attestationRequest, invalid).pipe(
            Effect.flip,
          );
          assert.strictEqual(failed.code, "provider_mismatch");
        }
      }).pipe(Effect.provide(layer));
    }),
);

it.effect("source-generation changes during observation never become an attestation", () =>
  Effect.gen(function* () {
    const fixture = makeProviderFixture();
    let reads = 0;
    const provider = makeWorkstreamsNativeProvider(
      {
        ...fixture.ports,
        authority: {
          readCurrent: Effect.sync(() => ({
            environmentId: binding.source_instance_id,
            authorityNamespace: binding.authority_namespace,
            storeGeneration: binding.store_generation + (reads++ > 0 ? 1 : 0),
          })),
        },
      },
      "v2",
    );
    const layer = Attestation.layer.pipe(
      Layer.provide(
        Layer.merge(
          Layer.succeed(Attestation.NativeProvider, provider),
          Layer.succeed(Attestation.SourceAdmission, { authorize: () => Effect.succeed(binding) }),
        ),
      ),
    );
    const response = yield* Effect.flatMap(Attestation.SourceAttestation, (source) =>
      source.attest({ expected_home: home, request: attestationRequest }),
    ).pipe(Effect.provide(layer));
    assert.strictEqual(response.state, "unknown");
    if (response.state === "unknown") assert.strictEqual(response.reason, "authority_changed");
  }),
);

it.effect(
  "unconfigured source fails explicitly; rejected source admission cannot observe or dispatch",
  () =>
    Effect.gen(function* () {
      const missing = yield* Effect.flatMap(Attestation.SourceAttestation, (source) =>
        source.context({ expected_home: home }),
      ).pipe(Effect.provide(Attestation.unconfiguredLayer), Effect.flip);
      assert.strictEqual(missing.code, "provider_unconfigured");
      const fixture = makeProviderFixture();
      const layer = Attestation.layer.pipe(
        Layer.provide(
          Layer.merge(
            Layer.succeed(
              Attestation.NativeProvider,
              makeWorkstreamsNativeProvider(fixture.ports, "v2"),
            ),
            Layer.succeed(Attestation.SourceAdmission, {
              authorize: () =>
                Effect.fail(new Attestation.ProviderError({ code: "provider_mismatch" })),
            }),
          ),
        ),
      );
      const denied = yield* Effect.flatMap(Attestation.SourceAttestation, (source) =>
        source.attest({ expected_home: home, request: attestationRequest }),
      ).pipe(Effect.provide(layer), Effect.flip);
      assert.strictEqual(denied.code, "provider_mismatch");
      assert.strictEqual(fixture.attempts.size, 0);
    }),
);
