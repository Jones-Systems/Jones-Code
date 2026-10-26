import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Evidence from "./evidence.ts";
import { syntheticFixture } from "./synthetic.ts";
it.effect(
  "unconfigured evidence explicitly excludes every requested reference and cannot read or authorize",
  () =>
    Effect.gen(function* () {
      const provider = yield* Evidence.RollupEvidenceProvider,
        f = syntheticFixture();
      expect(provider.contract).toBe("workstreams-rollup-evidence/1.0.0");
      expect(provider.configured).toBe(false);
      expect(yield* provider.describe([f.binding])).toEqual([
        { kind: "excluded", native_reference_id: "ref:test", reason: "content_unavailable" },
      ]);
      expect(yield* provider.verifyCurrentAuthorization(f.manifest)).toBe(false);
      expect(
        (yield* Effect.flip(
          provider.readExtractionInputs(f.manifest, new AbortController().signal),
        )).code,
      ).toBe("provider_unconfigured");
    }).pipe(Effect.provide(Evidence.unconfiguredLayer)),
);
