import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { syntheticFixture, syntheticEvidenceLayer } from "./synthetic.ts";
import * as Evidence from "./evidence.ts";
import { createManifest } from "./generation.ts";
it.effect(
  "synthetic inputs require exact owner/account/selection and current source decisions",
  () => {
    const f = syntheticFixture();
    let allowed = true;
    return Effect.gen(function* () {
      const p = yield* Evidence.RollupEvidenceProvider;
      expect((yield* p.describe([f.binding]))[0]?.kind).toBe("available");
      expect((yield* p.describe([{ ...f.binding, account_id: "account:other" }]))[0]).toMatchObject(
        { kind: "excluded", reason: "content_unavailable" },
      );
      expect(
        yield* p.verifyCurrentAuthorization(
          createManifest({ ...f.manifest, owner_id: "owner:other" }),
        ),
      ).toBe(false);
      expect(yield* p.readExtractionInputs(f.manifest, new AbortController().signal)).toEqual([
        f.input,
      ]);
      allowed = false;
      expect(yield* p.verifyCurrentAuthorization(f.manifest)).toBe(false);
      expect((yield* p.describe([f.binding]))[0]).toMatchObject({
        kind: "excluded",
        reason: "not_authorized",
      });
      expect(
        (yield* Effect.flip(p.readExtractionInputs(f.manifest, new AbortController().signal))).code,
      ).toBe("input_unavailable");
    }).pipe(Effect.provide(syntheticEvidenceLayer([{ ...f, authorized: () => allowed }])));
  },
);
