import { describe, expect, it } from "vitest";
import * as FS from "node:fs/promises";
import corpus from "./conformance/v1/cases.json" with { type: "json" };
import source from "./conformance/v1/source.json" with { type: "json" };
import { executeRecipe } from "./Conformance.testkit.ts";
import { sha256 } from "./Filesystem.ts";
import { incarnation, listingDigest, manifestBytes, type Entry } from "./OutputAttestation.ts";

it("pins the exact A1 corpus and incarnation vector", async () => {
  expect(sha256(await FS.readFile(new URL("./conformance/v1/cases.json", import.meta.url)))).toBe(
    source.sha256,
  );
  expect(source.commit).toBe("d330101012fc7f020b0f72e2d5432ccd2d8ea94c");
  expect(incarnation(corpus.reservation_identity_vector.binding)).toBe(
    corpus.reservation_identity_vector.incarnation_sha256,
  );
});
describe("shared wire vectors", () => {
  for (const vector of corpus.digest_vectors)
    it(vector.id, () => {
      const entries: Entry[] = vector.entries;
      expect(listingDigest(entries)).toBe(vector.listing_sha256);
      expect(manifestBytes(entries).toString("hex")).toBe(vector.manifest_preimage_hex);
      expect(sha256(manifestBytes(entries))).toBe(vector.manifest_sha256);
    });
});
describe("materialized shared qualification recipes", () => {
  for (const scenario of corpus.cases)
    it(
      scenario.id,
      async () => {
        const result = await executeRecipe(scenario.recipe);
        expect({ outcome: result.outcome, level: result.level, reasons: result.reasons }).toEqual(
          scenario.expected,
        );
      },
      30000,
    );
});
