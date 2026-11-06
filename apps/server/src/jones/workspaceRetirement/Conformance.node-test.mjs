import assert from "node:assert/strict";
import * as FS from "node:fs/promises";
import { test } from "node:test";
import corpus from "./conformance/v1/cases.json" with { type: "json" };
import source from "./conformance/v1/source.json" with { type: "json" };
import { executeRecipe } from "./Conformance.testkit.ts";
import { sha256 } from "./Filesystem.ts";
import { incarnation, listingDigest, manifestBytes } from "./OutputAttestation.ts";

test("pinned A1 corpus bytes and incarnation are unchanged", async () => {
  assert.equal(
    sha256(await FS.readFile(new URL("./conformance/v1/cases.json", import.meta.url))),
    source.sha256,
  );
  assert.equal(source.commit, "d330101012fc7f020b0f72e2d5432ccd2d8ea94c");
  assert.equal(
    incarnation(corpus.reservation_identity_vector.binding),
    corpus.reservation_identity_vector.incarnation_sha256,
  );
});
for (const vector of corpus.digest_vectors)
  test(vector.id + " wire preimages", () => {
    assert.equal(listingDigest(vector.entries), vector.listing_sha256);
    assert.equal(manifestBytes(vector.entries).toString("hex"), vector.manifest_preimage_hex);
    assert.equal(sha256(manifestBytes(vector.entries)), vector.manifest_sha256);
  });
for (const scenario of corpus.cases)
  test(scenario.id, async () => {
    const result = await executeRecipe(scenario.recipe);
    assert.deepEqual(
      { outcome: result.outcome, level: result.level, reasons: result.reasons },
      scenario.expected,
    );
  });
