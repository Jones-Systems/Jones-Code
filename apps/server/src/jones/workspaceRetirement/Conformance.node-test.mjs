// Test-only clock adapter; native filesystem, Git and proof behavior is unchanged.
import { installEffectSeam } from "./EffectSeam.node-test-support.mjs";
installEffectSeam();
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeTest from "node:test";
import corpus from "./conformance/v1/cases.json" with { type: "json" };
import source from "./conformance/v1/source.json" with { type: "json" };
const { executeRecipe } = await import("./Conformance.testkit.ts");
import { sha256 } from "./Filesystem.ts";
import { incarnation, listingDigest, manifestBytes } from "./OutputAttestation.ts";

NodeTest.test("pinned A1 corpus bytes and incarnation are unchanged", async () => {
  NodeAssert.equal(
    sha256(await NodeFSP.readFile(new URL("./conformance/v1/cases.json", import.meta.url))),
    source.sha256,
  );
  NodeAssert.equal(source.commit, "d330101012fc7f020b0f72e2d5432ccd2d8ea94c");
  NodeAssert.equal(
    incarnation(corpus.reservation_identity_vector.binding),
    corpus.reservation_identity_vector.incarnation_sha256,
  );
});
for (const vector of corpus.digest_vectors)
  NodeTest.test(vector.id + " wire preimages", () => {
    NodeAssert.equal(listingDigest(vector.entries), vector.listing_sha256);
    NodeAssert.equal(manifestBytes(vector.entries).toString("hex"), vector.manifest_preimage_hex);
    NodeAssert.equal(sha256(manifestBytes(vector.entries)), vector.manifest_sha256);
  });
for (const scenario of corpus.cases)
  NodeTest.test(scenario.id, async () => {
    const result = await executeRecipe(scenario.recipe);
    NodeAssert.deepEqual(
      { outcome: result.outcome, level: result.level, reasons: result.reasons },
      scenario.expected,
    );
  });
