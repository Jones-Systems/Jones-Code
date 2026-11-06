import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import corpus from "./conformance/v1/cases.json" with { type: "json" };
import source from "./conformance/v1/source.json" with { type: "json" };
import { executeRecipe } from "./Conformance.testkit.ts";
import { sha256 } from "./Filesystem.ts";
import { incarnation, listingDigest, manifestBytes, type Entry } from "./OutputAttestation.ts";

it.effect("pins the exact A1 corpus and incarnation vector", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const file = yield* path.fromFileUrl(new URL("./conformance/v1/cases.json", import.meta.url));
    expect(sha256(yield* fs.readFile(file))).toBe(source.sha256);
    expect(source.commit).toBe("d330101012fc7f020b0f72e2d5432ccd2d8ea94c");
    expect(incarnation(corpus.reservation_identity_vector.binding)).toBe(
      corpus.reservation_identity_vector.incarnation_sha256,
    );
  }).pipe(Effect.provide(NodeServices.layer)),
);
describe("shared wire vectors", () => {
  it.each(corpus.digest_vectors)("$id", (vector) => {
    const entries: Entry[] = vector.entries;
    expect(listingDigest(entries)).toBe(vector.listing_sha256);
    expect(manifestBytes(entries).toString("hex")).toBe(vector.manifest_preimage_hex);
    expect(sha256(manifestBytes(entries))).toBe(vector.manifest_sha256);
  });
});
describe("materialized shared qualification recipes", () => {
  it.each(corpus.cases)("$id", async (scenario) => {
    const result = await executeRecipe(scenario.recipe);
    expect({ outcome: result.outcome, level: result.level, reasons: result.reasons }).toEqual(
      scenario.expected,
    );
  });
});
