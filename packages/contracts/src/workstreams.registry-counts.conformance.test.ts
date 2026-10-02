import { describe, expect, it } from "vite-plus/test";
import * as NodeCrypto from "node:crypto";
import * as Schema from "effect/Schema";
import conformanceRaw from "./workstreams-fixtures/registry-counts.conformance.json.fixture?raw";
import negativesRaw from "./workstreams-fixtures/registry-counts.negative-cases.json.fixture?raw";
import { WorkstreamCapabilities, WorkstreamRegistryCounts } from "./workstreams.ts";

interface Corpus {
  readonly source: {
    readonly repository: string;
    readonly commit: string;
    readonly path: string;
    readonly sha256: string;
  };
  readonly cases: ReadonlyArray<{
    readonly name: string;
    readonly schema: string;
    readonly value: unknown;
  }>;
}
const conformance = JSON.parse(conformanceRaw) as Corpus;
const negatives = JSON.parse(negativesRaw) as Corpus;
const options = { onExcessProperty: "error" as const };
const sha256 = (raw: string) => NodeCrypto.createHash("sha256").update(raw).digest("hex");
const decode = (fixture: Corpus["cases"][number]) => {
  if (fixture.schema.endsWith("/RegistryCounts"))
    return Schema.decodeUnknownSync(WorkstreamRegistryCounts)(fixture.value, options);
  if (fixture.schema.endsWith("/Capabilities"))
    return Schema.decodeUnknownSync(WorkstreamCapabilities)(fixture.value, options);
  throw new Error(`Unsupported additive fixture ${fixture.schema}`);
};

describe("additive registry counts canonical fixtures", () => {
  it("pins selected case bytes and their complete canonical source corpora", () => {
    expect(sha256(conformanceRaw)).toBe(
      "dd65bbb21f3fd74157f44a67a509f7c8a798ca0162d7f49ab32e8711ef2bfa41",
    );
    expect(sha256(negativesRaw)).toBe(
      "3c39f87aaa7185b97baf46e2afb871b0765c20da0917f2effdb15ea2d988b6a8",
    );
    for (const [corpus, name, digest] of [
      [
        conformance,
        "conformance",
        "b28c57077fde370ac34e879c2a17f4c72af68cff7893d3aa41c6b6c79503853c",
      ],
      [
        negatives,
        "negative-cases",
        "b9272fc85eef9d87fa4a941f60a8f623860138a03f9066e52006f888ecee2945",
      ],
    ] as const)
      expect(corpus.source).toEqual({
        repository: "Jones-Systems/chatgpt-control-plane",
        commit: "9d7f199ffe25fabcdf4b993b59fd6019948f138a",
        path: `contracts/workstreams/v1/fixtures/${name}.json`,
        sha256: digest,
      });
    expect(conformance.cases.map((fixture) => fixture.name)).toEqual([
      "owner lifecycle counts with missing declaration",
      "counts-capable registry",
    ]);
    expect(negatives.cases.map((fixture) => fixture.name)).toEqual([
      "counts require explicit unknown lifecycle",
      "counts reject negative totals",
      "counts reject content bodies",
    ]);
  });
  it("accepts every additive counts and capability case", () => {
    for (const fixture of conformance.cases)
      expect(() => decode(fixture), fixture.name).not.toThrow();
  });
  it("rejects every additive negative case", () => {
    for (const fixture of negatives.cases) expect(() => decode(fixture), fixture.name).toThrow();
  });
});
