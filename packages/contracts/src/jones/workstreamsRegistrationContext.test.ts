/// <reference types="vite-plus/client" />
// @effect-diagnostics nodeBuiltinImport:off - Hashes immutable fixture bytes outside an Effect runtime.
import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import * as NodeCrypto from "node:crypto";
import manifest from "../../contracts/workstreams-registration-context/v1/manifest.json" with { type: "json" };
import conformanceCorpus from "../../contracts/workstreams-registration-context/v1/fixtures/conformance.json" with { type: "json" };
import negativeCorpus from "../../contracts/workstreams-registration-context/v1/fixtures/negative-cases.json" with { type: "json" };
import readmeRaw from "../../contracts/workstreams-registration-context/v1/README.md?raw";
import conformanceRaw from "../../contracts/workstreams-registration-context/v1/fixtures/conformance.json?raw";
import negativesRaw from "../../contracts/workstreams-registration-context/v1/fixtures/negative-cases.json?raw";
import schemaRaw from "../../contracts/workstreams-registration-context/v1/schemas/registration-context.schema.json?raw";
import v2Manifest from "../../contracts/workstreams-registration-context/v2/manifest.json" with { type: "json" };
import v2Conformance from "../../contracts/workstreams-registration-context/v2/fixtures/conformance.json" with { type: "json" };
import v2Negatives from "../../contracts/workstreams-registration-context/v2/fixtures/negative-cases.json" with { type: "json" };
import v2Semantics from "../../contracts/workstreams-registration-context/v2/fixtures/semantic-cases.json" with { type: "json" };
import v2ReadmeRaw from "../../contracts/workstreams-registration-context/v2/README.md?raw";
import v2ConformanceRaw from "../../contracts/workstreams-registration-context/v2/fixtures/conformance.json?raw";
import v2NegativesRaw from "../../contracts/workstreams-registration-context/v2/fixtures/negative-cases.json?raw";
import v2SemanticsRaw from "../../contracts/workstreams-registration-context/v2/fixtures/semantic-cases.json?raw";
import v2SchemaRaw from "../../contracts/workstreams-registration-context/v2/schemas/registration-context.schema.json?raw";
import v3Manifest from "../../contracts/workstreams-registration-context/v3/manifest.json" with { type: "json" };
import v3Conformance from "../../contracts/workstreams-registration-context/v3/fixtures/conformance.json" with { type: "json" };
import v3Negatives from "../../contracts/workstreams-registration-context/v3/fixtures/negative-cases.json" with { type: "json" };
import v3Semantics from "../../contracts/workstreams-registration-context/v3/fixtures/semantic-cases.json" with { type: "json" };
import v3ReadmeRaw from "../../contracts/workstreams-registration-context/v3/README.md?raw";
import v3ConformanceRaw from "../../contracts/workstreams-registration-context/v3/fixtures/conformance.json?raw";
import v3NegativesRaw from "../../contracts/workstreams-registration-context/v3/fixtures/negative-cases.json?raw";
import v3SemanticsRaw from "../../contracts/workstreams-registration-context/v3/fixtures/semantic-cases.json?raw";
import v3SchemaRaw from "../../contracts/workstreams-registration-context/v3/schemas/registration-context.schema.json?raw";
import * as Context from "./workstreamsRegistrationContext.ts";

const rawFiles: Readonly<Record<string, string>> = {
  "README.md": readmeRaw,
  "fixtures/conformance.json": conformanceRaw,
  "fixtures/negative-cases.json": negativesRaw,
  "schemas/registration-context.schema.json": schemaRaw,
};

interface Fixture {
  readonly name: string;
  readonly schema: string;
  readonly value: unknown;
}
const conformance: readonly Fixture[] = conformanceCorpus.cases;
const negatives: readonly Fixture[] = negativeCorpus.cases;
const decoders = new Map<string, (value: unknown) => unknown>([
  ["Request", Schema.decodeUnknownSync(Context.WorkstreamsRegistrationContextRequest)],
  ["Build", Schema.decodeUnknownSync(Context.WorkstreamsRegistrationContextBuild)],
  ["T3Source", Schema.decodeUnknownSync(Context.WorkstreamsRegistrationContextT3Source)],
  ["GitHubSource", Schema.decodeUnknownSync(Context.WorkstreamsRegistrationContextGitHubSource)],
  ["Source", Schema.decodeUnknownSync(Context.WorkstreamsRegistrationContextSource)],
  ["Sources", Schema.decodeUnknownSync(Context.WorkstreamsRegistrationContextSources)],
  ["Response", Schema.decodeUnknownSync(Context.WorkstreamsRegistrationContextResponse)],
]);
function decode(fixture: Fixture): unknown {
  const name = fixture.schema.slice(fixture.schema.lastIndexOf("/") + 1);
  const decoder = decoders.get(name);
  if (decoder === undefined) throw new Error(`Uncovered fixture schema: ${name}`);
  return decoder(fixture.value);
}

describe("workstreams-registration-context/1.0.0 native contract", () => {
  it.each(manifest.files)("preserves manifest-pinned bytes for $path", ({ path, sha256 }) => {
    const bytes = rawFiles[path];
    if (bytes === undefined) throw new Error(`Uncovered manifest file: ${path}`);
    expect(NodeCrypto.createHash("sha256").update(bytes).digest("hex")).toBe(sha256);
  });

  it("selects the pinned family and complete versioned fixture corpus", () => {
    expect(manifest.contract_family).toBe(Context.WORKSTREAMS_REGISTRATION_CONTEXT_FAMILY);
    expect(manifest.contract_version).toBe(Context.WORKSTREAMS_REGISTRATION_CONTEXT_VERSION);
    expect(manifest.manifest_sha256).toBe(Context.WORKSTREAMS_REGISTRATION_CONTEXT_MANIFEST_SHA256);
    expect(manifest.files.map((file) => file.path)).toEqual(
      manifest.files.map((file) => file.path).sort(),
    );
    expect(conformanceCorpus.contract_family).toBe(Context.WORKSTREAMS_REGISTRATION_CONTEXT_FAMILY);
    expect(conformanceCorpus.contract_version).toBe(
      Context.WORKSTREAMS_REGISTRATION_CONTEXT_VERSION,
    );
    expect(negativeCorpus.contract_family).toBe(Context.WORKSTREAMS_REGISTRATION_CONTEXT_FAMILY);
    expect(negativeCorpus.contract_version).toBe(Context.WORKSTREAMS_REGISTRATION_CONTEXT_VERSION);
    expect(conformance).toHaveLength(20);
    expect(negatives).toHaveLength(161);
  });

  it.each(conformance)("decodes $name without caller parse options", (fixture) => {
    expect(decode(fixture)).toEqual(fixture.value);
  });

  it.each(negatives)("rejects $name without caller parse options", (fixture) => {
    expect(() => decode(fixture)).toThrow();
  });
});

const v2RawFiles: Readonly<Record<string, string>> = {
  "README.md": v2ReadmeRaw,
  "fixtures/conformance.json": v2ConformanceRaw,
  "fixtures/negative-cases.json": v2NegativesRaw,
  "fixtures/semantic-cases.json": v2SemanticsRaw,
  "schemas/registration-context.schema.json": v2SchemaRaw,
};
const decodeV2 = (fixture: Fixture) => {
  if (fixture.schema === "#/$defs/Request")
    return Schema.decodeUnknownSync(Context.WorkstreamsRegistrationContextRequest)(fixture.value);
  return Schema.decodeUnknownSync(Context.WorkstreamsRegistrationContextV2Response)(fixture.value);
};

describe("workstreams-registration-context/2.0.0 pinned consumer", () => {
  it.each(v2Manifest.files)("preserves producer bytes for $path", ({ path, sha256 }) => {
    const bytes = v2RawFiles[path];
    if (bytes === undefined) throw new Error(`Uncovered manifest file: ${path}`);
    expect(NodeCrypto.createHash("sha256").update(bytes).digest("hex")).toBe(sha256);
  });

  it("pins the producer and exact family manifest independently of v1", () => {
    expect(Context.WORKSTREAMS_REGISTRATION_CONTEXT_V2_PRODUCER_COMMIT).toBe(
      "0817d23ca2547c5ff8b88b0034e6e2b1de0dd431",
    );
    expect(v2Manifest.contract_version).toBe(Context.WORKSTREAMS_REGISTRATION_CONTEXT_V2_VERSION);
    const digestInput = v2Manifest.files.map(({ path, sha256 }) => `${sha256}  ${path}\n`).join("");
    expect(NodeCrypto.createHash("sha256").update(digestInput).digest("hex")).toBe(
      Context.WORKSTREAMS_REGISTRATION_CONTEXT_V2_MANIFEST_SHA256,
    );
    expect(v2Manifest.manifest_sha256).toBe(
      Context.WORKSTREAMS_REGISTRATION_CONTEXT_V2_MANIFEST_SHA256,
    );
    expect(v2Conformance.contract_version).toBe("2.0.0");
    expect(v2Negatives.contract_version).toBe("2.0.0");
    expect(v2Semantics.contract_version).toBe("2.0.0");
  });

  it.each(v2Conformance.cases)("decodes $name", (fixture) => {
    expect(decodeV2(fixture)).toEqual(fixture.value);
  });
  it.each([...v2Negatives.cases, ...v2Semantics.cases])("rejects $name", (fixture) => {
    expect(() => decodeV2(fixture)).toThrow();
  });
});

const v3RawFiles: Readonly<Record<string, string>> = {
  "README.md": v3ReadmeRaw,
  "fixtures/conformance.json": v3ConformanceRaw,
  "fixtures/negative-cases.json": v3NegativesRaw,
  "fixtures/semantic-cases.json": v3SemanticsRaw,
  "schemas/registration-context.schema.json": v3SchemaRaw,
};
const decodeV3 = (fixture: Fixture) => {
  if (fixture.schema === "#/$defs/Request")
    return Schema.decodeUnknownSync(Context.WorkstreamsRegistrationContextRequest)(fixture.value);
  return Schema.decodeUnknownSync(Context.WorkstreamsRegistrationContextV3Response)(fixture.value);
};

describe("workstreams-registration-context/3.0.0 pinned consumer", () => {
  it("accepts exactly the three ordered native profile policies on a closed T3 descriptor", () => {
    const response = Schema.decodeUnknownSync(Context.WorkstreamsRegistrationContextV3Response)(
      v3Conformance.cases[1]!.value,
    );
    const source = response.sources.find((entry) => entry.provider === "t3")!;
    const decodeSource = Schema.decodeUnknownSync(Context.WorkstreamsRegistrationContextV3T3Source);
    const v1 = "workstreams-t3-provider/1.0.0";
    const v2 = "workstreams-t3-provider/2.0.0";
    for (const accepted_native_profiles of [[v1], [v2, v1], [v2]]) {
      const value = { ...source, accepted_native_profiles };
      expect(decodeSource(value)).toEqual(value);
    }
    for (const accepted_native_profiles of [[], [v1, v2], [v1, v1], [v2, v1, v2], ["unknown"]]) {
      expect(() => decodeSource({ ...source, accepted_native_profiles })).toThrow();
    }
    expect(() => decodeSource({ ...source, observed_build: source.enrolled_build })).toThrow();
  });

  it.each(v3Manifest.files)("preserves producer bytes for $path", ({ path, sha256 }) => {
    const bytes = v3RawFiles[path];
    if (bytes === undefined) throw new Error(`Uncovered manifest file: ${path}`);
    expect(NodeCrypto.createHash("sha256").update(bytes).digest("hex")).toBe(sha256);
  });

  it("pins the producer and exact family manifest independently of v1", () => {
    expect(Context.WORKSTREAMS_REGISTRATION_CONTEXT_V3_PRODUCER_COMMIT).toBe(
      "e319288320c25bd6d899b3ab8d60da549354ae6f",
    );
    expect(v3Manifest.contract_version).toBe(Context.WORKSTREAMS_REGISTRATION_CONTEXT_V3_VERSION);
    const digestInput = v3Manifest.files.map(({ path, sha256 }) => `${sha256}  ${path}\n`).join("");
    expect(NodeCrypto.createHash("sha256").update(digestInput).digest("hex")).toBe(
      Context.WORKSTREAMS_REGISTRATION_CONTEXT_V3_MANIFEST_SHA256,
    );
    expect(v3Manifest.manifest_sha256).toBe(
      Context.WORKSTREAMS_REGISTRATION_CONTEXT_V3_MANIFEST_SHA256,
    );
    expect(v3Manifest.files.find(({ path }) => path.startsWith("schemas/"))?.sha256).toBe(
      "816e269b29912e30efeb9e1707c4b7438e15fca6fe7e0c02c3797ff8e1fb924f",
    );
    expect(v3Conformance.cases).toHaveLength(7);
    expect(v3Negatives.cases).toHaveLength(32);
    expect(v3Semantics.cases).toHaveLength(2);
    expect(v3Conformance.contract_version).toBe("3.0.0");
    expect(v3Negatives.contract_version).toBe("3.0.0");
    expect(v3Semantics.contract_version).toBe("3.0.0");
  });

  it.each(v3Conformance.cases)("decodes $name", (fixture) => {
    expect(decodeV3(fixture)).toEqual(fixture.value);
  });
  it.each([...v3Negatives.cases, ...v3Semantics.cases])("rejects $name", (fixture) => {
    expect(() => decodeV3(fixture)).toThrow();
  });
});
