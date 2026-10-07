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
      [...manifest.files.map((file) => file.path)].sort(),
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
