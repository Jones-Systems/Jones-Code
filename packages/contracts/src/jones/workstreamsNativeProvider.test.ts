/// <reference types="vite-plus/client" />
// @effect-diagnostics nodeBuiltinImport:off - Hashes immutable fixture bytes outside an Effect runtime.
import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import * as NodeCrypto from "node:crypto";
import manifest from "../../contracts/workstreams-t3-provider/v1/manifest.json" with { type: "json" };
import conformanceCorpus from "../../contracts/workstreams-t3-provider/v1/fixtures/conformance.json" with { type: "json" };
import negativeCorpus from "../../contracts/workstreams-t3-provider/v1/fixtures/negative-cases.json" with { type: "json" };
import readmeRaw from "../../contracts/workstreams-t3-provider/v1/README.md?raw";
import conformanceRaw from "../../contracts/workstreams-t3-provider/v1/fixtures/conformance.json?raw";
import negativesRaw from "../../contracts/workstreams-t3-provider/v1/fixtures/negative-cases.json?raw";
import schemaRaw from "../../contracts/workstreams-t3-provider/v1/schemas/provider.schema.json?raw";
import * as Provider from "./workstreamsNativeProvider.ts";

const rawFiles: Readonly<Record<string, string>> = {
  "README.md": readmeRaw,
  "fixtures/conformance.json": conformanceRaw,
  "fixtures/negative-cases.json": negativesRaw,
  "schemas/provider.schema.json": schemaRaw,
};

interface Fixture {
  readonly name: string;
  readonly schema: string;
  readonly value: unknown;
}
const conformance: readonly Fixture[] = conformanceCorpus.cases;
const negatives: readonly Fixture[] = negativeCorpus.cases;
const decoders = new Map<string, (value: unknown) => unknown>([
  ["NativeIdentity", Schema.decodeUnknownSync(Provider.WorkstreamsNativeIdentity)],
  ["Build", Schema.decodeUnknownSync(Provider.WorkstreamsNativeBuild)],
  ["Context", Schema.decodeUnknownSync(Provider.WorkstreamsNativeContext)],
  ["ContextRequest", Schema.decodeUnknownSync(Provider.WorkstreamsNativeContextRequest)],
  ["ContextResponse", Schema.decodeUnknownSync(Provider.WorkstreamsNativeContextResponse)],
  ["AttestationRequest", Schema.decodeUnknownSync(Provider.WorkstreamsNativeAttestationRequest)],
  [
    "RegistrationAttestation",
    Schema.decodeUnknownSync(Provider.WorkstreamsNativeRegistrationAttestation),
  ],
  ["AttestationResponse", Schema.decodeUnknownSync(Provider.WorkstreamsNativeAttestationResponse)],
  ["Association", Schema.decodeUnknownSync(Provider.WorkstreamsNativeAssociation)],
  ["SettlementRequest", Schema.decodeUnknownSync(Provider.WorkstreamsNativeSettlementRequest)],
  [
    "SettlementLookupRequest",
    Schema.decodeUnknownSync(Provider.WorkstreamsNativeSettlementLookupRequest),
  ],
  ["ResultEvidence", Schema.decodeUnknownSync(Provider.WorkstreamsNativeResultEvidence)],
  ["TerminalResult", Schema.decodeUnknownSync(Provider.WorkstreamsNativeTerminalResult)],
  ["AcceptedReceipt", Schema.decodeUnknownSync(Provider.WorkstreamsNativeAcceptedReceipt)],
  ["RejectedReceipt", Schema.decodeUnknownSync(Provider.WorkstreamsNativeRejectedReceipt)],
  ["SettlementEvent", Schema.decodeUnknownSync(Provider.WorkstreamsNativeSettlementEvent)],
  ["SettlementResponse", Schema.decodeUnknownSync(Provider.WorkstreamsNativeSettlementResponse)],
  [
    "SettlementLookupResponse",
    Schema.decodeUnknownSync(Provider.WorkstreamsNativeSettlementLookupResponse),
  ],
  ["ProviderRequest", Schema.decodeUnknownSync(Provider.WorkstreamsNativeProviderRequest)],
  ["ProviderResponse", Schema.decodeUnknownSync(Provider.WorkstreamsNativeProviderResponse)],
]);
function decode(fixture: Fixture): unknown {
  const name = fixture.schema.slice(fixture.schema.lastIndexOf("/") + 1);
  const decoder = decoders.get(name);
  if (decoder === undefined) throw new Error(`Uncovered fixture schema: ${name}`);
  return decoder(fixture.value);
}

describe("workstreams-t3-provider/1.0.0 native contract", () => {
  it.each(manifest.files)("preserves manifest-pinned bytes for $path", ({ path, sha256 }) => {
    const bytes = rawFiles[path];
    if (bytes === undefined) throw new Error(`Uncovered manifest file: ${path}`);
    expect(NodeCrypto.createHash("sha256").update(bytes).digest("hex")).toBe(sha256);
  });

  it("selects the pinned family and complete versioned fixture corpus", () => {
    expect(manifest.contract_family).toBe(Provider.WORKSTREAMS_T3_PROVIDER_FAMILY);
    expect(manifest.contract_version).toBe(Provider.WORKSTREAMS_T3_PROVIDER_VERSION);
    expect(manifest.manifest_sha256).toBe(Provider.WORKSTREAMS_T3_PROVIDER_MANIFEST_SHA256);
    expect(manifest.files.map((file) => file.path)).toEqual(
      [...manifest.files.map((file) => file.path)].sort(),
    );
    expect(conformanceCorpus.contract_family).toBe(Provider.WORKSTREAMS_T3_PROVIDER_FAMILY);
    expect(conformanceCorpus.contract_version).toBe(Provider.WORKSTREAMS_T3_PROVIDER_VERSION);
    expect(negativeCorpus.contract_family).toBe(Provider.WORKSTREAMS_T3_PROVIDER_FAMILY);
    expect(negativeCorpus.contract_version).toBe(Provider.WORKSTREAMS_T3_PROVIDER_VERSION);
    expect(conformance).toHaveLength(58);
    expect(negatives).toHaveLength(135);
  });

  it.each(conformance)("decodes $name without caller parse options", (fixture) => {
    expect(decode(fixture)).toEqual(fixture.value);
  });

  it.each(negatives)("rejects $name without caller parse options", (fixture) => {
    expect(() => decode(fixture)).toThrow();
  });

  it("retains full association fields for the transport identity evaluator", () => {
    const fixture = conformance.find(
      (candidate) => candidate.name === "association mismatch requires adapter evaluation",
    );
    if (fixture === undefined) throw new Error("Missing association mismatch fixture");
    const response = Schema.decodeUnknownSync(Provider.WorkstreamsNativeSettlementResponse)(
      fixture.value,
    );
    expect(response.state).toBe("terminal");
    if (response.state !== "terminal") throw new Error("Expected terminal fixture");
    expect(response.result.native_evidence.principal_id).not.toBe(response.request.principal_id);
  });
});

const v2Files = import.meta.glob<string>("../../contracts/workstreams-t3-provider/v2/**", {
  query: "?raw",
  import: "default",
  eager: true,
});
const v2File = (path: string) => v2Files[`../../contracts/workstreams-t3-provider/v2/${path}`]!;
const v2Manifest = JSON.parse(v2File("manifest.json")) as typeof manifest;
const v2Conformance = JSON.parse(v2File("fixtures/conformance.json")) as { cases: Fixture[] };
const v2Negatives = JSON.parse(v2File("fixtures/negative-cases.json")) as { cases: Fixture[] };
const v2Decoders = new Map(decoders);
v2Decoders.set(
  "ContextResponse",
  Schema.decodeUnknownSync(Provider.WorkstreamsNativeContextResponseV2),
);
v2Decoders.set(
  "AttestationResponse",
  Schema.decodeUnknownSync(Provider.WorkstreamsNativeAttestationResponseV2),
);
v2Decoders.set(
  "SettlementResponse",
  Schema.decodeUnknownSync(Provider.WorkstreamsNativeSettlementResponseV2),
);
v2Decoders.set(
  "SettlementLookupResponse",
  Schema.decodeUnknownSync(Provider.WorkstreamsNativeSettlementLookupResponseV2),
);
v2Decoders.set(
  "ProviderResponse",
  Schema.decodeUnknownSync(Provider.WorkstreamsNativeProviderResponseV2),
);
const decodeV2 = (fixture: Fixture) => {
  const name = fixture.schema.slice(fixture.schema.lastIndexOf("/") + 1);
  const decoder = v2Decoders.get(name);
  if (!decoder) throw new Error(`Uncovered v2 fixture schema: ${name}`);
  return decoder(fixture.value);
};
describe("workstreams-t3-provider/2.0.0 additive native contract", () => {
  it.each(v2Manifest.files)("preserves producer bytes for $path", ({ path, sha256 }) => {
    expect(NodeCrypto.createHash("sha256").update(v2File(path)).digest("hex")).toBe(sha256);
  });
  it("pins the complete producer manifest", () => {
    const records = v2Manifest.files.map(({ path, sha256 }) => `${sha256}  ${path}\n`).join("");
    expect(NodeCrypto.createHash("sha256").update(records).digest("hex")).toBe(
      Provider.WORKSTREAMS_T3_PROVIDER_V2_MANIFEST_SHA256,
    );
    expect(v2Manifest.manifest_sha256).toBe(Provider.WORKSTREAMS_T3_PROVIDER_V2_MANIFEST_SHA256);
  });
  it.each(v2Conformance.cases)("decodes $name", (fixture) => {
    expect(decodeV2(fixture)).toEqual(fixture.value);
  });
  it.each(
    (
      JSON.parse(v2File("fixtures/semantic-cases.json")) as {
        cases: Array<Fixture & { expected_enrollment: unknown }>;
      }
    ).cases,
  )("preserves the full $name for consumer qualification", (fixture) => {
    const response = Schema.decodeUnknownSync(Provider.WorkstreamsNativeContextResponseV2)(
      fixture.value,
    );
    expect(response.state).toBe("ready");
    if (response.state === "ready")
      expect(response.enrollment).not.toEqual(fixture.expected_enrollment);
  });
  it.each(v2Negatives.cases)("rejects $name", (fixture) => {
    expect(() => decodeV2(fixture)).toThrow();
  });
});
