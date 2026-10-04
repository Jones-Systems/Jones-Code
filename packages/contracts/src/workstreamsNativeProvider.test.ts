import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import manifest from "../contracts/workstreams-t3-provider/v1/manifest.json" with { type: "json" };
import conformanceCorpus from "../contracts/workstreams-t3-provider/v1/fixtures/conformance.json" with { type: "json" };
import negativeCorpus from "../contracts/workstreams-t3-provider/v1/fixtures/negative-cases.json" with { type: "json" };
import * as Provider from "./workstreamsNativeProvider.ts";

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
    const bytes = NodeFS.readFileSync(
      new URL(`../contracts/workstreams-t3-provider/v1/${path}`, import.meta.url),
    );
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
