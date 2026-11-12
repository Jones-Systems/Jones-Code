import corpus from "./fixtures/workstreamsNative/v1/conformance.json" with { type: "json" };
import { WorkstreamsNativeReadEnvelope } from "./workstreamsNativeRead.ts";
import { WorkstreamsNativePlacementPage } from "./workstreamsNativePlacement.ts";
import { WorkstreamsSnapshotHeader } from "./workstreamsSnapshot.ts";
import { WorkstreamsRollupSummary } from "./workstreamsRollups.ts";
import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import * as Native from "./workstreamsNative.ts";
import { ExecutionEnvironmentCapabilities } from "../environment.ts";
import conformanceRaw from "../workstreams-fixtures/conformance.json.fixture?raw";
const registry = {
  registry_instance_id: "12345678-1234-1234-1234-123456789abc",
  owner_id: "owner-1",
  canonical_origin: "https://registry.workstreams.invalid",
  home_environment_id: "env-home",
  server_generation: 1,
};
const capability = {
  contract: Native.WORKSTREAMS_NATIVE_CONTRACT,
  mode: "native-home",
  registry,
  reads: [Native.WORKSTREAMS_NATIVE_READ_CONTRACT],
  placement_proofs: [Native.WORKSTREAMS_NATIVE_PLACEMENT_CONTRACT],
};
describe("native home protocol", () => {
  it("negotiates additively and rejects incoherent member and home capabilities", () => {
    const decode = decodeExecutionEnvironmentCapabilities;
    expect(decode({}).workstreamsNative).toBeUndefined();
    expect(decode({ workstreamsNative: capability }).workstreamsNative).toEqual(capability);
    const member = { ...capability, mode: "native-member", home_link: "expired" };
    expect(decodeWorkstreamsNativeCapability(member)).toEqual(member);
    for (const value of [
      { ...member, home_link: undefined },
      { ...capability, registry: null },
      { ...capability, home_link: "ok" },
      {
        ...capability,
        placement_proofs: [
          Native.WORKSTREAMS_NATIVE_PLACEMENT_CONTRACT,
          Native.WORKSTREAMS_NATIVE_PLACEMENT_CONTRACT,
        ],
      },
      { ...capability, registry: { ...registry, server_generation: Number.MAX_SAFE_INTEGER + 1 } },
    ])
      expect(() => decodeWorkstreamsNativeCapability(value)).toThrow();
  });
  it("retains command bytes for recovery while refusing malformed relay envelopes", () => {
    const corpus = JSON.parse(conformanceRaw) as {
      cases: Array<{ schema: string; value: unknown }>;
    };
    const command = corpus.cases.find((item) => item.schema.endsWith("/Command"))?.value;
    if (!command) throw new Error("Missing v1 fixture");
    const expected_home = {
      registry_instance_id: registry.registry_instance_id,
      owner_id: registry.owner_id,
      home_environment_id: registry.home_environment_id,
      server_generation: 1,
    };
    const request = {
      expected_home,
      relay: {
        source_environment_id: "env-source",
        origin: { kind: "agent" },
        linked_origin_environment_id: null,
        hop: 1,
      },
      body: JSON.stringify(command, null, 2),
      body_sha256: "a".repeat(64),
    };
    const decode = decodeWorkstreamsNativeHomeCommand;
    expect(decode(request).body).toBe(request.body);
    for (const value of [
      { ...request, body: command },
      { ...request, body: "{" },
      { ...request, relay: { ...request.relay, hop: 2 } },
      { ...request, expected_home: { ...expected_home, server_generation: 0 } },
      { ...request, relay: { ...request.relay, token: "unsupported" } },
    ])
      expect(() => decode(value)).toThrow();
  });
  it("never treats an incomplete archive as proven absence", () => {
    const decode = decodeWorkstreamsNativeReceiptLookup;
    const unknown = { kind: "unknown", reason: "import_incomplete" };
    expect(decode(unknown)).toEqual(unknown);
    const absent = {
      kind: "proven_absent",
      witness: { server_generation: 1, registry_version: 0, archive: "complete" },
    };
    expect(decode(absent)).toEqual(absent);
    for (const value of [
      { kind: "absent" },
      { ...absent, witness: { ...absent.witness, archive: "partial" } },
      { ...unknown, receipt: null },
    ])
      expect(() => decode(value)).toThrow();
    expect(() =>
      decodeWorkstreamsNativeHomeReceiptRequest({
        command_ids: ["command-000000001", "command-000000001"],
      }),
    ).toThrow();
  });
  it("refuses malformed encrypted-envelope encodings", () => {
    const envelope = {
      v: 1,
      alg: "A256GCM",
      key_id: "key-1",
      nonce: "A".repeat(16),
      ct: "YQ==",
      tag: "A".repeat(22) + "==",
    };
    const decode = decodeWorkstreamsEncryptedEnvelope;
    expect(decode(envelope)).toEqual(envelope);
    for (const change of [
      { nonce: "A".repeat(12) },
      { tag: "A".repeat(16) },
      { ct: "abc" },
      { plaintext: "forbidden" },
    ])
      expect(() => decode({ ...envelope, ...change })).toThrow();
  });
});

const decodeExecutionEnvironmentCapabilities = Schema.decodeUnknownSync(
  ExecutionEnvironmentCapabilities,
);
const decodeWorkstreamsEncryptedEnvelope = Schema.decodeUnknownSync(
  Native.WorkstreamsEncryptedEnvelope,
);
const decodeWorkstreamsNativeCapability = Schema.decodeUnknownSync(
  Native.WorkstreamsNativeCapability,
);
const decodeWorkstreamsNativeHomeCommand = Schema.decodeUnknownSync(
  Native.WorkstreamsNativeHomeCommand,
);
const decodeWorkstreamsNativeHomeReceiptRequest = Schema.decodeUnknownSync(
  Native.WorkstreamsNativeHomeReceiptRequest,
);
const decodeWorkstreamsNativeReceiptLookup = Schema.decodeUnknownSync(
  Native.WorkstreamsNativeReceiptLookup,
);
const decodeWorkstreamsNativePlacementPage = Schema.decodeUnknownSync(
  WorkstreamsNativePlacementPage,
);
const decodeWorkstreamsNativeReadEnvelope = Schema.decodeUnknownSync(WorkstreamsNativeReadEnvelope);
const decodeWorkstreamsRollupSummary = Schema.decodeUnknownSync(WorkstreamsRollupSummary);
const decodeWorkstreamsSnapshotHeader = Schema.decodeUnknownSync(WorkstreamsSnapshotHeader);
const vectorDecoders = new Map<string, (input: unknown) => unknown>([
  ["WorkstreamsNativeCapability", decodeWorkstreamsNativeCapability],
  ["WorkstreamsNativeReadEnvelope", decodeWorkstreamsNativeReadEnvelope],
  ["WorkstreamsNativePlacementPage", decodeWorkstreamsNativePlacementPage],
  ["WorkstreamsSnapshotHeader", decodeWorkstreamsSnapshotHeader],
  ["WorkstreamsRollupSummary", decodeWorkstreamsRollupSummary],
]);
it.each(corpus.cases)("round-trips the published $schema consumer vector", ({ schema, value }) => {
  const decode = vectorDecoders.get(schema);
  if (!decode) throw new Error("Unknown native vector schema");
  expect(decode(JSON.parse(JSON.stringify(value)))).toEqual(value);
});
