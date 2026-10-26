import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { T3ThreadPlacement } from "../workstreamPlacements.ts";
import {
  WorkstreamsNativeThreadPlacement,
  WorkstreamsNativePlacementPage,
} from "./workstreamsNativePlacement.ts";
const item = {
  membership_id: "m-1",
  workstream_id: "ws-1",
  native_reference_id: "ref-1",
  kind: "primary",
  source_instance_id: "source-1",
  native_thread_id: "thread-1",
  attestation_version: 1,
  attested_at: "2026-10-25T12:00:00Z",
  expires_at: "2026-10-25T12:00:00.1Z",
  evidence_sha256: "a".repeat(64),
  source_binding_version: 1,
  authority_namespace: "authority-1",
  store_generation: 1,
  attestation_source: "native",
  evidence_contract: "workstreams-t3-provider/2.0.0",
};
describe("native placement proofs", () => {
  it("retains historical decoding and compares mixed precision without losing microseconds", () => {
    const { attestation_source: _source, evidence_contract: _contract, ...legacy } = item;
    expect(decodeT3ThreadPlacement(legacy)).toEqual(legacy);
    const decode = decodeWorkstreamsNativeThreadPlacement;
    expect(decode(item)).toEqual(item);
    for (const value of [
      legacy,
      { ...item, attestation_source: "control-plane" },
      { ...item, evidence_contract: "workstreams-t3-provider/1.0.0" },
      { ...item, expires_at: item.attested_at },
      {
        ...item,
        attested_at: "2026-10-25T12:00:00.000002Z",
        expires_at: "2026-10-25T12:00:00.000001Z",
      },
    ])
      expect(() => decode(value)).toThrow();
  });
  it("binds a complete placement snapshot to its home generation", () => {
    const page = {
      inventory_sha256: "b".repeat(64),
      context: {
        owner_id: "owner-1",
        principal_id: "principal-1",
        authorization_revision: 1,
        server_generation: 2,
        registry_version: 0,
      },
      provenance: {
        authority: "native-home",
        registry_instance_id: "12345678-1234-1234-1234-123456789abc",
        home_environment_id: "home-1",
        home_generation: 2,
        served_by_environment_id: "source-1",
        observed_at: "2026-10-25T12:00:00Z",
      },
      items: [item],
      next_cursor: null,
    };
    const decode = decodeWorkstreamsNativePlacementPage;
    expect(decode(page)).toEqual(page);
    expect(() =>
      decode({ ...page, provenance: { ...page.provenance, home_generation: 1 } }),
    ).toThrow();
    expect(() => decode({ ...page, complete: false })).toThrow();
  });
});

const decodeT3ThreadPlacement = Schema.decodeUnknownSync(T3ThreadPlacement);
const decodeWorkstreamsNativePlacementPage = Schema.decodeUnknownSync(
  WorkstreamsNativePlacementPage,
);
const decodeWorkstreamsNativeThreadPlacement = Schema.decodeUnknownSync(
  WorkstreamsNativeThreadPlacement,
);
