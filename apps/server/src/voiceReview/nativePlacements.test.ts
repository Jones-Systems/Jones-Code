import { describe, expect, it } from "vite-plus/test";
import { ThreadRegistryComposedSnapshot } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

const legacyPlacement = {
  membership_id: "historical-membership",
  workstream_id: "legacy-id:kept",
  native_reference_id: "historical-reference",
  kind: "primary",
  source_instance_id: "historical-environment",
  native_thread_id: "historical-thread",
  authority_namespace: "historical-authority",
  store_generation: 3,
  attestation_version: 2,
  attested_at: "2026-01-01T00:00:00Z",
  expires_at: "2026-01-01T00:05:00Z",
  evidence_sha256: "a".repeat(64),
  source_binding_version: 1,
};
const historical = {
  schema: "voice.registry-read/v1",
  snapshot_revision: 3,
  next_cursor: null,
  partial: false,
  unavailable: [],
  threads: [
    {
      thread_key: '["historical-host","historical-environment","historical-thread"]',
      registration: null,
      summary: null,
      activity: null,
      freshness: {},
      associations: [],
      native_memberships: [{ workstream_ref: "native:legacy-id:kept", placement: legacyPlacement }],
    },
  ],
};

describe("voice membership wire compatibility", () => {
  it("round trips legacy native envelopes and persisted references without personal fields", () => {
    const decoded = Schema.decodeUnknownSync(ThreadRegistryComposedSnapshot)(historical);
    expect(decoded.threads[0]?.personal_memberships).toBeUndefined();
    expect(Schema.encodeSync(ThreadRegistryComposedSnapshot)(decoded)).toEqual(historical);
  });
  it("accepts new local membership without placement proofs while retaining the legacy array", () => {
    const membership = {
      workstream_ref: "native:legacy-id:kept",
      workstream_id: "legacy-id:kept",
      name: "Synthetic group",
      environment_id: "historical-environment",
      thread_id: "historical-thread",
      revision: 4,
    };
    const current = {
      ...historical,
      threads: historical.threads.map((thread) => ({
        ...thread,
        native_memberships: [],
        personal_memberships: [membership],
      })),
    };
    expect(
      Schema.encodeSync(ThreadRegistryComposedSnapshot)(
        Schema.decodeUnknownSync(ThreadRegistryComposedSnapshot)(current),
      ),
    ).toEqual(current);
  });
});
