import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  WorkstreamsNativeHomeRead,
  WorkstreamsNativeReadEnvelope,
} from "./workstreamsNativeRead.ts";
const read = {
  contract: "workstreams-read/1.0.0",
  registry: {
    registry_instance_id: "12345678-1234-1234-1234-123456789abc",
    owner_id: "owner-1",
    canonical_origin: "https://registry.workstreams.invalid",
    server_generation: 1,
    registry_version: 0,
  },
  provenance: {
    authority: "native-home",
    home_environment_id: "home-1",
    served_by_environment_id: "member-1",
    source: "live",
    observed_at: "2026-10-25T12:00:00Z",
    stale: false,
  },
  coverage: {
    registry: "complete",
    sources: [
      {
        source_instance_id: "source-1",
        mapping: "unmapped",
        attestation: "unknown",
        observed_at: null,
      },
    ],
  },
};
describe("native read coverage", () => {
  it("preserves unknown source evidence and rejects false registry completeness", () => {
    const decode = decodeWorkstreamsNativeReadEnvelope;
    expect(decode(read)).toEqual(read);
    const cached = {
      ...read,
      provenance: { ...read.provenance, source: "cache", stale: true },
      coverage: { ...read.coverage, registry: "stale" },
    };
    expect(decode(cached)).toEqual(cached);
    for (const value of [
      { ...read, provenance: cached.provenance },
      { ...read, coverage: { ...read.coverage, registry: "stale" } },
      { ...read, provenance: { ...read.provenance, authority: "legacy-archive" } },
      {
        ...read,
        coverage: {
          ...read.coverage,
          sources: [{ ...read.coverage.sources[0], attestation: "current" }],
        },
      },
      {
        ...read,
        coverage: {
          ...read.coverage,
          sources: [...read.coverage.sources, ...read.coverage.sources],
        },
      },
    ])
      expect(() => decode(value)).toThrow();
  });
  it("requires the identities appropriate to each forwarded read", () => {
    const decode = decodeWorkstreamsNativeHomeRead;
    for (const value of [
      { route: "workstream", params: { workstream_id: "ws-1" } },
      { route: "reference", params: { native_reference_id: "ref-1" } },
      { route: "history", params: { limit: 100 } },
      { route: "appearance", params: { workstream_ids: ["ws-1"] } },
      {
        route: "placements",
        params: { identities: [{ source_instance_id: "env-1", native_thread_id: "thread-1" }] },
      },
    ])
      expect(decode(value)).toEqual(value);
    for (const value of [
      { route: "workstream", params: {} },
      { route: "reference", params: { workstream_id: "ws-1" } },
      { route: "counts", params: { cursor: "abc" } },
      { route: "placements", params: {} },
      { route: "history", params: { limit: 101 } },
    ])
      expect(() => decode(value)).toThrow();
  });
});

const decodeWorkstreamsNativeHomeRead = Schema.decodeUnknownSync(WorkstreamsNativeHomeRead);
const decodeWorkstreamsNativeReadEnvelope = Schema.decodeUnknownSync(WorkstreamsNativeReadEnvelope);
