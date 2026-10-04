import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { readFileSync } from "node:fs";
import { WorkQueueMetadata, WorkQueueMetadataResult } from "./workQueueMetadata.ts";

const item = {
  request_id: "request-1",
  workstream_id: "canonical-id",
  canonical_binding: null,
  entry_kind: "ordinary",
  request_kind: "initial",
  lane: "normal",
  queue_state: "unknown",
  submitted_at_ms: null,
  target: null,
  dispatch_status: null,
  native_command_status: null,
  finish_line: "not_tracked",
};
const fixture = {
  schema: "codex.t3-work-queue-metadata/v1",
  source: {
    queue_id: "queue",
    host_id: "host",
    environment_ref: "environment",
    exporter_instance_id: "exporter",
  },
  observed_at_ms: 1_000,
  snapshot_token: "a".repeat(64),
  coverage: "complete",
  items: [item],
  authority_effect: "none",
};
const decode = Schema.decodeUnknownSync(WorkQueueMetadata);

describe("work queue metadata contract", () => {
  it("decodes the exact Python producer golden artifact without changing its identity", () => {
    const bytes = readFileSync(new URL("./fixtures/work_queue_metadata_v1.json", import.meta.url));
    expect(bytes.byteLength).toBe(923);
    expect(bytes[bytes.length - 1]).toBe(125);
    const parsed = JSON.parse(bytes.toString("utf8"));
    const snapshot = decode(parsed);
    expect(snapshot).toEqual(parsed);
    expect(snapshot.snapshot_token).toBe(
      "5a648884903f4aa9d8932802522ff8735c2d2b040c5ec32ede1235001a8015fb",
    );
    expect(snapshot.items[0]).toMatchObject({
      workstream_id: "canonical-work",
      canonical_binding: {
        membership_id: "membership",
        native_reference_id: "native-ref",
        native_thread_id: "thread",
      },
      finish_line: "not_tracked",
    });
  });
  it("preserves raw identity and unknown state without synthesizing completion", () => {
    expect(decode(fixture)).toEqual(fixture);
    expect(() => decode({ ...fixture, items: [{ ...item, finish_line: "completed" }] })).toThrow();
  });
  it("rejects private and unknown fields at every object boundary with default decoding", () => {
    const variants = [
      { ...fixture, prompt: "private" },
      { ...fixture, source: { ...fixture.source, path: "/private" } },
      { ...fixture, items: [{ ...item, body: "private" }] },
      {
        ...fixture,
        items: [
          {
            ...item,
            target: {
              host_id: "host",
              environment_ref: "env",
              thread_id: "thread",
              account: "private",
            },
          },
        ],
      },
    ];
    for (const value of variants) expect(() => decode(value)).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(WorkQueueMetadataResult)({
        status: "unavailable",
        reason: "source_unavailable",
        error: "private",
      }),
    ).toThrow();
  });
  it("rejects unsafe times, excessive items, duplicate requests and invalid discriminants", () => {
    for (const value of [
      { ...fixture, observed_at_ms: Number.MAX_SAFE_INTEGER + 1 },
      { ...fixture, observed_at_ms: -1 },
      { ...fixture, observed_at_ms: 1.5 },
      { ...fixture, schema: "other" },
      {
        ...fixture,
        items: Array.from({ length: 1001 }, (_, index) => ({ ...item, request_id: `r${index}` })),
      },
      { ...fixture, items: [item, item] },
      { ...fixture, items: [{ ...item, queue_state: "success" }] },
      { ...fixture, items: [{ ...item, submitted_at_ms: 1_001 }] },
    ])
      expect(() => decode(value)).toThrow();
  });
  it("accepts only bounded, current-at-sample, exact-thread canonical bindings", () => {
    const binding = {
      owner_id: "owner",
      server_generation: 1,
      registry_version: 0,
      membership_id: "member",
      native_reference_id: "ref",
      source_instance_id: "source",
      native_thread_id: "thread",
      authority_namespace: "namespace",
      store_generation: 1,
      expires_at: "2026-10-04T12:00:00.123456Z",
    };
    const bound = {
      ...item,
      canonical_binding: binding,
      target: { host_id: "host", environment_ref: "environment", thread_id: "thread" },
    };
    expect(decode({ ...fixture, items: [bound] }).items[0]?.canonical_binding).toEqual(binding);
    for (const change of [
      { expires_at: "1970-01-01T00:00:00Z" },
      { expires_at: "2026-02-30T12:00:00Z" },
      { native_thread_id: "other" },
      { store_generation: 0 },
      { private_evidence: "secret" },
    ]) {
      expect(() =>
        decode({ ...fixture, items: [{ ...bound, canonical_binding: { ...binding, ...change } }] }),
      ).toThrow();
    }
  });
});
