import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import producerRaw from "./fixtures/work_queue_metadata_v1.json.fixture?raw";
import {
  WorkQueueMetadata,
  WorkQueueMetadataV1,
  WorkQueueMetadataV2,
  WorkQueueMetadataResult,
} from "./workQueueMetadata.ts";

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
const decodeResult = Schema.decodeUnknownSync(WorkQueueMetadataResult);

describe("work queue metadata contract", () => {
  it("decodes the exact Python producer golden artifact without changing its identity", () => {
    const bytes = new TextEncoder().encode(producerRaw);
    expect(bytes.byteLength).toBe(923);
    expect(bytes[bytes.length - 1]).toBe(125);
    const parsed = JSON.parse(producerRaw);
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
      decodeResult({
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

const handoff = {
  command_id: "native-command",
  message_id: "native-message",
  command_status: "accepted",
  correlation_status: "exact",
  receipt_sequence: 0,
  turn_id: "native-turn",
  turn_state: "pending",
  observed_at_ms: 1_000,
};
const v2Item = {
  ...item,
  target: { host_id: "host", environment_ref: "environment", thread_id: "thread" },
  payload_sha256: "b".repeat(64),
  native_handoff: handoff,
};
const v2 = { ...fixture, schema: "codex.t3-work-queue-metadata/v2", items: [v2Item] };

describe("v2 native handoff metadata", () => {
  it("round trips v1 unchanged and accepts nullable v2 evidence without manufacturing proof", () => {
    expect(Schema.encodeSync(WorkQueueMetadata)(decode(fixture))).toEqual(fixture);
    expect(Schema.decodeUnknownSync(WorkQueueMetadataV1)(fixture)).toEqual(fixture);
    expect(Schema.decodeUnknownSync(WorkQueueMetadataV2)(v2)).toEqual(v2);
    expect(decode(v2)).toEqual(v2);
    const unconfirmed = { ...v2, items: [{ ...item, payload_sha256: null, native_handoff: null }] };
    expect(decode(unconfirmed)).toEqual(unconfirmed);
    expect(() => decode({ ...fixture, items: [v2Item] })).toThrow();
  });
  it("rejects absent, partial, unknown, unsafe, or unbound proof", () => {
    const { turn_id: _turn, ...partial } = handoff;
    for (const change of [
      { native_handoff: partial },
      { native_handoff: { ...handoff, receipt_sequence: Number.MAX_SAFE_INTEGER + 1 } },
      { native_handoff: { ...handoff, receipt_sequence: -1 } },
      { native_handoff: { ...handoff, observed_at_ms: 0.5 } },
      { native_handoff: { ...handoff, command_status: "rejected" } },
      { native_handoff: { ...handoff, correlation_status: "inferred" } },
      { native_handoff: { ...handoff, turn_state: "success" } },
      { native_handoff: { ...handoff, text: "private" } },
      { native_handoff: { ...handoff, command_id: "x".repeat(513) } },
      { payload_sha256: "A".repeat(64) },
      { payload_sha256: null },
      { target: null },
    ])
      expect(() => decode({ ...v2, items: [{ ...v2Item, ...change }] })).toThrow();
    expect(() => decode({ ...v2, items: [item] })).toThrow();
    for (const turn_state of ["pending", "running", "completed", "interrupted", "error"]) {
      expect(
        decode({ ...v2, items: [{ ...v2Item, native_handoff: { ...handoff, turn_state } }] }),
      ).toMatchObject({ items: [{ native_handoff: { turn_state } }] });
    }
  });
});

const pendingHandoff = { ...handoff, correlation_status: "pending", turn_id: null };
const v3 = {
  ...v2,
  schema: "codex.t3-work-queue-metadata/v3",
  items: [{ ...v2Item, native_handoff: pendingHandoff }],
};

describe("v3 recorded handoff metadata", () => {
  it("preserves accepted pre-start evidence without inventing an exact turn", () => {
    expect(decode(v3)).toEqual(v3);
    expect(Schema.encodeSync(WorkQueueMetadata)(decode(v3))).toEqual(v3);
    expect(decode({ ...v3, items: [v2Item] })).toEqual({ ...v3, items: [v2Item] });
    const unconfirmed = { ...v3, items: [{ ...v2Item, native_handoff: null }] };
    expect(decode(unconfirmed)).toEqual(unconfirmed);
    expect(() => decode({ ...v3, schema: v2.schema })).toThrow();
  });
  it.each(["error", "interrupted"])(
    "preserves the same recorded receipt when a pre-start handoff becomes %s",
    (terminalState) => {
      for (const turn_state of ["pending", terminalState]) {
        const native_handoff = { ...pendingHandoff, turn_state };
        const sample = { ...v3, items: [{ ...v2Item, native_handoff }] };
        expect(decode(sample)).toEqual(sample);
        expect(Schema.encodeSync(WorkQueueMetadata)(decode(sample))).toEqual(sample);
        expect(() => decode({ ...sample, schema: v2.schema })).toThrow();
      }
    },
  );
  it("rejects contradictory, incomplete, unbound and private v3 proof", () => {
    const { turn_state: _state, ...missingState } = pendingHandoff;
    const { turn_id: _turnId, ...missingTurn } = pendingHandoff;
    for (const native_handoff of [
      { ...pendingHandoff, turn_id: "invented-turn" },
      { ...pendingHandoff, turn_state: "running" },
      { ...pendingHandoff, turn_state: "completed" },
      { ...pendingHandoff, turn_state: null },
      { ...pendingHandoff, correlation_status: "exact" },
      { ...pendingHandoff, correlation_status: "inferred" },
      { ...pendingHandoff, command_status: "rejected" },
      { ...pendingHandoff, receipt_sequence: 0.5 },
      { ...pendingHandoff, text: "private" },
      missingState,
      missingTurn,
    ]) {
      expect(() => decode({ ...v3, items: [{ ...v2Item, native_handoff }] })).toThrow();
    }
    for (const change of [{ target: null }, { payload_sha256: null }, { text: "private" }]) {
      expect(() => decode({ ...v3, items: [{ ...v3.items[0], ...change }] })).toThrow();
    }
    expect(() => decode({ ...v3, text: "private" })).toThrow();
  });
});
