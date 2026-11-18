import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  VoiceReviewDraft,
  VoiceReviewQueueDeliveryLinks,
  VoiceReviewRevisionPayload,
  VoiceReviewEditSavePayload,
  VoiceReviewEditCancelPayload,
  VoiceReviewSendOptions,
  VoiceReviewSendReviewedPayload,
} from "./voiceReview.ts";

const decodeRevision = Schema.decodeUnknownSync(VoiceReviewRevisionPayload);
const decodeEditSave = Schema.decodeUnknownSync(VoiceReviewEditSavePayload);
const decodeEditCancel = Schema.decodeUnknownSync(VoiceReviewEditCancelPayload);

it("decodes broker dependency objects and still emits draft IDs for existing clients", () => {
  const shape = Schema.Struct({ dependencies: VoiceReviewDraft.fields.dependencies });
  const decode = Schema.decodeUnknownSync(shape);
  const encode = Schema.encodeSync(shape);
  const dependency = { draft_id: "prior-capture", revision: 2, kind: "follows" };
  const result = decode({ dependencies: ["legacy-id", dependency] });
  expect(result).toEqual({ dependencies: ["legacy-id", "prior-capture"] });
  expect(encode(result)).toEqual(result);
  expect(decode({})).toEqual({});
  expect(() => decode({ dependencies: [{ ...dependency, revision: 0 }] })).toThrow();
});

describe("voice review mutation wire contract", () => {
  it("rejects coercion, nonpositive revisions, and extra fields", () => {
    const decode = decodeRevision;
    expect(decode({ expected_revision: 1 })).toEqual({ expected_revision: 1 });
    for (const value of [true, false, "1", 0, -1, 1.5, null]) {
      expect(() => decode({ expected_revision: value })).toThrow();
    }
    expect(() => decode({ expected_revision: 1, source_id: "other" })).toThrow();
    expect(() => decode({ expected_revision: 1, edit_handle: "secret" })).toThrow();
  });
  it("requires an edit handle and preserves literal text with the 100000 character ceiling", () => {
    const decode = decodeEditSave;
    const valid = {
      expected_revision: 2,
      edit_handle: "opaque",
      text: " <script>literal</script> ",
    };
    expect(decode(valid)).toEqual(valid);
    expect(() => decode({ ...valid, text: " \n " })).toThrow();
    expect(() => decode({ ...valid, text: "a".repeat(100_001) })).toThrow();
    expect(() => decode({ ...valid, edit_handle: "" })).toThrow();
    expect(() => decodeEditCancel({ expected_revision: 1 })).toThrow();
  });
});

describe("versioned reviewed send wires", () => {
  it("requires a closed v1 options response with explicit availability", () => {
    const decode = Schema.decodeUnknownSync(VoiceReviewSendOptions);
    const empty = {
      schema: "voice.reviewed-send/v1",
      workstream_ids: [],
      can_send_without_workstream: false,
    };
    expect(decode(empty)).toEqual(empty);
    expect(decode({ ...empty, workstream_ids: ["workstream:fixture"] }).workstream_ids).toEqual([
      "workstream:fixture",
    ]);
    for (const value of [
      null,
      { ...empty, schema: "voice.reviewed-send/v2" },
      { ...empty, workstream_ids: [null] },
      { ...empty, workstream_ids: [" "] },
      { ...empty, can_send_without_workstream: "false" },
      { schema: empty.schema, workstream_ids: [] },
      { ...empty, expected_revision: 1 },
    ]) {
      expect(() => decode(value)).toThrow();
    }
  });

  it("requires a nullable choice and keeps omitted fields distinct from null", () => {
    const decode = Schema.decodeUnknownSync(VoiceReviewSendReviewedPayload);
    const unassigned = { expected_revision: 1, workstream_id: null };
    const named = {
      ...unassigned,
      workstream_id: "workstream:fixture",
      text: " <script>literal</script> ",
      additional_instructions: "Keep formatting.",
      edit_handle: "opaque",
    };
    expect(decode(unassigned)).toEqual(unassigned);
    expect(decode({ ...unassigned, text: " \n " })).toEqual({ ...unassigned, text: " \n " });
    expect(decode(named)).toEqual(named);
    for (const value of [
      { expected_revision: 1 },
      { ...unassigned, expected_revision: "1" },
      { ...unassigned, workstream_id: undefined },
      { ...unassigned, workstream_id: "" },
      { ...unassigned, workstream_id: false },
      { ...unassigned, text: null },
      { ...unassigned, text: undefined },
      { ...unassigned, text: "a".repeat(100_001) },
      { ...unassigned, additional_instructions: null },
      { ...unassigned, additional_instructions: " \n " },
      { ...unassigned, edit_handle: null },
      { ...unassigned, edit_handle: "" },
      { ...unassigned, source_id: "another-source" },
    ]) {
      expect(() => decode(value)).toThrow();
    }
    expect(() => decodeRevision(unassigned)).toThrow();
  });
});

describe("voice queue delivery links", () => {
  const delivery = {
    delivery_id: "delivery",
    queue_request_id: "request",
    payload_sha256: "a".repeat(64),
    destination: { backend: "codex", instance: "worker", thread_id: "thread", project_id: null },
  };
  const decode = Schema.decodeUnknownSync(VoiceReviewQueueDeliveryLinks);
  it("accepts bounded ready receipts and explicit unavailable outcomes", () => {
    expect(decode({ status: "ready", reason: null, deliveries: [delivery] }).deliveries).toEqual([
      delivery,
    ]);
    for (const unlinked_delivery_count of [0, 1, 199]) {
      expect(
        decode({ status: "ready", reason: null, deliveries: [delivery], unlinked_delivery_count })
          .unlinked_delivery_count,
      ).toBe(unlinked_delivery_count);
    }
    for (const reason of ["forbidden", "source_unavailable", "invalid_receipt", "limit"]) {
      expect(decode({ status: "unavailable", reason, deliveries: [] }).reason).toBe(reason);
    }
  });
  it("rejects contradictory, malformed, oversized and private receipt projections", () => {
    for (const value of [
      { status: "ready", reason: "forbidden", deliveries: [] },
      { status: "unavailable", reason: null, deliveries: [] },
      { status: "unavailable", reason: "forbidden", deliveries: [delivery] },
      { status: "ready", reason: null, deliveries: Array(201).fill(delivery) },
      { status: "ready", reason: null, deliveries: [{ ...delivery, prompt: "private" }] },
      { status: "ready", reason: null, deliveries: [{ ...delivery, payload_sha256: "invalid" }] },
      {
        status: "ready",
        reason: null,
        deliveries: [{ ...delivery, destination: { ...delivery.destination, secret: "private" } }],
      },
      { status: "ready", reason: null, deliveries: [], detail: "private" },
      { status: "ready", reason: null, deliveries: [delivery], unlinked_delivery_count: 200 },
      { status: "unavailable", reason: "forbidden", deliveries: [], unlinked_delivery_count: 1 },
    ])
      expect(() => decode(value)).toThrow();
  });
  it("rejects invalid unlinked counts without coercion", () => {
    for (const unlinked_delivery_count of [-1, 0.5, 201, "1", true, null]) {
      expect(() =>
        decode({ status: "ready", reason: null, deliveries: [], unlinked_delivery_count }),
      ).toThrow();
    }
    expect(
      decode({ status: "ready", reason: null, deliveries: [], unlinked_delivery_count: 200 })
        .unlinked_delivery_count,
    ).toBe(200);
  });
});
