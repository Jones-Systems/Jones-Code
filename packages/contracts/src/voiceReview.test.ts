import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  VoiceReviewQueueDeliveryLinks,
  VoiceReviewRevisionPayload,
  VoiceReviewEditSavePayload,
  VoiceReviewEditCancelPayload,
} from "./voiceReview.ts";

const decodeRevision = Schema.decodeUnknownSync(VoiceReviewRevisionPayload);
const decodeEditSave = Schema.decodeUnknownSync(VoiceReviewEditSavePayload);
const decodeEditCancel = Schema.decodeUnknownSync(VoiceReviewEditCancelPayload);

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

describe("voice queue delivery links", () => {
  const delivery = {
    delivery_id: "delivery", queue_request_id: "request", payload_sha256: "a".repeat(64),
    destination: { backend: "codex", instance: "worker", thread_id: "thread", project_id: null },
  };
  const decode = Schema.decodeUnknownSync(VoiceReviewQueueDeliveryLinks);
  it("accepts bounded ready receipts and explicit unavailable outcomes", () => {
    expect(decode({ status: "ready", reason: null, deliveries: [delivery] }).deliveries).toEqual([delivery]);
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
      { status: "ready", reason: null, deliveries: [{ ...delivery, destination: { ...delivery.destination, secret: "private" } }] },
      { status: "ready", reason: null, deliveries: [], detail: "private" },
    ]) expect(() => decode(value)).toThrow();
  });
});
