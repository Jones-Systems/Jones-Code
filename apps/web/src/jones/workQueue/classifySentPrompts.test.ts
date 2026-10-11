import { describe, expect, it } from "vite-plus/test";
import type { VoiceReviewRecentEntry, WorkQueueMetadata } from "@t3tools/contracts";
import { voiceReviewRecentFixture } from "@t3tools/client-runtime/voice-review/fixtures";
import { classifySentPrompts } from "./classifySentPrompts";

const snapshot = (): WorkQueueMetadata => ({
  schema: "codex.t3-work-queue-metadata/v2",
  source: { queue_id: "queue", host_id: "host", environment_ref: "native-env", exporter_instance_id: "exporter" },
  observed_at_ms: 2000, snapshot_token: "a".repeat(64), coverage: "complete", authority_effect: "none",
  items: [{
    request_id: "request", workstream_id: "workstream", canonical_binding: null,
    entry_kind: "ordinary", request_kind: "initial", lane: "normal", queue_state: "waiting_remote",
    submitted_at_ms: 1000, target: { host_id: "host", environment_ref: "native-env", thread_id: "thread" },
    dispatch_status: "accepted", native_command_status: "accepted", finish_line: "not_tracked",
    payload_sha256: "b".repeat(64), native_handoff: {
      command_id: "native-command", message_id: "message", command_status: "accepted",
      correlation_status: "exact", receipt_sequence: 0, turn_id: "turn", turn_state: "pending", observed_at_ms: 1500,
    },
  }],
});
const voice = (): VoiceReviewRecentEntry => ({
  ...voiceReviewRecentFixture.entries[0]!, text: "Authorized retained prompt",
  queue_delivery_links: { status: "ready", reason: null, deliveries: [{
    delivery_id: "delivery", queue_request_id: "request", payload_sha256: "b".repeat(64),
    destination: { backend: "t3", instance: "an-instance-not-the-environment", thread_id: "thread", project_id: null },
  }] },
});

describe("native handoff classification", () => {
  it("renders one sent prompt from exact proof and linkage while provider is pending", () => {
    const result = classifySentPrompts(snapshot(), [voice()], "native-env");
    expect(result.sent).toHaveLength(1);
    expect(result.sent[0]).toMatchObject({ text: "Authorized retained prompt", providerState: "pending" });
    expect(result.sent[0]!.sourceLabel).toContain("an-instance-not-the-environment");
    expect(result.remainingVoice).toEqual([]);
    expect([...result.confirmedRequests]).toEqual(["request"]);
  });
  it("keeps accepted commands without an exact receipt and v1 rows queued", () => {
    const sample = snapshot();
    if (sample.schema !== "codex.t3-work-queue-metadata/v2") throw new Error("fixture");
    const unconfirmed = { ...sample, items: sample.items.map((item) => ({ ...item, native_handoff: null })) };
    expect(classifySentPrompts(unconfirmed, [voice()], "native-env").sent).toEqual([]);
    const legacy: WorkQueueMetadata = { ...sample, schema: "codex.t3-work-queue-metadata/v1", items: sample.items.map(({ payload_sha256: _digest, native_handoff: _proof, ...item }) => item) };
    expect(classifySentPrompts(legacy, [voice()], "native-env").confirmedRequests.size).toBe(0);
  });
  it("keeps absent, unavailable, mismatched and ambiguous voice links queued", () => {
    const entry = voice();
    const links = entry.queue_delivery_links!;
    const delivery = links.deliveries[0]!;
    const { queue_delivery_links: _links, ...oldServerEntry } = entry;
    const cases: VoiceReviewRecentEntry[] = [
      oldServerEntry,
      { ...entry, queue_delivery_links: null },
      { ...entry, queue_delivery_links: { status: "unavailable", reason: "source_unavailable", deliveries: [] } },
      { ...entry, queue_delivery_links: { ...links, deliveries: [{ ...delivery, payload_sha256: "c".repeat(64) }] } },
      { ...entry, queue_delivery_links: { ...links, deliveries: [{ ...delivery, destination: { ...delivery.destination, thread_id: "other" } }] } },
      { ...entry, queue_delivery_links: { ...links, deliveries: [delivery, { ...delivery, delivery_id: "ambiguous" }] } },
    ];
    for (const candidate of cases) {
      const result = classifySentPrompts(snapshot(), [candidate], "native-env");
      expect(result.sent[0]!.text).toBeNull();
      expect(result.remainingVoice).toHaveLength(1);
    }
  });
  it("respects deleted or expired voice text even when linkage matches", () => {
    for (const text_state of ["deleted", "expired", "unavailable"] as const) {
      const result = classifySentPrompts(snapshot(), [{ ...voice(), text_state }], "native-env");
      expect(result.sent[0]!.text).toBeNull();
    }
  });
  it("does not use an instance as a native environment binding", () => {
    for (const nativeId of [null, "an-instance-not-the-environment", "other-env"]) {
      const result = classifySentPrompts(snapshot(), [voice()], nativeId);
      expect(result.sent[0]!.text).toBeNull();
      expect(result.remainingVoice).toHaveLength(1);
    }
  });
  it("suppresses only the matched delivery and retains an unconfirmed sibling", () => {
    const entry = voice();
    const links = entry.queue_delivery_links!;
    const sibling = { ...links.deliveries[0]!, delivery_id: "sibling", queue_request_id: "unconfirmed" };
    const result = classifySentPrompts(snapshot(), [{ ...entry, queue_delivery_links: { ...links, deliveries: [...links.deliveries, sibling] } }], "native-env");
    expect(result.sent).toHaveLength(1);
    expect(result.remainingVoice[0]!.queue_delivery_links?.deliveries).toEqual([sibling]);
  });
});
