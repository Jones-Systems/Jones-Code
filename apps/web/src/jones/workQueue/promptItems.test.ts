import { describe, expect, it } from "vite-plus/test";
import type { VoiceReviewRecentEntry, WorkQueueMetadata } from "@t3tools/contracts";
import { voiceReviewRecentFixture } from "@t3tools/client-runtime/voice-review/fixtures";
import {
  buildPromptItems,
  findPromptItem,
  hasConfirmedHandoff,
  queueItemKey,
  voiceItemKey,
} from "./promptItems";
import type { ConfirmedSentPrompt } from "./SentPrompts";

type Item = Extract<
  WorkQueueMetadata,
  { schema: "codex.t3-work-queue-metadata/v2" }
>["items"][number];

const proof = {
  command_id: "native-command",
  message_id: "message",
  command_status: "accepted",
  correlation_status: "exact",
  receipt_sequence: 0,
  turn_id: "turn",
  turn_state: "pending",
  observed_at_ms: 1500,
} as const;
const queueItem = (overrides: Partial<Item> = {}): Item => ({
  request_id: "request",
  workstream_id: "workstream",
  canonical_binding: null,
  entry_kind: "ordinary",
  request_kind: "initial",
  lane: "normal",
  queue_state: "waiting_remote",
  submitted_at_ms: 1000,
  target: { host_id: "host", environment_ref: "native-env", thread_id: "thread" },
  dispatch_status: "accepted",
  native_command_status: "accepted",
  finish_line: "not_tracked",
  payload_sha256: "b".repeat(64),
  native_handoff: null,
  ...overrides,
});
const snapshot = (items: Item[]): WorkQueueMetadata => ({
  schema: "codex.t3-work-queue-metadata/v2",
  source: {
    queue_id: "queue",
    host_id: "host",
    environment_ref: "native-env",
    exporter_instance_id: "exporter",
  },
  observed_at_ms: 2000,
  snapshot_token: "a".repeat(64),
  coverage: "complete",
  authority_effect: "none",
  items,
});
const delivery = (id: string, request: string, thread = "thread") => ({
  delivery_id: id,
  queue_request_id: request,
  payload_sha256: "b".repeat(64),
  destination: { backend: "t3", instance: "instance", thread_id: thread, project_id: null },
});
const voice = (
  deliveries: ReturnType<typeof delivery>[],
  overrides: Partial<VoiceReviewRecentEntry> = {},
): VoiceReviewRecentEntry => ({
  ...voiceReviewRecentFixture.entries[0]!,
  text: "Released prompt",
  queue_delivery_links: { status: "ready", reason: null, unlinked_delivery_count: 0, deliveries },
  ...overrides,
});
const pendingDraft = voiceReviewRecentFixture.entries[1]!.draft;

describe("Prompts item model", () => {
  it("places non-terminal drafts in Pending and released voice prompts in Queued", () => {
    const deleted = voiceReviewRecentFixture.entries[2]!.draft;
    const result = buildPromptItems({
      drafts: [pendingDraft, deleted],
      recent: [voiceReviewRecentFixture.entries[0]!],
      snapshot: null,
      nativeEnvironmentId: null,
    });
    expect(result.pending.map((item) => item.key)).toEqual([voiceItemKey(pendingDraft.id)]);
    expect(result.pending[0]).toMatchObject({
      stage: "pending",
      origin: "voice",
      text: { kind: "available", text: pendingDraft.text },
      timeKind: "captured",
    });
    expect(result.queued.map((item) => item.key)).toEqual(["voice:released-1"]);
    expect(result.queued[0]!.text).toEqual({
      kind: "available",
      text: voiceReviewRecentFixture.entries[0]!.text,
    });
    expect(result.sent).toEqual([]);
    expect(result.sourceBound).toBe(false);
  });

  it("moves only a proved delivery to Sent and keeps the voice text with it", () => {
    const result = buildPromptItems({
      drafts: [],
      recent: [voice([delivery("delivery", "request")])],
      snapshot: snapshot([queueItem({ native_handoff: proof })]),
      nativeEnvironmentId: "native-env",
    });
    expect(result.queued).toEqual([]);
    expect(result.sent).toHaveLength(1);
    const item = result.sent[0]!;
    expect(item).toMatchObject({
      key: "voice:released-1",
      origin: "voice",
      text: { kind: "available", text: "Released prompt" },
      timeKind: "handoff",
    });
    expect(item.destinations).toHaveLength(1);
    expect(item.destinations[0]).toMatchObject({
      deliveryId: "delivery",
      queueRequestId: "request",
      threadId: "thread",
      targetEnvironmentRef: "native-env",
    });
    expect(item.destinations[0]!.handoff?.providerState).toBe("pending");
    expect([...result.confirmedRequests]).toEqual(["request"]);
    expect(findPromptItem(result, "voice:released-1")).toBe(item);
    expect(findPromptItem(result, "voice:missing")).toBeNull();
  });

  it("keeps an unproved request in Queued, linked to its one matching delivery", () => {
    const result = buildPromptItems({
      drafts: [],
      recent: [voice([delivery("delivery", "request")])],
      snapshot: snapshot([queueItem()]),
      nativeEnvironmentId: "native-env",
    });
    expect(result.sent).toEqual([]);
    expect(result.queued).toHaveLength(1);
    expect(result.queued[0]!.destinations[0]!.queueItem?.request_id).toBe("request");
  });

  it("does not link a request with an ambiguous or mismatched delivery", () => {
    for (const deliveries of [
      [delivery("one", "request"), delivery("two", "request")],
      [delivery("other-thread", "request", "another-thread")],
    ]) {
      const result = buildPromptItems({
        drafts: [],
        recent: [voice(deliveries)],
        snapshot: snapshot([queueItem()]),
        nativeEnvironmentId: "native-env",
      });
      expect(result.queued.map((item) => item.key)).toEqual([
        "voice:released-1",
        queueItemKey("request"),
      ]);
      expect(
        result.queued[0]!.destinations.every((destination) => destination.queueItem === null),
      ).toBe(true);
    }
  });

  it("does not attach voice text when the queue source is bound to another environment", () => {
    const sample = snapshot([queueItem({ native_handoff: proof })]);
    const result = buildPromptItems({
      drafts: [],
      recent: [voice([delivery("delivery", "request")])],
      snapshot: { ...sample, source: { ...sample.source, environment_ref: "foreign-env" } },
      nativeEnvironmentId: "native-env",
    });
    expect(result.sourceBound).toBe(false);
    expect(result.sent).toHaveLength(1);
    expect(result.sent[0]).toMatchObject({
      key: queueItemKey("request"),
      origin: "queue",
      text: { kind: "not_carried" },
    });
    expect(result.queued.map((item) => item.key)).toEqual(["voice:released-1"]);
  });

  it("reports text states without substituting content", () => {
    const deleted = voiceReviewRecentFixture.entries[2]!;
    const result = buildPromptItems({
      drafts: [],
      recent: [
        { ...deleted, draft: { ...deleted.draft, state: "released", command_id: "c" } },
        {
          ...deleted,
          draft: { ...deleted.draft, id: "e", state: "released", command_id: "c2" },
          text_state: "expired",
        },
      ],
      snapshot: null,
      nativeEnvironmentId: null,
    });
    expect(result.queued.map((item) => item.text)).toEqual([
      { kind: "deleted" },
      { kind: "expired" },
    ]);
  });

  it("keeps unlinked delivery counts only when the voice source reports them", () => {
    const result = buildPromptItems({
      drafts: [],
      recent: [
        voice([], {
          queue_delivery_links: {
            status: "ready",
            reason: null,
            unlinked_delivery_count: 2,
            deliveries: [],
          },
        }),
      ],
      snapshot: null,
      nativeEnvironmentId: null,
    });
    expect(result.queued[0]!.unlinkedDeliveries).toBe(2);
  });
});

describe("handoff confirmation", () => {
  const confirmed: ConfirmedSentPrompt = {
    id: "request",
    text: null,
    target: { threadId: "thread", label: null },
    sentAt: 1500,
    confirmation: { status: "confirmed", kind: "handoff-receipt", receiptId: "r", source: "s" },
    sourceLabel: null,
    workstreams: [],
  };
  it("accepts a complete handoff receipt", () => {
    expect(hasConfirmedHandoff(confirmed)).toBe(true);
  });
  it.each([
    { status: "released", kind: "handoff-receipt", receiptId: "r", source: "s" },
    { status: "confirmed", kind: "command-accepted", receiptId: "r", source: "s" },
    { status: "confirmed", kind: "handoff-receipt", receiptId: " ", source: "s" },
    { status: "confirmed", kind: "handoff-receipt", receiptId: "r", source: "" },
    null,
  ])("rejects incomplete confirmation %j", (confirmation) => {
    expect(
      hasConfirmedHandoff({ ...confirmed, confirmation } as unknown as ConfirmedSentPrompt),
    ).toBe(false);
  });
  it("rejects a handoff without a valid time", () => {
    expect(hasConfirmedHandoff({ ...confirmed, sentAt: Number.NaN })).toBe(false);
  });
});
