import { describe, expect, it } from "vite-plus/test";
import type { VoiceReviewDraft } from "@t3tools/contracts";
import { voiceReviewRecentFixture } from "@t3tools/client-runtime/voice-review/fixtures";
import type { PromptItem, WorkQueueItem } from "./promptItems";
import { promptAttention } from "./promptAttention";

const draft = (overrides: Partial<VoiceReviewDraft>): VoiceReviewDraft => ({
  ...voiceReviewRecentFixture.entries[1]!.draft,
  ...overrides,
});
const pending = (overrides: Partial<VoiceReviewDraft>): PromptItem => ({
  key: "voice:draft",
  stage: "pending",
  origin: "voice",
  draft: draft(overrides),
  entry: null,
  text: { kind: "available", text: "text" },
  destinations: [],
  unlinkedDeliveries: null,
  time: null,
  timeKind: null,
});
const withQueueState = (stage: "queued" | "sent", queueState: string, providerState?: "error") =>
  ({
    ...pending({}),
    stage,
    destinations: [
      {
        key: "request",
        deliveryId: null,
        queueRequestId: "request",
        threadId: "thread",
        targetEnvironmentRef: null,
        targetHostId: null,
        backend: null,
        instance: null,
        queueItem: { queue_state: queueState, dispatch_status: "accepted" } as WorkQueueItem,
        handoff: providerState
          ? ({ providerState } as NonNullable<PromptItem["destinations"][number]["handoff"]>)
          : null,
      },
    ],
  }) satisfies PromptItem;

describe("Needs you", () => {
  it("flags pending prompts only the person can move", () => {
    expect(promptAttention(pending({ state: "blocked" }))).toEqual({
      needsYou: true,
      band: "needs-you",
    });
    expect(
      promptAttention(pending({ state: "held", routing_state: "owner_required" })).needsYou,
    ).toBe(true);
    expect(promptAttention(pending({ state: "editing" })).needsYou).toBe(true);
    expect(
      promptAttention(pending({ state: "editing" }), { uncertain: false, ownsEdit: true }),
    ).toEqual({ needsYou: false, band: "paused" });
    expect(
      promptAttention(pending({ state: "held" }), { uncertain: true, ownsEdit: false }),
    ).toEqual({ needsYou: true, band: "needs-you" });
  });
  it("bands the rest of Pending by what happens next", () => {
    expect(promptAttention(pending({ state: "held" })).band).toBe("releasing");
    expect(promptAttention(pending({ state: "paused" })).band).toBe("paused");
    expect(promptAttention(pending({ state: "released" })).band).toBe("other");
  });
  it("flags queued items whose queue state needs reconciliation", () => {
    expect(promptAttention(withQueueState("queued", "held")).needsYou).toBe(true);
    expect(promptAttention(withQueueState("queued", "waiting_remote"))).toEqual({
      needsYou: false,
      band: "waiting",
    });
    expect(promptAttention(withQueueState("queued", "observed_terminal")).band).toBe("other");
  });
  it("flags sent items only when the turn failed or was interrupted", () => {
    expect(promptAttention(withQueueState("sent", "observed_terminal", "error")).needsYou).toBe(
      true,
    );
    expect(promptAttention(withQueueState("sent", "observed_terminal")).needsYou).toBe(false);
  });
});
