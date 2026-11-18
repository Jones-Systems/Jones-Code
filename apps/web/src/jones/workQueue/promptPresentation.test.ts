import { describe, expect, it } from "vite-plus/test";
import type { VoiceReviewDraft } from "@t3tools/contracts";
import { voiceReviewRecentFixture } from "@t3tools/client-runtime/voice-review/fixtures";
import type { PromptDestination, PromptItem, WorkQueueItem } from "./promptItems";
import {
  assignmentReasonLabel,
  assignmentStateLabel,
  assignmentSummary,
  blockedReasonLabel,
  destinationAssignment,
  destinationNeedsAttention,
  draftStateLabel,
  draftStatus,
  itemStatus,
  NEEDS_DESTINATION,
  promptProgressSteps,
  queueStateLabel,
  queueUnavailableMessage,
  requestKindLabel,
  routingStateLabel,
  textStateLabel,
  turnNeedsAttention,
  turnStateLabel,
} from "./promptPresentation";

const draft = (overrides: Partial<VoiceReviewDraft> = {}): VoiceReviewDraft => ({
  ...voiceReviewRecentFixture.entries[1]!.draft,
  ...overrides,
});
const local = {
  seconds: null,
  busy: false,
  uncertain: false,
  conflict: false,
  ownsEdit: false,
  unavailable: false,
};
const queueItem = (overrides: Record<string, unknown> = {}) =>
  ({
    request_id: "request",
    workstream_id: "workstream",
    canonical_binding: null,
    entry_kind: "ordinary",
    request_kind: "initial",
    lane: "normal",
    queue_state: "queued",
    submitted_at_ms: 1000,
    target: { host_id: "host", environment_ref: "env", thread_id: "thread" },
    dispatch_status: "accepted",
    native_command_status: null,
    finish_line: "not_tracked",
    ...overrides,
  }) as WorkQueueItem;
const destination = (overrides: Partial<PromptDestination> = {}): PromptDestination => ({
  key: "request",
  deliveryId: null,
  queueRequestId: "request",
  threadId: "thread",
  targetEnvironmentRef: "env",
  targetHostId: "host",
  backend: null,
  instance: null,
  queueItem: queueItem(),
  handoff: null,
  ...overrides,
});
const item = (overrides: Partial<PromptItem> = {}): PromptItem => ({
  key: "queue:request",
  stage: "queued",
  origin: "queue",
  draft: null,
  entry: null,
  text: { kind: "not_carried" },
  destinations: [destination()],
  unlinkedDeliveries: null,
  time: 1000,
  timeKind: "submitted",
  ...overrides,
});
const handoff = (providerState?: "pending" | "running" | "error") => ({
  id: "request",
  text: null,
  target: { threadId: "thread", label: null },
  sentAt: 1500,
  confirmation: {
    status: "confirmed" as const,
    kind: "handoff-receipt" as const,
    receiptId: "receipt",
    source: "source",
  },
  sourceLabel: null,
  workstreams: [],
  ...(providerState ? { providerState } : {}),
});

describe("pending draft status", () => {
  it("applies local state before the server state", () => {
    const held = draft({ state: "held" });
    expect(draftStatus(held, { ...local, unavailable: true, busy: true }).label).toBe("Last known");
    expect(draftStatus(held, { ...local, busy: true, uncertain: true }).label).toBe("Working…");
    expect(draftStatus(held, { ...local, uncertain: true, conflict: true }).label).toBe(
      "Outcome unknown",
    );
    expect(draftStatus(held, { ...local, conflict: true }).label).toBe("Changed elsewhere");
  });
  it("labels every draft state with the release countdown where known", () => {
    expect(draftStatus(draft({ state: "held" }), { ...local, seconds: 12 }).label).toBe(
      "Releasing in 12s",
    );
    expect(draftStatus(draft({ state: "held" }), local).label).toBe("Held");
    expect(draftStatus(draft({ state: "paused" }), { ...local, seconds: 4 }).label).toBe(
      "Paused · 4s left",
    );
    expect(draftStatus(draft({ state: "paused" }), local).label).toBe("Paused");
    expect(draftStatus(draft({ state: "editing" }), { ...local, ownsEdit: true }).label).toBe(
      "Editing",
    );
    expect(draftStatus(draft({ state: "editing" }), local)).toMatchObject({
      label: "Being edited elsewhere",
      tone: "warning",
    });
    expect(draftStatus(draft({ state: "released" }), local).label).toBe("Released");
    expect(draftStatus(draft({ state: "deleted" }), local).label).toBe("Deleted");
    expect(draftStatus(draft({ state: "expired" }), local).label).toBe("Expired");
    expect(draftStatus(draft({ state: "future" as VoiceReviewDraft["state"] }), local).label).toBe(
      "Unrecognized state (future)",
    );
  });
  it("explains a missing destination without offering to set one", () => {
    const status = draftStatus(draft({ state: "blocked", reason: "route_required" }), local);
    expect(status.label).toBe("Blocked · Needs a destination");
    expect(status.explanation).toBe(NEEDS_DESTINATION);
    expect(draftStatus(draft({ state: "blocked", reason: "capacity" }), local).label).toBe(
      "Blocked · Destination at capacity",
    );
  });
  it("keeps unknown and missing values visible", () => {
    expect(blockedReasonLabel(null)).toBe("Reason not reported");
    expect(blockedReasonLabel("new_reason")).toBe("Unrecognized reason (new_reason)");
    expect(draftStateLabel(undefined)).toBe("State unknown");
    expect(draftStateLabel("paused")).toBe("Paused");
  });
});

describe("routing, queue and turn labels", () => {
  it("names each routing state", () => {
    expect(routingStateLabel(null, null).label).toBe("Destination not observed yet");
    expect(routingStateLabel("unclassified", null).label).toBe("Not routed yet");
    expect(routingStateLabel("classifying", null).label).toBe("Choosing a destination…");
    expect(routingStateLabel("proposed", "thread-a")).toEqual({
      label: "Proposed: thread-a",
      detail: "Will go here on release.",
    });
    expect(routingStateLabel("frozen", null).label).toBe(
      "Destination locked: destination not reported",
    );
    expect(routingStateLabel("awaiting_dependency", null, ["a", "b"]).label).toBe(
      "Waiting on 2 earlier prompts",
    );
    expect(routingStateLabel("awaiting_dependency", null, ["a"]).label).toBe(
      "Waiting on 1 earlier prompt",
    );
    expect(routingStateLabel("owner_required", null).label).toBe("Needs your decision");
    expect(routingStateLabel("later", null).label).toBe("Unrecognized routing state (later)");
  });
  it("labels queue states without claiming a handoff", () => {
    expect(queueStateLabel(null).label).toBe("Queue intake not observed");
    expect(queueStateLabel("observed_terminal").label).toBe(
      "Ended in queue · handoff not confirmed",
    );
    expect(queueStateLabel("held").tone).toBe("warning");
    expect(queueStateLabel("new").label).toBe("Unrecognized queue state (new)");
  });
  it("labels turns and request kinds", () => {
    expect(turnStateLabel("pending")).toBe("Turn not started");
    expect(turnStateLabel(undefined)).toBe("Not observed");
    expect(turnNeedsAttention("error")).toBe(true);
    expect(turnNeedsAttention("running")).toBe(false);
    expect(requestKindLabel("owner_followup")).toBe("Follow-up");
    expect(requestKindLabel(null)).toBe("Queue request");
  });
  it("explains why queue metadata is unavailable", () => {
    expect(queueUnavailableMessage("source_unavailable")).toBe("the source cannot be read");
    expect(queueUnavailableMessage("other")).toBe("the source reported other");
  });
  it("labels text states", () => {
    expect(textStateLabel({ kind: "available", text: "literal" })).toBe("literal");
    expect(textStateLabel({ kind: "deleted" })).toBe("Text deleted");
    expect(textStateLabel({ kind: "expired" })).toBe("Text expired");
    expect(textStateLabel({ kind: "not_carried" })).toBe("Text not available for this request");
    expect(textStateLabel({ kind: "unavailable" })).toBe("Prompt text is unavailable.");
  });
});

describe("assignments", () => {
  const name = (id: string) => `Name of ${id}`;
  const base = {
    workstream_id: "desired",
    observed_workstream_id: null,
    reason: "assignment_pending",
  } as const;
  it("summarizes each assignment outcome", () => {
    expect(assignmentSummary({ ...base, state: "satisfied" }, name)).toBe("In Name of desired");
    expect(assignmentSummary({ ...base, state: "issued" }, name)).toBe(
      "Assigning to Name of desired…",
    );
    expect(
      assignmentSummary(
        {
          ...base,
          state: "conflict",
          observed_workstream_id: "other",
          reason: "assignment_conflict",
        },
        name,
      ),
    ).toBe("Thread is in Name of other; the queue asked for Name of desired");
    expect(
      assignmentSummary(
        { ...base, state: "rejected", reason: "assignment_rejected:thread_not_found" },
        name,
      ),
    ).toBe("Could not assign: Thread not found");
    expect(
      assignmentSummary({ ...base, state: "cancelled", reason: "assignment_cancelled" }, name),
    ).toBe("Cancelled: Assignment cancelled");
  });
  it("keeps unrecognized assignment values visible", () => {
    expect(assignmentStateLabel("permanent")).toBe("Permanent hold");
    expect(assignmentStateLabel("new")).toBe("Unrecognized assignment state (new)");
    expect(assignmentReasonLabel("new")).toBe("Unrecognized assignment reason (new)");
  });
  it("reads an assignment only from a queue item that carries one", () => {
    expect(destinationAssignment(null)).toBeUndefined();
    expect(destinationAssignment(destination())).toBeUndefined();
    const assignment = { ...base, state: "conflict" };
    const withAssignment = destination({ queueItem: queueItem({ assignment }) });
    expect(destinationAssignment(withAssignment)).toEqual(assignment);
    expect(destinationNeedsAttention(withAssignment)).toBe(true);
    expect(destinationNeedsAttention(destination())).toBe(false);
    expect(
      destinationNeedsAttention(destination({ queueItem: queueItem({ queue_state: "held" }) })),
    ).toBe(true);
  });
});

describe("item status and progress", () => {
  it("never calls a queued item sent and qualifies Sent by the turn", () => {
    expect(itemStatus(item()).label).toBe("Queued");
    expect(itemStatus(item({ destinations: [] })).label).toBe(
      "Released · queue intake not observed",
    );
    const sent = item({
      stage: "sent",
      destinations: [destination({ handoff: handoff("pending") })],
    });
    expect(itemStatus(sent)).toMatchObject({ label: "Handed off", tone: "success" });
    expect(
      itemStatus(
        item({ stage: "sent", destinations: [destination({ handoff: handoff("error") })] }),
      ).tone,
    ).toBe("warning");
  });
  it("reports terminal queue evidence without contradicting a correlated handoff", () => {
    const terminal = queueItem({ queue_state: "observed_terminal" });
    const sent = promptProgressSteps(
      item({ stage: "sent" }),
      destination({ queueItem: terminal, handoff: handoff("pending") }),
    );
    expect(sent[3]!.detail).toBe("Queue entry ended · normal lane");
    expect(sent[4]!.detail).toBe("Handoff confirmed");
    const queued = promptProgressSteps(item(), destination({ queueItem: terminal }));
    expect(queued[3]!.detail).toBe("Ended in queue · handoff not confirmed · normal lane");
    expect(queued[4]!.status).toBe("not_observed");
  });
  it("reports each step from its own evidence and never tracks completion", () => {
    const queued = promptProgressSteps(item(), destination());
    expect(queued.map((step) => [step.key, step.status])).toEqual([
      ["captured", "not_applicable"],
      ["held", "not_applicable"],
      ["released", "not_applicable"],
      ["queued", "observed"],
      ["handoff", "not_observed"],
      ["turn", "not_observed"],
      ["finished", "not_applicable"],
    ]);
    expect(queued.at(-1)!.detail).toBe("Not tracked");
    const pending = promptProgressSteps(
      item({
        stage: "pending",
        origin: "voice",
        draft: draft({ state: "held" }),
        destinations: [],
      }),
      null,
      { label: "Releasing in 3s", tone: "info", explanation: null },
    );
    expect(pending[1]).toMatchObject({ status: "current", detail: "Releasing in 3s" });
    expect(pending[2]!.status).toBe("not_observed");
    const sent = promptProgressSteps(
      item({
        stage: "sent",
        origin: "voice",
        draft: draft({ state: "released", command_id: "c" }),
      }),
      destination({ handoff: handoff("pending") }),
    );
    expect(sent.map((step) => step.status)).toEqual([
      "observed",
      "observed",
      "observed",
      "observed",
      "observed",
      "observed",
      "not_applicable",
    ]);
    expect(sent[1]!.detail).toBe("Review hold ended");
    expect(sent[5]!.detail).toBe("Turn not started");
  });
});
