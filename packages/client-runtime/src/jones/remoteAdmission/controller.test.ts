import { expect, it, vi } from "vite-plus/test";
import { createReviewController, type Item, type Page, type ReviewClient } from "./controller.ts";

const time = "2026-11-19T16:00:00.000Z";
const hold: Item = {
  record: {
    id: "held-during-disconnect",
    idempotencyKey: "synthetic-request",
    bindingDigest: "a".repeat(64),
    requestDigest: "b".repeat(64),
    envelope: {
      identity: { clientId: "synthetic-origin", linkId: null },
      operation: "t3_thread_launch",
      payload: {},
      target: { projectId: null, threadId: null, runtimeMode: "auto", interactionMode: "default" },
      receiverPolicy: { enabled: true, revision: "receiver-policy", modelSelection: null },
    },
    status: "held",
    commandId: null,
    reason: null,
    category: null,
    createdAt: time,
    updatedAt: time,
    resolvedBy: null,
    resolvedAt: null,
  },
  attempt: "original",
  blocked: false,
  accessId: null,
  grantEligible: false,
  outcome: null,
  availability: "unknown",
  threadId: null,
  messageStage: null,
  reason: null,
  canRetry: false,
  canRecheck: false,
};

it("surfaces a current hold on reconnect while Done is selected and the next change is still pending", async () => {
  const page = (records: readonly Item[]): Page => ({
    revision: "current-revision",
    policyRevision: "receiver-policy",
    records,
    access: [],
    nextCursor: null,
    recordCount: records.length,
    counts: {
      currentHeldAdmissions: 1,
      blockedHeldAdmissions: 0,
      heldAdmissions: 1,
      inFlightAdmissions: 0,
      failedAdmissions: 0,
      unresolvedAdmissions: 0,
      activeAccessScopes: 0,
      activeThreads: 0,
    },
  });
  const client: ReviewClient = {
    capabilities: async () => ({ canDecide: true, features: new Set(["reviewV2", "changeFeed"]) }),
    page: vi.fn(async (view) => page(view === "current" ? [hold] : [])),
    lookup: vi.fn(),
    lookupAccess: vi.fn(),
    mutate: vi.fn(),
    head: vi.fn(async () => "receiver-head"),
    changes: vi.fn<ReviewClient["changes"]>(
      (_cursor, signal) =>
        new Promise<never>((_resolve, reject) =>
          signal.addEventListener("abort", () => reject(new Error("aborted")), { once: true }),
        ),
    ),
  };
  const controller = createReviewController(client);
  await controller.load("done");
  expect(controller.getSnapshot().incoming).toEqual([]);
  const feed = controller.watchResults();
  try {
    await vi.waitFor(() => expect(client.changes).toHaveBeenCalled());
    expect(controller.getSnapshot().view).toBe("done");
    expect(controller.getSnapshot().incoming).toEqual([hold]);
    expect(controller.getSnapshot().results).toEqual([]);
    expect(client.head).toHaveBeenCalledOnce();
    expect(client.mutate).not.toHaveBeenCalled();
  } finally {
    controller.stopResults();
    await feed;
  }
});
