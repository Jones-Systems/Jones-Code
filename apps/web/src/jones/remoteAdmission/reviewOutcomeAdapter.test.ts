import { expect, it, vi } from "vite-plus/test";
import type {
  RemoteAdmissionLaunchResult,
  RemoteAdmissionReviewItem,
} from "@t3tools/contracts/jones/remoteAdmissionReview";
import {
  createCanonicalReviewClient,
  type ReviewTransport,
} from "@t3tools/client-runtime/jones/remoteAdmission/adapter";
import { admissionFixture } from "./testFixtures";

function transportFor(result: RemoteAdmissionLaunchResult) {
  const item: RemoteAdmissionReviewItem = {
    record: admissionFixture({ status: "dispatched" }),
    extension: {
      grantable: false,
      grantUnavailableReason: null,
      grant: null,
      evaluator: null,
      blocked: null,
      accessBasis: null,
      attemptId: null,
      result,
      recoveredResult: null,
      originNotifiedAt: null,
      retryEligible: false,
      recheckEligible: false,
    },
  };
  const unused = vi.fn(async () => {
    throw new Error("Unexpected transport operation");
  });
  const transport: ReviewTransport = {
    capabilities: unused,
    review: unused,
    item: vi.fn(async () => item),
    grants: unused,
    decision: unused,
    updateGrant: unused,
    revokeGrant: unused,
    restoreGrant: unused,
    retry: unused,
    recheck: unused,
    changes: unused,
  };
  return { transport, item, unused };
}
it.each(["not_created", "unknown", "refused", "created"] as const)(
  "retains canonical %s evidence despite a dispatched V1 record",
  async (outcome) => {
    const result: RemoteAdmissionLaunchResult = {
      outcome,
      stages: {
        creation: outcome === "created" ? "confirmed" : outcome,
        availability: "unknown",
        initialMessage: outcome === "not_created" || outcome === "refused" ? "none" : "unconfirmed",
        preparation: "ready",
        providerProgress: "not_started",
      },
      threadId: null,
      recordedAt: "2026-11-17T13:00:00Z",
      reasonCode: "synthetic_receiver_evidence",
    };
    const f = transportFor(result);
    const mapped = await createCanonicalReviewClient(f.transport).lookup(
      f.item.record.id,
      f.item.record.bindingDigest,
    );
    expect(mapped.record.status).toBe("dispatched");
    expect(mapped.launchResult).toEqual(result);
    expect(mapped.originalLaunchResult).toEqual(result);
    expect(mapped.outcome).toBe(outcome === "refused" ? "failed" : outcome);
    expect(mapped.reason).toBe(result.reasonCode);
    expect(mapped).not.toHaveProperty("locator");
    expect(f.unused).not.toHaveBeenCalled();
  },
);
it("preserves original evidence when a recovered result supplies the displayed stages", async () => {
  const original: RemoteAdmissionLaunchResult = {
    outcome: "unknown",
    stages: {
      creation: "unknown",
      availability: "unknown",
      initialMessage: "unconfirmed",
      preparation: "unknown",
      providerProgress: "unknown",
    },
    threadId: null,
    recordedAt: "2026-11-17T13:00:00Z",
    reasonCode: "interrupted",
  };
  const f = transportFor(original);
  const recovered: RemoteAdmissionLaunchResult = {
    ...original,
    outcome: "created",
    threadId: "confirmed-destination",
    stages: {
      ...original.stages,
      creation: "confirmed",
      availability: "available",
      initialMessage: "accepted",
    },
    reasonCode: "receipt_and_shell_confirmed",
  };
  const recoveredItem: RemoteAdmissionReviewItem = {
    ...f.item,
    extension: {
      ...f.item.extension,
      recoveredResult: {
        admissionId: f.item.record.id,
        revision: 1,
        result: recovered,
        evidence: {
          evidenceDigest: "c".repeat(64),
          observedAt: recovered.recordedAt,
          identityPath: "hashed",
          creationCommandId: "creation-command",
          initialMessageCommandId: "creation-command:initial-message",
          abandonedClaim: true,
          readsComplete: true,
          contradictions: [],
        },
      },
    },
  };
  f.transport.item = vi.fn(async () => recoveredItem);
  const mapped = await createCanonicalReviewClient(f.transport).lookup(f.item.record.id);
  expect(mapped.launchResult).toEqual(recovered);
  expect(mapped.originalLaunchResult).toEqual(original);
  expect(f.item.extension.result).toEqual(original);
  expect(f.unused).not.toHaveBeenCalled();
});
