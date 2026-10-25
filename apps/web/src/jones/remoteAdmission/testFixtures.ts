import type { RemoteAdmissionRecord, RemoteAdmissionSnapshot } from "@t3tools/contracts";

export function admissionFixture(
  overrides: Partial<RemoteAdmissionRecord> = {},
): RemoteAdmissionRecord {
  return {
    id: "synthetic-request",
    bindingDigest: "a".repeat(64),
    requestDigest: "b".repeat(64),
    idempotencyKey: "synthetic-key",
    status: "held",
    commandId: null,
    createdAt: "2026-10-24T12:00:00Z",
    updatedAt: "2026-10-24T12:00:00Z",
    category: "risky",
    reason: "Review the requested workspace change.",
    resolvedAt: null,
    resolvedBy: null,
    envelope: {
      identity: { clientId: "synthetic-vps", linkId: "synthetic-link" },
      operation: "t3_thread_launch",
      payload: {
        params: {
          title: "Inspect synthetic project",
          message: "Read the synthetic workspace and explain the change.",
        },
        invocation: { client: { label: "Synthetic VPS" } },
        target: { projects: [{ workspaceRoot: "/synthetic/project" }] },
      },
      target: {
        projectId: "synthetic-project",
        threadId: null,
        runtimeMode: "full-access",
        interactionMode: "default",
      },
      receiverPolicy: { enabled: true, revision: "synthetic-policy", modelSelection: null },
    },
    ...overrides,
  };
}

export function admissionSnapshot(pending = [admissionFixture()]): RemoteAdmissionSnapshot {
  return {
    eligible: true,
    policy: { enabled: true, revision: "synthetic-policy", modelSelection: null },
    pending,
  };
}
