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

/** Context-rich synthetic row; the original fixture remains unchanged for Settings. */
export function contextAdmissionFixture(): RemoteAdmissionRecord {
  const record = admissionFixture();
  return {
    ...record,
    envelope: {
      ...record.envelope,
      target: { ...record.envelope.target, threadId: "receiver-thread" },
      payload: {
        params: {
          title: "Inspect synthetic project",
          message: "Read the **synthetic workspace** and explain the change.",
        },
        invocation: {
          client: {
            label: "Authenticated laptop",
            sessionId: "bound-session",
            remoteOrigin: {
              schemaVersion: "t3.linked-message-origin/v1",
              environmentId: "sender-environment",
              linkSessionId: "bound-session",
              thread: { threadId: "sender-thread", title: "Legacy sender thread" },
            },
          },
        },
        advisory: {
          schemaVersion: "t3.remote-admission-advisory/v1",
          requestContext: {
            schemaVersion: "t3.linked-request-context/v1",
            sentAt: "2026-10-24T11:59:00Z",
            project: {
              projectId: "sender-project",
              title: "Sender project",
              icon: { kind: "emoji", emoji: "🏡" },
            },
            parent: {
              threadId: "sender-parent",
              title: "Sender parent",
              environmentId: "parent-environment",
            },
            thread: { threadId: "sender-thread", title: "Sender thread" },
          },
        },
        target: {
          projects: [{ id: "synthetic-project", workspaceRoot: "/synthetic/project" }],
          threads: [
            {
              id: "receiver-thread",
              projectId: "synthetic-project",
              worktreePath: "/synthetic/worktree",
              deleted: false,
            },
          ],
        },
      },
    },
  };
}
