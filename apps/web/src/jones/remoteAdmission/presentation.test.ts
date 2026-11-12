import { describe, expect, it } from "vite-plus/test";
import type { RemoteAdmissionRecord } from "@t3tools/contracts";
import {
  admissionCategory,
  admissionTarget,
  admissionPresentation,
  admissionStatus,
  groupAdmissions,
} from "./presentation";
import { admissionFixture, contextAdmissionFixture } from "./testFixtures";

describe("owner remote request presentation", () => {
  it("groups service failures separately from risk and scopes identical labels by authenticated source", () => {
    const risk = admissionFixture();
    const unavailable = admissionFixture({ id: "failed", category: "evaluation_unavailable" });
    const interrupted = admissionFixture({ id: "interrupted", category: "evaluation_interrupted" });
    const other = admissionFixture({
      id: "other",
      envelope: { ...risk.envelope, identity: { clientId: "other", linkId: null } },
    });
    expect(
      groupAdmissions([risk, unavailable, interrupted, other]).map((group) => group.records.length),
    ).toEqual([1, 2, 1]);
    expect(admissionCategory(unavailable)).toBe("Evaluator service failure");
    expect(admissionCategory(risk)).toBe("Request review");
    expect(admissionStatus(admissionFixture({ status: "dispatching" }))).toContain("unknown");
    expect(admissionStatus(admissionFixture({ status: "approved" }))).toBe(
      "Approved · awaiting dispatch",
    );
  });
  it("shows received task text, authenticated sender label, and envelope-scoped location", () => {
    const record = {
      envelope: {
        operation: "t3_thread_launch",
        target: { threadId: null, projectId: "project" },
        payload: {
          params: { title: "Fix layout", message: "Make the button readable" },
          invocation: { client: { label: "VPS peer" } },
          target: {
            threads: [{ worktreePath: "/synthetic/task" }],
            projects: [{ id: "project", workspaceRoot: "/synthetic/root" }],
          },
        },
      },
    } as unknown as RemoteAdmissionRecord;
    expect(admissionPresentation(record)).toMatchObject({
      title: "Fix layout",
      message: "Make the button readable",
      source: "VPS peer",
      target: { projectId: "project", threadId: null, location: "/synthetic/root" },
    });
  });
  it("uses stored schedule text when a remote request starts a schedule", () => {
    const record = {
      envelope: {
        operation: "run_scheduled_task_now",
        target: { threadId: null, projectId: null },
        payload: { params: {}, target: { scheduled: { prompt: "Check the synthetic project" } } },
      },
    } as unknown as RemoteAdmissionRecord;
    expect(admissionPresentation(record).message).toBe("Check the synthetic project");
  });
});

function contextRecord(context: Record<string, unknown>, client: Record<string, unknown> = {}) {
  const record = contextAdmissionFixture();
  return {
    ...record,
    envelope: {
      ...record.envelope,
      payload: {
        params: {},
        invocation: {
          client: { label: "Authenticated\u202e laptop", sessionId: "session", ...client },
        },
        advisory: {
          schemaVersion: "t3.remote-admission-advisory/v1",
          requestContext: { schemaVersion: "t3.linked-request-context/v1", ...context },
        },
      },
    },
  } as unknown as RemoteAdmissionRecord;
}

describe("sender context and receiver target boundaries", () => {
  it.each(["2026-02-30T00:00:00Z", "malformed", "2026-10-24T12:00:00+00:00", null])(
    "never labels invalid sender time %s as sent",
    (sentAt) => {
      expect(admissionPresentation(contextRecord({ sentAt })).sentAt).toBeNull();
    },
  );
  it("keeps independently valid parts, authenticates the source label, and preserves raw data", () => {
    const record = contextRecord({
      sentAt: "2026-10-24T11:59:00Z",
      label: "Spoofed label",
      thread: { threadId: "thread", title: "Readable thread" },
      parent: { threadId: "bad\u202eID" },
      project: { projectId: "project", title: "Readable project", icon: { kind: "future-icon" } },
    });
    const before = JSON.stringify(record);
    const view = admissionPresentation(record);
    expect(view.source).toBe("Authenticated laptop");
    expect(view.sentAt).toBe("2026-10-24T11:59:00Z");
    expect(view.thread?.label).toBe("Readable thread");
    expect(view.parent).toBeNull();
    expect(view.project).toMatchObject({ label: "Readable project", icon: null });
    expect(JSON.stringify(record)).toBe(before);
    expect(
      admissionPresentation(
        contextRecord({
          sentAt: "bad",
          thread: { threadId: "thread" },
          project: { projectId: "project", icon: { kind: "emoji", emoji: 42 } },
        }),
      ),
    ).toMatchObject({ sentAt: null, thread: { label: "Thread thread" }, project: { icon: null } });
  });
  it("retains unknown-icon context after storage roundtrip, including explicit null", () => {
    for (const icon of [{ kind: "future-icon" }, null]) {
      const record = JSON.parse(
        JSON.stringify(
          contextRecord({
            sentAt: "2026-10-24T11:59:00Z",
            project: { projectId: "p", title: "Project", icon },
          }),
        ),
      ) as RemoteAdmissionRecord;
      expect(admissionPresentation(record)).toMatchObject({
        sentAt: "2026-10-24T11:59:00Z",
        project: { label: "Project", icon: null },
      });
    }
  });
  it("uses legacy context only with a matching session, and matches thread IDs for title fallback", () => {
    const origin = {
      linkSessionId: "session",
      thread: { threadId: "legacy", title: "Legacy\u202e title\n👩‍💻" },
    };
    const legacy = contextRecord({}, { remoteOrigin: origin });
    expect(admissionPresentation(legacy)).toMatchObject({
      sentAt: null,
      thread: { label: "Legacy title 👩‍💻" },
    });
    expect(
      admissionPresentation(
        contextRecord({}, { remoteOrigin: { ...origin, linkSessionId: "other" } }),
      ).thread,
    ).toBeNull();
    expect(
      admissionPresentation(
        contextRecord({ thread: { threadId: "legacy" } }, { remoteOrigin: origin }),
      ).thread?.label,
    ).toBe("Legacy title 👩‍💻");
    expect(
      admissionPresentation(
        contextRecord({ thread: { threadId: "different" } }, { remoteOrigin: origin }),
      ).thread?.label,
    ).toBe("Thread different");
    const noAdvisory = {
      ...legacy,
      envelope: {
        ...legacy.envelope,
        payload: { invocation: { client: { sessionId: "session", remoteOrigin: origin } } },
      },
    };
    expect(admissionPresentation(noAdvisory).thread?.label).toBe("Legacy title 👩‍💻");
  });
  it("keeps a missing sender time distinct from an invalid one and does not use issuedAt", () => {
    expect(
      admissionPresentation(contextRecord({ issuedAt: "2026-10-24T11:59:00Z" })),
    ).toMatchObject({ sentAt: null, invalidSentAt: null, title: null });
    expect(admissionPresentation(contextRecord({ sentAt: "bad" }))).toMatchObject({
      sentAt: null,
      invalidSentAt: "bad",
    });
  });
  it.each([
    ["t3_thread_launch", "Start a task"],
    ["t3_thread_send", "Continue a task"],
    ["schedule_task", "Schedule a task"],
    ["update_scheduled_task", "Update a scheduled task"],
    ["run_scheduled_task_now", "Run a scheduled task now"],
    ["t3_thread_link", "Link threads"],
    ["other", "Remote request · other"],
    ["__proto__", "Remote request · __proto__"],
  ])("names operation %s", (operation, label) => {
    const record = admissionFixture();
    expect(
      admissionPresentation({ ...record, envelope: { ...record.envelope, operation } }).operation,
    ).toBe(label);
  });
  it("uses only envelope IDs and matching captured entries, and never invents deletion from cache absence", () => {
    const record = contextAdmissionFixture();
    expect(admissionTarget(record)).toEqual({
      threadId: "receiver-thread",
      projectId: "synthetic-project",
      threadState: null,
      projectState: null,
      location: "/synthetic/worktree",
    });
    const scheduled = {
      ...record,
      envelope: {
        ...record.envelope,
        target: { ...record.envelope.target, threadId: null, projectId: null },
        payload: {
          params: { threadId: "untrusted-param" },
          target: {
            scheduled: { threadId: "scheduled-thread", projectId: "scheduled-project" },
            threads: [{ id: "scheduled-thread", projectId: "captured-project", missing: true }],
            projects: [{ id: "captured-project", deletedAt: "2026-10-24T00:00:00Z" }],
          },
        },
      },
    };
    expect(admissionTarget(scheduled)).toMatchObject({
      threadId: "scheduled-thread",
      projectId: "captured-project",
      threadState: "not found when requested",
      projectState: "deleted when requested",
      location: null,
    });
    expect(
      admissionTarget(
        contextRecord({
          project: { projectId: "sender-project" },
          thread: { threadId: "sender-thread" },
        }),
      ),
    ).toMatchObject({
      threadId: "receiver-thread",
      projectId: "synthetic-project",
      threadState: null,
      projectState: null,
    });
  });
});
