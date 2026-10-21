import { describe, expect, it } from "vite-plus/test";
import type { RemoteAdmissionRecord } from "@t3tools/contracts";
import { admissionPresentation } from "./presentation";

describe("owner remote request presentation", () => {
  it("shows received task text, authenticated sender label, and resolved worktree", () => {
    const record = {
      envelope: {
        operation: "t3_thread_launch",
        target: { threadId: null, projectId: "project" },
        payload: {
          params: { title: "Fix layout", message: "Make the button readable" },
          invocation: { client: { label: "VPS peer" } },
          target: {
            threads: [{ worktreePath: "/synthetic/task" }],
            projects: [{ workspaceRoot: "/synthetic/root" }],
          },
        },
      },
    } as unknown as RemoteAdmissionRecord;
    expect(admissionPresentation(record)).toEqual({
      title: "Fix layout",
      message: "Make the button readable",
      source: "VPS peer",
      target: "/synthetic/task",
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
