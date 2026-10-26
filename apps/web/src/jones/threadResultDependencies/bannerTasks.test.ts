import { RunId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import { deriveDependencyBannerTasks } from "./bannerTasks";

const threadId = ThreadId.make("waiter");
const task = {
  taskId: "jones-dependency:review",
  kind: "monitor",
  awaited: true,
  description: "Waiting for VPS review: release readiness",
} as const;
const base = {
  threadId,
  latestRun: { id: RunId.make("latest"), ordinal: 1, status: "completed" as const },
  providerThreads: [],
  turnItems: [],
};
const shell = {
  id: threadId,
  status: "completed" as const,
  activityRunStatus: null,
  pendingBackgroundTasks: [task],
};

describe("dependency banner roster", () => {
  it("shows who and why after the foreground completes and clears from shell-only refreshes", () => {
    expect(deriveDependencyBannerTasks({ ...base, shell })).toEqual([task]);
    expect(
      deriveDependencyBannerTasks({ ...base, shell: { ...shell, pendingBackgroundTasks: [] } }),
    ).toEqual([]);
  });

  it("requires the same thread shell and only merges Jones dependency entries", () => {
    expect(
      deriveDependencyBannerTasks({ ...base, shell: { ...shell, id: ThreadId.make("other") } }),
    ).toEqual([]);
    expect(deriveDependencyBannerTasks({
      ...base,
      shell: {
        ...shell,
        pendingBackgroundTasks: [task, { taskId: "unrelated", kind: "subagent" }],
      },
    })).toEqual([task]);
    expect(deriveDependencyBannerTasks(base)).toEqual([]);
  });

  it("preserves foreground suppression while detail and shell catch up independently", () => {
    expect(deriveDependencyBannerTasks({
      ...base, shell, runs: [{ ...base.latestRun, status: "running" }],
    })).toEqual([]);
    expect(deriveDependencyBannerTasks({
      ...base, shell: { ...shell, activityRunStatus: "running" },
    })).toEqual([]);
    expect(deriveDependencyBannerTasks({
      ...base, shell: { id: threadId, status: "running", pendingBackgroundTasks: [task] },
    })).toEqual([]);
    expect(deriveDependencyBannerTasks({
      ...base, shell, latestRun: { ...base.latestRun, status: "queued" },
    })).toEqual([]);
  });

  it("retains detail-derived commands and deduplicates repeated dependencies", () => {
    const command = { taskId: "server", kind: "command" as const };
    const tasks = deriveDependencyBannerTasks({
      ...base,
      shell: { ...shell, pendingBackgroundTasks: [task, task] },
      turnItems: [{
        id: command.taskId, type: "command_execution", status: "running", title: null,
      }],
    });
    expect(tasks).toEqual([command, task]);
  });
});
