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
const child = {
  taskId: "turn-item:delegated-task:outside-window",
  kind: "subagent",
  childThreadId: ThreadId.make("child"),
  description: "Review release readiness",
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

  it("merges every same-thread shell duty, including ordinary tasks outside the detail window", () => {
    expect(
      deriveDependencyBannerTasks({ ...base, shell: { ...shell, id: ThreadId.make("other") } }),
    ).toEqual([]);
    expect(
      deriveDependencyBannerTasks({
        ...base,
        shell: {
          ...shell,
          pendingBackgroundTasks: [task, child, { taskId: "server", kind: "command" }],
        },
      }),
    ).toEqual([task, child, { taskId: "server", kind: "command" }]);
    expect(deriveDependencyBannerTasks(base)).toEqual([]);
  });

  it.each([
    [10, 10, false],
    [11, 10, false],
    [9, 10, true],
    [undefined, 10, true],
    [10, undefined, true],
    [null, 10, true],
    [10, null, true],
    [undefined, undefined, true],
  ] as const)(
    "applies exact-item terminal precedence at detail %s / shell %s (visible %s)",
    (detailAppliedSequence, shellSnapshotSequence, visible) => {
      const input = {
        ...base,
        shell: { ...shell, pendingBackgroundTasks: [child, task] },
        detailAppliedSequence,
        shellSnapshotSequence,
        turnItems: [
          {
            id: child.taskId,
            type: "subagent" as const,
            status: "completed" as const,
            title: null,
          },
          {
            id: "different-item",
            type: "subagent" as const,
            status: "failed" as const,
            title: null,
          },
        ],
      };
      expect(deriveDependencyBannerTasks(input)).toEqual(visible ? [child, task] : [task]);
    },
  );

  it("matches a terminal item's native task id without treating its storage id as another task", () => {
    const input = {
      ...base,
      shell: { ...shell, pendingBackgroundTasks: [child, { ...child, taskId: "stored-item" }] },
      detailAppliedSequence: 10,
      shellSnapshotSequence: 10,
      turnItems: [
        {
          id: "stored-item",
          nativeItemRef: { nativeId: child.taskId },
          type: "subagent" as const,
          status: "cancelled" as const,
          title: null,
        },
      ],
    };
    expect(deriveDependencyBannerTasks(input)).toEqual([{ ...child, taskId: "stored-item" }]);
  });

  it("does not treat a terminal text item as evidence that background work ended", () => {
    const input = {
      ...base,
      shell: { ...shell, pendingBackgroundTasks: [child] },
      detailAppliedSequence: 10,
      shellSnapshotSequence: 10,
      turnItems: [
        {
          id: child.taskId,
          type: "assistant_message" as const,
          status: "completed" as const,
          title: null,
        },
      ],
    };
    expect(deriveDependencyBannerTasks(input)).toEqual([child]);
  });

  it("clears an out-of-window task on an empty shell refresh or remount without a shell", () => {
    const input = { ...base, shell: { ...shell, pendingBackgroundTasks: [child] } };
    expect(deriveDependencyBannerTasks(input)).toEqual([child]);
    expect(
      deriveDependencyBannerTasks({ ...input, shell: { ...shell, pendingBackgroundTasks: [] } }),
    ).toEqual([]);
    expect(deriveDependencyBannerTasks({ ...base, shell: null })).toEqual([]);
  });

  it("preserves foreground suppression while detail and shell catch up independently", () => {
    const shellWithChild = { ...shell, pendingBackgroundTasks: [task, child] };
    expect(
      deriveDependencyBannerTasks({
        ...base,
        shell: shellWithChild,
        runs: [{ ...base.latestRun, status: "running" }],
      }),
    ).toEqual([]);
    expect(
      deriveDependencyBannerTasks({
        ...base,
        shell: { ...shellWithChild, activityRunStatus: "running" },
      }),
    ).toEqual([]);
    expect(
      deriveDependencyBannerTasks({
        ...base,
        shell: { id: threadId, status: "running", pendingBackgroundTasks: [task, child] },
      }),
    ).toEqual([]);
    expect(
      deriveDependencyBannerTasks({
        ...base,
        shell: shellWithChild,
        latestRun: { ...base.latestRun, status: "queued" },
      }),
    ).toEqual([]);
  });

  it("retains detail-derived commands and deduplicates repeated dependencies", () => {
    const command = { taskId: "server", kind: "command" as const };
    const tasks = deriveDependencyBannerTasks({
      ...base,
      shell: {
        ...shell,
        pendingBackgroundTasks: [task, task, { ...command, description: "Shell label" }],
      },
      turnItems: [
        {
          id: command.taskId,
          type: "command_execution",
          status: "running",
          title: "Detail label",
        },
      ],
    });
    expect(tasks).toEqual([{ ...command, description: "Detail label" }, task]);
  });
});
