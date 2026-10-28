import { presentThreadShell, applyShellStreamEvent } from "@t3tools/client-runtime/state/shell";
import { presentPendingBackgroundWork } from "@t3tools/client-runtime/state/thread-execution";
import {
  EnvironmentId,
  RunId,
  RuntimeRequestId,
  type OrchestrationV2ShellSnapshot,
} from "@t3tools/contracts";
import { derivePendingBackgroundWork } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import { describe, expect, it } from "vite-plus/test";
import { resolveThreadListV2Status } from "../../features/threads/threadListV2";
import { makeRawThreadShell } from "../../test-fixtures";

const environmentId = EnvironmentId.make("synthetic-result-dependencies");
const task = {
  taskId: "jones-dependency:answer",
  kind: "monitor",
  awaited: true,
  description: "VPS review ended its turn without answering: release readiness",
} as const;
const terminal = makeRawThreadShell({
  status: "completed",
  latestRunId: RunId.make("completed"),
  pendingBackgroundTasks: [task],
});

describe("mobile result dependency presentation", () => {
  it("shows Waiting after foreground completion with who and why in the background sheet", () => {
    const thread = presentThreadShell(environmentId, terminal);
    expect(resolveThreadListV2Status(thread)).toBe("waiting");
    expect(presentPendingBackgroundWork(thread.pendingBackgroundTasks)).toMatchObject({
      waiting: true,
      items: [{ taskId: task.taskId, label: task.description, kind: "monitor" }],
    });
  });

  it.each([
    ["user_input", "input"],
    ["command", "approval"],
  ] as const)("prioritizes the waiter's own %s request over the dependency", (kind, status) => {
    const thread = presentThreadShell(environmentId, {
      ...terminal,
      pendingRuntimeRequest: {
        id: RuntimeRequestId.make("own-request"),
        kind,
        createdAt: terminal.updatedAt,
      },
    });
    expect(resolveThreadListV2Status(thread)).toBe(status);
  });

  it("keeps active foreground work Working through the shared roster gate", () => {
    const active = { id: RunId.make("active"), ordinal: 2, status: "running" as const };
    const tasks = derivePendingBackgroundWork({
      latestRun: active,
      runs: [active],
      providerThreads: [],
      turnItems: [],
      dependencies: [task],
    });
    const thread = presentThreadShell(environmentId, {
      ...terminal,
      status: "running",
      activeRunId: active.id,
      activityRunStatus: "running",
      pendingBackgroundTasks: tasks,
    });
    expect(tasks).toEqual([]);
    expect(resolveThreadListV2Status(thread)).toBe("working");
  });

  it("clears the list and sheet when an answer or release refreshes the shell without an event", () => {
    const snapshot: OrchestrationV2ShellSnapshot = {
      schemaVersion: 1,
      snapshotSequence: 7,
      projects: [],
      threads: [terminal],
      archivedThreads: [],
    };
    const refreshed = applyShellStreamEvent(snapshot, {
      kind: "thread.refreshed",
      sequence: 7,
      location: "active",
      thread: { ...terminal, pendingBackgroundTasks: [] },
    });
    const source = refreshed.threads[0];
    if (source === undefined) throw new Error("The refreshed thread must remain visible");
    const thread = presentThreadShell(environmentId, source);
    expect(refreshed.snapshotSequence).toBe(7);
    expect(resolveThreadListV2Status(thread)).toBe("ready");
    expect(presentPendingBackgroundWork(thread.pendingBackgroundTasks)).toBeNull();
  });
});
