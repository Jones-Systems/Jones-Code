import {
  ProviderThreadId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  type OrchestrationV2PendingBackgroundTask,
  type OrchestrationV2RunStatus,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";
import {
  backgroundWorkHoldsCompletion,
  derivePendingBackgroundWork,
} from "../../orchestrationV2PendingBackgroundWork.ts";
import { resolveThreadAttentionPill, shellAttentionInput } from "../threadAttention.ts";

const dependency: OrchestrationV2PendingBackgroundTask = {
  taskId: "jones-dependency:request-1",
  kind: "monitor",
  awaited: true,
  description: "VPS analysis ended its turn without answering: waiting for the review",
};
const run = (status: OrchestrationV2RunStatus) => ({ id: RunId.make("run"), ordinal: 1, status });
const base = { providerThreads: [], turnItems: [], dependencies: [dependency] };

function terminalShell(): OrchestrationV2ThreadShell {
  const id = ThreadId.make("waiter");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const now = DateTime.makeUnsafe("2026-10-26T00:00:00.000Z");
  return {
    id,
    projectId: ProjectId.make("project"),
    title: "Waiter",
    providerInstanceId,
    modelSelection: { instanceId: providerInstanceId, model: "synthetic" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { rootThreadId: id, parentThreadId: null, relationshipToParent: null },
    forkedFrom: null,
    createdBy: "user",
    creationSource: "web",
    latestRunId: RunId.make("run"),
    activeRunId: null,
    status: "completed",
    latestRunCompletedAt: now,
    pendingRuntimeRequest: null,
    latestVisibleMessage: null,
    latestUserMessageAt: null,
    hasActionableProposedPlan: false,
    itemCount: 0,
    visibleItemCount: 0,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: DateTime.makeUnsafe("2026-10-25T00:00:00.000Z"),
    deletedAt: null,
  };
}

describe("result dependencies in pending background work", () => {
  it("preserves Working and owner-request priority above dependency Waiting and completion", () => {
    const shell = terminalShell();
    const tasks = derivePendingBackgroundWork({ ...base, latestRun: run("completed") });
    const pill = (thread: OrchestrationV2ThreadShell) =>
      resolveThreadAttentionPill({ thread: shellAttentionInput(thread) });
    expect(pill({ ...shell, pendingBackgroundTasks: tasks })).toBe("Waiting");
    for (const [kind, expected] of [
      ["user_input", "Awaiting Input"],
      ["permission", "Pending Approval"],
    ] as const) {
      expect(
        pill({
          ...shell,
          pendingBackgroundTasks: tasks,
          pendingRuntimeRequest: {
            id: RuntimeRequestId.make("own"),
            kind,
            createdAt: shell.updatedAt,
          },
        }),
      ).toBe(expected);
    }
    const active = run("running");
    expect(
      pill({
        ...shell,
        status: "running",
        activeRunId: active.id,
        activityRunStatus: "running",
        pendingBackgroundTasks: derivePendingBackgroundWork({
          ...base,
          latestRun: active,
          runs: [active],
        }),
      }),
    ).toBe("Working");
    expect(pill({ ...shell, pendingBackgroundTasks: [] })).toBe("Completed");
  });

  it.each(["completed", "interrupted", "cancelled", "failed", "waiting"] as const)(
    "keeps a real dependency after a %s foreground run",
    (status) => {
      const tasks = derivePendingBackgroundWork({ ...base, latestRun: run(status) });
      expect(tasks).toEqual([dependency]);
      expect(backgroundWorkHoldsCompletion(tasks)).toBe(true);
    },
  );

  it("keeps dependencies with no foreground run and clears them when released or answered", () => {
    expect(derivePendingBackgroundWork({ ...base, latestRun: null })).toEqual([dependency]);
    expect(derivePendingBackgroundWork({ ...base, latestRun: undefined })).toEqual([dependency]);
    expect(derivePendingBackgroundWork({ ...base, latestRun: null, dependencies: [] })).toEqual([]);
    expect(
      derivePendingBackgroundWork({ ...base, latestRun: run("completed"), dependencies: [] }),
    ).toEqual([]);
  });

  it.each(["preparing", "queued", "starting", "running", "rolled_back"] as const)(
    "preserves the latest-run presentation gate for %s",
    (status) =>
      expect(derivePendingBackgroundWork({ ...base, latestRun: run(status) })).toEqual([]),
  );

  it("suppresses dependencies during another activity run, including before the first latest run", () => {
    for (const latestRun of [null, run("completed")]) {
      expect(derivePendingBackgroundWork({ ...base, latestRun, hasActiveRun: true })).toEqual([]);
      expect(derivePendingBackgroundWork({ ...base, latestRun, runs: [run("running")] })).toEqual(
        [],
      );
    }
  });

  it("preserves provider tasks and PR monitors while deduplicating dependency identities", () => {
    const command: OrchestrationV2PendingBackgroundTask = { taskId: "server", kind: "command" };
    const tasks = derivePendingBackgroundWork({
      ...base,
      latestRun: run("completed"),
      providerThreads: [
        {
          id: ProviderThreadId.make("provider"),
          pendingBackgroundTasks: [command, dependency],
        },
      ],
      dependencies: [dependency, dependency],
      pullRequests: [
        {
          host: "github.com",
          repository: "Jones-Systems/Jones-Code",
          number: 720,
          url: "https://github.com/Jones-Systems/Jones-Code/pull/720",
          source: "agent",
          watch: {
            startedAt: "2026-10-26T00:00:00.000Z",
            headSha: null,
            failedChecks: [],
            passed: false,
            passedChecks: [],
            remarksThrough: "2026-10-26T00:00:00.000Z",
            remarkIds: [],
            conflicting: false,
            wakes: 0,
          },
        },
      ],
    });
    expect(tasks.filter((task) => task.taskId === dependency.taskId)).toEqual([dependency]);
    expect(tasks).toContainEqual(command);
    expect(tasks).toContainEqual(
      expect.objectContaining({
        taskId: "pull-request-watch:github.com/jones-systems/jones-code#720",
        kind: "monitor",
        awaited: true,
      }),
    );
  });
});
