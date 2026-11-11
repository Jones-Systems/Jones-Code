import {
  MessageId,
  NodeId,
  ProviderDriverKind,
  ProviderThreadId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import {
  mergeShellSnapshotProjects,
  presentThreadShell,
} from "@t3tools/client-runtime/state/shell";
import { presentPendingBackgroundWork } from "@t3tools/client-runtime/state/thread-execution";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";
import { deriveThreadPendingBackgroundTasks } from "../../components/ChatView.logic";
import { makeThreadFixture, makeThreadProjectionFixture } from "../../test-fixtures";
import { resolveThreadStatusPill } from "../../components/Sidebar.logic";

describe("ChatView result dependency integration", () => {
  it("preserves sidebar priority over completion and clears Waiting after a shell refresh", () => {
    const fixture = makeThreadFixture();
    const task = {
      taskId: "jones-dependency:review",
      kind: "monitor",
      awaited: true,
      description: "Waiting for VPS review",
    } as const;
    const source = {
      ...fixture.source,
      status: "completed" as const,
      latestRunId: RunId.make("latest"),
      latestRunCompletedAt: fixture.source.updatedAt,
      lastVisitedAt: DateTime.makeUnsafe("2025-12-31T00:00:00.000Z"),
      pendingBackgroundTasks: [task],
    };
    const thread = presentThreadShell(fixture.environmentId, source);
    expect(resolveThreadStatusPill({ thread })?.label).toBe("Waiting");
    const request = { id: RuntimeRequestId.make("own-request"), createdAt: source.updatedAt };
    const input = presentThreadShell(fixture.environmentId, {
      ...source,
      pendingRuntimeRequest: { ...request, kind: "user_input" },
    });
    expect(resolveThreadStatusPill({ thread: input })?.label).toBe("Awaiting Input");
    const approval = presentThreadShell(fixture.environmentId, {
      ...source,
      pendingRuntimeRequest: { ...request, kind: "permission" },
    });
    expect(resolveThreadStatusPill({ thread: approval })?.label).toBe("Pending Approval");
    const cleared = presentThreadShell(fixture.environmentId, {
      ...source,
      pendingBackgroundTasks: [],
    });
    expect(resolveThreadStatusPill({ thread: cleared })?.label).toBe("Completed");
  });

  it("shows an ordinary out-of-window child in the banner and clears it after reconnect", () => {
    const projection = makeThreadProjectionFixture();
    const run = {
      id: RunId.make("run"),
      threadId: projection.thread.id,
      ordinal: 1,
      providerInstanceId: projection.thread.providerInstanceId,
      modelSelection: projection.thread.modelSelection,
      providerThreadId: ProviderThreadId.make("provider"),
      userMessageId: MessageId.make("message"),
      rootNodeId: null,
      activeAttemptId: null,
      status: "completed" as const,
      requestedAt: projection.updatedAt,
      startedAt: projection.updatedAt,
      completedAt: projection.updatedAt,
      checkpointId: null,
      contextHandoffId: null,
    };
    const detail = { ...projection, runs: [run] };
    const task = {
      taskId: "turn-item:delegated-task:outside-window",
      kind: "subagent",
      childThreadId: ThreadId.make("child"),
      description: "Review release readiness",
    } as const;
    const shell = {
      id: projection.thread.id,
      status: "completed" as const,
      activityRunStatus: null,
      pendingBackgroundTasks: [task],
    };
    expect(deriveThreadPendingBackgroundTasks(detail, shell)).toEqual([task]);
    const banner = presentPendingBackgroundWork(deriveThreadPendingBackgroundTasks(detail, shell));
    expect(banner).toMatchObject({
      waiting: true,
      items: [{ label: task.description, childThreadId: task.childThreadId }],
    });
    const raw = makeThreadFixture().source;
    const initial = {
      schemaVersion: 1 as const,
      snapshotSequence: 8,
      projects: [],
      threads: [{ ...raw, ...shell }],
      archivedThreads: [],
    };
    const reconnected = mergeShellSnapshotProjects(initial, {
      ...initial,
      snapshotSequence: 9,
      threads: [{ ...raw, ...shell, pendingBackgroundTasks: [] }],
    });
    expect(
      presentPendingBackgroundWork(
        deriveThreadPendingBackgroundTasks(detail, reconnected.threads[0]),
      ),
    ).toBeNull();
    expect(
      deriveThreadPendingBackgroundTasks(detail, { ...shell, pendingBackgroundTasks: [] }),
    ).toEqual([]);
    expect(
      deriveThreadPendingBackgroundTasks(
        {
          ...detail,
          runs: [{ ...run, status: "running", completedAt: null }],
        },
        shell,
      ),
    ).toEqual([]);
    expect(deriveThreadPendingBackgroundTasks(null, shell)).toEqual([]);

    const terminal: OrchestrationV2TurnItem = {
      id: TurnItemId.make(task.taskId),
      threadId: projection.thread.id,
      runId: run.id,
      nodeId: null,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: 1,
      type: "subagent",
      subagentId: NodeId.make("child-task"),
      origin: "app_owned",
      driver: ProviderDriverKind.make("codex"),
      providerInstanceId: projection.thread.providerInstanceId,
      childThreadId: task.childThreadId,
      prompt: task.description,
      result: "Review complete",
      status: "completed",
      title: null,
      startedAt: projection.updatedAt,
      completedAt: projection.updatedAt,
      updatedAt: projection.updatedAt,
    };
    const completedDetail = { ...detail, turnItems: [terminal] };
    expect(
      deriveThreadPendingBackgroundTasks(completedDetail, shell, {
        detailAppliedSequence: 8,
        shellSnapshotSequence: 9,
      }),
    ).toEqual([task]);
    expect(
      deriveThreadPendingBackgroundTasks(completedDetail, shell, {
        detailAppliedSequence: 9,
        shellSnapshotSequence: 9,
      }),
    ).toEqual([]);
  });
});
