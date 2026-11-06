import { MessageId, ProviderThreadId, RunId, RuntimeRequestId } from "@t3tools/contracts";
import { presentThreadShell } from "@t3tools/client-runtime/state/shell";
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

  it("uses shell-only duties after completion without changing detail history", () => {
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
      taskId: "jones-dependency:review",
      kind: "monitor",
      awaited: true,
      description: "Waiting for VPS review",
    } as const;
    const shell = {
      id: projection.thread.id,
      status: "completed" as const,
      activityRunStatus: null,
      pendingBackgroundTasks: [task],
    };
    expect(deriveThreadPendingBackgroundTasks(detail, shell)).toEqual([task]);
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
  });
});
