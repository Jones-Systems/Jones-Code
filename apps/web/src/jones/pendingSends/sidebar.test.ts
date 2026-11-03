import { describe, expect, it } from "vite-plus/test";
import {
  EnvironmentId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import { buildLocalDraftThread } from "../../components/ChatView.logic";
import {
  filterSidebarV2VisibleThreads,
  resolveSidebarThreadStatus,
  resolveThreadStatusPill,
} from "../../components/Sidebar.logic";
import { classifyThreadStatus, selectThreadStatus } from "../threadStatusFilter/model";
import { createPendingSendStore } from "./store";
import { mergePendingSidebarThreads, overlayPendingThread } from "./sidebar";
const environmentId = EnvironmentId.make("env");
const id = ThreadId.make("thread");
const model = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" };
const thread = buildLocalDraftThread(
  id,
  {
    threadId: id,
    environmentId,
    projectId: ProjectId.make("project"),
    logicalProjectKey: "project",
    createdAt: "2026-10-26T20:00:00Z",
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    envMode: "local",
    startFromOrigin: false,
  },
  model,
);
function fixture() {
  const store = createPendingSendStore();
  store.admit(environmentId, id, {
    id: MessageId.make("message"),
    role: "user",
    text: "hello",
    runId: null,
    createdAt: "2026-10-26T20:00:00Z",
    updatedAt: "2026-10-26T20:00:00Z",
    streaming: false,
  });
  store.attachThread(environmentId, id, thread);
  return store;
}
describe("local send sidebar", () => {
  it("shows a new thread as Working in modern and legacy views and deduplicates promotion", () => {
    const store = fixture();
    const local = mergePendingSidebarThreads([], store.getSnapshot());
    expect(local).toHaveLength(1);
    expect(resolveSidebarThreadStatus(local[0]!)).toBe("working");
    expect(
      resolveSidebarThreadStatus(
        overlayPendingThread({ ...thread, blockedOnThreadWait: true }, store.getSnapshot()),
      ),
    ).toBe("working");
    expect(resolveThreadStatusPill({ thread: local[0]! })?.label).toBe("Working");
    const promoted = mergePendingSidebarThreads(
      [{ ...thread, title: "Server title" }],
      store.getSnapshot(),
    );
    expect(promoted).toHaveLength(1);
    expect(promoted[0]?.title).toBe("Server title");
  });
  it("preserves approval, input and guest priority over local work", () => {
    const entries = fixture().getSnapshot();
    expect(
      resolveSidebarThreadStatus(
        overlayPendingThread(
          {
            ...thread,
            hasPendingApprovals: true,
            hasPendingUserInput: true,
            pendingGuestMessageCount: 1,
          },
          entries,
        ),
      ),
    ).toBe("approval");
    expect(
      resolveSidebarThreadStatus(
        overlayPendingThread(
          { ...thread, hasPendingUserInput: true, pendingGuestMessageCount: 1 },
          entries,
        ),
      ),
    ).toBe("input");
    expect(
      resolveSidebarThreadStatus(
        overlayPendingThread({ ...thread, pendingGuestMessageCount: 1 }, entries),
      ),
    ).toBe("request");
  });
  it("keeps child-only work out of top-level Working counts (#713)", () => {
    const childId = ThreadId.make("child");
    const child = {
      ...thread,
      id: childId,
      lineage: { rootThreadId: id, parentThreadId: id, relationshipToParent: "subagent" as const },
      source: {
        ...thread.source,
        id: childId,
        lineage: {
          rootThreadId: id,
          parentThreadId: id,
          relationshipToParent: "subagent" as const,
        },
      },
    };
    const store = createPendingSendStore();
    store.admit(environmentId, childId, {
      id: MessageId.make("child-message"),
      role: "user",
      text: "hello",
      runId: null,
      createdAt: "2026-10-26T20:00:00Z",
      updatedAt: "2026-10-26T20:00:00Z",
      streaming: false,
    });
    const all = mergePendingSidebarThreads([thread, child], store.getSnapshot());
    const scoped = filterSidebarV2VisibleThreads(all, null, new Set());
    const result = selectThreadStatus(scoped, "working", (thread) =>
      classifyThreadStatus(thread, "current"),
    );
    expect(result.counts.working).toBe(0);
    expect(result.threads).toEqual([]);
  });
});
