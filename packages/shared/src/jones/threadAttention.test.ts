import {
  ProjectId,
  ProviderInstanceId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";
import {
  hasUnseenCompletion,
  pendingSubagentRequestsByThread,
  resolveThreadAttentionPill,
  resolveThreadAttentionStatus,
  shellAttentionInput,
  type ThreadAttentionInput,
} from "./threadAttention.ts";

const now = DateTime.makeUnsafe("2026-10-18T12:00:00.000Z");
function shell(overrides: Partial<OrchestrationV2ThreadShell> = {}): OrchestrationV2ThreadShell {
  return {
    id: ThreadId.make("parent"),
    projectId: ProjectId.make("project"),
    title: "Thread",
    providerInstanceId: ProviderInstanceId.make("codex"),
    modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "test-model" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: {
      rootThreadId: ThreadId.make("parent"),
      parentThreadId: null,
      relationshipToParent: null,
    },
    forkedFrom: null,
    createdBy: "user",
    creationSource: "web",
    latestRunId: null,
    activeRunId: null,
    status: "idle",
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
    lastVisitedAt: null,
    deletedAt: null,
    ...overrides,
  };
}
function request(kind: NonNullable<OrchestrationV2ThreadShell["pendingRuntimeRequest"]>["kind"]) {
  return { id: RuntimeRequestId.make("request"), kind, createdAt: now };
}
function child(
  id: string,
  kind: NonNullable<OrchestrationV2ThreadShell["pendingRuntimeRequest"]>["kind"],
  overrides: Partial<OrchestrationV2ThreadShell> = {},
) {
  return shell({
    id: ThreadId.make(id),
    creationSource: "mcp",
    status: "running",
    lineage: {
      rootThreadId: ThreadId.make("parent"),
      parentThreadId: ThreadId.make("parent"),
      relationshipToParent: "subagent",
    },
    pendingRuntimeRequest: request(kind),
    ...overrides,
  });
}
const completed = () =>
  shellAttentionInput(
    shell({ latestRunId: RunId.make("run"), status: "completed", latestRunCompletedAt: now }),
  );

describe("shared native thread attention", () => {
  it("prioritizes own or child approvals, then input, then runtime status", () => {
    const base = shellAttentionInput(shell({ latestRunId: RunId.make("run"), status: "running" }));
    for (const [override, status, pill] of [
      [{}, "working", "Working"],
      [{ hasPendingUserInput: true }, "input", "Awaiting Input"],
      [
        { hasPendingUserInput: true, pendingSubagentRequest: "approval" },
        "approval",
        "Pending Approval",
      ],
      [
        { pendingSubagentRequest: "user_input", hasPendingApprovals: true },
        "approval",
        "Pending Approval",
      ],
    ] as const) {
      const state = { ...base, ...override };
      expect(resolveThreadAttentionStatus(state)).toBe(status);
      expect(resolveThreadAttentionPill({ thread: state })).toBe(pill);
    }
  });
  it.each([
    "preparing",
    "queued",
    "starting",
    "running",
    "waiting",
    "idle",
    "failed",
    "completed",
  ] as const)("preserves %s runtime status and separate pill", (status) => {
    const state = shellAttentionInput(shell({ latestRunId: RunId.make("run"), status }));
    expect(resolveThreadAttentionStatus(state)).toBe(
      status === "idle"
        ? "waiting"
        : status === "failed"
          ? "failed"
          : status === "completed"
            ? "ready"
            : "working",
    );
    expect(resolveThreadAttentionPill({ thread: state })).toBe(
      ["preparing", "queued", "starting"].includes(status)
        ? "Connecting"
        : ["running", "waiting"].includes(status)
          ? "Working"
          : null,
    );
    if (status === "failed")
      expect(
        resolveThreadAttentionStatus({
          ...state,
          runtime: { ...state.runtime!, lastErrorClass: "usage_limit" },
        }),
      ).toBe("limited");
  });
  it("keeps background waits visible and excludes persistent commands", () => {
    const base = shell({
      latestRunId: RunId.make("run"),
      status: "completed",
      interactionMode: "plan",
      hasActionableProposedPlan: true,
    });
    for (const kind of ["subagent", "monitor", "background_task"] as const) {
      const state = shellAttentionInput({
        ...base,
        pendingBackgroundTasks: [{ taskId: "task", kind }],
      });
      expect(resolveThreadAttentionStatus(state)).toBe("waiting");
      expect(resolveThreadAttentionPill({ thread: state })).toBe("Waiting");
    }
    const plan = shellAttentionInput({
      ...base,
      pendingBackgroundTasks: [{ taskId: "task", kind: "command" }],
    });
    expect(resolveThreadAttentionStatus(plan)).toBe("ready");
    expect(resolveThreadAttentionPill({ thread: plan })).toBeNull();
    expect(
      resolveThreadAttentionPill({
        thread: { ...plan, runtime: { ...plan.runtime!, activeRunId: base.latestRunId } },
      }),
    ).toBeNull();
  });
  it("does not turn a stale proposed plan into a request for attention", () => {
    const stalePlan = {
      ...completed(),
      interactionMode: "plan" as const,
      hasActionableProposedPlan: true,
    };
    for (const lastVisitedAt of [undefined, null, "2026-10-18T12:00:00.000Z"]) {
      expect(resolveThreadAttentionPill({ thread: { ...stalePlan, lastVisitedAt } })).toBeNull();
    }
    expect(
      resolveThreadAttentionPill({
        thread: { ...stalePlan, lastVisitedAt: "2026-10-17T12:00:00.000Z" },
      }),
    ).toBe("Completed");
    for (const [request, pill] of [
      [{ hasPendingUserInput: true }, "Awaiting Input"],
      [{ hasPendingApprovals: true }, "Pending Approval"],
      [{ pendingSubagentRequest: "user_input" }, "Awaiting Input"],
      [{ pendingSubagentRequest: "approval" }, "Pending Approval"],
    ] as const) {
      expect(resolveThreadAttentionPill({ thread: { ...stalePlan, ...request } })).toBe(pill);
    }
  });
  it("preserves null/unavailable visits, malformed visits and unseen completion separately", () => {
    const base = completed();
    for (const lastVisitedAt of [
      undefined,
      null,
      "2026-10-18T12:00:00.000Z",
      "2026-10-19T12:00:00.000Z",
    ]) {
      expect(hasUnseenCompletion({ ...base, lastVisitedAt })).toBe(false);
    }
    for (const lastVisitedAt of ["invalid", "2026-10-17T12:00:00.000Z"]) {
      const state = { ...base, lastVisitedAt };
      expect(hasUnseenCompletion(state)).toBe(true);
      expect(resolveThreadAttentionPill({ thread: state })).toBe("Completed");
      expect(resolveThreadAttentionStatus(state)).toBe("ready");
    }
    expect(
      hasUnseenCompletion({
        ...base,
        latestRun: { ...base.latestRun!, completedAt: "invalid" },
        lastVisitedAt: "invalid",
      }),
    ).toBe(false);
  });
  it("bubbles nested live child requests with approval priority and cycle protection", () => {
    const parent = shell();
    const input = child("child-input", "user_input");
    const nested = child("grandchild", "permission", {
      lineage: { ...input.lineage, parentThreadId: input.id },
    });
    for (const threads of [
      [parent, input, nested],
      [nested, input, parent],
    ]) {
      const requests = pendingSubagentRequestsByThread(threads);
      expect(requests.get(parent.id)).toBe("approval");
      expect(requests.get(input.id)).toBe("approval");
    }
    const cycle = { ...input, lineage: { ...input.lineage, parentThreadId: nested.id } };
    expect(pendingSubagentRequestsByThread([cycle, nested]).size).toBe(2);
  });
  it("excludes ended, auth-refresh, fork and provider-native children", () => {
    const children = [
      child("ended", "user_input", { activityRunStatus: null }),
      child("auth", "auth_refresh"),
      child("native", "permission", { creationSource: "provider" }),
      child("fork", "user_input", {
        lineage: {
          rootThreadId: ThreadId.make("parent"),
          parentThreadId: ThreadId.make("parent"),
          relationshipToParent: "fork",
        },
      }),
    ];
    expect(pendingSubagentRequestsByThread(children).size).toBe(0);
    expect(
      pendingSubagentRequestsByThread([
        child("queued", "user_input", { status: "queued", activityRunStatus: "running" }),
      ]).get(ThreadId.make("parent")),
    ).toBe("user_input");
  });
  it("preserves opaque user input and auth refresh without manufacturing request semantics", () => {
    expect(
      shellAttentionInput(shell({ pendingRuntimeRequest: request("auth_refresh") }))
        .hasPendingApprovals,
    ).toBe(false);
    const opaque = shellAttentionInput(shell({ pendingRuntimeRequest: request("user_input") }));
    expect(resolveThreadAttentionStatus(opaque)).toBe("input");
    const plain: ThreadAttentionInput = shellAttentionInput(shell());
    expect(resolveThreadAttentionStatus(plain)).toBe("ready");
    expect(resolveThreadAttentionPill({ thread: plain })).toBeNull();
  });
});
