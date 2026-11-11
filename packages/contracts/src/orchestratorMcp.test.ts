import * as OrchestratorMcp from "./orchestratorMcp.ts";
import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";

import {
  OrchestratorMcpThreadSettleInput,
  OrchestratorMcpThreadSettleResult,
  OrchestratorMcpDelegateTaskInput,
  OrchestratorMcpDelegateTaskResult,
} from "./orchestratorMcp.ts";

const decodeDelegateTaskInput = Schema.decodeUnknownSync(OrchestratorMcpDelegateTaskInput);
const decodeDelegateTaskResult = Schema.decodeUnknownSync(OrchestratorMcpDelegateTaskResult);

describe("orchestrator MCP contracts", () => {
  const threadListItem = {
    threadId: "thread:background-work",
    title: "Waiting for background work",
    createdBy: "user",
    creationSource: "web",
    status: "completed",
    latestRunId: null,
    providerInstanceId: "codex",
    model: "fixture",
    runtimeMode: "full-access",
    interactionMode: "default",
    linkedPullRequest: null,
    settled: false,
    settledAt: null,
    snoozed: false,
    snoozedUntil: null,
    parentThreadId: null,
    relationshipToParent: null,
    itemCount: 0,
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
  };
  const threadDetail = {
    ...threadListItem,
    projectId: "project:background-work",
    activeRunId: null,
    titleRegeneration: null,
    branch: null,
    worktreePath: null,
    runCount: 0,
    pendingRequestCount: 0,
    archived: false,
  };

  it("retains background work evidence on detail and paged list responses", () => {
    const backgroundWork = {
      pendingBackgroundTasks: [
        {
          taskId: "task:child",
          kind: "subagent",
          description: "Finish the delegated work",
          parentTaskId: "task:parent",
          providerInstanceId: "codex",
          childThreadId: "thread:child",
        },
        { taskId: "task:command", kind: "command", awaited: true },
        { taskId: "task:monitor", kind: "monitor" },
        { taskId: "task:background", kind: "background_task" },
      ],
      backgroundHoldsCompletion: true,
      pendingBackgroundTasksTruncated: false,
    };
    const detailPayload = { ...threadDetail, ...backgroundWork };
    expect(
      Schema.decodeUnknownSync(OrchestratorMcp.OrchestratorMcpThreadDetail)(detailPayload),
    ).toEqual(detailPayload);
    const listPayload = {
      projectId: threadDetail.projectId,
      currentThreadId: null,
      threads: [{ ...threadListItem, ...backgroundWork, pendingBackgroundTasksTruncated: true }],
      nextCursor: 1,
      total: 2,
    };
    expect(
      Schema.decodeUnknownSync(OrchestratorMcp.OrchestratorMcpThreadListResult)(listPayload),
    ).toEqual(listPayload);
    const empty = {
      ...threadListItem,
      pendingBackgroundTasks: [],
      backgroundHoldsCompletion: false,
      pendingBackgroundTasksTruncated: false,
    };
    expect(Schema.decodeUnknownSync(OrchestratorMcp.OrchestratorMcpThreadListItem)(empty)).toEqual(
      empty,
    );
  });

  it("keeps absent background evidence unknown in older detail and paged peer payloads", () => {
    const detail = Schema.decodeUnknownSync(OrchestratorMcp.OrchestratorMcpThreadDetail)(
      threadDetail,
    );
    const list = Schema.decodeUnknownSync(OrchestratorMcp.OrchestratorMcpThreadListResult)({
      projectId: threadDetail.projectId,
      currentThreadId: null,
      threads: [threadListItem],
      nextCursor: 1,
      total: 2,
    });
    for (const thread of [detail, ...list.threads]) {
      for (const field of [
        "pendingBackgroundTasks",
        "backgroundHoldsCompletion",
        "pendingBackgroundTasksTruncated",
      ]) {
        expect(Object.hasOwn(thread, field)).toBe(false);
      }
    }
    expect(list.nextCursor).toBe(1);
  });

  it("decodes cross-provider delegated task requests and durable results", () => {
    const request = decodeDelegateTaskInput({
      task: "Inspect the workspace and report the result.",
      target: {
        providerInstanceId: "claudeAgent",
        model: "claude-sonnet-4-6",
      },
      mode: "wait",
      timeoutMs: 5_000,
      clientRequestId: "delegate-1",
      runtimeMode: "inherit",
      interactionMode: "inherit",
    });
    const result = decodeDelegateTaskResult({
      taskId: "node-task-1",
      childThreadId: "thread-child-1",
      childRunId: "run-child-1",
      childNodeId: "node-task-1",
      status: "completed",
      workState: "result_available",
      hasPendingChildRuns: false,
      providerInstanceId: "claudeAgent",
      model: "claude-sonnet-4-6",
      summary: "Workspace inspected.",
      resultContextTransferId: "context-transfer-result-1",
      latestTerminalRunId: "run-child-1",
      latestTerminalStatus: "completed",
      latestTerminalSummary: "Workspace inspected.",
      latestTerminalResultContextTransferId: "context-transfer-result-1",
      hasPendingQuestion: false,
      pendingApproval: null,
      waitTimedOut: false,
    });

    expect(request.target?.providerInstanceId).toBe("claudeAgent");
    expect(result.status).toBe("completed");
    expect(result.summary).toBe("Workspace inspected.");
    expect(result.pendingApproval).toBeNull();
    const pendingApproval = {
      requestId: "request-approval-1",
      kind: "command",
      runId: "run-child-1",
    };
    expect(
      decodeDelegateTaskResult({
        ...result,
        pendingApproval: { ...pendingApproval, input: { command: "private command" } },
      }).pendingApproval,
    ).toEqual(pendingApproval);
    expect(() =>
      decodeDelegateTaskResult({
        ...result,
        pendingApproval: { ...pendingApproval, kind: "user_input" },
      }),
    ).toThrow();
  });
});

describe("self settlement contracts", () => {
  it("requires a stable request key and has no other-thread selector", () => {
    const decode = Schema.decodeUnknownSync(OrchestratorMcpThreadSettleInput, {
      onExcessProperty: "error",
    });
    expect(decode({ clientRequestId: "finish-1" })).toEqual({ clientRequestId: "finish-1" });
    for (const input of [
      {},
      { clientRequestId: "" },
      { clientRequestId: "finish-1", threadId: "other" },
    ]) {
      expect(() => decode(input)).toThrow();
    }
  });
  it("preserves bounded plain-text project remainder items", () => {
    const decode = Schema.decodeUnknownSync(OrchestratorMcpThreadSettleInput);
    const projectRemaining = [
      "Ship the follow-up",
      "Review <script> as plain text",
      "é".repeat(1024),
    ];
    expect(decode({ clientRequestId: "finish-items", projectRemaining })).toEqual({
      clientRequestId: "finish-items",
      projectRemaining,
    });
    for (const items of [
      [""],
      ["   "],
      ["two\nlines"],
      ["tab\there"],
      ["é".repeat(1025)],
      Array(65).fill("work"),
    ]) {
      expect(() => decode({ clientRequestId: "finish-items", projectRemaining: items })).toThrow();
    }
    expect(
      decode({ clientRequestId: "finish-items", projectRemaining: Array(64).fill("work") })
        .projectRemaining,
    ).toHaveLength(64);
  });
  it("reports accepted intent bound to the original run rather than completed settlement", () => {
    const decode = Schema.decodeUnknownSync(OrchestratorMcpThreadSettleResult);
    const receipt = {
      status: "accepted",
      threadId: "thread-1",
      runId: "run-1",
      clientRequestId: "finish-1",
    };
    expect(decode(receipt)).toEqual(receipt);
    expect(() => decode({ ...receipt, status: "settled" })).toThrow();
  });
});

it("keeps native activity optional and validates explicit incomplete background evidence", () => {
  const decode = Schema.decodeUnknownSync(OrchestratorMcp.OrchestratorMcpThreadActivityObservation);
  expect(
    decode({
      foreground: "working",
      background: null,
      backgroundStatus: "unknown",
      reason: "native_background_coverage_incomplete",
    }),
  ).toEqual({
    foreground: "working",
    background: null,
    backgroundStatus: "unknown",
    reason: "native_background_coverage_incomplete",
  });
  expect(
    Schema.is(OrchestratorMcp.OrchestratorMcpThreadActivityObservation)({
      foreground: null,
      background: "busy",
      backgroundStatus: "known",
    }),
  ).toBe(false);
});

describe("compact orchestration capability contracts", () => {
  it("decodes search and environment selection together", () => {
    const decode = Schema.decodeUnknownSync(OrchestratorMcp.OrchestratorMcpCapabilitiesInput);
    expect(decode({ environmentId: "environment:remote", search: "sonnet 4.6" })).toEqual({
      environmentId: "environment:remote",
      search: "sonnet 4.6",
    });
    expect(decode({})).toEqual({});
    expect(() => decode({ search: " " })).toThrow();
    expect(() => decode({ search: "a".repeat(201) })).toThrow();
  });

  it("retains compact options and accepts absent or explicit catalog labels and omitted counts", () => {
    const decode = Schema.decodeUnknownSync(OrchestratorMcp.OrchestratorMcpProviderCapability);
    const provider = {
      providerInstanceId: "codex",
      driverKind: "codex",
      displayName: null,
      models: [
        {
          id: "model:synthetic",
          options: [
            { id: "reasoningEffort", values: ["high", "max"], default: "high" },
            { id: "fastMode", values: [false, true] },
          ],
        },
      ],
      canRunChildTask: true,
      canRunCrossProviderChildTask: true,
      constraints: [],
    };
    expect(decode(provider)).toEqual(provider);
    const labeled = {
      ...provider,
      models: provider.models.map((model) => ({ ...model, label: "Synthetic model" })),
      omittedModels: 12,
    };
    expect(decode(labeled)).toEqual(labeled);
    expect(() => decode({ ...provider, omittedModels: -1 })).toThrow();
  });
});
