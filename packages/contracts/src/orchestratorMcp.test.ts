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
