import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { TaskContinueInput, TaskContinuationEvidence } from "./taskContinuation.ts";
import { OrchestratorMcpDelegateTaskResult } from "../orchestratorMcp.ts";

describe("guarded task continuation contract", () => {
  const decode = Schema.decodeUnknownSync(TaskContinueInput);
  it("requires a stable bounded key and accepts an optional native compare guard", () => {
    expect(decode({ taskId: "task", message: "continue", clientRequestId: "review-2" })).toEqual({
      taskId: "task",
      message: "continue",
      clientRequestId: "review-2",
    });
    for (const patch of [
      { clientRequestId: "" },
      { clientRequestId: "x".repeat(257) },
      { message: " " },
      { message: "x".repeat(120001) },
      { expectedNativeSessionId: " " },
    ])
      expect(() =>
        decode({ taskId: "task", message: "continue", clientRequestId: "key", ...patch }),
      ).toThrow();
  });
  it("does not claim account identity or legacy transaction evidence", () => {
    const evidence = {
      sourceTaskId: "task",
      sourceRunId: "run",
      sourceEvidence: "attempt",
      providerInstanceId: "claude",
      providerThreadId: "provider",
      nativeSessionId: "native",
      modelSelection: {
        instanceId: "claude",
        model: "opus",
        options: [{ id: "effort", value: "max" }],
      },
      verification: "pending",
      verifiedRunId: null,
      account: "unavailable",
    };
    expect(Schema.is(TaskContinuationEvidence)(evidence)).toBe(true);
    expect(Schema.is(TaskContinuationEvidence)({ ...evidence, sourceAttemptId: "attempt" })).toBe(
      true,
    );
    expect(
      Schema.is(TaskContinuationEvidence)({ ...evidence, sourceEvidence: "legacy_start_batch" }),
    ).toBe(false);
    expect(Schema.is(TaskContinuationEvidence)({ ...evidence, account: "verified" })).toBe(false);
  });
  it("decodes older task results and preserves optional captured session provenance", () => {
    const legacy = {
      taskId: "task",
      childThreadId: "child",
      childRunId: "run",
      childNodeId: "task",
      status: "completed",
      workState: "result_available",
      hasPendingChildRuns: false,
      latestTerminalRunId: "run",
      latestTerminalStatus: "completed",
      latestTerminalSummary: null,
      latestTerminalResultContextTransferId: null,
      providerInstanceId: "claude",
      model: "opus",
      summary: "Done",
      resultContextTransferId: null,
      hasPendingQuestion: false,
      pendingApproval: null,
      waitTimedOut: false,
    };
    const decodeResult = Schema.decodeUnknownSync(OrchestratorMcpDelegateTaskResult);
    expect(decodeResult(legacy)).toEqual(legacy);
    const nativeSession = {
      nativeSessionId: "native",
      sourceRunId: "run",
      sourceAttemptId: "attempt",
      evidence: "attempt",
    };
    expect(decodeResult({ ...legacy, nativeSession })).toEqual({ ...legacy, nativeSession });
  });
});
