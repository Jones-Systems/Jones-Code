import { describe, expect, it } from "vite-plus/test";
import { summarizeT3ToolCalls, type T3ToolSummaryCall } from "../t3ToolSummary.ts";

function completed(input: unknown, output?: unknown): T3ToolSummaryCall {
  return { input, output, outcome: "completed" };
}

describe("summarizeT3ToolCalls", () => {
  it("counts accepted self-settlement targets across replay without claiming settlement completed", () => {
    const accepted = completed(
      { clientRequestId: "request-1" },
      { status: "accepted", threadId: "thread-1", runId: "run-1", clientRequestId: "request-1" },
    );
    expect(summarizeT3ToolCalls("thread-settle", [accepted, accepted])).toEqual({
      label: "Requested settlement for 1 thread",
      failedCount: 0,
    });
    const second = completed(
      { clientRequestId: "request-2" },
      { status: "accepted", threadId: "thread-2", runId: "run-2", clientRequestId: "request-2" },
    );
    expect(summarizeT3ToolCalls("thread-settle", [accepted, accepted, second])).toEqual({
      label: "Requested settlement for 2 threads",
      failedCount: 0,
    });
  });

  it("keeps denied and unfinished self-settlement calls distinct from accepted requests", () => {
    const denied = completed(
      { clientRequestId: "denied-request" },
      {
        structuredContent: {
          _tag: "OrchestratorMcpFailure",
          code: "capability_denied",
          message: "Unavailable",
        },
      },
    );
    const unfinished: T3ToolSummaryCall = {
      input: { clientRequestId: "unfinished-request" },
      output: undefined,
      outcome: "unfinished",
    };
    expect(summarizeT3ToolCalls("thread-settle", [denied])).toEqual({
      label: "Tried to request settlement for 1 thread",
      failedCount: 1,
    });
    expect(summarizeT3ToolCalls("thread-settle", [unfinished])).toEqual({
      label: "Tried to request settlement for 1 thread",
      failedCount: 0,
    });
    const accepted = completed(
      { clientRequestId: "accepted-request" },
      {
        status: "accepted",
        threadId: "thread-1",
        runId: "run-1",
        clientRequestId: "accepted-request",
      },
    );
    expect(summarizeT3ToolCalls("thread-settle", [denied, unfinished, accepted])).toEqual({
      label: "Requested settlement for 1 thread",
      failedCount: 1,
    });
  });
});
