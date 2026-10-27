import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { TaskContinueInput, TaskContinuationEvidence } from "./taskContinuation.ts";

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
    expect(
      Schema.is(TaskContinuationEvidence)({ ...evidence, sourceEvidence: "legacy_start_batch" }),
    ).toBe(false);
    expect(Schema.is(TaskContinuationEvidence)({ ...evidence, account: "verified" })).toBe(false);
  });
});
