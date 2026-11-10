import { describe, expect, it } from "vite-plus/test";
import {
  CommandId,
  MessageId,
  ProviderInstanceId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2Command,
} from "@t3tools/contracts";
import { transientSteerTarget } from "./TransientSteer.ts";

const selection = { instanceId: ProviderInstanceId.make("codex"), model: "test" };
const runId = RunId.make("active");
const attemptId = RunAttemptId.make("attempt");
const command: Extract<OrchestrationV2Command, { type: "message.dispatch" }> = {
  type: "message.dispatch",
  commandId: CommandId.make("send"),
  threadId: ThreadId.make("thread"),
  messageId: MessageId.make("message"),
  createdBy: "user",
  creationSource: "web",
  text: "Follow-up",
  attachments: [],
  deliveryIntent: "steer",
  dispatchMode: { type: "start_immediately" },
};
const steer = { type: "steer_active" as const, targetRunId: runId };
function projection(status = "running", providerTurns: unknown[] = []) {
  return {
    runs: [{ id: runId, status, activeAttemptId: attemptId, modelSelection: selection }],
    providerTurns,
  } as unknown as OrchestrationV2ThreadProjection;
}

describe("transient steer delivery", () => {
  it.each(["web", "mobile"] as const)(
    "defers a %s follow-up in a native turn gap",
    (creationSource) => {
      expect(
        transientSteerTarget(projection(), { ...command, creationSource }, steer, selection, false)
          ?.id,
      ).toBe(runId);
    },
  );
  it("keeps a healthy turn on its steer validation path", () => {
    expect(
      transientSteerTarget(
        projection("running", [{ runAttemptId: attemptId, status: "running" }]),
        command,
        steer,
        selection,
        false,
      ),
    ).toBeUndefined();
  });
  it.each([undefined, "auto", "restart"] as const)(
    "preserves legacy and %s delivery",
    (deliveryIntent) => {
      const { deliveryIntent: _, ...legacy } = command;
      expect(
        transientSteerTarget(
          projection(),
          { ...legacy, ...(deliveryIntent && { deliveryIntent }) },
          steer,
          selection,
          false,
        ),
      ).toBeUndefined();
    },
  );
  it("preserves separate-turn commands and configuration transition validation", () => {
    expect(transientSteerTarget(projection(), command, steer, selection, true)).toBeUndefined();
    expect(
      transientSteerTarget(projection(), command, steer, { ...selection, model: "other" }, false),
    ).toBeUndefined();
    expect(
      transientSteerTarget(
        projection(),
        command,
        steer,
        { ...selection, instanceId: ProviderInstanceId.make("other") },
        false,
      ),
    ).toBeUndefined();
  });
  it.each(["completed", "interrupted", "failed", "cancelled", "queued"])(
    "never defers a %s target",
    (status) => {
      expect(
        transientSteerTarget(projection(status), command, steer, selection, false),
      ).toBeUndefined();
    },
  );
  it("does not broaden MCP or server-generated delivery", () => {
    expect(
      transientSteerTarget(
        projection(),
        { ...command, createdBy: "agent", creationSource: "server" },
        steer,
        selection,
        false,
      ),
    ).toBeUndefined();
  });
});
