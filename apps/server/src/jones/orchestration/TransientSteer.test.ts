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
  type ModelSelection,
} from "@t3tools/contracts";
import { ownerSteerSelection } from "../threadConfiguration/ownerSelection.ts";
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
        transientSteerTarget(
          projection(),
          { ...command, creationSource },
          steer,
          () => selection,
          false,
        )?.id,
      ).toBe(runId);
    },
  );
  it.each([
    { ...selection, instanceId: ProviderInstanceId.make("other") },
    { ...selection, model: "prospective" },
    { ...selection, options: [{ id: "reasoningEffort", value: "high" }] },
  ] satisfies ModelSelection[])("uses inherited owner selection with saved %j", (saved) => {
    const inherited = (active: ModelSelection) =>
      ownerSteerSelection({
        ownerMessage: true,
        explicit: false,
        pending: undefined,
        requested: saved,
        active,
      }).modelSelection;
    expect(transientSteerTarget(projection(), command, steer, inherited, false)?.id).toBe(runId);
    expect(transientSteerTarget(projection(), command, steer, () => saved, false)).toBeUndefined();
  });
  it.each([
    { explicit: true, pending: undefined, requested: { ...selection, model: "explicit" } },
    { explicit: false, pending: { ...selection, model: "pending" }, requested: selection },
    { explicit: false, pending: selection, requested: { ...selection, model: "saved" } },
  ])("preserves owner transition validation: %j", (input) => {
    const effective = (active: ModelSelection) =>
      ownerSteerSelection({
        ownerMessage: true,
        ...input,
        active,
      }).modelSelection;
    const target = transientSteerTarget(projection(), command, steer, effective, false);
    expect(target?.id).toBe(input.pending === selection ? runId : undefined);
  });
  it("keeps a healthy turn on its steer validation path", () => {
    expect(
      transientSteerTarget(
        projection("running", [{ runAttemptId: attemptId, status: "running" }]),
        command,
        steer,
        () => selection,
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
          () => selection,
          false,
        ),
      ).toBeUndefined();
    },
  );
  it("preserves separate-turn commands and configuration transition validation", () => {
    expect(
      transientSteerTarget(projection(), command, steer, () => selection, true),
    ).toBeUndefined();
    expect(
      transientSteerTarget(
        projection(),
        command,
        steer,
        () => ({ ...selection, model: "other" }),
        false,
      ),
    ).toBeUndefined();
    expect(
      transientSteerTarget(
        projection(),
        command,
        steer,
        () => ({ ...selection, instanceId: ProviderInstanceId.make("other") }),
        false,
      ),
    ).toBeUndefined();
  });
  it.each(["completed", "interrupted", "failed", "cancelled", "queued"])(
    "never defers a %s target",
    (status) => {
      expect(
        transientSteerTarget(projection(status), command, steer, () => selection, false),
      ).toBeUndefined();
    },
  );
  it("does not broaden MCP or server-generated delivery", () => {
    expect(
      transientSteerTarget(
        projection(),
        { ...command, createdBy: "agent", creationSource: "server" },
        steer,
        () => selection,
        false,
      ),
    ).toBeUndefined();
  });
});
