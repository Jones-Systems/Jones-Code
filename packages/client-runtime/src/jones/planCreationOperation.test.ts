import { describe, expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  MessageId,
  PlanId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import {
  advancePlanCreation,
  decodePlanCreationOperation,
  encodePlanCreationOperation,
  planCreateDelivery,
  planCreationSlotKey,
} from "./planCreationOperation.ts";

const selection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6.1-sol" };
const source = { threadId: ThreadId.make("source"), planId: PlanId.make("plan") };
const create = {
  commandId: CommandId.make("create-plan"),
  threadId: ThreadId.make("target"),
  projectId: ProjectId.make("project"),
  title: "Implement plan",
  modelSelection: selection,
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: "work/plan",
  worktreePath: "/synthetic/plan",
  createdAt: "2026-11-05T16:00:00.000Z",
} as const;
const record = {
  schemaVersion: 1,
  environmentId: EnvironmentId.make("environment"),
  sourceThreadId: source.threadId,
  sourcePlanId: source.planId,
  action: "implement-in-new-thread",
  phase: "create-pending",
  create,
  prepared: {
    method: "orchestration.dispatchCommand",
    payload: {
      type: "message.dispatch",
      commandId: CommandId.make("message-command"),
      threadId: create.threadId,
      messageId: MessageId.make("message"),
      text: "Implement the captured plan",
      attachments: [],
      modelSelection: selection,
      modelSelectionExplicit: true,
      sourcePlanRef: source,
      dispatchMode: { type: "start_immediately" },
      createdBy: "user",
      creationSource: "web",
    },
  },
} as const;

describe("two-stage plan operation codec", () => {
  it("retains all frozen IDs, explicitness, source linkage and modes across JSON reload", () => {
    const operation = decodePlanCreationOperation(record);
    const restored = decodePlanCreationOperation(
      JSON.parse(JSON.stringify(encodePlanCreationOperation(operation))),
    );
    expect(restored).toEqual(operation);
    expect(Object.isFrozen(restored.create.modelSelection)).toBe(true);
    expect(Object.isFrozen(restored.prepared.payload)).toBe(true);
    const changedDefaults = {
      ...restored,
      create: { ...restored.create, modelSelection: { ...selection, model: "new-model" } },
    };
    expect(planCreationSlotKey(restored)).toBe(planCreationSlotKey(changedDefaults));
  });

  it.each(["commandId", "commandType", "threadId"] as const)(
    "holds mismatched %s evidence as unknown",
    (field) => {
      const operation = decodePlanCreationOperation(record);
      const evidence = {
        kind: "dispatch",
        commandType: "thread.create",
        commandId: create.commandId,
        threadId: create.threadId,
        status: "rejected",
      };
      expect(planCreateDelivery({ deliveryEvidence: evidence }, operation)).toBe("rejected");
      expect(
        planCreateDelivery({ deliveryEvidence: { ...evidence, [field]: "other" } }, operation),
      ).toBe("unknown");
      expect(
        planCreateDelivery(
          { message: "rejected", _tag: "OrchestrationV2DispatchCommandError" },
          operation,
        ),
      ).toBe("unknown");
    },
  );

  it("distinguishes accepted create evidence from completion of its first message", () => {
    const operation = decodePlanCreationOperation(record);
    expect(
      planCreateDelivery(
        {
          deliveryEvidence: {
            kind: "dispatch",
            commandType: "thread.create",
            commandId: create.commandId,
            threadId: create.threadId,
            status: "accepted",
          },
        },
        operation,
      ),
    ).toBe("accepted");
    const message = advancePlanCreation(operation, "message-pending");
    const rejected = advancePlanCreation(message, "message-rejected");
    const accepted = advancePlanCreation(rejected, "accepted");
    expect(advancePlanCreation(accepted, "message-rejected").phase).toBe("accepted");
    expect(() => advancePlanCreation(operation, "accepted")).toThrow();
    expect(() =>
      advancePlanCreation(advancePlanCreation(operation, "create-rejected"), "message-pending"),
    ).toThrow();
  });

  it.each([
    { schemaVersion: 2 },
    {
      prepared: {
        ...record.prepared,
        payload: { ...record.prepared.payload, threadId: "different" },
      },
    },
    {
      prepared: {
        ...record.prepared,
        payload: { ...record.prepared.payload, sourcePlanRef: { ...source, planId: "different" } },
      },
    },
    {
      prepared: {
        ...record.prepared,
        payload: { ...record.prepared.payload, modelSelectionExplicit: undefined },
      },
    },
    {
      prepared: {
        ...record.prepared,
        payload: { ...record.prepared.payload, commandId: create.commandId },
      },
    },
    {
      prepared: {
        ...record.prepared,
        payload: { ...record.prepared.payload, text: "x".repeat(256 * 1024) },
      },
    },
  ])("refuses an unknown, inconsistent or oversized saved record", (change) => {
    expect(() => decodePlanCreationOperation({ ...record, ...change })).toThrow();
  });
});
