import { decodePreparedThreadTurn } from "@t3tools/client-runtime/jones/preparedThreadTurn";
import {
  CommandId,
  MessageId,
  ORCHESTRATION_V2_WS_METHODS,
  ThreadId,
  type OrchestrationCommandObservation,
} from "@t3tools/contracts";
import { initialMessageCommandId } from "@t3tools/contracts/jones/deliveryEvidence";
import { expect, it } from "vite-plus/test";

import { acceptedAutoLaunchSequence } from "./outbox";

it("requires a receipt for the original launch or its first message before clearing an Auto intent", () => {
  const prepared = decodePreparedThreadTurn({
    method: ORCHESTRATION_V2_WS_METHODS.launchThread,
    payload: {
      commandId: "original-launch",
      threadId: "original-thread",
      projectId: "project",
      title: "Queued task",
      creationSource: "mobile",
      generateTitle: true,
      accountRouting: { mode: "auto" },
      modelSelection: { instanceId: "codex_personal", model: "gpt-5.4" },
      runtimeMode: "full-access",
      interactionMode: "default",
      workspaceStrategy: { type: "root" },
      initialMessage: { messageId: "original-message", text: "Run the task", attachments: [] },
    },
  });
  if (prepared.method !== ORCHESTRATION_V2_WS_METHODS.launchThread) {
    throw new Error("Expected prepared launch");
  }
  const original: OrchestrationCommandObservation = {
    commandId: CommandId.make("original-launch"),
    threadId: ThreadId.make("original-thread"),
    messageId: MessageId.make("original-message"),
    commandStatus: "accepted",
    acceptedSequence: 42,
    snapshotSequence: 42,
    correlation: "exact",
    target: null,
    turn: null,
  };
  expect(acceptedAutoLaunchSequence(prepared.payload, original)).toBe(42);
  expect(
    acceptedAutoLaunchSequence(prepared.payload, {
      ...original,
      commandId: initialMessageCommandId(original.commandId),
      correlation: "pending",
    }),
  ).toBe(42);
  for (const variance of [
    { correlation: "missing" },
    { correlation: "ambiguous" },
    { commandStatus: "not_found" },
    { commandStatus: "rejected" },
    { commandId: CommandId.make("other-command") },
    { threadId: ThreadId.make("other-thread") },
    { messageId: MessageId.make("other-message") },
  ] as const) {
    expect(
      acceptedAutoLaunchSequence(prepared.payload, { ...original, ...variance }),
    ).toBeUndefined();
  }
});
