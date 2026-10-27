import { assert, describe, it } from "@effect/vitest";
import {
  ChatAttachmentId,
  MessageId,
  NodeId,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  ScheduledTaskId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2Run,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { automaticTitleContext, eligibleOwnerMessages } from "./evidence.ts";

const threadId = ThreadId.make("thread");
const now = DateTime.makeUnsafe("2026-10-26T00:00:00Z");
const root = NodeId.make("root");
const provider = ProviderThreadId.make("root-provider");
function message(
  id: string,
  changes: Partial<OrchestrationV2ConversationMessage> = {},
): OrchestrationV2ConversationMessage {
  return {
    id: MessageId.make(id),
    threadId,
    runId: null,
    nodeId: null,
    role: "user",
    text: id,
    attachments: [],
    streaming: false,
    createdBy: "user",
    creationSource: "web",
    createdAt: now,
    updatedAt: now,
    ...changes,
  };
}
function run(changes: Partial<OrchestrationV2Run> = {}): OrchestrationV2Run {
  return {
    id: RunId.make("run"),
    threadId,
    ordinal: 1,
    providerInstanceId: ProviderInstanceId.make("codex"),
    modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "test" },
    providerThreadId: provider,
    userMessageId: MessageId.make("one"),
    rootNodeId: root,
    activeAttemptId: null,
    status: "completed",
    requestedAt: now,
    startedAt: now,
    completedAt: now,
    checkpointId: null,
    contextHandoffId: null,
    ...changes,
  };
}
function node(
  id: string,
  changes: Partial<OrchestrationV2ExecutionNode> = {},
): OrchestrationV2ExecutionNode {
  return {
    id: NodeId.make(id),
    threadId,
    runId: RunId.make("run"),
    parentNodeId: root,
    rootNodeId: root,
    kind: "assistant_message",
    status: "completed",
    countsForRun: true,
    providerThreadId: provider,
    providerTurnId: null,
    nativeItemRef: null,
    runtimeRequestId: null,
    checkpointScopeId: null,
    startedAt: now,
    completedAt: now,
    ...changes,
  };
}
function item(
  id: string,
  ordinal: number,
  changes: Partial<Extract<OrchestrationV2TurnItem, { type: "assistant_message" }>> = {},
): OrchestrationV2TurnItem {
  return {
    id: TurnItemId.make(id),
    threadId,
    runId: RunId.make("run"),
    nodeId: NodeId.make(id),
    providerThreadId: provider,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal,
    status: "completed",
    title: null,
    startedAt: now,
    completedAt: now,
    updatedAt: now,
    type: "assistant_message",
    messageId: MessageId.make(id),
    text: id,
    streaming: false,
    ...changes,
  };
}

describe("automatic title evidence", () => {
  it("requires owner provenance, retains owner MCP and queued inputs, deduplicates, and excludes rollback", () => {
    const excluded = [
      message("agent", { createdBy: "agent", creationSource: "mcp" }),
      message("server", { creationSource: "server" }),
      message("system", { createdBy: "system" }),
      message("/compact"),
      message("/logout"),
      message("notification", {
        notification: { source: { kind: "background_task" }, outcome: "updated", summary: "wake" },
      }),
      message("delegation", {
        delegatedCompletion: {
          parentRunId: RunId.make("run"),
          generation: 1,
          taskIds: [NodeId.make("task")],
        },
      }),
      message("schedule", { scheduledTaskId: ScheduledTaskId.make("schedule") }),
      message("workflow", { workflowScript: true }),
      message("forwarded", { senderThreadId: ThreadId.make("sender") }),
      message("rolled-back", { runId: RunId.make("rollback") }),
    ];
    const messages = [
      message("one"),
      ...excluded,
      message("queued-owner", { creationSource: "mcp" }),
      message("one"),
    ];
    assert.deepEqual(
      eligibleOwnerMessages({
        messages,
        runs: [run({ id: RunId.make("rollback"), status: "rolled_back" })],
      }).map((m) => m.id),
      ["one", "queued-owner"],
    );
  });

  it.each(["codex", "claude-code"])(
    "selects the latest completed root-provider reply through native-shaped %s message nodes",
    (driver) => {
      const messages = [
        message("one"),
        message("reply-old", {
          role: "assistant",
          runId: RunId.make("run"),
          nodeId: NodeId.make("reply-old"),
        }),
        message("reply", {
          role: "assistant",
          runId: RunId.make("run"),
          nodeId: NodeId.make("reply"),
        }),
        message("delegated", {
          role: "assistant",
          runId: RunId.make("run"),
          nodeId: NodeId.make("delegated"),
        }),
        message("streaming", {
          role: "assistant",
          streaming: true,
          runId: RunId.make("run"),
          nodeId: NodeId.make("streaming"),
        }),
      ];
      const projection = {
        messages,
        runs: [run({ providerInstanceId: ProviderInstanceId.make(driver) })],
        nodes: [
          node("root", { parentNodeId: null, kind: "root_turn" }),
          node("reply-old"),
          node("reply"),
          node("task", { kind: "subagent" }),
          node("delegated", { parentNodeId: NodeId.make("task") }),
          node("streaming"),
        ],
        turnItems: [
          item("reply-old", 1),
          item("reply", 2),
          item("delegated", 3),
          item("streaming", 4),
        ],
      };
      const context = automaticTitleContext(projection, [MessageId.make("one")]);
      assert.include(context.message, "ASSISTANT:\nreply");
      assert.notInclude(context.message, "reply-old");
      assert.notInclude(context.message, "delegated");
      assert.notInclude(context.message, "streaming");
      assert.notInclude(
        automaticTitleContext({ ...projection, runs: [run({ status: "running" })] }, [
          MessageId.make("one"),
        ]).message,
        "ASSISTANT:",
      );
    },
  );

  it("bounds huge handoffs while preserving the first goal and later clarification, without extra owners", () => {
    const messages = [
      message("one", {
        attachments: Array.from({ length: 8 }, (_, index) => ({
          type: "image" as const,
          id: ChatAttachmentId.make(`attachment-${index}`),
          name: `image-${index}`,
          mimeType: "image/png",
          sizeBytes: 1,
        })),
        text:
          "Goal: installation and update choices\n" +
          "old recovery history ".repeat(10_000) +
          "\nAdopt this goal",
      }),
      message("two", { text: "Choose storage and full backup versus clone" }),
      message("three", { text: "Keep updates manual" }),
      message("four", { text: "SHOULD NOT ENTER" }),
    ];
    const context = automaticTitleContext(
      { messages, runs: [], nodes: [], turnItems: [] },
      messages.slice(0, 3).map((m) => m.id),
    );
    assert.isAtMost(context.message.length, 8_000);
    assert.equal(context.attachments.length, 4);
    assert.equal(context.attachments[0]?.id, "attachment-0");
    assert.include(context.message, "Goal: installation and update choices");
    assert.include(context.message, "Adopt this goal");
    assert.include(context.message, "full backup versus clone");
    assert.include(context.message, "Keep updates manual");
    assert.notInclude(context.message, "SHOULD NOT ENTER");
  });
});
