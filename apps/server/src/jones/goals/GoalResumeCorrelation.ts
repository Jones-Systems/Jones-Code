import {
  type CommandId,
  type MessageId,
  type ThreadId,
  OrchestrationV2ConversationMessageJson,
  OrchestrationV2ExecutionNodeJson,
  OrchestrationV2SubagentJson,
  OrchestrationV2TurnItemJson,
} from "@t3tools/contracts";
import { derive } from "@t3tools/provider-core/server/IdAllocator";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import { RecordedRunJson } from "../../orchestration-v2/RecordedTypes.ts";

const readRun = Schema.decodeEffect(Schema.fromJsonString(RecordedRunJson));
const readMessage = Schema.decodeEffect(
  Schema.fromJsonString(OrchestrationV2ConversationMessageJson),
);
const readNode = Schema.decodeEffect(Schema.fromJsonString(OrchestrationV2ExecutionNodeJson));
const readTask = Schema.decodeEffect(Schema.fromJsonString(OrchestrationV2SubagentJson));
const readItem = Schema.decodeEffect(Schema.fromJsonString(OrchestrationV2TurnItemJson));

interface CommandEvent {
  readonly sequence: number;
  readonly stream_id: string;
  readonly aggregate_kind: string;
  readonly event_type: string;
  readonly payload_json: string;
}

/** Prove the parent's follow-up trio without removing its receipt-sequence tail. */
export const makeGoalResumeCorrelation = Effect.gen(function* () {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  return (input: {
    readonly threadId: ThreadId;
    readonly commandId: CommandId;
    readonly messageId: MessageId;
    readonly commandType: string | undefined;
    readonly events: ReadonlyArray<CommandEvent>;
  }) =>
    Effect.gen(function* () {
      const none = new Set<number>();
      if (
        input.commandType !== "message.dispatch" ||
        input.commandId !== String(input.messageId) ||
        !input.commandId.startsWith("goal:")
      )
        return none;
      const foreign = input.events.filter((e) => e.stream_id !== input.threadId);
      if (foreign.length !== 3 || foreign.some((e) => e.aggregate_kind !== "thread")) return none;
      const child = yield* projections.getThreadShell(input.threadId);
      const parentId = child?.lineage.parentThreadId;
      if (
        child?.lineage.relationshipToParent !== "subagent" ||
        parentId == null ||
        parentId === input.threadId ||
        foreign.some((e) => e.stream_id !== parentId)
      )
        return none;
      const one = (type: string, rows = input.events) => {
        const matches = rows.filter((e) => e.event_type === type);
        return matches.length === 1 ? matches[0] : undefined;
      };
      const birth = one("run.created");
      const update = one("run.updated");
      const message = one("message.updated");
      const node = one("node.updated", foreign);
      const task = one("subagent.updated", foreign);
      const item = one("turn-item.updated", foreign);
      if (!birth || !update || !message || !node || !task || !item) return none;
      const run = yield* readRun(birth.payload_json);
      const bound = yield* readRun(update.payload_json);
      const sent = yield* readMessage(message.payload_json);
      const ownerNode = yield* readNode(node.payload_json);
      const ownedTask = yield* readTask(task.payload_json);
      const turnItem = yield* readItem(item.payload_json);
      const taskId = derive.delegatedTaskNode(input);
      if (
        run.threadId !== input.threadId ||
        run.userMessageId !== input.messageId ||
        bound.id !== run.id ||
        bound.threadId !== run.threadId ||
        bound.userMessageId !== run.userMessageId ||
        bound.delegatedTaskId !== taskId ||
        sent.id !== input.messageId ||
        sent.threadId !== input.threadId ||
        sent.runId !== run.id ||
        sent.role !== "user" ||
        sent.text !== "/goal resume" ||
        sent.senderThreadId !== parentId ||
        sent.createdBy !== "agent" ||
        sent.creationSource !== "mcp" ||
        ownedTask.id !== taskId ||
        ownedTask.threadId !== parentId ||
        ownedTask.childThreadId !== input.threadId ||
        ownedTask.origin !== "app_owned" ||
        ownedTask.providerInstanceId !== run.providerInstanceId ||
        ownedTask.providerThreadId !== run.providerThreadId ||
        ownerNode.id !== taskId ||
        ownerNode.threadId !== parentId ||
        ownerNode.runId !== ownedTask.runId ||
        ownerNode.kind !== "subagent" ||
        ownerNode.parentNodeId !== ownedTask.parentNodeId ||
        turnItem.type !== "subagent" ||
        turnItem.id !== derive.delegatedTaskTurnItem(input) ||
        turnItem.threadId !== parentId ||
        turnItem.runId !== ownedTask.runId ||
        turnItem.nodeId !== taskId ||
        turnItem.subagentId !== taskId ||
        turnItem.childThreadId !== input.threadId ||
        turnItem.origin !== "app_owned"
      )
        return none;
      return new Set(foreign.map((e) => e.sequence));
    });
});
