import {
  EventId,
  MessageId,
  NodeId,
  PlanId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2Run,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import type * as DateTime from "effect/DateTime";

/**
 * Records a lost process left open under ended work, as seen on a long-running host,
 * beside records that are open by design and must survive recovery: a held queued run,
 * an async question and a Codex todo list.
 */

const driver = ProviderDriverKind.make("codex");
const providerInstanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId: providerInstanceId, model: "gpt-5.4" };

let eventCounter = 0;
const eventId = (prefix: string) => EventId.make(`${prefix}:event:${(eventCounter += 1)}`);

export const threadCreated = (
  threadId: ThreadId,
  now: DateTime.Utc,
  activeProviderThreadId: ProviderThreadId | null,
): OrchestrationV2DomainEvent => ({
  id: eventId(threadId),
  type: "thread.created",
  threadId,
  occurredAt: now,
  payload: {
    id: threadId,
    projectId: ProjectId.make("project:orphaned-run-records"),
    title: "Orphaned run records",
    providerInstanceId,
    modelSelection,
    createdBy: "user",
    creationSource: "web",
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  },
});

export const providerThreadUpdated = (
  threadId: ThreadId,
  providerThreadId: ProviderThreadId,
  now: DateTime.Utc,
): OrchestrationV2DomainEvent => ({
  id: eventId(threadId),
  type: "provider-thread.updated",
  threadId,
  driver,
  providerInstanceId,
  occurredAt: now,
  payload: {
    id: providerThreadId,
    driver,
    providerInstanceId,
    providerSessionId: null,
    appThreadId: threadId,
    ownerNodeId: null,
    nativeThreadRef: { driver, nativeId: `native:${providerThreadId}`, strength: "strong" },
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: 1,
    lastRunOrdinal: 1,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  },
});

export const runUpdated = (run: OrchestrationV2Run): OrchestrationV2DomainEvent => ({
  id: eventId(run.threadId),
  type: "run.updated",
  threadId: run.threadId,
  runId: run.id,
  providerInstanceId,
  occurredAt: run.requestedAt,
  payload: run,
});

export const makeRun = (input: {
  readonly threadId: ThreadId;
  readonly id: RunId;
  readonly ordinal: number;
  readonly status: OrchestrationV2Run["status"];
  readonly providerThreadId: ProviderThreadId;
  readonly rootNodeId: NodeId | null;
  readonly activeAttemptId: RunAttemptId | null;
  readonly now: DateTime.Utc;
  readonly queueHeld?: boolean;
}): OrchestrationV2Run => {
  const ended = !["queued", "preparing", "starting", "running", "waiting"].includes(input.status);
  return {
    id: input.id,
    threadId: input.threadId,
    ordinal: input.ordinal,
    providerInstanceId,
    modelSelection,
    providerThreadId: input.providerThreadId,
    userMessageId: MessageId.make(`message:${input.id}`),
    rootNodeId: input.rootNodeId,
    activeAttemptId: input.activeAttemptId,
    status: input.status,
    ...(input.queueHeld === undefined ? {} : { queueHeld: input.queueHeld, queuePosition: 1 }),
    requestedAt: input.now,
    startedAt: input.status === "queued" ? null : input.now,
    completedAt: ended ? input.now : null,
    checkpointId: null,
    contextHandoffId: null,
  };
};

export const attemptCreated = (input: {
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly id: RunAttemptId;
  readonly rootNodeId: NodeId;
  readonly providerThreadId: ProviderThreadId;
  readonly providerTurnId: ProviderTurnId | null;
  readonly status: "pending" | "running" | "completed" | "interrupted" | "failed" | "cancelled";
  readonly now: DateTime.Utc;
}): OrchestrationV2DomainEvent => ({
  id: eventId(input.threadId),
  type: "run-attempt.created",
  threadId: input.threadId,
  runId: input.runId,
  nodeId: input.rootNodeId,
  providerInstanceId,
  occurredAt: input.now,
  payload: {
    id: input.id,
    runId: input.runId,
    attemptOrdinal: 1,
    rootNodeId: input.rootNodeId,
    providerInstanceId,
    providerThreadId: input.providerThreadId,
    providerTurnId: input.providerTurnId,
    reason: "initial",
    status: input.status,
    startedAt: input.now,
    completedAt: input.status === "pending" || input.status === "running" ? null : input.now,
  },
});

export const makeNode = (input: {
  readonly threadId: ThreadId;
  readonly id: NodeId;
  readonly runId: RunId | null;
  readonly rootNodeId: NodeId;
  readonly kind: OrchestrationV2ExecutionNode["kind"];
  readonly status: OrchestrationV2ExecutionNode["status"];
  readonly providerThreadId: ProviderThreadId;
  readonly providerTurnId: ProviderTurnId | null;
  readonly runtimeRequestId?: RuntimeRequestId;
  readonly now: DateTime.Utc;
}): OrchestrationV2ExecutionNode => ({
  id: input.id,
  threadId: input.threadId,
  runId: input.runId,
  parentNodeId: input.id === input.rootNodeId ? null : input.rootNodeId,
  rootNodeId: input.rootNodeId,
  kind: input.kind,
  status: input.status,
  countsForRun: input.kind === "root_turn",
  providerThreadId: input.providerThreadId,
  providerTurnId: input.providerTurnId,
  nativeItemRef: null,
  runtimeRequestId: input.runtimeRequestId ?? null,
  checkpointScopeId: null,
  startedAt: input.now,
  completedAt: ["pending", "running", "waiting"].includes(input.status) ? null : input.now,
});

export const nodeUpdated = (node: OrchestrationV2ExecutionNode): OrchestrationV2DomainEvent => ({
  id: eventId(node.threadId),
  type: "node.updated",
  threadId: node.threadId,
  ...(node.runId === null ? {} : { runId: node.runId }),
  nodeId: node.id,
  providerInstanceId,
  occurredAt: node.startedAt!,
  payload: node,
});

export const makeProviderTurn = (input: {
  readonly id: ProviderTurnId;
  readonly providerThreadId: ProviderThreadId;
  readonly nodeId: NodeId;
  readonly runAttemptId: RunAttemptId | null;
  readonly ordinal: number;
  readonly status: OrchestrationV2ProviderTurn["status"];
  readonly now: DateTime.Utc;
}): OrchestrationV2ProviderTurn => ({
  id: input.id,
  providerThreadId: input.providerThreadId,
  nodeId: input.nodeId,
  runAttemptId: input.runAttemptId,
  nativeTurnRef: { driver, nativeId: `native:${input.id}`, strength: "strong" },
  ordinal: input.ordinal,
  status: input.status,
  startedAt: input.now,
  completedAt: input.status === "pending" || input.status === "running" ? null : input.now,
});

export const providerTurnUpdated = (
  threadId: ThreadId,
  turn: OrchestrationV2ProviderTurn,
  runId?: RunId,
): OrchestrationV2DomainEvent => ({
  id: eventId(threadId),
  type: "provider-turn.updated",
  threadId,
  ...(runId === undefined ? {} : { runId }),
  nodeId: turn.nodeId,
  providerInstanceId,
  occurredAt: turn.startedAt!,
  payload: turn,
});

const itemBase = (input: {
  readonly threadId: ThreadId;
  readonly id: TurnItemId;
  readonly runId: RunId | null;
  readonly nodeId: NodeId | null;
  readonly providerThreadId: ProviderThreadId;
  readonly providerTurnId: ProviderTurnId | null;
  readonly ordinal: number;
  readonly status: OrchestrationV2TurnItem["status"];
  readonly now: DateTime.Utc;
}) => ({
  id: input.id,
  threadId: input.threadId,
  runId: input.runId,
  nodeId: input.nodeId,
  providerThreadId: input.providerThreadId,
  providerTurnId: input.providerTurnId,
  nativeItemRef: null,
  parentItemId: null,
  ordinal: input.ordinal,
  status: input.status,
  title: null,
  startedAt: input.now,
  completedAt: ["pending", "running", "waiting"].includes(input.status) ? null : input.now,
  updatedAt: input.now,
});

export const reasoningItem = (input: Parameters<typeof itemBase>[0]): OrchestrationV2TurnItem => ({
  ...itemBase(input),
  type: "reasoning",
  text: "Thinking",
  streaming: input.status === "running",
});

export const todoListItem = (input: Parameters<typeof itemBase>[0]): OrchestrationV2TurnItem => ({
  ...itemBase(input),
  type: "todo_list",
  planId: PlanId.make(`plan:${input.id}`),
  steps: [{ id: "finish", text: "Finish", status: "running" }],
});

export const userInputItem = (
  input: Parameters<typeof itemBase>[0] & { readonly requestId: RuntimeRequestId },
): OrchestrationV2TurnItem => ({
  ...itemBase(input),
  type: "user_input_request",
  requestId: input.requestId,
  questions: [
    {
      id: "choice",
      header: "Choice",
      question: "Which one?",
      options: [{ label: "A", description: "Option A" }],
    },
  ],
  responseMode: "message",
});

export const turnItemUpdated = (item: OrchestrationV2TurnItem): OrchestrationV2DomainEvent => ({
  id: eventId(item.threadId),
  type: "turn-item.updated",
  threadId: item.threadId,
  ...(item.runId === null ? {} : { runId: item.runId }),
  ...(item.nodeId === null ? {} : { nodeId: item.nodeId }),
  providerInstanceId,
  occurredAt: item.updatedAt,
  payload: item,
});

export const asyncQuestionRequest = (input: {
  readonly threadId: ThreadId;
  readonly id: RuntimeRequestId;
  readonly nodeId: NodeId;
  readonly providerTurnId: ProviderTurnId;
  readonly now: DateTime.Utc;
}): OrchestrationV2DomainEvent => ({
  id: eventId(input.threadId),
  type: "runtime-request.updated",
  threadId: input.threadId,
  nodeId: input.nodeId,
  providerInstanceId,
  occurredAt: input.now,
  payload: {
    id: input.id,
    nodeId: input.nodeId,
    providerTurnId: input.providerTurnId,
    nativeRequestRef: null,
    kind: "user_input",
    status: "pending",
    responseCapability: { type: "message" },
    createdAt: input.now,
    resolvedAt: null,
  },
});

/** One thread whose completed run left work open, plus a held queued follow-up. */
export const endedRunThread = (prefix: string, now: DateTime.Utc) => {
  const threadId = ThreadId.make(`${prefix}:thread`);
  const providerThreadId = ProviderThreadId.make(`${prefix}:provider-thread`);
  const runId = RunId.make(`${prefix}:run:ended`);
  const attemptId = RunAttemptId.make(`${prefix}:attempt:ended`);
  const rootNodeId = NodeId.make(`${prefix}:node:root`);
  const providerTurnId = ProviderTurnId.make(`${prefix}:provider-turn`);
  const toolNodeId = NodeId.make(`${prefix}:node:tool`);
  const reasoningNodeId = NodeId.make(`${prefix}:node:reasoning`);
  const reasoningItemId = TurnItemId.make(`${prefix}:item:reasoning`);
  const todoNodeId = NodeId.make(`${prefix}:node:todo`);
  const todoItemId = TurnItemId.make(`${prefix}:item:todo`);
  const questionNodeId = NodeId.make(`${prefix}:node:question`);
  const questionItemId = TurnItemId.make(`${prefix}:item:question`);
  const questionRequestId = RuntimeRequestId.make(`${prefix}:request:question`);
  const heldRunId = RunId.make(`${prefix}:run:held`);
  const heldRootNodeId = NodeId.make(`${prefix}:node:held-root`);
  const common = { threadId, providerThreadId, now };
  const events: Array<OrchestrationV2DomainEvent> = [
    threadCreated(threadId, now, providerThreadId),
    providerThreadUpdated(threadId, providerThreadId, now),
    runUpdated(
      makeRun({
        ...common,
        id: runId,
        ordinal: 1,
        status: "completed",
        rootNodeId,
        activeAttemptId: attemptId,
      }),
    ),
    attemptCreated({
      ...common,
      runId,
      id: attemptId,
      rootNodeId,
      providerTurnId,
      status: "completed",
    }),
    nodeUpdated(
      makeNode({
        ...common,
        id: rootNodeId,
        runId,
        rootNodeId,
        kind: "root_turn",
        status: "completed",
        providerTurnId,
      }),
    ),
    providerTurnUpdated(
      threadId,
      makeProviderTurn({
        ...common,
        id: providerTurnId,
        nodeId: rootNodeId,
        runAttemptId: attemptId,
        ordinal: 1,
        status: "running",
      }),
      runId,
    ),
    nodeUpdated(
      makeNode({
        ...common,
        id: toolNodeId,
        runId,
        rootNodeId,
        kind: "tool_call",
        status: "running",
        providerTurnId,
      }),
    ),
    nodeUpdated(
      makeNode({
        ...common,
        id: reasoningNodeId,
        runId,
        rootNodeId,
        kind: "reasoning",
        status: "running",
        providerTurnId,
      }),
    ),
    turnItemUpdated(
      reasoningItem({
        ...common,
        id: reasoningItemId,
        runId,
        nodeId: reasoningNodeId,
        providerTurnId,
        ordinal: 1,
        status: "running",
      }),
    ),
    nodeUpdated(
      makeNode({
        ...common,
        id: todoNodeId,
        runId,
        rootNodeId,
        kind: "todo_list",
        status: "running",
        providerTurnId,
      }),
    ),
    turnItemUpdated(
      todoListItem({
        ...common,
        id: todoItemId,
        runId,
        nodeId: todoNodeId,
        providerTurnId,
        ordinal: 2,
        status: "running",
      }),
    ),
    nodeUpdated(
      makeNode({
        ...common,
        id: questionNodeId,
        runId,
        rootNodeId,
        kind: "user_input_request",
        status: "waiting",
        providerTurnId,
        runtimeRequestId: questionRequestId,
      }),
    ),
    asyncQuestionRequest({
      threadId,
      id: questionRequestId,
      nodeId: questionNodeId,
      providerTurnId,
      now,
    }),
    turnItemUpdated(
      userInputItem({
        ...common,
        id: questionItemId,
        runId,
        nodeId: questionNodeId,
        providerTurnId,
        ordinal: 3,
        status: "waiting",
        requestId: questionRequestId,
      }),
    ),
    runUpdated(
      makeRun({
        ...common,
        id: heldRunId,
        ordinal: 2,
        status: "queued",
        rootNodeId: heldRootNodeId,
        activeAttemptId: null,
        queueHeld: true,
      }),
    ),
    nodeUpdated(
      makeNode({
        ...common,
        id: heldRootNodeId,
        runId: heldRunId,
        rootNodeId: heldRootNodeId,
        kind: "root_turn",
        status: "pending",
        providerTurnId: null,
      }),
    ),
  ];
  return {
    events,
    threadId,
    orphaned: {
      providerTurnIds: [providerTurnId],
      nodeIds: [toolNodeId, reasoningNodeId],
      turnItemIds: [reasoningItemId],
    },
    preserved: {
      runIds: [heldRunId],
      nodeIds: [todoNodeId, questionNodeId, heldRootNodeId],
      turnItemIds: [todoItemId, questionItemId],
      runtimeRequestIds: [questionRequestId],
    },
  };
};

/** A provider-native subagent thread: runless root turn ended, its provider turn did not. */
export const runlessSubagentThread = (prefix: string, now: DateTime.Utc) => {
  const threadId = ThreadId.make(`${prefix}:thread`);
  const providerThreadId = ProviderThreadId.make(`${prefix}:provider-thread`);
  const rootNodeId = NodeId.make(`${prefix}:node:root`);
  const providerTurnId = ProviderTurnId.make(`${prefix}:provider-turn`);
  const reasoningNodeId = NodeId.make(`${prefix}:node:reasoning`);
  const common = { threadId, providerThreadId, now };
  const events: Array<OrchestrationV2DomainEvent> = [
    threadCreated(threadId, now, providerThreadId),
    providerThreadUpdated(threadId, providerThreadId, now),
    nodeUpdated(
      makeNode({
        ...common,
        id: rootNodeId,
        runId: null,
        rootNodeId,
        kind: "root_turn",
        status: "cancelled",
        providerTurnId,
      }),
    ),
    providerTurnUpdated(
      threadId,
      makeProviderTurn({
        ...common,
        id: providerTurnId,
        nodeId: rootNodeId,
        runAttemptId: null,
        ordinal: 1,
        status: "running",
      }),
    ),
    nodeUpdated(
      makeNode({
        ...common,
        id: reasoningNodeId,
        runId: null,
        rootNodeId,
        kind: "reasoning",
        status: "running",
        providerTurnId,
      }),
    ),
  ];
  return {
    events,
    threadId,
    orphaned: { providerTurnIds: [providerTurnId], nodeIds: [reasoningNodeId], turnItemIds: [] },
  };
};
