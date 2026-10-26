import {
  CommandId,
  ContextTransferId,
  EnvironmentId,
  EventId,
  MessageId,
  NodeId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
  type OrchestrationV2Subagent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { derive } from "@t3tools/provider-core/server/IdAllocator";
import { idleThreadProjection, liveThreadShell } from "../../mcp/McpToolAccess.testkit.ts";
import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import type { TaskContinuationRecord } from "./TaskContinuationStore.ts";

export function fixture(legacyInitialRun = false) {
  const now = DateTime.makeUnsafe("2026-10-25T20:00:00.000Z");
  const instanceId = ProviderInstanceId.make("claude");
  const modelSelection = {
    instanceId,
    model: "claude-opus-5-5",
    options: [{ id: "effort", value: "max" }],
  };
  const parentId = ThreadId.make("parent");
  const commandId = CommandId.make("command:mcp:fixture:delegate-task:source");
  const childId = derive.delegatedTaskThread({ commandId });
  const taskId = derive.delegatedTaskNode({ commandId });
  const providerId = ProviderThreadId.make("claude-row");
  const parent = idleThreadProjection(liveThreadShell(parentId));
  const child = idleThreadProjection(liveThreadShell(childId, { activeRunId: null }));
  const run: OrchestrationV2Run = {
    id: RunId.make("source-run"),
    threadId: childId,
    ordinal: 1,
    providerInstanceId: instanceId,
    modelSelection,
    providerThreadId: providerId,
    userMessageId: derive.delegatedTaskMessage({ commandId }),
    rootNodeId: NodeId.make("source-root"),
    activeAttemptId: RunAttemptId.make("source-attempt"),
    status: "completed",
    requestedAt: now,
    startedAt: now,
    completedAt: now,
    checkpointId: null,
    contextHandoffId: null,
    ...(legacyInitialRun ? {} : { delegatedTaskId: taskId }),
  };
  const attempt: OrchestrationV2RunAttempt = {
    id: RunAttemptId.make("source-attempt"),
    runId: run.id,
    attemptOrdinal: 1,
    rootNodeId: NodeId.make("source-root"),
    providerInstanceId: instanceId,
    providerThreadId: providerId,
    providerTurnId: ProviderTurnId.make("source-turn"),
    nativeThreadId: "original-native",
    reason: "initial",
    status: "completed",
    startedAt: now,
    completedAt: now,
  };
  const task: OrchestrationV2Subagent = {
    id: taskId,
    threadId: parentId,
    runId: RunId.make("parent-run"),
    parentNodeId: NodeId.make("parent-root"),
    origin: "app_owned",
    createdBy: "agent",
    driver: ProviderDriverKind.make("claudeAgent"),
    providerInstanceId: instanceId,
    providerThreadId: null,
    childThreadId: childId,
    nativeTaskRef: null,
    prompt: "first",
    title: "Child",
    model: modelSelection.model,
    modelSelection,
    status: "completed",
    result: "Immutable original result",
    startedAt: now,
    completedAt: now,
    updatedAt: now,
  };
  const provider: OrchestrationV2ProviderThread = {
    id: providerId,
    driver: ProviderDriverKind.make("claudeAgent"),
    providerInstanceId: instanceId,
    providerSessionId: null,
    appThreadId: childId,
    ownerNodeId: null,
    nativeThreadRef: {
      driver: ProviderDriverKind.make("claudeAgent"),
      nativeId: "original-native",
      strength: "strong",
    },
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: 1,
    lastRunOrdinal: 1,
    handoffIds: [],
    forkedFrom: null,
    pendingBackgroundTasks: [],
    contextUsage: null,
    nativeMetadata: null,
    goal: null,
    createdAt: now,
    updatedAt: now,
  };
  const { delegatedTaskId: _sourceTaskId, ...parentRunBase } = run;
  const parentRun: OrchestrationV2Run = {
    ...parentRunBase,
    id: RunId.make("parent-run"),
    threadId: parentId,
    providerInstanceId: parent.thread.providerInstanceId,
    modelSelection: parent.thread.modelSelection,
    providerThreadId: null,
    userMessageId: MessageId.make("parent-message"),
    rootNodeId: NodeId.make("parent-root"),
    activeAttemptId: null,
    status: "running",
    completedAt: null,
  };
  const source = {
    ...child,
    thread: {
      ...child.thread,
      providerInstanceId: instanceId,
      modelSelection,
      activeProviderThreadId: providerId,
      lineage: {
        parentThreadId: parentId,
        relationshipToParent: "subagent" as const,
        rootThreadId: parentId,
      },
      forkedFrom: { type: "node" as const, nodeId: taskId },
    },
    runs: [run],
    attempts: [attempt],
    providerThreads: [provider],
    providerTurns: [
      {
        id: ProviderTurnId.make("source-turn"),
        providerThreadId: providerId,
        runAttemptId: attempt.id,
        nodeId: attempt.rootNodeId,
        ordinal: 1,
        status: "completed" as const,
        nativeTurnRef: null,
        startedAt: now,
        completedAt: now,
      },
    ],
  };
  const owner = { ...parent, runs: [parentRun], subagents: [task] };
  const spawnTransfer = {
    id: ContextTransferId.make("spawn-transfer"),
    type: "subagent_spawn" as const,
    sourceThreadId: parentId,
    targetThreadId: childId,
    sourcePoint: { threadId: parentId, runId: parentRun.id },
    basePoint: null,
    sourceProviderInstanceId: parentRun.providerInstanceId,
    targetProviderInstanceId: instanceId,
    targetRunId: run.id,
    status: "consumed" as const,
    resolution: null,
    createdBy: "agent" as const,
    error: null,
    createdAt: now,
    updatedAt: now,
    consumedAt: now,
  };
  const scope: McpInvocationScope = {
    environmentId: EnvironmentId.make("env"),
    issuedAt: 1,
    capabilities: new Set(["orchestration"]),
    requestNamespace: "old-parent-session",
    thread: {
      threadId: parentId,
      providerSessionId: "mcp-parent",
      providerInstanceId: parent.thread.providerInstanceId,
    },
    client: undefined,
  };
  const row: TaskContinuationRecord = {
    messageId: MessageId.make("message:jones:task-continue:test"),
    commandId: CommandId.make("continue-command"),
    runId: RunId.make("continued-run"),
    parentThreadId: parentId,
    childThreadId: childId,
    taskId: NodeId.make("continued-task"),
    sourceTaskId: taskId,
    sourceRunId: run.id,
    sourceAttemptId: attempt.id,
    sourceEvidence: "attempt",
    providerInstanceId: instanceId,
    providerThreadId: providerId,
    nativeSessionId: "original-native",
    modelSelection,
    requestDigest: "digest",
    acceptedAt: DateTime.formatIso(now),
  };
  let serial = 0;
  const event = <T extends OrchestrationV2DomainEvent["type"]>(
    type: T,
    threadId: ThreadId,
    payload: Extract<OrchestrationV2DomainEvent, { type: T }>["payload"],
  ): Extract<OrchestrationV2DomainEvent, { type: T }> =>
    ({
      id: EventId.make(`seed:${serial++}`),
      type,
      threadId,
      occurredAt: now,
      payload,
    }) as Extract<OrchestrationV2DomainEvent, { type: T }>;
  return {
    now,
    parent: owner,
    child: source,
    task,
    run,
    attempt,
    provider,
    row,
    scope,
    event,
    spawnTransfer,
  };
}
