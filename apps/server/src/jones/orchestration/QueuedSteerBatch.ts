import type {
  OrchestrationV2Command,
  OrchestrationV2ThreadProjection,
  OrchestrationV2DomainEvent,
  TurnItemId,
  MessageId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { modelSelectionsEqual } from "@t3tools/shared/model";
import { planQueuedSteerBatch } from "@t3tools/shared/jones/queuedSteerBatch";
import { queuedRunsInDeliveryOrder } from "../../orchestration-v2/QueuedRunOrder.ts";

export type QueuedSteerBatchCommand = Extract<
  OrchestrationV2Command,
  { type: "queued-message.promote-batch-to-steer" }
>;
type Projection = Pick<
  OrchestrationV2ThreadProjection,
  | "thread"
  | "runs"
  | "messages"
  | "nodes"
  | "attempts"
  | "providerThreads"
  | "providerSessions"
  | "providerTurns"
  | "runtimeRequests"
>;

export function queuedSteerBatchCandidates(
  projection: Projection,
  targetRunId: QueuedSteerBatchCommand["targetRunId"],
  skipHeld = false,
) {
  const target = projection.runs.find((run) => run.id === targetRunId);
  return queuedRunsInDeliveryOrder(projection).flatMap((run) => {
    if (skipHeld && run.queueHeld === true) return [];
    const message = projection.messages.find((candidate) => candidate.id === run.userMessageId);
    if (message?.delegatedCompletion !== undefined || message?.notification !== undefined)
      return [];
    const root = projection.nodes.find((node) => node.id === run.rootNodeId);
    const attempt = projection.attempts.find((candidate) => candidate.id === run.activeAttemptId);
    return [
      {
        run,
        message,
        root,
        attempt,
        eligible:
          target !== undefined &&
          message !== undefined &&
          root !== undefined &&
          attempt !== undefined &&
          run.sourcePlanRef === undefined &&
          run.restartContinuationOfRunId === undefined &&
          run.providerThreadId === target.providerThreadId &&
          run.providerInstanceId === target.providerInstanceId &&
          modelSelectionsEqual(run.modelSelection, target.modelSelection) &&
          !/^\/(?:compact|logout|goal)(?:\s|$)/iu.test(message.text.trim()),
      },
    ];
  });
}

export function validateQueuedSteerBatch(
  projection: Projection,
  command: QueuedSteerBatchCommand,
  skipHeld = false,
) {
  if (projection.thread.archivedAt !== null || projection.thread.deletedAt !== null)
    return { error: "Thread is not active." } as const;
  const target = projection.runs.find((run) => run.id === command.targetRunId);
  const provider = projection.providerThreads.find((row) => row.id === target?.providerThreadId);
  const session = projection.providerSessions.find((row) => row.id === provider?.providerSessionId);
  const turn = projection.providerTurns.find(
    (row) => row.runAttemptId === target?.activeAttemptId && row.status === "running",
  );
  if (
    target?.status !== "running" ||
    target.rootNodeId === null ||
    provider === undefined ||
    session === undefined ||
    turn === undefined ||
    turn.providerThreadId !== provider.id ||
    projection.thread.activeProviderThreadId !== provider.id ||
    projection.thread.providerInstanceId !== target.providerInstanceId ||
    !modelSelectionsEqual(projection.thread.modelSelection, target.modelSelection) ||
    !["codex", "claudeAgent"].includes(session.driver) ||
    session.capabilities.turns.supportsActiveSteering !== true ||
    !["ready", "running"].includes(session.status)
  )
    return { error: "Batch steering requires an unchanged active Codex or Claude turn." } as const;
  const activeMessage = projection.messages.find((message) => message.id === target.userMessageId);
  if (
    activeMessage !== undefined &&
    /^\/(?:compact|logout|goal)(?:\s|$)/iu.test(activeMessage.text.trim())
  )
    return { error: "Wait for the active command to finish." } as const;
  if (projection.runtimeRequests.some((request) => request.status === "pending"))
    return { error: "Resolve the pending request before steering queued messages." } as const;
  const candidates = queuedSteerBatchCandidates(projection, target.id, skipHeld);
  const selected = candidates.slice(0, command.items.length);
  if (
    selected.length !== command.items.length ||
    new Set(command.items.map((item) => item.queuedRunId)).size !== command.items.length ||
    selected.some(
      (entry, index) =>
        entry.run.id !== command.items[index]!.queuedRunId ||
        entry.message?.id !== command.items[index]!.messageId ||
        DateTime.formatIso(entry.message.updatedAt) !== command.items[index]!.messageUpdatedAt,
    )
  )
    return { error: "stale_or_edited: refresh the queue and retry its leading messages." } as const;
  if (selected.some((entry) => !entry.eligible))
    return { error: "This queue prefix requires individual delivery." } as const;
  const plan = planQueuedSteerBatch(
    selected.map((entry) => ({ message: entry.message!, eligible: entry.eligible })),
  );
  if (plan.count !== command.items.length)
    return {
      error: `${plan.reason}: ${plan.count} messages fit; use a smaller batch or the first row's Steer action.`,
    } as const;
  return {
    target,
    rootNodeId: target.rootNodeId,
    provider,
    session,
    turn,
    selected,
    plan,
  } as const;
}

type BatchEvent = {
  [K in OrchestrationV2DomainEvent["type"]]: Omit<
    Extract<OrchestrationV2DomainEvent, { type: K }>,
    "id"
  >;
}[OrchestrationV2DomainEvent["type"]];
export function queuedSteerBatchEvents(
  batch: Exclude<ReturnType<typeof validateQueuedSteerBatch>, { readonly error: string }>,
  command: QueuedSteerBatchCommand,
  now: DateTime.Utc,
  ordinal: number,
  turnItemId: (messageId: MessageId) => TurnItemId,
): ReadonlyArray<BatchEvent> {
  const events: Array<BatchEvent> = [];
  for (const [index, entry] of batch.selected.entries()) {
    const { run, root, attempt, message } = entry;
    events.push({
      type: "run.updated",
      threadId: command.threadId,
      runId: run.id,
      nodeId: root!.id,
      providerInstanceId: run.providerInstanceId,
      occurredAt: now,
      payload: { ...run, status: "cancelled", queuePosition: null, completedAt: now },
    });
    events.push({
      type: "run-attempt.updated",
      threadId: command.threadId,
      runId: run.id,
      nodeId: root!.id,
      providerInstanceId: run.providerInstanceId,
      occurredAt: now,
      payload: { ...attempt!, status: "cancelled", completedAt: now },
    });
    events.push({
      type: "node.updated",
      threadId: command.threadId,
      runId: run.id,
      nodeId: root!.id,
      providerInstanceId: run.providerInstanceId,
      occurredAt: now,
      payload: { ...root!, status: "cancelled", completedAt: now },
    });
    const moved = {
      ...message!,
      runId: batch.target.id,
      nodeId: batch.rootNodeId,
      updatedAt: now,
    };
    events.push({
      type: "message.updated",
      threadId: command.threadId,
      runId: batch.target.id,
      nodeId: batch.rootNodeId,
      providerInstanceId: batch.target.providerInstanceId,
      occurredAt: now,
      payload: moved,
    });
    events.push({
      type: "turn-item.updated",
      threadId: command.threadId,
      runId: batch.target.id,
      nodeId: batch.rootNodeId,
      providerInstanceId: batch.target.providerInstanceId,
      occurredAt: now,
      payload: {
        id: turnItemId(message!.id),
        threadId: command.threadId,
        runId: batch.target.id,
        nodeId: batch.rootNodeId,
        providerThreadId: batch.provider.id,
        providerTurnId: batch.turn.id,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: ordinal + index,
        status: "completed",
        title: null,
        startedAt: now,
        completedAt: now,
        updatedAt: now,
        type: "user_message",
        messageId: message!.id,
        inputIntent: "promoted_queued_to_steer",
        text: message!.text,
        attachments: message!.attachments,
        createdBy: message!.createdBy,
        creationSource: message!.creationSource,
        ...(message!.context ? { context: message!.context } : {}),
        ...(message!.senderThreadId === undefined
          ? {}
          : { senderThreadId: message!.senderThreadId }),
        ...(message!.scheduledTaskId === undefined
          ? {}
          : { scheduledTaskId: message!.scheduledTaskId }),
      },
    });
  }
  return events;
}
