import {
  type CommandId,
  type ExecutionEnvironmentCapabilities,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import { planQueuedSteerBatch } from "@t3tools/shared/jones/queuedSteerBatch";
import { modelSelectionsEqual } from "@t3tools/shared/model";
import * as DateTime from "effect/DateTime";
import { deriveThreadQueueWorkflowState } from "../../state/threadWorkflows.ts";
import type { PromoteQueuedBatchInput } from "./commands.ts";

export type QueuedSteerBatchCapability = ExecutionEnvironmentCapabilities["queuedSteerBatch"];

export function deriveQueuedSteerBatch(input: {
  readonly projection: OrchestrationV2ThreadProjection | null | undefined;
  readonly capability: QueuedSteerBatchCapability;
  readonly editing: boolean;
  readonly hasUnsavedMessages: boolean;
  readonly busy: boolean;
}) {
  const projection = input.projection;
  if (!projection || !input.capability) return null;
  const workflow = deriveThreadQueueWorkflowState(projection);
  const active = workflow.activeRun;
  const providerThread = projection.providerThreads.find(
    (thread) => thread.id === active?.providerThreadId,
  );
  const session = projection.providerSessions.find(
    (candidate) => candidate.id === providerThread?.providerSessionId,
  );
  const supportsActiveBatch =
    session?.capabilities.turns.supportsActiveSteering === true &&
    (session.driver === "codex" || session.driver === "claudeAgent");

  const activeMessage = projection.messages.find((message) => message.id === active?.userMessageId);
  const specialCommand = (text: string) => /^\/(?:compact|logout|goal)(?:\s|$)/iu.test(text.trim());
  const candidates = workflow.queuedRuns.map(({ run }) => {
    const message = projection.messages.find((item) => item.id === run.userMessageId);
    return {
      run,
      message,
      eligible:
        message !== undefined &&
        active !== null &&
        !specialCommand(message.text) &&
        run.sourcePlanRef === undefined &&
        run.restartContinuationOfRunId === undefined &&
        run.providerThreadId === active.providerThreadId &&
        run.providerInstanceId === active.providerInstanceId &&
        projection.nodes.some((node) => node.id === run.rootNodeId) &&
        projection.attempts.some((attempt) => attempt.id === run.activeAttemptId) &&
        modelSelectionsEqual(run.modelSelection, active.modelSelection) &&
        modelSelectionsEqual(run.modelSelection, projection.thread.modelSelection),
    };
  });
  const plan = planQueuedSteerBatch(
    candidates.map(({ message, eligible }) => ({
      eligible,
      message: message ?? { text: "", attachments: [], createdBy: "user", creationSource: "web" },
    })),
    input.capability,
  );
  const items = candidates.slice(0, plan.count).flatMap(({ run, message }) =>
    message
      ? [
          {
            queuedRunId: run.id,
            messageId: message.id,
            messageUpdatedAt: DateTime.formatIso(message.updatedAt),
          },
        ]
      : [],
  );
  const reason = input.busy
    ? "Wait for the current queue action to finish."
    : input.editing
      ? "Finish editing the queued message first."
      : input.hasUnsavedMessages
        ? "Wait for queued messages to finish saving."
        : projection.runtimeRequests.some((request) => request.status === "pending")
          ? "Resolve the pending approval or question first."
          : projection.thread.archivedAt !== null || projection.thread.deletedAt !== null
            ? "This thread is no longer active."
            : !workflow.canPromoteToSteer || active?.status !== "running"
              ? "There is no active provider turn to steer."
              : !supportsActiveBatch || !session || !["ready", "running"].includes(session.status)
                ? "Batch steering requires an active Codex or Claude steering session."
                : active.rootNodeId === null ||
                    projection.thread.activeProviderThreadId !== providerThread?.id ||
                    projection.thread.providerInstanceId !== active.providerInstanceId ||
                    !modelSelectionsEqual(projection.thread.modelSelection, active.modelSelection)
                  ? "The active provider changed. Wait for the thread to synchronize."
                  : activeMessage && specialCommand(activeMessage.text)
                    ? "Wait for the current maintenance command to finish."
                    : plan.count === 0
                      ? plan.reason === "first_member_over_budget"
                        ? "The first message exceeds the batch limit. Edit it or use its individual Steer action."
                        : "The first queued message needs its own turn. Use its individual action."
                      : null;
  const detail =
    plan.remainingCount > 0
      ? `${plan.remainingCount} still queued — ${plan.reason === "ineligible" ? "the next message needs its own turn" : "batch size limit"}.`
      : "Send the queued messages together as one steering input.";
  return {
    targetRunId: active?.id ?? null,
    items,
    count: plan.count,
    remainingCount: plan.remainingCount,
    label:
      plan.count === 0
        ? `Steer all (${candidates.length})`
        : plan.remainingCount === 0
          ? `Steer all (${plan.count})`
          : `Steer next ${plan.count}`,
    reason,
    detail,
  };
}

export type QueuedSteerBatchView = NonNullable<ReturnType<typeof deriveQueuedSteerBatch>>;

export function createQueuedSteerBatchSubmission(allocateId: () => CommandId) {
  let pending = false;
  let retry: PromoteQueuedBatchInput | null = null;
  let accepted: PromoteQueuedBatchInput | null = null;
  return {
    get pending() {
      return pending;
    },
    get retry() {
      return retry;
    },
    awaitingProjection(items: QueuedSteerBatchView["items"]) {
      return (
        accepted !== null &&
        items.some((item) => accepted!.items.some((sent) => sent.messageId === item.messageId))
      );
    },
    async submit(
      input: Omit<PromoteQueuedBatchInput, "commandId">,
      dispatch: (command: PromoteQueuedBatchInput) => Promise<{ readonly _tag: string }>,
    ): Promise<"success" | "failure" | "pending"> {
      if (pending) return "pending";
      const command = retry ?? { ...input, commandId: allocateId() };
      pending = true;
      try {
        const result = await dispatch(command);
        if (result._tag === "Success") {
          accepted = command;
          retry = null;
          return "success";
        }
        retry = command;
        return "failure";
      } catch {
        retry = command;
        return "failure";
      } finally {
        pending = false;
      }
    },
  };
}
