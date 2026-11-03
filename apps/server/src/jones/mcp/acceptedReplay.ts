import {
  type OrchestrationV2Command,
  type MessageId,
  type ThreadId,
  OrchestratorMcpFailure,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import type * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import type * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";

type ReplayCommand = Extract<
  OrchestrationV2Command,
  { readonly type: "thread.create" | "message.dispatch" | "delegated_task.request" }
>;

const invalidReplay = () =>
  new OrchestratorMcpFailure({
    code: "orchestration_error",
    message: "The recorded command outcome cannot be reconstructed. Retry the same key unchanged.",
  });

/** Read errors and rejected receipts never authorize a fresh admission. */
export const acceptedReplay = Effect.fn("jones.mcp.acceptedReplay")(function* (
  threads: Pick<
    ThreadManagement.ThreadManagementService["Service"],
    "getCommandReceipt" | "dispatch"
  >,
  command: ReplayCommand,
) {
  const receipt = yield* threads
    .getCommandReceipt(command.commandId)
    .pipe(Effect.mapError(invalidReplay));
  if (Option.isNone(receipt)) return undefined;
  const threadId =
    command.type === "delegated_task.request" ? command.parentThreadId : command.threadId;
  if (
    receipt.value.commandId !== command.commandId ||
    receipt.value.commandType !== command.type ||
    receipt.value.threadId !== threadId
  ) {
    return yield* invalidReplay();
  }
  if (receipt.value.status === "rejected")
    return yield* new OrchestratorMcpFailure({
      code: "orchestration_error",
      message: "This command key has an authoritative rejection and cannot start revised work.",
    });

  // Accepted receipts are immutable. Reusing that exact ID/aggregate reads its
  // events through the orchestrator's receipt path, before any current catalog work.
  // This is command-key replay, not a whole-payload equality guarantee.
  const result = yield* threads.dispatch(command).pipe(Effect.mapError(invalidReplay));
  if (
    result.storedEvents.length === 0 ||
    result.storedEvents.some((stored) => stored.commandId !== command.commandId)
  ) {
    return yield* invalidReplay();
  }
  return result;
});

export const recordedCreation = Effect.fn("jones.mcp.recordedCreation")(function* (
  result: Orchestrator.OrchestratorV2DispatchResult,
  threadId: ThreadId,
) {
  const event = result.storedEvents.find(
    (stored) => stored.event.type === "thread.created" && stored.event.threadId === threadId,
  )?.event;
  if (event?.type !== "thread.created" || event.payload.id !== threadId)
    return yield* invalidReplay();
  return event.payload;
});

export const recordedDelegation = Effect.fn("jones.mcp.recordedDelegation")(function* (
  result: Orchestrator.OrchestratorV2DispatchResult,
  parentThreadId: ThreadId,
) {
  const event = result.storedEvents.find(
    (stored) =>
      stored.event.type === "subagent.updated" &&
      stored.event.threadId === parentThreadId &&
      stored.event.payload.origin === "app_owned",
  )?.event;
  if (
    event?.type !== "subagent.updated" ||
    event.payload.threadId !== parentThreadId ||
    event.payload.childThreadId === null
  ) {
    return yield* invalidReplay();
  }
  const task = event.payload;
  const child = yield* recordedCreation(result, task.childThreadId!);
  if (
    child.lineage.parentThreadId !== parentThreadId ||
    child.forkedFrom?.type !== "node" ||
    child.forkedFrom.nodeId !== task.id ||
    child.lineage.relationshipToParent !== "subagent" ||
    child.modelSelection.instanceId !== task.providerInstanceId ||
    child.modelSelection.model !== task.model
  ) {
    return yield* invalidReplay();
  }
  return { task, child };
});

export const recordedMessage = Effect.fn("jones.mcp.recordedMessage")(function* (
  result: Orchestrator.OrchestratorV2DispatchResult,
  threadId: ThreadId,
  messageId: MessageId,
  senderThreadId: ThreadId,
) {
  if (
    !result.storedEvents.some(
      (stored) =>
        stored.event.type === "message.updated" &&
        stored.event.threadId === threadId &&
        stored.event.payload.id === messageId &&
        stored.event.payload.senderThreadId === senderThreadId,
    )
  )
    return yield* invalidReplay();
});
