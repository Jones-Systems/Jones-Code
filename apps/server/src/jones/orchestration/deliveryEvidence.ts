import type {
  CommandId,
  OrchestrationV2Command,
  OrchestrationV2ThreadLaunchInput,
  ThreadId,
} from "@t3tools/contracts";
import {
  type DispatchDeliveryEvidence,
  type LaunchDeliveryEvidence,
  initialMessageCommandId,
} from "@t3tools/contracts/jones/deliveryEvidence";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import type * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import type * as EventSink from "../../orchestration-v2/EventSink.ts";
import type {
  RecordedAppThread,
  RecordedStoredEvent,
} from "../../orchestration-v2/RecordedTypes.ts";

type ReceiptReader = Pick<Receipts.CommandReceiptStoreV2["Service"], "getByCommandId">;
type EventReader = Pick<EventSink.EventSinkV2["Service"], "readByCommandId">;
export type LaunchDeliveryRequest = Pick<
  OrchestrationV2ThreadLaunchInput,
  "commandId" | "projectId" | "threadId" | "reuseExistingThread"
> & {
  readonly initialMessage?:
    | Pick<NonNullable<OrchestrationV2ThreadLaunchInput["initialMessage"]>, "messageId">
    | undefined;
};

const receiptMatches = (
  receipt: Receipts.CommandReceiptV2,
  commandId: CommandId,
  commandType: string,
  threadId: ThreadId | undefined,
) =>
  receipt.commandId === commandId &&
  receipt.commandType === commandType &&
  (threadId === undefined || receipt.threadId === threadId);

const eventsMatch = (
  receipt: Receipts.CommandReceiptV2,
  events: ReadonlyArray<RecordedStoredEvent>,
) =>
  events.length > 0 &&
  events.at(-1)?.sequence === receipt.resultSequence &&
  events.every(
    (stored) => stored.commandId === receipt.commandId && stored.sequence <= receipt.resultSequence,
  );

/** Only the original command's immutable claim can establish its project and target. */
export function recordedLaunchClaim(
  input: LaunchDeliveryRequest,
  receipt: Receipts.CommandReceiptV2,
  events: ReadonlyArray<RecordedStoredEvent>,
): RecordedAppThread | undefined {
  const commandType =
    input.reuseExistingThread === true ? "thread.metadata.update" : "thread.create";
  if (
    receipt.status !== "accepted" ||
    !receiptMatches(receipt, input.commandId, commandType, input.threadId) ||
    !eventsMatch(receipt, events)
  )
    return;
  const eventType =
    input.reuseExistingThread === true ? "thread.metadata-updated" : "thread.created";
  const event = events.find(
    ({ event }) => event.type === eventType && event.threadId === receipt.threadId,
  )?.event;
  if (
    (event?.type === "thread.created" || event?.type === "thread.metadata-updated") &&
    event.payload.id === receipt.threadId &&
    event.payload.projectId === input.projectId
  )
    return event.payload;
}

/** This request's first user message and the run it created, among one command's own events. */
function firstMessageIn(
  input: LaunchDeliveryRequest,
  events: ReadonlyArray<RecordedStoredEvent>,
  threadId: ThreadId,
) {
  const event = events.find(
    ({ event }) =>
      event.type === "message.updated" &&
      event.threadId === threadId &&
      event.payload.role === "user" &&
      (input.initialMessage?.messageId === undefined ||
        event.payload.id === input.initialMessage.messageId),
  )?.event;
  if (event?.type !== "message.updated") return;
  const run = events.find(
    ({ event: candidate }) =>
      candidate.type === "run.created" &&
      candidate.threadId === threadId &&
      candidate.payload.userMessageId === event.payload.id,
  )?.event;
  if (run?.type === "run.created") return { message: event.payload, run: run.payload };
}

/** A shell or a receipt for another message is never evidence for this first prompt. */
export function recordedLaunchMessage(
  input: LaunchDeliveryRequest,
  receipt: Receipts.CommandReceiptV2,
  events: ReadonlyArray<RecordedStoredEvent>,
  threadId: ThreadId,
) {
  if (
    receipt.status !== "accepted" ||
    !receiptMatches(
      receipt,
      initialMessageCommandId(input.commandId),
      "message.dispatch",
      threadId,
    ) ||
    !eventsMatch(receipt, events)
  )
    return;
  return firstMessageIn(input, events, threadId);
}

/** Whether a new thread's own claim recorded any first prompt or run, matching or not. */
function claimCarriesFirstMessage(
  input: LaunchDeliveryRequest,
  events: ReadonlyArray<RecordedStoredEvent>,
  threadId: ThreadId,
) {
  return (
    input.reuseExistingThread !== true &&
    events.some(
      ({ event }) =>
        (event.type === "run.created" && event.threadId === threadId) ||
        (event.type === "message.updated" &&
          event.threadId === threadId &&
          event.payload.role === "user"),
    )
  );
}

/**
 * A new thread's `thread.create` claim can carry its first prompt in the same original
 * command. Only that claim's exact accepted receipt and its own recorded events prove
 * it; a reused-thread claim never carries one.
 */
function recordedClaimMessage(
  input: LaunchDeliveryRequest,
  receipt: Receipts.CommandReceiptV2,
  events: ReadonlyArray<RecordedStoredEvent>,
  threadId: ThreadId,
) {
  if (
    input.reuseExistingThread === true ||
    recordedLaunchClaim(input, receipt, events)?.id !== threadId
  )
    return;
  return firstMessageIn(input, events, threadId);
}

/** Read the two possible carriers of the first message before admitting a retry. */
export const readLaunchMessage = Effect.fn("jones.orchestration.readLaunchMessage")(function* (
  receipts: ReceiptReader,
  events: EventReader,
  input: LaunchDeliveryRequest,
  claimReceipt: Receipts.CommandReceiptV2,
  claimEvents: ReadonlyArray<RecordedStoredEvent>,
) {
  const commandId = initialMessageCommandId(input.commandId);
  const threadId = claimReceipt.threadId;
  const separate = yield* receipts.getByCommandId(commandId);
  if (claimCarriesFirstMessage(input, claimEvents, threadId)) {
    const match =
      Option.isNone(separate) && recordedClaimMessage(input, claimReceipt, claimEvents, threadId);
    return match
      ? {
          status: "accepted" as const,
          ...match,
          storedEvents: claimEvents,
          sequence: claimReceipt.resultSequence,
          replayed: true,
        }
      : { status: "conflict" as const };
  }
  if (Option.isNone(separate)) return { status: "absent" as const };
  const receipt = separate.value;
  if (!receiptMatches(receipt, commandId, "message.dispatch", threadId))
    return { status: "conflict" as const };
  if (receipt.status === "rejected") return { status: "rejected" as const, receipt };
  const storedEvents = yield* events.readByCommandId({ commandId }).pipe(Stream.runCollect);
  const match = recordedLaunchMessage(input, receipt, storedEvents, threadId);
  return match
    ? {
        status: "accepted" as const,
        ...match,
        storedEvents,
        sequence: receipt.resultSequence,
        replayed: true,
      }
    : { status: "conflict" as const };
});

export const dispatchDeliveryEvidence = Effect.fn("jones.orchestration.dispatchDeliveryEvidence")(
  function* (receipts: ReceiptReader, command: OrchestrationV2Command) {
    const threadId =
      command.type === "thread.fork" || command.type === "thread.merge_back"
        ? command.targetThreadId
        : command.type === "delegated_task.request" ||
            command.type === "delegated_task.wake-policy" ||
            command.type === "delegated_task.completion-delivery.acknowledge" ||
            command.type === "delegated_task.completion-delivery.dispose" ||
            command.type === "thread.created.record"
          ? command.parentThreadId
          : command.threadId;
    const unknown: DispatchDeliveryEvidence = {
      kind: "dispatch",
      commandId: command.commandId,
      commandType: command.type,
      threadId,
      status: "unknown",
    };
    return yield* receipts.getByCommandId(command.commandId).pipe(
      Effect.map((receipt): DispatchDeliveryEvidence =>
        Option.isSome(receipt) &&
        receiptMatches(receipt.value, command.commandId, command.type, threadId)
          ? { ...unknown, status: receipt.value.status }
          : unknown,
      ),
      // Evidence is diagnostic after a failure; it must never replace that failure.
      Effect.catchCause(() => Effect.succeed(unknown)),
    );
  },
);

export const launchDeliveryEvidence = Effect.fn("jones.orchestration.launchDeliveryEvidence")(
  function* (receipts: ReceiptReader, events: EventReader, input: LaunchDeliveryRequest) {
    const initialMessage: LaunchDeliveryEvidence["initialMessage"] =
      input.initialMessage === undefined
        ? { status: "not-requested" }
        : {
            status: "unknown",
            commandId: initialMessageCommandId(input.commandId),
            ...(input.initialMessage.messageId === undefined
              ? {}
              : { messageId: input.initialMessage.messageId }),
          };
    const unknown: LaunchDeliveryEvidence = {
      kind: "launch",
      commandId: input.commandId,
      projectId: input.projectId,
      creation: { status: "unknown" },
      initialMessage,
    };
    const receipt = yield* receipts
      .getByCommandId(input.commandId)
      .pipe(Effect.catchCause(() => Effect.succeedNone));
    const commandType =
      input.reuseExistingThread === true ? "thread.metadata.update" : "thread.create";
    if (
      Option.isNone(receipt) ||
      !receiptMatches(receipt.value, input.commandId, commandType, input.threadId)
    )
      return unknown;
    if (receipt.value.status === "rejected")
      return { ...unknown, creation: { status: "rejected" } } satisfies LaunchDeliveryEvidence;
    const claimEvents = yield* events.readByCommandId({ commandId: input.commandId }).pipe(
      Stream.runCollect,
      Effect.catchCause(() => Effect.succeed([])),
    );
    const claim = recordedLaunchClaim(input, receipt.value, claimEvents);
    if (claim === undefined) return unknown;
    const creation = { status: "accepted", threadId: claim.id } as const;
    const claimed: LaunchDeliveryEvidence = { ...unknown, creation };
    if (input.initialMessage === undefined) return claimed;
    // The wire names the first prompt by its derived delivery key, whichever command carried it.
    const messageCommandId = initialMessageCommandId(input.commandId);
    const result = yield* readLaunchMessage(
      receipts,
      events,
      input,
      receipt.value,
      claimEvents,
    ).pipe(Effect.catchCause(() => Effect.succeed({ status: "unknown" as const })));
    if (result.status === "accepted")
      return {
        ...claimed,
        initialMessage: {
          status: "accepted",
          commandId: messageCommandId,
          messageId: result.message.id,
        },
      } satisfies LaunchDeliveryEvidence;
    if (result.status === "rejected")
      return {
        ...claimed,
        initialMessage: { ...initialMessage, status: "rejected", commandId: messageCommandId },
      } satisfies LaunchDeliveryEvidence;
    return claimed;
  },
);
