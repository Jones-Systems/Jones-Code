import { remapComposerContextAttachments } from "@t3tools/shared/composerContextReferences";
import { appendUserInputAttachmentPaths } from "../provider/userInputAttachments.ts";
import {
  CommandId,
  MessageId,
  OrchestrationV2Command,
  type ChatAttachment,
  type ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { resolveAttachmentPath } from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";
import * as Orchestrator from "./Orchestrator.ts";

import * as AttachmentClaims from "./AttachmentClaims.ts";
import * as ThreadLaunch from "./ThreadLaunchService.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";
import * as EventSink from "./EventSink.ts";
import * as CommandReceipts from "./CommandReceiptStore.ts";
import { makeCommitTransaction } from "./CommitTransaction.ts";
import { canonicalJson } from "./CanonicalJson.ts";

type AttachmentMessageCommand = Extract<
  OrchestrationV2Command,
  { readonly type: "message.dispatch" | "queued-run.edit" }
>;

const withIntakeClaims = <A, E, R>(
  input: {
    readonly commandId: CommandId;
    readonly receiptCommandId: CommandId;
    readonly threadId: ThreadId;
    readonly attachments: ReadonlyArray<ChatAttachment>;
    readonly requestData: unknown;
    readonly context?: import("@t3tools/contracts").OrchestrationMessageContext;
    readonly expected: (
      claim: AttachmentClaims.CorrelatedAttachments,
      messageId: MessageId,
      accepted: AttachmentMessageCommand | undefined,
    ) => AttachmentMessageCommand;
    readonly launch?: ThreadLaunch.ThreadLaunchInput;
    readonly knownNotAccepted: (error: unknown) => boolean;
  },
  use: (claim: AttachmentClaims.CorrelatedAttachments) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const receipts = yield* CommandReceipts.CommandReceiptStoreV2;
    const sink = yield* EventSink.EventSinkV2;
    const config = yield* ServerConfig.ServerConfig;
    const transactions = yield* makeCommitTransaction();
    const refuse = (message: string) => new AttachmentClaims.AttachmentClaimError({ message });
    const read: AttachmentClaims.CorrelatedClaimPorts["read"] = (claim) =>
      Effect.gen(function* () {
        if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
          return yield* refuse("Attachment replay cannot observe an enclosing SQL transaction.");
        return yield* transactions
          .withTransaction(
            Effect.gen(function* () {
              const projectReceipt = yield* receipts.getProjectByCommandId(input.receiptCommandId);
              if (Option.isSome(projectReceipt))
                return yield* refuse("Attachment command belongs to a project receipt.");
              const receipt = Option.getOrNull(
                yield* receipts.getByCommandId(input.receiptCommandId),
              );
              const events = Array.from(
                yield* sink
                  .readByCommandId({ commandId: input.receiptCommandId })
                  .pipe(Stream.runCollect),
              );
              if (receipt === null) {
                if (events.length !== 0)
                  return yield* refuse("Attachment command outcome is unknown.");
                return { type: "unhandled" as const };
              }
              if (
                receipt.commandId !== input.receiptCommandId ||
                receipt.threadId !== input.threadId ||
                !["message.dispatch", "queued-run.edit"].includes(receipt.commandType)
              )
                return yield* refuse("Attachment receipt identity conflicts with this request.");
              if (receipt.status === "rejected") return { type: "rejected" as const };
              if (
                events.length === 0 ||
                Math.max(...events.map((event) => event.sequence)) !== receipt.resultSequence ||
                events.some(
                  (event) =>
                    event.commandId !== receipt.commandId ||
                    event.event.threadId !== input.threadId ||
                    event.sequence > receipt.resultSequence,
                )
              )
                return yield* refuse("Accepted attachment receipt has inconsistent raw events.");
              const messages = events.filter((entry) => entry.event.type === "message.updated");
              if (messages.length !== 1 || messages[0]!.event.type !== "message.updated")
                return yield* refuse("Attachment command has no unique original message mapping.");
              const event = messages[0]!.event;
              const message = event.payload;
              const lifetime = sink.ordinaryCheckoutLifetime;
              let original: AttachmentMessageCommand | undefined;
              if (receipt.commandType === "message.dispatch") {
                if (
                  lifetime === undefined ||
                  sink.validateOrdinaryCheckoutCommandReplay === undefined
                )
                  return yield* refuse("Original checkout replay proof is unavailable.");
                const bindings = yield* lifetime.readCurrentCommands(input.receiptCommandId);
                if (bindings.length !== 1 || bindings[0]!.threadId !== input.threadId)
                  return yield* refuse(
                    "Attachment acceptance has no unique original checkout command.",
                  );
                const decoded = yield* Schema.decodeUnknownEffect(OrchestrationV2Command)(
                  bindings[0]!.canonicalCommand,
                );
                if (decoded.type !== "message.dispatch")
                  return yield* refuse("Original attachment command type changed.");
                original = decoded;
              }
              const expected = yield* Effect.try({
                try: () => input.expected(claim, message.id, original),
                catch: (cause) =>
                  Schema.is(AttachmentClaims.AttachmentClaimError)(cause)
                    ? cause
                    : refuse("Original attachment command cannot be reconstructed."),
              });
              if (
                expected.type !== receipt.commandType ||
                expected.threadId !== input.threadId ||
                expected.commandId !== input.receiptCommandId ||
                message.threadId !== input.threadId ||
                message.id !==
                  (expected.type === "message.dispatch" ? expected.messageId : message.id) ||
                event.runId !== message.runId ||
                (expected.type === "queued-run.edit" && message.runId !== expected.runId) ||
                message.text !== expected.text ||
                canonicalJson(message.attachments) !== canonicalJson(claim.attachments) ||
                (expected.context !== undefined &&
                  canonicalJson(message.context) !== canonicalJson(expected.context))
              )
                return yield* refuse(
                  "Original accepted attachment mapping does not match this request.",
                );
              if (expected.type === "message.dispatch") {
                const encoded = yield* Schema.encodeEffect(OrchestrationV2Command)(expected);
                if (
                  original === undefined ||
                  canonicalJson(encoded) !== canonicalJson(bindingsFor(original))
                )
                  return yield* refuse("Original canonical attachment command changed.");
                const validateReplay = sink.validateOrdinaryCheckoutCommandReplay;
                if (validateReplay === undefined)
                  return yield* refuse("Original checkout replay proof is unavailable.");
                yield* validateReplay(expected, input.threadId);
              }
              if (input.launch !== undefined) {
                const launchReceipt = Option.getOrNull(
                  yield* receipts.getByCommandId(input.commandId),
                );
                const launchEvents = Array.from(
                  yield* sink
                    .readByCommandId({ commandId: input.commandId })
                    .pipe(Stream.runCollect),
                );
                const births = launchEvents.filter(
                  (entry) =>
                    entry.event.type === "thread.created" ||
                    entry.event.type === "thread.metadata-updated",
                );
                if (
                  Option.isSome(yield* receipts.getProjectByCommandId(input.commandId)) ||
                  launchReceipt?.status !== "accepted" ||
                  launchReceipt.threadId !== input.threadId ||
                  launchReceipt.commandType !==
                    (input.launch.reuseExistingThread === true
                      ? "thread.metadata.update"
                      : "thread.create") ||
                  births.length !== 1 ||
                  launchEvents.some(
                    (entry) =>
                      entry.commandId !== input.commandId ||
                      entry.event.threadId !== input.threadId ||
                      entry.sequence > launchReceipt.resultSequence,
                  )
                )
                  return yield* refuse("Attachment launch has no authentic original thread claim.");
                const birth = births[0]!.event;
                if (
                  birth.type === "thread.created" &&
                  birth.payload.projectId !== input.launch.projectId
                )
                  return yield* refuse("Attachment launch project changed.");
              }
              return { type: "accepted" as const, recoveredMessageId: message.id };
            }),
          )
          .pipe(
            Effect.mapError((cause) =>
              Schema.is(AttachmentClaims.AttachmentClaimError)(cause)
                ? cause
                : new AttachmentClaims.AttachmentClaimError({
                    message: "Attachment receipt readback is unavailable or unknown.",
                    cause,
                  }),
            ),
          );
      });
    const withRollbackReadback: AttachmentClaims.CorrelatedClaimPorts["withRollbackReadback"] = (
      claim,
      removeOwned,
    ) =>
      Effect.gen(function* () {
        if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
          return yield* refuse("Attachment rollback cannot observe an enclosing SQL transaction.");
        return yield* transactions
          .withTransaction(
            Effect.gen(function* () {
              const projectReceipt = yield* receipts.getProjectByCommandId(input.receiptCommandId);
              const receipt = Option.getOrNull(
                yield* receipts.getByCommandId(input.receiptCommandId),
              );
              const events = Array.from(
                yield* sink
                  .readByCommandId({ commandId: input.receiptCommandId })
                  .pipe(Stream.runCollect),
              );
              if (
                Option.isSome(projectReceipt) ||
                events.length !== 0 ||
                receipt?.status === "accepted" ||
                (receipt !== null &&
                  (receipt.threadId !== input.threadId ||
                    !["message.dispatch", "queued-run.edit"].includes(receipt.commandType)))
              )
                return yield* refuse(
                  "Fresh attachment rollback has accepted, conflicting or unknown command evidence.",
                );
              const readRetention = sink.readThreadRetainedAttachmentPaths;
              if (readRetention === undefined)
                return yield* refuse("Attachment retention proof is unavailable.");
              const retention = yield* readRetention(input.threadId);
              if (retention.status === "complete") {
                const retained = new Set(retention.relativePaths);
                for (const path of claim.claimedPaths) {
                  if (retained.has(path.slice(config.attachmentsDir.length + 1)))
                    return yield* refuse(
                      "Another accepted command retains a fresh attachment claim.",
                    );
                }
              } else {
                // No birth is different from an unavailable existing/deleted history.
                // This read is held through removal; a concurrent accepted birth cannot
                // become visible between the absence proof and this invocation's cleanup.
                const rows =
                  yield* sql`SELECT thread_id FROM orchestration_v2_projection_threads WHERE thread_id = ${input.threadId} LIMIT 1`;
                if (
                  rows.length !== 0 ||
                  (yield* sink.latestSequence({ threadId: input.threadId })) !== 0
                )
                  return yield* refuse(
                    "Attachment retention is unknown for an existing thread history.",
                  );
              }
              yield* removeOwned;
            }),
          )
          .pipe(
            Effect.mapError((cause) =>
              Schema.is(AttachmentClaims.AttachmentClaimError)(cause)
                ? cause
                : new AttachmentClaims.AttachmentClaimError({
                    message: "Attachment rollback readback is unavailable.",
                    cause,
                  }),
            ),
          );
      });
    return yield* AttachmentClaims.withCorrelatedClaims(
      {
        threadId: input.threadId,
        attachments: input.attachments,
        ports: {
          storeIdentity: sql,
          commandId: input.commandId,
          requestData: input.requestData,
          read,
          withRollbackReadback,
          knownNotAccepted: input.knownNotAccepted,
        },
      },
      use,
    );
  });

const bindingsFor = Schema.encodeSync(OrchestrationV2Command);
const remappedContext = (
  context: import("@t3tools/contracts").OrchestrationMessageContext | undefined,
  original: ReadonlyArray<ChatAttachment>,
  claim: AttachmentClaims.CorrelatedAttachments,
) =>
  context === undefined
    ? undefined
    : remapComposerContextAttachments(context, original, claim.attachments);

// These dispatcher failures occur in receipt validation or planning, before
// commitCommand. Generic dispatch errors can follow a commit and remain uncertain.
function dispatchWasNotAccepted(
  error: Orchestrator.OrchestratorV2Error | ThreadManagement.ThreadManagementError,
) {
  switch (error._tag) {
    case "OrchestratorCommandRejectedError":
    case "OrchestratorProjectionError":
    case "OrchestratorProviderAdapterError":
    case "OrchestratorCommandPreviouslyRejectedError":
    case "OrchestratorCommandIdConflictError":
    case "OrchestratorSubagentThreadReadOnlyError":
    case "OrchestratorThreadMessagesBlockedError":
      return true;
    default:
      return false;
  }
}
const isOrchestratorError = Schema.is(Orchestrator.OrchestratorV2Error);
const isThreadManagementError = Schema.is(ThreadManagement.ThreadManagementError);
const knownDispatchRefusal = (error: unknown) =>
  (isOrchestratorError(error) || isThreadManagementError(error)) && dispatchWasNotAccepted(error);

const releaseUnusedClaims = Effect.fn("ThreadMessageIntake.releaseUnusedClaims")(function* (
  claimedPaths: ReadonlyArray<string>,
  accepted: ReadonlyArray<ChatAttachment>,
) {
  if (claimedPaths.length === 0) return;
  const config = yield* ServerConfig.ServerConfig;
  const retained = new Set(
    accepted.map((attachment) =>
      resolveAttachmentPath({
        attachmentsDir: config.attachmentsDir,
        attachment,
      }),
    ),
  );
  yield* AttachmentClaims.releaseClaimedAttachments(
    claimedPaths.filter((path) => !retained.has(path)),
  );
});

export const dispatchCommand = Effect.fn("ThreadMessageIntake.dispatchCommand")(function* (
  command: OrchestrationV2Command,
) {
  const threads = yield* ThreadManagement.ThreadManagementService;
  if (command.type === "runtime-request.respond" && command.attachmentsByQuestionId) {
    const config = yield* ServerConfig.ServerConfig;
    const incomingByQuestionId = command.attachmentsByQuestionId;
    yield* AttachmentClaims.validateAttachmentLimits(Object.values(incomingByQuestionId).flat());
    // Claims accumulate across questions, so all of preparation shares one
    // rollback boundary: any failure before dispatch removes every new copy.
    const claimedPaths: string[] = [];
    const prepared = yield* Effect.gen(function* () {
      const attachmentsByQuestionId: import("@t3tools/contracts").UserInputAttachments = {};
      for (const [questionId, attachments] of Object.entries(incomingByQuestionId)) {
        const claimed = yield* AttachmentClaims.claimPendingAttachments({
          threadId: command.threadId,
          attachments,
        });
        claimedPaths.push(...claimed.claimedPaths);
        Object.defineProperty(attachmentsByQuestionId, questionId, {
          value: claimed.attachments,
          enumerable: true,
        });
      }
      const answers = yield* appendUserInputAttachmentPaths({
        answers: command.answers ?? {},
        attachmentsByQuestionId,
        attachmentsDir: config.attachmentsDir,
      }).pipe(
        Effect.mapError(
          (cause) => new AttachmentClaims.AttachmentClaimError({ message: cause.issue }),
        ),
      );
      return { answers, attachmentsByQuestionId };
    }).pipe(Effect.onError(() => AttachmentClaims.releaseClaimedAttachments(claimedPaths)));
    return yield* threads
      .dispatch({
        ...command,
        answers: prepared.answers,
        attachmentsByQuestionId: prepared.attachmentsByQuestionId,
      })
      .pipe(
        Effect.tap((result) => {
          // A replayed receipt reports the first attempt's answer, so this
          // attempt's copies go unreferenced and are released. The resolved
          // turn item proves the respond was applied; questionAnswer is only
          // recorded when the accepted command carried attachments, so its
          // absence means the accepted answer referenced no copies. With no
          // resolved item at all the outcome is ambiguous and everything stays.
          const answeredItems = result.storedEvents.flatMap(({ event }) =>
            event.type === "turn-item.updated" &&
            event.payload.type === "user_input_request" &&
            event.payload.requestId === command.requestId
              ? [event.payload]
              : [],
          );
          return answeredItems.length > 0
            ? releaseUnusedClaims(
                claimedPaths,
                answeredItems.flatMap((item) =>
                  item.questionAnswer === undefined
                    ? []
                    : Object.values(item.questionAnswer.attachmentsByQuestionId).flat(),
                ),
              )
            : Effect.void;
        }),
        Effect.tapError((error) =>
          dispatchWasNotAccepted(error)
            ? AttachmentClaims.releaseClaimedAttachments(claimedPaths)
            : Effect.void,
        ),
      );
  }
  if (
    command.type !== "message.dispatch" &&
    (command.type !== "queued-run.edit" || command.attachments === undefined)
  )
    return yield* threads.dispatch(command);
  if ((command.attachments ?? []).some(AttachmentClaims.attachmentIsPendingUpload)) {
    return yield* withIntakeClaims(
      {
        commandId: command.commandId,
        receiptCommandId: command.commandId,
        threadId: command.threadId,
        attachments: command.attachments ?? [],
        requestData: { kind: "dispatch", command },
        ...(command.context === undefined ? {} : { context: command.context }),
        expected: (claim) => ({
          ...command,
          attachments: claim.attachments,
          ...(command.context === undefined
            ? {}
            : { context: remappedContext(command.context, command.attachments ?? [], claim) }),
        }),
        knownNotAccepted: knownDispatchRefusal,
      },
      (claim) =>
        threads.dispatch({
          ...command,
          attachments: claim.attachments,
          ...(command.context === undefined
            ? {}
            : { context: remappedContext(command.context, command.attachments ?? [], claim) }),
        }),
    );
  }
  const claimed = yield* AttachmentClaims.claimPendingAttachments({
    threadId: command.threadId,
    attachments: command.attachments ?? [],
  });
  return yield* threads
    .dispatch({
      ...command,
      attachments: claimed.attachments,
      ...(command.context
        ? {
            context: remapComposerContextAttachments(
              command.context,
              command.attachments ?? [],
              claimed.attachments,
            ),
          }
        : {}),
    })
    .pipe(
      Effect.tap((result) =>
        releaseUnusedClaims(
          claimed.claimedPaths,
          result.storedEvents.flatMap(({ event }) =>
            event.type === "message.updated" ? event.payload.attachments : [],
          ),
        ),
      ),
      Effect.tapError((error) =>
        dispatchWasNotAccepted(error)
          ? AttachmentClaims.releaseClaimedAttachments(claimed.claimedPaths)
          : Effect.void,
      ),
    );
});

export const sendToThread = Effect.fn("ThreadMessageIntake.sendToThread")(function* (
  input: ThreadManagement.ThreadManagementSendInput,
) {
  const threads = yield* ThreadManagement.ThreadManagementService;
  if (input.attachments.some(AttachmentClaims.attachmentIsPendingUpload)) {
    return yield* withIntakeClaims(
      {
        commandId: input.commandId,
        receiptCommandId: input.commandId,
        threadId: input.threadId,
        attachments: input.attachments,
        requestData: { kind: "send", input },
        expected: (claim, _messageId, accepted) => {
          const mode = accepted?.type === "message.dispatch" ? accepted.dispatchMode : undefined;
          const allowed =
            mode !== undefined &&
            (input.mode === "queue"
              ? mode.type === "queue_after_active"
              : input.mode === "steer"
                ? mode.type === "steer_active"
                : input.mode === "restart"
                  ? mode.type === "restart_active"
                  : mode.type === "start_immediately" || mode.type === "steer_active");
          if (!allowed || mode === undefined)
            throw new AttachmentClaims.AttachmentClaimError({
              message: "Original send dispatch mode does not match this request.",
            });
          return {
            type: "message.dispatch",
            commandId: input.commandId,
            threadId: input.threadId,
            messageId: input.messageId,
            text: input.text,
            attachments: claim.attachments,
            dispatchMode: mode,
            createdBy: input.createdBy,
            creationSource: input.creationSource,
            ...(input.scheduledTaskId === undefined
              ? {}
              : { scheduledTaskId: input.scheduledTaskId }),
            ...(input.senderThreadId === undefined ? {} : { senderThreadId: input.senderThreadId }),
            ...(input.modelSelection === undefined ? {} : { modelSelection: input.modelSelection }),
          };
        },
        knownNotAccepted: knownDispatchRefusal,
      },
      (claim) => threads.sendToThread({ ...input, attachments: claim.attachments }),
    );
  }
  const claimed = yield* AttachmentClaims.claimPendingAttachments(input);
  return yield* threads.sendToThread({ ...input, attachments: claimed.attachments }).pipe(
    Effect.tap((result) => releaseUnusedClaims(claimed.claimedPaths, result.message.attachments)),
    Effect.tapError((error) =>
      dispatchWasNotAccepted(error)
        ? AttachmentClaims.releaseClaimedAttachments(claimed.claimedPaths)
        : Effect.void,
    ),
  );
});

export const launchThread = Effect.fn("ThreadMessageIntake.launchThread")(function* (
  input: ThreadLaunch.ThreadLaunchInput,
) {
  const launches = yield* ThreadLaunch.ThreadLaunchService;
  yield* AttachmentClaims.validateAttachmentLimits(input.initialMessage?.attachments ?? []);
  if (!input.initialMessage?.attachments.some(AttachmentClaims.attachmentIsPendingUpload)) {
    return yield* launches.launch(input);
  }
  if (input.threadId === undefined) {
    return yield* new AttachmentClaims.AttachmentClaimError({
      message: "Uploaded attachments need a thread id at launch.",
    });
  }
  const originalMessage = input.initialMessage;
  const threadId = input.threadId;
  return yield* withIntakeClaims(
    {
      commandId: input.commandId,
      receiptCommandId: CommandId.make(`${input.commandId}:initial-message`),
      threadId: input.threadId,
      attachments: originalMessage.attachments,
      requestData: { kind: "launch", input },
      ...(originalMessage.context === undefined ? {} : { context: originalMessage.context }),
      launch: input,
      expected: (claim, messageId) => ({
        type: "message.dispatch",
        commandId: CommandId.make(`${input.commandId}:initial-message`),
        threadId,
        messageId: originalMessage.messageId ?? messageId,
        text: originalMessage.text,
        attachments: claim.attachments,
        ...(originalMessage.context === undefined
          ? {}
          : {
              context: remappedContext(originalMessage.context, originalMessage.attachments, claim),
            }),
        ...(originalMessage.scheduledTaskId === undefined
          ? {}
          : { scheduledTaskId: originalMessage.scheduledTaskId }),
        ...(originalMessage.senderThreadId === undefined
          ? {}
          : { senderThreadId: originalMessage.senderThreadId }),
        ...(originalMessage.titleSeed === undefined
          ? input.generateTitle === true
            ? { titleSeed: input.title }
            : {}
          : { titleSeed: originalMessage.titleSeed }),
        ...(originalMessage.sourcePlanRef === undefined
          ? {}
          : { sourcePlanRef: originalMessage.sourcePlanRef }),
        modelSelection: input.modelSelection,
        dispatchMode: {
          type: "defer_start",
          workspaceStrategy: input.workspaceStrategy,
          ...(input.runSetupScript === undefined ? {} : { runSetupScript: input.runSetupScript }),
        },
        createdBy: input.createdBy,
        creationSource: input.creationSource,
      }),
      knownNotAccepted: (error) =>
        Schema.is(ThreadLaunch.ThreadLaunchError)(error) &&
        (error.operation === "resolve-project" ||
          ((error.operation === "create-thread" || error.operation === "dispatch-message") &&
            isOrchestratorError(error.cause) &&
            (error.operation !== "create-thread" ||
              error.cause._tag !== "OrchestratorProjectionError") &&
            dispatchWasNotAccepted(error.cause))),
    },
    (claim) =>
      launches.launch({
        ...input,
        initialMessage: {
          ...originalMessage,
          ...(originalMessage.messageId === undefined && claim.recoveredMessageId !== undefined
            ? { messageId: MessageId.make(claim.recoveredMessageId) }
            : {}),
          attachments: claim.attachments,
          ...(originalMessage.context
            ? {
                context: remapComposerContextAttachments(
                  originalMessage.context,
                  originalMessage.attachments,
                  claim.attachments,
                ),
              }
            : {}),
        },
      }),
  );
});
