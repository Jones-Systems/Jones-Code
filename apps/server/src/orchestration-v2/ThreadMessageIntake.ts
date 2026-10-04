import { remapComposerContextAttachments } from "@t3tools/shared/composerContextReferences";
import { appendUserInputAttachmentPaths } from "../provider/userInputAttachments.ts";
import {
  CommandId,
  type ThreadId,
  type ChatAttachment,
  type OrchestrationV2Command,
  type OrchestrationMessageContext,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { resolveAttachmentPath } from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";
import * as Orchestrator from "./Orchestrator.ts";

import * as AttachmentClaims from "./AttachmentClaims.ts";
import * as ThreadLaunch from "./ThreadLaunchService.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";
import * as EventSink from "./EventSink.ts";
import * as Witness from "./NormalizationWitness.ts";
import { DispatchGuardRejectedError } from "./DispatchGuard.ts";

// These dispatcher failures occur in receipt validation or planning, before
// commitCommand. Generic dispatch errors can follow a commit and remain uncertain.
function dispatchWasNotAccepted(error: unknown) {
  if (!isOrchestratorError(error)) return false;
  switch (error._tag) {
    case "OrchestratorCommandRejectedError":
    case "OrchestratorProjectionError":
    case "OrchestratorProviderAdapterError":
    case "OrchestratorCommandPreviouslyRejectedError":
    case "OrchestratorCommandIdConflictError":
    case "OrchestratorSubagentThreadReadOnlyError":
      return true;
    default:
      return false;
  }
}
const isOrchestratorError = Schema.is(Orchestrator.OrchestratorV2Error);

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

interface ReplayInput {
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly commandType: "message.dispatch" | "queued-run.edit" | "runtime-request.respond";
  readonly rawInput: unknown;
  readonly hasReferences: boolean;
  readonly launch?: boolean;
}

const conflict = (input: ReplayInput) =>
  new Orchestrator.OrchestratorCommandIdConflictError({
    commandId: input.commandId,
    commandType: input.commandType,
    receiptThreadId: input.threadId,
    commandThreadId: input.threadId,
  });
const preparationForReplay = (
  row: Witness.NormalizationWitnessV1,
): Witness.NormalizationWitnessPreparation => ({
  commandId: row.commandId,
  requestDigest: row.requestDigest,
  attachments: row.attachments,
  contextRemaps: row.contextRemaps,
  mode: "replay",
});
const readReplay = Effect.fnUntraced(function* (input: ReplayInput) {
  const sink = yield* EventSink.EventSinkV2;
  const mapReadError = (cause: unknown) =>
    new Orchestrator.OrchestratorDispatchError({
      commandId: input.commandId,
      commandType: input.commandType,
      cause,
    });
  const observed = yield* sink
    .readNormalizationWitness(input.commandId)
    .pipe(Effect.mapError(mapReadError));
  if (observed.witness === null) {
    if (observed.receipt !== null && input.hasReferences) return yield* conflict(input);
    return null;
  }
  const row = observed.witness;
  if (row.commandType !== input.commandType) return yield* conflict(input);
  const birth = yield* sink
    .readApplicationBirthRecord(input.threadId)
    .pipe(Effect.mapError(mapReadError));
  yield* Witness.compareForReplay(row, input, AttachmentClaims.probePendingAttachment, birth).pipe(
    Effect.catchTag("NormalizationWitnessConflict", () => Effect.fail(conflict(input))),
  );
  return row;
});

const contextRemaps = (
  context: OrchestrationMessageContext | undefined,
  before: ReadonlyArray<ChatAttachment>,
  after: ReadonlyArray<ChatAttachment>,
): ReadonlyArray<Witness.NormalizationContextRemapV1> => {
  const ids = new Map(before.map((attachment, index) => [attachment.id, after[index]?.id]));
  return (
    context?.records.flatMap((record) => {
      if ((record.kind !== "image" && record.kind !== "file") || !("attachmentId" in record))
        return [];
      const finalId = ids.get(record.attachmentId);
      return finalId === undefined || finalId === record.attachmentId
        ? []
        : [{ sourceId: record.attachmentId, finalId }];
    }) ?? []
  );
};

const isGuardRejection = Schema.is(DispatchGuardRejectedError);
const isSuperseded = Schema.is(Witness.NormalizationWitnessSuperseded);
const isWitnessConflict = Schema.is(Witness.NormalizationWitnessConflict);
const isLaunchError = Schema.is(ThreadLaunch.ThreadLaunchError);

const withFreshNormalization = Effect.fnUntraced(function* <A, E, R>(
  input: ReplayInput,
  claimed: AttachmentClaims.ClaimedAttachments,
  remaps: ReadonlyArray<Witness.NormalizationContextRemapV1>,
  invoke: (
    preparation: Witness.NormalizationWitnessPreparation,
    acceptedCommand?: OrchestrationV2Command,
  ) => Effect.Effect<A, E, R>,
) {
  const preparation: Witness.NormalizationWitnessPreparation = {
    commandId: input.commandId,
    requestDigest: Witness.requestDigest(input.rawInput),
    attachments: claimed.witnessAttachments,
    contextRemaps: remaps,
    mode: "fresh",
  };
  return yield* invoke(preparation).pipe(
    Effect.catch((error) =>
      Effect.gen(function* () {
        const outcome: unknown = input.launch
          ? isLaunchError(error) && error.operation === "dispatch-message"
            ? error.cause
            : undefined
          : error;
        if (isWitnessConflict(outcome) && outcome.commandId === input.commandId) {
          yield* AttachmentClaims.releaseClaimedAttachments(claimed.claimedPaths);
          return yield* conflict(input);
        }
        const collided =
          (isSuperseded(outcome) && outcome.commandId === input.commandId) ||
          (isGuardRejection(outcome) &&
            outcome.commandType === input.commandType &&
            outcome.reason === "identity_conflict" &&
            (outcome.detail ===
              "ordinary checkout command differs from its original accepted admission" ||
              outcome.detail ===
                "checkout command or joined operation differs from its original acceptance"));
        if (!collided) return yield* Effect.fail(error);
        const observed = yield* Effect.result(readReplay(input));
        if (observed._tag === "Failure" || observed.success === null)
          return yield* Effect.fail(error);
        const row = observed.success;
        const accepted = row.acceptedCommand;
        const winnerAttachments =
          accepted.type === "runtime-request.respond"
            ? Object.values(accepted.attachmentsByQuestionId ?? {}).flat()
            : accepted.type === "message.dispatch" || accepted.type === "queued-run.edit"
              ? (accepted.attachments ?? [])
              : [];
        const winnerIds = new Set([
          ...row.attachments.map((attachment) => attachment.finalId),
          ...winnerAttachments.map((attachment) => attachment.id),
          ...row.contextRemaps.map((remap) => remap.finalId),
        ]);
        if (claimed.witnessAttachments.some((attachment) => winnerIds.has(attachment.finalId)))
          return yield* Effect.fail(error);
        yield* AttachmentClaims.releaseClaimedAttachments(claimed.claimedPaths);
        // One qualified retry still traverses the original guards with the current execution context.
        return yield* invoke(preparationForReplay(row), row.acceptedCommand);
      }),
    ),
  );
});

const emptyClaims: AttachmentClaims.ClaimedAttachments = {
  attachments: [],
  claimedPaths: [],
  witnessAttachments: [],
};

export const dispatchCommand = Effect.fn("ThreadMessageIntake.dispatchCommand")(function* (
  command: OrchestrationV2Command,
) {
  const threads = yield* ThreadManagement.ThreadManagementService;
  if (
    command.type !== "message.dispatch" &&
    command.type !== "queued-run.edit" &&
    command.type !== "runtime-request.respond"
  )
    return yield* threads.dispatch(command);
  const input: ReplayInput = {
    commandId: command.commandId,
    threadId: command.threadId,
    commandType: command.type,
    rawInput: command,
    hasReferences:
      command.type === "runtime-request.respond"
        ? Object.values(command.attachmentsByQuestionId ?? {}).some(
            (attachments) => attachments.length > 0,
          )
        : (command.attachments?.length ?? 0) > 0 || (command.context?.records.length ?? 0) > 0,
  };
  const replay = yield* readReplay(input);
  if (replay !== null)
    return yield* threads
      .dispatch(replay.acceptedCommand)
      .pipe(
        Effect.provideService(Witness.NormalizationWitnessCarrier, {
          ...preparationForReplay(replay),
          acceptedCommand: replay.acceptedCommand,
        }),
      );
  const dispatchPrepared = (
    normalized: OrchestrationV2Command,
    claimed: AttachmentClaims.ClaimedAttachments,
    remaps: ReadonlyArray<Witness.NormalizationContextRemapV1> = [],
  ) =>
    withFreshNormalization(input, claimed, remaps, (preparation, acceptedCommand = normalized) =>
      threads
        .dispatch(acceptedCommand)
        .pipe(
          Effect.provideService(Witness.NormalizationWitnessCarrier, {
            ...preparation,
            acceptedCommand,
          }),
        ),
    );
  if (command.type === "runtime-request.respond" && command.attachmentsByQuestionId) {
    const config = yield* ServerConfig.ServerConfig;
    const incomingByQuestionId = command.attachmentsByQuestionId;
    yield* AttachmentClaims.validateAttachmentLimits(Object.values(incomingByQuestionId).flat());
    // Claims accumulate across questions, so all of preparation shares one
    // rollback boundary: any failure before dispatch removes every new copy.
    const claimedPaths: string[] = [];
    const witnessAttachments: Witness.NormalizationAttachmentV1[] = [];
    const prepared = yield* Effect.gen(function* () {
      const attachmentsByQuestionId: import("@t3tools/contracts").UserInputAttachments = {};
      for (const [questionId, attachments] of Object.entries(incomingByQuestionId)) {
        const claimed = yield* AttachmentClaims.claimPendingAttachments({
          threadId: command.threadId,
          attachments,
        });
        claimedPaths.push(...claimed.claimedPaths);
        witnessAttachments.push(...claimed.witnessAttachments);
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
    return yield* dispatchPrepared(
      {
        ...command,
        answers: prepared.answers,
        attachmentsByQuestionId: prepared.attachmentsByQuestionId,
      },
      {
        attachments: Object.values(prepared.attachmentsByQuestionId).flat(),
        claimedPaths,
        witnessAttachments,
      },
    ).pipe(
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
    return yield* dispatchPrepared(command, emptyClaims);
  const claimed = yield* AttachmentClaims.claimPendingAttachments({
    threadId: command.threadId,
    attachments: command.attachments ?? [],
  });
  return yield* dispatchPrepared(
    {
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
    },
    claimed,
    contextRemaps(command.context, command.attachments ?? [], claimed.attachments),
  ).pipe(
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
  const replayInput: ReplayInput = {
    commandId: input.commandId,
    threadId: input.threadId,
    commandType: "message.dispatch",
    rawInput: input,
    hasReferences: input.attachments.length > 0,
  };
  const replay = yield* readReplay(replayInput);
  if (replay !== null) {
    if (replay.acceptedCommand.type !== "message.dispatch") return yield* conflict(replayInput);
    return yield* threads.sendToThread(
      { ...input, attachments: replay.acceptedCommand.attachments },
      {
        preparation: preparationForReplay(replay),
        acceptedCommand: replay.acceptedCommand,
      },
    );
  }
  const claimed = yield* AttachmentClaims.claimPendingAttachments(input);
  return yield* withFreshNormalization(replayInput, claimed, [], (preparation, acceptedCommand) => {
    if (acceptedCommand !== undefined && acceptedCommand.type !== "message.dispatch")
      return Effect.fail(conflict(replayInput));
    return threads.sendToThread(
      { ...input, attachments: acceptedCommand?.attachments ?? claimed.attachments },
      {
        preparation,
        ...(acceptedCommand === undefined ? {} : { acceptedCommand }),
      },
    );
  }).pipe(
    Effect.tap((result) => releaseUnusedClaims(claimed.claimedPaths, result.message.attachments)),
    Effect.tapError((error) =>
      isOrchestratorError(error) && dispatchWasNotAccepted(error)
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
  if (
    input.initialMessage === undefined ||
    (input.threadId === undefined &&
      !input.initialMessage.attachments.some(AttachmentClaims.attachmentIsPendingUpload))
  )
    return yield* launches.launch(input);
  if (input.threadId === undefined) {
    return yield* new AttachmentClaims.AttachmentClaimError({
      message: "Uploaded attachments need a thread id at launch.",
    });
  }
  const replayInput: ReplayInput = {
    commandId: CommandId.make(`${input.commandId}:initial-message`),
    threadId: input.threadId,
    commandType: "message.dispatch",
    rawInput: input,
    launch: true,
    hasReferences:
      input.initialMessage.attachments.length > 0 ||
      (input.initialMessage.context?.records.length ?? 0) > 0,
  };
  const replay = yield* readReplay(replayInput);
  if (replay !== null) {
    if (replay.acceptedCommand.type !== "message.dispatch") return yield* conflict(replayInput);
    return yield* launches
      .launch({
        ...input,
        initialMessage: {
          ...input.initialMessage,
          attachments: replay.acceptedCommand.attachments,
          ...(replay.acceptedCommand.context === undefined
            ? {}
            : { context: replay.acceptedCommand.context }),
        },
      })
      .pipe(
        Effect.provideService(ThreadLaunch.ThreadLaunchNormalization, {
          launchCommandId: input.commandId,
          preparation: preparationForReplay(replay),
          acceptedCommand: replay.acceptedCommand,
        }),
      );
  }
  const claimed = yield* AttachmentClaims.claimPendingAttachments({
    threadId: input.threadId,
    attachments: input.initialMessage.attachments,
  });
  const normalizedInput: ThreadLaunch.ThreadLaunchInput = {
    ...input,
    initialMessage: {
      ...input.initialMessage,
      attachments: claimed.attachments,
      ...(input.initialMessage.context
        ? {
            context: remapComposerContextAttachments(
              input.initialMessage.context,
              input.initialMessage.attachments,
              claimed.attachments,
            ),
          }
        : {}),
    },
  };
  return yield* withFreshNormalization(
    replayInput,
    claimed,
    contextRemaps(
      input.initialMessage.context,
      input.initialMessage.attachments,
      claimed.attachments,
    ),
    (
      preparation,
      acceptedCommand,
    ): Effect.Effect<
      ThreadLaunch.ThreadLaunchResult,
      ThreadLaunch.ThreadLaunchError | Orchestrator.OrchestratorCommandIdConflictError
    > => {
      if (acceptedCommand !== undefined && acceptedCommand.type !== "message.dispatch")
        return Effect.fail(conflict(replayInput));
      return launches
        .launch(
          acceptedCommand === undefined
            ? normalizedInput
            : {
                ...normalizedInput,
                initialMessage: {
                  ...input.initialMessage!,
                  attachments: acceptedCommand.attachments,
                  ...(acceptedCommand.context === undefined
                    ? {}
                    : { context: acceptedCommand.context }),
                },
              },
        )
        .pipe(
          Effect.provideService(ThreadLaunch.ThreadLaunchNormalization, {
            launchCommandId: input.commandId,
            preparation,
            ...(acceptedCommand === undefined ? {} : { acceptedCommand }),
          }),
        );
    },
  ).pipe(
    Effect.tap((result) =>
      releaseUnusedClaims(
        claimed.claimedPaths,
        result.projection.messages.flatMap((message) => message.attachments),
      ),
    ),
    Effect.tapError((error) => {
      if (!isLaunchError(error)) return Effect.void;
      // Project/receipt reads precede message dispatch. The create-thread error
      // also wraps post-message projection reads, so its tag alone is not proof.
      const notAccepted =
        error.operation === "resolve-project" ||
        error.operation === "read-receipt" ||
        ((error.operation === "create-thread" || error.operation === "dispatch-message") &&
          isOrchestratorError(error.cause) &&
          // Projection errors under create-thread can occur after the message commit.
          (error.operation !== "create-thread" ||
            error.cause._tag !== "OrchestratorProjectionError") &&
          dispatchWasNotAccepted(error.cause));
      return notAccepted
        ? AttachmentClaims.releaseClaimedAttachments(claimed.claimedPaths)
        : Effect.void;
    }),
  );
});
