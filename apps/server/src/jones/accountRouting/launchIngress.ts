import {
  NonNegativeInt,
  OrchestrationV2ThreadLaunchError,
  ThreadId,
  type ModelSelection,
  type OrchestrationV2ThreadLaunchInput,
} from "@t3tools/contracts";
import { initialMessageCommandId } from "@t3tools/contracts/jones/deliveryEvidence";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import * as CommandReceiptStore from "../../orchestration-v2/CommandReceiptStore.ts";
import type { AccountBinding } from "./accountBinding.ts";
import {
  AccountAutoRoutingError,
  selectAutoAccount,
  unsupportedAccountRoutingWorkspace,
  type SelectAutoAccountInput,
} from "./selectAutoAccount.ts";

type LaunchInput = Pick<
  OrchestrationV2ThreadLaunchInput,
  | "accountRouting"
  | "modelSelection"
  | "projectId"
  | "reuseExistingThread"
  | "runtimeMode"
  | "interactionMode"
  | "workspaceStrategy"
>;

interface LaunchSelection {
  readonly modelSelection: ModelSelection;
  readonly binding?: AccountBinding;
}

type SelectLaunchAccount<R> = (
  input: SelectAutoAccountInput,
) => Effect.Effect<LaunchSelection, AccountAutoRoutingError, R>;

/** The launch service owns replay. Presence of any receipt defers to its immutable claim checks. */
export const resolveLaunchSelection = Effect.fn("AccountRouting.resolveLaunchSelection")(function* <
  ReceiptR,
  SelectR,
>(
  input: LaunchInput,
  hasReceipt: Effect.Effect<boolean, AccountAutoRoutingError, ReceiptR>,
  select: (
    input: SelectAutoAccountInput,
  ) => Effect.Effect<LaunchSelection, AccountAutoRoutingError, SelectR>,
) {
  const representative: LaunchSelection = { modelSelection: input.modelSelection };
  if (input.accountRouting === undefined) return representative;
  if (input.reuseExistingThread === true) {
    return yield* new AccountAutoRoutingError({
      code: "reuse_existing_thread",
      message:
        "Auto account routing is only available when starting a new thread. Choose a specific account to update an existing thread.",
    });
  }
  if (yield* hasReceipt) return representative;
  const strategy = input.workspaceStrategy;
  const selection =
    strategy.type === "root" || strategy.type === "existing_worktree"
      ? select({
          projectId: input.projectId,
          modelSelection: input.modelSelection,
          runtimeMode: input.runtimeMode,
          interactionMode: input.interactionMode,
          ...(strategy.type === "existing_worktree" ? { cwd: strategy.worktreePath } : {}),
        })
      : Effect.fail(unsupportedAccountRoutingWorkspace());
  // A concurrent same-command caller can win while this caller reads allowance.
  return yield* selection.pipe(
    Effect.catch((error) =>
      hasReceipt.pipe(
        Effect.flatMap((exists) => (exists ? Effect.succeed(representative) : Effect.fail(error))),
      ),
    ),
  );
});

export const makeAutoLaunchResolver = <R>(selectAccount: SelectLaunchAccount<R>) =>
  Effect.fn("resolveLaunchAutoAccount")((input: OrchestrationV2ThreadLaunchInput) => {
    const rejectionPrefix = "Auto account routing refused: ";
    const representative: LaunchSelection = { modelSelection: input.modelSelection };
    const unavailable = () =>
      new AccountAutoRoutingError({
        code: "verification_unavailable",
        message:
          "Auto account routing could not verify this launch receipt. Retry the same launch.",
      });
    const receiptStore = Effect.gen(function* () {
      const receipts = yield* Effect.serviceOption(CommandReceiptStore.CommandReceiptStoreV2);
      if (Option.isNone(receipts)) return yield* unavailable();
      return receipts.value;
    });
    const readReceipt = receiptStore.pipe(
      Effect.flatMap((receipts) => receipts.getByCommandId(input.commandId)),
      Effect.mapError(unavailable),
    );
    const toLaunchError = (cause: AccountAutoRoutingError, rejected: boolean) => {
      const { commandId, projectId, initialMessage } = input;
      return new OrchestrationV2ThreadLaunchError({
        commandId,
        projectId,
        message: cause.message,
        cause,
        ...(rejected
          ? {
              deliveryEvidence: {
                kind: "launch" as const,
                commandId,
                projectId,
                creation: { status: "rejected" as const },
                initialMessage:
                  initialMessage === undefined
                    ? { status: "not-requested" as const }
                    : {
                        status: "rejected" as const,
                        commandId: initialMessageCommandId(commandId),
                        ...(initialMessage.messageId === undefined
                          ? {}
                          : { messageId: initialMessage.messageId }),
                      },
              },
            }
          : {}),
      });
    };
    return Effect.gen(function* () {
      if (input.accountRouting === undefined || input.reuseExistingThread === true)
        return yield* resolveLaunchSelection(input, Effect.succeed(false), selectAccount).pipe(
          Effect.mapError((cause) => toLaunchError(cause, false)),
        );
      const previous = yield* readReceipt.pipe(
        Effect.mapError((cause) => toLaunchError(cause, false)),
      );
      if (
        Option.isSome(previous) &&
        previous.value.commandType === "thread.create" &&
        previous.value.status === "rejected" &&
        (input.threadId === undefined || previous.value.threadId === input.threadId) &&
        previous.value.error?.startsWith(rejectionPrefix)
      )
        return yield* toLaunchError(
          new AccountAutoRoutingError({
            code: "previously_rejected",
            message: previous.value.error.slice(rejectionPrefix.length),
          }),
          true,
        );
      if (Option.isSome(previous)) return representative;
      return yield* resolveLaunchSelection(
        input,
        Effect.map(readReceipt, Option.isSome),
        selectAccount,
      ).pipe(
        Effect.catch((cause) =>
          Effect.gen(function* () {
            if (cause.code === "reuse_existing_thread" || cause.code === "verification_unavailable")
              return yield* toLaunchError(cause, false);
            // Reserve the command id before telling a client it may try again under a new id.
            // Even a caller already between selection and dispatch can only replay this rejection.
            const receipts = yield* receiptStore.pipe(
              Effect.mapError((error) => toLaunchError(error, false)),
            );
            const threadId = input.threadId ?? ThreadId.make(input.commandId);
            const fenced = yield* receipts
              .insertIfAbsent({
                commandId: input.commandId,
                threadId,
                commandType: "thread.create",
                acceptedAt: yield* DateTime.now,
                resultSequence: NonNegativeInt.make(0),
                status: "rejected",
                error: `${rejectionPrefix}${cause.message}`,
              })
              .pipe(Effect.mapError(() => toLaunchError(unavailable(), false)));
            if (fenced) return yield* toLaunchError(cause, true);
            // Someone else owns the durable result, so leave replay validation to launch.
            return representative;
          }),
        ),
      );
    });
  });

export const resolveLaunchAutoAccount = makeAutoLaunchResolver(selectAutoAccount);
