import {
  OrchestrationV2ThreadLaunchError,
  type ModelSelection,
  type OrchestrationV2ThreadLaunchInput,
} from "@t3tools/contracts";
import { initialMessageCommandId } from "@t3tools/contracts/jones/deliveryEvidence";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import * as CommandReceiptStore from "../../orchestration-v2/CommandReceiptStore.ts";
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

/** The launch service owns replay. Presence of any receipt defers to its immutable claim checks. */
export const resolveLaunchSelection = Effect.fn("AccountRouting.resolveLaunchSelection")(function* <
  ReceiptR,
  SelectR,
>(
  input: LaunchInput,
  hasReceipt: Effect.Effect<boolean, AccountAutoRoutingError, ReceiptR>,
  select: (
    input: SelectAutoAccountInput,
  ) => Effect.Effect<ModelSelection, AccountAutoRoutingError, SelectR>,
) {
  if (input.accountRouting === undefined) return input.modelSelection;
  if (input.reuseExistingThread === true) {
    return yield* new AccountAutoRoutingError({
      code: "reuse_existing_thread",
      message:
        "Auto account routing is only available when starting a new thread. Choose a specific account to update an existing thread.",
    });
  }
  if (yield* hasReceipt) return input.modelSelection;
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
        Effect.flatMap((exists) =>
          exists ? Effect.succeed(input.modelSelection) : Effect.fail(error),
        ),
      ),
    ),
  );
});

export const resolveLaunchAutoAccount = (input: OrchestrationV2ThreadLaunchInput) => {
  const hasReceipt = Effect.gen(function* () {
    const unavailable = new AccountAutoRoutingError({
      code: "verification_unavailable",
      message: "Auto account routing could not verify this launch receipt. Retry the same launch.",
    });
    const receipts = yield* Effect.serviceOption(CommandReceiptStore.CommandReceiptStoreV2);
    if (Option.isNone(receipts)) return yield* unavailable;
    return yield* receipts.value.getByCommandId(input.commandId).pipe(
      Effect.map(Option.isSome),
      Effect.mapError(() => unavailable),
    );
  });
  return resolveLaunchSelection(input, hasReceipt, selectAutoAccount).pipe(
    Effect.mapError((cause) => {
      const { commandId, projectId, initialMessage } = input;
      const rejectedBeforeClaim =
        cause.code !== "reuse_existing_thread" && cause.code !== "verification_unavailable";
      return new OrchestrationV2ThreadLaunchError({
        commandId,
        projectId,
        message: cause.message,
        cause,
        ...(rejectedBeforeClaim
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
    }),
  );
};
