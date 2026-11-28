import {
  OrchestrationV2ThreadLaunchError,
  type ModelSelection,
  type OrchestrationV2ThreadLaunchInput,
} from "@t3tools/contracts";
import {
  initialMessageCommandId,
  type LaunchDeliveryEvidence,
} from "@t3tools/contracts/jones/deliveryEvidence";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

import type * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import * as ProjectCloneTracker from "../../project/ProjectCloneTracker.ts";
import {
  autoLaunchRequest,
  isReservedAutoLaunchCommand,
  matchesAutoLaunch,
  type AccountBinding,
  type AutoLaunchDecision,
  type AutoLaunchWorkspace,
} from "./accountBinding.ts";
import {
  AccountAutoRoutingError,
  selectAutoAccount,
  unsupportedAccountRoutingWorkspace,
  type SelectAutoAccountInput,
} from "./selectAutoAccount.ts";

type LaunchSelection = {
  readonly modelSelection: ModelSelection;
  readonly binding?: AccountBinding;
  readonly workspace?: AutoLaunchWorkspace;
};

type LaunchReceipts = Pick<
  EventSink.EventSinkV2["Service"],
  "readAutoLaunch" | "commitAutoLaunchRefusal"
>;

const unavailable = () =>
  new AccountAutoRoutingError({
    code: "verification_unavailable",
    message: "Auto account routing could not verify this launch. Retry the same launch.",
  });

export function autoLaunchDeliveryEvidence(
  input: Pick<OrchestrationV2ThreadLaunchInput, "commandId" | "projectId"> & {
    readonly initialMessage?:
      | Pick<NonNullable<OrchestrationV2ThreadLaunchInput["initialMessage"]>, "messageId">
      | undefined;
  },
  status: "unknown" | "rejected",
): LaunchDeliveryEvidence {
  return {
    kind: "launch",
    commandId: input.commandId,
    projectId: input.projectId,
    creation: { status },
    initialMessage:
      input.initialMessage === undefined
        ? { status: "not-requested" }
        : {
            status,
            commandId: initialMessageCommandId(input.commandId),
            ...(input.initialMessage.messageId === undefined
              ? {}
              : { messageId: input.initialMessage.messageId }),
          },
  };
}

const resolveWorkspace = Effect.fn("AccountRouting.resolveLaunchWorkspace")(function* (
  input: OrchestrationV2ThreadLaunchInput,
) {
  const strategy = input.workspaceStrategy;
  if (strategy.type !== "root" && strategy.type !== "existing_worktree")
    return yield* unsupportedAccountRoutingWorkspace();
  const projects = yield* ProjectStore.ProjectStoreV2;
  const project = yield* projects.get(input.projectId).pipe(Effect.mapError(unavailable));
  if (Option.isNone(project)) return yield* unavailable();
  const projectRoot = project.value.workspaceRoot;
  if (strategy.type === "root") return { projectRoot, worktreePath: null };

  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  if (!path.isAbsolute(strategy.worktreePath)) return yield* unsupportedAccountRoutingWorkspace();
  // The observer checks worktree registration. Pass that same canonical directory to creation.
  const [root, worktreePath] = yield* Effect.all([
    fs.realPath(projectRoot),
    fs.realPath(strategy.worktreePath),
  ]).pipe(Effect.mapError(unsupportedAccountRoutingWorkspace));
  if (root === worktreePath) return yield* unsupportedAccountRoutingWorkspace();
  return { projectRoot, worktreePath };
});

export const makeAutoLaunchResolver = <R>(
  selectAccount: (
    input: SelectAutoAccountInput,
  ) => Effect.Effect<LaunchSelection, AccountAutoRoutingError, R>,
) =>
  Effect.fn("AccountRouting.resolveLaunchAutoAccount")(function* (
    input: OrchestrationV2ThreadLaunchInput,
    receipts: LaunchReceipts,
    request = autoLaunchRequest(input),
  ) {
    const representative: LaunchSelection = { modelSelection: input.modelSelection };
    if (request === undefined) return representative;
    const toLaunchError = (cause: AccountAutoRoutingError, status: "unknown" | "rejected") =>
      new OrchestrationV2ThreadLaunchError({
        commandId: input.commandId,
        projectId: input.projectId,
        message: cause.message,
        cause,
        deliveryEvidence: autoLaunchDeliveryEvidence(input, status),
      });
    if (input.reuseExistingThread === true)
      return yield* toLaunchError(
        new AccountAutoRoutingError({
          code: "reuse_existing_thread",
          message:
            "Auto account routing is only available when starting a new thread. Choose a specific account to update an existing thread.",
        }),
        "unknown",
      );

    const { readAutoLaunch, commitAutoLaunchRefusal } = receipts;
    if (readAutoLaunch === undefined || commitAutoLaunchRefusal === undefined)
      return yield* toLaunchError(unavailable(), "unknown");

    const replay = (decision: AutoLaunchDecision) => {
      if (!matchesAutoLaunch(request, decision))
        return Effect.fail(toLaunchError(unavailable(), "unknown"));
      return decision.refusal === null
        ? Effect.succeed(representative)
        : Effect.fail(
            toLaunchError(
              new AccountAutoRoutingError({
                code: "previously_rejected",
                message: decision.refusal,
              }),
              "rejected",
            ),
          );
    };
    if (isReservedAutoLaunchCommand(input.commandId))
      return yield* toLaunchError(unavailable(), "unknown");
    const prior = yield* readAutoLaunch(input.commandId).pipe(
      Effect.mapError(() => toLaunchError(unavailable(), "unknown")),
    );
    if (prior !== undefined) return yield* replay(prior);

    const cloneTracker = yield* ProjectCloneTracker.ProjectCloneTracker;
    yield* ProjectCloneTracker.rejectCommandsDuringClone(cloneTracker, {
      type: "thread.create",
      projectId: input.projectId,
    }).pipe(Effect.mapError(() => toLaunchError(unavailable(), "unknown")));
    const workspace = yield* resolveWorkspace(input).pipe(
      Effect.mapError((cause) => toLaunchError(cause, "unknown")),
    );
    const selection = yield* selectAccount({
      projectId: input.projectId,
      modelSelection: input.modelSelection,
      runtimeMode: input.runtimeMode,
      interactionMode: input.interactionMode,
      ...(workspace.worktreePath === null ? {} : { cwd: workspace.worktreePath }),
    }).pipe(Effect.result);
    if (selection._tag === "Success") return { ...selection.success, workspace };

    const cause = selection.failure;
    if (cause.code === "unsupported_workspace" || cause.code === "verification_unavailable")
      return yield* toLaunchError(cause, "unknown");
    yield* ProjectCloneTracker.rejectCommandsDuringClone(cloneTracker, {
      type: "thread.create",
      projectId: input.projectId,
    }).pipe(Effect.mapError(() => toLaunchError(unavailable(), "unknown")));
    const decision = yield* commitAutoLaunchRefusal({ request, workspace }, cause.message).pipe(
      Effect.mapError(() => toLaunchError(unavailable(), "unknown")),
    );
    if (decision === undefined) return yield* toLaunchError(unavailable(), "unknown");
    if (decision.refusal === cause.message && matchesAutoLaunch(request, decision))
      return yield* toLaunchError(cause, "rejected");
    return yield* replay(decision);
  });

export const resolveLaunchAutoAccount = makeAutoLaunchResolver(selectAutoAccount);
