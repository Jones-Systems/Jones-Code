import { claimForCaller } from "../../linkedThreads/launch.ts";
import {
  CommandId,
  ModelSelection,
  OrchestratorMcpFailure,
  ProviderInteractionMode,
  RuntimeMode,
  type LinkedThreadOriginLink,
  type ClientLaunchLookup,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as Receipts from "../../../orchestration-v2/CommandReceiptStore.ts";
import * as EventSink from "../../../orchestration-v2/EventSink.ts";
import * as ThreadLaunch from "../../../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";
import { outputRequestProblem } from "../../../orchestration-v2/runOutput.ts";
import * as Project from "../../../project/ProjectService.ts";
import * as Git from "../../../vcs/GitVcsDriver.ts";
import type { Caller } from "../../../mcp/threadAccess.ts";
import { unavailable } from "../../../mcp/threadAccess.ts";
import { callerLinkOrigin } from "../../../mcp/linkOrigin.ts";
import { existingAccountRoutingWorkspaceRequired } from "../accountRouting.ts";
import { resolveAutoLaunchModel } from "../launchModelSelection.ts";
import { withAccountLaunchBinding } from "../../accountRouting/accountBinding.ts";
import { recordedLaunchClaim } from "../../orchestration/deliveryEvidence.ts";
import type { StartedModes, ThreadStartGate } from "../../../mcp/McpToolAccess.ts";
import {
  clientLaunchIds,
  clientLaunchDigest,
  type ClientLaunchRequest,
} from "./ClientLaunchIdentity.ts";
import { resolveLaunchDefaults } from "./launchDefaults.ts";

interface Claim {
  readonly request_digest: string;
  readonly message_requested: number;
  readonly auto_admission: string | null;
}

const AutoAdmission = Schema.fromJsonString(
  Schema.Struct({
    projectRoot: Schema.NonEmptyString,
    physicalRoot: Schema.NonEmptyString,
    worktreePath: Schema.NullOr(Schema.NonEmptyString),
    modelSelection: ModelSelection,
    accountDigest: Schema.NonEmptyString,
    runtimeMode: RuntimeMode,
    interactionMode: ProviderInteractionMode,
  }),
);
type AutoAdmission = typeof AutoAdmission.Type;
const encodeAutoAdmission = Schema.encodeEffect(AutoAdmission);
const decodeAutoAdmission = Schema.decodeUnknownEffect(AutoAdmission);

const isMcpFailure = Schema.is(OrchestratorMcpFailure);
const fenceReason = "jones-client-launch:v1:fenced";
const readClaim = Effect.fn("ClientLaunchService.readClaim")(function* (commandId: CommandId) {
  const sql = yield* SqlClient.SqlClient;
  return (yield* sql<Claim>`SELECT request_digest, message_requested, auto_admission
    FROM jones_mcp_client_launch_requests WHERE command_id = ${commandId}`)[0];
}, Effect.mapError(unavailable));
const readReceipt = (commandId: CommandId) =>
  Receipts.CommandReceiptStoreV2.pipe(
    Effect.flatMap((receipts) => receipts.getByCommandId(commandId)),
    Effect.mapError(unavailable),
    Effect.map(Option.getOrUndefined),
  );

export const lookup = Effect.fn("jones.clientLaunch.lookup")(function* (
  namespace: string,
  clientRequestId: string,
): Effect.fn.Return<
  ClientLaunchLookup,
  OrchestratorMcpFailure,
  | SqlClient.SqlClient
  | Receipts.CommandReceiptStoreV2
  | ThreadLaunch.ThreadLaunchService
  | ThreadManagement.ThreadManagementService
> {
  const ids = clientLaunchIds(namespace, clientRequestId);
  const receipt = yield* readReceipt(ids.commandId);
  const claim = yield* readClaim(ids.commandId);
  const base: ClientLaunchLookup = {
    clientRequestId,
    state: "absent",
    basis: "none",
    threadId: null,
    requestDigest: claim?.request_digest ?? null,
    claimed: claim !== undefined,
    threadDeleted: false,
    initialMessage:
      claim === undefined ? "unknown" : claim.message_requested === 0 ? "not_requested" : "pending",
    runId: null,
    runStatus: null,
    preparation: "not_applicable",
    workspace: null,
    reason: null,
  };
  if (receipt === undefined) return base;
  if (receipt.status === "rejected")
    return {
      ...base,
      state: "no_effect",
      basis: receipt.error === fenceReason ? "fence" : "rejection",
      reason: receipt.error,
    };
  const threads = yield* ThreadManagement.ThreadManagementService;
  const shell = yield* threads.getThreadShell(ids.threadId).pipe(Effect.mapError(unavailable));
  const messageReceipt =
    claim?.message_requested === 1
      ? yield* readReceipt(CommandId.make(`${ids.commandId}:initial-message`))
      : undefined;
  const launches = yield* ThreadLaunch.ThreadLaunchService;
  // Older combined launch commits can carry the exact message without a separate receipt.
  const carried =
    claim?.message_requested === 1 && messageReceipt === undefined && shell !== null
      ? yield* launches.getDeliveryEvidence({
          commandId: ids.commandId,
          threadId: ids.threadId,
          projectId: shell.projectId,
          initialMessage: { messageId: ids.messageId },
        })
      : undefined;
  const initialMessage: ClientLaunchLookup["initialMessage"] =
    claim === undefined
      ? "unknown"
      : claim.message_requested === 0
        ? "not_requested"
        : (messageReceipt?.status ??
          (carried?.initialMessage.status === "accepted" ? "accepted" : "pending"));
  const launched = {
    ...base,
    state: "launched" as const,
    basis: "receipt" as const,
    threadId: ids.threadId,
    initialMessage,
    reason: messageReceipt?.status === "rejected" ? messageReceipt.error : null,
  };
  if (shell === null || shell.deletedAt !== null) return { ...launched, threadDeleted: true };
  const projection = yield* threads
    .getThreadProjection(ids.threadId)
    .pipe(Effect.mapError(unavailable));
  const run = projection.runs.find((entry) => entry.userMessageId === ids.messageId);
  let preparation: ClientLaunchLookup["preparation"] = "not_applicable";
  if (run !== undefined) {
    if (run.status !== "preparing") preparation = "left_preparation";
    else {
      if (yield* launches.preparationActive(ids.commandId)) preparation = "in_progress";
      else {
        const worktree = yield* readReceipt(CommandId.make(`${ids.commandId}:progress:worktree`));
        const setup = yield* readReceipt(CommandId.make(`${ids.commandId}:progress:setup`));
        preparation =
          worktree?.status === "accepted" || setup?.status === "accepted"
            ? "unknown_effect"
            : "unscheduled";
      }
    }
  }
  return {
    ...launched,
    runId: run?.id ?? null,
    runStatus: run?.status ?? null,
    preparation,
    workspace: {
      type: shell.worktreePath === null ? "root" : "existing_worktree",
      branch: shell.branch,
      worktreePath: shell.worktreePath,
    },
  };
});

export const fence = Effect.fn("jones.clientLaunch.fence")(function* (
  namespace: string,
  clientRequestId: string,
) {
  const ids = clientLaunchIds(namespace, clientRequestId);
  if ((yield* readReceipt(ids.commandId)) === undefined) {
    const sink = yield* EventSink.EventSinkV2;
    yield* sink
      .commitRejectedCommand({
        commandId: ids.commandId,
        threadId: ids.threadId,
        commandType: "thread.create",
        rejectedAt: yield* DateTime.now,
        error: fenceReason,
      })
      .pipe(Effect.mapError(unavailable));
  }
  const result = yield* lookup(namespace, clientRequestId);
  if (result.state === "absent") return yield* unavailable();
  return { ...result, state: result.state };
});

// Linked launches use upstream IDs; Jones receipts cannot establish absence or fence that work.
const assertLocalReceiptCaller = (caller: Caller) =>
  callerLinkOrigin(caller) === undefined
    ? Effect.void
    : Effect.fail(
        new OrchestratorMcpFailure({
          code: "capability_denied",
          message:
            "Launch lookup and fencing are unavailable for linked-environment work. Retry the original launch with the same clientRequestId and unchanged request; a Jones receipt cannot prove its outcome.",
        }),
      );

export const lookupForCaller = Effect.fn("jones.clientLaunch.lookupForCaller")(function* (
  caller: Caller,
  clientRequestId: string,
) {
  yield* assertLocalReceiptCaller(caller);
  return yield* lookup(caller.scope.requestNamespace, clientRequestId);
});

export const fenceForCaller = Effect.fn("jones.clientLaunch.fenceForCaller")(function* (
  caller: Caller,
  clientRequestId: string,
) {
  yield* assertLocalReceiptCaller(caller);
  return yield* fence(caller.scope.requestNamespace, clientRequestId);
});

export const launch = Effect.fn("jones.clientLaunch.launch")(function* (
  input: ClientLaunchRequest,
  caller: Caller,
  modes: StartedModes,
  gate: ThreadStartGate,
  navigationExcluded = false,
) {
  if (
    input.scratch !== undefined ||
    (input.attachments?.length ?? 0) > 0 ||
    (input.workspaceStrategy?.type === "worktree" && !input.message?.trim()) ||
    (caller.caller === undefined && input.projectId === undefined)
  ) {
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message:
        "Keyed launches require a project, no scratch or attachments, and a message for a new worktree.",
    });
  }
  if (input.generateTitle === true && !input.message?.trim())
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message: "Title generation requires a message. No thread was created.",
    });
  if (input.outputSchema !== undefined) {
    const problem = input.message?.trim()
      ? outputRequestProblem(input.outputSchema, { text: input.message })
      : "outputSchema needs a message that asks for the result.";
    if (problem !== null)
      return yield* new OrchestratorMcpFailure({ code: "invalid_request", message: problem });
  }
  const autoModelSelection = input.accountRouting === undefined ? undefined : input.modelSelection;
  if (input.accountRouting !== undefined && autoModelSelection === undefined)
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message:
        "Auto account routing requires an explicit modelSelection with a provider and model.",
    });
  const projectId = input.projectId ?? caller.caller!.projectId;
  const ids = clientLaunchIds(caller.scope.requestNamespace, input.clientRequestId);
  const digest = yield* Effect.try({
    try: () => clientLaunchDigest(input, projectId),
    catch: () =>
      new OrchestratorMcpFailure({
        code: "invalid_request",
        message: "The launch request is not canonical JSON.",
      }),
  });
  const receipt = yield* readReceipt(ids.commandId);
  if (receipt?.status === "rejected")
    return yield* new OrchestratorMcpFailure({
      code: "launch_fenced",
      message: "This launch key has an authoritative rejection.",
    });
  const priorClaim = yield* readClaim(ids.commandId);
  if (priorClaim !== undefined && priorClaim.request_digest !== digest)
    return yield* new OrchestratorMcpFailure({
      code: "request_conflict",
      message: "The key belongs to a different launch request.",
    });
  const launches = yield* ThreadLaunch.ThreadLaunchService;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const workspaceStrategy = input.workspaceStrategy ?? { type: "root" as const };
  const initialMessage =
    input.message === undefined
      ? {}
      : {
          initialMessage: {
            messageId: ids.messageId,
            text: input.message,
            attachments: [],
            ...(input.outputSchema === undefined ? {} : { outputSchema: input.outputSchema }),
            ...(caller.caller === undefined ? {} : { senderThreadId: caller.caller.id }),
          },
        };
  const common = {
    holdPreparationAfterProgress: true as const,
    commandId: ids.commandId,
    threadId: ids.threadId,
    projectId,
    title: input.title.trim(),
    ...(input.generateTitle === true ? { generateTitle: true } : {}),
    workspaceStrategy,
    ...initialMessage,
    createdBy: "agent" as const,
    creationSource: "mcp" as const,
  };
  const invoke = (request: ThreadLaunch.ThreadLaunchInput) =>
    launches.launch(request).pipe(Effect.mapError(unavailable));
  const getProject = Effect.gen(function* () {
    const projects = yield* Project.ProjectService;
    const project = yield* projects.getById(projectId).pipe(Effect.mapError(unavailable));
    if (Option.isNone(project))
      return yield* new OrchestratorMcpFailure({
        code: "invalid_request",
        message: "The project was not found.",
      });
    return project.value;
  });
  const requireStoredAutoAdmission = (claim: Claim) =>
    decodeAutoAdmission(claim.auto_admission).pipe(
      Effect.mapError(
        () =>
          new OrchestratorMcpFailure({
            code: "orchestration_error",
            message: "The original Auto launch admission is unavailable for this key.",
          }),
      ),
    );
  const unavailableWorkspace = () =>
    new OrchestratorMcpFailure({
      code: "orchestration_error",
      message: "The original Auto launch workspace is unavailable.",
    });
  const boundWorkspace = Effect.fn("ClientLaunchService.boundWorkspace")(function* (
    admission: AutoAdmission,
  ) {
    const project = yield* getProject;
    const fs = yield* FileSystem.FileSystem;
    const real = (path: string) => fs.realPath(path).pipe(Effect.mapError(unavailableWorkspace));
    if (
      project.workspaceRoot !== admission.projectRoot ||
      (yield* real(project.workspaceRoot)) !== admission.physicalRoot
    )
      return yield* unavailableWorkspace();
    if (workspaceStrategy.type === "root" && admission.worktreePath === null)
      return workspaceStrategy;
    if (
      workspaceStrategy.type !== "existing_worktree" ||
      admission.worktreePath === null ||
      (yield* real(admission.worktreePath)) !== admission.worktreePath
    )
      return yield* unavailableWorkspace();
    const git = yield* Git.GitVcsDriver;
    const paths = yield* git
      .listWorktreePaths(project.workspaceRoot)
      .pipe(Effect.mapError(unavailableWorkspace));
    const registered = yield* Effect.findFirst(paths, (path) =>
      fs.realPath(path).pipe(
        Effect.map((resolved) => resolved === admission.worktreePath),
        Effect.orElseSucceed(() => false),
      ),
    );
    if (Option.isNone(registered)) return yield* unavailableWorkspace();
    return { ...workspaceStrategy, worktreePath: admission.worktreePath };
  });
  const launchWithAdmission = (request: ThreadLaunch.ThreadLaunchInput, admission: AutoAdmission) =>
    withAccountLaunchBinding(
      invoke(request),
      ids.commandId,
      {
        instanceId: admission.modelSelection.instanceId,
        digest: admission.accountDigest,
      },
      {
        projectRoot: admission.projectRoot,
        worktreePath: admission.worktreePath,
      },
    );
  const readOriginal = Effect.fn("ClientLaunchService.readOriginal")(function* () {
    const receipt = yield* readReceipt(ids.commandId);
    if (receipt === undefined) return yield* unavailable();
    const sink = yield* EventSink.EventSinkV2;
    const events = yield* sink
      .readByCommandId({ commandId: ids.commandId })
      .pipe(Stream.runCollect, Effect.mapError(unavailable));
    const original = recordedLaunchClaim(common, receipt, events);
    return original === undefined ? yield* unavailable() : original;
  });
  let originLink: LinkedThreadOriginLink | undefined;
  let replayed = receipt?.status === "accepted";
  const continueAccepted = Effect.fn("ClientLaunchService.continueAccepted")(function* () {
    const claim = yield* readClaim(ids.commandId);
    if (claim?.request_digest !== digest)
      return yield* new OrchestratorMcpFailure({
        code: "request_conflict",
        message: "The key belongs to a different launch request.",
      });
    originLink = yield* claimForCaller(caller, ids.threadId, ids.commandId, navigationExcluded);
    const state = yield* lookup(caller.scope.requestNamespace, input.clientRequestId);
    if (state.threadDeleted)
      return yield* new OrchestratorMcpFailure({
        code: "orchestration_error",
        message: "The recorded launch thread was deleted; this key cannot relaunch it.",
      });
    const shell = yield* threads.getThreadShell(ids.threadId).pipe(Effect.mapError(unavailable));
    if (shell === null) return yield* unavailable();
    if (
      state.initialMessage === "pending" ||
      (state.initialMessage === "accepted" && state.preparation === "unscheduled")
    ) {
      const original = autoModelSelection === undefined ? undefined : yield* readOriginal();
      const admission =
        original === undefined || claim.auto_admission === null
          ? undefined
          : yield* requireStoredAutoAdmission(claim);
      let workspace = workspaceStrategy;
      if (admission !== undefined && original !== undefined) {
        if (admission.worktreePath !== original.worktreePath) return yield* unavailableWorkspace();
        workspace = yield* boundWorkspace(admission);
      } else if (original !== undefined && workspaceStrategy.type === "existing_worktree") {
        const fs = yield* FileSystem.FileSystem;
        const checkout = original.worktreePath;
        if (
          checkout === null ||
          (yield* fs.realPath(checkout).pipe(Effect.orElseSucceed(() => undefined))) !== checkout
        )
          return yield* unavailableWorkspace();
        workspace = { ...workspaceStrategy, worktreePath: checkout };
      }
      const request = {
        ...common,
        workspaceStrategy: workspace,
        modelSelection: original?.modelSelection ?? shell.modelSelection,
        runtimeMode: shell.runtimeMode,
        interactionMode: shell.interactionMode,
      };
      yield* gate.continueWithin(
        shell,
        admission === undefined ? invoke(request) : launchWithAdmission(request, admission),
      );
    }
  });
  if (replayed) {
    yield* continueAccepted();
  } else {
    const preflight = Effect.gen(function* () {
      const project = yield* getProject;
      if (
        autoModelSelection !== undefined &&
        (workspaceStrategy.type === "worktree" || workspaceStrategy.type === "auto")
      )
        return yield* existingAccountRoutingWorkspaceRequired();
      const fs = yield* FileSystem.FileSystem;
      const physicalRoot =
        autoModelSelection === undefined
          ? project.workspaceRoot
          : yield* fs.realPath(project.workspaceRoot).pipe(Effect.mapError(unavailableWorkspace));
      let cwd = physicalRoot;
      if (workspaceStrategy.type === "existing_worktree") {
        const git = yield* Git.GitVcsDriver;
        const real = (path: string) => fs.realPath(path).pipe(Effect.orElseSucceed(() => path));
        const paths = yield* git
          .listWorktreePaths(project.workspaceRoot)
          .pipe(Effect.orElseSucceed((): ReadonlyArray<string> => []));
        const requested = yield* real(workspaceStrategy.worktreePath);
        const checks = yield* Effect.forEach(paths, real);
        const registered = paths[checks.indexOf(requested)];
        if (registered === undefined)
          return yield* new OrchestratorMcpFailure({
            code: "invalid_request",
            message: "worktreePath must be one of the project's git worktrees.",
          });
        cwd = autoModelSelection === undefined ? registered : requested;
      }
      if (workspaceStrategy.type === "worktree")
        yield* launches.checkWorktreeBase({ projectId, workspaceStrategy }).pipe(
          Effect.mapError(
            () =>
              new OrchestratorMcpFailure({
                code: "invalid_request",
                message:
                  "The requested worktree base could not be resolved. No thread was created.",
              }),
          ),
        );
      const { modelSelection, runtimeMode } = yield* resolveLaunchDefaults(
        input,
        caller,
        projectId,
        modes,
        Effect.succeed(project),
      );
      if (autoModelSelection === undefined) return { modelSelection, runtimeMode };
      const routed = yield* resolveAutoLaunchModel({
        caller,
        projectId,
        modelSelection: autoModelSelection,
        runtimeMode,
        interactionMode: modes.interactionMode,
        cwd,
      });
      return {
        modelSelection,
        runtimeMode,
        autoAdmission: {
          projectRoot: project.workspaceRoot,
          physicalRoot,
          worktreePath: workspaceStrategy.type === "existing_worktree" ? cwd : null,
          modelSelection: routed.modelSelection,
          accountDigest: routed.binding.digest,
          runtimeMode,
          interactionMode: modes.interactionMode,
        } satisfies AutoAdmission,
      };
    });
    // A pending Auto key already owns a binding. Only new proposals inspect usage
    // and resolve an alias. Concurrent proposals use the winner inside the transaction.
    const proposal =
      autoModelSelection !== undefined && priorClaim !== undefined ? undefined : yield* preflight;
    const sql = yield* SqlClient.SqlClient;
    const claim = yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const currentReceipt = yield* readReceipt(ids.commandId);
          if (currentReceipt?.status === "rejected")
            return yield* new OrchestratorMcpFailure({
              code: "launch_fenced",
              message: "This launch key has an authoritative rejection.",
            });
          let claim = yield* readClaim(ids.commandId);
          if (claim !== undefined) {
            if (claim.request_digest !== digest)
              return yield* new OrchestratorMcpFailure({
                code: "request_conflict",
                message: "The key belongs to a different launch request.",
              });
          } else {
            if (currentReceipt !== undefined)
              return yield* new OrchestratorMcpFailure({
                code: "request_conflict",
                message: "The existing receipt has no matching claim.",
              });
            if (proposal === undefined) return yield* unavailable();
            claim = {
              request_digest: digest,
              message_requested: input.message === undefined ? 0 : 1,
              auto_admission:
                proposal.autoAdmission === undefined
                  ? null
                  : yield* encodeAutoAdmission(proposal.autoAdmission).pipe(
                      Effect.mapError(unavailable),
                    ),
            };
            yield* sql`INSERT INTO jones_mcp_client_launch_requests
              (command_id, thread_id, request_digest, request_version, message_requested, recorded_at, auto_admission)
              VALUES (${ids.commandId}, ${ids.threadId}, ${claim.request_digest}, 1, ${claim.message_requested},
                ${DateTime.formatIso(yield* DateTime.now)}, ${claim.auto_admission})`.pipe(
              Effect.mapError(unavailable),
            );
          }
          originLink = yield* claimForCaller(
            caller,
            ids.threadId,
            ids.commandId,
            navigationExcluded,
          );
          return claim;
        }),
      )
      .pipe(Effect.mapError((error) => (isMcpFailure(error) ? error : unavailable())));
    // Another identical caller may have accepted while this call was in preflight.
    // Reconcile that receipt with recorded modes before attempting continuation.
    if ((yield* readReceipt(ids.commandId))?.status === "accepted") {
      replayed = true;
      yield* continueAccepted();
    } else {
      const admission =
        autoModelSelection === undefined ? undefined : yield* requireStoredAutoAdmission(claim);
      const modelSelection = admission?.modelSelection ?? proposal?.modelSelection;
      const runtimeMode = admission?.runtimeMode ?? proposal?.runtimeMode;
      if (modelSelection === undefined || runtimeMode === undefined) return yield* unavailable();
      const interactionMode = admission?.interactionMode ?? modes.interactionMode;
      const request = {
        ...common,
        ...(admission === undefined ? {} : { workspaceStrategy: yield* boundWorkspace(admission) }),
        modelSelection,
        runtimeMode,
        interactionMode,
        ...(modes.linkOrigin === undefined ? {} : { linkOrigin: modes.linkOrigin }),
      };
      const result = yield* gate
        .continueWithin(
          admission === undefined ? modes : admission,
          admission === undefined ? invoke(request) : launchWithAdmission(request, admission),
        )
        .pipe(
          Effect.catch((error) =>
            Effect.gen(function* () {
              const latestReceipt = yield* readReceipt(ids.commandId);
              if (latestReceipt?.status === "rejected")
                return yield* new OrchestratorMcpFailure({
                  code: "launch_fenced",
                  message: "This launch key has an authoritative rejection.",
                });
              // A fresh invocation can commit creation before its message is refused.
              // That outcome requires lookup, unlike a pre-effect continuation refusal.
              if (
                latestReceipt?.status === "accepted" &&
                (error.code === "runtime_mode_escalation_denied" ||
                  error.code === "interaction_mode_escalation_denied")
              )
                return yield* unavailable();
              return yield* error;
            }),
          ),
        );
      replayed = result.resumed;
    }
  }
  const state = yield* lookup(caller.scope.requestNamespace, input.clientRequestId);
  const shell = yield* threads.getThreadShell(ids.threadId).pipe(Effect.mapError(unavailable));
  if (shell === null) return yield* unavailable();
  const modelSelection =
    autoModelSelection === undefined
      ? shell.modelSelection
      : (yield* readOriginal()).modelSelection;
  return {
    threadId: ids.threadId,
    projectId: shell.projectId,
    modelSelection,
    runId: state.runId,
    status: state.runStatus,
    replayed,
    ...(originLink === undefined ? {} : { originLink }),
    requestDigest: digest,
    initialMessage: state.initialMessage,
    preparation: state.preparation,
  };
});
