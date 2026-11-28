import type { Tool } from "effect/ai";
import { claimForCaller } from "../../../jones/linkedThreads/launch.ts";
import * as ClientLaunch from "../../../jones/mcp/clientLaunch/ClientLaunchService.ts";
import { clientLaunchIds } from "../../../jones/mcp/clientLaunch/ClientLaunchIdentity.ts";
import { inheritsLinkedDelegationModes } from "../../../jones/mcp/delegatedTaskSupervision/DelegatedTaskSupervision.ts";
import {
  CommandId,
  MessageId,
  ThreadId,
  OrchestratorMcpFailure,
  type OrchestrationV2ThreadLaunchWorkspaceStrategy,
  ProjectId,
  type TaskFolderError,
  type Project as ProjectRecord,
  T3_PROJECT_FILE_NAME,
} from "@t3tools/contracts";
import * as Digest from "effect/crypto/Digest";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { OrchestratorRuntimeModeAboveCeilingError } from "../../../orchestration-v2/Orchestrator.ts";
import * as CommandReceipts from "../../../orchestration-v2/CommandReceiptStore.ts";
import { outputRequestProblem } from "../../../orchestration-v2/runOutput.ts";
import * as ThreadLaunch from "../../../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadMessageIntake from "../../../orchestration-v2/ThreadMessageIntake.ts";
import * as Claims from "../../../orchestration-v2/AttachmentClaims.ts";
import * as Project from "../../../project/ProjectService.ts";
import * as ProjectCloneTracker from "../../../project/ProjectCloneTracker.ts";
import { mutateProjectWithClone } from "../../../project/ProjectMutation.ts";
import * as ManagedProjectFolders from "../../../project/ManagedProjectFolders.ts";
import * as Repositories from "../../../sourceControl/SourceControlRepositoryService.ts";
import * as GitVcsDriver from "../../../vcs/GitVcsDriver.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as ServerSettings from "../../../serverSettings.ts";
import * as ProviderRegistry from "../../../provider/ProviderRegistry.ts";
import * as DateTime from "effect/DateTime";
import { resolveNewThreadModelSelection } from "@t3tools/shared/usageLimits";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import {
  resolveProjectFileBackedSetting,
  resolveProjectSettings,
} from "@t3tools/shared/projectSettings";
import { resolveProjectScripts } from "@t3tools/shared/projectScripts";
import { parseT3ProjectFile } from "@t3tools/shared/t3ProjectFile";
import {
  providerOptionDescriptors,
  refuseAgentModePick,
  resolveRuntimeMode,
} from "../../delegatedTaskModes.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { withAccountLaunchBinding } from "../../../jones/accountRouting/accountBinding.ts";
import {
  existingAccountRoutingWorkspaceRequired,
  localAccountRoutingRequired,
} from "../../../jones/mcp/accountRouting.ts";
import {
  resolveAutoLaunchModel,
  validateLaunchModel,
} from "../../../jones/mcp/launchModelSelection.ts";
import { newCommandId, readCaller, resolveProjectId, unavailable } from "../../threadAccess.ts";
import { ProjectToolkit } from "./tools.ts";

const isAboveCeilingError = Schema.is(OrchestratorRuntimeModeAboveCeilingError);

function projectFailure(error: Project.ProjectServiceError) {
  if (error._tag === "ProjectOperationError") return unavailable();
  const message =
    error._tag === "ProjectInvalidError"
      ? error.detail
      : error._tag === "ProjectNotFoundError"
        ? "The project was not found."
        : error._tag === "ProjectConflictError"
          ? "The workspace is already registered to a project."
          : "The project is not empty; force=true is required to delete it.";
  return new OrchestratorMcpFailure({ code: "invalid_request", message });
}

function worktreeCheckHint(
  error:
    | ThreadLaunch.ThreadLaunchBaseRefError
    | ThreadLaunch.ThreadLaunchBranchExistsError
    | ThreadLaunch.ThreadLaunchBranchConflictError
    | ThreadLaunch.ThreadLaunchNotRepositoryError
    | TaskFolderError,
  strategy: { readonly startFromOrigin?: boolean | undefined },
) {
  if (error._tag === "TaskFolderError")
    return "Pass `repositories` paths from the project's tracked Git folders.";
  if (error._tag === "ThreadLaunchNotRepositoryError")
    return `Use workspaceStrategy: {type: "root"}, or pass the projectId of a Git repository.`;
  if (error._tag === "ThreadLaunchBranchExistsError")
    return "Pass a branch name that does not exist yet, or omit branch to get a generated name.";
  if (error._tag === "ThreadLaunchBranchConflictError") {
    const sibling = error.branch.replaceAll("/", "-");
    return `Use a name that does not nest under or contain an existing branch${
      sibling === error.branch ? "" : `, such as "${sibling}"`
    }.`;
  }
  return `Pass a branch, tag, or commit that exists there${
    strategy.startFromOrigin === true
      ? "; no remote of the project has it either."
      : ", or set startFromOrigin:true if it exists only on a remote."
  }`;
}

/**
 * Require project worktrees so launches cannot target arbitrary host directories. Returns the
 * path Git registered, so the thread stays bound to that worktree.
 */
const projectWorktreePath = Effect.fn("mcp.assertProjectWorktree")(function* (
  workspaceRoot: string,
  worktreePath: string,
) {
  const git = yield* GitVcsDriver.GitVcsDriver;
  const fileSystem = yield* FileSystem.FileSystem;
  const real = (path: string) => fileSystem.realPath(path).pipe(Effect.orElseSucceed(() => path));
  const registered = yield* git
    .listWorktreePaths(workspaceRoot)
    .pipe(Effect.orElseSucceed((): ReadonlyArray<string> => []));
  const requested = yield* real(worktreePath);
  for (const candidate of registered) {
    if ((yield* real(candidate)) === requested) return candidate;
  }
  return yield* new OrchestratorMcpFailure({
    code: "invalid_request",
    message:
      "worktreePath must be one of the project's git worktrees. t3_worktree_list shows them.",
  });
});

/** The composer falls back to the project's t3.json when no setting picks the mode. */
const projectFileThreadEnvMode = Effect.fn("mcp.projectFileThreadEnvMode")(function* (
  workspaceRoot: string,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const contents = yield* fileSystem
    .readFileString(`${workspaceRoot}/${T3_PROJECT_FILE_NAME}`)
    .pipe(Effect.orElseSucceed(() => null));
  return resolveProjectFileBackedSetting(
    "defaultThreadEnvMode",
    null,
    contents === null ? null : parseT3ProjectFile(contents),
  ).value;
});

// Anchored to the fatal line: other diagnostics, such as a dubious-ownership refusal, quote the path.
const NON_REPOSITORY_FATAL =
  /^fatal: (?:not a git repository|gitfile does not point to a valid repository)/im;

/** Branches from the checkout's current branch, as the composer does; a checkout with no commit or a detached HEAD stays shared. */
const defaultWorktreeStrategy = Effect.fn("mcp.defaultWorktreeStrategy")(function* (
  workspaceRoot: string,
): Effect.fn.Return<
  OrchestrationV2ThreadLaunchWorkspaceStrategy | undefined,
  OrchestratorMcpFailure,
  GitVcsDriver.GitVcsDriver
> {
  const git = yield* GitVcsDriver.GitVcsDriver;
  const unreadable = new OrchestratorMcpFailure({
    code: "orchestration_error",
    message:
      "Could not read the project's current branch for the new worktree. No thread was created. Retry, or pass workspaceStrategy.",
  });
  const commit = yield* git
    .execute({
      operation: "mcp.defaultWorktreeStrategy",
      cwd: workspaceRoot,
      // Not peeled: a branch whose tip object is missing still resolves, so only an unborn HEAD exits 1.
      args: ["rev-parse", "--verify", "--quiet", "HEAD"],
      allowNonZeroExit: true,
      timeoutMs: 5_000,
    })
    .pipe(Effect.orElseSucceed(() => null));
  // Only Git saying "no commit yet" (exit 1) or "not a repository" keeps the launch shared.
  if (commit?.exitCode === 1 || (commit !== null && NON_REPOSITORY_FATAL.test(commit.stderr)))
    return undefined;
  if (commit?.exitCode !== 0) return yield* unreadable;
  // symbolic-ref skips the index, so a held index lock cannot drop the launch into the shared checkout.
  const head = yield* git
    .execute({
      operation: "mcp.defaultWorktreeStrategy",
      cwd: workspaceRoot,
      args: ["symbolic-ref", "--quiet", "--short", "HEAD"],
      allowNonZeroExit: true,
      timeoutMs: 5_000,
    })
    .pipe(Effect.orElseSucceed(() => null));
  if (head?.exitCode === 1) return undefined;
  const branch = head?.exitCode === 0 ? head.stdout.trim() : "";
  if (branch.length === 0) return yield* unreadable;
  return { type: "worktree" as const, baseRef: branch };
});

/** A fixed-length id, so a long key fits in the scratch folder name derived from it. */
const launchCommandId = Effect.fn("mcp.launchCommandId")(function* (
  namespace: string,
  key: string,
) {
  const crypto = yield* Digest.Digest;
  // @effect-diagnostics-next-line preferSchemaOverJson:off - a JSON pair keeps the caller apart from the key.
  const bytes = new TextEncoder().encode(JSON.stringify([namespace, key]));
  const digest = yield* crypto.digest(Digest.sha256, bytes).pipe(Effect.mapError(unavailable));
  return CommandId.make(`mcp:launch:${Hex.encode(digest)}`);
});

const access = Effect.gen(function* () {
  yield* readCaller();
  return yield* Project.ProjectService;
});

/** Settings, not the project record, hold the default model and actions that new threads use. */
const withSavedProjectSettings = Effect.gen(function* () {
  const settings = yield* ServerSettings.ServerSettingsService.pipe(
    Effect.flatMap((service) => service.getSettings),
    Effect.mapError(unavailable),
  );
  return (project: ProjectRecord) => {
    const resolved = resolveProjectSettings(settings, project.id, project);
    return {
      ...project,
      defaultModelSelection:
        resolved.sources.defaultModelSelection === "project"
          ? resolved.settings.defaultModelSelection
          : null,
      scripts: resolveProjectScripts(settings, project),
    };
  };
});

// The project is already committed, so a failed Settings read must not report the create as failed.
const withSavedProjectSettingsAfterCreate = withSavedProjectSettings.pipe(
  Effect.orElseSucceed(() => (project: ProjectRecord) => project),
);

/**
 * The id a linked keyed launch derives from the caller's namespace and key:
 * upstream's `launchCommandId`, so the id stays within what thread-id
 * consumers accept.
 */
export const linkedLaunchCommandId = launchCommandId;

/**
 * The id earlier builds derived for a linked keyed launch. It carries
 * percent-encoded separators and, for a client or forwarded key, runs to
 * about 127 characters, so it is never issued for a new launch; it is only
 * recognised on retry.
 */
export const legacyLinkedLaunchCommandId = (namespace: string, key: string) =>
  CommandId.make(`mcp:launch:${encodeURIComponent(namespace)}:${encodeURIComponent(key)}`);

/**
 * Resolves the command id of a linked keyed launch. A retry of a launch that
 * an earlier build accepted under the legacy id keeps that id, so the launch
 * service replays it instead of creating a second thread. Every other key
 * uses the hashed id.
 */
const linkedKeyedLaunchCommandId = Effect.fn("mcp.linkedKeyedLaunchCommandId")(function* (
  namespace: string,
  key: string,
) {
  const legacy = legacyLinkedLaunchCommandId(namespace, key);
  const receipts = yield* CommandReceipts.CommandReceiptStoreV2;
  // A failed read stops the launch: guessing could create a duplicate thread.
  const accepted = yield* receipts.getByCommandId(legacy).pipe(Effect.mapError(unavailable));
  return Option.isSome(accepted) ? legacy : yield* linkedLaunchCommandId(namespace, key);
});

const linkedWorkflowRun = (
  input: Tool.Parameters<typeof ProjectToolkit.tools.t3_thread_launch>,
  scope: McpInvocationContext.McpInvocationScope,
  linkOrigin: McpToolAccess.StartedModes["linkOrigin"],
) => {
  const client = scope.client;
  const origin = client?.remoteOrigin;
  const workflowRun = input.workflowRun;
  // This is an assertion from the authenticated peer. The origin header binds it to the
  // session, and ordinary forwarding rewrites request keys instead of passing workflow keys.
  return workflowRun !== undefined &&
    scope.thread === undefined &&
    client?.linked === true &&
    linkOrigin?.sessionId === client.sessionId &&
    origin?.linkSessionId === client.sessionId &&
    origin.environmentId === workflowRun.environmentId &&
    origin.environmentId !== scope.environmentId &&
    origin.thread === undefined &&
    client.senderThread === undefined &&
    input.delegatedFrom === undefined &&
    input.clientRequestId !== undefined &&
    /^workflow:[a-f0-9]{64}$/u.test(input.clientRequestId)
    ? workflowRun
    : undefined;
};

export const launchThread = Effect.fn("mcp.launchThread")(function* (
  input: Tool.Parameters<typeof ProjectToolkit.tools.t3_thread_launch>,
  gate: McpToolAccess.ThreadStartGate,
) {
  const keyedCaller = input.clientRequestId === undefined ? undefined : yield* readCaller();
  const modes = yield* gate.start;
  if (input.accountRouting !== undefined) {
    if (modes.linkOrigin !== undefined) return yield* localAccountRoutingRequired();
    if (input.delegatedFrom !== undefined || input.workflowRun !== undefined)
      return yield* new OrchestratorMcpFailure({
        code: "invalid_request",
        message: "A linked delegation or workflow launch requires a concrete modelSelection.",
      });
    if (input.modelSelection === undefined)
      return yield* new OrchestratorMcpFailure({
        code: "invalid_request",
        message:
          "Auto account routing requires an explicit modelSelection with a provider and model.",
      });
    if (
      input.scratch === true ||
      input.workspaceStrategy?.type === "worktree" ||
      input.workspaceStrategy?.type === "auto"
    )
      return yield* existingAccountRoutingWorkspaceRequired();
  }
  // Jones replay owns local keyed launches; linked keyed launches use upstream's hashed id
  // below and keep their origin.
  if (
    keyedCaller !== undefined &&
    input.clientRequestId !== undefined &&
    modes.linkOrigin === undefined
  ) {
    if (
      input.accountRouting === undefined &&
      input.modelSelection?.options?.length &&
      keyedCaller.limits.runtimeMode !== "full-access" &&
      (yield* ClientLaunch.lookupForCaller(keyedCaller, input.clientRequestId)).state === "absent"
    ) {
      const { instanceId, options } = input.modelSelection;
      const providers = yield* (yield* ProviderRegistry.ProviderRegistry).getProviders;
      yield* refuseAgentModePick({
        callerRuntimeMode: keyedCaller.limits.runtimeMode,
        picked: options,
        descriptors: providerOptionDescriptors(
          providers.find((provider) => provider.instanceId === instanceId),
        ),
        held:
          keyedCaller.caller?.modelSelection.instanceId === instanceId
            ? keyedCaller.caller.modelSelection.options
            : undefined,
      });
    }
    return yield* ClientLaunch.launch(
      { ...input, clientRequestId: input.clientRequestId },
      keyedCaller,
      modes,
      gate,
      input.delegatedFrom !== undefined,
    );
  }
  const { runtimeMode, interactionMode, linkOrigin } = modes;

  const scope = yield* McpInvocationContext.McpInvocationContext;
  const context = yield* readCaller();
  const { caller } = context;
  const workflowRun = linkedWorkflowRun(input, scope, linkOrigin);
  // A retry with the same key replays the first launch: the ids derive
  // from the caller and the key, and the launch service replays a
  // command it already accepted.
  const commandId =
    input.clientRequestId === undefined
      ? yield* newCommandId()
      : yield* linkedKeyedLaunchCommandId(scope.requestNamespace, input.clientRequestId);
  const threadId = ThreadId.make(commandId);
  const messageId = MessageId.make(commandId);
  const attachments = input.attachments ?? [];
  if (input.generateTitle === true && !input.message?.trim() && attachments.length === 0)
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message: "Title generation requires a message or attachments. No thread was created.",
    });
  const schemaProblem =
    input.outputSchema === undefined
      ? null
      : outputRequestProblem(input.outputSchema, {
          text: input.message ?? "",
          attachments: input.attachments,
        });
  if (schemaProblem !== null || (input.outputSchema !== undefined && !input.message))
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message: schemaProblem ?? "outputSchema needs a message that asks for the result.",
    });
  if (attachments.some((attachment) => !Claims.attachmentIsPendingUpload(attachment)))
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message: "A new thread accepts only pending attachment uploads.",
    });
  if (
    input.scratch === true &&
    (input.projectId !== undefined || input.workspaceStrategy !== undefined)
  )
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message:
        "scratch:true picks its own project and folder; omit projectId and workspaceStrategy.",
    });
  const projectId =
    input.scratch === true
      ? (yield* ManagedProjectFolders.ManagedProjectFolders.pipe(
          Effect.flatMap((folders) => folders.ensureScratchProject),
          Effect.mapError(
            (error) =>
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: error.message,
              }),
          ),
        )).projectId
      : yield* resolveProjectId(context, input.projectId);
  // A retry of an accepted launch replays it: it keeps the model it started
  // with and skips the checks only a new launch needs, which the project
  // may no longer pass, such as a base branch deleted since.
  const launched =
    input.clientRequestId === undefined
      ? null
      : yield* context.threads.getThreadShell(threadId).pipe(Effect.mapError(unavailable));
  const launchedModel =
    launched === null || launched.deletedAt !== null ? undefined : launched.modelSelection;
  const replaying = launchedModel !== undefined;
  if (launched !== null && replaying && launched.projectId !== projectId)
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message: "A launch retry must use the original project.",
    });
  const project =
    replaying && input.workspaceStrategy?.type !== "existing_worktree"
      ? undefined
      : yield* Project.ProjectService.pipe(
          Effect.flatMap((projects) => projects.getById(projectId)),
          Effect.mapError(unavailable),
          Effect.flatMap(
            Option.match({
              onNone: () =>
                Effect.fail(
                  new OrchestratorMcpFailure({
                    code: "invalid_request",
                    message: "The project was not found.",
                  }),
                ),
              onSome: Effect.succeed,
            }),
          ),
        );
  const workspaceStrategy =
    project !== undefined && input.workspaceStrategy?.type === "existing_worktree"
      ? {
          ...input.workspaceStrategy,
          worktreePath: yield* projectWorktreePath(
            project.workspaceRoot,
            input.workspaceStrategy.worktreePath,
          ),
        }
      : input.workspaceStrategy;
  const cwd =
    workspaceStrategy?.type === "existing_worktree"
      ? workspaceStrategy.worktreePath
      : project?.workspaceRoot;
  const validatedModelSelection =
    !replaying && input.accountRouting === undefined && input.modelSelection !== undefined
      ? yield* validateLaunchModel(context, input.modelSelection, cwd)
      : input.modelSelection;
  const projectSettings =
    replaying || project === undefined
      ? undefined
      : yield* ServerSettings.ServerSettingsService.pipe(
          Effect.flatMap((settings) => settings.getSettings),
          Effect.mapError(unavailable),
          Effect.map((settings) => resolveProjectSettings(settings, projectId, project).settings),
        );
  // An outside caller that names no workspace follows the new-threads-in-a-worktree
  // default, as the composer does.
  const wantsDefaultWorktree =
    workspaceStrategy === undefined &&
    caller === undefined &&
    project !== undefined &&
    projectSettings !== undefined &&
    (projectSettings.defaultThreadEnvMode ??
      (yield* projectFileThreadEnvMode(project.workspaceRoot))) === "worktree";
  if (input.accountRouting !== undefined && wantsDefaultWorktree)
    return yield* existingAccountRoutingWorkspaceRequired();
  const requestedStrategy =
    wantsDefaultWorktree && project !== undefined
      ? yield* defaultWorktreeStrategy(project.workspaceRoot)
      : workspaceStrategy;
  // An omitted startFromOrigin follows the new-worktree setting, as the composer and
  // t3_worktree_handoff do. Otherwise a stale local branch silently becomes the base.
  const strategy =
    projectSettings !== undefined &&
    requestedStrategy?.type === "worktree" &&
    requestedStrategy.startFromOrigin === undefined
      ? {
          ...requestedStrategy,
          startFromOrigin: projectSettings.newWorktreesStartFromOrigin,
        }
      : requestedStrategy;
  // Reject unresolved base refs and blocked branch names before accepting a launch; worktree provisioning happens later.
  if (!replaying && strategy?.type === "worktree")
    yield* ThreadLaunch.ThreadLaunchService.pipe(
      Effect.flatMap((launches) =>
        launches.checkWorktreeBase({ projectId, workspaceStrategy: strategy }),
      ),
      Effect.mapError(
        (error) =>
          new OrchestratorMcpFailure({
            code: "invalid_request",
            message: `${error.message} No thread was created. ${worktreeCheckHint(error, strategy)}`,
          }),
      ),
    );
  // A caller outside a thread has nothing to inherit, so it starts with the
  // project's effective defaults, as the composer does. Its mode stays
  // within the ceiling it was approved with.
  const projectDefaults = caller === undefined ? projectSettings : undefined;
  let startedRuntimeMode =
    projectDefaults !== undefined &&
    input.runtimeMode === undefined &&
    !inheritsLinkedDelegationModes({
      delegatedFrom: input.delegatedFrom,
      linkOrigin,
      narrowedTo: scope.client?.narrowedTo,
    })
      ? yield* resolveRuntimeMode(runtimeMode, projectDefaults.defaultRuntimeMode).pipe(
          Effect.orElseSucceed(() => runtimeMode),
        )
      : runtimeMode;
  let routedSelection;
  if (input.accountRouting !== undefined) {
    if (cwd === undefined || input.modelSelection === undefined) return yield* unavailable();
    routedSelection = yield* resolveAutoLaunchModel({
      caller: context,
      projectId,
      modelSelection: input.modelSelection,
      runtimeMode: startedRuntimeMode,
      interactionMode,
      cwd,
    });
  }
  const modelSelection =
    routedSelection?.modelSelection ??
    validatedModelSelection ??
    launchedModel ??
    caller?.modelSelection ??
    (projectDefaults
      ? resolveNewThreadModelSelection({
          defaultSelection: projectDefaults.defaultModelSelection,
          fallbackSelection: projectDefaults.fallbackModelSelection,
          providers: yield* ProviderRegistry.ProviderRegistry.pipe(
            Effect.flatMap((registry) => registry.getProviders),
          ),
          settings: projectDefaults,
          projectId,
          now: DateTime.toEpochMillis(yield* DateTime.now),
        }).selection
      : null) ??
    undefined;
  if (modelSelection === undefined)
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message:
        "Pass modelSelection: the project has no default model. orchestrator_capabilities lists providers and models.",
    });
  if (!replaying && input.accountRouting === undefined) {
    const providers = yield* (yield* ProviderRegistry.ProviderRegistry).getProviders;
    startedRuntimeMode = yield* resolveRuntimeMode(startedRuntimeMode, input.runtimeMode, {
      modelSelection,
      supportedRuntimeModes: providers.find(
        (provider) => provider.instanceId === modelSelection.instanceId,
      )?.supportedRuntimeModes,
    });
  }
  const originLink = yield* claimForCaller(
    context,
    threadId,
    commandId,
    input.delegatedFrom !== undefined,
  );
  const result = yield* withAccountLaunchBinding(
    ThreadMessageIntake.launchThread({
      commandId,
      threadId,
      projectId,
      title: input.title,
      ...(input.generateTitle === undefined ? {} : { generateTitle: input.generateTitle }),
      ...(input.generateTitle === true ? {} : { titleRenamed: true }),
      modelSelection,
      runtimeMode: startedRuntimeMode,
      interactionMode,
      workspaceStrategy: strategy ?? { type: "root" },
      ...(input.message === undefined && attachments.length === 0
        ? {}
        : {
            initialMessage: {
              messageId,
              ...(caller === undefined ? {} : { senderThreadId: caller.id }),
              text: input.message ?? "",
              attachments,
              ...(input.outputSchema === undefined ? {} : { outputSchema: input.outputSchema }),
            },
          }),
      createdBy: "agent",
      creationSource: "mcp",
      ...(linkOrigin === undefined ? {} : { linkOrigin }),
      // Kept only alongside linkOrigin: thread.create drops it otherwise.
      ...(input.delegatedFrom === undefined ? {} : { delegatedFrom: input.delegatedFrom }),
      ...(workflowRun === undefined ? {} : { workflowRun }),
    }),
    commandId,
    routedSelection?.binding,
  ).pipe(
    Effect.mapError((error) => {
      if (error._tag === "AttachmentClaimError") {
        return new OrchestratorMcpFailure({
          code: "orchestration_error",
          message: error.message,
        });
      }
      if (error._tag === "ThreadManagementProviderInstanceDisabledError") {
        return new OrchestratorMcpFailure({
          code: "invalid_request",
          message: error.message,
        });
      }
      if (error._tag === "ThreadLaunchError" && error.conversationFailure !== undefined) {
        return new OrchestratorMcpFailure({
          code: "orchestration_error",
          message: error.message,
        });
      }
      // The administrator's ceiling refuses the mode; say so rather than "unavailable".
      if (error._tag === "ThreadLaunchError" && isAboveCeilingError(error.cause)) {
        return new OrchestratorMcpFailure({
          code: "invalid_request",
          message: error.cause.message,
        });
      }
      return unavailable();
    }),
  );
  const thread = result.projection.thread;
  const run = result.projection.runs.find((run) => run.userMessageId === messageId);
  return {
    ...(originLink === undefined ? {} : { originLink }),
    threadId: thread.id,
    link: McpInvocationContext.threadLinkFor(scope, thread.id, thread.title),
    projectId: thread.projectId,
    modelSelection: thread.modelSelection,
    runId: run?.id ?? null,
    status: run?.status ?? null,
  };
});

const handlers = (
  launchLanes: KeyedLock.KeyedLock<CommandId>,
): McpToolAccess.Handlers<typeof ProjectToolkit.tools> => ({
  t3_thread_launch: McpToolAccess.startsOrContinuesThreads(
    (input: Tool.Parameters<typeof ProjectToolkit.tools.t3_thread_launch>) => input,
    (input, gate) =>
      input.clientRequestId === undefined
        ? launchThread(input, gate)
        : Effect.gen(function* () {
            const scope = yield* McpInvocationContext.McpInvocationContext;
            const { commandId } = clientLaunchIds(scope.requestNamespace, input.clientRequestId!);
            // Replay the winner of a concurrent admission before looking at mutable usage.
            return yield* launchLanes.withLock(commandId, launchThread(input, gate));
          }),
    // The launched thread carries the caller's link, if it has one.
    "stamped",
    (input) =>
      input.accountRouting === undefined ? Effect.void : Effect.fail(localAccountRoutingRequired()),
  ),
  // Jones hook: capability checks precede every launch-state read or fence write.
  t3_thread_launch_lookup: McpToolAccess.reads((input) =>
    Effect.gen(function* () {
      const caller = yield* readCaller();
      return yield* ClientLaunch.lookupForCaller(caller, input.clientRequestId);
    }),
  ),
  t3_thread_launch_fence: McpToolAccess.writesWithCapability((input) =>
    Effect.gen(function* () {
      const caller = yield* readCaller();
      return yield* ClientLaunch.fenceForCaller(caller, input.clientRequestId);
    }),
  ),
  t3_project_list: McpToolAccess.reads((input) =>
    Effect.gen(function* () {
      const projects = yield* access;
      // Identities are resolved: linked environments find this side's project by repository.
      const page = yield* projects
        .listPage({ cursor: input.cursor ?? 0, limit: input.limit ?? 20 })
        .pipe(Effect.mapError(unavailable));
      return { ...page, projects: page.projects.map(yield* withSavedProjectSettings) };
    }),
  ),
  t3_project_read: McpToolAccess.reads((input) =>
    Effect.gen(function* () {
      const projects = yield* access;
      const result = yield* projects.getById(input.projectId).pipe(Effect.mapError(unavailable));
      if (Option.isNone(result))
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "The project was not found.",
        });
      return (yield* withSavedProjectSettings)(result.value);
    }),
  ),
  t3_project_create: McpToolAccess.writesEnvironment(
    ({ workspaceRoot, ...input }) =>
      Effect.gen(function* () {
        const projects = yield* Project.ProjectService;
        if (workspaceRoot === undefined) {
          // Only updates record model defaults; reject defaults that creation would silently drop.
          if (
            input.scripts !== undefined ||
            input.trackedFolders !== undefined ||
            input.createWorkspaceRootIfMissing !== undefined ||
            input.defaultModelSelection !== undefined
          )
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message:
                "A project started from its title takes only a title; set scripts or defaultModelSelection afterwards with t3_project_update.",
            });
          const folders = yield* ManagedProjectFolders.ManagedProjectFolders;
          const created = yield* folders.createNamedProject({ name: input.title }).pipe(
            Effect.mapError(
              (error) =>
                new OrchestratorMcpFailure({
                  code: "orchestration_error",
                  message: error.message,
                }),
            ),
          );
          const project = yield* projects
            .getById(created.projectId)
            .pipe(
              Effect.mapError(unavailable),
              Effect.flatMap(
                Option.match({ onNone: () => Effect.fail(unavailable()), onSome: Effect.succeed }),
              ),
            );
          return {
            ...(yield* withSavedProjectSettingsAfterCreate)(project),
            ...(created.commitError === undefined ? {} : { commitError: created.commitError }),
          };
        }
        if (input.defaultModelSelection !== undefined && input.defaultModelSelection !== null)
          return yield* new OrchestratorMcpFailure({
            code: "invalid_request",
            message:
              "Creating a project does not set its default model; set defaultModelSelection afterwards with t3_project_update.",
          });
        const commandId = yield* newCommandId();
        const created = yield* projects
          .create({ ...input, workspaceRoot, commandId, projectId: ProjectId.make(commandId) })
          .pipe(Effect.mapError(projectFailure));
        return (yield* withSavedProjectSettingsAfterCreate)(created);
      }),
    // Linked work may bring the repository it works in; an existing
    // registered workspace is still refused.
    { linkedCallers: "allowed" },
  ),
  t3_project_update: McpToolAccess.writesEnvironment((input) =>
    Effect.gen(function* () {
      const projects = yield* Project.ProjectService;
      const updated = yield* projects
        .update({ ...input, commandId: yield* newCommandId() })
        .pipe(Effect.mapError(projectFailure));
      return (yield* withSavedProjectSettings)(updated);
    }),
  ),
  t3_project_delete: McpToolAccess.writesEnvironment((input) =>
    Effect.gen(function* () {
      const folders = yield* ManagedProjectFolders.ManagedProjectFolders;
      if (yield* folders.isScratchProject(input.projectId))
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message:
            '"No project" is the built-in scratch project and cannot be deleted through MCP. The user can remove it in Settings.',
        });
      const projects = yield* Project.ProjectService;
      const clones = yield* ProjectCloneTracker.ProjectCloneTracker;
      return yield* mutateProjectWithClone(projects, clones, {
        ...input,
        type: "project.delete",
        commandId: yield* newCommandId(),
      }).pipe(Effect.mapError(projectFailure));
    }),
  ),
  t3_project_clone: McpToolAccess.writesEnvironment(
    (input) =>
      Effect.gen(function* () {
        const repositories = yield* Repositories.SourceControlRepositoryService;
        // Agents' MCP calls have their own long budget, and an aborted call interrupts git.
        return yield* repositories.cloneRepository(input, { timeoutMs: null }).pipe(
          Effect.mapError(
            (error) =>
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: error.detail,
              }),
          ),
        );
      }),
    // An existing destination is never adopted.
    { linkedCallers: "allowed" },
  ),
});

export const layer = McpToolAccess.toLayer(
  ProjectToolkit,
  Effect.map(KeyedLock.make<CommandId>(), handlers),
);
