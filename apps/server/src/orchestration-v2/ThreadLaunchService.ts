import * as NativeBootstrap from "../jones/nativeCreation/NativeBootstrapDispatch.ts";
import * as NativeAuthority from "../jones/nativeCreation/NativeCreationAuthority.ts";
import * as NativeWorkspace from "../jones/nativeCreation/NativeCreationWorkspacePreparation.ts";
import type * as NativeWorkspaceTypes from "../jones/nativeCreation/NativeCreationWorkspaceTypes.ts";
import * as ServerConfig from "../config.ts";
import { EnvironmentAuthenticatedPrincipal } from "@t3tools/contracts";
import * as SqlClient from "effect/sql/SqlClient";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as LaunchPreparation from "../jones/ordinaryCheckout/LaunchPreparation.ts";
import * as LegacyBootstrapLaunch from "../jones/legacyBootstrap/LegacyBootstrapLaunch.ts";
import * as RepoFreshnessLaunch from "../jones/git/repoFreshnessLaunch.ts";
import * as ConversationPolicy from "../jones/conversationWorkspace/ConversationPolicy.ts";
import * as AutoLaunchStrategy from "../jones/conversationWorkspace/LaunchStrategy.ts";
import * as WorkspaceProvenance from "../jones/workspaceLifecycle/Provenance.ts";
import * as ThreadMetadataReads from "../jones/orchestration/threadMetadataReads.ts";
import * as IdleWorktreeAdmission from "../jones/orchestration/IdleWorktreeAdmission.ts";
import { taskFolderProgressId } from "../jones/orchestration/WorkspacePreparationRead.ts";
import {
  launchDeliveryEvidence,
  readLaunchMessage,
  recordedLaunchClaim,
  type LaunchDeliveryRequest,
} from "../jones/orchestration/deliveryEvidence.ts";
import {
  initialMessageCommandId,
  type LaunchDeliveryEvidence,
} from "@t3tools/contracts/jones/deliveryEvidence";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { canonicalLegacyPayload, legacyPayloadHash } from "./LegacyBootstrap.ts";
import type { LegacyPreflightOutcome } from "./LegacyBootstrapPreflight.ts";
import * as CheckoutIntent from "../jones/ordinaryCheckout/CheckoutIntent.ts";
import { CheckoutError } from "../jones/ordinaryCheckout/CheckoutProtocol.ts";
import type { OrchestrationV2ServerCommand } from "@t3tools/contracts";
import * as WorktreeSetupTracker from "../project/WorktreeSetupTracker.ts";
import * as ProjectCloneTracker from "../project/ProjectCloneTracker.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import {
  CommandId,
  type ChatAttachment,
  type MessageId,
  type ModelSelection,
  type OrchestrationV2Actor,
  type OrchestrationV2Command,
  type OrchestrationV2LegacyBootstrapPolicy,
  type OrchestrationV2LegacyPreflightBinding,
  type OrchestrationV2CreationSource,
  type OrchestrationV2DelegatedFrom,
  type OrchestrationV2WorkflowOrigin,
  type OrchestrationV2LinkOrigin,
  type OrchestrationV2ProviderThreadNativeMetadata,
  type OrchestrationV2ThreadProjection,
  type ProviderDriverKind,
  type ProviderInteractionMode,
  ProjectId,
  type RunId,
  type RuntimeMode,
  type ScheduledTaskId,
  type GitManagerServiceError,
  TaskFolderError,
  TextGenerationError,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import {
  buildTemporaryWorktreeBranchName,
  flattenTemporaryWorktreeBranchName,
  formatGeneratedBranchName,
  isTemporaryWorktreeBranch,
  resolveDefaultWorktreeBaseBranch,
} from "@t3tools/shared/git";

import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import * as GitWorkflow from "../git/GitWorkflowService.ts";
import type * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import { parseGitCloneProgressLine } from "../project/gitCloneProgress.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "../project/ProjectSetupScriptRunner.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as TextGeneration from "../textGeneration/TextGeneration.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as EventSink from "./EventSink.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Orchestrator from "./Orchestrator.ts";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import { randomUuidV4 } from "@t3tools/provider-core/server/randomUuid";
import { DispatchModeLimit, exceededDispatchModeLimit } from "./DispatchModeLimit.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";
import type { RecordedRun } from "./RecordedTypes.ts";
import * as TaskFolders from "../workspace/TaskFolders.ts";

// The agent waits for its branch name; a rate-limited provider would hold it for minutes.
const BRANCH_NAME_TIMEOUT = "20 seconds";

export type ThreadLaunchWorkspaceStrategy =
  | {
      /** Conversation-first when the project and provider allow it, otherwise an eager worktree. */
      readonly type: "auto";
      readonly branch?: string | undefined;
      readonly baseRef?: string | undefined;
    }
  | { readonly type: "root"; readonly branch?: string | undefined }
  | {
      readonly type: "existing_worktree";
      readonly worktreePath: string;
      readonly branch?: string | undefined;
    }
  | {
      readonly type: "worktree";
      /** Omitted: preparation bases the worktree on the repository's default or checked-out branch. */
      readonly baseRef?: string | undefined;
      readonly branch?: string | undefined;
      readonly startFromOrigin?: boolean | undefined;
      readonly baseFreshness?: "verified" | "fetch-now" | undefined;
      readonly repositories?: ReadonlyArray<{ readonly path: string }> | undefined;
    };

export interface ThreadLaunchInitialMessage {
  readonly messageId?: MessageId;
  readonly scheduledTaskId?: ScheduledTaskId;
  readonly senderThreadId?: ThreadId;
  readonly text: string;
  readonly titleSeed?: string;
  readonly sourcePlanRef?: Extract<
    OrchestrationV2Command,
    { type: "message.dispatch" }
  >["sourcePlanRef"];
  readonly attachments: ReadonlyArray<ChatAttachment>;
  readonly context?: import("@t3tools/contracts").OrchestrationMessageContext | undefined;
  /** Ask the first run for a result as data matching this JSON Schema. */
  readonly outputSchema?: Readonly<Record<string, Schema.Json>> | undefined;
}

export interface ThreadLaunchInput {
  readonly commandId: CommandId;
  /** Server-owned legacy compatibility inputs; public launch schemas do not decode them. */
  readonly preparationReleaseCommandId?: CommandId;
  readonly legacyBootstrap?: OrchestrationV2LegacyBootstrapPolicy;
  /** The receiving transport verified write access for this foreground request. */
  readonly syncPrimaryOnFreshFetch?: true;
  readonly runSetupScript?: boolean;
  readonly threadId?: ThreadId;
  readonly reuseExistingThread?: boolean;
  readonly projectId: ProjectId;
  readonly title: string;
  readonly generateTitle?: boolean;
  /** The caller chose `title`, so a first message keeps it. */
  readonly titleRenamed?: boolean;
  readonly modelSelection: ModelSelection;
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: ProviderInteractionMode;
  readonly workspaceStrategy: ThreadLaunchWorkspaceStrategy;
  readonly initialMessage?: ThreadLaunchInitialMessage;
  readonly importedNativeThread?: {
    readonly ref: {
      readonly driver: ProviderDriverKind;
      readonly nativeId: string;
      readonly strength: "strong";
    };
    readonly metadata?: OrchestrationV2ProviderThreadNativeMetadata;
  };
  readonly createdBy: OrchestrationV2Actor;
  readonly creationSource: OrchestrationV2CreationSource;
  /** The linked environment this launch is for, if one asked for it. */
  readonly linkOrigin?: OrchestrationV2LinkOrigin;
  /** The parent there, when that environment's agent delegated this as a task. */
  readonly delegatedFrom?: OrchestrationV2DelegatedFrom;
  readonly workflowRun?: OrchestrationV2WorkflowOrigin;
  /** Jones hook: a replay leaves preparation that already recorded durable progress to its owner. */
  readonly holdPreparationAfterProgress?: true;
}

/** The durable hold of a worktree launched without a message, with its retained owner. */
interface IdlePreparation {
  readonly identity: IdleWorktreeAdmission.Identity;
  readonly store: IdleWorktreeAdmission.Store;
}

type PreparationInput = Pick<
  ThreadLaunchInput,
  | "commandId"
  | "projectId"
  | "workspaceStrategy"
  | "initialMessage"
  | "runSetupScript"
  | "syncPrimaryOnFreshFetch"
  | "linkOrigin"
> & {
  readonly reusedWorktree?: { readonly baseRef: string | undefined };
  readonly askedByAgent?: { readonly threadId: ThreadId | undefined };
  readonly nativeCreation?: boolean;
  readonly idleWorktreeAdmission?: IdlePreparation;
  readonly freshTaskFolderLaunch?: boolean;
};

export interface ThreadLaunchPreparationInput {
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly runId: RunId;
}

export interface ThreadLaunchResult {
  readonly threadId: ThreadId;
  readonly projection: OrchestrationV2ThreadProjection;
  /** The launch's last write (the initial message, else the thread claim) was accepted before this call. */
  readonly resumed: boolean;
  readonly sequence?: number;
  /** Exact accepted receipt of the original legacy release command C. */
  readonly legacyReleaseSequence?: number;
}

export class ThreadLaunchError extends Schema.TaggedError<ThreadLaunchError>()(
  "ThreadLaunchError",
  {
    operation: Schema.Literals([
      "resolve-project",
      "read-receipt",
      "generate-metadata",
      "provision-worktree",
      "run-setup-script",
      "create-thread",
      "update-thread",
      "dispatch-message",
      "release-run",
      "fail-run",
    ]),
    commandId: CommandId,
    projectId: ProjectId,
    threadId: Schema.optional(ThreadId),
    cause: Schema.Defect(),
    bootstrapThreadDisposition: Schema.optional(Schema.Literal("deleted")),
    conversationFailure: Schema.optional(
      Schema.Struct({
        detail: Schema.String,
        stage: Schema.Literals(["preflight", "allocation"]),
        cleanup: Schema.optional(Schema.Literals(["deleted", "preserved", "unknown"])),
      }),
    ),
  },
) {
  override get message(): string {
    if (this.conversationFailure !== undefined) {
      const { stage, detail, cleanup } = this.conversationFailure;
      const prefix =
        stage === "preflight" ? "On write preflight failed." : "On write allocation failed.";
      const followUp =
        cleanup === "deleted"
          ? " The empty thread was removed. Start a new launch to retry."
          : cleanup === "unknown"
            ? " The thread cleanup outcome is unresolved."
            : cleanup === "preserved"
              ? " The thread was preserved."
              : "";
      const thread = this.threadId === undefined ? "" : ` Thread ${this.threadId}.`;
      return `${prefix} ${detail}${followUp}${thread}`;
    }
    return `Thread launch ${this.commandId} failed during ${this.operation}.`;
  }
}

export class ThreadLaunchBaseRefError extends Schema.TaggedError<ThreadLaunchBaseRefError>()(
  "ThreadLaunchBaseRefError",
  {
    projectId: ProjectId,
    projectTitle: Schema.String,
    workspaceRoot: Schema.String,
    baseRef: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Base ref "${this.baseRef}" does not resolve to a commit in project "${this.projectTitle}" (${this.workspaceRoot}).`;
  }
}

export class ThreadLaunchBranchExistsError extends Schema.TaggedError<ThreadLaunchBranchExistsError>()(
  "ThreadLaunchBranchExistsError",
  {
    projectId: ProjectId,
    projectTitle: Schema.String,
    branch: Schema.String,
  },
) {
  override get message(): string {
    return `Branch "${this.branch}" already exists in project "${this.projectTitle}".`;
  }
}

export class ThreadLaunchBranchConflictError extends Schema.TaggedError<ThreadLaunchBranchConflictError>()(
  "ThreadLaunchBranchConflictError",
  {
    projectId: ProjectId,
    projectTitle: Schema.String,
    branch: Schema.String,
    conflictingBranch: Schema.String,
  },
) {
  override get message(): string {
    return `Branch "${this.branch}" cannot be created in project "${this.projectTitle}": branch "${this.conflictingBranch}" exists.`;
  }
}

export class ThreadLaunchNotRepositoryError extends Schema.TaggedError<ThreadLaunchNotRepositoryError>()(
  "ThreadLaunchNotRepositoryError",
  {
    projectId: ProjectId,
    projectTitle: Schema.String,
    workspaceRoot: Schema.String,
  },
) {
  override get message(): string {
    return `Project "${this.projectTitle}" (${this.workspaceRoot}) is not a Git repository.`;
  }
}

export class ThreadLaunchService extends Context.Service<
  ThreadLaunchService,
  {
    readonly dispatchNativeBootstrap?: (
      submission: unknown,
    ) => Effect.Effect<
      NativeBootstrap.NativeBootstrapDispatchResult,
      NativeAuthority.NativeCreationAuthorityError,
      EnvironmentAuthenticatedPrincipal
    >;
    readonly prepareNativeWorkspace?: (
      input: NativeWorkspace.NativeWorkspaceInput,
    ) => Effect.Effect<
      NativeWorkspaceTypes.NativeWorkspaceVerified,
      NativeWorkspace.NativeWorkspaceError
    >;
    readonly preflightLegacyBootstrap: (
      binding: OrchestrationV2LegacyPreflightBinding,
    ) => Effect.Effect<LegacyPreflightOutcome, ThreadLaunchError>;
    readonly launch: (
      input: ThreadLaunchInput,
    ) => Effect.Effect<
      ThreadLaunchResult,
      ThreadLaunchError | ThreadManagement.ThreadManagementProviderInstanceDisabledError
    >;
    /** Jones hook: read-only delivery evidence of an earlier launch, from its durable receipts. */
    readonly getDeliveryEvidence: (
      input: LaunchDeliveryRequest,
    ) => Effect.Effect<LaunchDeliveryEvidence>;
    /** Check a new worktree's base ref and branch name before creation for early errors; launch otherwise accepts and reports these failures inside the thread. */
    readonly checkWorktreeBase: (
      input: Pick<ThreadLaunchInput, "projectId" | "workspaceStrategy"> & {
        /** Skips the check when this launch was already accepted, so its retry replays. */
        readonly commandId?: CommandId;
      },
    ) => Effect.Effect<
      void,
      | ThreadLaunchBaseRefError
      | ThreadLaunchBranchExistsError
      | ThreadLaunchBranchConflictError
      | ThreadLaunchNotRepositoryError
      | TaskFolderError
    >;
    /** True only when the project's folder is known not to be a Git repository. */
    readonly isNonGitProject: (projectId: ProjectId) => Effect.Effect<boolean>;
    readonly retryPreparation: (
      input: ThreadLaunchPreparationInput,
    ) => Effect.Effect<
      Orchestrator.OrchestratorV2DispatchResult,
      | Orchestrator.OrchestratorV2Error
      | ThreadManagement.ThreadManagementProviderInstanceDisabledError
    >;
    // Jones hook: read-only observation of the existing preparation reservation.
    readonly preparationActive: (commandId: CommandId) => Effect.Effect<boolean>;
    readonly prepareRun: (input: ThreadLaunchPreparationInput) => Effect.Effect<void>;
  }
>()("t3/orchestration-v2/ThreadLaunchService") {}

const isThreadLaunchError = Schema.is(ThreadLaunchError);

function failureDetail(error: unknown): string {
  if (isThreadLaunchError(error)) {
    const cause = error.cause;
    const detail = cause instanceof Error ? cause.message : String(cause);
    return `Workspace preparation failed during ${error.operation.replaceAll("-", " ")}: ${detail}`;
  }
  return `Workspace preparation failed: ${error instanceof Error ? error.message : String(error)}`;
}

const make = Effect.fn("ThreadLaunchService.make")(function* (
  launchPreparation: Option.Option<LaunchPreparation.LaunchPreparation["Service"]>,
) {
  const nativeBootstrapContext = yield* Effect.context<never>();
  const nativeBootstrapConfig = yield* Effect.serviceOption(ServerConfig.ServerConfig);
  const nativeBootstrapPath = yield* Effect.serviceOption(Path.Path);
  const nativeBootstrapBootId = yield* randomUuidV4;
  const nativeWorkspace = yield* Effect.serviceOption(
    NativeWorkspace.NativeCreationWorkspacePreparation,
  );
  const projects = yield* ProjectService.ProjectService;
  const setupTracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
  const cloneTracker = yield* ProjectCloneTracker.ProjectCloneTracker;
  const terminals = yield* TerminalManager.TerminalManager;
  const git = yield* GitWorkflow.GitWorkflowService;
  const checkpointStore = yield* CheckpointStore.CheckpointStore;
  const setupScripts = yield* ProjectSetupScriptRunner.ProjectSetupScriptRunner;
  const providerRegistry = yield* ProviderRegistry.ProviderRegistry;
  const serverSettings = yield* ServerSettings.ServerSettingsService;
  const textGeneration = yield* TextGeneration.TextGeneration;
  const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
  const ids = yield* IdAllocator.IdAllocatorV2;
  const threads = yield* ThreadManagement.ThreadManagementService;
  // Jones retained owners: provenance capture, `auto` resolution, indexed metadata reads.
  const captureProvenance = yield* WorkspaceProvenance.optionalCapture;
  const autoStrategy = yield* AutoLaunchStrategy.make;
  const resolveFreshWorktreeBase = yield* RepoFreshnessLaunch.make();
  const metadataReads = yield* Effect.serviceOption(ThreadMetadataReads.ThreadMetadataReads);
  // Retain the service's database and lifetime. Lazy legacy construction must
  // not borrow a request fiber's context when it first executes.
  const legacySql = yield* Effect.serviceOption(SqlClient.SqlClient);
  const legacyOutbox = yield* Effect.serviceOption(EffectOutbox.EffectOutboxV2);
  const legacyContext =
    yield* Effect.context<
      Exclude<
        Effect.Services<typeof LegacyBootstrapLaunch.make>,
        SqlClient.SqlClient | EffectOutbox.EffectOutboxV2
      >
    >();
  const legacyBootstrap =
    Option.isSome(legacySql) && Option.isSome(legacyOutbox)
      ? Option.some(
          yield* Effect.cached(
            LegacyBootstrapLaunch.make.pipe(
              Effect.provideContext(
                legacyContext.pipe(
                  Context.add(SqlClient.SqlClient, legacySql.value),
                  Context.add(EffectOutbox.EffectOutboxV2, legacyOutbox.value),
                ),
              ),
            ),
          ),
        )
      : Option.none();
  const eventSink = yield* EventSink.EventSinkV2;
  const idleWorktrees = eventSink.idleWorktreeAdmission;
  const managedFolders = yield* ManagedProjectFolders.ManagedProjectFolders;
  const taskFolders = yield* TaskFolders.TaskFolders;
  const ordinaryIntent = yield* Effect.serviceOption(CheckoutIntent.CheckoutIntent);
  const requirePreparationGraph = Effect.gen(function* () {
    if (Option.isSome(ordinaryIntent) && Option.isNone(launchPreparation))
      return yield* new CheckoutError({ reason: "unavailable" });
  });
  const dispatchAccepted = (command: OrchestrationV2ServerCommand, projectRoot: string) =>
    Option.isSome(launchPreparation) &&
    !(command.type === "thread.create" && command.importedNativeThread !== undefined)
      ? launchPreparation.value.accept(command, [projectRoot], threads.dispatch)
      : threads.dispatch(command);
  // A project-folder launch that names no branch records the folder's checkout, as the composer does.
  const launchBranch = (strategy: ThreadLaunchWorkspaceStrategy, workspaceRoot: string) =>
    strategy.type === "root" && strategy.branch === undefined
      ? git.currentBranch(workspaceRoot).pipe(Effect.orElseSucceed(() => null))
      : Effect.succeed(strategy.branch ?? null);
  const maxRuntimeMode = serverSettings.managedPolicy?.limits?.maxRuntimeMode;
  const projectCommandsFor = Effect.fn("ThreadLaunchService.projectCommandsFor")(function* (
    input: PreparationInput,
    threadId: ThreadId,
  ) {
    const liveThread = (id: ThreadId) =>
      threads.getThreadShell(id).pipe(
        Effect.map((shell) => (shell === null || shell.deletedAt !== null ? null : shell)),
        Effect.orElseSucceed(() => null),
      );
    const target = yield* liveThread(threadId);
    if (
      target === null ||
      TaskFolders.projectCommandsForLink(target.linkOrigin) !== "permitted" ||
      TaskFolders.projectCommandsForLink(input.linkOrigin) !== "permitted"
    )
      return "request-not-permitted" as const;
    if (input.askedByAgent === undefined) return "permitted" as const;
    const asker =
      input.askedByAgent.threadId === undefined
        ? target
        : yield* liveThread(input.askedByAgent.threadId);
    return TaskFolders.projectCommandsForAgent(
      asker?.linkOrigin,
      asker ?? { runtimeMode: "approval-required", interactionMode: "default" },
      maxRuntimeMode,
    );
  });
  const askedByAgent = (
    thread: Pick<OrchestrationV2ThreadProjection["thread"], "createdBy">,
    message: OrchestrationV2ThreadProjection["messages"][number] | undefined,
  ): Pick<PreparationInput, "askedByAgent"> =>
    (message?.createdBy ?? thread.createdBy) === "agent"
      ? { askedByAgent: { threadId: message?.senderThreadId } }
      : {};
  const preparationScope = yield* Scope.make("sequential");
  const scheduledLaunches = yield* Ref.make<ReadonlySet<CommandId>>(new Set());
  yield* Effect.addFinalizer(() => Scope.close(preparationScope, Exit.void));

  const mapError =
    (
      input: Pick<PreparationInput, "commandId" | "projectId">,
      operation: ThreadLaunchError["operation"],
      threadId?: ThreadId,
    ) =>
    (cause: unknown) =>
      new ThreadLaunchError({
        operation,
        commandId: input.commandId,
        projectId: input.projectId,
        ...(threadId === undefined ? {} : { threadId }),
        cause,
      });

  const mapConversationFailure = (
    input: ThreadLaunchInput,
    cause: ConversationPolicy.ConversationWorkspaceError,
    stage: "preflight" | "allocation",
    threadId?: ThreadId,
    cleanup?: "deleted" | "preserved" | "unknown",
  ) =>
    new ThreadLaunchError({
      operation: "provision-worktree",
      commandId: input.commandId,
      projectId: input.projectId,
      ...(threadId === undefined ? {} : { threadId }),
      cause,
      conversationFailure: {
        stage,
        detail: cause.message,
        ...(cleanup === undefined ? {} : { cleanup }),
      },
    });

  const readIdleWorktree = (input: ThreadLaunchInput, threadId: ThreadId) =>
    idleWorktrees === undefined
      ? Effect.succeed(null)
      : idleWorktrees
          .read(threadId)
          .pipe(Effect.mapError(mapError(input, "read-receipt", threadId)));

  const resolveLaunchProject = (input: ThreadLaunchInput) =>
    projects.getById(input.projectId).pipe(
      Effect.mapError(mapError(input, "resolve-project")),
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.fail(mapError(input, "resolve-project")("Project not found.")),
          onSome: Effect.succeed,
        }),
      ),
    );

  const recordBranchRenameFailure = Effect.fn("ThreadLaunchService.recordBranchRenameFailure")(
    function* (threadId: ThreadId, runId: RunId, branch: string, cause: Cause.Cause<unknown>) {
      const error = Cause.squash(cause);
      const now = yield* DateTime.now;
      yield* eventSink.write({
        events: [
          {
            id: yield* ids.allocate.event({ threadId }),
            type: "turn-item.updated",
            threadId,
            runId,
            occurredAt: now,
            payload: {
              id: ids.derive.runSignalTurnItem({ runId, signal: "branch-rename" }),
              threadId,
              runId,
              nodeId: null,
              providerThreadId: null,
              providerTurnId: null,
              nativeItemRef: null,
              parentItemId: null,
              // EventSink allocates the real position, after the run's existing items.
              ordinal: 0,
              status: "completed",
              title: "Branch not renamed",
              startedAt: now,
              completedAt: now,
              updatedAt: now,
              type: "system_notice",
              message: `Could not rename the worktree branch, so it stays ${branch}. ${error instanceof Error ? error.message : String(error)}`,
            },
          },
        ],
      });
    },
    Effect.catchCause((cause) =>
      Effect.logWarning("Could not record the branch rename failure", { cause }),
    ),
  );

  const readReceipt = (
    input: Pick<PreparationInput, "commandId" | "projectId"> & {
      readonly threadId?: ThreadId | undefined;
    },
    commandId: CommandId,
  ) =>
    receipts
      .getByCommandId(commandId)
      .pipe(Effect.mapError(mapError(input, "read-receipt", input.threadId)));

  const validateReusableThread = Effect.fn("ThreadLaunchService.validateReusableThread")(function* (
    input: ThreadLaunchInput,
    threadId: ThreadId,
  ) {
    const projection = yield* threads
      .getThreadRecords(threadId, ["runs"])
      .pipe(Effect.mapError(mapError(input, "update-thread", threadId)));
    if (
      projection.thread.projectId !== input.projectId ||
      projection.thread.archivedAt !== null ||
      projection.thread.deletedAt !== null ||
      (yield* threads
        .getMessageCount(threadId)
        .pipe(Effect.mapError(mapError(input, "update-thread", threadId)))) > 0 ||
      projection.runs.length > 0
    ) {
      return yield* mapError(
        input,
        "update-thread",
        threadId,
      )("Only an empty active thread in the target project can change workspace during launch.");
    }
  });

  const checkWorktreeBase: ThreadLaunchService["Service"]["checkWorktreeBase"] = Effect.fn(
    "ThreadLaunchService.checkWorktreeBase",
  )(function* (input) {
    const strategy = input.workspaceStrategy;
    if (strategy.type !== "worktree") return;
    const commandId = input.commandId;
    const [isScratch, receipt, projectOption] = yield* Effect.all(
      [
        managedFolders.isScratchProject(input.projectId),
        commandId === undefined
          ? Effect.succeedNone
          : receipts.getByCommandId(commandId).pipe(Effect.orElseSucceed(() => Option.none())),
        projects.getById(input.projectId).pipe(Effect.orElseSucceed(() => Option.none())),
      ],
      { concurrency: "unbounded" },
    );
    // launch() turns a Scratch worktree request into a root launch.
    if (isScratch) return;
    if (Option.isSome(receipt) && receipt.value.status === "accepted") return;
    const project = Option.getOrUndefined(projectOption);
    if (project === undefined) return;
    const cwd = project.workspaceRoot;
    const taskPlan = yield* taskFolders.plan({
      projectId: project.id,
      repositories: strategy.repositories,
    });
    if (
      taskPlan !== null &&
      (Option.isSome(launchPreparation) || strategy.baseFreshness !== undefined)
    ) {
      return yield* new TaskFolderError({
        operation: "plan",
        detail:
          "This preparation requires verified custody and base selection for every task folder checkout.",
      });
    }
    // Each repository of a plain project folder starts from its own default branch.
    if (taskPlan !== null && !taskPlan.projectIsRepository) return;
    const { baseRef, branch } = strategy;
    // Errors keep their order below: missing repository, then base ref, then branch exists or conflicts.
    const [repositoryRoot, hasBaseCommit, localBranches] = yield* Effect.all(
      [
        git.repositoryRoot(cwd).pipe(Effect.orElseSucceed(() => undefined)),
        // An omitted base resolves during preparation. `:/text` is a commit search,
        // which cannot be asked for a commit type.
        baseRef === undefined || baseRef.startsWith(":")
          ? Effect.succeed(true)
          : // `git worktree add` reads `-` as the previous checkout.
            git
              .hasCommit({ cwd, refName: baseRef === "-" ? "@{-1}" : baseRef })
              .pipe(Effect.orElseSucceed(() => true)),
        branch !== undefined
          ? git
              .listLocalBranchNames(cwd)
              .pipe(Effect.orElseSucceed((): ReadonlyArray<string> => []))
          : Effect.succeed<ReadonlyArray<string>>([]),
      ],
      { concurrency: "unbounded" },
    );
    if (repositoryRoot === null)
      return yield* new ThreadLaunchNotRepositoryError({
        projectId: project.id,
        projectTitle: project.title,
        workspaceRoot: cwd,
      });
    const baseResolves =
      hasBaseCommit ||
      strategy.baseFreshness !== undefined ||
      (taskPlan !== null && strategy.startFromOrigin === true) ||
      baseRef === undefined ||
      (yield* Effect.gen(function* () {
        if (strategy.startFromOrigin === true) {
          const remote = yield* git.resolveWorktreeBaseRemote({ cwd, refName: baseRef });
          // The remote may have deleted the branch; ask it rather than trusting the name.
          if (
            remote !== null &&
            (yield* git.fetchWorktreeBaseCommit({ cwd, refName: baseRef, remote })) !== null
          )
            return true;
        }
        // git worktree add accepts remote-tracking branches; let Git decide when any matching ref exists.
        if (yield* git.hasRefNamed({ cwd, refName: baseRef })) return true;
        return !(yield* git.hasCommit({ cwd, refName: "HEAD" }));
      }).pipe(Effect.orElseSucceed(() => true)));
    if (!baseResolves && baseRef !== undefined)
      return yield* new ThreadLaunchBaseRefError({
        projectId: project.id,
        projectTitle: project.title,
        workspaceRoot: project.workspaceRoot,
        baseRef,
      });
    if (branch === undefined) return;
    if (localBranches.includes(branch))
      return yield* new ThreadLaunchBranchExistsError({
        projectId: project.id,
        projectTitle: project.title,
        branch,
      });
    // Provisioning flattens temporary names that collide, so only explicit names are refused.
    if (isTemporaryWorktreeBranch(branch)) return;
    const conflictingBranch = localBranches.find(
      (existing) => existing.startsWith(`${branch}/`) || branch.startsWith(`${existing}/`),
    );
    if (conflictingBranch !== undefined)
      return yield* new ThreadLaunchBranchConflictError({
        projectId: project.id,
        projectTitle: project.title,
        branch,
        conflictingBranch,
      });
  });

  const isNonGitProject: ThreadLaunchService["Service"]["isNonGitProject"] = Effect.fn(
    "ThreadLaunchService.isNonGitProject",
  )(function* (projectId) {
    const project = yield* projects
      .getById(projectId)
      .pipe(Effect.orElseSucceed(() => Option.none()));
    if (Option.isNone(project)) return false;
    // Unlike `git.repositoryRoot`, this fails on a repository Git cannot read, which is not "no repository".
    return yield* checkpointStore.isGitRepository(project.value.workspaceRoot).pipe(
      Effect.map((isRepository) => !isRepository),
      Effect.orElseSucceed(() => false),
    );
  });

  /**
   * A recorded worktree strategy can outlive the ref it names: tasks saved before
   * the base followed the repo's default branch kept `"main"`, and a base branch
   * can be deleted after it was recorded. Re-base on the remote's default branch,
   * then on the checked-out commit, instead of failing every launch.
   */
  const worktreeFallbackBase = Effect.fn("ThreadLaunchService.worktreeFallbackBase")(function* (
    cwd: string,
    remoteName: string | null,
  ) {
    if (remoteName !== null) {
      const defaultBranch = yield* git.resolveDefaultBranchName(cwd, remoteName);
      if (defaultBranch !== null) {
        // Fetch freshens the default branch the way the recorded base would have been.
        const remoteBase = yield* git
          .fetchWorktreeBaseCommit({
            cwd,
            refName: defaultBranch,
            remote: { remoteName, branchName: defaultBranch },
          })
          .pipe(
            Effect.map((base) => ({ fetchFailed: false as const, base })),
            Effect.orElseSucceed(() => ({ fetchFailed: true as const, base: null })),
          );
        if (remoteBase.base !== null) {
          return {
            startRef: remoteBase.base.commitSha,
            baseRefName: `refs/remotes/${remoteBase.base.remoteRefName}`,
          };
        }
        // A completed fetch that found no branch means the branch is gone and its
        // tracking ref is stale; only a failed fetch can fall back to it.
        if (remoteBase.fetchFailed) {
          const trackingRef = `refs/remotes/${remoteName}/${defaultBranch}`;
          if (yield* git.hasCommit({ cwd, refName: trackingRef })) {
            return { startRef: trackingRef, baseRefName: trackingRef };
          }
        }
      }
    }
    const currentBranch = yield* git.currentBranch(cwd);
    if (currentBranch !== null) {
      return { startRef: currentBranch, baseRefName: currentBranch };
    }
    return (yield* git.hasCommit({ cwd, refName: "HEAD" }))
      ? { startRef: "HEAD", baseRefName: "HEAD" }
      : null;
  });

  /** A worktree launched without a base starts from the default, else the checked-out, branch. */
  const defaultWorktreeBase = Effect.fn("ThreadLaunchService.defaultWorktreeBase")(function* (
    input: PreparationInput,
    cwd: string,
    threadId: ThreadId,
  ) {
    const refs = yield* git
      .listRefs({ cwd, limit: 100 })
      .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
    const automaticBase = resolveDefaultWorktreeBaseBranch(refs.refs);
    if (!refs.isRepo || automaticBase === null)
      return yield* mapError(
        input,
        "provision-worktree",
        threadId,
      )(
        refs.isRepo
          ? "No default or checked-out branch is available. Select a base branch before retrying."
          : "New worktree mode requires a Git repository.",
      );
    yield* setupTracker.update(threadId, (snapshot) => ({ ...snapshot, baseRef: automaticBase }));
    return automaticBase;
  });

  const prepareInBackground = Effect.fn("ThreadLaunchService.prepareInBackground")(function* (
    input: PreparationInput,
    threadId: ThreadId,
    runId: RunId | null,
    releaseWorkspace: Effect.Effect<void>,
    operation?: LaunchPreparation.LaunchOperation,
    observeExistingSetup = false,
  ) {
    const project = yield* projects.getById(input.projectId).pipe(
      Effect.mapError(mapError(input, "resolve-project", threadId)),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(mapError(input, "resolve-project", threadId)("Project no longer exists.")),
          onSome: Effect.succeed,
        }),
      ),
    );

    const dispatch = (command: OrchestrationV2ServerCommand) =>
      dispatchAccepted(command, project.workspaceRoot);
    const reused = input.reusedWorktree;
    const tracked = input.workspaceStrategy.type === "worktree" || reused !== undefined;
    let createdWorktreePath: string | null = null;
    let createdBranch: string | null = null;
    let createdTaskFolder: string | null = null;
    let setupTerminalId: string | null = null;
    let workspaceRecorded = false;
    let workspaceWriteIssued = false;
    let runReleased = false;
    let provenanceBaseRef: string | null = null;
    const idle = input.idleWorktreeAdmission;
    // An idle hold withdraws cleanup before its durable release and never regains it.
    let idleCleanupOwned = true;
    // Stop background renames before forgetting worktrees; completed renames record them again.
    let branchRename: Fiber.Fiber<unknown, ThreadLaunchError> | null = null;
    let stopBranchName = Effect.void;
    if (input.workspaceStrategy.type === "worktree") {
      yield* setupTracker.begin({
        threadId,
        branch: input.workspaceStrategy.branch ?? null,
        baseRef: input.workspaceStrategy.baseRef ?? null,
        stages: ["fetch", "checkout", "submodules", "setup-script", "agent"],
        fiber: yield* Effect.fiber,
      });
    } else if (reused !== undefined) {
      yield* setupTracker.begin({
        threadId,
        branch: input.workspaceStrategy.branch ?? null,
        baseRef: reused.baseRef ?? null,
        stages: ["setup-script", "agent"],
        fiber: yield* Effect.fiber,
      });
    }
    const projectCommandsNow = projectCommandsFor(input, threadId);
    yield* Effect.gen(function* () {
      const strategy = input.workspaceStrategy;
      const taskPlan =
        strategy.type === "worktree"
          ? yield* taskFolders
              .plan({ projectId: project.id, repositories: strategy.repositories })
              .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)))
          : null;
      const previousTaskFolderProgress =
        runId === null ? Option.none() : yield* readReceipt(input, taskFolderProgressId(runId));
      if (Option.isSome(previousTaskFolderProgress))
        return yield* mapError(
          input,
          "provision-worktree",
          threadId,
        )(
          "Task folder preparation was already issued and needs reconciliation before it can resume.",
        );
      if (
        strategy.type === "worktree" &&
        taskPlan === null &&
        (input.freshTaskFolderLaunch === true ||
          (input.freshTaskFolderLaunch === undefined && strategy.repositories !== undefined))
      ) {
        return yield* mapError(
          input,
          "provision-worktree",
          threadId,
        )("The accepted task folder plan no longer resolves to a task folder.");
      }
      if (
        taskPlan !== null &&
        (input.freshTaskFolderLaunch !== true ||
          operation !== undefined ||
          idle !== undefined ||
          input.nativeCreation === true)
      ) {
        return yield* mapError(
          input,
          "provision-worktree",
          threadId,
        )(
          "Task folder preparation is held because the operation has no verified multi-checkout custody.",
        );
      }
      const projectIsRepository = taskPlan === null || taskPlan.projectIsRepository;
      if (taskPlan !== null && strategy.type === "worktree" && strategy.baseFreshness !== undefined)
        return yield* mapError(
          input,
          "provision-worktree",
          threadId,
        )("Task folder preparation requires a base policy for every checkout.");
      const initialMessage = input.initialMessage;
      const generateBranchNameFor = Effect.fn("ThreadLaunchService.generateBranchNameFor")(
        function* (cwd: string, message: ThreadLaunchInitialMessage) {
          const settings = resolveProjectSettings(
            yield* serverSettings.getSettings,
            input.projectId,
          ).settings;
          const modelSelection =
            settings.sourceControlWriterModelSelection === null
              ? settings.textGenerationModelSelection
              : ServerSettings.resolveSourceControlWriterModelSelection(
                  settings,
                  yield* providerRegistry.getProviders,
                  input.projectId,
                );
          return yield* textGeneration
            .generateBranchName({
              naming: {
                mode: settings.branchNamingMode,
                prefix: settings.branchNamePrefix,
                instructions: settings.branchNameInstructions,
              },
              cwd,
              message: message.text,
              attachments: message.attachments,
              ...(message.context ? { context: message.context } : {}),
              modelSelection,
              projectId: input.projectId,
            })
            .pipe(
              Effect.timeoutOrElse({
                duration: BRANCH_NAME_TIMEOUT,
                orElse: () =>
                  Effect.fail(
                    new TextGenerationError({
                      operation: "generateBranchName",
                      detail: `Timed out after ${BRANCH_NAME_TIMEOUT}.`,
                    }),
                  ),
              }),
              Effect.map((result) => ({
                branch: result.branch,
                exactName: settings.branchNamingMode === "custom",
              })),
            );
        },
      );

      const requestedBranch = input.workspaceStrategy.branch;
      const generatedTemporaryBranch =
        input.workspaceStrategy.type === "worktree" && requestedBranch === undefined;
      let branch: string | null;
      if (generatedTemporaryBranch) {
        const uuid = yield* randomUuidV4;
        branch = buildTemporaryWorktreeBranchName(() => uuid.replaceAll("-", ""));
        const settings = yield* serverSettings.getSettings.pipe(
          Effect.map((settings) => resolveProjectSettings(settings, input.projectId).settings),
          Effect.orElseSucceed(() => null),
        );
        if (settings?.branchNamingMode === "static") {
          branch = formatGeneratedBranchName(branch.slice(-8), {
            mode: "static",
            prefix: settings.branchNamePrefix,
            instructions: "",
          });
        }
      } else {
        branch = yield* launchBranch(input.workspaceStrategy, project.workspaceRoot);
      }
      let worktreePath =
        input.workspaceStrategy.type === "existing_worktree"
          ? input.workspaceStrategy.worktreePath
          : null;
      // Generate the name during checkout. Renaming still requires the recorded workspace.
      const branchName =
        strategy.type === "worktree" &&
        reused === undefined &&
        initialMessage !== undefined &&
        branch !== null &&
        (generatedTemporaryBranch || isTemporaryWorktreeBranch(branch))
          ? yield* generateBranchNameFor(project.workspaceRoot, initialMessage).pipe(
              Effect.forkIn(preparationScope),
              Effect.tap((fiber) =>
                Effect.sync(() => {
                  stopBranchName = Fiber.interrupt(fiber);
                }),
              ),
              Effect.uninterruptible,
            )
          : null;
      if (input.workspaceStrategy.type === "worktree") {
        if (idle !== undefined)
          yield* idle.store
            .issueWorktree(idle.identity)
            .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
        if (runId !== null) {
          yield* dispatch({
            type: "prepared-run.progress",
            commandId:
              taskPlan === null
                ? CommandId.make(`${input.commandId}:progress:worktree`)
                : taskFolderProgressId(runId),
            threadId,
            runId,
            phase: "worktree",
          }).pipe(Effect.mapError(mapError(input, "update-thread", threadId)));
        }
        const mode = input.workspaceStrategy.baseFreshness;
        if (mode === "fetch-now") yield* setupTracker.stageStatus(threadId, "fetch", "running");
        const freshness =
          mode === undefined
            ? undefined
            : yield* resolveFreshWorktreeBase(
                input.projectId,
                mode,
                input.workspaceStrategy.baseRef,
                input.syncPrimaryOnFreshFetch === true,
              );
        if (freshness?.status === "failed") {
          yield* setupTracker.stageStatus(threadId, "fetch", "failed", freshness.detail);
          return yield* mapError(
            input,
            "provision-worktree",
            threadId,
          )(new Error(freshness.detail));
        }
        if (freshness !== undefined) {
          yield* setupTracker.stageStatus(
            threadId,
            "fetch",
            mode === "fetch-now" ? "done" : "skipped",
            freshness.detail,
          );
        }
        const requestedBaseRef =
          freshness?.status === "selected"
            ? freshness.baseRefName
            : (input.workspaceStrategy.baseRef ??
              (projectIsRepository
                ? yield* defaultWorktreeBase(input, project.workspaceRoot, threadId)
                : "HEAD"));
        let startRef = freshness?.status === "selected" ? freshness.startRef : requestedBaseRef;
        let baseRefName = requestedBaseRef;
        const remote =
          projectIsRepository &&
          mode === undefined &&
          input.workspaceStrategy.startFromOrigin === true
            ? yield* git
                .resolveWorktreeBaseRemote({
                  cwd: project.workspaceRoot,
                  refName: requestedBaseRef,
                })
                .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)))
            : null;
        const shouldFetch = remote !== null;
        if (mode === undefined) {
          yield* setupTracker.stageStatus(threadId, "fetch", shouldFetch ? "running" : "skipped");
        }
        let fetchedBase = false;
        if (remote !== null) {
          const remoteBase = yield* git
            .fetchWorktreeBaseCommit({
              cwd: project.workspaceRoot,
              refName: requestedBaseRef,
              remote,
            })
            .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
          if (remoteBase !== null) {
            startRef = remoteBase.commitSha;
            baseRefName = `refs/remotes/${remoteBase.remoteRefName}`;
            fetchedBase = true;
          }
        }
        if (shouldFetch) yield* setupTracker.stageStatus(threadId, "fetch", "done");
        // `:/text` bases are commit searches, not refs; `hasCommit` treats the appended
        // `^{commit}` as part of the pattern (same guard as checkWorktreeBase).
        const recordedBaseResolves =
          !projectIsRepository ||
          fetchedBase ||
          startRef.startsWith(":") ||
          (yield* git
            .hasCommit({
              cwd: project.workspaceRoot,
              // `git worktree add` reads `-` as the previous checkout.
              refName: startRef === "-" ? "@{-1}" : startRef,
            })
            .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId))));
        if (!recordedBaseResolves && mode === "fetch-now") {
          const detail =
            "The verified remote commit is no longer available in the project repository.";
          yield* setupTracker.stageStatus(threadId, "fetch", "failed", detail);
          return yield* mapError(input, "provision-worktree", threadId)(new Error(detail));
        }
        if (!recordedBaseResolves) {
          const remoteName =
            mode !== undefined
              ? null
              : (remote?.remoteName ??
                (yield* git
                  .resolvePrimaryRemoteName(project.workspaceRoot)
                  .pipe(Effect.orElseSucceed(() => null))));
          const fallback = yield* worktreeFallbackBase(project.workspaceRoot, remoteName).pipe(
            Effect.orElseSucceed(() => null),
          );
          if (fallback !== null) {
            startRef = fallback.startRef;
            baseRefName = fallback.baseRefName;
            if (freshness?.status === "selected") {
              yield* setupTracker.stageStatus(
                threadId,
                "fetch",
                "skipped",
                "The last verified commit is unavailable. Using the local base.",
              );
            }
          }
        }
        yield* setupTracker.update(threadId, (snapshot) => ({ ...snapshot, baseRef: baseRefName }));
        if (
          taskPlan === null &&
          branch !== null &&
          (generatedTemporaryBranch || isTemporaryWorktreeBranch(branch))
        ) {
          const parts = branch.split("/");
          for (let depth = 1; depth < parts.length; depth++) {
            if (
              yield* git
                .hasCommit({
                  cwd: project.workspaceRoot,
                  refName: `refs/heads/${parts.slice(0, depth).join("/")}`,
                })
                .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)))
            ) {
              branch = generatedTemporaryBranch
                ? branch.replaceAll("/", "-")
                : flattenTemporaryWorktreeBranchName(branch);
              break;
            }
          }
        }
        yield* setupTracker.stageStatus(threadId, "checkout", "running");
        let submoduleProgressReportedAt = Number.NEGATIVE_INFINITY;
        const createWorktree =
          operation !== undefined && Option.isSome(launchPreparation)
            ? (
                createInput: Parameters<typeof git.createWorktree>[0],
                options: Parameters<typeof git.createWorktree>[1],
              ) => launchPreparation.value.createWorktree(operation, createInput, options)
            : git.createWorktree;
        const progress: GitVcsDriver.CreateWorktreeProgress = {
          onWorktreeClaimed: (path) =>
            Effect.sync(() => {
              createdWorktreePath = path;
              createdBranch = branch;
            }),
          onCheckoutProgress: (progress) =>
            setupTracker.stage(threadId, "checkout", { percent: progress.percent }),
          onSubmodulesStarted: () =>
            setupTracker
              .stageStatus(threadId, "checkout", "done")
              .pipe(Effect.andThen(setupTracker.stageStatus(threadId, "submodules", "running"))),
          onSubmodulesDisabled: ({ source }) =>
            setupTracker.stageStatus(threadId, "submodules", "skipped", `disabled in ${source}`),
          onSubmoduleLine: (line) => {
            // Paths may contain quotes; git closes the path with "': ".
            const submodulePath = /^\s*Submodule path '(.+)': /.exec(line)?.[1];
            if (submodulePath !== undefined) {
              return setupTracker.stage(threadId, "submodules", { detail: submodulePath });
            }
            if (parseGitCloneProgressLine(line) === null) return Effect.void;
            // Git redraws on every percent; hold the websocket to one update a second.
            return Effect.flatMap(Clock.currentTimeMillis, (now) => {
              if (now - submoduleProgressReportedAt < 1000) return Effect.void;
              submoduleProgressReportedAt = now;
              return setupTracker.stage(threadId, "submodules", {
                detail: line.trim().replace(/\s+/g, " "),
              });
            });
          },
          onSubmodulesFinished: ({ ok, detail }) =>
            setupTracker.stageStatus(
              threadId,
              "submodules",
              ok ? "done" : "warning",
              ok ? undefined : (detail ?? "submodule checkout failed"),
            ),
        };
        if (taskPlan === null) {
          const created = yield* createWorktree(
            {
              cwd: project.workspaceRoot,
              refName: startRef,
              newRefName: branch!,
              baseRefName,
              path: null,
            },
            { progress },
          ).pipe(
            Effect.catchIf(
              (error) => error.reason === "ref_not_found",
              (cause) =>
                Effect.fail(
                  new ThreadLaunchBaseRefError({
                    projectId: project.id,
                    projectTitle: project.title,
                    workspaceRoot: project.workspaceRoot,
                    baseRef: requestedBaseRef,
                    cause,
                  }),
                ),
            ),
            Effect.mapError(mapError(input, "provision-worktree", threadId)),
          );
          provenanceBaseRef = baseRefName;
          worktreePath = created.worktree.path;
          branch = created.worktree.refName;
        } else {
          const created = yield* taskFolders
            .create({
              plan: taskPlan,
              branch: branch!,
              projectStart: { refName: startRef, baseRefName },
              baseBranch: input.workspaceStrategy.baseRef,
              startFromOrigin: input.workspaceStrategy.startFromOrigin === true,
              projectCommands: projectCommandsNow,
              projectProgress: {
                ...progress,
                // TaskFolders owns rollback until it returns the complete folder.
                onWorktreeClaimed: (path) =>
                  setupTracker.update(threadId, (snapshot) => ({
                    ...snapshot,
                    worktreePath: path,
                  })),
              },
              onRepository: ({ name, done, total }) =>
                setupTracker.stage(threadId, "checkout", {
                  detail: `${name} (${done} of ${total})`,
                }),
            })
            .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
          createdTaskFolder = created.taskFolder.root;
          worktreePath = created.taskFolder.projectPath;
          branch = created.branch;
          if (created.taskFolder.projectIsWorktree && !created.laidOutByProject)
            provenanceBaseRef = baseRefName;
        }
        createdWorktreePath = worktreePath;
        createdBranch = branch;
        yield* setupTracker.update(threadId, (snapshot) => ({
          ...snapshot,
          worktreePath,
          branch,
          stages: snapshot.stages.map((stage) =>
            stage.id === "submodules" && stage.status === "pending"
              ? { ...stage, status: "skipped", detail: "none" }
              : stage,
          ),
        }));
        yield* setupTracker.stageStatus(threadId, "checkout", "done");
      }

      if (worktreePath !== null) {
        const folder = yield* taskFolders
          .find(worktreePath)
          .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
        if (createdTaskFolder !== null && folder?.root !== createdTaskFolder)
          return yield* mapError(
            input,
            "provision-worktree",
            threadId,
          )("The task folder's completed checkout record is unavailable.");
        if (folder !== null) {
          if (
            folder.projectId !== input.projectId ||
            branch === null ||
            (createdTaskFolder === null && (reused !== undefined || operation !== undefined))
          ) {
            return yield* mapError(
              input,
              "provision-worktree",
              threadId,
            )("The original task folder binding and preparation custody could not be verified.");
          }
          yield* taskFolders
            .assertReady({ taskFolder: folder, branch })
            .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
        }
      }

      // Do not rewrite reused worktrees; that could undo the original branch rename.
      if (reused === undefined) {
        const workspaceCommand = {
          type: "thread.metadata.update" as const,
          commandId: CommandId.make(`${input.commandId}:workspace`),
          threadId,
          branch,
          worktreePath,
        };
        workspaceWriteIssued = true;
        yield* (
          operation !== undefined && Option.isSome(launchPreparation)
            ? launchPreparation.value.workspace(operation, workspaceCommand, threads.dispatch)
            : dispatch(workspaceCommand)
        ).pipe(Effect.mapError(mapError(input, "update-thread", threadId)));
      }
      workspaceRecorded = true;
      // A conversation launch has no physical workspace yet; its binding records its own provenance.
      if (input.workspaceStrategy.type !== "auto")
        yield* captureProvenance({
          threadId,
          reason: input.workspaceStrategy.type === "worktree" ? "launch" : "reference",
          createdBy: "system",
          baseRef: provenanceBaseRef,
          baseIsCurrentHead: provenanceBaseRef !== null && reused === undefined,
          ...(runId === null ? {} : { runId }),
        });
      if (idle !== undefined) {
        if (worktreePath === null)
          return yield* mapError(
            input,
            "provision-worktree",
            threadId,
          )("Idle worktree path is unavailable.");
        yield* idle.store
          .recordWorkspace(idle.identity, worktreePath)
          .pipe(Effect.mapError(mapError(input, "update-thread", threadId)));
      }

      if (
        createdWorktreePath !== null &&
        worktreePath === createdWorktreePath &&
        branch !== null &&
        initialMessage !== undefined &&
        (generatedTemporaryBranch || isTemporaryWorktreeBranch(branch))
      ) {
        const oldBranch = branch;
        const worktreeCwd = worktreePath;
        const generatedName =
          branchName === null
            ? generateBranchNameFor(worktreeCwd, initialMessage)
            : Fiber.join(branchName);
        branchRename = yield* generatedName.pipe(
          Effect.flatMap(({ branch: newBranch, exactName }) => {
            const renameInput = {
              cwd: worktreeCwd,
              oldBranch,
              newBranch,
              ...(exactName ? { exactName: true } : {}),
            };
            const commandFor = (renamedBranch: string) => ({
              type: "thread.metadata.update" as const,
              commandId: CommandId.make(`${input.commandId}:branch-rename`),
              threadId,
              branch: renamedBranch,
              worktreePath: worktreeCwd,
              expectedBranch: oldBranch,
              expectedWorktreePath: worktreeCwd,
            });
            if (operation !== undefined && Option.isSome(launchPreparation))
              return launchPreparation.value
                .rename(operation, renameInput, commandFor, threads.dispatch)
                .pipe(
                  Effect.andThen(threads.getThreadShell(threadId)),
                  Effect.flatMap((shell) =>
                    shell?.branch == null
                      ? Effect.fail(new CheckoutError({ reason: "unavailable" }))
                      : Effect.succeed({ branch: shell.branch }),
                  ),
                );
            const rename: Effect.Effect<
              { readonly branch: string },
              GitManagerServiceError | TaskFolderError
            > =
              createdTaskFolder === null
                ? git.renameBranch(renameInput)
                : taskFolders.renameBranch({
                    root: createdTaskFolder,
                    oldBranch,
                    newBranch,
                    ...(exactName ? { exactName: true } : {}),
                  });
            return rename.pipe(
              Effect.tap((renamed) =>
                Effect.sync(() => {
                  createdBranch = renamed.branch;
                }),
              ),
              Effect.tap((renamed) => {
                const updated = dispatch(commandFor(renamed.branch));
                return createdTaskFolder !== null
                  ? updated
                  : updated.pipe(
                      Effect.catchCause((cause) =>
                        Effect.logWarning("Parent thread worktree branch update failed", {
                          threadId,
                          cause,
                        }),
                      ),
                    );
              }),
              Effect.uninterruptible,
            );
          }),
          Effect.flatMap((renamed) =>
            Effect.gen(function* () {
              // Update parents first so children inherit names; reread each pass for newly updated descendants.
              const visited = new Set([threadId]);
              while (true) {
                // Jones hook: the indexed metadata owner answers without a full shell snapshot.
                const matchingIds: ReadonlyArray<ThreadId> = Option.isSome(metadataReads)
                  ? yield* metadataReads.value
                      .listIds({ worktree: { path: worktreeCwd, branch: oldBranch } })
                      .pipe(NodeSqliteClient.readOnly)
                  : yield* threads
                      .getShellSnapshot({ worktreePath: worktreeCwd })
                      .pipe(
                        Effect.map((snapshot) =>
                          [...snapshot.threads, ...snapshot.archivedThreads]
                            .filter((thread) => thread.branch === oldBranch)
                            .map((thread) => thread.id),
                        ),
                      );
                const sharing = matchingIds.filter((id) => !visited.has(id));
                if (sharing.length === 0) break;
                for (const id of sharing) {
                  visited.add(id);
                  yield* dispatch({
                    type: "thread.metadata.update",
                    commandId: CommandId.make(`${input.commandId}:branch-rename:${id}`),
                    threadId: id,
                    branch: renamed.branch,
                    expectedBranch: oldBranch,
                    expectedWorktreePath: worktreeCwd,
                  }).pipe(
                    Effect.catchCause((cause) =>
                      Effect.logWarning("Shared thread worktree branch update failed", {
                        threadId: id,
                        cause,
                      }),
                    ),
                  );
                }
              }
            }),
          ),
          Effect.catchCause((cause) =>
            Effect.logWarning("Thread worktree branch rename failed", {
              commandId: input.commandId,
              threadId,
              oldBranch,
              cause,
            }).pipe(
              Effect.andThen(
                runId === null || Cause.hasInterruptsOnly(cause)
                  ? Effect.void
                  : recordBranchRenameFailure(threadId, runId, oldBranch, cause),
              ),
              Effect.andThen(
                operation === undefined && createdTaskFolder === null
                  ? Effect.void
                  : Effect.failCause(cause),
              ),
            ),
          ),
          Effect.mapError(mapError(input, "provision-worktree", threadId)),
          Effect.forkIn(preparationScope),
        );
      } else {
        yield* stopBranchName;
      }

      if (runId !== null && worktreePath !== null) {
        yield* dispatch({
          type: "prepared-run.progress",
          commandId: CommandId.make(`${input.commandId}:progress:setup`),
          threadId,
          runId,
          phase: "setup",
        }).pipe(Effect.mapError(mapError(input, "update-thread", threadId)));
      }
      yield* setupTracker.stageStatus(threadId, "setup-script", "running");
      const runSetup =
        operation !== undefined && Option.isSome(launchPreparation)
          ? (setupInput: ProjectSetupScriptRunner.ProjectSetupScriptRunnerInput) =>
              launchPreparation.value.runSetup(operation, setupInput)
          : setupScripts.runForThread;
      // Jones hook: a root launch runs the project's setup in the project root, an
      // explicit opt-out skips setup, and an `auto` placement never runs it here.
      const setupCwd = worktreePath ?? project.workspaceRoot;
      const runsSetup =
        input.runSetupScript !== false &&
        strategy.type !== "auto" &&
        (yield* projectCommandsNow) === "permitted";
      if (idle !== undefined && runsSetup)
        yield* idle.store
          .issueSetup(idle.identity)
          .pipe(Effect.mapError(mapError(input, "run-setup-script", threadId)));
      const setup = !runsSetup
        ? ({ status: "no-script" } as const)
        : yield* runSetup({
            threadId,
            projectId: input.projectId,
            projectCwd: project.workspaceRoot,
            worktreePath: setupCwd,
            ...(tracked
              ? {
                  observeCompletion: {
                    onOutputLine: (line: string) =>
                      setupTracker.appendTail(threadId, "setup-script", line),
                  },
                }
              : observeExistingSetup
                ? { observeCompletion: {} }
                : {}),
            project: {
              id: project.id,
              workspaceRoot: project.workspaceRoot,
              scripts: project.scripts,
            },
          }).pipe(Effect.mapError(mapError(input, "run-setup-script", threadId)));

      let awaitAsyncSetup = Effect.void;
      if (setup.status === "started") {
        setupTerminalId = setup.terminalId;
        if (createdTaskFolder !== null) {
          yield* setup.writeCompletion.pipe(
            Effect.mapError(mapError(input, "run-setup-script", threadId)),
          );
          if (!setup.async && setup.completion === undefined)
            return yield* mapError(
              input,
              "run-setup-script",
              threadId,
            )("The task folder's synchronous setup did not provide a completion receipt.");
        }
        yield* setupTracker.update(threadId, (snapshot) => ({
          ...snapshot,
          setupScript: {
            name: setup.scriptName,
            command: setup.scriptCommand,
            terminalId: setup.terminalId,
          },
        }));
        if (setup.completion) {
          const awaitCompletion = Effect.gen(function* () {
            const completion = yield* setup.completion!;
            yield* setupTracker.stageStatus(
              threadId,
              "setup-script",
              completion.exitCode === 0 ? "done" : "failed",
              `exited with ${completion.exitCode ?? "no exit code"}`,
            );
            if (completion.exitCode !== 0 && !setup.async)
              return yield* mapError(
                input,
                "run-setup-script",
                threadId,
              )(`Setup script exited with ${completion.exitCode ?? "no exit code"}.`);
          });
          if (setup.async) {
            // Record the result when the script ends, not after the agent starts.
            const asyncSetup = yield* awaitCompletion.pipe(
              Effect.catchCause((cause) =>
                setupTracker.stageStatus(
                  threadId,
                  "setup-script",
                  "failed",
                  failureDetail(Cause.squash(cause)),
                ),
              ),
              Effect.forkChild,
            );
            awaitAsyncSetup = Fiber.join(asyncSetup);
          } else {
            yield* awaitCompletion;
          }
        } else {
          yield* setupTracker.stageStatus(threadId, "setup-script", "done");
        }
      } else {
        yield* setupTracker.stageStatus(threadId, "setup-script", "skipped");
      }
      // The agent must start on the branch name the thread will show.
      if (branchRename !== null) yield* Fiber.join(branchRename);
      if (createdTaskFolder !== null && createdBranch !== null) {
        const taskFolder = yield* taskFolders
          .find(createdTaskFolder)
          .pipe(Effect.mapError(mapError(input, "release-run", threadId)));
        const shell = yield* threads
          .getThreadShell(threadId)
          .pipe(Effect.mapError(mapError(input, "release-run", threadId)));
        if (
          taskFolder?.root !== createdTaskFolder ||
          shell?.worktreePath !== worktreePath ||
          shell?.branch !== createdBranch
        )
          return yield* mapError(
            input,
            "release-run",
            threadId,
          )("The task folder changed before the prepared run could be released.");
        yield* taskFolders
          .assertReady({ taskFolder, branch: createdBranch })
          .pipe(Effect.mapError(mapError(input, "release-run", threadId)));
      }
      yield* setupTracker.markUncancellable(threadId);
      yield* setupTracker.stageStatus(threadId, "agent", "running");
      if (idle !== undefined) {
        const releasedPath = worktreePath;
        if (releasedPath === null)
          return yield* mapError(
            input,
            "release-run",
            threadId,
          )("Idle worktree path is unavailable.");
        // Withdraw cleanup before either durable release write. A lost reply can already expose the checkout.
        // This generation never regains cleanup ownership, including on interruption or failed readback.
        yield* Effect.gen(function* () {
          idleCleanupOwned = false;
          yield* idle.store.prepareRelease(idle.identity, releasedPath);
          yield* idle.store.release(idle.identity);
        }).pipe(Effect.mapError(mapError(input, "release-run", threadId)), Effect.uninterruptible);
      }
      if (runId !== null) {
        const releaseCommand = {
          type: "prepared-run.release" as const,
          commandId: CommandId.make(`${input.commandId}:release`),
          threadId,
          runId,
        };
        if (operation !== undefined && Option.isSome(launchPreparation))
          yield* launchPreparation.value
            .ready(operation)
            .pipe(Effect.mapError(mapError(input, "release-run", threadId)));
        yield* (
          operation !== undefined && Option.isSome(launchPreparation)
            ? launchPreparation.value.release(operation, releaseCommand, threads.dispatch)
            : dispatch(releaseCommand)
        ).pipe(
          Effect.mapError(mapError(input, "release-run", threadId)),
          Effect.tap(() =>
            Effect.sync(() => {
              runReleased = true;
            }),
          ),
          Effect.uninterruptible,
        );
      }
      yield* releaseWorkspace;
      yield* setupTracker.stageStatus(threadId, "agent", "done");
      yield* awaitAsyncSetup;
      yield* setupTracker.finish(threadId, "done");
    }).pipe(
      Effect.onError((cause) =>
        Effect.gen(function* () {
          const cancelled = Cause.hasInterruptsOnly(cause);
          if (idle !== undefined)
            yield* idle.store.markUnresolved(idle.identity).pipe(Effect.ignore);
          if (branchRename !== null) yield* Fiber.interrupt(branchRename);
          yield* stopBranchName;
          yield* setupTracker.finish(
            threadId,
            cancelled ? "cancelled" : "failed",
            cancelled ? null : failureDetail(Cause.squash(cause)),
          );
          if (cancelled && setupTerminalId)
            yield* terminals
              .close({ threadId, terminalId: setupTerminalId, deleteHistory: true })
              .pipe(Effect.ignore);
          if (
            operation === undefined &&
            tracked &&
            createdWorktreePath &&
            idleCleanupOwned &&
            // An idle hold deletes only with its own durable proof; unresolved keeps the checkout.
            (idle === undefined ||
              (yield* idle.store
                .cleanupAllowed(idle.identity)
                .pipe(Effect.catchCause(() => Effect.succeed(false))))) &&
            ((cancelled && !runReleased) || !workspaceRecorded)
          ) {
            const removedPath = createdWorktreePath;
            const removedBranch = createdBranch;
            yield* Effect.gen(function* () {
              if (createdTaskFolder !== null) {
                if (workspaceWriteIssued) return;
                const { kept } = yield* taskFolders.remove({
                  root: createdTaskFolder,
                  force: false,
                  forget: true,
                  projectCommands: "request-not-permitted",
                });
                if (kept.length > 0)
                  return yield* new TaskFolderError({
                    operation: "remove",
                    detail:
                      "An abandoned task folder still has checkouts or files that need reconciliation.",
                  });
              } else {
                yield* taskFolders.removeForProjectFolder(
                  { cwd: project.workspaceRoot, path: removedPath, force: false },
                  "request-not-permitted",
                );
                if (removedBranch !== null)
                  yield* git
                    .deleteLocalBranch({
                      cwd: project.workspaceRoot,
                      refName: removedBranch,
                      force: false,
                    })
                    .pipe(Effect.ignoreCause({ log: true }));
              }
              yield* dispatch({
                type: "thread.metadata.update",
                commandId: CommandId.make(`${input.commandId}:cancel-workspace`),
                threadId,
                expectedWorktreePath: workspaceRecorded ? removedPath : null,
                worktreePath: null,
                branch: null,
              }).pipe(Effect.ignore);
            }).pipe(
              Effect.catchCause((removeCause) =>
                Effect.logWarning("Abandoned thread worktree retained for reconciliation", {
                  commandId: input.commandId,
                  threadId,
                  path: removedPath,
                  cause: removeCause,
                }),
              ),
            );
          }
        }),
      ),
    );
  });

  const failPreparedRun = (
    input: Pick<PreparationInput, "commandId">,
    threadId: ThreadId,
    runId: RunId | null,
    cause: unknown,
    retryPersistence = true,
  ) => {
    if (runId === null) {
      return Effect.logWarning("Thread workspace preparation failed", {
        commandId: input.commandId,
        threadId,
        cause,
      });
    }
    const persistFailure = threads.dispatch({
      type: "prepared-run.fail",
      commandId: CommandId.make(`${input.commandId}:fail`),
      threadId,
      runId,
      failure: makeProviderFailure({
        cause,
        message: failureDetail(cause),
        class: "validation_error",
        retryable: false,
      }),
    });
    const runIsPreparing = threads.getThreadRecords(threadId, ["runs"], { runIds: [runId] }).pipe(
      Effect.map((current) =>
        current.runs.some((run) => run.id === runId && run.status === "preparing"),
      ),
      // The read can fail in the same outage, which says nothing about the run.
      Effect.catchCause(() => Effect.succeed(true)),
    );
    return persistFailure.pipe(
      Effect.catchCause((persistCause) =>
        Effect.gen(function* () {
          yield* Effect.logWarning("Failed to persist thread workspace preparation failure", {
            commandId: input.commandId,
            threadId,
            cause,
            persistCause,
          });
          if (
            !retryPersistence ||
            Cause.hasInterruptsOnly(persistCause) ||
            !(yield* runIsPreparing)
          )
            return;
          // A full disk rejects this write as well, and nothing else moves the run out of
          // "preparing", so it would hold the thread's queue until a restart. Retry until
          // storage recovers or the run settles another way.
          yield* persistFailure.pipe(
            // Storage failures can be defects; retry both failure channels.
            Effect.catchCauseIf(
              (retryCause) => !Cause.hasInterruptsOnly(retryCause),
              (retryCause) =>
                runIsPreparing.pipe(
                  Effect.flatMap((preparing) =>
                    preparing ? Effect.fail(retryCause) : Effect.void,
                  ),
                ),
            ),
            Effect.retry(
              Schedule.exponential("1 second").pipe(
                Schedule.modifyDelay(({ duration }) =>
                  Effect.succeed(Duration.min(duration, Duration.seconds(30))),
                ),
              ),
            ),
            Effect.ignoreCause,
            // Callers reach this from finalizers; shutdown must still be able to stop it.
            Effect.interruptible,
          );
        }),
      ),
    );
  };

  const reservePreparation = (commandId: CommandId) =>
    Ref.modify(scheduledLaunches, (scheduled) => {
      if (scheduled.has(commandId)) return [false, scheduled] as const;
      const next = new Set(scheduled);
      next.add(commandId);
      return [true, next] as const;
    });

  const releasePreparation = (commandId: CommandId) =>
    Ref.update(scheduledLaunches, (scheduled) => {
      const next = new Set(scheduled);
      next.delete(commandId);
      return next;
    });

  const schedulePreparation = Effect.fn("ThreadLaunchService.schedulePreparation")(function* (
    input: PreparationInput,
    threadId: ThreadId,
    runId: RunId | null,
  ) {
    const preparation = Effect.gen(function* () {
      // Existing checkouts remain externally owned. An app-owned delegated task
      // still owns its setup terminal and waits for synchronous setup before release.
      const observeExistingSetup =
        runId !== null &&
        input.workspaceStrategy.type === "existing_worktree" &&
        input.reusedWorktree === undefined &&
        (yield* Effect.gen(function* () {
          const { thread } = yield* threads.getThreadRecords(threadId, []);
          const parentId = thread.lineage.parentThreadId;
          if (thread.lineage.relationshipToParent !== "subagent" || parentId === null) return false;
          const { subagents } = yield* threads.getThreadRecords(
            parentId,
            ["subagents"],
            thread.forkedFrom?.type === "node"
              ? { subagentIds: [thread.forkedFrom.nodeId] }
              : undefined,
          );
          return subagents.some(
            (task) => task.origin === "app_owned" && task.childThreadId === threadId,
          );
        }));
      const prepare = Effect.fnUntraced(function* (releaseWorkspace: Effect.Effect<void>) {
        yield* requirePreparationGraph;
        if (runId !== null && input.nativeCreation !== true && Option.isSome(launchPreparation)) {
          if (
            Option.isSome(yield* readReceipt(input, taskFolderProgressId(runId))) ||
            (input.workspaceStrategy.type === "worktree" &&
              ((input.freshTaskFolderLaunch !== false &&
                input.workspaceStrategy.repositories !== undefined) ||
                (yield* taskFolders.plan({
                  projectId: input.projectId,
                  repositories: input.workspaceStrategy.repositories,
                })) !== null))
          )
            return yield* mapError(
              input,
              "provision-worktree",
              threadId,
            )("The ordinary preparation receipt cannot authorize a task folder's checkouts.");
          return yield* launchPreparation.value.withRun(
            threadId,
            runId,
            threads.dispatch,
            (operation, strategy) =>
              prepareInBackground(
                { ...input, workspaceStrategy: strategy },
                threadId,
                runId,
                releaseWorkspace,
                operation,
                observeExistingSetup,
              ),
          );
        }
        return yield* prepareInBackground(
          input,
          threadId,
          runId,
          releaseWorkspace,
          undefined,
          observeExistingSetup,
        );
      });
      const preparingWorkspace = Effect.acquireUseRelease(
        input.workspaceStrategy.type === "worktree"
          ? setupTracker.holdWorkspace(threadId)
          : Effect.succeed(Effect.void),
        prepare,
        (releaseWorkspace) => releaseWorkspace,
      );
      if (
        runId === null ||
        (Option.isNone(launchPreparation) &&
          input.workspaceStrategy.type !== "worktree" &&
          input.reusedWorktree === undefined &&
          !observeExistingSetup)
      ) {
        return yield* preparingWorkspace;
      }
      const afterSequence = yield* threads.getThreadEventSequence(threadId);
      const projection = yield* threads.getThreadRecords(threadId, ["runs"], { runIds: [runId] });
      if (projection.runs.find((run) => run.id === runId)?.status !== "preparing") return;
      const interruption = threads
        .streamStoredEventsFrom({ threadId, afterSequence, eventType: "run.updated" })
        .pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "run.updated" &&
              stored.event.payload.id === runId &&
              (stored.event.payload.status === "interrupted" ||
                stored.event.payload.status === "cancelled"),
          ),
          Stream.runHead,
          Effect.flatMap((event) => (Option.isSome(event) ? Effect.interrupt : Effect.never)),
        );
      yield* Effect.raceFirst(preparingWorkspace, interruption);
    });
    yield* preparation.pipe(
      Effect.onError((cause) =>
        failPreparedRun(
          input,
          threadId,
          runId,
          Cause.hasInterruptsOnly(cause) ? "Worktree setup cancelled." : Cause.squash(cause),
        ),
      ),
      Effect.ignoreCause,
      Effect.ensuring(releasePreparation(input.commandId)),
      Effect.forkIn(preparationScope),
    );
  });

  const resumeIdlePreparation = Effect.fn("ThreadLaunchService.resumeIdlePreparation")(function* (
    input: PreparationInput,
    idle: IdlePreparation,
  ) {
    const { identity, store } = idle;
    const threadId = identity.threadId;
    const readCurrent = store.read(threadId).pipe(
      Effect.mapError(mapError(input, "read-receipt", threadId)),
      Effect.filterOrFail(
        (current): current is NonNullable<typeof current> =>
          current !== null && IdleWorktreeAdmission.sameIdentity(current, identity),
        () => mapError(input, "read-receipt", threadId)("Idle preparation identity changed."),
      ),
    );
    const recorded = yield* readCurrent;
    if (recorded.phase === "ready") return;
    if (recorded.phase === "prepared")
      return yield* store
        .release(identity)
        .pipe(Effect.mapError(mapError(input, "release-run", threadId)), Effect.uninterruptible);
    if (!(yield* reservePreparation(input.commandId))) return;
    yield* Effect.gen(function* () {
      const current = yield* readCurrent;
      if (current.phase === "pending") {
        // A creation retry may now carry a message. Its original idle producer still has no run or message.
        yield* schedulePreparation(
          {
            commandId: input.commandId,
            projectId: input.projectId,
            workspaceStrategy: input.workspaceStrategy,
            idleWorktreeAdmission: idle,
          },
          threadId,
          null,
        );
        return;
      }
      yield* releasePreparation(input.commandId);
      if (current.phase === "ready") return;
      if (current.phase === "prepared")
        return yield* store
          .release(identity)
          .pipe(Effect.mapError(mapError(input, "release-run", threadId)), Effect.uninterruptible);
      return yield* mapError(
        input,
        "provision-worktree",
        threadId,
      )(new IdleWorktreeAdmission.IdleWorktreeAdmissionError({ threadId, reason: "unresolved" }));
    }).pipe(Effect.onError(() => releasePreparation(input.commandId)));
  });

  // Jones hook: `auto` resolves before Scratch normalization. Scratch has no Git
  // repository for a conversation worktree, so it allocates eagerly like `worktree`.
  const resolveAutoStrategy = Effect.fn("ThreadLaunchService.resolveAutoStrategy")(function* (
    request: ThreadLaunchInput,
  ) {
    if (request.workspaceStrategy.type !== "auto") {
      const unchanged: AutoLaunchStrategy.AutoLaunchResolution = {
        strategy: request.workspaceStrategy,
        fallbackDriver: undefined,
      };
      return unchanged;
    }
    const settings = resolveProjectSettings(
      yield* serverSettings.getSettings.pipe(Effect.mapError(mapError(request, "resolve-project"))),
      request.projectId,
    ).settings;
    return yield* autoStrategy.resolve({
      strategy: request.workspaceStrategy,
      eagerAllocation:
        settings.worktreeAllocation === "eager" ||
        (yield* managedFolders.isScratchProject(request.projectId)),
      instanceId: request.modelSelection.instanceId,
    });
  });

  const launch: ThreadLaunchService["Service"]["launch"] = Effect.fn("ThreadLaunchService.launch")(
    function* (request) {
      if (
        request.legacyBootstrap !== undefined ||
        request.preparationReleaseCommandId !== undefined
      ) {
        if (Option.isNone(legacyBootstrap))
          return yield* new ThreadLaunchError({
            operation: "create-thread",
            commandId: request.commandId,
            projectId: request.projectId,
            ...(request.threadId === undefined ? {} : { threadId: request.threadId }),
            cause: "Legacy launch requires its owning database and effect outbox.",
          });
        return yield* (yield* legacyBootstrap.value).launch(request);
      }
      yield* requirePreparationGraph.pipe(
        Effect.mapError(mapError(request, "create-thread", request.threadId)),
      );
      const priorLaunchReceipt = yield* readReceipt(request, request.commandId);
      const claimEvents = Option.isSome(priorLaunchReceipt)
        ? yield* eventSink
            .readByCommandId({ commandId: request.commandId })
            .pipe(
              Stream.runCollect,
              Effect.mapError(mapError(request, "read-receipt", request.threadId)),
            )
        : [];
      const recordedClaim = Option.isSome(priorLaunchReceipt)
        ? recordedLaunchClaim(request, priorLaunchReceipt.value, claimEvents)
        : undefined;
      if (Option.isSome(priorLaunchReceipt) && recordedClaim === undefined)
        return yield* mapError(
          request,
          request.reuseExistingThread ? "update-thread" : "create-thread",
          request.threadId,
        )("The launch command cannot be replayed without its matching accepted immutable claim.");
      const replayMessage = yield* Effect.gen(function* () {
        if (!recordedClaim || Option.isNone(priorLaunchReceipt) || !request.initialMessage) return;
        const result = yield* readLaunchMessage(
          receipts,
          eventSink,
          request,
          priorLaunchReceipt.value,
          claimEvents,
        ).pipe(Effect.mapError(mapError(request, "read-receipt", recordedClaim.id)));
        if (result.status === "absent") return;
        if (result.status === "accepted") return result;
        return yield* mapError(
          request,
          "dispatch-message",
          recordedClaim.id,
        )(
          result.status === "rejected"
            ? new Orchestrator.OrchestratorCommandPreviouslyRejectedError({
                commandId: result.receipt.commandId,
                commandType: result.receipt.commandType,
                detail: result.receipt.error ?? "Previously rejected.",
              })
            : "The first message cannot be replayed without its matching accepted immutable receipt.",
        );
      });
      const needsAdmission =
        recordedClaim === undefined ||
        (request.initialMessage !== undefined && replayMessage === undefined);
      const priorIdle =
        recordedClaim !== undefined && request.workspaceStrategy.type === "auto"
          ? yield* readIdleWorktree(request, recordedClaim.id)
          : null;
      const replayAuto =
        recordedClaim !== undefined &&
        request.workspaceStrategy.type === "auto" &&
        priorIdle === null &&
        autoStrategy.available &&
        replayMessage === undefined;
      let resumeAllocation = false;
      let alreadyBound = false;
      if (replayAuto) {
        const shell = yield* threads
          .getThreadShell(recordedClaim.id)
          .pipe(Effect.mapError(mapError(request, "read-receipt", recordedClaim.id)));
        if (shell === null || shell.deletedAt !== null || shell.projectId !== request.projectId)
          return yield* mapError(request, "create-thread", recordedClaim.id)("Thread not found.");
        if (shell.worktreePath === null && shell.archivedAt === null) {
          alreadyBound =
            (yield* autoStrategy
              .lookup(recordedClaim.id)
              .pipe(
                Effect.mapError((cause) =>
                  mapConversationFailure(request, cause, "allocation", recordedClaim.id),
                ),
              )) !== null;
          if (!alreadyBound) {
            const usage = yield* threads
              .getThreadRecords(recordedClaim.id, ["runs", "messages"])
              .pipe(Effect.mapError(mapError(request, "read-receipt", recordedClaim.id)));
            resumeAllocation = usage.runs.length === 0 && usage.messages.length === 0;
          }
        }
      }
      let autoResolution: AutoLaunchStrategy.AutoLaunchResolution = {
        strategy: replayMessage?.run.workspacePreparation ?? request.workspaceStrategy,
        fallbackDriver: undefined,
      };
      if (priorIdle !== null && request.workspaceStrategy.type === "auto") {
        autoResolution = {
          strategy: { ...request.workspaceStrategy, type: "worktree" },
          fallbackDriver: undefined,
        };
      } else if (!alreadyBound && (needsAdmission || resumeAllocation)) {
        autoResolution = yield* resolveAutoStrategy(
          recordedClaim === undefined
            ? request
            : { ...request, modelSelection: recordedClaim.modelSelection },
        );
      }
      const resolvedRequest: ThreadLaunchInput =
        autoResolution.strategy === request.workspaceStrategy
          ? request
          : { ...request, workspaceStrategy: autoResolution.strategy };
      // Scratch has no Git repository. Normalize before folder allocation so
      // durable preparation and retries use the thread's own Scratch folder.
      const input: ThreadLaunchInput =
        Option.isNone(priorLaunchReceipt) &&
        resolvedRequest.workspaceStrategy.type === "worktree" &&
        (yield* managedFolders.isScratchProject(resolvedRequest.projectId))
          ? { ...resolvedRequest, workspaceStrategy: { type: "root" } }
          : resolvedRequest;
      if (needsAdmission)
        yield* ProjectCloneTracker.rejectCommandsDuringClone(cloneTracker, {
          type: "thread.create",
          projectId: input.projectId,
        }).pipe(Effect.mapError(mapError(input, "resolve-project")));
      const project =
        needsAdmission || (resumeAllocation && input.workspaceStrategy.type === "auto")
          ? yield* resolveLaunchProject(input)
          : null;
      if (input.reuseExistingThread === true && input.threadId === undefined) {
        return yield* mapError(
          input,
          "update-thread",
        )("Reusing an existing thread requires a thread id.");
      }

      const launchReceipt = priorLaunchReceipt;
      if (needsAdmission)
        yield* threads.assertProviderInstanceEnabled({
          projectId: input.projectId,
          commandId: recordedClaim ? initialMessageCommandId(input.commandId) : input.commandId,
          instanceId: (recordedClaim?.modelSelection ?? input.modelSelection).instanceId,
        });
      return yield* Effect.gen(function* () {
        // Recover ids from accepted launch receipts before allocating; retries have no client id and new ids collide with receipts.
        const candidateThreadId =
          recordedClaim?.id ??
          input.threadId ??
          (yield* ids.allocate
            .thread({ projectId: input.projectId })
            .pipe(Effect.mapError(mapError(input, "create-thread"))));

        if (input.reuseExistingThread === true && Option.isNone(launchReceipt)) {
          yield* validateReusableThread(input, candidateThreadId);
        }

        const requestedWorkspaceStrategy: ThreadLaunchWorkspaceStrategy =
          input.workspaceStrategy.type === "root" && Option.isNone(launchReceipt)
            ? Option.match(
                yield* managedFolders
                  .folderForThread({
                    projectId: input.projectId,
                    threadId: candidateThreadId,
                    text: input.initialMessage?.text ?? input.title,
                  })
                  .pipe(Effect.mapError(mapError(input, "provision-worktree", candidateThreadId))),
                {
                  onNone: () => input.workspaceStrategy,
                  onSome: (worktreePath) => ({ type: "existing_worktree", worktreePath }),
                },
              )
            : input.workspaceStrategy;
        const taskPlan =
          needsAdmission && requestedWorkspaceStrategy.type === "worktree"
            ? yield* taskFolders
                .plan({
                  projectId: input.projectId,
                  repositories: requestedWorkspaceStrategy.repositories,
                })
                .pipe(Effect.mapError(mapError(input, "provision-worktree", candidateThreadId)))
            : null;
        if (
          taskPlan !== null &&
          requestedWorkspaceStrategy.type === "worktree" &&
          (input.initialMessage === undefined ||
            Option.isSome(launchPreparation) ||
            input.importedNativeThread !== undefined ||
            requestedWorkspaceStrategy.baseFreshness !== undefined)
        ) {
          return yield* mapError(
            input,
            "provision-worktree",
            candidateThreadId,
          )(
            "This launch path does not have a preparation receipt and base policy for every task folder checkout.",
          );
        }
        // Preserve the admitted selection in the run even when the client asked for defaults.
        const workspaceStrategy =
          taskPlan !== null &&
          requestedWorkspaceStrategy.type === "worktree" &&
          requestedWorkspaceStrategy.repositories === undefined
            ? {
                ...requestedWorkspaceStrategy,
                repositories: taskPlan.repositories.map(({ path }) => ({ path })),
              }
            : requestedWorkspaceStrategy;
        if (project !== null && project.id !== input.projectId)
          return yield* mapError(
            input,
            "resolve-project",
            candidateThreadId,
          )("Project identity changed.");
        const allocation =
          workspaceStrategy.type === "auto" &&
          project !== null &&
          !alreadyBound &&
          (recordedClaim === undefined || resumeAllocation)
            ? autoStrategy.allocationInput(workspaceStrategy, {
                threadId: candidateThreadId,
                projectId: project.id,
                projectRoot: project.workspaceRoot,
              })
            : undefined;
        // Jones hook: deterministic checks leave no thread claim. Allocation checks again after claim.
        if (allocation !== undefined && recordedClaim === undefined)
          yield* autoStrategy
            .preflight(allocation)
            .pipe(
              Effect.mapError((cause) =>
                mapConversationFailure(
                  input,
                  cause,
                  "preflight",
                  input.reuseExistingThread === true ? candidateThreadId : undefined,
                ),
              ),
            );
        // A previously accepted claim is never redispatched. Only a fresh creation reads its checkout.
        const initialBranch =
          project !== null && recordedClaim === undefined
            ? yield* launchBranch(workspaceStrategy, project.workspaceRoot)
            : null;
        const initialWorktreePath =
          workspaceStrategy.type === "existing_worktree" ? workspaceStrategy.worktreePath : null;
        const initialMessage =
          input.initialMessage === undefined
            ? undefined
            : {
                messageId:
                  replayMessage?.message.id ??
                  input.initialMessage.messageId ??
                  (yield* ids.allocate
                    .message({ threadId: candidateThreadId, ordinal: 1 })
                    .pipe(Effect.mapError(mapError(input, "dispatch-message", candidateThreadId)))),
                text: input.initialMessage.text,
                ...(input.initialMessage.scheduledTaskId === undefined
                  ? {}
                  : { scheduledTaskId: input.initialMessage.scheduledTaskId }),
                ...(input.initialMessage.senderThreadId === undefined
                  ? {}
                  : { senderThreadId: input.initialMessage.senderThreadId }),
                attachments: input.initialMessage.attachments,
                ...(input.initialMessage.context ? { context: input.initialMessage.context } : {}),
                ...(input.initialMessage.outputSchema === undefined
                  ? {}
                  : { outputSchema: input.initialMessage.outputSchema }),
                ...(input.generateTitle === true ? { titleSeed: input.title } : {}),
              };
        // Jones hook: a worktree launched without a message holds later messages until it is ready.
        const holdsIdleWorktree =
          workspaceStrategy.type === "worktree" && input.initialMessage === undefined;
        const idleRequestKey = legacyPayloadHash(
          canonicalLegacyPayload({
            projectId: input.projectId,
            workspaceStrategy,
            runSetupScript: input.runSetupScript !== false,
            reuseExistingThread: input.reuseExistingThread === true,
          }),
        );
        const idleIntent: IdleWorktreeAdmission.Intent | undefined = holdsIdleWorktree
          ? {
              threadId: candidateThreadId,
              commandId: input.commandId,
              requestKey: idleRequestKey,
              generation: yield* randomUuidV4,
              claimType:
                input.reuseExistingThread === true ? "thread.metadata.update" : "thread.create",
            }
          : undefined;
        const claimDispatch = (projectRoot: string) =>
          input.reuseExistingThread === true
            ? dispatchAccepted(
                {
                  type: "thread.metadata.update",
                  commandId: input.commandId,
                  threadId: candidateThreadId,
                  expectedEmpty: true,
                },
                projectRoot,
              )
            : dispatchAccepted(
                {
                  type: "thread.create",
                  commandId: input.commandId,
                  threadId: candidateThreadId,
                  projectId: input.projectId,
                  title: input.title,
                  modelSelection: input.modelSelection,
                  runtimeMode: input.runtimeMode,
                  interactionMode: input.interactionMode,
                  branch: initialBranch,
                  worktreePath: initialWorktreePath,
                  ...(input.importedNativeThread === undefined
                    ? {}
                    : { importedNativeThread: input.importedNativeThread }),
                  ...(input.linkOrigin === undefined ? {} : { linkOrigin: input.linkOrigin }),
                  ...(input.delegatedFrom === undefined
                    ? {}
                    : { delegatedFrom: input.delegatedFrom }),
                  ...(input.workflowRun === undefined ? {} : { workflowRun: input.workflowRun }),
                  createdBy: input.createdBy,
                  creationSource: input.creationSource,
                  // On write binds before recording the message. Agent messages recheck caller modes.
                  ...(workspaceStrategy.type === "auto" ||
                  initialMessage === undefined ||
                  input.createdBy === "agent" ||
                  input.initialMessage?.sourcePlanRef !== undefined
                    ? {}
                    : { initialMessage: { ...initialMessage, workspaceStrategy } }),
                },
                projectRoot,
              );
        const claimed =
          recordedClaim && Option.isSome(launchReceipt)
            ? {
                sequence: launchReceipt.value.resultSequence,
                storedEvents: claimEvents,
                replayed: true,
              }
            : project === null
              ? yield* mapError(
                  input,
                  "create-thread",
                  candidateThreadId,
                )("Missing fresh launch admission.")
              : yield* claimDispatch(project.workspaceRoot).pipe(
                  Effect.provideService(IdleWorktreeAdmission.LaunchIntent, idleIntent),
                  Effect.mapError(
                    mapError(
                      input,
                      input.reuseExistingThread === true ? "update-thread" : "create-thread",
                      candidateThreadId,
                    ),
                  ),
                );
        // A concurrent caller may have committed between our admission read and claim.
        // Qualify that winner's immutable results before using its thread or first prompt.
        if (Option.isNone(launchReceipt) && claimed.replayed === true)
          return yield* launch(request);
        const threadId =
          claimed.storedEvents.find((stored) => stored.event.type.startsWith("thread."))?.event
            .threadId ?? candidateThreadId;
        if (allocation !== undefined) {
          const created =
            Option.isNone(launchReceipt) &&
            claimed.replayed !== true &&
            input.reuseExistingThread !== true
              ? claimed.storedEvents.find(
                  ({ event }) => event.type === "thread.created" && event.threadId === threadId,
                )?.event
              : undefined;
          let cleanup: "deleted" | "preserved" | "unknown" =
            created === undefined ? "preserved" : "unknown";
          const removeUnusedShell =
            created?.type !== "thread.created"
              ? undefined
              : threads
                  .dispatch({
                    type: "thread.delete",
                    commandId: CommandId.make(`${input.commandId}:on-write-allocation-failed`),
                    threadId,
                    onlyIfUnused: true,
                  })
                  .pipe(
                    Effect.provideService(
                      ConversationPolicy.FailedLaunchCleanup,
                      created.payload.updatedAt,
                    ),
                    Effect.match({
                      onFailure: () => {
                        cleanup = "unknown";
                      },
                      onSuccess: ({ storedEvents }) => {
                        cleanup = storedEvents.some(
                          ({ event }) =>
                            event.type === "thread.deleted" && event.threadId === threadId,
                        )
                          ? "deleted"
                          : "preserved";
                      },
                    }),
                  );
          yield* autoStrategy
            .allocate({ ...allocation, threadId }, removeUnusedShell)
            .pipe(
              Effect.mapError((cause) =>
                mapConversationFailure(input, cause, "allocation", threadId, cleanup),
              ),
            );
        } else yield* autoStrategy.observeFallback(autoResolution, threadId);
        const idleRecord = priorIdle ?? (yield* readIdleWorktree(input, threadId));
        if (
          (holdsIdleWorktree && idleRecord === null) ||
          (idleRecord !== null &&
            (idleRecord.commandId !== input.commandId || idleRecord.requestKey !== idleRequestKey))
        )
          return yield* mapError(
            input,
            "read-receipt",
            threadId,
          )("Idle worktree launch has no matching durable preparation identity.");
        if (idleRecord !== null && idleWorktrees !== undefined)
          yield* resumeIdlePreparation(
            { ...input, workspaceStrategy },
            { identity: idleRecord, store: idleWorktrees },
          );
        let runId: RunId | null = null;
        let messageSequence: number | undefined;
        let messageWasAlreadyAccepted = false;
        if (initialMessage !== undefined) {
          // A new thread's claim carries its first message. A reused thread, or a replayed
          // claim that carried none, takes it in a command of its own.
          let accepted = replayMessage ?? claimed;
          if (
            !replayMessage &&
            !claimed.storedEvents.some((stored) => stored.event.type === "run.created")
          ) {
            const messageCommandId = initialMessageCommandId(input.commandId);
            const messageReceipt = yield* readReceipt(input, messageCommandId);
            messageWasAlreadyAccepted = Option.isSome(messageReceipt);
            const inheritedDispatchLimit = yield* DispatchModeLimit;
            if (project === null)
              return yield* mapError(
                input,
                "dispatch-message",
                threadId,
              )("Missing fresh message admission.");
            accepted = yield* dispatchAccepted(
              {
                ...initialMessage,
                ...(input.initialMessage?.sourcePlanRef === undefined
                  ? {}
                  : { sourcePlanRef: input.initialMessage.sourcePlanRef }),
                type: "message.dispatch",
                commandId: messageCommandId,
                threadId,
                modelSelection: recordedClaim?.modelSelection ?? input.modelSelection,
                dispatchMode: { type: "defer_start", workspaceStrategy },
                createdBy: input.createdBy,
                creationSource: input.creationSource,
              },
              project.workspaceRoot,
            ).pipe(
              // An agent's launch can land on a thread that a replay, or a concurrent
              // launch with the same key, created with broader modes. The orchestrator
              // refuses the message under the thread's lock; the user's own clients
              // launch into their own drafts, whatever modes those hold.
              input.createdBy === "agent"
                ? Effect.provideService(DispatchModeLimit, {
                    runtimeMode:
                      inheritedDispatchLimit !== undefined &&
                      exceededDispatchModeLimit(inheritedDispatchLimit, {
                        runtimeMode: input.runtimeMode,
                        interactionMode: inheritedDispatchLimit.interactionMode,
                      }) === "runtime"
                        ? inheritedDispatchLimit.runtimeMode
                        : input.runtimeMode,
                    interactionMode:
                      inheritedDispatchLimit?.interactionMode === "plan"
                        ? "plan"
                        : input.interactionMode,
                    ...(inheritedDispatchLimit?.refused === undefined
                      ? {}
                      : { refused: inheritedDispatchLimit.refused }),
                  })
                : (effect) => effect,
              Effect.mapError(mapError(input, "dispatch-message", threadId)),
            );
          }
          // The derived message can race independently after an earlier shell-only claim.
          if (!replayMessage && accepted !== claimed && accepted.replayed === true)
            return yield* launch(request);
          messageWasAlreadyAccepted ||= accepted.replayed === true;
          messageSequence = accepted.sequence;
          const runCreated = accepted.storedEvents.find(
            (stored) => stored.event.type === "run.created",
          );
          runId = runCreated?.event.type === "run.created" ? runCreated.event.payload.id : null;
          if (runId === null) {
            return yield* mapError(
              input,
              "dispatch-message",
              threadId,
            )("Initial message was accepted without a durable run.");
          }
        }

        const projection = yield* threads
          .getThreadProjection(threadId)
          .pipe(Effect.mapError(mapError(input, "create-thread", threadId)));

        // Replay resumes a preparing run through its reservation. Idle worktrees resume above.
        const shouldSchedule =
          idleRecord === null && (runId === null ? Option.isNone(launchReceipt) : true);
        if (shouldSchedule && (yield* reservePreparation(input.commandId))) {
          yield* Effect.gen(function* () {
            const current =
              runId === null
                ? null
                : yield* threads
                    .getThreadRecords(threadId, ["runs"], { runIds: [runId] })
                    .pipe(Effect.mapError(mapError(input, "update-thread", threadId)));
            if (
              current !== null &&
              !current.runs.some((run) => run.id === runId && run.status === "preparing")
            ) {
              return yield* releasePreparation(input.commandId);
            }
            // Jones hook: inspect durable progress only while owning this reservation.
            if (input.holdPreparationAfterProgress === true && runId !== null) {
              for (const suffix of [":progress:worktree", ":progress:setup"]) {
                const progress = yield* readReceipt(
                  input,
                  CommandId.make(`${input.commandId}${suffix}`),
                );
                if (Option.isSome(progress) && progress.value.status === "accepted")
                  return yield* releasePreparation(input.commandId);
              }
            }
            const boundWorktreePath = current?.thread.worktreePath ?? null;
            // A retry prepares the worktree the accepted launch asked for, not the
            // retry's own strategy, whose defaults may resolve differently.
            const accepted = current?.runs.find((run) => run.id === runId)?.workspacePreparation;
            const preparationStrategy: ThreadLaunchWorkspaceStrategy =
              Option.isSome(launchReceipt) &&
              (accepted?.type === "worktree" || accepted?.type === "existing_worktree")
                ? accepted
                : Option.isSome(launchReceipt) &&
                    workspaceStrategy.type === "root" &&
                    boundWorktreePath !== null
                  ? {
                      type: "existing_worktree",
                      worktreePath: boundWorktreePath,
                      branch: workspaceStrategy.branch,
                    }
                  : workspaceStrategy;
            // The thread's link decides, as it does when preparation is retried: a request
            // that reuses a thread names none.
            const { linkOrigin: _requestLinkOrigin, ...preparation } = input;
            // The accepted run's message says who asked, as it does when preparation is
            // retried: a request that repeats the launch may name someone else.
            const acceptedRun = current?.runs.find((run) => run.id === runId);
            const acceptedMessages =
              acceptedRun === undefined
                ? null
                : (yield* threads
                    .getThreadRecords(threadId, ["messages"], {
                      messageIds: [acceptedRun.userMessageId],
                    })
                    .pipe(Effect.mapError(mapError(input, "update-thread", threadId)))).messages;
            const askedBy =
              current === null || acceptedRun === undefined || acceptedMessages === null
                ? input.createdBy === "agent"
                  ? { askedByAgent: { threadId: input.initialMessage?.senderThreadId } }
                  : {}
                : askedByAgent(
                    current.thread,
                    acceptedMessages.find((message) => message.id === acceptedRun.userMessageId),
                  );
            yield* schedulePreparation(
              {
                ...preparation,
                ...askedBy,
                ...(current?.thread.linkOrigin === undefined
                  ? {}
                  : { linkOrigin: current.thread.linkOrigin }),
                ...(Option.isNone(launchReceipt) &&
                !messageWasAlreadyAccepted &&
                claimed.replayed !== true &&
                workspaceStrategy.type === "worktree"
                  ? { freshTaskFolderLaunch: taskPlan !== null }
                  : {}),
                ...(replayMessage === undefined
                  ? {}
                  : {
                      initialMessage: {
                        messageId: replayMessage.message.id,
                        text: replayMessage.message.text,
                        attachments: replayMessage.message.attachments,
                        context: replayMessage.message.context,
                      },
                      ...(replayMessage.run.workspaceRunSetupScript === undefined
                        ? {}
                        : { runSetupScript: replayMessage.run.workspaceRunSetupScript }),
                    }),
                workspaceStrategy: preparationStrategy,
                nativeCreation: input.importedNativeThread !== undefined,
              },
              threadId,
              runId,
            );
          }).pipe(Effect.onError(() => releasePreparation(input.commandId)));
        }

        return {
          threadId,
          projection,
          // An attempt that failed after claiming the thread recorded nothing, so
          // only a repeat of the final write counts as resumed.
          resumed:
            input.initialMessage === undefined
              ? Option.isSome(launchReceipt) || claimed.replayed === true
              : messageWasAlreadyAccepted,
          ...(messageSequence === undefined ? {} : { sequence: messageSequence }),
        };
      });
    },
    // The message commits a preparing run that only this call schedules, so a
    // caller that goes away must not stop the launch between the two.
    Effect.uninterruptible,
  );

  const prepareRun: ThreadLaunchService["Service"]["prepareRun"] = Effect.fn(
    "ThreadLaunchService.prepareRun",
  )(function* (input) {
    const scheduled = yield* Effect.gen(function* () {
      const { thread, runs } = yield* threads.getThreadRecords(input.threadId, ["runs"], {
        runIds: [input.runId],
      });
      const run = runs.find((candidate) => candidate.id === input.runId);
      const workspacePreparation = run?.workspacePreparation;
      if (run?.status !== "preparing" || workspacePreparation === undefined) return;
      // Ordinary checkout custody requires a resolved strategy; the receiving
      // preparation resolves `auto` and an omitted base itself.
      if (
        Option.isSome(launchPreparation) &&
        (workspacePreparation.type === "auto" ||
          (workspacePreparation.type === "worktree" && workspacePreparation.baseRef === undefined))
      )
        return yield* new ThreadLaunchError({
          operation: "provision-worktree",
          commandId: input.commandId,
          projectId: thread.projectId,
          threadId: input.threadId,
          cause: "Stored worktree preparation has no resolved strategy.",
        });
      if (!(yield* reservePreparation(input.commandId))) return;
      yield* scheduleRunPreparation(input, thread, run, workspacePreparation).pipe(
        Effect.onError(() => releasePreparation(input.commandId)),
      );
    }).pipe(Effect.exit);
    if (Exit.isFailure(scheduled)) {
      yield* failPreparedRun(
        input,
        input.threadId,
        input.runId,
        Cause.squash(scheduled.cause),
        false,
      );
    }
  });

  const retryPreparation: ThreadLaunchService["Service"]["retryPreparation"] = Effect.fn(
    "ThreadLaunchService.retryPreparation",
  )(function* (input) {
    const dispatched = yield* threads.dispatch({
      type: "prepared-run.retry",
      commandId: input.commandId,
      threadId: input.threadId,
      runId: input.runId,
    });
    yield* prepareRun(input);
    return dispatched;
  });

  const scheduleRunPreparation = Effect.fn("ThreadLaunchService.scheduleRunPreparation")(function* (
    input: ThreadLaunchPreparationInput,
    thread: OrchestrationV2ThreadProjection["thread"],
    run: RecordedRun,
    workspacePreparation: ThreadLaunchWorkspaceStrategy,
  ) {
    const { messages } = yield* threads.getThreadRecords(input.threadId, ["messages"], {
      messageIds: [run.userMessageId],
    });
    const message = messages.find((candidate) => candidate.id === run.userMessageId);
    const reuse =
      workspacePreparation.type === "worktree" &&
      thread.worktreePath !== null &&
      thread.branch !== null
        ? {
            strategy: {
              type: "existing_worktree" as const,
              worktreePath: thread.worktreePath,
              branch: thread.branch,
            },
            reusedWorktree: { baseRef: workspacePreparation.baseRef },
          }
        : null;
    return yield* schedulePreparation(
      {
        commandId: input.commandId,
        projectId: thread.projectId,
        workspaceStrategy: reuse?.strategy ?? workspacePreparation,
        ...(thread.linkOrigin === undefined ? {} : { linkOrigin: thread.linkOrigin }),
        ...askedByAgent(thread, message),
        ...(run.workspaceRunSetupScript === undefined
          ? {}
          : { runSetupScript: run.workspaceRunSetupScript }),
        ...(reuse === null ? {} : { reusedWorktree: reuse.reusedWorktree }),
        ...(message === undefined
          ? {}
          : {
              initialMessage: {
                text: message.text,
                attachments: message.attachments,
                ...(message.context ? { context: message.context } : {}),
                ...(run.outputSchema === undefined ? {} : { outputSchema: run.outputSchema }),
              },
            }),
      },
      input.threadId,
      run.id,
    );
  });

  return ThreadLaunchService.of({
    dispatchNativeBootstrap: Effect.fn("ThreadLaunchService.dispatchNativeBootstrap")(
      (submission) =>
        Effect.flatMap(EnvironmentAuthenticatedPrincipal, (principal) =>
          Option.isSome(nativeBootstrapConfig) && Option.isSome(nativeBootstrapPath)
            ? NativeBootstrap.dispatchNativeBootstrap(submission, {
                bootId: nativeBootstrapBootId,
                worktreesDir: nativeBootstrapConfig.value.worktreesDir,
                deriveRunId: (threadId) => ids.derive.run({ threadId, ordinal: 1 }),
              }).pipe(
                Effect.provideService(EnvironmentAuthenticatedPrincipal, principal),
                Effect.provideService(Path.Path, nativeBootstrapPath.value),
                Effect.provide(nativeBootstrapContext),
              )
            : Effect.fail(
                new NativeAuthority.NativeCreationAuthorityError({
                  code: "unsupported_authority",
                  message: "Native bootstrap server configuration is unavailable",
                }),
              ),
        ),
    ),
    prepareNativeWorkspace: Effect.fn("ThreadLaunchService.prepareNativeWorkspace")((input) =>
      Option.isSome(nativeWorkspace)
        ? nativeWorkspace.value.prepare(input)
        : Effect.fail(
            new NativeWorkspace.NativeWorkspaceError({
              code: "unavailable",
              message: "Native workspace preparation is not qualified",
            }),
          ),
    ),
    launch,
    preflightLegacyBootstrap: Effect.fn("ThreadLaunchService.preflightLegacyBootstrap")(
      function* (binding) {
        if (Option.isNone(legacyBootstrap))
          return yield* new ThreadLaunchError({
            operation: "provision-worktree",
            commandId: binding.policy.createCommandId,
            projectId: binding.policy.projectId,
            threadId: binding.policy.threadId,
            cause: "Legacy preflight requires its owning database and effect outbox.",
          });
        return yield* (yield* legacyBootstrap.value).preflightLegacyBootstrap(binding);
      },
    ),
    getDeliveryEvidence: Effect.fn("ThreadLaunchService.getDeliveryEvidence")((input) =>
      launchDeliveryEvidence(receipts, eventSink, input),
    ),
    checkWorktreeBase,
    isNonGitProject,
    retryPreparation,
    preparationActive: Effect.fn("ThreadLaunchService.preparationActive")((commandId) =>
      Ref.get(scheduledLaunches).pipe(Effect.map((scheduled) => scheduled.has(commandId))),
    ),
    prepareRun,
  });
});

// Ordinary checkout custody. Production does not install it until that graph qualifies.
export const layer = Layer.effect(
  ThreadLaunchService,
  Effect.flatMap(LaunchPreparation.LaunchPreparation, (preparation) =>
    make(Option.some(preparation)),
  ),
);

/**
 * Production composition: launches prepare through the receiving owners (auto
 * resolution, provenance capture, idle-worktree admission, metadata reads) with
 * no ordinary checkout custody. A host that installs CheckoutIntent without
 * LaunchPreparation still refuses preparation before Git.
 */
export const layerProduction = Layer.effect(ThreadLaunchService, make(Option.none()));

// Explicit upstream fixture path. It never issues ordinary preparation custody;
// installing CheckoutIntent makes its preparation entry refuse before Git.
export const layerRawForTests = Layer.effect(ThreadLaunchService, make(Option.none()));
