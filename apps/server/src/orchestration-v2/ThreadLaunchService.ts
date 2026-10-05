import { canonicalJson } from "./CanonicalJson.ts";
import * as DateTime from "effect/DateTime";
import * as FileSystem from "effect/FileSystem";
import * as ServerConfig from "../config.ts";
import { nativeWorktreePath } from "../vcs/worktreePath.ts";
import * as Stream from "effect/Stream";
import * as Fiber from "effect/Fiber";
import { EventSinkV2 } from "./EventSink.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import { DelegatedCheckoutPlanV1 } from "./DelegatedCheckoutPolicy.ts";
import * as OrdinaryCheckout from "./OrdinaryCheckoutOwnership.ts";
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
  type OrchestrationV2CreationSource,
  type OrchestrationV2ProviderThreadNativeMetadata,
  type OrchestrationV2ThreadProjection,
  type ProviderDriverKind,
  type ProviderInteractionMode,
  ProjectId,
  type RunId,
  type RuntimeMode,
  type VcsRef,
  type ScheduledTaskId,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import {
  buildTemporaryWorktreeBranchName,
  isTemporaryWorktreeBranch,
  resolveDefaultWorktreeBaseBranch,
} from "@t3tools/shared/git";

import * as GitWorkflow from "../git/GitWorkflowService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "../project/ProjectSetupScriptRunner.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as ProviderRegistry from "../provider/Services/ProviderRegistry.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as TextGeneration from "../textGeneration/TextGeneration.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as IdAllocator from "./IdAllocator.ts";
import type * as Orchestrator from "./Orchestrator.ts";
import { makeProviderFailure } from "./ProviderFailure.ts";
import { randomUuidV4 } from "./RandomUuid.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";

export type ThreadLaunchWorkspaceStrategy =
  | { readonly type: "root"; readonly branch?: string | undefined }
  | {
      readonly type: "existing_worktree";
      readonly worktreePath: string;
      readonly branch?: string | undefined;
    }
  | {
      readonly type: "worktree";
      readonly baseRef?: string | undefined;
      readonly branch?: string | undefined;
      readonly startFromOrigin?: boolean | undefined;
    };

export interface ThreadLaunchInitialMessage {
  readonly messageId?: MessageId;
  readonly scheduledTaskId?: ScheduledTaskId;
  readonly senderThreadId?: ThreadId;
  readonly text: string;
  readonly attachments: ReadonlyArray<ChatAttachment>;
  readonly context?: import("@t3tools/contracts").OrchestrationMessageContext | undefined;
}

export interface ThreadLaunchInput {
  readonly commandId: CommandId;
  readonly threadId?: ThreadId;
  readonly reuseExistingThread?: boolean;
  readonly projectId: ProjectId;
  readonly title: string;
  readonly generateTitle?: boolean;
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
}

/** What workspace preparation reads from a launch; a retry rebuilds it from the run. */
type PreparationInput = Pick<
  ThreadLaunchInput,
  "commandId" | "projectId" | "workspaceStrategy" | "initialMessage"
> & {
  /**
   * Set when a retry reuses the worktree its failed attempt created and
   * recorded. Its setup is tracked like a new one, but the thread already
   * records the workspace, and a branch rename may still be running.
   */
  readonly reusedWorktree?: { readonly baseRef: string | undefined };
  readonly preparedTarget?: {
    readonly branch: string | null;
    readonly worktreePath: string | null;
  };
  readonly acceptedPreparation?: {
    readonly command: Extract<
      import("@t3tools/contracts").OrchestrationV2Command,
      { readonly type: "thread.create" | "thread.metadata.update" }
    >;
    readonly event: OrdinaryCheckout.OrdinaryAcceptedEventV1;
  };
};

export interface ThreadLaunchRetryInput {
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly runId: RunId;
}

export interface ThreadLaunchResult {
  readonly threadId: ThreadId;
  readonly projection: OrchestrationV2ThreadProjection;
  readonly resumed: boolean;
}

export type OrdinaryPreparedPhysicalResultV1 = {
  readonly version: 1;
  readonly producerId: string;
  readonly execution: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1;
  readonly targetSource: {
    readonly projectWorkspaceRoot: string;
    readonly worktreePath: string | null;
  };
  readonly checkoutPath: string;
  readonly observedAt: string;
} & (
  | {
      readonly kind: "prepared_setup_completed";
      readonly branch: string | null;
      readonly readback?: {
        readonly cwd: string;
        readonly refName: string | null;
        readonly isRepo: boolean;
      };
      readonly worktree: {
        readonly path: string;
        readonly refName: string;
        readonly headSha?: string;
      } | null;
      readonly setup:
        | { readonly status: "no-script" }
        | {
            readonly status: "completed";
            readonly scriptId: string;
            readonly terminalId: string;
            readonly cwd: string;
            readonly exitCode: number;
            readonly durationMs: number;
          };
    }
  | {
      readonly kind: "prepared_branch_renamed";
      readonly oldBranch: string;
      readonly requestedBranch: string;
      readonly renamedBranch: string;
      readonly readback: { readonly cwd: string; readonly refName: string };
    }
  | {
      readonly kind: "prepared_failure_observed";
      readonly branch: string | null;
      readonly readback: {
        readonly cwd: string;
        readonly refName: string | null;
        readonly isRepo: boolean;
      };
      readonly worktree: {
        readonly path: string;
        readonly refName: string;
        readonly headSha?: string;
      } | null;
      readonly failure: string;
      readonly setup: {
        readonly status: "no_managed_process";
        readonly managerId: string;
        readonly ownerBirth: OrdinaryCheckout.OrdinaryApplicationBirthV1;
        readonly targetCount: 0;
      };
    }
);

const issuedOrdinaryPreparedPhysicalResults = new WeakMap<
  object,
  {
    readonly result: OrdinaryPreparedPhysicalResultV1;
    readonly snapshot: string;
  }
>();

export function readIssuedOrdinaryPreparedPhysicalResult(
  value: unknown,
): OrdinaryPreparedPhysicalResultV1 | null {
  if (value === null || typeof value !== "object") return null;
  const issued = issuedOrdinaryPreparedPhysicalResults.get(value);
  if (issued === undefined) return null;
  try {
    return JSON.stringify(issued.result) === issued.snapshot ? issued.result : null;
  } catch {
    return null;
  }
}

function issueOrdinaryPreparedPhysicalResult<T extends OrdinaryPreparedPhysicalResultV1>(
  result: T,
): T {
  issuedOrdinaryPreparedPhysicalResults.set(result, { result, snapshot: JSON.stringify(result) });
  return result;
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
  },
) {
  override get message(): string {
    return `Thread launch ${this.commandId} failed during ${this.operation}.`;
  }
}

export class ThreadLaunchService extends Context.Service<
  ThreadLaunchService,
  {
    readonly prepareDelegated?: (
      effect: EffectOutbox.OrchestrationEffectV2,
    ) => Effect.Effect<void, ThreadLaunchError | Orchestrator.OrchestratorV2Error>;
    readonly launch: (
      input: ThreadLaunchInput,
    ) => Effect.Effect<ThreadLaunchResult, ThreadLaunchError>;
    /** Dispatches prepared-run.retry and prepares the run's workspace again. */
    readonly retryPreparation: (
      input: ThreadLaunchRetryInput,
    ) => Effect.Effect<Orchestrator.OrchestratorV2DispatchResult, Orchestrator.OrchestratorV2Error>;
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

const make = Effect.gen(function* () {
  const eventSink = yield* Effect.serviceOption(EventSinkV2);
  const effectOutbox = yield* Effect.serviceOption(EffectOutbox.EffectOutboxV2);
  const fileSystem = yield* Effect.serviceOption(FileSystem.FileSystem);
  const serverConfig = yield* Effect.serviceOption(ServerConfig.ServerConfig);
  const projects = yield* ProjectService.ProjectService;
  const setupTracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
  const cloneTracker = yield* ProjectCloneTracker.ProjectCloneTracker;
  const terminals = yield* TerminalManager.TerminalManager;
  const git = yield* GitWorkflow.GitWorkflowService;
  const setupScripts = yield* ProjectSetupScriptRunner.ProjectSetupScriptRunner;
  const providerRegistry = yield* ProviderRegistry.ProviderRegistry;
  const serverSettings = yield* ServerSettings.ServerSettingsService;
  const textGeneration = yield* TextGeneration.TextGeneration;
  const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
  const ids = yield* IdAllocator.IdAllocatorV2;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const managedFolders = yield* ManagedProjectFolders.ManagedProjectFolders;
  const preparationScope = yield* Scope.make("sequential");
  const scheduledLaunches = yield* Ref.make<ReadonlySet<CommandId>>(new Set());
  yield* Effect.addFinalizer(() => Scope.close(preparationScope, Exit.void));

  const mapError =
    (input: PreparationInput, operation: ThreadLaunchError["operation"], threadId?: ThreadId) =>
    (cause: unknown) =>
      new ThreadLaunchError({
        operation,
        commandId: input.commandId,
        projectId: input.projectId,
        ...(threadId === undefined ? {} : { threadId }),
        cause,
      });

  const readReceipt = (input: ThreadLaunchInput, commandId: CommandId) =>
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

  const prepareInBackground = Effect.fn("ThreadLaunchService.prepareInBackground")(function* (
    input: PreparationInput,
    threadId: ThreadId,
    runId: RunId | null,
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

    if (
      Option.isNone(eventSink) ||
      eventSink.value.ordinaryCheckoutLifetime === undefined ||
      Option.isNone(fileSystem)
    )
      return yield* mapError(
        input,
        "provision-worktree",
        threadId,
      )("The owning preparation admission and physical observer are unavailable.");
    const lifetime = eventSink.value.ordinaryCheckoutLifetime;
    const fs = fileSystem.value;
    const initial = yield* threads
      .getThreadProjection(threadId)
      .pipe(Effect.mapError(mapError(input, "update-thread", threadId)));
    const target = input.preparedTarget ?? {
      branch: initial.thread.branch,
      worktreePath: initial.thread.worktreePath,
    };
    const targetSource = {
      projectWorkspaceRoot: project.workspaceRoot,
      worktreePath: target.worktreePath,
    };
    const canonicalProjectRoot = yield* fs
      .realPath(project.workspaceRoot)
      .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
    const canonicalCheckoutPath =
      input.workspaceStrategy.type === "worktree" && input.reusedWorktree === undefined
        ? target.worktreePath!
        : yield* fs
            .realPath(target.worktreePath ?? project.workspaceRoot)
            .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
    const admission =
      runId === null
        ? input.acceptedPreparation === undefined
          ? null
          : yield* lifetime
              .capturePreparedLaunch({
                command: input.acceptedPreparation.command,
                preparationEvent: input.acceptedPreparation.event,
                target: {
                  threadId,
                  projectId: project.id,
                  branch: target.branch,
                  canonicalProjectRoot,
                  canonicalCheckoutPath,
                  source: targetSource,
                },
              })
              .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)))
        : yield* lifetime
            .readAdmissionForRun({ threadId, runId })
            .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
    if (
      admission === null ||
      admission.capture.threadId !== threadId ||
      admission.capture.projectId !== project.id ||
      admission.capture.canonicalProjectRoot !== canonicalProjectRoot ||
      admission.capture.canonicalCheckoutPath !== canonicalCheckoutPath ||
      (runId !== null && admission.run?.runId !== runId)
    )
      return yield* mapError(
        input,
        "provision-worktree",
        threadId,
      )("The accepted preparation lost its original birth, run or physical target.");
    const admissionRef = OrdinaryCheckout.ordinaryCheckoutAdmissionRefV1(admission);
    const source: typeof OrdinaryCheckout.OrdinaryCheckoutPreparedExecutionSourceV1.Type =
      runId !== null && admission.run !== null
        ? { kind: "prepared_run", admission: admissionRef, preparation: admission.run }
        : {
            kind: "prepared_launch",
            admission: admissionRef,
            preparationCommandId: admission.capture.commandId,
            preparationEvent: input.acceptedPreparation!.event,
            applicationBirth: admission.capture.applicationBirth,
            projectId: admission.capture.projectId,
            canonicalProjectRoot,
            canonicalCheckoutPath,
            branch: admission.capture.branch,
          };
    const producerId = yield* randomUuidV4;
    const producerFiber = yield* Effect.fiber;
    let producerActive = true;
    let created = input.workspaceStrategy.type !== "worktree";
    let creationEntered = false;
    let physicalUnknown = false;
    let ownershipLost = false;
    let releaseEntered = false;
    let effectiveBranch = target.branch;
    let renameFiber: Fiber.Fiber<void, never> | null = null;
    let renameEntered = false;
    const revalidateProducer = Effect.gen(function* () {
      if (
        !producerActive ||
        producerFiber.pollUnsafe() !== undefined ||
        physicalUnknown ||
        (yield* fs.realPath(project.workspaceRoot)) !== canonicalProjectRoot
      )
        return yield* mapError(
          input,
          "provision-worktree",
          threadId,
        )("The original physical preparation producer is unavailable.");
      if (created || (creationEntered && (yield* fs.exists(canonicalCheckoutPath)))) {
        if (
          (yield* fs.realPath(target.worktreePath ?? project.workspaceRoot)) !==
          canonicalCheckoutPath
        )
          return yield* mapError(
            input,
            "provision-worktree",
            threadId,
          )("The original checkout changed its physical identity.");
      } else if (yield* fs.exists(canonicalCheckoutPath))
        return yield* mapError(
          input,
          "provision-worktree",
          threadId,
        )("An unqualified existing checkout cannot replace the original future target.");
    });
    const reserved = yield* lifetime
      .beginUse({
        operationId: `${input.commandId}:ordinary-preparation`,
        admission: admissionRef,
        source,
        targetSource,
      })
      .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
    if (reserved.status === "observe_only") return;
    const originalUse = reserved.record.subject.use;
    const execution = yield* lifetime
      .bindExecution({
        originalUse,
        executor: { kind: "actual_prepared_producer", producerId, source },
        targetSource,
        revalidateProducer,
      })
      .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
    const revalidateExecution = lifetime.revalidateExecution(execution).pipe(
      Effect.asVoid,
      Effect.onError(() =>
        Effect.sync(() => {
          ownershipLost = true;
        }),
      ),
      Effect.mapError(mapError(input, "provision-worktree", threadId)),
    );
    const renew = Effect.gen(function* () {
      yield* revalidateExecution;
      const now = yield* DateTime.now;
      yield* lifetime.renewExecution({
        ref: execution,
        now,
        newExpiry: DateTime.add(now, { minutes: 5 }),
      });
    }).pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
    let worktreeResult: Extract<
      OrdinaryPreparedPhysicalResultV1,
      { kind: "prepared_setup_completed" }
    >["worktree"] = null;
    let setupResult: Extract<
      OrdinaryPreparedPhysicalResultV1,
      { kind: "prepared_setup_completed" }
    >["setup"] = { status: "no-script" };
    yield* revalidateExecution;

    const reused = input.reusedWorktree;
    const tracked = input.workspaceStrategy.type === "worktree" || reused !== undefined;
    let createdWorktreePath: string | null = null;
    let setupTerminalId: string | null = null;
    let workspaceRecorded = false;
    if (input.workspaceStrategy.type === "worktree") {
      yield* setupTracker.begin({
        threadId,
        branch: input.workspaceStrategy.branch ?? null,
        baseRef: input.workspaceStrategy.baseRef ?? null,
        stages: ["fetch", "checkout", "setup-script", "agent"],
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
    yield* Effect.scoped(
      Effect.gen(function* () {
        const renewal = yield* Effect.forever(
          Effect.sleep("10 seconds").pipe(Effect.andThen(renew)),
        ).pipe(Effect.forkScoped);
        const preparation = Effect.gen(function* () {
          const initialMessage = input.initialMessage;
          const generateBranchNameFor = (cwd: string, message: ThreadLaunchInitialMessage) =>
            Effect.gen(function* () {
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
                })
                .pipe(
                  Effect.map((result) => ({
                    branch: result.branch,
                    exactName: settings.branchNamingMode === "custom",
                  })),
                );
            });

          let branch = target.branch;
          let worktreePath = target.worktreePath;
          if (input.workspaceStrategy.type === "worktree") {
            if (runId !== null) {
              yield* threads
                .dispatch({
                  type: "prepared-run.progress",
                  commandId: CommandId.make(`${input.commandId}:progress:worktree`),
                  threadId,
                  runId,
                  phase: "worktree",
                })
                .pipe(Effect.mapError(mapError(input, "update-thread", threadId)));
            }
            let baseRef = input.workspaceStrategy.baseRef;
            let selectedBase: VcsRef | undefined;
            if (baseRef === undefined) {
              const refs = yield* git
                .listRefs({ cwd: project.workspaceRoot, limit: 100 })
                .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
              const automaticBase = resolveDefaultWorktreeBaseBranch(refs.refs);
              if (!refs.isRepo || automaticBase === null) {
                return yield* mapError(
                  input,
                  "provision-worktree",
                  threadId,
                )(
                  refs.isRepo
                    ? "No default or checked-out branch is available. Select a base branch before retrying."
                    : "New worktree mode requires a Git repository.",
                );
              }
              baseRef = automaticBase;
              selectedBase = refs.refs.find((ref) => ref.name === baseRef);
              yield* setupTracker.update(threadId, (snapshot) => ({
                ...snapshot,
                baseRef: automaticBase,
              }));
            }
            let startRef = baseRef;
            // "Start from origin" is a stored default; repos without the requested
            // remote branch fall back to the local base branch.
            const startFromOrigin =
              input.workspaceStrategy.startFromOrigin === true &&
              (yield* git
                .remoteExists({ cwd: project.workspaceRoot, remoteName: "origin" })
                .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId))));
            yield* setupTracker.stageStatus(
              threadId,
              "fetch",
              startFromOrigin ? "running" : "skipped",
            );
            if (startFromOrigin) {
              if (selectedBase === undefined && baseRef.includes("/")) {
                let cursor: number | undefined;
                do {
                  const refs = yield* git
                    .listRefs({
                      cwd: project.workspaceRoot,
                      query: baseRef,
                      limit: 100,
                      includeMatchingRemoteRefs: true,
                      ...(cursor === undefined ? {} : { cursor }),
                    })
                    .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
                  const localBase = refs.refs.find((ref) => ref.name === baseRef && !ref.isRemote);
                  selectedBase =
                    localBase ?? selectedBase ?? refs.refs.find((ref) => ref.name === baseRef);
                  if (localBase || refs.nextCursor === null) break;
                  cursor = refs.nextCursor;
                } while (true);
              }
              const selectedRemote = selectedBase?.isRemote ? selectedBase.remoteName : undefined;
              const branchName =
                selectedRemote === "origin" ? baseRef.slice("origin/".length) : baseRef;
              // Scoped fetch treats origin/ as a remote prefix, so fetch all refs
              // when that spelling belongs to a local branch or another remote.
              const ambiguousOriginPrefix =
                selectedRemote === undefined && baseRef.startsWith("origin/");
              yield* revalidateExecution;
              yield* git
                .fetchRemote({
                  cwd: project.workspaceRoot,
                  remoteName: "origin",
                  ...(!ambiguousOriginPrefix &&
                  (selectedRemote === undefined || selectedRemote === "origin")
                    ? { refName: baseRef }
                    : {}),
                })
                .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
              const resolvedRemoteBase =
                selectedRemote === undefined || selectedRemote === "origin"
                  ? yield* git
                      .resolveRemoteTrackingCommitIfExists({
                        cwd: project.workspaceRoot,
                        remoteName: "origin",
                        branchName,
                      })
                      .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)))
                  : null;
              if (resolvedRemoteBase !== null) startRef = resolvedRemoteBase.commitSha;
            }
            if (startFromOrigin) yield* setupTracker.stageStatus(threadId, "fetch", "done");
            yield* setupTracker.stageStatus(threadId, "checkout", "running");
            yield* revalidateExecution;
            const worktree = yield* git
              .createWorktree(
                {
                  cwd: project.workspaceRoot,
                  refName: startRef,
                  newRefName: branch!,
                  baseRefName: baseRef,
                  path: target.worktreePath,
                },
                {
                  revalidateMutation: revalidateExecution.pipe(
                    Effect.tap(() =>
                      Effect.sync(() => {
                        creationEntered = true;
                      }),
                    ),
                  ),
                  progress: {
                    onWorktreeClaimed: (path) =>
                      Effect.sync(() => {
                        createdWorktreePath = path;
                      }),
                    onCheckoutProgress: (progress) =>
                      setupTracker.stage(threadId, "checkout", { percent: progress.percent }),
                  },
                },
              )
              .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
            created = true;
            yield* revalidateExecution;
            if (
              worktree.worktree.path !== target.worktreePath ||
              worktree.worktree.refName !== target.branch
            )
              return yield* mapError(
                input,
                "provision-worktree",
                threadId,
              )("The created checkout differs from its preplanned original target.");
            const head = yield* git
              .resolveCommit({ cwd: worktree.worktree.path, revision: "HEAD" })
              .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
            worktreeResult = {
              path: worktree.worktree.path,
              refName: worktree.worktree.refName,
              headSha: head.commitSha,
            };
            worktreePath = worktree.worktree.path;
            branch = worktree.worktree.refName;
            createdWorktreePath = worktreePath;
            yield* setupTracker.update(threadId, (snapshot) => ({
              ...snapshot,
              worktreePath,
              branch,
            }));
            yield* setupTracker.stageStatus(threadId, "checkout", "done");
          }

          // A reused worktree is already recorded, and rewriting it could undo
          // the first attempt's branch rename.
          if (reused === undefined) {
            yield* threads
              .dispatch({
                type: "thread.metadata.update",
                commandId: CommandId.make(`${input.commandId}:workspace`),
                threadId,
                branch,
                worktreePath,
              })
              .pipe(Effect.mapError(mapError(input, "update-thread", threadId)));
          }
          workspaceRecorded = true;

          // Rename temporary branches (server-invented above, or sent by clients
          // that name worktrees themselves) in the background so generation latency
          // never delays provisioning or the provider turn. The temporary name
          // simply sticks if generation or the rename fails.
          if (
            reused === undefined &&
            worktreePath !== null &&
            branch !== null &&
            initialMessage !== undefined &&
            isTemporaryWorktreeBranch(branch)
          ) {
            const oldBranch = branch;
            const worktreeCwd = worktreePath;
            renameFiber = yield* generateBranchNameFor(worktreeCwd, initialMessage).pipe(
              Effect.flatMap(({ branch: newBranch, exactName }) =>
                Effect.gen(function* () {
                  if (threads.dispatchOrdinaryPreparedBranchRename === undefined)
                    return yield* mapError(
                      input,
                      "update-thread",
                      threadId,
                    )("The owning physical branch transition capability is unavailable.");
                  yield* revalidateExecution;
                  renameEntered = true;
                  const renamed = yield* git.renameBranch({
                    cwd: canonicalCheckoutPath,
                    oldBranch,
                    newBranch,
                    ...(exactName ? { exactName: true } : {}),
                  });
                  if (renamed.branch === oldBranch) {
                    renameEntered = false;
                    return;
                  }
                  const readback = yield* git.localStatus({ cwd: canonicalCheckoutPath });
                  if (!readback.isRepo || readback.refName !== renamed.branch)
                    return yield* mapError(
                      input,
                      "update-thread",
                      threadId,
                    )("The original branch rename has no matching physical readback.");
                  const observation = issueOrdinaryPreparedPhysicalResult({
                    version: 1,
                    kind: "prepared_branch_renamed",
                    producerId,
                    execution,
                    targetSource,
                    checkoutPath: canonicalCheckoutPath,
                    observedAt: DateTime.formatIso(yield* DateTime.now),
                    oldBranch,
                    requestedBranch: newBranch,
                    renamedBranch: renamed.branch,
                    readback: { cwd: canonicalCheckoutPath, refName: renamed.branch },
                  });
                  yield* threads.dispatchOrdinaryPreparedBranchRename(
                    {
                      type: "thread.metadata.update",
                      commandId: CommandId.make(`${input.commandId}:branch-rename`),
                      threadId,
                      branch: renamed.branch,
                      worktreePath: worktreeCwd,
                    },
                    observation,
                  );
                  effectiveBranch = renamed.branch;
                  renameEntered = false;
                }),
              ),
              Effect.catchCause((cause) =>
                Effect.gen(function* () {
                  if (renameEntered) {
                    physicalUnknown = true;
                    yield* lifetime
                      .retainExecutionUnknown(
                        execution,
                        `The original physical branch rename has no qualified end: ${String(Cause.squash(cause))}`,
                      )
                      .pipe(Effect.ignore);
                  }
                  yield* Effect.logWarning("Thread worktree branch rename failed", {
                    commandId: input.commandId,
                    threadId,
                    oldBranch,
                    cause,
                  });
                }),
              ),
              Effect.forkScoped,
            );
          }

          const cwd = canonicalCheckoutPath;
          if (runId !== null) {
            yield* threads
              .dispatch({
                type: "prepared-run.progress",
                commandId: CommandId.make(`${input.commandId}:progress:setup`),
                threadId,
                runId,
                phase: "setup",
              })
              .pipe(Effect.mapError(mapError(input, "update-thread", threadId)));
          }
          yield* setupTracker.stageStatus(threadId, "setup-script", "running");
          yield* revalidateExecution;
          const setup = yield* setupScripts
            .runForThread({
              threadId,
              projectId: input.projectId,
              projectCwd: project.workspaceRoot,
              worktreePath: cwd,
              ...(tracked
                ? {
                    observeCompletion: {
                      onOutputLine: (line: string) =>
                        setupTracker.appendTail(threadId, "setup-script", line),
                    },
                  }
                : {}),
              project: {
                id: project.id,
                workspaceRoot: project.workspaceRoot,
                scripts: project.scripts,
              },
            })
            .pipe(Effect.mapError(mapError(input, "run-setup-script", threadId)));

          let awaitAsyncSetup = Effect.void;
          if (setup.status === "started") {
            setupTerminalId = setup.terminalId;
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
                if (
                  completion.exitCode === null ||
                  !Number.isInteger(completion.exitCode) ||
                  !Number.isFinite(completion.durationMs) ||
                  completion.durationMs < 0
                )
                  return yield* mapError(
                    input,
                    "run-setup-script",
                    threadId,
                  )("The actual setup process has no qualified physical completion.");
                setupResult = {
                  status: "completed",
                  scriptId: setup.scriptId,
                  terminalId: setup.terminalId,
                  cwd: setup.cwd,
                  exitCode: completion.exitCode,
                  durationMs: completion.durationMs,
                };
                yield* revalidateExecution;
                yield* setupTracker.stage(threadId, "setup-script", {
                  status: completion.exitCode === 0 ? "done" : "failed",
                  detail: `exited with ${completion.exitCode ?? "no exit code"}`,
                });
                if (completion.exitCode !== 0 && !setup.async)
                  return yield* mapError(
                    input,
                    "run-setup-script",
                    threadId,
                  )(`Setup script exited with ${completion.exitCode ?? "no exit code"}.`);
              });
              if (setup.async) {
                awaitAsyncSetup = awaitCompletion.pipe(
                  Effect.catchCause((cause) =>
                    setupTracker.stage(threadId, "setup-script", {
                      status: "failed",
                      detail: failureDetail(Cause.squash(cause)),
                    }),
                  ),
                );
              } else {
                yield* awaitCompletion;
              }
            } else {
              return yield* mapError(
                input,
                "run-setup-script",
                threadId,
              )("The started setup has no retained physical completion observer.");
            }
          } else {
            yield* setupTracker.stageStatus(threadId, "setup-script", "skipped");
          }
          yield* setupTracker.markUncancellable(threadId);
          yield* setupTracker.stageStatus(threadId, "agent", "running");
          if (runId !== null) {
            yield* revalidateExecution;
            if (threads.dispatchOrdinaryPreparedRunRelease === undefined)
              return yield* mapError(
                input,
                "release-run",
                threadId,
              )("The original preparation release capability is unavailable.");
            releaseEntered = true;
            yield* threads
              .dispatchOrdinaryPreparedRunRelease(
                {
                  type: "prepared-run.release",
                  commandId: CommandId.make(`${input.commandId}:release`),
                  threadId,
                  runId,
                },
                originalUse,
                execution,
              )
              .pipe(Effect.mapError(mapError(input, "release-run", threadId)));
          }
          yield* setupTracker.stageStatus(threadId, "agent", "done");
          yield* awaitAsyncSetup;
          if (renameFiber !== null) yield* Fiber.join(renameFiber);
          yield* revalidateExecution;
          const readback = yield* git.localStatus({ cwd });
          const unspecifiedRoot = targetSource.worktreePath === null && effectiveBranch === null;
          if (
            (!unspecifiedRoot && (!readback.isRepo || readback.refName !== effectiveBranch)) ||
            (!readback.isRepo && readback.refName !== null)
          )
            return yield* mapError(
              input,
              "provision-worktree",
              threadId,
            )("The completed preparation differs from its original physical checkout and branch.");
          const physical = issueOrdinaryPreparedPhysicalResult({
            version: 1,
            kind: "prepared_setup_completed",
            producerId,
            execution,
            targetSource,
            checkoutPath: canonicalCheckoutPath,
            observedAt: DateTime.formatIso(yield* DateTime.now),
            branch: effectiveBranch,
            readback: {
              cwd: canonicalCheckoutPath,
              refName: readback.refName,
              isRepo: readback.isRepo,
            },
            worktree: worktreeResult,
            setup: setupResult,
          });
          yield* lifetime.recordPreparedOutcome({
            ref: execution,
            actualProducerOutcome: { kind: "prepared_completed", observation: physical },
            revalidateProducer,
            completeOriginalUse: runId === null,
          });
          yield* setupTracker.finish(threadId, "done");
        }).pipe(
          Effect.onError((cause) =>
            Effect.gen(function* () {
              const cancelled = Cause.hasInterruptsOnly(cause);
              yield* setupTracker.finish(
                threadId,
                cancelled ? "cancelled" : "failed",
                cancelled ? null : failureDetail(Cause.squash(cause)),
              );
              if (renameFiber !== null)
                yield* !cancelled && renameEntered
                  ? Fiber.await(renameFiber)
                  : Fiber.interrupt(renameFiber);
              const settleQualifiedFailure = Effect.gen(function* () {
                yield* revalidateExecution;
                const managed = yield* terminals.captureOwnedTargets({
                  threadId,
                  ownerBirth: admission.capture.applicationBirth,
                });
                if (
                  managed.status !== "captured" ||
                  managed.threadId !== threadId ||
                  managed.targets.length > 0
                )
                  return false;
                yield* git.invalidateLocalStatus(canonicalCheckoutPath);
                const readback = yield* git.localStatus({ cwd: canonicalCheckoutPath });
                const unspecifiedRoot =
                  targetSource.worktreePath === null && effectiveBranch === null;
                if (
                  (!unspecifiedRoot &&
                    (!readback.isRepo || readback.refName !== effectiveBranch)) ||
                  (!readback.isRepo && readback.refName !== null)
                )
                  return false;
                const physical = issueOrdinaryPreparedPhysicalResult({
                  version: 1,
                  kind: "prepared_failure_observed",
                  producerId,
                  execution,
                  targetSource,
                  checkoutPath: canonicalCheckoutPath,
                  observedAt: DateTime.formatIso(yield* DateTime.now),
                  branch: effectiveBranch,
                  readback: {
                    cwd: canonicalCheckoutPath,
                    refName: readback.refName,
                    isRepo: readback.isRepo,
                  },
                  worktree: worktreeResult,
                  failure: failureDetail(Cause.squash(cause)),
                  setup: {
                    status: "no_managed_process",
                    managerId: managed.managerId,
                    ownerBirth: admission.capture.applicationBirth,
                    targetCount: 0,
                  },
                });
                yield* lifetime.recordPreparedOutcome({
                  ref: execution,
                  actualProducerOutcome: { kind: "prepared_failed", observation: physical },
                  revalidateProducer,
                  completeOriginalUse: true,
                });
                return true;
              });
              const settled =
                cancelled ||
                ownershipLost ||
                physicalUnknown ||
                releaseEntered ||
                setupTerminalId !== null ||
                (tracked ? worktreeResult === null : createdWorktreePath !== null)
                  ? false
                  : yield* settleQualifiedFailure.pipe(
                      Effect.catchCause((settleCause) =>
                        Effect.logWarning(
                          "The failed preparation retains its unqualified physical end",
                          { operationId: originalUse.operationId, cause: settleCause },
                        ).pipe(Effect.as(false)),
                      ),
                    );
              if (!settled) {
                physicalUnknown = true;
                yield* lifetime
                  .retainExecutionUnknown(
                    execution,
                    `Original preparation outcome unavailable: ${String(Cause.squash(cause))}`,
                  )
                  .pipe(Effect.ignore);
              }
            }),
          ),
        );
        yield* Effect.raceFirst(preparation, Fiber.join(renewal));
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          producerActive = false;
        }),
      ),
    );
  });

  const failPreparedRun = (
    input: Pick<PreparationInput, "commandId">,
    threadId: ThreadId,
    runId: RunId | null,
    cause: unknown,
  ) =>
    runId === null
      ? Effect.logWarning("Thread workspace preparation failed", {
          commandId: input.commandId,
          threadId,
          cause,
        })
      : threads
          .dispatch({
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
          })
          .pipe(
            Effect.catchCause((persistCause) =>
              Effect.logWarning("Failed to persist thread workspace preparation failure", {
                commandId: input.commandId,
                threadId,
                cause,
                persistCause,
              }),
            ),
          );

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
    yield* prepareInBackground(input, threadId, runId).pipe(
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

  const launch: ThreadLaunchService["Service"]["launch"] = Effect.fn("ThreadLaunchService.launch")(
    function* (input) {
      yield* ProjectCloneTracker.rejectCommandsDuringClone(cloneTracker, {
        type: "thread.create",
        projectId: input.projectId,
      }).pipe(Effect.mapError(mapError(input, "resolve-project")));
      const project = yield* projects.getById(input.projectId).pipe(
        Effect.mapError(mapError(input, "resolve-project")),
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.fail(mapError(input, "resolve-project")("Project not found.")),
            onSome: Effect.succeed,
          }),
        ),
      );
      if (input.reuseExistingThread === true && input.threadId === undefined) {
        return yield* mapError(
          input,
          "update-thread",
        )("Reusing an existing thread requires a thread id.");
      }

      const launchReceipt = yield* readReceipt(input, input.commandId);
      return yield* Effect.gen(function* () {
        // A retried launch has no client-supplied id to replay against, so
        // recover the thread id its accepted create was recorded under before
        // allocating another one; a fresh id would only collide with the
        // recorded receipt.
        const reusableLaunchReceipt =
          input.threadId === undefined &&
          Option.isSome(launchReceipt) &&
          launchReceipt.value.status === "accepted" &&
          launchReceipt.value.commandType === "thread.create"
            ? launchReceipt.value
            : undefined;
        const candidateThreadId =
          input.threadId ??
          reusableLaunchReceipt?.threadId ??
          (yield* ids.allocate
            .thread({ projectId: input.projectId })
            .pipe(Effect.mapError(mapError(input, "create-thread"))));

        if (reusableLaunchReceipt !== undefined) {
          const shell = yield* threads
            .getThreadShell(candidateThreadId)
            .pipe(Effect.mapError(mapError(input, "create-thread", candidateThreadId)));
          if (shell === null) {
            return yield* mapError(input, "create-thread", candidateThreadId)("Thread not found.");
          }
          if (shell.projectId !== input.projectId) {
            return yield* mapError(
              input,
              "resolve-project",
              candidateThreadId,
            )("Project identity changed.");
          }
        }

        if (input.reuseExistingThread === true && Option.isNone(launchReceipt)) {
          yield* validateReusableThread(input, candidateThreadId);
        }

        // A Scratch thread launched at the project root runs in a folder of its
        // own. Only the first attempt claims one; a retry replays its create.
        const workspaceStrategy: ThreadLaunchWorkspaceStrategy =
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
        let target: { readonly branch: string | null; readonly worktreePath: string | null };
        if (Option.isSome(launchReceipt) && launchReceipt.value.status === "accepted") {
          if (Option.isNone(eventSink))
            return yield* mapError(
              input,
              "read-receipt",
              candidateThreadId,
            )("The original launch target is unavailable.");
          const originalEvents = yield* eventSink.value
            .readByCommandId({ commandId: input.commandId })
            .pipe(
              Stream.runCollect,
              Effect.mapError(mapError(input, "read-receipt", candidateThreadId)),
            );
          const original = originalEvents.find(
            (stored) =>
              stored.event.type === "thread.created" ||
              stored.event.type === "thread.metadata-updated",
          );
          if (
            original === undefined ||
            (original.event.type !== "thread.created" &&
              original.event.type !== "thread.metadata-updated") ||
            original.event.threadId !== candidateThreadId ||
            original.event.payload.projectId !== input.projectId
          )
            return yield* mapError(
              input,
              "create-thread",
              candidateThreadId,
            )("The accepted launch cannot be replayed under this thread or project.");
          target = {
            branch: original.event.payload.branch,
            worktreePath: original.event.payload.worktreePath,
          };
          if (
            workspaceStrategy.type === "worktree" &&
            (target.branch === null || target.worktreePath === null)
          )
            return yield* mapError(
              input,
              "provision-worktree",
              candidateThreadId,
            )(
              "The accepted launch has no preplanned worktree target; reconcile its original preparation.",
            );
        } else if (workspaceStrategy.type === "worktree") {
          if (Option.isNone(serverConfig) || Option.isNone(fileSystem))
            return yield* mapError(
              input,
              "provision-worktree",
              candidateThreadId,
            )("The configured physical worktree directory is unavailable.");
          const worktreesDir = yield* fileSystem.value
            .realPath(serverConfig.value.worktreesDir)
            .pipe(Effect.mapError(mapError(input, "provision-worktree", candidateThreadId)));
          const uuid = workspaceStrategy.branch === undefined ? yield* randomUuidV4 : "";
          const branch =
            workspaceStrategy.branch ??
            buildTemporaryWorktreeBranchName(() => uuid.replaceAll("-", ""));
          target = {
            branch,
            worktreePath: nativeWorktreePath({ worktreesDir, cwd: project.workspaceRoot, branch }),
          };
        } else
          target = {
            branch: workspaceStrategy.branch ?? null,
            worktreePath:
              workspaceStrategy.type === "existing_worktree"
                ? workspaceStrategy.worktreePath
                : null,
          };
        const initialBranch = target.branch;
        const initialWorktreePath = target.worktreePath;
        const claimCommand: Extract<
          import("@t3tools/contracts").OrchestrationV2Command,
          { readonly type: "thread.create" | "thread.metadata.update" }
        > =
          input.reuseExistingThread === true
            ? {
                type: "thread.metadata.update",
                commandId: input.commandId,
                threadId: candidateThreadId,
                expectedEmpty: true,
                branch: initialBranch,
                worktreePath: initialWorktreePath,
              }
            : ({
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
                createdBy: input.createdBy,
                creationSource: input.creationSource,
              } as const);
        const claimed = yield* threads
          .dispatch(claimCommand)
          .pipe(
            Effect.mapError(
              mapError(
                input,
                input.reuseExistingThread === true ? "update-thread" : "create-thread",
                candidateThreadId,
              ),
            ),
          );
        const threadId =
          claimed.storedEvents.find((stored) => stored.event.type.startsWith("thread."))?.event
            .threadId ?? candidateThreadId;
        if (project.id !== input.projectId) {
          return yield* mapError(input, "resolve-project", threadId)("Project identity changed.");
        }

        const preparationEvent = claimed.storedEvents.find(
          (stored) =>
            stored.event.type === "thread.created" ||
            stored.event.type === "thread.metadata-updated",
        );
        if (preparationEvent === undefined)
          return yield* mapError(
            input,
            "create-thread",
            threadId,
          )("Launch acceptance has no original preparation event.");
        const acceptedPreparation = {
          command: claimCommand,
          event: {
            eventId: preparationEvent.event.id,
            sequence: preparationEvent.sequence,
            threadId,
            commandId: preparationEvent.commandId,
            eventType: preparationEvent.event.type,
          },
        };

        let runId: RunId | null = null;
        let messageWasAlreadyAccepted = false;
        if (input.initialMessage !== undefined) {
          const messageCommandId = CommandId.make(`${input.commandId}:initial-message`);
          const messageReceipt = yield* readReceipt(input, messageCommandId);
          messageWasAlreadyAccepted = Option.isSome(messageReceipt);
          const messageId =
            input.initialMessage.messageId ??
            (yield* ids.allocate
              .message({ threadId, ordinal: 1 })
              .pipe(Effect.mapError(mapError(input, "dispatch-message", threadId))));
          const dispatched = yield* threads
            .dispatch({
              type: "message.dispatch",
              commandId: messageCommandId,
              threadId,
              messageId,
              text: input.initialMessage.text,
              ...(input.initialMessage.scheduledTaskId === undefined
                ? {}
                : { scheduledTaskId: input.initialMessage.scheduledTaskId }),
              ...(input.initialMessage.senderThreadId === undefined
                ? {}
                : { senderThreadId: input.initialMessage.senderThreadId }),
              attachments: input.initialMessage.attachments,
              ...(input.initialMessage.context ? { context: input.initialMessage.context } : {}),
              ...(input.generateTitle === true ? { titleSeed: input.title } : {}),
              modelSelection: input.modelSelection,
              dispatchMode: { type: "defer_start", workspaceStrategy },
              createdBy: input.createdBy,
              creationSource: input.creationSource,
            })
            .pipe(Effect.mapError(mapError(input, "dispatch-message", threadId)));
          const runCreated = dispatched.storedEvents.find(
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
        const runIsPreparing =
          runId !== null &&
          projection.runs.some((run) => run.id === runId && run.status === "preparing");
        const shouldSchedule = runId === null ? Option.isNone(launchReceipt) : runIsPreparing;
        // A retried root launch prepares the folder its first attempt bound, so
        // a Scratch thread keeps its own. Other root launches bind no folder.
        const boundWorktreePath = projection.thread.worktreePath;
        const preparationStrategy: ThreadLaunchWorkspaceStrategy =
          Option.isSome(launchReceipt) &&
          workspaceStrategy.type === "root" &&
          boundWorktreePath !== null
            ? {
                type: "existing_worktree",
                worktreePath: boundWorktreePath,
                branch: workspaceStrategy.branch,
              }
            : workspaceStrategy;
        if (shouldSchedule) {
          const ownsPreparation = yield* reservePreparation(input.commandId);
          if (ownsPreparation) {
            yield* Effect.gen(function* () {
              const preparationStillRequired =
                runId === null
                  ? true
                  : yield* threads.getThreadRecords(threadId, ["runs"], { runIds: [runId] }).pipe(
                      Effect.map((current) =>
                        current.runs.some((run) => run.id === runId && run.status === "preparing"),
                      ),
                      Effect.mapError(mapError(input, "update-thread", threadId)),
                    );
              if (preparationStillRequired) {
                yield* schedulePreparation(
                  {
                    ...input,
                    workspaceStrategy: preparationStrategy,
                    preparedTarget: target,
                    acceptedPreparation,
                  },
                  threadId,
                  runId,
                );
              } else {
                yield* releasePreparation(input.commandId);
              }
            }).pipe(Effect.onError(() => releasePreparation(input.commandId)));
          }
        }

        return {
          threadId,
          projection,
          resumed: Option.isSome(launchReceipt) || messageWasAlreadyAccepted,
        };
      });
    },
  );

  const retryPreparation: ThreadLaunchService["Service"]["retryPreparation"] = Effect.fn(
    "ThreadLaunchService.retryPreparation",
  )(function* (input) {
    const dispatched = yield* threads.dispatch({
      type: "prepared-run.retry",
      commandId: input.commandId,
      threadId: input.threadId,
      runId: input.runId,
    });
    // A replayed retry finds the run already past preparation, or prepared by
    // the attempt that first reserved this command.
    // From here the run is preparing again; anything that stops preparation
    // from being scheduled must fail it, or it would wait in preparing forever.
    const scheduled = yield* Effect.gen(function* () {
      const projection = yield* threads.getThreadProjection(input.threadId);
      const run = projection.runs.find((candidate) => candidate.id === input.runId);
      const workspacePreparation = run?.workspacePreparation;
      if (run?.status !== "preparing" || workspacePreparation === undefined) return;
      if (!(yield* reservePreparation(input.commandId))) return;
      yield* scheduleRetriedPreparation(input, projection, run, workspacePreparation).pipe(
        Effect.onError(() => releasePreparation(input.commandId)),
      );
    }).pipe(Effect.exit);
    if (Exit.isFailure(scheduled)) {
      yield* failPreparedRun(input, input.threadId, input.runId, Cause.squash(scheduled.cause));
    }
    return dispatched;
  });

  const scheduleRetriedPreparation = (
    input: ThreadLaunchRetryInput,
    projection: OrchestrationV2ThreadProjection,
    run: OrchestrationV2ThreadProjection["runs"][number],
    workspacePreparation: ThreadLaunchWorkspaceStrategy,
  ) => {
    const message = projection.messages.find((candidate) => candidate.id === run.userMessageId);
    // A worktree the failed attempt already created is reused, not created again.
    const reuse =
      workspacePreparation.type === "worktree" &&
      projection.thread.worktreePath !== null &&
      projection.thread.branch !== null
        ? {
            strategy: {
              type: "existing_worktree" as const,
              worktreePath: projection.thread.worktreePath,
              branch: projection.thread.branch,
            },
            reusedWorktree: { baseRef: workspacePreparation.baseRef },
          }
        : null;
    return schedulePreparation(
      {
        commandId: input.commandId,
        projectId: projection.thread.projectId,
        workspaceStrategy: reuse?.strategy ?? workspacePreparation,
        ...(reuse === null ? {} : { reusedWorktree: reuse.reusedWorktree }),
        ...(message === undefined
          ? {}
          : {
              initialMessage: {
                text: message.text,
                attachments: message.attachments,
                ...(message.context ? { context: message.context } : {}),
              },
            }),
      },
      input.threadId,
      run.id,
    );
  };

  const prepareDelegated = Effect.fn("ThreadLaunchService.prepareDelegated")(function* (
    effect: EffectOutbox.OrchestrationEffectV2,
  ) {
    const projection = yield* threads.getThreadProjection(effect.threadId);
    const projectId = projection.thread.projectId;
    const fail = (cause: unknown) =>
      new ThreadLaunchError({
        operation: "provision-worktree",
        commandId: effect.commandId,
        projectId,
        threadId: effect.threadId,
        cause,
      });
    return yield* Effect.gen(function* () {
      if (
        effect.request.type !== "delegated-workspace.prepare" ||
        Option.isNone(eventSink) ||
        eventSink.value.ordinaryCheckoutLifetime === undefined ||
        Option.isNone(effectOutbox) ||
        Option.isNone(fileSystem) ||
        threads.dispatchOrdinaryPreparedRunRelease === undefined
      )
        return yield* fail(
          "The delegated checkout producer requires its owning admission, release and filesystem capabilities.",
        );
      const lifetime = eventSink.value.ordinaryCheckoutLifetime;
      const outbox = effectOutbox.value;
      const fs = fileSystem.value;
      const request = effect.request;
      const plan = yield* Schema.decodeUnknownEffect(DelegatedCheckoutPlanV1)(request.plan, {
        onExcessProperty: "error",
      });
      const linked = yield* lifetime.readEffectLink(effect);
      const admission = linked?.admission;
      const project = Option.getOrNull(yield* projects.getById(projectId));
      const run = projection.runs.find((item) => item.id === request.runId);
      if (
        admission?.run === null ||
        admission?.run === undefined ||
        project === null ||
        admission.capture.origin.kind !== "delegated_child" ||
        admission.capture.origin.parentThreadId !== plan.parentThreadId ||
        effect.threadId !== plan.childThreadId ||
        run?.status !== "preparing" ||
        run.id !== admission.run.runId ||
        project.workspaceRoot !== plan.projectWorkspaceRoot ||
        admission.capture.canonicalProjectRoot !== plan.canonicalProjectRoot ||
        admission.capture.canonicalCheckoutPath !== plan.worktreePath ||
        admission.capture.branch !== plan.branch ||
        projection.thread.worktreePath !== plan.worktreePath ||
        projection.thread.branch !== plan.branch ||
        effect.status !== "running" ||
        effect.leaseOwner === null ||
        effect.leaseExpiresAt === null
      )
        return yield* fail(
          "Delegated preparation lost its exact committed child, pinned local source, run or original claim.",
        );
      const originalClaim = { workerId: effect.leaseOwner, expectedAttempt: effect.attemptCount };
      const targetSource = {
        projectWorkspaceRoot: plan.projectWorkspaceRoot,
        worktreePath: plan.worktreePath,
      };
      const source = {
        kind: "prepared_run" as const,
        admission: OrdinaryCheckout.ordinaryCheckoutAdmissionRefV1(admission),
        preparation: admission.run,
      };
      const producerId = yield* randomUuidV4;
      let created = false;
      let creationEntered = false;
      let producerActive = true;
      let releaseEntered = false;
      const revalidateProducer = Effect.gen(function* () {
        const currentEffect = Option.getOrNull(yield* outbox.get(effect.id));
        const currentProjection = yield* threads.getThreadProjection(effect.threadId);
        const currentRun = currentProjection.runs.find(
          (item) => item.id === source.preparation.runId,
        );
        if (
          !producerActive ||
          currentEffect?.status !== "running" ||
          currentEffect.leaseOwner !== originalClaim.workerId ||
          currentEffect.attemptCount !== originalClaim.expectedAttempt ||
          currentEffect.leaseExpiresAt === null ||
          currentEffect.leaseExpiresAt <= DateTime.formatIso(yield* DateTime.now) ||
          currentEffect.request.type !== "delegated-workspace.prepare" ||
          canonicalJson(
            yield* Schema.encodeEffect(DelegatedCheckoutPlanV1)(currentEffect.request.plan),
          ) !== canonicalJson(yield* Schema.encodeEffect(DelegatedCheckoutPlanV1)(plan)) ||
          currentProjection.thread.projectId !== projectId ||
          currentProjection.thread.worktreePath !== plan.worktreePath ||
          currentProjection.thread.branch !== plan.branch ||
          currentProjection.thread.deletedAt !== null ||
          currentRun?.activeAttemptId !== source.preparation.runAttemptId ||
          currentRun.rootNodeId !== source.preparation.nodeId ||
          currentRun.userMessageId !== source.preparation.messageId
        )
          return yield* fail(
            "The original delegated producer, application birth target or claim changed.",
          );
        if ((yield* fs.realPath(project.workspaceRoot)) !== plan.canonicalProjectRoot)
          return yield* fail("The admitted project root changed its physical identity.");
        if (created || (creationEntered && (yield* fs.exists(plan.worktreePath)))) {
          if ((yield* fs.realPath(plan.worktreePath)) !== plan.worktreePath)
            return yield* fail("The child checkout changed its physical path.");
          const status = yield* git.localStatus({ cwd: plan.worktreePath });
          if (!status.isRepo || status.refName !== plan.branch)
            return yield* fail("The child checkout lost its original local branch.");
        } else if (yield* fs.exists(plan.worktreePath))
          return yield* fail("A new delegated child cannot adopt an existing physical checkout.");
      });
      const reserved = yield* lifetime.beginUse({
        operationId: `delegated-prepare:${effect.id}:${effect.attemptCount}`,
        admission: source.admission,
        source,
        targetSource,
      });
      if (reserved.status !== "reserved")
        return yield* fail(
          "An interrupted original preparation is observe-only; it cannot be restarted or replaced.",
        );
      const originalUse = reserved.record.subject.use;
      const execution = yield* lifetime.bindExecution({
        originalUse,
        executor: { kind: "actual_prepared_producer", producerId, source },
        targetSource,
        revalidateProducer,
      });
      const revalidateExecution = lifetime.revalidateExecution(execution);
      const renew = Effect.gen(function* () {
        yield* revalidateProducer;
        const current = Option.getOrNull(yield* outbox.get(effect.id));
        if (current?.leaseExpiresAt === null || current?.leaseExpiresAt === undefined)
          return yield* fail("The original delegated claim deadline is unavailable.");
        const now = yield* DateTime.now;
        const newExpiry = DateTime.add(now, { minutes: 5 });
        if (
          !(yield* outbox.renewClaim({
            effectId: effect.id,
            ...originalClaim,
            expectedLeaseExpiresAt: current.leaseExpiresAt,
            leaseExpiresAt: DateTime.formatIso(newExpiry),
          }))
        )
          return yield* fail("The original preparation claim changed before renewal.");
        yield* lifetime.renewExecution({ ref: execution, now, newExpiry });
      });
      yield* revalidateExecution;
      yield* renew;
      return yield* Effect.scoped(
        Effect.gen(function* () {
          const renewal = yield* Effect.forever(
            Effect.sleep("10 seconds").pipe(Effect.andThen(renew)),
          ).pipe(Effect.forkScoped);
          const prepare = Effect.gen(function* () {
            yield* revalidateExecution;
            const resolvedBase = yield* git.resolveCommit({
              cwd: plan.canonicalProjectRoot,
              revision: plan.parentCommit,
            });
            if (resolvedBase.commitSha !== plan.parentCommit)
              return yield* fail(
                "The accepted parent commit is not the exact locally available base.",
              );
            yield* revalidateExecution;
            const worktree = yield* git.createWorktree(
              {
                cwd: plan.canonicalProjectRoot,
                refName: plan.parentCommit,
                newRefName: plan.branch,
                baseRefName: plan.parentCommit,
                path: plan.worktreePath,
              },
              {
                revalidateMutation: revalidateExecution.pipe(
                  Effect.asVoid,
                  Effect.tap(() =>
                    Effect.sync(() => {
                      creationEntered = true;
                    }),
                  ),
                  Effect.mapError((cause) => fail(cause)),
                ),
              },
            );
            created = true;
            yield* revalidateExecution;
            const head = yield* git.resolveCommit({ cwd: plan.worktreePath, revision: "HEAD" });
            if (
              worktree.worktree.path !== plan.worktreePath ||
              worktree.worktree.refName !== plan.branch ||
              head.commitSha !== plan.parentCommit
            )
              return yield* fail(
                "The actual isolated child checkout differs from its accepted committed base.",
              );
            const setup = yield* setupScripts.runForThread({
              threadId: effect.threadId,
              projectId,
              projectCwd: project.workspaceRoot,
              worktreePath: plan.worktreePath,
              project: {
                id: project.id,
                workspaceRoot: project.workspaceRoot,
                scripts: project.scripts,
              },
              observeCompletion: {},
            });
            let setupResult: Extract<
              OrdinaryPreparedPhysicalResultV1,
              { kind: "prepared_setup_completed" }
            >["setup"] = { status: "no-script" };
            const finishSetup = Effect.gen(function* () {
              if (setup.status !== "started") return;
              if (setup.completion === undefined)
                return yield* fail("A started setup has no retained completion observation.");
              const completion = yield* setup.completion;
              if (
                completion.exitCode === null ||
                !Number.isInteger(completion.exitCode) ||
                !Number.isFinite(completion.durationMs) ||
                completion.durationMs < 0
              )
                return yield* fail(
                  "Setup completion is unknown; the original physical use must remain held.",
                );
              setupResult = {
                status: "completed",
                scriptId: setup.scriptId,
                terminalId: setup.terminalId,
                cwd: setup.cwd,
                exitCode: completion.exitCode,
                durationMs: completion.durationMs,
              };
              if (completion.exitCode !== 0 && !setup.async)
                return yield* fail(`Setup script exited with ${completion.exitCode}.`);
              yield* revalidateExecution;
            });
            if (setup.status === "started" && !setup.async) yield* finishSetup;
            yield* revalidateExecution;
            releaseEntered = true;
            yield* threads.dispatchOrdinaryPreparedRunRelease!(
              {
                type: "prepared-run.release",
                commandId: CommandId.make(`${effect.commandId}:release`),
                threadId: effect.threadId,
                runId: source.preparation.runId,
              },
              originalUse,
              execution,
            );
            if (setup.status === "started" && setup.async) yield* finishSetup;
            yield* revalidateExecution;
            const readback = yield* git.localStatus({ cwd: plan.worktreePath });
            if (!readback.isRepo || readback.refName !== plan.branch)
              return yield* fail(
                "Preparation lost its original post-native checkout and ref readback.",
              );
            const physical = issueOrdinaryPreparedPhysicalResult({
              version: 1,
              kind: "prepared_setup_completed",
              producerId,
              execution,
              targetSource,
              checkoutPath: plan.worktreePath,
              observedAt: DateTime.formatIso(yield* DateTime.now),
              branch: plan.branch,
              readback: {
                cwd: plan.worktreePath,
                refName: readback.refName,
                isRepo: readback.isRepo,
              },
              worktree: { path: plan.worktreePath, refName: plan.branch, headSha: head.commitSha },
              setup: setupResult,
            });
            yield* lifetime.recordPreparedOutcome({
              ref: execution,
              actualProducerOutcome: { kind: "prepared_completed", observation: physical },
              revalidateProducer: revalidateProducer.pipe(
                Effect.andThen(
                  Effect.gen(function* () {
                    if (readIssuedOrdinaryPreparedPhysicalResult(physical) !== physical)
                      return yield* fail(
                        "The original preparation observation changed after issuance.",
                      );
                  }),
                ),
              ),
            });
            producerActive = false;
          });
          yield* Effect.raceFirst(prepare, Fiber.join(renewal));
        }),
      ).pipe(
        Effect.onError((cause) =>
          lifetime
            .retainExecutionUnknown(
              execution,
              `Original delegated preparation ${releaseEntered ? "after release" : "before release"}: ${String(Cause.squash(cause))}`,
            )
            .pipe(
              Effect.catchCause((holdCause) =>
                Effect.logError("Could not persist original delegated uncertainty", {
                  effectId: effect.id,
                  cause: holdCause,
                }),
              ),
            ),
        ),
      );
    }).pipe(Effect.mapError(fail));
  });

  return ThreadLaunchService.of({ launch, retryPreparation, prepareDelegated });
});

export const layer = Layer.effect(ThreadLaunchService, make);
