import * as WorktreeSetupTracker from "../project/WorktreeSetupTracker.ts";
import * as ProjectCloneTracker from "../project/ProjectCloneTracker.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import {
  CommandId,
  EnvironmentAuthenticatedPrincipal,
  type ChatAttachment,
  MessageId,
  type ModelSelection,
  type NativeBootstrapSubmission,
  NativeBootstrapDispatchResultV2,
  type OrchestrationV2Actor,
  type OrchestrationV2CreationSource,
  type OrchestrationV2ProviderThreadNativeMetadata,
  type OrchestrationV2ThreadProjection,
  type ProviderDriverKind,
  type ProviderInteractionMode,
  ProjectId,
  type RunId,
  type RuntimeMode,
  type ScheduledTaskId,
  ThreadId,
  OrchestrationV2Command,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as FileSystem from "effect/FileSystem";
import { buildTemporaryWorktreeBranchName, isTemporaryWorktreeBranch } from "@t3tools/shared/git";

import * as GitWorkflow from "../git/GitWorkflowService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "../project/ProjectSetupScriptRunner.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as ProviderRegistry from "../provider/Services/ProviderRegistry.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as TextGeneration from "../textGeneration/TextGeneration.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as EventSink from "./EventSink.ts";
import * as OrdinaryCheckout from "./OrdinaryCheckoutOwnership.ts";
import { makeProviderFailure } from "./ProviderFailure.ts";
import { randomUuidV4 } from "./RandomUuid.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";
import * as ServerConfig from "../config.ts";
import { nativeWorktreePath } from "../vcs/worktreePath.ts";
import {
  NativeCreationRepository,
  type NativeCreationRepositoryError,
} from "../persistence/Services/NativeCreationRepository.ts";
import {
  NativeCreationAuthority,
  NativeCreationAuthorityError,
  type NativeCreationStage,
} from "./NativeCreationAuthority.ts";
import {
  decodeNativeBootstrapSubmission,
  nativeCreationV2CommandDigest,
  type NativeCreationPreparationError,
} from "./NativeCreationPreparation.ts";
import { OrchestratorCommandIdConflictError, type OrchestratorV2Error } from "./Orchestrator.ts";
import * as Witness from "./NormalizationWitness.ts";

export type ThreadLaunchWorkspaceStrategy =
  | { readonly type: "root"; readonly branch?: string | undefined }
  | {
      readonly type: "existing_worktree";
      readonly worktreePath: string;
      readonly branch?: string | undefined;
    }
  | {
      readonly type: "worktree";
      readonly baseRef: string;
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

export class ThreadLaunchNormalization extends Context.Reference<
  | {
      readonly launchCommandId: CommandId;
      readonly preparation: Witness.NormalizationWitnessPreparation;
      readonly acceptedCommand?: Extract<
        OrchestrationV2Command,
        { readonly type: "message.dispatch" }
      >;
    }
  | undefined
>("t3/orchestration-v2/ThreadLaunchNormalization", { defaultValue: () => undefined }) {}

export interface ThreadLaunchResult {
  readonly threadId: ThreadId;
  readonly projection: OrchestrationV2ThreadProjection;
  readonly resumed: boolean;
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
    readonly launch: (
      input: ThreadLaunchInput,
    ) => Effect.Effect<ThreadLaunchResult, ThreadLaunchError>;
    readonly dispatchNativeBootstrap: (
      submission: NativeBootstrapSubmission,
      server: { readonly nativeCreationBootId: string },
    ) => Effect.Effect<
      NativeBootstrapDispatchResultV2,
      | ThreadLaunchError
      | NativeCreationPreparationError
      | NativeCreationAuthorityError
      | NativeCreationRepositoryError
      | OrchestratorV2Error,
      | EnvironmentAuthenticatedPrincipal
      | NativeCreationAuthority
      | NativeCreationRepository
      | ServerConfig.ServerConfig
      | FileSystem.FileSystem
    >;
  }
>()("t3/orchestration-v2/ThreadLaunchService") {}

// A producer result describes actual returned work and readback. The retained
// producer closure must qualify it; constructing this shape supplies no proof.
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

interface PreparedWorkspaceTarget {
  readonly branch: string | null;
  readonly worktreePath: string | null;
}

interface AcceptedLaunchPreparation {
  readonly command: Extract<
    OrchestrationV2Command,
    { readonly type: "thread.create" | "thread.metadata.update" }
  >;
  readonly event: OrdinaryCheckout.OrdinaryAcceptedEventV1;
}

const isThreadLaunchError = Schema.is(ThreadLaunchError);

function failureDetail(error: unknown): string {
  const details: string[] = [];
  const seen = new Set<unknown>();
  let cause = isThreadLaunchError(error) ? error.cause : error;
  while (cause !== undefined && cause !== null && !seen.has(cause) && seen.size < 8) {
    seen.add(cause);
    const detail = cause instanceof Error ? cause.message : String(cause);
    if (detail.length > 0 && !details.includes(detail)) details.push(detail);
    cause = typeof cause === "object" && "cause" in cause ? cause.cause : undefined;
  }
  const operation = isThreadLaunchError(error)
    ? ` during ${error.operation.replaceAll("-", " ")}`
    : "";
  return `Workspace preparation failed${operation}: ${details.join(": ")}`;
}

const make = Effect.gen(function* () {
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
  const serverConfig = yield* Effect.serviceOption(ServerConfig.ServerConfig);
  const eventSink = yield* Effect.serviceOption(EventSink.EventSinkV2);
  const preparationScope = yield* Scope.make("sequential");
  const scheduledLaunches = yield* Ref.make<ReadonlySet<CommandId>>(new Set());
  const ownershipHeldPreparations = new Set<CommandId>();
  yield* Effect.addFinalizer(() => Scope.close(preparationScope, Exit.void));

  const mapError =
    (input: ThreadLaunchInput, operation: ThreadLaunchError["operation"], threadId?: ThreadId) =>
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
    input: ThreadLaunchInput,
    threadId: ThreadId,
    runId: RunId | null,
    target: PreparedWorkspaceTarget,
    preparation: AcceptedLaunchPreparation,
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

    if (Option.isNone(eventSink)) {
      return yield* mapError(
        input,
        "provision-worktree",
        threadId,
      )("The original preparation cannot be associated with its execution.");
    }
    const sink = eventSink.value;
    const admission =
      runId === null
        ? yield* threads
            .captureOrdinaryPreparedLaunch(preparation.command, preparation.event)
            .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)))
        : yield* threads
            .readOrdinaryCheckoutAdmissionForRun({ threadId, runId })
            .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
    if (
      admission === null ||
      admission.capture.threadId !== threadId ||
      admission.capture.projectId !== project.id ||
      admission.capture.branch !== target.branch ||
      (runId !== null && admission.run?.runId !== runId)
    ) {
      return yield* mapError(
        input,
        "provision-worktree",
        threadId,
      )("The original accepted preparation is unavailable or differs from its captured target.");
    }
    const admissionRef = OrdinaryCheckout.ordinaryCheckoutAdmissionRefV1(admission);
    const source: typeof OrdinaryCheckout.OrdinaryCheckoutPreparedExecutionSourceV1.Type =
      runId !== null && admission.run !== null
        ? { kind: "prepared_run", admission: admissionRef, preparation: admission.run }
        : {
            kind: "prepared_launch",
            admission: admissionRef,
            preparationCommandId: preparation.command.commandId,
            preparationEvent: preparation.event,
            applicationBirth: admission.capture.applicationBirth,
            projectId: admission.capture.projectId,
            canonicalProjectRoot: admission.capture.canonicalProjectRoot,
            canonicalCheckoutPath: admission.capture.canonicalCheckoutPath,
            branch: admission.capture.branch,
          };
    const reserved = yield* threads
      .beginOrdinaryPreparedCheckoutUse(admission, {
        operationId: `${input.commandId}:ordinary-preparation`,
        source,
      })
      .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
    if (reserved.status === "observe_only") return;
    const originalUse = reserved.record.subject.use;
    const targetSource = {
      projectWorkspaceRoot: project.workspaceRoot,
      worktreePath: target.worktreePath,
    };
    const producerId = `${originalUse.operationId}:producer`;
    const producerFiber = yield* Effect.fiber;
    const tracked = input.workspaceStrategy.type === "worktree";
    let createdWorktreePath: string | null = null;
    let worktreeCreationEntered = false;
    let worktreeBaseRejected = false;
    let setupTerminalId: string | null = null;
    let releaseEntered = false;
    let ownershipLost = false;
    let renameFiber: Fiber.Fiber<void, never> | null = null;
    let renameEntered = false;
    let effectiveBranch = target.branch;
    let renameUnknown = false;
    let worktreeResult: Extract<
      OrdinaryPreparedPhysicalResultV1,
      { kind: "prepared_setup_completed" }
    >["worktree"] = null;
    let setupResult:
      | Extract<OrdinaryPreparedPhysicalResultV1, { kind: "prepared_setup_completed" }>["setup"]
      | null = null;
    const revalidateCaptured = Effect.suspend(() =>
      ownershipLost || renameUnknown
        ? Effect.fail(
            new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
              reason: "unknown_use",
              threadId,
              path: admission.capture.canonicalCheckoutPath,
              message: "The captured preparation lost its original execution ownership.",
            }),
          )
        : Effect.void,
    );
    const execution = yield* sink
      .bindOrdinaryCheckoutExecution({
        originalUse,
        executor: { kind: "actual_prepared_producer", producerId, source },
        targetSource,
        revalidateProducer: revalidateCaptured,
      })
      .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
    yield* threads
      .revalidateOrdinaryCheckoutUse(admission, originalUse)
      .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
    yield* threads
      .registerOrdinaryCheckoutExecution(execution, {
        revalidateCaptured,
        onLoss: Effect.gen(function* () {
          ownershipLost = true;
          ownershipHeldPreparations.add(input.commandId);
          if (renameFiber !== null) yield* Fiber.interrupt(renameFiber);
          if (setupTerminalId !== null) {
            yield* terminals
              .close({ threadId, terminalId: setupTerminalId, deleteHistory: false })
              .pipe(Effect.ignore);
          }
          yield* Fiber.interrupt(producerFiber);
        }),
      })
      .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
    const revalidateExecution = threads
      .revalidateOrdinaryCheckoutExecution(execution)
      .pipe(
        Effect.andThen(revalidateCaptured),
        Effect.mapError(mapError(input, "provision-worktree", threadId)),
      );
    if (tracked) {
      yield* setupTracker.begin({
        threadId,
        branch: target.branch,
        baseRef: input.workspaceStrategy.baseRef,
        stages: ["fetch", "checkout", "setup-script", "agent"],
        fiber: yield* Effect.fiber,
      });
    }
    yield* Effect.gen(function* () {
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

      const branch = target.branch;
      const worktreePath = target.worktreePath;
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
        let startRef = input.workspaceStrategy.baseRef;
        // "Start from origin" is a stored default; repos without the requested
        // remote branch fall back to the local base branch.
        const startFromOrigin =
          input.workspaceStrategy.startFromOrigin === true &&
          (yield* git
            .remoteExists({ cwd: project.workspaceRoot, remoteName: "origin" })
            .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId))));
        yield* setupTracker.stageStatus(threadId, "fetch", startFromOrigin ? "running" : "skipped");
        if (startFromOrigin) {
          yield* revalidateExecution;
          yield* git
            .fetchRemote({
              cwd: project.workspaceRoot,
              remoteName: "origin",
              refName: input.workspaceStrategy.baseRef,
            })
            .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
          const remoteBaseExists = yield* git
            .remoteBranchExists({
              cwd: project.workspaceRoot,
              refName: input.workspaceStrategy.baseRef,
              remoteName: "origin",
            })
            .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
          if (remoteBaseExists) {
            startRef = yield* git
              .resolveRemoteTrackingCommit({
                cwd: project.workspaceRoot,
                refName: input.workspaceStrategy.baseRef,
                fallbackRemoteName: "origin",
              })
              .pipe(
                Effect.map((resolved) => resolved.commitSha),
                Effect.mapError(mapError(input, "provision-worktree", threadId)),
              );
          }
        }
        if (
          !(yield* git
            .isRepository(project.workspaceRoot)
            .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId))))
        ) {
          worktreeBaseRejected = true;
          return yield* mapError(
            input,
            "provision-worktree",
            threadId,
          )(`Project root ${project.workspaceRoot} is not a Git repository.`);
        }
        if (
          !(yield* git
            .hasCommit({ cwd: project.workspaceRoot, refName: startRef })
            .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId))))
        ) {
          worktreeBaseRejected = true;
          return yield* mapError(
            input,
            "provision-worktree",
            threadId,
          )(`Worktree base ${startRef} has no commit.`);
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
              baseRefName: input.workspaceStrategy.baseRef,
              path: target.worktreePath,
            },
            {
              revalidateMutation: revalidateExecution.pipe(
                Effect.tap(() =>
                  Effect.sync(() => {
                    worktreeCreationEntered = true;
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
        if (
          worktree.worktree.path !== target.worktreePath ||
          worktree.worktree.refName !== target.branch
        ) {
          return yield* mapError(
            input,
            "provision-worktree",
            threadId,
          )(
            "Git returned a worktree outside the accepted path or branch; reconcile its outcome before retrying.",
          );
        }
        worktreeResult = worktree.worktree;
        createdWorktreePath = worktreePath;
        yield* setupTracker.update(threadId, (snapshot) => ({ ...snapshot, worktreePath, branch }));
        yield* setupTracker.stageStatus(threadId, "checkout", "done");
      }

      yield* threads
        .dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make(`${input.commandId}:workspace`),
          threadId,
          branch,
          worktreePath,
        })
        .pipe(Effect.mapError(mapError(input, "update-thread", threadId)));

      // Generation stays concurrent with setup and release. The captured producer
      // retains the rename child until its actual Git result and readback settle.
      if (
        worktreePath !== null &&
        branch !== null &&
        initialMessage !== undefined &&
        isTemporaryWorktreeBranch(branch)
      ) {
        const oldBranch = branch;
        const worktreeCwd = worktreePath;
        renameFiber = yield* Effect.gen(function* () {
          const generated = yield* generateBranchNameFor(worktreeCwd, initialMessage);
          yield* revalidateExecution;
          renameEntered = true;
          const renamed = yield* git.renameBranch({
            cwd: worktreeCwd,
            oldBranch,
            newBranch: generated.branch,
            ...(generated.exactName ? { exactName: true } : {}),
          });
          yield* git.invalidateLocalStatus(worktreeCwd);
          const readback = yield* git.localStatus({ cwd: worktreeCwd });
          if (
            !readback.isRepo ||
            readback.refName === null ||
            readback.refName !== renamed.branch
          ) {
            return yield* mapError(
              input,
              "provision-worktree",
              threadId,
            )("The renamed branch could not be verified at its original checkout.");
          }
          const physical = issueOrdinaryPreparedPhysicalResult({
            version: 1,
            kind: "prepared_branch_renamed",
            producerId,
            execution,
            targetSource,
            checkoutPath: worktreeCwd,
            observedAt: DateTime.formatIso(yield* DateTime.now),
            oldBranch,
            requestedBranch: generated.branch,
            renamedBranch: renamed.branch,
            readback: { cwd: worktreeCwd, refName: readback.refName },
          });
          yield* threads.dispatchOrdinaryPreparedBranchRename(
            CommandId.make(`${input.commandId}:branch-rename`),
            physical,
          );
          effectiveBranch = renamed.branch;
        }).pipe(
          // Interruption skips catchCause; an entered rename keeps its uncertainty here.
          Effect.onError(() =>
            Effect.sync(() => {
              if (renameEntered) {
                renameUnknown = true;
                ownershipHeldPreparations.add(input.commandId);
              }
            }),
          ),
          Effect.catchCause((cause) =>
            Effect.logWarning("Thread worktree branch rename failed", {
              commandId: input.commandId,
              threadId,
              oldBranch,
              cause,
            }),
          ),
          Effect.forkIn(preparationScope),
        );
      }

      const cwd = worktreePath ?? project.workspaceRoot;
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
          observeCompletion: {
            onOutputLine: (line: string) => setupTracker.appendTail(threadId, "setup-script", line),
          },
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
              completion.exitCode !== null &&
              Number.isInteger(completion.exitCode) &&
              Number.isFinite(completion.durationMs) &&
              completion.durationMs >= 0
            ) {
              setupResult = {
                status: "completed",
                scriptId: setup.scriptId,
                terminalId: setup.terminalId,
                cwd: setup.cwd,
                exitCode: completion.exitCode,
                durationMs: completion.durationMs,
              };
            }
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
          ownershipHeldPreparations.add(input.commandId);
          return yield* mapError(
            input,
            "run-setup-script",
            threadId,
          )(
            "The started setup has no captured completion observation; reconcile its original execution.",
          );
        }
      } else {
        setupResult = { status: "no-script" };
        yield* setupTracker.stageStatus(threadId, "setup-script", "skipped");
      }
      yield* setupTracker.markUncancellable(threadId);
      yield* setupTracker.stageStatus(threadId, "agent", "running");
      if (runId !== null) {
        yield* revalidateExecution;
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
      if (setupResult === null || renameUnknown || ownershipLost) {
        ownershipHeldPreparations.add(input.commandId);
        yield* sink
          .recordOrdinaryCheckoutExecutorOutcome({
            ref: execution,
            actualProducerOutcome: {
              kind: "unknown",
              reason: "The captured setup or branch rename has no qualified physical end.",
              observedAt: yield* DateTime.now,
            },
          })
          .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
        return yield* setupTracker.finish(
          threadId,
          "failed",
          "Preparation physical outcome remains unknown.",
        );
      }
      {
        yield* revalidateExecution;
        yield* git.invalidateLocalStatus(cwd);
        const readback = yield* git
          .localStatus({ cwd })
          .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
        const unspecifiedRoot = targetSource.worktreePath === null && effectiveBranch === null;
        if (
          (!unspecifiedRoot && (!readback.isRepo || readback.refName !== effectiveBranch)) ||
          (!readback.isRepo && readback.refName !== null)
        )
          return yield* mapError(
            input,
            "provision-worktree",
            threadId,
          )("The completed preparation no longer matches its qualified checkout.");
        const physical = issueOrdinaryPreparedPhysicalResult({
          version: 1,
          kind: "prepared_setup_completed",
          producerId,
          execution,
          targetSource,
          checkoutPath: cwd,
          observedAt: DateTime.formatIso(yield* DateTime.now),
          branch: effectiveBranch,
          readback: { cwd, refName: readback.refName, isRepo: readback.isRepo },
          worktree: worktreeResult,
          setup: setupResult,
        });
        const actualProducerOutcome = {
          kind: "prepared_completed" as const,
          observation: physical,
        };
        yield* sink
          .withTransaction(
            Effect.gen(function* () {
              const retirement = yield* sink.recordOrdinaryCheckoutExecutorOutcome({
                ref: execution,
                actualProducerOutcome,
                revalidateProducer: revalidateCaptured.pipe(
                  Effect.andThen(
                    Effect.suspend(() =>
                      readIssuedOrdinaryPreparedPhysicalResult(physical) === physical
                        ? Effect.void
                        : Effect.fail(
                            new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
                              reason: "unknown_use",
                              threadId,
                              path: cwd,
                              message: "The captured completion result changed.",
                            }),
                          ),
                    ),
                  ),
                ),
              });
              if (runId === null)
                yield* sink.completeOrdinaryCheckoutUse({
                  originalUse,
                  expectedAssociationOrdinal: retirement.ordinal,
                  completionEvidence: { ref: execution, actualProducerOutcome },
                });
            }),
          )
          .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
      }
      yield* setupTracker.finish(threadId, "done");
    }).pipe(
      Effect.onErrorIf(
        () => true,
        (cause: Cause.Cause<ThreadLaunchError>) =>
          Effect.gen(function* () {
            const cancelled = Cause.hasInterruptsOnly(cause);
            // A failure retains an entered rename until its actual Git result and
            // readback settle; interrupting it would turn a known end into unknown.
            if (renameFiber !== null)
              yield* !cancelled && renameEntered
                ? Fiber.await(renameFiber)
                : Fiber.interrupt(renameFiber);
            yield* setupTracker.finish(
              threadId,
              cancelled ? "cancelled" : "failed",
              cancelled ? null : failureDetail(Cause.squash(cause)),
            );
            if (cancelled && !ownershipLost && !renameUnknown && tracked && createdWorktreePath) {
              yield* revalidateExecution;
              if (setupTerminalId)
                yield* terminals
                  .close({ threadId, terminalId: setupTerminalId, deleteHistory: true })
                  .pipe(Effect.ignore);
              yield* git
                .removeWorktree({
                  cwd: project.workspaceRoot,
                  path: createdWorktreePath,
                  force: true,
                })
                .pipe(Effect.ignore);
              yield* threads
                .dispatch({
                  type: "thread.metadata.update",
                  commandId: CommandId.make(`${input.commandId}:cancel-workspace`),
                  threadId,
                  worktreePath: null,
                  branch: null,
                })
                .pipe(Effect.ignore);
            }
            // Only a rejected base proves the planned target cannot be created; a
            // transient failure such as fetch keeps it visible for diagnosis and retry.
            if (
              tracked &&
              worktreeBaseRejected &&
              !worktreeCreationEntered &&
              createdWorktreePath === null &&
              !ownershipLost &&
              !renameUnknown
            ) {
              yield* threads
                .dispatch(
                  {
                    type: "thread.metadata.update",
                    commandId: CommandId.make(`${input.commandId}:uncreated-workspace`),
                    threadId,
                    worktreePath: null,
                    branch: null,
                  },
                  threads.revalidateOrdinaryCheckoutExecution(execution).pipe(Effect.asVoid),
                )
                .pipe(Effect.mapError(mapError(input, "update-thread", threadId)));
            }
            // A failure ends the use only when this producer reads back its whole
            // physical end: the verified checkout and ref, and no T3-managed terminal
            // for the original birth. Any gap, error or later release stays unknown.
            const settleQualifiedFailure = Effect.gen(function* () {
              const cwd = target.worktreePath ?? project.workspaceRoot;
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
              yield* git.invalidateLocalStatus(cwd);
              const readback = yield* git.localStatus({ cwd });
              const unspecifiedRoot =
                targetSource.worktreePath === null && effectiveBranch === null;
              if (
                (!unspecifiedRoot && (!readback.isRepo || readback.refName !== effectiveBranch)) ||
                (!readback.isRepo && readback.refName !== null)
              )
                return false;
              const physical = issueOrdinaryPreparedPhysicalResult({
                version: 1,
                kind: "prepared_failure_observed",
                producerId,
                execution,
                targetSource,
                checkoutPath: cwd,
                observedAt: DateTime.formatIso(yield* DateTime.now),
                branch: effectiveBranch,
                readback: { cwd, refName: readback.refName, isRepo: readback.isRepo },
                worktree: worktreeResult,
                failure: failureDetail(Cause.squash(cause)),
                setup: {
                  status: "no_managed_process",
                  managerId: managed.managerId,
                  ownerBirth: admission.capture.applicationBirth,
                  targetCount: 0,
                },
              });
              const actualProducerOutcome = {
                kind: "prepared_failed" as const,
                observation: physical,
              };
              yield* sink.withTransaction(
                Effect.gen(function* () {
                  const retirement = yield* sink.recordOrdinaryCheckoutExecutorOutcome({
                    ref: execution,
                    actualProducerOutcome,
                    revalidateProducer: revalidateCaptured.pipe(
                      Effect.andThen(
                        Effect.suspend(() =>
                          readIssuedOrdinaryPreparedPhysicalResult(physical) === physical
                            ? Effect.void
                            : Effect.fail(
                                new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
                                  reason: "unknown_use",
                                  threadId,
                                  path: cwd,
                                  message: "The captured failure result changed.",
                                }),
                              ),
                        ),
                      ),
                    ),
                  });
                  yield* sink.completeOrdinaryCheckoutUse({
                    originalUse,
                    expectedAssociationOrdinal: retirement.ordinal,
                    completionEvidence: { ref: execution, actualProducerOutcome },
                  });
                }),
              );
              return true;
            });
            const failureSettled =
              cancelled ||
              ownershipLost ||
              renameUnknown ||
              releaseEntered ||
              setupTerminalId !== null ||
              (tracked ? worktreeResult === null : createdWorktreePath !== null)
                ? false
                : yield* settleQualifiedFailure.pipe(
                    Effect.catchCause((settleCause) =>
                      Effect.logWarning(
                        "The failed preparation keeps its unqualified physical end",
                        {
                          operationId: originalUse.operationId,
                          cause: settleCause,
                        },
                      ).pipe(Effect.as(false)),
                    ),
                  );
            if (!failureSettled)
              yield* sink
                .recordOrdinaryCheckoutExecutorOutcome({
                  ref: execution,
                  actualProducerOutcome: {
                    kind: "unknown",
                    reason: failureDetail(Cause.squash(cause)),
                    observedAt: yield* DateTime.now,
                  },
                })
                .pipe(
                  Effect.catchCause(() =>
                    Effect.logWarning(
                      "The original preparation outcome could not be recorded",
                      originalUse.operationId,
                    ),
                  ),
                );
          }),
      ),
    );
  });

  const failPreparedRun = (
    input: ThreadLaunchInput,
    threadId: ThreadId,
    runId: RunId | null,
    cause: unknown,
  ) =>
    ownershipHeldPreparations.has(input.commandId)
      ? Effect.logWarning("Thread preparation awaits its original ownership outcome", {
          commandId: input.commandId,
          threadId,
          cause,
        })
      : runId === null
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
              Effect.mapError(mapError(input, "fail-run", threadId)),
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
    input: ThreadLaunchInput,
    threadId: ThreadId,
    runId: RunId | null,
    target: PreparedWorkspaceTarget,
    preparation: AcceptedLaunchPreparation,
  ) {
    yield* prepareInBackground(input, threadId, runId, target, preparation).pipe(
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
      const suppliedNormalization = yield* ThreadLaunchNormalization;
      const normalization =
        suppliedNormalization?.launchCommandId === input.commandId &&
        suppliedNormalization.preparation.commandId === `${input.commandId}:initial-message`
          ? suppliedNormalization
          : undefined;
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
        // Select the isolated target before create and deferred message acceptance.
        // Exact replay reads the original event, never a subsequently renamed branch.
        let target: PreparedWorkspaceTarget;
        if (Option.isSome(launchReceipt) && launchReceipt.value.status === "accepted") {
          if (Option.isNone(eventSink)) {
            return yield* mapError(
              input,
              "read-receipt",
              candidateThreadId,
            )("The original launch target cannot be read; reconcile this launch before retrying.");
          }
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
          ) {
            return yield* mapError(
              input,
              "create-thread",
              candidateThreadId,
            )("The accepted launch cannot be replayed under this thread or project.");
          }
          target = {
            branch: original.event.payload.branch,
            worktreePath: original.event.payload.worktreePath,
          };
          if (
            workspaceStrategy.type === "worktree" &&
            (target.branch === null || target.worktreePath === null)
          ) {
            return yield* mapError(
              input,
              "provision-worktree",
              candidateThreadId,
            )(
              "This accepted launch has no preplanned worktree target; reconcile it before resuming preparation.",
            );
          }
        } else if (workspaceStrategy.type === "worktree") {
          if (Option.isNone(serverConfig)) {
            return yield* mapError(
              input,
              "provision-worktree",
              candidateThreadId,
            )("The configured worktree directory is unavailable.");
          }
          const uuid = workspaceStrategy.branch === undefined ? yield* randomUuidV4 : "";
          const branch =
            workspaceStrategy.branch ??
            buildTemporaryWorktreeBranchName(() => uuid.replaceAll("-", ""));
          target = {
            branch,
            worktreePath: nativeWorktreePath({
              worktreesDir: serverConfig.value.worktreesDir,
              cwd: project.workspaceRoot,
              branch,
            }),
          };
        } else {
          target = {
            branch: workspaceStrategy.branch ?? null,
            worktreePath:
              workspaceStrategy.type === "existing_worktree"
                ? workspaceStrategy.worktreePath
                : null,
          };
        }
        const initialBranch = target.branch;
        const initialWorktreePath = target.worktreePath;
        const claimCommand: AcceptedLaunchPreparation["command"] =
          input.reuseExistingThread === true
            ? {
                type: "thread.metadata.update",
                commandId: input.commandId,
                threadId: candidateThreadId,
                expectedEmpty: true,
                branch: initialBranch,
                worktreePath: initialWorktreePath,
              }
            : {
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
              };
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

        const acceptedPreparationEvent = claimed.storedEvents.find(
          (stored) =>
            stored.commandId === input.commandId &&
            stored.event.threadId === threadId &&
            (stored.event.type === "thread.created" ||
              stored.event.type === "thread.metadata-updated"),
        );
        if (acceptedPreparationEvent === undefined) {
          return yield* mapError(
            input,
            "create-thread",
            threadId,
          )("Launch acceptance has no original preparation event.");
        }
        const acceptedPreparation: AcceptedLaunchPreparation = {
          command: claimCommand,
          event: {
            eventId: acceptedPreparationEvent.event.id,
            sequence: acceptedPreparationEvent.sequence,
            threadId,
            commandId: acceptedPreparationEvent.commandId,
            eventType: acceptedPreparationEvent.event.type,
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
            normalization?.acceptedCommand?.messageId ??
            (yield* ids.allocate
              .message({ threadId, ordinal: 1 })
              .pipe(Effect.mapError(mapError(input, "dispatch-message", threadId))));
          const messageCommand: Extract<
            OrchestrationV2Command,
            { readonly type: "message.dispatch" }
          > = {
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
            dispatchMode: { type: "defer_start" },
            createdBy: input.createdBy,
            creationSource: input.creationSource,
          };
          const acceptedCommand = normalization?.acceptedCommand ?? messageCommand;
          if (
            normalization !== undefined &&
            (normalization.preparation.commandId !== messageCommandId ||
              (normalization.preparation.mode === "replay") !==
                (normalization.acceptedCommand !== undefined) ||
              Witness.acceptedCommandDigest(acceptedCommand) !==
                Witness.acceptedCommandDigest(messageCommand))
          ) {
            return yield* mapError(
              input,
              "dispatch-message",
              threadId,
            )(
              new OrchestratorCommandIdConflictError({
                commandId: messageCommandId,
                commandType: "message.dispatch",
                receiptThreadId: acceptedCommand.threadId,
                commandThreadId: threadId,
              }),
            );
          }
          const dispatched = yield* threads.dispatch(acceptedCommand).pipe(
            Effect.provideService(
              Witness.NormalizationWitnessCarrier,
              normalization === undefined
                ? undefined
                : {
                    ...normalization.preparation,
                    acceptedCommand,
                  },
            ),
            Effect.mapError(mapError(input, "dispatch-message", threadId)),
          );
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
        const boundWorktreePath = target.worktreePath;
        const preparationStrategy: ThreadLaunchWorkspaceStrategy =
          Option.isSome(launchReceipt) &&
          workspaceStrategy.type === "root" &&
          boundWorktreePath !== null
            ? {
                type: "existing_worktree",
                worktreePath: boundWorktreePath,
                branch: target.branch ?? undefined,
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
                  { ...input, workspaceStrategy: preparationStrategy },
                  threadId,
                  runId,
                  target,
                  acceptedPreparation,
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

  const dispatchNativeBootstrap: ThreadLaunchService["Service"]["dispatchNativeBootstrap"] =
    Effect.fn("ThreadLaunchService.dispatchNativeBootstrap")(function* (submission, server) {
      const { preparation, guard } = yield* decodeNativeBootstrapSubmission(submission);
      const principal = yield* EnvironmentAuthenticatedPrincipal;
      const authority = yield* NativeCreationAuthority;
      const repository = yield* NativeCreationRepository;
      const config = yield* ServerConfig.ServerConfig;
      const fs = yield* FileSystem.FileSystem;
      const original = preparation.command;
      const commandId = yield* Schema.decodeUnknownEffect(CommandId)(original.commandId).pipe(
        Effect.mapError(
          () =>
            new NativeCreationAuthorityError({
              code: "invalid_preparation",
              message: "Native logical command ID is invalid.",
            }),
        ),
      );
      const threadId = yield* Schema.decodeUnknownEffect(ThreadId)(original.threadId).pipe(
        Effect.mapError(
          () =>
            new NativeCreationAuthorityError({
              code: "invalid_preparation",
              message: "Native thread ID is invalid.",
            }),
        ),
      );
      const messageId = yield* Schema.decodeUnknownEffect(MessageId)(
        original.message.messageId,
      ).pipe(
        Effect.mapError(
          () =>
            new NativeCreationAuthorityError({
              code: "invalid_preparation",
              message: "Native message ID is invalid.",
            }),
        ),
      );
      const held = (message: string) =>
        new NativeCreationAuthorityError({ code: "unresolved_claim", message });
      if (
        commandId !== original.commandId ||
        threadId !== original.threadId ||
        messageId !== original.message.messageId ||
        server.nativeCreationBootId.length === 0
      ) {
        return yield* new NativeCreationAuthorityError({
          code: "invalid_preparation",
          message: "Native IDs must remain exact and the server boot correlation must be present.",
        });
      }
      const branch = original.bootstrap.prepareWorktree.branch;
      const resources = Object.freeze({
        projectCwd: preparation.binding.project_cwd,
        branch,
        worktreePath: nativeWorktreePath({
          worktreesDir: config.worktreesDir,
          cwd: preparation.binding.project_cwd,
          branch,
        }),
      });
      const input = Object.freeze({
        actorSessionId: principal.sessionId,
        preparation,
        guard,
        resources,
      });
      const authorize = (stage: NativeCreationStage) => authority.authorize({ ...input, stage });
      const nowIso = DateTime.now.pipe(Effect.map(DateTime.formatIso));
      const claimed = yield* repository.claim(
        {
          preparation,
          resources,
          actorSessionId: principal.sessionId,
          claimId: `native:v2:${commandId}`,
          claimedBootId: server.nativeCreationBootId,
          claimedAt: yield* nowIso,
          grantId: guard.grantId,
          grantRevision: guard.grantRevision,
        },
        authorize("claim"),
      );
      const claimId = claimed.history.intent.claimId;
      if (
        claimed.history.intent.grantId !== guard.grantId ||
        claimed.history.intent.grantRevision !== guard.grantRevision
      ) {
        return yield* new NativeCreationAuthorityError({
          code: "stale_grant",
          message: "Native retry cannot replace its claimed grant.",
        });
      }
      const acceptedResult = Effect.gen(function* () {
        const observation = yield* threads.observeCommand({ threadId, commandId, messageId });
        return yield* Schema.decodeUnknownEffect(NativeBootstrapDispatchResultV2)({
          version: 2,
          commandId,
          threadId,
          messageId,
          commandAcceptance: "accepted",
          creation: observation.creation ?? null,
        }).pipe(
          Effect.mapError(() =>
            held("Native stage receipts do not yet prove ordered logical acceptance."),
          ),
        );
      });
      const existingHistory = yield* repository.readBoundedHistoryByThread(threadId);
      if (existingHistory?.finalReceipt?.status === "accepted") return yield* acceptedResult;
      const nativeFacts = yield* Effect.serviceOption(EventSink.EventSinkV2);
      const deleteId = CommandId.make(`${commandId}:bootstrap-thread-delete`);
      const deletion = { type: "thread.delete" as const, commandId: deleteId, threadId };
      const readFacts = (id: CommandId) =>
        Option.isSome(nativeFacts)
          ? nativeFacts.value
              .readNativeCommandFacts({ threadId, commandId: id })
              .pipe(Effect.mapError(() => held("Native recovery command facts are unavailable.")))
          : Effect.fail(held("Native recovery requires its coherent private command facts."));
      const observeRecovery = Effect.gen(function* () {
        const companion = yield* repository.readThreadRecoveryCommand(deleteId);
        if (companion === null) return "absent" as const;
        const history = yield* repository.readHistoryByClaim(claimId);
        const starts = history.effects.filter(
          (fact) => fact.effectId === companion.commandStartEffectId && fact.phase === "started",
        );
        const completions = history.effects.filter(
          (fact) => fact.effectId === companion.commandStartEffectId && fact.phase === "completed",
        );
        const facts = yield* readFacts(deleteId);
        const completion = completions[0];
        const start = starts[0];
        const receipt = facts.receipt;
        if (
          companion.claimId !== claimId ||
          companion.threadId !== threadId ||
          companion.commandId !== deleteId ||
          companion.commandDigest !== nativeCreationV2CommandDigest(deletion) ||
          history.effectOverflow ||
          facts.eventMetadataOverflow ||
          starts.length !== 1 ||
          start?.kind !== "native_command" ||
          start.commandId !== deleteId ||
          start.commandDigest !== companion.commandDigest ||
          start.commandType !== "thread.delete" ||
          start.threadId !== threadId ||
          completions.length !== 1 ||
          completion?.kind !== "native_command" ||
          completion.phase !== "completed" ||
          completion.commandId !== deleteId ||
          completion.threadId !== threadId ||
          completion.commandType !== "thread.delete" ||
          completion.commandDigest !== companion.commandDigest ||
          completion.ordinal <= start.ordinal ||
          receipt?.status !== "accepted" ||
          receipt.commandId !== deleteId ||
          receipt.threadId !== threadId ||
          receipt.commandType !== "thread.delete" ||
          receipt.resultSequence !== completion.sequence ||
          facts.events.filter(
            (stored) =>
              stored.commandId === deleteId &&
              stored.event.threadId === threadId &&
              stored.event.type === "thread.deleted",
          ).length !== 1 ||
          facts.eventMetadata.filter(
            (event) =>
              event.eventId === completion.eventId &&
              event.sequence === completion.sequence &&
              event.commandId === deleteId &&
              event.aggregateKind === "thread" &&
              event.aggregateId === threadId &&
              event.applicationEventVersion === 2,
          ).length !== 1
        ) {
          return yield* held(
            "Native recovery has a prior unresolved or contradictory result; observe its original command.",
          );
        }
        return "completed" as const;
      });
      if ((yield* observeRecovery) === "completed")
        return yield* held(
          "The failed native preparation was already logically deleted; external cleanup remains separate.",
        );
      const priorRecovery = yield* repository.readHistoryByClaim(claimId);
      if (
        priorRecovery.effects.some(
          (fact) =>
            (fact.kind === "cleanup" && fact.resource.kind === "thread") ||
            (fact.kind === "native_command" && fact.commandId === deleteId),
        )
      ) {
        return yield* held(
          "Native recovery already has a prior start; observe it before preparing again.",
        );
      }
      const recoverCreatedThread = Effect.gen(function* () {
        if ((yield* observeRecovery) === "completed") return;
        const history = yield* repository.readHistoryByClaim(claimId);
        const finalFacts = yield* readFacts(commandId);
        if (finalFacts.receipt?.status === "accepted") return;
        const chain = finalFacts.nativeCreationHistory;
        const stages = [
          [`${commandId}:native:v2:create`, "thread.create"],
          [`${commandId}:native:v2:message`, "message.dispatch"],
          [commandId, "prepared-run.release"],
        ];
        if (
          history.effectOverflow ||
          finalFacts.eventMetadataOverflow ||
          finalFacts.receipt !== null ||
          history.normalizedCommandDigest === null ||
          chain === null ||
          chain.claimId !== claimId ||
          chain.originalCommandId !== commandId ||
          chain.threadId !== threadId ||
          chain.overflow ||
          chain.finalReceipt !== null ||
          chain.unresolvedEffects.length > 0 ||
          chain.stageCommands.length !== 3 ||
          stages.some(
            ([id, type]) =>
              chain.stageCommands.filter(
                (stage) => stage.commandId === id && stage.commandType === type,
              ).length !== 1,
          ) ||
          history.effectsV2.some((fact) => fact.commandId === commandId) ||
          [...history.effects, ...history.effectsV2].some(
            (fact) =>
              fact.phase === "started" &&
              ![...history.effects, ...history.effectsV2].some(
                (end) =>
                  end.effectId === fact.effectId &&
                  end.phase === "completed" &&
                  (!("result" in end) || end.result !== "unknown"),
              ),
          ) ||
          history.effects.some(
            (fact) => fact.phase === "completed" && "result" in fact && fact.result === "unknown",
          ) ||
          [
            "effects",
            "unknown_effect_holds",
            "runtime_evidence",
            "restart_continuations",
            "launch_workflows",
            "source_runtime",
          ].some(
            (name) =>
              !Array.isArray(finalFacts.commitSnapshot.records[name]) ||
              finalFacts.commitSnapshot.records[name]!.length > 0,
          )
        ) {
          return yield* held(
            "Native thread recovery cannot cross an unresolved effect or final-command outcome.",
          );
        }
        const createId = CommandId.make(`${commandId}:native:v2:create`);
        const facts = yield* readFacts(createId);
        const birth = facts.incarnation;
        const stage = facts.nativeCreationHistory?.stageCommands.find(
          (entry) => entry.commandId === createId,
        );
        if (facts.receipt === null && stage?.receipt == null) return;
        if (
          facts.creationProvenance !== "native_created" ||
          birth === null ||
          facts.eventMetadataOverflow ||
          facts.receipt?.status !== "accepted" ||
          facts.receipt.commandType !== "thread.create" ||
          facts.receipt.threadId !== threadId ||
          stage?.receipt?.status !== "accepted" ||
          stage.event?.eventId !== birth.eventId ||
          stage.event.sequence !== birth.sequence ||
          facts.projection?.thread.deletedAt !== null ||
          facts.events.filter(
            (stored) =>
              stored.commandId === createId &&
              stored.event.type === "thread.created" &&
              stored.event.id === birth.eventId &&
              stored.sequence === birth.sequence &&
              stored.event.threadId === threadId,
          ).length !== 1
        ) {
          return yield* held("Native recovery has no exact accepted current V2 creation birth.");
        }
        const resource = { kind: "thread" as const, threadId, incarnation: birth };
        const recoveryScopeId = `${preparation.operationId}:cleanup:thread`;
        const authorization = {
          ...input,
          stage: "cleanup" as const,
          recoveryScopeId,
          recoveryResource: resource,
        };
        const cleanupStartEffectId = `${deleteId}:cleanup`;
        const commandStartEffectId = `${deleteId}:command`;
        const cleanup = yield* repository.startEffect(
          claimId,
          {
            kind: "cleanup",
            phase: "started",
            effectId: cleanupStartEffectId,
            timestamp: yield* nowIso,
            resource,
            recoveryScopeId,
          },
          authority.authorize(authorization),
        );
        yield* repository.reserveThreadRecoveryCommand({
          version: 2,
          claimId,
          commandId: deleteId,
          threadId,
          commandType: "thread.delete",
          canonicalCommand: deletion,
          commandDigest: nativeCreationV2CommandDigest(deletion),
          commandStartEffectId,
          cleanupStartEffectId,
          cleanupStartOrdinal: cleanup.ordinal,
          recoveryScopeId,
          resource,
        });
        yield* repository.startEffect(
          claimId,
          {
            kind: "native_command",
            phase: "started",
            effectId: commandStartEffectId,
            timestamp: yield* nowIso,
            commandId: deleteId,
            threadId,
            commandType: "thread.delete",
            commandDigest: nativeCreationV2CommandDigest(deletion),
          },
          authorize("native_command"),
        );
        const context = yield* authority.issueThreadRecovery({
          claimId,
          commandStartEffectId,
          cleanupStartEffectId,
          authorization,
        });
        yield* threads.dispatchNativeCreationRecovery(deletion, context);
        // Logical deletion completes its command start, never external cleanup's outcome.
        yield* observeRecovery;
      });
      const prepareNative = Effect.gen(function* () {
        type Started = Exclude<
          Parameters<NativeCreationRepository["Service"]["startEffect"]>[1],
          { readonly kind: "native_command" | "cleanup" }
        >;
        type Completed = Parameters<NativeCreationRepository["Service"]["completeEffect"]>[1];
        // A committed start without a successful end keeps its original effect ID held.
        const runEffect = <E, R>(
          stage: NativeCreationStage,
          started: Started,
          perform: Effect.Effect<Completed, E, R>,
        ) =>
          Effect.uninterruptibleMask((restore) =>
            Effect.gen(function* () {
              const history = yield* repository.readHistoryByClaim(claimId);
              const prior = history.effects.filter((fact) => fact.effectId === started.effectId);
              const end = prior.find((fact) => fact.phase === "completed");
              if (prior.length > 0) {
                if (
                  end?.phase === "completed" &&
                  "result" in end &&
                  end.result === "succeeded" &&
                  prior.length === 2 &&
                  prior[0]?.phase === "started" &&
                  prior[0].kind === started.kind
                )
                  return end;
                return yield* held(
                  `Native ${stage} has an unresolved prior effect; observe the original effect ID.`,
                );
              }
              yield* repository.startEffect(claimId, started, restore(authorize(stage)));
              const permission = yield* Effect.exit(restore(authorize(stage)));
              if (Exit.isFailure(permission)) {
                yield* repository.completeEffect(
                  claimId,
                  started.kind === "setup"
                    ? {
                        ...started,
                        phase: "completed",
                        timestamp: yield* nowIso,
                        exitCode: null,
                        result: "failed",
                      }
                    : {
                        ...started,
                        phase: "completed",
                        timestamp: yield* nowIso,
                        result: "failed",
                      },
                );
                return yield* Effect.failCause(permission.cause);
              }
              const outcome = yield* Effect.exit(restore(perform));
              if (Exit.isFailure(outcome)) {
                const unknown: Completed =
                  started.kind === "setup"
                    ? {
                        ...started,
                        phase: "completed",
                        timestamp: yield* nowIso,
                        exitCode: null,
                        result: "unknown",
                      }
                    : {
                        ...started,
                        phase: "completed",
                        timestamp: yield* nowIso,
                        result: "unknown",
                      };
                yield* repository.completeEffect(claimId, unknown);
                if (Cause.hasInterruptsOnly(outcome.cause)) return yield* Effect.interrupt;
                return yield* held(
                  `Native ${stage} outcome is unknown: ${String(Cause.squash(outcome.cause))}`,
                );
              }
              return yield* repository.completeEffect(claimId, outcome.value);
            }),
          );
        const lifecycle = <E>(
          stage: Extract<
            NativeCreationStage,
            | "normalization"
            | "tracker_registration"
            | "bootstrap_detachment"
            | "worktree_ownership"
            | "setup_detachment"
            | "setup_completion_detachment"
          >,
          perform: Effect.Effect<void, E>,
        ) =>
          Effect.gen(function* () {
            const fact = {
              kind: "lifecycle" as const,
              phase: "started" as const,
              effectId: `${commandId}:native:v2:${stage}`,
              timestamp: yield* nowIso,
              threadId,
              action: stage,
            };
            return yield* runEffect(
              stage,
              fact,
              perform.pipe(
                Effect.andThen(nowIso),
                Effect.map((timestamp) => ({
                  ...fact,
                  phase: "completed" as const,
                  timestamp,
                  result: "succeeded" as const,
                })),
              ),
            );
          });
        const initial = original.bootstrap.createThread;
        const createId = yield* Schema.decodeUnknownEffect(CommandId)(
          `${commandId}:native:v2:create`,
        ).pipe(Effect.mapError(() => held("Native create stage ID is invalid.")));
        const messageStageId = yield* Schema.decodeUnknownEffect(CommandId)(
          `${commandId}:native:v2:message`,
        ).pipe(Effect.mapError(() => held("Native message stage ID is invalid.")));
        if (
          createId !== `${commandId}:native:v2:create` ||
          messageStageId !== `${commandId}:native:v2:message`
        )
          return yield* held("Derived native stage IDs cannot be normalized.");
        const decodeStage = (value: unknown) =>
          Schema.decodeUnknownEffect(OrchestrationV2Command)(value, {
            onExcessProperty: "error",
          }).pipe(
            Effect.mapError(
              () =>
                new NativeCreationAuthorityError({
                  code: "invalid_preparation",
                  message: "Canonical native command cannot be normalized to its V2 stage.",
                }),
            ),
          );
        const create = yield* decodeStage({
          type: "thread.create",
          commandId: createId,
          threadId,
          projectId: initial.projectId,
          title: initial.title,
          modelSelection: initial.modelSelection,
          runtimeMode: initial.runtimeMode,
          interactionMode: initial.interactionMode,
          branch: resources.branch,
          worktreePath: resources.worktreePath,
          createdBy: "user",
          creationSource: "server",
        });
        const message = yield* decodeStage({
          type: "message.dispatch",
          commandId: messageStageId,
          threadId,
          messageId,
          text: original.message.text,
          attachments: [],
          modelSelection: initial.modelSelection,
          dispatchMode: { type: "defer_start" },
          createdBy: "user",
          creationSource: "server",
        });
        const runId = ids.derive.run({ threadId, ordinal: 1 });
        const release = yield* decodeStage({
          type: "prepared-run.release",
          commandId,
          threadId,
          runId,
        });
        if (
          create.type !== "thread.create" ||
          message.type !== "message.dispatch" ||
          release.type !== "prepared-run.release"
        )
          return yield* held("Native stage normalization changed a stage type.");
        yield* lifecycle(
          "normalization",
          Effect.gen(function* () {
            yield* repository.reserveCommandIdentities(claimId, [
              createId,
              messageStageId,
              commandId,
            ]);
            for (const command of [create, message, release])
              yield* repository.reserveCommand(claimId, command);
            yield* repository.recordNormalizedCommand(claimId, release);
          }),
        );
        const stageInput = { ...input, claimId, stage: "native_command" as const };
        yield* threads.dispatchNativeCreationStage(create, stageInput);
        yield* lifecycle(
          "tracker_registration",
          setupTracker.begin({
            threadId,
            branch,
            baseRef: preparation.binding.base_branch,
            stages: ["fetch", "checkout", "setup-script", "agent"],
            fiber: yield* Effect.fiber,
          }),
        );
        const requireDetached = Effect.gen(function* () {
          const attachment = yield* threads.readCurrentThreadRuntimeAttachment(threadId);
          if (attachment.status !== "stopped")
            return yield* held(
              "Native bootstrap cannot detach an unknown or current runtime without an exact durable stop fence.",
            );
        });
        yield* lifecycle("bootstrap_detachment", requireDetached);
        yield* threads.dispatchNativeCreationStage(message, stageInput);
        yield* lifecycle(
          "worktree_ownership",
          threads.acquireWorktreeOwnership(threadId, resources.worktreePath).pipe(Effect.asVoid),
        );
        const requireLease = Effect.gen(function* () {
          const incarnation = yield* threads.getThreadOwnershipIncarnation(threadId);
          const resourcePath = yield* fs
            .realPath(resources.worktreePath)
            .pipe(Effect.orElseSucceed(() => resources.worktreePath));
          const leases = yield* threads.listWorktreeOwnershipLeases;
          const current = leases.filter(
            (lease) =>
              lease.resourcePath === resourcePath &&
              lease.ownerThreadId === threadId &&
              Option.isSome(incarnation) &&
              lease.ownerIncarnation === incarnation.value &&
              lease.branch === resources.branch,
          );
          if (current.length !== 1 || current[0]!.expiresAtMs <= (yield* Clock.currentTimeMillis))
            return yield* held(
              "Native preparation no longer owns its exact current checkout lease.",
            );
        }).pipe(
          Effect.mapError((cause) =>
            cause._tag === "NativeCreationAuthorityError"
              ? cause
              : held("Current native checkout ownership is unavailable."),
          ),
        );
        const project = yield* projects.getById(ProjectId.make(initial.projectId)).pipe(
          Effect.mapError(() => held("Native project is unavailable.")),
          Effect.flatMap(
            Option.match({
              onNone: () => Effect.fail(held("Native project no longer exists.")),
              onSome: Effect.succeed,
            }),
          ),
        );
        if (project.workspaceRoot !== resources.projectCwd)
          return yield* held("Native project checkout changed after the claim.");
        let startRef = preparation.binding.base_branch;
        if (preparation.binding.start_from_origin) {
          yield* authorize("fetch");
          const remoteExists = yield* git
            .remoteExists({ cwd: resources.projectCwd, remoteName: "origin" })
            .pipe(Effect.mapError(() => held("Native remote state is unknown.")));
          if (remoteExists) {
            const fact = {
              kind: "fetch" as const,
              phase: "started" as const,
              effectId: `${commandId}:native:v2:fetch`,
              timestamp: yield* nowIso,
              projectCwd: resources.projectCwd,
              baseRef: preparation.binding.base_branch,
            };
            yield* runEffect(
              "fetch",
              fact,
              requireLease.pipe(
                Effect.andThen(authorize("fetch")),
                Effect.andThen(
                  git.fetchRemote({
                    cwd: resources.projectCwd,
                    remoteName: "origin",
                    refName: startRef,
                  }),
                ),
                Effect.andThen(nowIso),
                Effect.map((timestamp) => ({
                  ...fact,
                  phase: "completed" as const,
                  timestamp,
                  result: "succeeded" as const,
                })),
              ),
            );
            if (
              yield* git
                .remoteBranchExists({
                  cwd: resources.projectCwd,
                  remoteName: "origin",
                  refName: startRef,
                })
                .pipe(Effect.mapError(() => held("Native fetched branch is unknown.")))
            ) {
              startRef = (yield* git
                .resolveRemoteTrackingCommit({
                  cwd: resources.projectCwd,
                  refName: startRef,
                  fallbackRemoteName: "origin",
                })
                .pipe(Effect.mapError(() => held("Native fetched commit is unknown.")))).commitSha;
            }
          }
        }
        const checkout = {
          kind: "worktree" as const,
          phase: "started" as const,
          effectId: `${commandId}:native:v2:worktree`,
          timestamp: yield* nowIso,
          projectCwd: resources.projectCwd,
          worktreePath: resources.worktreePath,
          branch: resources.branch,
          baseRef: preparation.binding.base_branch,
          ownership: "claimed" as const,
        };
        yield* runEffect(
          "worktree",
          checkout,
          Effect.gen(function* () {
            yield* requireLease;
            if (
              yield* fs
                .exists(resources.worktreePath)
                .pipe(Effect.mapError(() => held("Native worktree existence is unknown.")))
            )
              return yield* held(
                "An unproved native worktree already exists; recreation and prune are held.",
              );
            yield* authorize("worktree");
            const created = yield* git.createWorktree({
              cwd: resources.projectCwd,
              refName: startRef,
              newRefName: branch,
              baseRefName: preparation.binding.base_branch,
              path: resources.worktreePath,
            });
            if (
              created.worktree.path !== resources.worktreePath ||
              created.worktree.refName !== resources.branch
            )
              return yield* held(
                "Native worktree result differs from its exact claimed resources.",
              );
            return {
              ...checkout,
              phase: "completed" as const,
              timestamp: yield* nowIso,
              ownership: "created" as const,
              result: "succeeded" as const,
            };
          }),
        );
        if (
          !(yield* fs
            .exists(resources.worktreePath)
            .pipe(Effect.mapError(() => held("Native worktree existence is unknown."))))
        )
          return yield* held(
            "The completed native checkout is missing; automatic prune and recreation are held.",
          );
        if (preparation.binding.run_setup_script) {
          const setupStart = {
            kind: "setup" as const,
            phase: "started" as const,
            effectId: `${commandId}:native:v2:setup`,
            timestamp: yield* nowIso,
            worktreePath: resources.worktreePath,
            terminalId: null,
          };
          const setup = yield* runEffect(
            "setup",
            setupStart,
            Effect.gen(function* () {
              yield* requireLease;
              yield* authorize("setup");
              const result = yield* setupScripts.runForThread({
                threadId,
                projectId: project.id,
                projectCwd: resources.projectCwd,
                worktreePath: resources.worktreePath,
                project: {
                  id: project.id,
                  workspaceRoot: project.workspaceRoot,
                  scripts: project.scripts,
                },
                observeCompletion: {
                  onOutputLine: (line) => setupTracker.appendTail(threadId, "setup-script", line),
                },
              });
              if (result.status === "no-script")
                return {
                  ...setupStart,
                  phase: "completed" as const,
                  timestamp: yield* nowIso,
                  exitCode: null,
                  result: "succeeded" as const,
                };
              if (result.completion === undefined)
                return yield* held("Native setup has no complete outcome observation.");
              const completion = yield* result.completion;
              return {
                ...setupStart,
                phase: "completed" as const,
                timestamp: yield* nowIso,
                terminalId: result.terminalId,
                exitCode: completion.exitCode,
                result:
                  completion.exitCode === 0
                    ? ("succeeded" as const)
                    : completion.exitCode === null
                      ? ("unknown" as const)
                      : ("failed" as const),
              };
            }),
          );
          if (setup.kind !== "setup" || setup.phase !== "completed" || setup.result !== "succeeded")
            return yield* held("Native setup did not complete successfully.");
          yield* lifecycle(
            "setup_completion_detachment",
            setup.terminalId === null
              ? Effect.void
              : terminals
                  .close({ threadId, terminalId: setup.terminalId, deleteHistory: false })
                  .pipe(Effect.asVoid),
          );
        }
        yield* lifecycle("setup_detachment", setupTracker.markUncancellable(threadId));
        yield* requireLease;
        yield* threads.dispatchNativeCreationStage(release, stageInput);
        yield* setupTracker.finish(threadId, "done");
        return yield* acceptedResult;
      }).pipe(
        Effect.onError((cause) =>
          Effect.uninterruptible(
            recoverCreatedThread.pipe(
              Effect.catchCause((recoveryCause) =>
                Effect.logWarning("Native bootstrap thread recovery is held", {
                  commandId,
                  threadId,
                  cause,
                  recoveryCause,
                }),
              ),
              Effect.andThen(
                setupTracker.finish(
                  threadId,
                  Cause.hasInterruptsOnly(cause) ? "cancelled" : "failed",
                  failureDetail(Cause.squash(cause)),
                ),
              ),
              Effect.asVoid,
            ),
          ),
        ),
        Effect.ensuring(releasePreparation(commandId)),
      );
      const fiber = yield* Effect.uninterruptibleMask(() =>
        Effect.gen(function* () {
          if (!(yield* reservePreparation(commandId)))
            return yield* held(
              "This native preparation is already active; observe its original claim.",
            );
          return yield* prepareNative.pipe(Effect.interruptible, Effect.forkIn(preparationScope));
        }),
      );
      return yield* Fiber.join(fiber);
    });

  return ThreadLaunchService.of({ launch, dispatchNativeBootstrap });
});

export const layer = Layer.effect(ThreadLaunchService, make);
