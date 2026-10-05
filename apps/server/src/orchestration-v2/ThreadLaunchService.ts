import {
  LegacyOwnedTerminalControl,
  LegacyNoTerminalControl,
  LegacyDeletionProvenance,
  LegacyFailureDeletionProvenance,
  type LegacyFailureDeleteCommand,
  type LegacyGuardRejectionDeleteCommand,
  type LegacyPreparationUpdate,
  type LegacyPreparation,
  type RecordedThreadProjection as OrchestrationV2ThreadProjection,
} from "./RecordedTypes.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import { awaitThreadCreationCleanup } from "./ThreadDeletion.ts";
import * as Deferred from "effect/Deferred";
import { makeLegacyPreflight, type LegacyPreflightOutcome } from "./LegacyBootstrapPreflight.ts";
import * as Fiber from "effect/Fiber";
import {
  canonicalLegacyPayload,
  legacyPayloadHash,
  legacyPreparationEffectId,
  legacyPreparationReleaseBlocker,
  legacyNeverInvokedSetupOpen,
  legacyBootstrapBirth,
  legacyPreparationGeneration,
  sameLegacyBootstrapPolicy,
} from "./LegacyBootstrap.ts";
import * as EventSink from "./EventSink.ts";
import * as Stream from "effect/Stream";
import * as WorktreeSetupTracker from "../project/WorktreeSetupTracker.ts";
import * as ProjectCloneTracker from "../project/ProjectCloneTracker.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import type { LegacyWorktreePreparationHooks } from "../vcs/GitVcsDriver.ts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import {
  CommandId,
  EventId,
  type ChatAttachment,
  type MessageId,
  type ModelSelection,
  type OrchestrationV2Actor,
  type OrchestrationV2Command,
  type OrchestrationV2LegacyBootstrapPolicy,
  type OrchestrationV2LegacyPreflightBinding,
  type OrchestrationV2CreationSource,
  type OrchestrationV2ProviderThreadNativeMetadata,
  type ProviderDriverKind,
  type ProviderInteractionMode,
  ProjectId,
  type RunId,
  type RuntimeMode,
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
      readonly baseRef: string;
      readonly branch?: string | undefined;
      readonly startFromOrigin?: boolean | undefined;
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
}

export interface ThreadLaunchInput {
  readonly commandId: CommandId;
  /** Server-owned compatibility input; public launch schemas do not decode it. */
  readonly preparationReleaseCommandId?: CommandId;
  readonly legacyBootstrap?: OrchestrationV2LegacyBootstrapPolicy;
  readonly threadId?: ThreadId;
  readonly reuseExistingThread?: boolean;
  readonly projectId: ProjectId;
  readonly title: string;
  readonly generateTitle?: boolean;
  readonly modelSelection: ModelSelection;
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: ProviderInteractionMode;
  readonly workspaceStrategy: ThreadLaunchWorkspaceStrategy;
  readonly runSetupScript?: boolean;
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
  | "commandId"
  | "projectId"
  | "workspaceStrategy"
  | "initialMessage"
  | "runSetupScript"
  | "preparationReleaseCommandId"
  | "legacyBootstrap"
> & {
  /**
   * Set when a retry reuses the worktree its failed attempt created and
   * recorded. Its setup is tracked like a new one, but the thread already
   * records the workspace, and a branch rename may still be running.
   */
  readonly reusedWorktree?: { readonly baseRef: string };
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
  },
) {
  override get message(): string {
    return `Thread launch ${this.commandId} failed during ${this.operation}.`;
  }
}

export class ThreadLaunchService extends Context.Service<
  ThreadLaunchService,
  {
    readonly preflightLegacyBootstrap: (
      binding: OrchestrationV2LegacyPreflightBinding,
    ) => Effect.Effect<LegacyPreflightOutcome, ThreadLaunchError>;
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

const isSetupOperationError = Schema.is(ProjectSetupScriptRunner.ProjectSetupScriptOperationError);
const isTerminalInputValidationError = Schema.is(
  TerminalManager.LegacyTerminalInputValidationError,
);

function failureDetail(error: unknown): string {
  if (isThreadLaunchError(error)) {
    const cause = error.cause;
    const detail = cause instanceof Error ? cause.message : String(cause);
    return `Workspace preparation failed during ${error.operation.replaceAll("-", " ")}: ${detail}`;
  }
  return `Workspace preparation failed: ${error instanceof Error ? error.message : String(error)}`;
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
  const eventSink = yield* EventSink.EventSinkV2;
  const ids = yield* IdAllocator.IdAllocatorV2;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const managedFolders = yield* ManagedProjectFolders.ManagedProjectFolders;
  const preparationScope = yield* Scope.make("sequential");
  const preflight = yield* makeLegacyPreflight;
  const scheduledLaunches = yield* Ref.make<{
    readonly commands: ReadonlySet<CommandId>;
    readonly legacyResults: ReadonlyMap<CommandId, Deferred.Deferred<void, ThreadLaunchError>>;
  }>({ commands: new Set(), legacyResults: new Map() });
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

  const readReceipt = (
    input: PreparationInput & { readonly threadId?: ThreadId | undefined },
    commandId: CommandId,
  ) =>
    receipts
      .getByCommandId(commandId)
      .pipe(Effect.mapError(mapError(input, "read-receipt", input.threadId)));

  const deleteLegacyGuardRejectedShell = Effect.fn(
    "ThreadLaunchService.deleteLegacyGuardRejectedShell",
  )(function* (input: PreparationInput, threadId: ThreadId, runId: RunId) {
    const policy = input.legacyBootstrap;
    if (policy === undefined || !policy.ownsNewThread) return false;
    const projection = yield* threads
      .getThreadProjection(threadId)
      .pipe(Effect.mapError(mapError(input, "release-run", threadId)));
    const run = projection.runs.find((run) => run.id === runId);
    const preparation = run?.legacyPreparation;
    const decision = run?.legacyReleaseDecision;
    const collect = (commandId: CommandId) =>
      eventSink.readByCommandId({ commandId }).pipe(
        Stream.runCollect,
        Effect.map((stored) => Array.from(stored)),
        Effect.mapError(mapError(input, "release-run", threadId)),
      );
    const release = yield* receipts
      .getByCommandId(policy.releaseCommandId)
      .pipe(Effect.mapError(mapError(input, "release-run", threadId)));
    if (
      run === undefined ||
      preparation === undefined ||
      decision === undefined ||
      Option.isNone(release) ||
      release.value.status !== "rejected"
    )
      return false;
    const receivingPolicy = { ...policy, runId };
    const released = (yield* collect(policy.releaseCommandId)).at(-1);
    const proof = legacyBootstrapBirth({
      policy: receivingPolicy,
      claimEvents: yield* collect(policy.createCommandId),
      birthEvents: yield* collect(policy.birthCommandId),
    });
    const claim = yield* receipts
      .getByCommandId(policy.createCommandId)
      .pipe(Effect.mapError(mapError(input, "release-run", threadId)));
    const birth = yield* receipts
      .getByCommandId(policy.birthCommandId)
      .pipe(Effect.mapError(mapError(input, "release-run", threadId)));
    if (
      proof.type !== "valid" ||
      Option.isNone(claim) ||
      Option.isNone(birth) ||
      claim.value.status !== "accepted" ||
      birth.value.status !== "accepted" ||
      claim.value.commandType !==
        (policy.ownsNewThread ? "thread.create" : "thread.metadata.update") ||
      birth.value.commandType !== "message.dispatch" ||
      claim.value.threadId !== threadId ||
      birth.value.threadId !== threadId ||
      release.value.commandType !== "prepared-run.release" ||
      release.value.threadId !== threadId ||
      released?.event.type !== "run.updated" ||
      released.event.id !== decision.evidenceEventId ||
      released.event.payload.id !== runId ||
      released.sequence !== release.value.resultSequence ||
      !sameLegacyBootstrapPolicy(decision.policy, receivingPolicy) ||
      !sameLegacyBootstrapPolicy(preparation.policy, receivingPolicy) ||
      decision.claimEventId !== proof.claimEventId ||
      decision.claimSequence !== proof.claimSequence ||
      decision.claimReceiptSequence !== claim.value.resultSequence ||
      decision.birthEventId !== proof.birthEventId ||
      decision.birthSequence !== proof.sequence ||
      decision.birthReceiptSequence !== birth.value.resultSequence ||
      canonicalLegacyPayload(released.event.payload.legacyReleaseDecision) !==
        canonicalLegacyPayload((({ deletion: _deletion, ...original }) => original)(decision))
    )
      return yield* mapError(
        input,
        "release-run",
        threadId,
      )("Guard deletion lacks authentic original rejection and birth receipts.");
    const correlation = {
      version: 1 as const,
      policy: receivingPolicy,
      threadId,
      runId,
      claimEventId: decision.claimEventId,
      claimSequence: decision.claimSequence,
      claimReceiptSequence: decision.claimReceiptSequence,
      birthEventId: decision.birthEventId,
      birthSequence: decision.birthSequence,
      birthReceiptSequence: decision.birthReceiptSequence,
      preparationGeneration: preparation.generation,
    };
    const identity = {
      type: "legacy-bootstrap.guard-rejection-delete" as const,
      commandId: CommandId.make(`${policy.createCommandId}:guard-rejection-delete`),
      threadId,
      runId,
      legacyBootstrap: receivingPolicy,
    };
    let command: LegacyGuardRejectionDeleteCommand;
    if (preparation.setup.status === "resolved")
      command = {
        ...identity,
        legacyOwnedControl: yield* Schema.decodeEffect(LegacyOwnedTerminalControl)({
          ...correlation,
          terminalId: preparation.setup.definition.terminalId,
          generation: preparation.setup.definition.generation,
        }).pipe(Effect.mapError(mapError(input, "release-run", threadId))),
      };
    else if (preparation.setup.status === "no_script" || preparation.setup.status === "opted_out")
      command = {
        ...identity,
        legacyNoControl: yield* Schema.decodeEffect(LegacyNoTerminalControl)({
          ...correlation,
          type: "no_control",
          workspacePath: projection.thread.worktreePath ?? preparation.projectWorkspaceRoot,
          projectWorkspaceRoot: preparation.projectWorkspaceRoot,
        }).pipe(Effect.mapError(mapError(input, "release-run", threadId))),
      };
    else return false;
    const dispatch = threads.dispatchLegacyGuardRejectionDelete;
    if (dispatch === undefined)
      return yield* mapError(
        input,
        "release-run",
        threadId,
      )("Authentic private guard-deletion owner is unavailable; preserve the shell.");
    const dispatched = yield* dispatch(command).pipe(
      Effect.mapError(mapError(input, "release-run", threadId)),
      Effect.result,
    );
    const deletion = yield* receipts
      .getByCommandId(command.commandId)
      .pipe(Effect.mapError(mapError(input, "release-run", threadId)));
    const deleted = yield* collect(command.commandId);
    if (Option.isNone(deletion)) {
      if (dispatched._tag === "Failure") return yield* dispatched.failure;
      return yield* mapError(
        input,
        "release-run",
        threadId,
      )("Guard deletion receipt is absent; preserve exact D identity.");
    }
    const expected = yield* Schema.decodeUnknownEffect(LegacyDeletionProvenance)({
      ...correlation,
      commandId: command.commandId,
      evidenceEventId: EventId.make(`${command.commandId}:event`),
      projectWorkspaceRoot: preparation.projectWorkspaceRoot,
      ...(command.legacyNoControl === undefined
        ? {
            type: "bound_control",
            control: command.legacyOwnedControl,
            workspacePath:
              preparation.setup.status === "resolved"
                ? preparation.setup.definition.cwd
                : undefined,
          }
        : {
            type: "no_control",
            control: command.legacyNoControl,
            workspacePath: command.legacyNoControl.workspacePath,
          }),
    }).pipe(Effect.mapError(mapError(input, "release-run", threadId)));
    const effects = yield* outbox
      .listByCommandId(command.commandId)
      .pipe(Effect.mapError(mapError(input, "release-run", threadId)));
    const cleanup = effects.filter((effect) => effect.request.type === "terminal.cleanup");
    const cleanupMatches =
      command.legacyNoControl !== undefined
        ? cleanup.length === 0
        : cleanup.length === 1 &&
          cleanup[0]?.request.type === "terminal.cleanup" &&
          cleanup[0].request.legacyOwnedControl !== undefined &&
          Schema.toEquivalence(LegacyOwnedTerminalControl)(
            cleanup[0].request.legacyOwnedControl,
            command.legacyOwnedControl,
          );
    const storedDecision =
      deleted[0]?.event.type === "run.updated"
        ? deleted[0].event.payload.legacyReleaseDecision
        : undefined;
    const originalDecision =
      storedDecision === undefined
        ? undefined
        : (({ deletion: _deletion, ...original }) => original)(storedDecision);
    if (
      deletion.value.status !== "accepted" ||
      deletion.value.commandType !== command.type ||
      deletion.value.threadId !== threadId ||
      (dispatched._tag === "Success" &&
        deletion.value.resultSequence !== dispatched.success.sequence) ||
      deleted.length !== 2 ||
      deleted[0]?.commandId !== command.commandId ||
      deleted[0].event.type !== "run.updated" ||
      deleted[0].event.threadId !== threadId ||
      deleted[0].event.runId !== runId ||
      deleted[0].event.id !== expected.evidenceEventId ||
      deleted[0].event.payload.id !== runId ||
      storedDecision?.deletion === undefined ||
      !Schema.toEquivalence(LegacyDeletionProvenance)(storedDecision.deletion, expected) ||
      canonicalLegacyPayload(originalDecision) !==
        canonicalLegacyPayload(released.event.payload.legacyReleaseDecision) ||
      deleted[1]?.commandId !== command.commandId ||
      deleted[1].event.type !== "thread.deleted" ||
      deleted[1].event.threadId !== threadId ||
      deleted[1].sequence !== deletion.value.resultSequence ||
      deleted[0].sequence + 1 !== deleted[1].sequence ||
      deleted[1].event.payload.deletedAt === null ||
      !cleanupMatches
    )
      return yield* mapError(
        input,
        "release-run",
        threadId,
      )("Guard deletion result is unresolved; preserve its exact D identity.");
    return true;
  });

  const deleteLegacyWorkspaceFailureShell = Effect.fn(
    "ThreadLaunchService.deleteLegacyWorkspaceFailureShell",
  )(function* (input: PreparationInput, threadId: ThreadId, runId: RunId) {
    const policy = input.legacyBootstrap;
    if (policy === undefined || !policy.ownsNewThread) return false;
    const projection = yield* threads
      .getThreadProjection(threadId)
      .pipe(Effect.mapError(mapError(input, "fail-run", threadId)));
    const run = projection.runs.find((entry) => entry.id === runId);
    const decision = run?.legacyPreparationFailureDecision;
    if (run === undefined || decision === undefined) return false;
    const receivingPolicy = { ...policy, runId };
    if (!sameLegacyBootstrapPolicy(decision.policy, receivingPolicy))
      return yield* mapError(
        input,
        "fail-run",
        threadId,
      )("Workspace-failure policy changed; retain the shell.");
    const control = yield* Schema.decodeUnknownEffect(LegacyNoTerminalControl)({
      version: 1,
      type: "no_control",
      policy: receivingPolicy,
      threadId,
      runId,
      claimEventId: decision.claimEventId,
      claimSequence: decision.claimSequence,
      claimReceiptSequence: decision.claimReceiptSequence,
      birthEventId: decision.birthEventId,
      birthSequence: decision.birthSequence,
      birthReceiptSequence: decision.birthReceiptSequence,
      preparationGeneration: decision.preparationGeneration,
      workspacePath: decision.workspacePath,
      projectWorkspaceRoot: decision.projectWorkspaceRoot,
    }).pipe(Effect.mapError(mapError(input, "fail-run", threadId)));
    const command: LegacyFailureDeleteCommand = {
      type: "legacy-bootstrap.failure-delete",
      commandId: CommandId.make(`${policy.createCommandId}:failure-delete`),
      threadId,
      runId,
      legacyBootstrap: receivingPolicy,
      legacyNoControl: control,
    };
    const dispatch = threads.dispatchLegacyFailureDelete;
    if (dispatch === undefined) return false;
    const dispatched = yield* dispatch(command).pipe(Effect.result);
    const receipt = yield* receipts
      .getByCommandId(command.commandId)
      .pipe(Effect.mapError(mapError(input, "fail-run", threadId)));
    const events = Array.from(
      yield* eventSink
        .readByCommandId({ commandId: command.commandId })
        .pipe(Stream.runCollect, Effect.mapError(mapError(input, "fail-run", threadId))),
    );
    if (Option.isNone(receipt)) return false;
    const evidence = events[0];
    const tombstone = events[1];
    const recordedDecision =
      evidence?.event.type === "run.updated"
        ? evidence.event.payload.legacyPreparationFailureDecision
        : undefined;
    const deletion = recordedDecision?.deletion;
    const validated =
      deletion === undefined
        ? undefined
        : yield* Schema.decodeUnknownEffect(LegacyFailureDeletionProvenance)(deletion).pipe(
            Effect.mapError(mapError(input, "fail-run", threadId)),
          );
    const { deletion: _oldDeletion, ...original } = decision;
    const { deletion: _newDeletion, ...persisted } = recordedDecision ?? decision;
    const effects = yield* outbox
      .listByCommandId(command.commandId)
      .pipe(Effect.mapError(mapError(input, "fail-run", threadId)));
    if (
      receipt.value.status !== "accepted" ||
      receipt.value.commandType !== command.type ||
      receipt.value.threadId !== threadId ||
      (dispatched._tag === "Success" &&
        receipt.value.resultSequence !== dispatched.success.sequence) ||
      events.length !== 2 ||
      evidence?.commandId !== command.commandId ||
      evidence.event.type !== "run.updated" ||
      evidence.event.id !== `${command.commandId}:event` ||
      evidence.event.threadId !== threadId ||
      evidence.event.runId !== runId ||
      validated?.provenance.type !== "no_control" ||
      validated.provenance.commandId !== command.commandId ||
      !Schema.toEquivalence(LegacyNoTerminalControl)(validated.provenance.control, control) ||
      validated.failureCommandId !== original.failureCommandId ||
      validated.failureEvidenceEventId !== original.evidenceEventId ||
      canonicalLegacyPayload(original) !== canonicalLegacyPayload(persisted) ||
      tombstone?.commandId !== command.commandId ||
      tombstone.event.type !== "thread.deleted" ||
      tombstone.event.threadId !== threadId ||
      tombstone.event.payload.deletedAt === null ||
      tombstone.sequence !== receipt.value.resultSequence ||
      evidence.sequence + 1 !== tombstone.sequence ||
      effects.length !== 0
    )
      return yield* mapError(
        input,
        "fail-run",
        threadId,
      )("Workspace-failure D outcome is unresolved; preserve exact identity and original error.");
    return true;
  });

  const legacyPreparationJournal = Effect.fn("ThreadLaunchService.legacyPreparationJournal")(
    function* (input: PreparationInput, threadId: ThreadId, runId: RunId) {
      const current = yield* threads
        .getThreadRecords(threadId, ["runs"], { runIds: [runId] })
        .pipe(Effect.mapError(mapError(input, "read-receipt", threadId)));
      let preparation = current.runs.find((run) => run.id === runId)?.legacyPreparation;
      if (preparation === undefined)
        return yield* mapError(
          input,
          "read-receipt",
          threadId,
        )("Legacy preparation has no authenticated ledger.");
      const bound = preparation;
      const persist = (commandId: CommandId, update: LegacyPreparationUpdate) =>
        Effect.gen(function* () {
          const committed = yield* threads
            .dispatch({
              type: "prepared-run.progress",
              commandId,
              threadId,
              runId,
              phase: "setup",
              legacyPreparationUpdate: update,
            })
            .pipe(Effect.mapError(mapError(input, "update-thread", threadId)));
          const receipt = yield* readReceipt(input, commandId);
          const stored = Array.from(
            yield* eventSink
              .readByCommandId({ commandId })
              .pipe(Stream.runCollect, Effect.mapError(mapError(input, "read-receipt", threadId))),
          );
          const event = stored[0];
          const next =
            event?.event.type === "run.updated" ? event.event.payload.legacyPreparation : undefined;
          const actual =
            update.type === "setup-policy"
              ? next?.setup
              : update.type === "initialize"
                ? next
                : next?.steps.find((step) => step.effectId === update.step.effectId);
          const expected =
            update.type === "setup-policy"
              ? update.setup
              : update.type === "initialize"
                ? update.preparation
                : update.step;
          if (
            Option.isNone(receipt) ||
            receipt.value.status !== "accepted" ||
            receipt.value.commandType !== "prepared-run.progress" ||
            receipt.value.threadId !== threadId ||
            stored.length !== 1 ||
            event?.event.type !== "run.updated" ||
            event.event.runId !== runId ||
            event.commandId !== commandId ||
            event.event.id !== `${commandId}:event` ||
            event.sequence !== receipt.value.resultSequence ||
            committed.sequence !== event.sequence ||
            next === undefined ||
            canonicalLegacyPayload(actual) !== canonicalLegacyPayload(expected)
          )
            return yield* mapError(
              input,
              "read-receipt",
              threadId,
            )("Legacy preparation intent or outcome commit/readback is unresolved.");
          preparation = next;
        });
      const intent = (effect: LegacyPreparation["steps"][number]["effect"]) =>
        Effect.gen(function* () {
          const effectId = legacyPreparationEffectId({ generation: bound.generation, effect });
          if (preparation?.steps.some((entry) => entry.effect.kind === effect.kind))
            return yield* mapError(
              input,
              "read-receipt",
              threadId,
            )("A recorded preparation effect requires reconciliation, never reexecution.");
          const stem = `${bound.policy.createCommandId}:preparation:${bound.generation}:${effectId}`;
          const step: LegacyPreparation["steps"][number] = {
            effectId,
            effect,
            inputHash: legacyPayloadHash(canonicalLegacyPayload(effect)),
            state: "intent",
            intentCommandId: CommandId.make(`${stem}:intent`),
            intentEventId: EventId.make(`${stem}:intent:event`),
          };
          yield* persist(step.intentCommandId, { type: "intent", step });
        });
      const outcome = (
        kind: LegacyPreparation["steps"][number]["effect"]["kind"],
        state: LegacyPreparation["steps"][number]["state"],
        evidence: NonNullable<LegacyPreparation["steps"][number]["evidence"]>,
      ) =>
        Effect.gen(function* () {
          const step = preparation?.steps.find((entry) => entry.effect.kind === kind);
          if (step === undefined || step.state !== "intent")
            return yield* mapError(
              input,
              "read-receipt",
              threadId,
            )("Legacy preparation outcome has no exact pending intent.");
          const commandId = CommandId.make(
            `${step.intentCommandId.slice(0, -":intent".length)}:outcome`,
          );
          yield* persist(commandId, {
            type: "outcome",
            step: {
              ...step,
              state,
              evidence,
              outcomeCommandId: commandId,
              outcomeEventId: EventId.make(`${commandId}:event`),
            },
          });
        });
      return {
        bound,
        get preparation() {
          return preparation;
        },
        persist,
        intent,
        outcome,
      };
    },
  );

  const legacyRenameHooks = Effect.fn("ThreadLaunchService.legacyRenameHooks")(function* (
    input: PreparationInput,
    threadId: ThreadId,
    runId: RunId,
  ) {
    const journal = yield* legacyPreparationJournal(input, threadId, runId);
    const preparation = journal.preparation;
    if (preparation === undefined)
      return yield* mapError(
        input,
        "read-receipt",
        threadId,
      )("Legacy rename preparation is unavailable.");
    const add = preparation.steps.find((step) => step.effect.kind === "worktree.add");
    const lastClaim = [...preparation.steps]
      .reverse()
      .find(
        (step) =>
          step.state === "known_succeeded" &&
          (step.evidence?.type === "worktree_claim" ||
            (step.evidence?.type === "settled_git" && step.evidence.claim !== undefined)),
      );
    const claim =
      lastClaim?.evidence !== undefined && "claim" in lastClaim.evidence
        ? lastClaim.evidence.claim
        : undefined;
    if (
      add?.effect.kind !== "worktree.add" ||
      add.effect.input.before === undefined ||
      claim === undefined
    )
      return yield* mapError(
        input,
        "read-receipt",
        threadId,
      )("Legacy rename has no exact durable creation claim.");
    const hooks: import("../vcs/GitVcsDriver.ts").LegacyBranchRenameHooks = {
      before: add.effect.input.before,
      claim,
      beforeEffect: (step) => journal.intent({ kind: "branch.rename", input: step }),
      afterEffect: (_step, result, material) =>
        Effect.gen(function* () {
          if (result !== "settled_success" || material === undefined) {
            yield* journal.outcome("branch.rename", "unknown", {
              type: "unknown",
              reason: "partial_material",
            });
            return yield* mapError(
              input,
              "update-thread",
              threadId,
            )("Legacy rename outcome is unresolved; preserve material and identity.");
          }
          yield* journal.outcome("branch.rename", "known_succeeded", {
            type: "settled_git",
            exitCode: 0,
            claim: material,
          });
        }),
    };
    return hooks;
  });

  const legacyWorktreeHooks = Effect.fn("ThreadLaunchService.legacyWorktreeHooks")(function* (
    input: PreparationInput,
    threadId: ThreadId,
    runId: RunId,
  ) {
    const journal = yield* legacyPreparationJournal(input, threadId, runId);
    const hooks: LegacyWorktreePreparationHooks = {
      neverInvoked: (step, reason) =>
        Effect.gen(function* () {
          const { kind, ...effectInput } = step;
          const intent = journal.preparation?.steps.find((entry) => entry.effect.kind === kind);
          if (
            kind !== "worktree.add" ||
            intent?.state !== "intent" ||
            canonicalLegacyPayload(intent.effect) !==
              canonicalLegacyPayload({ kind, input: effectInput })
          )
            return yield* mapError(
              input,
              "read-receipt",
              threadId,
            )("Never-invoked owner refusal differs from its exact accepted intent.");
          yield* journal.outcome(kind, "known_no_effect_failure", {
            type: "never_invoked",
            owner: "git",
            reason,
          });
        }),
      beforeEffect: (step) =>
        Effect.gen(function* () {
          if (step.kind === "worktree.add" && step.before === undefined)
            return yield* mapError(
              input,
              "read-receipt",
              threadId,
            )("Worktree intent has no exact owner absence observation.");
          const { kind, ...effectInput } = step;
          yield* journal.intent({ kind, input: effectInput });
          if (journal.preparation?.commonDirectory !== step.commonDirectory)
            return yield* mapError(
              input,
              "read-receipt",
              threadId,
            )("Worktree intent common-directory readback differs from the executing owner.");
        }),
      afterEffect: (step, result, claim) =>
        Effect.gen(function* () {
          if (result === "settled_success" && claim !== undefined) {
            yield* journal.outcome(
              step.kind,
              "known_succeeded",
              step.kind === "worktree.add"
                ? { type: "worktree_claim", claim }
                : { type: "settled_git", exitCode: 0, claim },
            );
            return;
          }
          yield* journal.outcome(step.kind, "unknown", {
            type: "unknown",
            reason: claim === undefined ? "ownership_changed" : "partial_material",
          });
          return yield* mapError(
            input,
            "provision-worktree",
            threadId,
          )("Worktree effect outcome is unknown; preserve exact bytes, identity and journal.");
        }),
    };
    return hooks;
  });

  const legacySetupHooks = Effect.fn("ThreadLaunchService.legacySetupHooks")(function* (
    input: PreparationInput,
    threadId: ThreadId,
    runId: RunId,
  ) {
    const journal = yield* legacyPreparationJournal(input, threadId, runId);
    const { bound, persist, intent, outcome } = journal;
    const terminalId = `legacy-setup:${bound.generation}`;
    const generation = legacyPayloadHash(
      canonicalLegacyPayload({ preparationGeneration: bound.generation, terminalId }),
    );
    const binding = {
      version: 1 as const,
      policy: bound.policy,
      threadId,
      runId,
      terminalId,
      generation,
      preparationGeneration: bound.generation,
      claimEventId: bound.claimEventId,
      claimSequence: bound.claimSequence,
      claimReceiptSequence: bound.claimReceiptSequence,
      birthEventId: bound.birthEventId,
      birthSequence: bound.birthSequence,
      birthReceiptSequence: bound.birthReceiptSequence,
    };
    const captured = bound.setup.status === "resolved" ? bound.setup.definition : undefined;
    const hooks: ProjectSetupScriptRunner.LegacySetupPreparationHooks = {
      binding,
      ...(captured === undefined ? {} : { capturedDefinition: captured }),
      noScript: () =>
        bound.setup.status === "no_script" || bound.setup.status === "opted_out"
          ? Effect.void
          : persist(
              CommandId.make(
                `${bound.policy.createCommandId}:preparation:${bound.generation}:setup-policy`,
              ),
              { type: "setup-policy", setup: { status: "no_script" } },
            ),
      beforeSpawn: (definition) =>
        Effect.gen(function* () {
          const preparation = journal.preparation;
          if (preparation?.setup.status === "unresolved")
            yield* persist(
              CommandId.make(
                `${bound.policy.createCommandId}:preparation:${bound.generation}:setup-policy`,
              ),
              { type: "setup-policy", setup: { status: "resolved", definition } },
            );
          else if (
            preparation?.setup.status !== "resolved" ||
            canonicalLegacyPayload(preparation.setup.definition) !==
              canonicalLegacyPayload(definition)
          )
            return yield* mapError(
              input,
              "read-receipt",
              threadId,
            )("Captured setup definition changed before spawn.");
          yield* intent({ kind: "setup.open", input: definition });
        }),
      neverInvoked: (proof) =>
        Effect.gen(function* () {
          const preparation = journal.preparation;
          const opened = preparation?.steps.find((step) => step.effect.kind === "setup.open");
          if (
            preparation?.setup.status !== "resolved" ||
            opened?.effect.kind !== "setup.open" ||
            opened.state !== "intent" ||
            canonicalLegacyPayload(proof.binding) !== canonicalLegacyPayload(binding) ||
            proof.shell !== preparation.setup.definition.shell ||
            canonicalLegacyPayload(proof.shellArgs) !==
              canonicalLegacyPayload(preparation.setup.definition.shellArgs) ||
            proof.cwd !== preparation.setup.definition.cwd ||
            canonicalLegacyPayload(opened.effect.input) !==
              canonicalLegacyPayload(preparation.setup.definition) ||
            !(
              proof.shell.length === 0 ||
              proof.shell.includes("\0") ||
              proof.shellArgs.some((arg) => arg.includes("\0"))
            )
          )
            return yield* mapError(
              input,
              "read-receipt",
              threadId,
            )("Never-invoked setup owner differs from its exact accepted intent.");
          yield* outcome("setup.open", "known_no_effect_failure", {
            type: "never_invoked",
            owner: "setup",
            reason: "input_validation_failed",
          });
        }),
      afterSpawn: (proof) =>
        outcome("setup.open", "known_succeeded", {
          type: "terminal_generation",
          terminalId: proof.binding.terminalId,
          generation: proof.binding.generation,
          shell: proof.shell,
          shellArgs: [...proof.shellArgs],
        }),
      beforeWrite: () =>
        Effect.gen(function* () {
          const preparation = journal.preparation;
          if (preparation?.setup.status !== "resolved")
            return yield* mapError(
              input,
              "read-receipt",
              threadId,
            )("Setup write lacks a captured definition.");
          const definition = preparation.setup.definition;
          yield* intent({
            kind: "setup.write",
            input: {
              terminalId,
              generation,
              definitionHash: definition.definitionHash,
              commandLine: definition.commandLine,
              completionToken: definition.completionToken,
            },
          });
        }),
      afterWrite: (result, inputCount) =>
        result === "accepted"
          ? outcome("setup.write", "known_started", {
              type: "terminal_write",
              terminalId,
              generation,
              inputCount,
            })
          : outcome("setup.write", "unknown", {
              type: "unknown",
              reason: "process_result_unavailable",
            }),
      afterCompletion: (completion) =>
        Effect.gen(function* () {
          const preparation = journal.preparation;
          if (
            preparation?.setup.status !== "resolved" ||
            preparation.setup.definition.completionToken === null
          )
            return yield* mapError(
              input,
              "read-receipt",
              threadId,
            )("Setup completion has no captured token.");
          const definition = preparation.setup.definition;
          const completionToken = definition.completionToken;
          if (completionToken === null)
            return yield* mapError(
              input,
              "read-receipt",
              threadId,
            )("Setup completion token is unavailable.");
          yield* intent({
            kind: "setup.completion",
            input: {
              terminalId,
              generation,
              definitionHash: definition.definitionHash,
              completionToken,
            },
          });
          yield* completion.exitCode === null
            ? outcome("setup.completion", "unknown", {
                type: "unknown",
                reason: "process_result_unavailable",
              })
            : outcome(
                "setup.completion",
                completion.exitCode === 0 ? "known_succeeded" : "known_completed_failure",
                {
                  type: "setup_completion",
                  terminalId,
                  generation,
                  exitCode: completion.exitCode,
                  durationMs: completion.durationMs,
                },
              );
        }),
    };
    return hooks;
  });

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

    if (input.legacyBootstrap !== undefined && runId !== null) {
      const current = yield* threads
        .getThreadRecords(threadId, ["runs"], { runIds: [runId] })
        .pipe(Effect.mapError(mapError(input, "read-receipt", threadId)));
      const run = current.runs.find((candidate) => candidate.id === runId);
      const policy = run?.legacyBootstrap;
      if (
        run === undefined ||
        policy === undefined ||
        !sameLegacyBootstrapPolicy(policy, { ...input.legacyBootstrap, runId })
      )
        return yield* mapError(
          input,
          "read-receipt",
          threadId,
        )("Legacy preparation birth policy is unavailable.");
      if (run.legacyPreparation === undefined) {
        for (const suffix of [":progress:worktree", ":progress:setup"] as const) {
          if (
            Option.isSome(yield* readReceipt(input, CommandId.make(`${input.commandId}${suffix}`)))
          )
            return yield* mapError(
              input,
              "read-receipt",
              threadId,
            )("Prior preparation progress has no attributable owner ledger.");
        }
        const collect = (commandId: CommandId) =>
          eventSink.readByCommandId({ commandId }).pipe(
            Stream.runCollect,
            Effect.map((events) => Array.from(events)),
            Effect.mapError(mapError(input, "read-receipt", threadId)),
          );
        const proof = legacyBootstrapBirth({
          policy,
          claimEvents: yield* collect(policy.createCommandId),
          birthEvents: yield* collect(policy.birthCommandId),
        });
        const claim = yield* readReceipt(input, policy.createCommandId);
        const birth = yield* readReceipt(input, policy.birthCommandId);
        if (proof.type !== "valid" || Option.isNone(claim) || Option.isNone(birth))
          return yield* mapError(
            input,
            "read-receipt",
            threadId,
          )("Legacy preparation requires exact accepted claim and birth readback.");
        const generation = legacyPreparationGeneration({
          runId,
          birthEventId: proof.birthEventId,
          birthSequence: proof.sequence,
        });
        const preparation: LegacyPreparation = {
          version: 1,
          policy,
          generation,
          claimEventId: proof.claimEventId,
          claimSequence: proof.claimSequence,
          claimReceiptSequence: claim.value.resultSequence,
          birthEventId: proof.birthEventId,
          birthSequence: proof.sequence,
          birthReceiptSequence: birth.value.resultSequence,
          projectWorkspaceRoot: project.workspaceRoot,
          commonDirectory: null,
          setup: { status: input.runSetupScript === false ? "opted_out" : "unresolved" },
          steps: [],
        };
        const commandId = CommandId.make(
          `${policy.createCommandId}:preparation:${generation}:initialize`,
        );
        const committed = yield* threads
          .dispatch({
            type: "prepared-run.progress",
            commandId,
            threadId,
            runId,
            phase: "setup",
            legacyPreparationUpdate: { type: "initialize", preparation },
          })
          .pipe(Effect.mapError(mapError(input, "update-thread", threadId)));
        const receipt = yield* readReceipt(input, commandId);
        const recorded = yield* collect(commandId);
        const event = recorded[0];
        if (
          Option.isNone(receipt) ||
          receipt.value.status !== "accepted" ||
          receipt.value.commandType !== "prepared-run.progress" ||
          receipt.value.threadId !== threadId ||
          recorded.length !== 1 ||
          event?.event.type !== "run.updated" ||
          event.event.runId !== runId ||
          event.event.id !== `${commandId}:event` ||
          event.commandId !== commandId ||
          event.sequence !== receipt.value.resultSequence ||
          committed.sequence !== event.sequence ||
          canonicalLegacyPayload(event.event.payload.legacyPreparation) !==
            canonicalLegacyPayload(preparation)
        )
          return yield* mapError(
            input,
            "read-receipt",
            threadId,
          )("Legacy preparation initialization commit/readback is unresolved.");
      }
    }

    const reused = input.reusedWorktree;
    const tracked = input.workspaceStrategy.type === "worktree" || reused !== undefined;
    let createdWorktreePath: string | null = null;
    let setupTerminalId: string | null = null;
    let workspaceRecorded = false;
    if (input.workspaceStrategy.type === "worktree") {
      yield* setupTracker.begin({
        threadId,
        branch: input.workspaceStrategy.branch ?? null,
        baseRef: input.workspaceStrategy.baseRef,
        stages: ["fetch", "checkout", "setup-script", "agent"],
        fiber: yield* Effect.fiber,
      });
    } else if (reused !== undefined) {
      yield* setupTracker.begin({
        threadId,
        branch: input.workspaceStrategy.branch ?? null,
        baseRef: reused.baseRef,
        stages: ["setup-script", "agent"],
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

      // The server owns worktree naming: without an explicit branch, provision
      // under a temporary `t3code/<hash>` name so the worktree never waits on
      // name generation, then rename in the background below.
      const requestedBranch = input.workspaceStrategy.branch;
      let branch: string | null;
      if (input.workspaceStrategy.type === "worktree" && requestedBranch === undefined) {
        const uuid = yield* randomUuidV4;
        branch = buildTemporaryWorktreeBranchName(() => uuid.replaceAll("-", ""));
      } else {
        branch = requestedBranch ?? null;
      }
      let worktreePath =
        input.workspaceStrategy.type === "existing_worktree"
          ? input.workspaceStrategy.worktreePath
          : null;
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
        if (startFromOrigin) yield* setupTracker.stageStatus(threadId, "fetch", "done");
        yield* setupTracker.stageStatus(threadId, "checkout", "running");
        const legacyPreparation =
          input.legacyBootstrap === undefined || runId === null
            ? undefined
            : yield* legacyWorktreeHooks(input, threadId, runId);
        const worktree = yield* git
          .createWorktree(
            {
              cwd: project.workspaceRoot,
              refName: startRef,
              newRefName: branch!,
              baseRefName: input.workspaceStrategy.baseRef,
              path: null,
            },
            {
              ...(legacyPreparation === undefined ? {} : { legacyPreparation }),
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
        worktreePath = worktree.worktree.path;
        branch = worktree.worktree.refName;
        createdWorktreePath = worktreePath;
        yield* setupTracker.update(threadId, (snapshot) => ({ ...snapshot, worktreePath, branch }));
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
        yield* generateBranchNameFor(worktreeCwd, initialMessage).pipe(
          Effect.flatMap(({ branch: newBranch, exactName }) =>
            Effect.gen(function* () {
              const legacyPreparation =
                input.legacyBootstrap === undefined || runId === null
                  ? undefined
                  : yield* legacyRenameHooks(input, threadId, runId);
              return yield* git.renameBranch({
                cwd: worktreeCwd,
                oldBranch,
                newBranch,
                ...(exactName ? { exactName: true } : {}),
                ...(legacyPreparation === undefined ? {} : { legacyPreparation }),
              });
            }),
          ),
          Effect.flatMap((renamed) =>
            threads.dispatch({
              type: "thread.metadata.update",
              commandId: CommandId.make(`${input.commandId}:branch-rename`),
              threadId,
              branch: renamed.branch,
              worktreePath: worktreeCwd,
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
      const legacyPreparation =
        input.legacyBootstrap !== undefined && runId !== null
          ? yield* legacySetupHooks(input, threadId, runId)
          : undefined;
      const setup = yield* (
        input.runSetupScript === false
          ? Effect.succeed({ status: "no-script" as const })
          : setupScripts.runForThread({
              threadId,
              projectId: input.projectId,
              projectCwd: project.workspaceRoot,
              worktreePath: cwd,
              ...(legacyPreparation === undefined ? {} : { legacyPreparation }),
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
      ).pipe(
        Effect.catch((cause) =>
          Effect.gen(function* () {
            if (
              input.legacyBootstrap === undefined ||
              runId === null ||
              !isSetupOperationError(cause) ||
              cause.operation !== "openTerminal" ||
              !isTerminalInputValidationError(cause.cause)
            )
              return yield* Effect.fail(mapError(input, "run-setup-script", threadId)(cause));
            const proof = cause.cause;
            const current = yield* threads
              .getThreadRecords(threadId, ["runs"], { runIds: [runId] })
              .pipe(Effect.mapError(mapError(input, "read-receipt", threadId)));
            const run = current.runs.find((candidate) => candidate.id === runId);
            const candidate = run === undefined ? undefined : legacyNeverInvokedSetupOpen(run);
            if (
              candidate === undefined ||
              proof.binding.threadId !== threadId ||
              proof.binding.runId !== runId ||
              canonicalLegacyPayload(proof.binding.policy) !==
                canonicalLegacyPayload(candidate.preparation.policy) ||
              proof.binding.preparationGeneration !== candidate.preparation.generation ||
              proof.binding.generation !== candidate.definition.generation ||
              proof.binding.terminalId !== candidate.definition.terminalId ||
              proof.binding.claimEventId !== candidate.preparation.claimEventId ||
              proof.binding.claimSequence !== candidate.preparation.claimSequence ||
              proof.binding.claimReceiptSequence !== candidate.preparation.claimReceiptSequence ||
              proof.binding.birthEventId !== candidate.preparation.birthEventId ||
              proof.binding.birthSequence !== candidate.preparation.birthSequence ||
              proof.binding.birthReceiptSequence !== candidate.preparation.birthReceiptSequence ||
              proof.shell !== candidate.definition.shell ||
              proof.cwd !== cwd ||
              canonicalLegacyPayload(proof.shellArgs) !==
                canonicalLegacyPayload(candidate.definition.shellArgs)
            )
              return yield* mapError(
                input,
                "read-receipt",
                threadId,
              )("Setup lexical refusal has no matching current owner ledger.");
            return { status: "not-entered" as const, detail: proof.detail };
          }),
        ),
      );

      if (setup.status === "no-script" && input.legacyBootstrap !== undefined && runId !== null) {
        const current = yield* threads
          .getThreadRecords(threadId, ["runs"], { runIds: [runId] })
          .pipe(Effect.mapError(mapError(input, "read-receipt", threadId)));
        const preparation = current.runs.find((run) => run.id === runId)?.legacyPreparation;
        if (preparation === undefined)
          return yield* mapError(
            input,
            "read-receipt",
            threadId,
          )("No-script result has no authenticated preparation ledger.");
        if (preparation.setup.status === "unresolved") {
          const commandId = CommandId.make(
            `${preparation.policy.createCommandId}:preparation:${preparation.generation}:setup-policy`,
          );
          const committed = yield* threads
            .dispatch({
              type: "prepared-run.progress",
              commandId,
              threadId,
              runId,
              phase: "setup",
              legacyPreparationUpdate: { type: "setup-policy", setup: { status: "no_script" } },
            })
            .pipe(Effect.mapError(mapError(input, "update-thread", threadId)));
          const receipt = yield* readReceipt(input, commandId);
          const recorded = Array.from(
            yield* eventSink
              .readByCommandId({ commandId })
              .pipe(Stream.runCollect, Effect.mapError(mapError(input, "read-receipt", threadId))),
          );
          const event = recorded[0];
          if (
            Option.isNone(receipt) ||
            receipt.value.status !== "accepted" ||
            receipt.value.threadId !== threadId ||
            receipt.value.commandType !== "prepared-run.progress" ||
            recorded.length !== 1 ||
            event?.event.type !== "run.updated" ||
            event.event.runId !== runId ||
            event.commandId !== commandId ||
            event.event.id !== `${commandId}:event` ||
            event.sequence !== receipt.value.resultSequence ||
            committed.sequence !== event.sequence ||
            event.event.payload.legacyPreparation?.setup.status !== "no_script"
          )
            return yield* mapError(
              input,
              "read-receipt",
              threadId,
            )("No-script outcome commit/readback is unresolved.");
        } else if (
          preparation.setup.status !== "opted_out" &&
          preparation.setup.status !== "no_script"
        ) {
          return yield* mapError(
            input,
            "read-receipt",
            threadId,
          )("No-script result conflicts with the captured setup policy.");
        }
      }

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
            const completion = yield* setup.completion!.pipe(
              Effect.mapError(mapError(input, "run-setup-script", threadId)),
            );
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
          yield* setupTracker.stageStatus(threadId, "setup-script", "done");
        }
      } else if (setup.status === "not-entered") {
        yield* setupTracker.stage(threadId, "setup-script", {
          status: "failed",
          detail: setup.detail,
        });
      } else {
        yield* setupTracker.stageStatus(threadId, "setup-script", "skipped");
      }
      yield* setupTracker.markUncancellable(threadId);
      yield* setupTracker.stageStatus(threadId, "agent", "running");
      if (runId !== null) {
        if (input.legacyBootstrap !== undefined) {
          const current = yield* threads
            .getThreadRecords(threadId, ["runs"], { runIds: [runId] })
            .pipe(Effect.mapError(mapError(input, "read-receipt", threadId)));
          const run = current.runs.find((candidate) => candidate.id === runId);
          if (run === undefined || run.legacyPreparation === undefined)
            return yield* mapError(
              input,
              "read-receipt",
              threadId,
            )("Legacy preparation release has no recorded ledger.");
          const preparation = run.legacyPreparation;
          const blocked = legacyPreparationReleaseBlocker({ run });
          if (blocked !== undefined)
            return yield* mapError(input, "provision-worktree", threadId)(blocked);
          for (const step of preparation.steps) {
            for (const transition of ["intent", "outcome"] as const) {
              const commandId =
                transition === "intent" ? step.intentCommandId : step.outcomeCommandId;
              if (commandId === undefined)
                return yield* mapError(
                  input,
                  "read-receipt",
                  threadId,
                )("Legacy preparation outcome identity is unavailable.");
              const receipt = yield* readReceipt(input, commandId);
              const records = Array.from(
                yield* eventSink
                  .readByCommandId({ commandId })
                  .pipe(
                    Stream.runCollect,
                    Effect.mapError(mapError(input, "read-receipt", threadId)),
                  ),
              );
              const record = records[0];
              const recorded =
                record?.event.type === "run.updated"
                  ? record.event.payload.legacyPreparation
                  : undefined;
              const actualStep = recorded?.steps.find((entry) => entry.effectId === step.effectId);
              const expectedStep =
                transition === "intent"
                  ? {
                      effectId: step.effectId,
                      effect: step.effect,
                      inputHash: step.inputHash,
                      state: "intent",
                      intentCommandId: step.intentCommandId,
                      intentEventId: step.intentEventId,
                    }
                  : step;
              if (
                Option.isNone(receipt) ||
                receipt.value.status !== "accepted" ||
                receipt.value.commandType !== "prepared-run.progress" ||
                receipt.value.threadId !== threadId ||
                records.length !== 1 ||
                record?.event.type !== "run.updated" ||
                record.event.runId !== runId ||
                record.commandId !== commandId ||
                record.event.id !== `${commandId}:event` ||
                record.sequence !== receipt.value.resultSequence ||
                recorded?.generation !== preparation.generation ||
                canonicalLegacyPayload(recorded.policy) !==
                  canonicalLegacyPayload(preparation.policy) ||
                canonicalLegacyPayload(actualStep) !== canonicalLegacyPayload(expectedStep)
              )
                return yield* mapError(
                  input,
                  "read-receipt",
                  threadId,
                )("Legacy preparation release has no exact intent and outcome readback.");
            }
          }
        }
        yield* threads
          .dispatch({
            type: "prepared-run.release",
            commandId:
              input.preparationReleaseCommandId ?? CommandId.make(`${input.commandId}:release`),
            threadId,
            runId,
            ...(input.legacyBootstrap === undefined
              ? {}
              : { legacyBootstrap: { ...input.legacyBootstrap, runId } }),
          })
          .pipe(
            Effect.catch((cause) =>
              input.legacyBootstrap === undefined
                ? Effect.fail(mapError(input, "release-run", threadId)(cause))
                : deleteLegacyGuardRejectedShell(input, threadId, runId).pipe(
                    Effect.flatMap((deleted) =>
                      Effect.fail(
                        new ThreadLaunchError({
                          operation: "release-run",
                          commandId: input.commandId,
                          projectId: input.projectId,
                          threadId,
                          cause,
                          ...(deleted ? { bootstrapThreadDisposition: "deleted" as const } : {}),
                        }),
                      ),
                    ),
                  ),
            ),
          );
      }
      yield* setupTracker.stageStatus(threadId, "agent", "done");
      if (input.legacyBootstrap !== undefined) {
        const completion = (yield* Ref.get(scheduledLaunches)).legacyResults.get(input.commandId);
        if (completion !== undefined) yield* Deferred.succeed(completion, undefined);
      }
      yield* awaitAsyncSetup;
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
          // A cancelled setup leaves nothing behind. A failed one keeps a worktree
          // the thread recorded, so a retry reuses it, and removes one it never
          // recorded, which a retry would otherwise duplicate.
          if (
            input.legacyBootstrap === undefined &&
            tracked &&
            createdWorktreePath &&
            (cancelled || !workspaceRecorded)
          ) {
            if (setupTerminalId)
              yield* terminals
                .close({ threadId, terminalId: setupTerminalId, deleteHistory: true })
                .pipe(Effect.ignore);
            const removedPath = createdWorktreePath;
            // The thread forgets the worktree only once it is gone; a failed
            // removal leaves the directory for the user to clean up rather than
            // reusing a checkout that may be half written.
            yield* git
              .removeWorktree({ cwd: project.workspaceRoot, path: removedPath, force: true })
              .pipe(
                Effect.andThen(
                  threads
                    .dispatch({
                      type: "thread.metadata.update",
                      commandId: CommandId.make(`${input.commandId}:cancel-workspace`),
                      threadId,
                      worktreePath: null,
                      branch: null,
                    })
                    .pipe(Effect.ignore),
                ),
                Effect.catchCause((removeCause) =>
                  Effect.logWarning("Failed to remove an abandoned thread worktree", {
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

  const reservePreparation = (commandId: CommandId, legacy = false) =>
    Effect.gen(function* () {
      const completion = legacy ? yield* Deferred.make<void, ThreadLaunchError>() : undefined;
      return yield* Ref.modify(scheduledLaunches, (state) => {
        if (state.commands.has(commandId)) return [false, state] as const;
        const commands = new Set(state.commands);
        commands.add(commandId);
        const legacyResults = new Map(state.legacyResults);
        if (completion !== undefined) legacyResults.set(commandId, completion);
        return [true, { commands, legacyResults }] as const;
      });
    });
  const releasePreparation = (commandId: CommandId) =>
    Ref.update(scheduledLaunches, (state) => {
      const commands = new Set(state.commands);
      commands.delete(commandId);
      const legacyResults = new Map(state.legacyResults);
      legacyResults.delete(commandId);
      return { commands, legacyResults };
    });

  const schedulePreparation = Effect.fn("ThreadLaunchService.schedulePreparation")(function* (
    input: PreparationInput,
    threadId: ThreadId,
    runId: RunId | null,
  ) {
    const completion = (yield* Ref.get(scheduledLaunches)).legacyResults.get(input.commandId);
    let failureDeleted = false;
    const prepare = prepareInBackground(input, threadId, runId).pipe(
      Effect.onError((cause) => {
        const failure = Cause.squash(cause);
        const releaseRejected =
          input.legacyBootstrap !== undefined &&
          isThreadLaunchError(failure) &&
          failure.operation === "release-run";
        if (releaseRejected) return Effect.void;
        return failPreparedRun(
          input,
          threadId,
          runId,
          Cause.hasInterruptsOnly(cause) ? "Worktree setup cancelled." : failure,
        ).pipe(
          Effect.andThen(() =>
            input.legacyBootstrap === undefined || runId === null
              ? Effect.void
              : deleteLegacyWorkspaceFailureShell(input, threadId, runId).pipe(
                  Effect.tap((deleted) =>
                    Effect.sync(() => {
                      failureDeleted = deleted;
                    }),
                  ),
                  Effect.catchCause((cleanupCause) =>
                    Effect.logWarning("Workspace-failure D was not qualified", {
                      commandId: input.commandId,
                      threadId,
                      cleanupCause,
                    }),
                  ),
                ),
          ),
        );
      }),
      Effect.mapError((failure) =>
        failureDeleted
          ? new ThreadLaunchError({
              operation: failure.operation,
              commandId: failure.commandId,
              projectId: failure.projectId,
              ...(failure.threadId === undefined ? {} : { threadId: failure.threadId }),
              cause: failure.cause,
              bootstrapThreadDisposition: "deleted",
            })
          : failure,
      ),
    );
    const preparation =
      completion === undefined
        ? prepare.pipe(Effect.exit, Effect.ensuring(releasePreparation(input.commandId)))
        : Effect.uninterruptibleMask((restore) =>
            restore(prepare).pipe(
              Effect.exit,
              Effect.flatMap((result) =>
                Effect.gen(function* () {
                  if (yield* Deferred.isDone(completion)) return;
                  if (Exit.isSuccess(result) || !Cause.hasInterruptsOnly(result.cause)) {
                    yield* Deferred.done(completion, result);
                    return;
                  }
                  const cancellation = Effect.gen(function* () {
                    const policy = input.legacyBootstrap;
                    const unresolved = () =>
                      mapError(
                        input,
                        "read-receipt",
                        threadId,
                      )(
                        "Preparation cancellation has no exact failed-run readback; preserve its unresolved state.",
                      );
                    if (
                      policy === undefined ||
                      runId === null ||
                      policy.createCommandId !== input.commandId
                    )
                      return yield* unresolved();
                    const receivingPolicy = { ...policy, runId };
                    const collect = (commandId: CommandId) =>
                      eventSink.readByCommandId({ commandId }).pipe(
                        Stream.runCollect,
                        Effect.map((events) => Array.from(events)),
                        Effect.mapError(mapError(input, "read-receipt", threadId)),
                      );
                    const c = yield* readReceipt(input, policy.releaseCommandId);
                    const projectC = yield* receipts
                      .getProjectByCommandId(policy.releaseCommandId)
                      .pipe(Effect.mapError(mapError(input, "read-receipt", threadId)));
                    if (
                      Option.isSome(c) ||
                      Option.isSome(projectC) ||
                      (yield* collect(policy.releaseCommandId)).length !== 0
                    )
                      return yield* unresolved();
                    const failureId = CommandId.make(`${input.commandId}:fail`);
                    const failure = yield* readReceipt(input, failureId);
                    const failed = yield* collect(failureId);
                    const projection = yield* threads
                      .getThreadProjection(threadId)
                      .pipe(Effect.mapError(mapError(input, "read-receipt", threadId)));
                    const run = projection.runs.find((entry) => entry.id === runId);
                    const birth = legacyBootstrapBirth({
                      policy: receivingPolicy,
                      claimEvents: yield* collect(policy.createCommandId),
                      birthEvents: yield* collect(policy.birthCommandId),
                    });
                    const claim = yield* readReceipt(input, policy.createCommandId);
                    const message = yield* readReceipt(input, policy.birthCommandId);
                    const failures = failed.filter((entry) => entry.event.type === "run.updated");
                    const event = failures[0];
                    const errors = failed.filter(
                      (entry) =>
                        entry.event.type === "turn-item.updated" &&
                        entry.event.payload.type === "error",
                    );
                    const errorEvent = errors[0];
                    const errorItem =
                      errorEvent?.event.type === "turn-item.updated" &&
                      errorEvent.event.payload.type === "error"
                        ? errorEvent.event.payload
                        : undefined;
                    const currentError = projection.turnItems.find(
                      (item) => item.id === errorItem?.id,
                    );
                    if (
                      birth.type !== "valid" ||
                      Option.isNone(claim) ||
                      Option.isNone(message) ||
                      claim.value.status !== "accepted" ||
                      message.value.status !== "accepted" ||
                      claim.value.commandType !== "thread.create" ||
                      message.value.commandType !== "message.dispatch" ||
                      claim.value.threadId !== threadId ||
                      message.value.threadId !== threadId ||
                      Option.isNone(failure) ||
                      failure.value.status !== "accepted" ||
                      failure.value.commandType !== "prepared-run.fail" ||
                      failure.value.threadId !== threadId ||
                      failures.length !== 1 ||
                      errors.length !== 1 ||
                      failed.some(
                        (entry) =>
                          entry.commandId !== failureId ||
                          entry.event.threadId !== threadId ||
                          entry.event.runId !== runId ||
                          entry.sequence > failure.value.resultSequence,
                      ) ||
                      event?.commandId !== failureId ||
                      event.event.type !== "run.updated" ||
                      event.event.threadId !== threadId ||
                      event.event.runId !== runId ||
                      event.event.payload.id !== runId ||
                      event.sequence !== failure.value.resultSequence ||
                      event.event.payload.status !== "failed" ||
                      errorItem?.threadId !== threadId ||
                      errorItem.runId !== runId ||
                      errorItem.status !== "failed" ||
                      errorItem.failure.message !== failureDetail("Worktree setup cancelled.") ||
                      errorItem.failure.code !== "workspace_preparation_failed" ||
                      currentError?.type !== "error" ||
                      canonicalLegacyPayload(currentError) !== canonicalLegacyPayload(errorItem) ||
                      run?.status !== "failed" ||
                      run.startedAt !== null ||
                      run.legacyBootstrap === undefined ||
                      !sameLegacyBootstrapPolicy(run.legacyBootstrap, receivingPolicy) ||
                      event.event.payload.legacyBootstrap === undefined ||
                      !sameLegacyBootstrapPolicy(
                        event.event.payload.legacyBootstrap,
                        receivingPolicy,
                      ) ||
                      canonicalLegacyPayload(event.event.payload.legacyPreparation) !==
                        canonicalLegacyPayload(run.legacyPreparation) ||
                      projection.thread.id !== threadId ||
                      projection.thread.projectId !== input.projectId ||
                      projection.thread.deletedAt !== null ||
                      run.userMessageId !== policy.messageId ||
                      run.legacyPreparation === undefined ||
                      run.legacyPreparation.claimEventId !== birth.claimEventId ||
                      run.legacyPreparation.claimReceiptSequence !== claim.value.resultSequence ||
                      run.legacyPreparation.birthEventId !== birth.birthEventId ||
                      run.legacyPreparation.birthReceiptSequence !== message.value.resultSequence
                    )
                      return yield* unresolved();
                    return mapError(
                      input,
                      "run-setup-script",
                      threadId,
                    )("Worktree setup cancelled.");
                  });
                  const error = yield* cancellation.pipe(
                    Effect.catchCause((cause) => {
                      const failure = Cause.squash(cause);
                      return Effect.succeed(
                        isThreadLaunchError(failure)
                          ? failure
                          : mapError(input, "read-receipt", threadId)(failure),
                      );
                    }),
                  );
                  yield* Deferred.fail(completion, error);
                }),
              ),
              Effect.ensuring(releasePreparation(input.commandId)),
            ),
          );
    yield* preparation.pipe(Effect.forkIn(preparationScope));
  });

  const launch: ThreadLaunchService["Service"]["launch"] = Effect.fn("ThreadLaunchService.launch")(
    function* (input) {
      if (input.preparationReleaseCommandId !== undefined || input.legacyBootstrap !== undefined) {
        const policy = input.legacyBootstrap;
        if (
          policy === undefined ||
          input.preparationReleaseCommandId !== policy.releaseCommandId ||
          input.commandId !== policy.createCommandId ||
          input.threadId !== policy.threadId ||
          input.projectId !== policy.projectId ||
          input.initialMessage?.messageId !== policy.messageId ||
          policy.ownsNewThread === (input.reuseExistingThread === true)
        )
          return yield* mapError(
            input,
            "create-thread",
            input.threadId,
          )("Legacy launch does not match its immutable preparation policy.");
      }
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

      if (input.legacyBootstrap !== undefined)
        yield* awaitThreadCreationCleanup(outbox, input.legacyBootstrap.threadId).pipe(
          Effect.mapError(mapError(input, "create-thread", input.threadId)),
        );
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
          input.legacyBootstrap === undefined &&
          input.workspaceStrategy.type === "root" &&
          Option.isNone(launchReceipt)
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
        const initialBranch = workspaceStrategy.branch ?? null;
        const initialWorktreePath =
          workspaceStrategy.type === "existing_worktree" ? workspaceStrategy.worktreePath : null;
        const claimDispatch =
          input.reuseExistingThread === true
            ? threads.dispatch({
                type: "thread.metadata.update",
                commandId: input.commandId,
                threadId: candidateThreadId,
                expectedEmpty: true,
                ...(input.legacyBootstrap === undefined
                  ? {}
                  : { legacyBootstrap: input.legacyBootstrap }),
              })
            : threads.dispatch({
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
                ...(input.legacyBootstrap === undefined
                  ? {}
                  : { legacyBootstrap: input.legacyBootstrap }),
              });
        const claimed = yield* claimDispatch.pipe(
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
              ...(input.initialMessage.titleSeed === undefined
                ? input.generateTitle === true
                  ? { titleSeed: input.title }
                  : {}
                : { titleSeed: input.initialMessage.titleSeed }),
              ...(input.initialMessage.sourcePlanRef === undefined
                ? {}
                : { sourcePlanRef: input.initialMessage.sourcePlanRef }),
              modelSelection: input.modelSelection,
              dispatchMode: {
                type: "defer_start",
                workspaceStrategy,
                ...(input.runSetupScript === undefined
                  ? {}
                  : { runSetupScript: input.runSetupScript }),
              },
              createdBy: input.createdBy,
              creationSource: input.creationSource,
              ...(input.legacyBootstrap === undefined
                ? {}
                : { legacyBootstrap: input.legacyBootstrap }),
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
        const persistedPolicy = projection.runs.find((run) => run.id === runId)?.legacyBootstrap;
        if (
          input.legacyBootstrap !== undefined &&
          (persistedPolicy === undefined ||
            !sameLegacyBootstrapPolicy(persistedPolicy, {
              ...input.legacyBootstrap,
              runId: runId!,
            }))
        )
          return yield* mapError(
            input,
            "dispatch-message",
            threadId,
          )("Legacy preparation policy changed after birth.");
        let legacyCompletion: Deferred.Deferred<void, ThreadLaunchError> | undefined;
        if (persistedPolicy !== undefined && messageWasAlreadyAccepted) {
          const active = (yield* Ref.get(scheduledLaunches)).legacyResults.get(input.commandId);
          const releaseReceipt = yield* readReceipt(input, persistedPolicy.releaseCommandId);
          if (Option.isSome(releaseReceipt) && releaseReceipt.value.status === "rejected")
            return yield* threads
              .dispatch({
                type: "prepared-run.release",
                commandId: persistedPolicy.releaseCommandId,
                threadId,
                runId: persistedPolicy.runId!,
                legacyBootstrap: persistedPolicy,
              })
              .pipe(
                Effect.catch((cause) =>
                  deleteLegacyGuardRejectedShell(
                    { ...input, legacyBootstrap: persistedPolicy },
                    threadId,
                    persistedPolicy.runId!,
                  ).pipe(
                    Effect.flatMap((deleted) =>
                      Effect.fail(
                        new ThreadLaunchError({
                          operation: "release-run",
                          commandId: input.commandId,
                          projectId: input.projectId,
                          threadId,
                          cause,
                          ...(deleted ? { bootstrapThreadDisposition: "deleted" as const } : {}),
                        }),
                      ),
                    ),
                  ),
                ),
                Effect.andThen(
                  mapError(
                    input,
                    "release-run",
                    threadId,
                  )("Rejected release unexpectedly returned acceptance."),
                ),
              );

          if (active === undefined && Option.isNone(releaseReceipt) && runIsPreparing) {
            for (const suffix of [":progress:worktree", ":progress:setup"] as const) {
              if (
                Option.isSome(
                  yield* readReceipt(input, CommandId.make(`${input.commandId}${suffix}`)),
                )
              )
                return yield* mapError(
                  input,
                  "provision-worktree",
                  threadId,
                )(
                  "Recorded preparation intent has no accepted release; its external outcome is unknown.",
                );
            }
          }
        }
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
          const ownsPreparation = yield* reservePreparation(
            input.commandId,
            persistedPolicy !== undefined,
          );
          legacyCompletion = (yield* Ref.get(scheduledLaunches)).legacyResults.get(input.commandId);
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
                    ...(persistedPolicy === undefined
                      ? {}
                      : {
                          legacyBootstrap: persistedPolicy,
                          preparationReleaseCommandId: persistedPolicy.releaseCommandId,
                        }),
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

        if (persistedPolicy !== undefined) {
          if (legacyCompletion !== undefined) yield* Deferred.await(legacyCompletion);
          const releaseReceipt = yield* readReceipt(input, persistedPolicy.releaseCommandId);
          if (Option.isNone(releaseReceipt) || releaseReceipt.value.status !== "accepted")
            return yield* mapError(
              input,
              "release-run",
              threadId,
            )(
              Option.isSome(releaseReceipt)
                ? (releaseReceipt.value.error ?? "Final release was rejected.")
                : "Preparation has no accepted release; preserve its unresolved state.",
            );
          const released = yield* threads
            .dispatch({
              type: "prepared-run.release",
              commandId: persistedPolicy.releaseCommandId,
              threadId,
              runId: persistedPolicy.runId!,
              legacyBootstrap: persistedPolicy,
            })
            .pipe(Effect.mapError(mapError(input, "release-run", threadId)));
          return {
            threadId,
            projection: yield* threads
              .getThreadProjection(threadId)
              .pipe(Effect.mapError(mapError(input, "read-receipt", threadId))),
            resumed: Option.isSome(launchReceipt) || messageWasAlreadyAccepted,
            legacyReleaseSequence: released.sequence,
          };
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
              },
            }),
      },
      input.threadId,
      run.id,
    );
  };

  return ThreadLaunchService.of({
    preflightLegacyBootstrap: (binding) =>
      preflight(binding).pipe(
        Effect.mapError(
          (cause) =>
            new ThreadLaunchError({
              operation: "provision-worktree",
              commandId: binding.policy.createCommandId,
              projectId: binding.policy.projectId,
              threadId: binding.policy.threadId,
              cause,
            }),
        ),
        Effect.forkIn(preparationScope),
        Effect.flatMap(Fiber.join),
      ),
    launch,
    retryPreparation,
  });
});

export const layer = Layer.effect(ThreadLaunchService, make);
