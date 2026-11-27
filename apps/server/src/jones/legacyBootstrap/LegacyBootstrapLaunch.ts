import * as SqlClient from "effect/sql/SqlClient";
import * as Storage from "./Storage.ts";
import * as LegacyCleanup from "./TerminalCleanupWorker.ts";
import * as WorkspaceProvenance from "../workspaceLifecycle/Provenance.ts";
import {
  LegacyOwnedTerminalControl,
  LegacyNoTerminalControl,
  LegacyDeletionProvenance,
  LegacyFailureDeletionProvenance,
  type LegacyFailureDeleteCommand,
  type LegacyGuardRejectionDeleteCommand,
  type LegacyPreparationUpdate,
  type LegacyPreparation,
} from "../../orchestration-v2/RecordedTypes.ts";
import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import { awaitThreadCreationCleanup } from "../../orchestration-v2/ThreadDeletion.ts";
import * as Deferred from "effect/Deferred";
import { makeLegacyPreflight } from "../../orchestration-v2/LegacyBootstrapPreflight.ts";
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
} from "../../orchestration-v2/LegacyBootstrap.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as Stream from "effect/Stream";
import * as WorktreeSetupTracker from "../../project/WorktreeSetupTracker.ts";
import * as ProjectCloneTracker from "../../project/ProjectCloneTracker.ts";
import * as TerminalManager from "../../terminal/Manager.ts";
import type { LegacyWorktreePreparationHooks } from "../../vcs/GitVcsDriver.ts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import {
  CommandId,
  EventId,
  TextGenerationError,
  type OrchestrationV2LegacyPreflightBinding,
  type RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";

import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";

import {
  buildTemporaryWorktreeBranchName,
  isTemporaryWorktreeBranch,
  flattenTemporaryWorktreeBranchName,
  resolveDefaultWorktreeBaseBranch,
  WORKTREE_BRANCH_PREFIX,
} from "@t3tools/shared/git";

import * as GitWorkflow from "../../git/GitWorkflowService.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "../../project/ProjectSetupScriptRunner.ts";
import * as ProviderRegistry from "../../provider/ProviderRegistry.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as TextGeneration from "../../textGeneration/TextGeneration.ts";
import * as CommandReceiptStore from "../../orchestration-v2/CommandReceiptStore.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import { randomUuidV4 } from "@t3tools/provider-core/server/randomUuid";
import {
  DispatchModeLimit,
  exceededDispatchModeLimit,
} from "../../orchestration-v2/DispatchModeLimit.ts";
import * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";

import { recordedLaunchMessage } from "../orchestration/deliveryEvidence.ts";
import { initialMessageCommandId } from "@t3tools/contracts/jones/deliveryEvidence";
import {
  ThreadLaunchError,
  type ThreadLaunchInput,
  type ThreadLaunchInitialMessage,
  type ThreadLaunchWorkspaceStrategy,
  type ThreadLaunchService,
} from "../../orchestration-v2/ThreadLaunchService.ts";

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
  readonly reusedWorktree?: { readonly baseRef: string | undefined };
};

// Only server-owned legacy bootstraps enter this retained protocol. Ordinary
// launches keep ThreadLaunchService's LaunchPreparation custody and behavior.
export const make = Effect.gen(function* () {
  const decodeEffectLegacyOwnedTerminalControl = Schema.decodeEffect(LegacyOwnedTerminalControl);
  const decodeEffectLegacyNoTerminalControl = Schema.decodeEffect(LegacyNoTerminalControl);
  const decodeUnknownEffectLegacyDeletionProvenance =
    Schema.decodeUnknownEffect(LegacyDeletionProvenance);
  const toEquivalenceLegacyOwnedTerminalControl = Schema.toEquivalence(LegacyOwnedTerminalControl);
  const toEquivalenceLegacyDeletionProvenance = Schema.toEquivalence(LegacyDeletionProvenance);
  const decodeEffectLegacyFailureDeletionProvenance = Schema.decodeEffect(
    LegacyFailureDeletionProvenance,
  );
  const toEquivalenceLegacyNoTerminalControl = Schema.toEquivalence(LegacyNoTerminalControl);
  const isThreadLaunchError = Schema.is(ThreadLaunchError);
  const isSetupOperationError = Schema.is(
    ProjectSetupScriptRunner.ProjectSetupScriptOperationError,
  );
  const isTerminalInputValidationError = Schema.is(
    TerminalManager.LegacyTerminalInputValidationError,
  );
  const captureProvenance = yield* WorkspaceProvenance.optionalCapture;
  const projects = yield* ProjectService.ProjectService;
  const setupTracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
  const cloneTracker = yield* ProjectCloneTracker.ProjectCloneTracker;
  const git = yield* GitWorkflow.GitWorkflowService;
  const setupScripts = yield* ProjectSetupScriptRunner.ProjectSetupScriptRunner;
  const providerRegistry = yield* ProviderRegistry.ProviderRegistry;
  const serverSettings = yield* ServerSettings.ServerSettingsService;
  const textGeneration = yield* TextGeneration.TextGeneration;
  const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
  const privateStorage = Storage.make(yield* SqlClient.SqlClient);
  const legacyCleanup = yield* Effect.serviceOption(LegacyCleanup.TerminalCleanupWorker);
  const eventSink = yield* EventSink.EventSinkV2;
  const ids = yield* IdAllocator.IdAllocatorV2;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const outbox = yield* EffectOutbox.EffectOutboxV2;
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

  const failureDetail = (error: unknown): string =>
    isThreadLaunchError(error)
      ? `Workspace preparation failed during ${error.operation.replaceAll("-", " ")}: ${error.cause instanceof Error ? error.cause.message : String(error.cause)}`
      : `Workspace preparation failed: ${error instanceof Error ? error.message : String(error)}`;

  const readReceipt = (
    input: PreparationInput & { readonly threadId?: ThreadId | undefined },
    commandId: CommandId,
  ) =>
    receipts
      .getByCommandId(commandId)
      .pipe(Effect.mapError(mapError(input, "read-receipt", input.threadId)));

  const readPrivateDeleteReceipt = Effect.fn("LegacyBootstrapLaunch.readPrivateDeleteReceipt")(
    function* (input: PreparationInput, threadId: ThreadId, commandId: CommandId) {
      const found = yield* privateStorage
        .readLegacyReceipt(commandId)
        .pipe(Effect.mapError(mapError(input, "read-receipt", threadId)));
      if (found === undefined) return Option.none<CommandReceiptStore.CommandReceiptV2>();
      if (found.domain !== "legacy-delete")
        return yield* mapError(
          input,
          "read-receipt",
          threadId,
        )("Private deletion receipt has a different owner domain.");
      return Option.some(found.receipt);
    },
  );

  const deleteLegacyGuardRejectedShell = Effect.fn(
    "ThreadLaunchService.deleteLegacyGuardRejectedShell",
  )(function* (input: PreparationInput, threadId: ThreadId, runId: RunId) {
    const policy = input.legacyBootstrap;
    if (policy === undefined || !policy.ownsNewThread) return false;
    const projection = yield* threads
      .getLegacyBootstrapRecords(threadId)
      .pipe(Effect.mapError(mapError(input, "release-run", threadId)));
    const run = projection.runs.find((run) => run.id === runId);
    const preparation = run?.legacyPreparation;
    const decision = run?.legacyReleaseDecision;
    const collect = (commandId: CommandId) =>
      eventSink.readLifecycleByCommandId({ commandId }).pipe(
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
        legacyOwnedControl: yield* decodeEffectLegacyOwnedTerminalControl({
          ...correlation,
          terminalId: preparation.setup.definition.terminalId,
          generation: preparation.setup.definition.generation,
        }).pipe(Effect.mapError(mapError(input, "release-run", threadId))),
      };
    else if (preparation.setup.status === "no_script" || preparation.setup.status === "opted_out")
      command = {
        ...identity,
        legacyNoControl: yield* decodeEffectLegacyNoTerminalControl({
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
    const deletion = yield* readPrivateDeleteReceipt(input, threadId, command.commandId);
    const deleted = yield* collect(command.commandId);
    if (Option.isNone(deletion)) {
      if (dispatched._tag === "Failure") return yield* dispatched.failure;
      return yield* mapError(
        input,
        "release-run",
        threadId,
      )("Guard deletion receipt is absent; preserve exact D identity.");
    }
    const expected = yield* decodeUnknownEffectLegacyDeletionProvenance({
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
    const privateCleanup = yield* privateStorage
      .readTerminalCleanup("effect:" + command.commandId + ":terminal.cleanup")
      .pipe(Effect.mapError(mapError(input, "release-run", threadId)));
    const cleanupMatches =
      cleanup.length === 0 &&
      (command.legacyNoControl !== undefined
        ? privateCleanup === undefined
        : privateCleanup !== undefined &&
          privateCleanup.commandId === command.commandId &&
          privateCleanup.threadId === threadId &&
          toEquivalenceLegacyOwnedTerminalControl(
            privateCleanup.control,
            command.legacyOwnedControl,
          ));
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
      !toEquivalenceLegacyDeletionProvenance(storedDecision.deletion, expected) ||
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
    if (command.legacyOwnedControl !== undefined) {
      if (Option.isNone(legacyCleanup))
        return yield* mapError(
          input,
          "release-run",
          threadId,
        )("Private terminal cleanup owner is unavailable.");
      yield* legacyCleanup.value.wake;
      while (true) {
        const settled = yield* privateStorage
          .readTerminalCleanup("effect:" + command.commandId + ":terminal.cleanup")
          .pipe(Effect.mapError(mapError(input, "release-run", threadId)));
        if (
          settled === undefined ||
          settled.commandId !== command.commandId ||
          settled.threadId !== threadId ||
          !toEquivalenceLegacyOwnedTerminalControl(settled.control, command.legacyOwnedControl)
        )
          return yield* mapError(
            input,
            "release-run",
            threadId,
          )("Private terminal cleanup identity changed.");
        if (settled.state === "succeeded" && settled.outcome === "closed") break;
        if (settled.state === "unknown")
          return yield* mapError(
            input,
            "release-run",
            threadId,
          )("Private terminal cleanup has an unknown physical outcome.");
        yield* Effect.sleep("100 millis");
      }
    }
    return true;
  });

  const deleteLegacyWorkspaceFailureShell = Effect.fn(
    "ThreadLaunchService.deleteLegacyWorkspaceFailureShell",
  )(function* (input: PreparationInput, threadId: ThreadId, runId: RunId) {
    const policy = input.legacyBootstrap;
    if (policy === undefined || !policy.ownsNewThread) return false;
    const projection = yield* threads
      .getLegacyBootstrapRecords(threadId)
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
    const control = yield* decodeEffectLegacyNoTerminalControl({
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
    const receipt = yield* readPrivateDeleteReceipt(input, threadId, command.commandId);
    const events = Array.from(
      yield* eventSink
        .readLifecycleByCommandId({ commandId: command.commandId })
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
        : yield* decodeEffectLegacyFailureDeletionProvenance(deletion).pipe(
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
      !toEquivalenceLegacyNoTerminalControl(validated.provenance.control, control) ||
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
        .getLegacyBootstrapRecords(threadId, { runIds: [runId] })
        .pipe(Effect.mapError(mapError(input, "read-receipt", threadId)));
      let preparation = current.runs.find((run) => run.id === runId)?.legacyPreparation;
      if (preparation === undefined)
        return yield* mapError(
          input,
          "read-receipt",
          threadId,
        )("Legacy preparation has no authenticated ledger.");
      const bound = preparation;
      const persist = Effect.fn("ThreadLaunchService.persist")(function* (
        commandId: CommandId,
        update: LegacyPreparationUpdate,
      ) {
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
            .readLifecycleByCommandId({ commandId })
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
      const intent = Effect.fn("ThreadLaunchService.intent")(function* (
        effect: LegacyPreparation["steps"][number]["effect"],
      ) {
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
      const outcome = Effect.fn("ThreadLaunchService.outcome")(function* (
        kind: LegacyPreparation["steps"][number]["effect"]["kind"],
        state: LegacyPreparation["steps"][number]["state"],
        evidence: NonNullable<LegacyPreparation["steps"][number]["evidence"]>,
      ) {
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
    const hooks: import("../../vcs/GitVcsDriver.ts").LegacyBranchRenameHooks = {
      before: add.effect.input.before,
      claim,
      beforeEffect: (step) => journal.intent({ kind: "branch.rename", input: step }),
      afterEffect: Effect.fn("ThreadLaunchService.afterEffect")(
        function* (_step, result, material) {
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
        },
      ),
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
      neverInvoked: Effect.fn("ThreadLaunchService.neverInvoked")(function* (step, reason) {
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
      beforeEffect: Effect.fn("ThreadLaunchService.beforeEffect")(function* (step) {
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
      afterEffect: Effect.fn("ThreadLaunchService.afterEffect")(function* (step, result, claim) {
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
    lifetime: Scope.Scope,
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
      lifetime,
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
      beforeSpawn: Effect.fn("ThreadLaunchService.beforeSpawn")(function* (definition) {
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
      neverInvoked: Effect.fn("ThreadLaunchService.neverInvoked")(function* (proof) {
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
      beforeWrite: Effect.fn("ThreadLaunchService.beforeWrite")(function* () {
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
      afterCompletion: Effect.fn("ThreadLaunchService.afterCompletion")(function* (completion) {
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
      .getLegacyBootstrapRecords(threadId)
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
    observeExistingSetup = false,
  ) {
    const lifetime = yield* Effect.scope;
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
        .getLegacyBootstrapRecords(threadId, { runIds: [runId] })
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
          eventSink.readLifecycleByCommandId({ commandId }).pipe(
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
    let branchRename: Fiber.Fiber<unknown> | null = null;
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
    yield* Effect.gen(function* () {
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
                duration: "20 seconds",
                orElse: () =>
                  Effect.fail(
                    new TextGenerationError({
                      operation: "generateBranchName",
                      detail: "Timed out after 20 seconds.",
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
      let branch: string | null;
      if (input.workspaceStrategy.type === "worktree" && requestedBranch === undefined) {
        const uuid = yield* randomUuidV4;
        branch = buildTemporaryWorktreeBranchName(() => uuid.replaceAll("-", ""));
      } else {
        branch = requestedBranch ?? null;
      }
      let provenanceBaseRef: string | null = null;
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
        // Receiving shared preparation: an omitted base starts from the default, else checked-out, branch.
        let baseRef = input.workspaceStrategy.baseRef;
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
          yield* setupTracker.update(threadId, (snapshot) => ({
            ...snapshot,
            baseRef: automaticBase,
          }));
        }
        let startRef = baseRef;
        let baseRefName = baseRef;
        const remote =
          input.workspaceStrategy.startFromOrigin === true
            ? yield* git
                .resolveWorktreeBaseRemote({
                  cwd: project.workspaceRoot,
                  refName: baseRef,
                })
                .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)))
            : null;
        const shouldFetch = remote !== null;
        yield* setupTracker.stageStatus(threadId, "fetch", shouldFetch ? "running" : "skipped");
        if (remote !== null) {
          const remoteBase = yield* git
            .fetchWorktreeBaseCommit({
              cwd: project.workspaceRoot,
              refName: baseRef,
              remote,
            })
            .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
          if (remoteBase !== null) {
            startRef = remoteBase.commitSha;
            baseRefName = `refs/remotes/${remoteBase.remoteRefName}`;
          }
        }
        if (shouldFetch) yield* setupTracker.stageStatus(threadId, "fetch", "done");
        if (
          branch !== null &&
          isTemporaryWorktreeBranch(branch) &&
          (yield* git
            .hasCommit({
              cwd: project.workspaceRoot,
              refName: `refs/heads/${WORKTREE_BRANCH_PREFIX}`,
            })
            .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId))))
        ) {
          branch = flattenTemporaryWorktreeBranchName(branch);
        }
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
              baseRefName,
              path: null,
            },
            {
              ...(legacyPreparation === undefined ? {} : { legacyPreparation }),
              progress: {
                onCheckoutProgress: (progress) =>
                  setupTracker.stage(threadId, "checkout", { percent: progress.percent }),
              },
            },
          )
          .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
        provenanceBaseRef = baseRefName;
        worktreePath = worktree.worktree.path;
        branch = worktree.worktree.refName;
        yield* setupTracker.update(threadId, (snapshot) => ({ ...snapshot, worktreePath, branch }));
        yield* setupTracker.stageStatus(threadId, "checkout", "done");
      }

      // Do not rewrite reused worktrees; that could undo the original branch rename.
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
      yield* captureProvenance({
        threadId,
        reason: input.workspaceStrategy.type === "worktree" ? "launch" : "reference",
        createdBy: "system",
        baseRef: provenanceBaseRef,
        baseIsCurrentHead: provenanceBaseRef !== null && reused === undefined,
        ...(runId === null ? {} : { runId }),
      });
      if (
        reused === undefined &&
        worktreePath !== null &&
        branch !== null &&
        initialMessage !== undefined &&
        isTemporaryWorktreeBranch(branch)
      ) {
        const oldBranch = branch;
        const worktreeCwd = worktreePath;
        branchRename = yield* generateBranchNameFor(worktreeCwd, initialMessage).pipe(
          Effect.flatMap(({ branch: newBranch, exactName }) =>
            Effect.gen(function* () {
              const legacyPreparation =
                input.legacyBootstrap === undefined || runId === null
                  ? undefined
                  : yield* legacyRenameHooks(input, threadId, runId);
              return yield* git
                .renameBranch({
                  cwd: worktreeCwd,
                  oldBranch,
                  newBranch,
                  ...(exactName ? { exactName: true } : {}),
                  ...(legacyPreparation === undefined ? {} : { legacyPreparation }),
                })
                .pipe(Effect.uninterruptible);
            }),
          ),
          Effect.flatMap((renamed) =>
            Effect.gen(function* () {
              yield* threads
                .dispatch({
                  type: "thread.metadata.update",
                  commandId: CommandId.make(`${input.commandId}:branch-rename`),
                  threadId,
                  branch: renamed.branch,
                  worktreePath: worktreeCwd,
                  expectedBranch: oldBranch,
                  expectedWorktreePath: worktreeCwd,
                })
                .pipe(
                  Effect.catchCause((cause) =>
                    Effect.logWarning("Parent thread worktree branch update failed", {
                      threadId,
                      cause,
                    }),
                  ),
                );
              // Update parents first so children inherit names; reread each pass for newly updated descendants.
              const visited = new Set([threadId]);
              while (true) {
                const snapshot = yield* threads.getShellSnapshot({ worktreePath: worktreeCwd });
                const matchingIds = [...snapshot.threads, ...snapshot.archivedThreads]
                  .filter((thread) => thread.branch === oldBranch)
                  .map((thread) => thread.id);
                const sharing = matchingIds.filter((id) => !visited.has(id));
                if (sharing.length === 0) break;
                for (const id of sharing) {
                  visited.add(id);
                  yield* threads
                    .dispatch({
                      type: "thread.metadata.update",
                      commandId: CommandId.make(`${input.commandId}:branch-rename:${id}`),
                      threadId: id,
                      branch: renamed.branch,
                      expectedBranch: oldBranch,
                      expectedWorktreePath: worktreeCwd,
                    })
                    .pipe(
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
            }),
          ),
          Effect.forkIn(preparationScope),
        );
      }

      if (runId !== null && worktreePath !== null) {
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
          ? yield* legacySetupHooks(input, threadId, runId, lifetime)
          : undefined;
      const cwd = worktreePath ?? project.workspaceRoot;
      const setup = yield* (
        input.runSetupScript === false
          ? Effect.succeed({ status: "no-script" as const })
          : setupScripts.runForThread({
              threadId,
              projectId: input.projectId,
              projectCwd: project.workspaceRoot,
              worktreePath: cwd,
              ...(legacyPreparation === undefined ? {} : { legacyPreparation }),
              // Jones: an app-owned delegated child's existing-worktree setup is
              // observed without the setup tracker, so release waits for a
              // synchronous script and Stop can close the script's terminal.
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
              return yield* mapError(input, "run-setup-script", threadId)(cause);
            const proof = cause.cause;
            const current = yield* threads
              .getLegacyBootstrapRecords(threadId, { runIds: [runId] })
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
          .getLegacyBootstrapRecords(threadId, { runIds: [runId] })
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
              .readLifecycleByCommandId({ commandId })
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
        yield* setup.writeCompletion.pipe(
          Effect.mapError(mapError(input, "run-setup-script", threadId)),
        );
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
      if (branchRename !== null) yield* Fiber.join(branchRename);
      yield* setupTracker.markUncancellable(threadId);
      yield* setupTracker.stageStatus(threadId, "agent", "running");
      if (runId !== null) {
        if (input.legacyBootstrap !== undefined) {
          const current = yield* threads
            .getLegacyBootstrapRecords(threadId, { runIds: [runId] })
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
                  .readLifecycleByCommandId({ commandId })
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
            Effect.uninterruptible,
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
          if (branchRename !== null) yield* Fiber.interrupt(branchRename);
          yield* setupTracker.finish(
            threadId,
            cancelled ? "cancelled" : "failed",
            cancelled ? null : failureDetail(Cause.squash(cause)),
          );
        }),
      ),
    );
  }, Effect.scoped);

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

  const reservePreparation = Effect.fn("ThreadLaunchService.reservePreparation")(function* (
    commandId: CommandId,
    legacy = false,
  ) {
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

  // Jones: true when the thread is a subagent child whose parent records it as
  // an app-owned delegated task (delegate_task), not a native or forked child.
  const isAppOwnedDelegatedChild = Effect.fn("ThreadLaunchService.isAppOwnedDelegatedChild")(
    (input: PreparationInput, threadId: ThreadId) =>
      Effect.gen(function* () {
        const { thread } = yield* threads.getThreadRecords(threadId, []);
        const parentThreadId = thread.lineage.parentThreadId;
        if (thread.lineage.relationshipToParent !== "subagent" || parentThreadId === null)
          return false;
        const { subagents } = yield* threads.getThreadRecords(parentThreadId, ["subagents"]);
        return subagents.some(
          (task) => task.origin === "app_owned" && task.childThreadId === threadId,
        );
      }).pipe(Effect.mapError(mapError(input, "update-thread", threadId))),
  );

  const schedulePreparation = Effect.fn("ThreadLaunchService.schedulePreparation")(function* (
    input: PreparationInput,
    threadId: ThreadId,
    runId: RunId | null,
  ) {
    const prepareWithInterruption = Effect.gen(function* () {
      // Jones: an app-owned delegated child's existing worktree has its setup
      // observed and is also prepared under Stop, so task_cancel closes its
      // setup terminal. The checkout is not removed: this launch did not
      // create it. Other existing-worktree and root launches are unchanged.
      const appOwnedExisting =
        runId !== null &&
        input.workspaceStrategy.type === "existing_worktree" &&
        input.reusedWorktree === undefined &&
        (yield* isAppOwnedDelegatedChild(input, threadId));
      const prepare = prepareInBackground(input, threadId, runId, appOwnedExisting);
      if (runId === null) return yield* prepare;
      if (
        input.workspaceStrategy.type !== "worktree" &&
        input.reusedWorktree === undefined &&
        !appOwnedExisting
      )
        return yield* prepare;
      const afterSequence = yield* threads
        .getThreadEventSequence(threadId)
        .pipe(Effect.mapError(mapError(input, "update-thread", threadId)));
      const projection = yield* threads
        .getLegacyBootstrapRecords(threadId)
        .pipe(Effect.mapError(mapError(input, "update-thread", threadId)));
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
          Effect.mapError(mapError(input, "update-thread", threadId)),
          Effect.flatMap((event) => (Option.isSome(event) ? Effect.interrupt : Effect.never)),
        );
      yield* Effect.raceFirst(prepare, interruption);
    });
    const completion = (yield* Ref.get(scheduledLaunches)).legacyResults.get(input.commandId);
    let failureDeleted = false;
    const prepare = prepareWithInterruption.pipe(
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
                      eventSink.readLifecycleByCommandId({ commandId }).pipe(
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
                    const publicProjection = yield* threads
                      .getThreadProjection(threadId)
                      .pipe(Effect.mapError(mapError(input, "read-receipt", threadId)));
                    const retained = yield* threads
                      .getLegacyBootstrapRecords(threadId)
                      .pipe(Effect.mapError(mapError(input, "read-receipt", threadId)));
                    const projection = { ...publicProjection, ...retained };
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
    function* (request) {
      const input = request;
      if (input.legacyBootstrap === undefined || input.initialMessage === undefined)
        return yield* mapError(
          input,
          "create-thread",
          input.threadId,
        )("Legacy bootstrap requires its policy and initial message.");

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
      yield* threads.assertProviderInstanceEnabled({
        projectId: input.projectId,
        commandId: input.commandId,
        instanceId: input.modelSelection.instanceId,
      });
      return yield* Effect.gen(function* () {
        // Recover ids from accepted launch receipts before allocating; retries have no client id and new ids collide with receipts.
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

        if (input.reuseExistingThread === true && Option.isNone(launchReceipt)) {
          yield* validateReusableThread(input, candidateThreadId);
        }

        const workspaceStrategy = input.workspaceStrategy;
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
                ...(input.linkOrigin === undefined ? {} : { linkOrigin: input.linkOrigin }),
                ...(input.delegatedFrom === undefined
                  ? {}
                  : { delegatedFrom: input.delegatedFrom }),
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
          const messageCommandId = initialMessageCommandId(input.commandId);
          const messageReceipt = yield* readReceipt(input, messageCommandId);
          const messageEvents =
            Option.isSome(messageReceipt) && messageReceipt.value.status === "accepted"
              ? yield* eventSink
                  .readLifecycleByCommandId({ commandId: messageCommandId })
                  .pipe(
                    Stream.runCollect,
                    Effect.mapError(mapError(input, "read-receipt", threadId)),
                  )
              : [];
          const recordedMessage = Option.isSome(messageReceipt)
            ? recordedLaunchMessage(input, messageReceipt.value, messageEvents, threadId)
            : undefined;
          if (Option.isSome(messageReceipt) && recordedMessage === undefined) {
            const receipt = messageReceipt.value;
            return yield* mapError(
              input,
              "dispatch-message",
              threadId,
            )(
              receipt.status === "rejected" &&
                receipt.commandId === messageCommandId &&
                receipt.commandType === "message.dispatch" &&
                receipt.threadId === threadId
                ? new Orchestrator.OrchestratorCommandPreviouslyRejectedError({
                    commandId: receipt.commandId,
                    commandType: receipt.commandType,
                    detail: receipt.error ?? "Previously rejected.",
                  })
                : "The initial message has no matching accepted immutable receipt; preserve its original identity.",
            );
          }
          messageWasAlreadyAccepted = recordedMessage !== undefined;
          const messageId =
            recordedMessage?.message.id ??
            input.initialMessage.messageId ??
            (yield* ids.allocate
              .message({ threadId, ordinal: 1 })
              .pipe(Effect.mapError(mapError(input, "dispatch-message", threadId))));
          const inheritedDispatchLimit = yield* DispatchModeLimit;
          const dispatched =
            Option.isSome(messageReceipt) && recordedMessage !== undefined
              ? {
                  sequence: messageReceipt.value.resultSequence,
                  storedEvents: messageEvents,
                  replayed: true,
                }
              : yield* threads
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
                    ...(input.initialMessage.context
                      ? { context: input.initialMessage.context }
                      : {}),
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
                  .pipe(
                    // An agent's launch can land on a thread that a replay, or a concurrent
                    // launch with the same key, created with broader modes. The orchestrator
                    // refuses the message under the thread's lock; the user's own clients
                    // launch into their own drafts, whatever modes those hold.
                    request.createdBy === "agent"
                      ? Effect.provideService(DispatchModeLimit, {
                          runtimeMode:
                            inheritedDispatchLimit !== undefined &&
                            exceededDispatchModeLimit(inheritedDispatchLimit, {
                              runtimeMode: request.runtimeMode,
                              interactionMode: inheritedDispatchLimit.interactionMode,
                            }) === "runtime"
                              ? inheritedDispatchLimit.runtimeMode
                              : request.runtimeMode,
                          interactionMode:
                            inheritedDispatchLimit?.interactionMode === "plan"
                              ? "plan"
                              : request.interactionMode,
                          ...(inheritedDispatchLimit?.refused === undefined
                            ? {}
                            : { refused: inheritedDispatchLimit.refused }),
                        })
                      : (effect) => effect,
                    Effect.mapError(mapError(input, "dispatch-message", threadId)),
                  );
          // A concurrent retry can read no receipt and still replay the dispatch.
          messageWasAlreadyAccepted ||= dispatched.replayed === true;
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
          .getLegacyBootstrapRecords(threadId)
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
                  : yield* threads.getLegacyBootstrapRecords(threadId, { runIds: [runId] }).pipe(
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
          // Preparation is already owned by the server scope. The socket can stop waiting
          // without interrupting that worker or skipping the durable launch handoff above.
          if (legacyCompletion !== undefined) {
            yield* Deferred.await(legacyCompletion).pipe(Effect.interruptible);
          }
          const releaseReceipt = yield* readReceipt(input, persistedPolicy.releaseCommandId);
          if (
            Option.isNone(releaseReceipt) ||
            releaseReceipt.value.status !== "accepted" ||
            releaseReceipt.value.commandId !== persistedPolicy.releaseCommandId ||
            releaseReceipt.value.commandType !== "prepared-run.release" ||
            releaseReceipt.value.threadId !== threadId
          )
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
          if (released.sequence !== releaseReceipt.value.resultSequence)
            return yield* mapError(
              input,
              "read-receipt",
              threadId,
            )("Release replay differs from the original accepted C receipt.");
          return {
            threadId,
            projection: yield* threads
              .getThreadProjection(threadId)
              .pipe(Effect.mapError(mapError(input, "read-receipt", threadId))),
            resumed: Option.isSome(launchReceipt) || messageWasAlreadyAccepted,
            legacyReleaseSequence: releaseReceipt.value.resultSequence,
          };
        }
        return yield* mapError(
          input,
          "release-run",
          threadId,
        )("Legacy launch has no retained release policy.");
      });
    },
  );

  return {
    launch,
    preflightLegacyBootstrap: Effect.fn("ThreadLaunchService.preflightLegacyBootstrap")(
      (binding: OrchestrationV2LegacyPreflightBinding) =>
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
        ),
    ),
  };
});
