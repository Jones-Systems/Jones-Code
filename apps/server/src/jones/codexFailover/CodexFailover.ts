import {
  CommandId,
  EventId,
  MessageId,
  type ModelSelection,
  type OrchestrationV2ServerCommand,
  type RunId,
  type ThreadId,
} from "@t3tools/contracts";
import { modelSelectionsEqual } from "@t3tools/shared/model";
import { latestRootProviderFailure } from "@t3tools/shared/orchestrationV2ThreadError";
import { turnItemUpdateCanEndBackgroundWork } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Sink from "../../orchestration-v2/EventSink.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import * as Adapters from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Switch from "../../orchestration-v2/ProviderSwitchService.ts";
import * as Locks from "../../orchestration-v2/ThreadCommandExecutor.ts";
import { isBufferedProviderNotification } from "../../orchestration-v2/NotificationMailbox.ts";
import type {
  RecordedLifecycleEvent,
  RecordedRun,
  RecordedStoredLifecycleEvent,
} from "../../orchestration-v2/RecordedTypes.ts";
import * as Registry from "../../provider/ProviderRegistry.ts";
import * as Settings from "../../serverSettings.ts";
import { selectionAdmissionReason } from "../threadConfiguration/selectionAdmission.ts";
import { readmitCodexFailoverTarget, selectCodexFailoverTarget } from "./CodexFailoverAdmission.ts";
import {
  codexFailoverChain,
  codexFailoverEligibility,
  codexFailoverFollowerSelection,
  codexFailoverPostSwitchEligible,
  latestCodexExecutedRun,
} from "./CodexFailoverEligibility.ts";
import * as Store from "./CodexFailoverStore.ts";
import * as Lineage from "./CodexFailoverLineage.ts";

const prefix = "jones:codex-failover:";
export const settlesTurnItem = (stored: RecordedStoredLifecycleEvent): boolean =>
  stored.event.type === "turn-item.updated" &&
  !String(stored.commandId).startsWith("command:runtime-reconcile:") &&
  turnItemUpdateCanEndBackgroundWork(stored.event.payload);
type Command = Extract<OrchestrationV2ServerCommand, { readonly type: "message.dispatch" }>;
type Envelope = {
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly commandType: string;
  readonly events: ReadonlyArray<RecordedLifecycleEvent>;
};
export class CodexFailoverError extends Schema.TaggedError<CodexFailoverError>()(
  "CodexFailoverError",
  {
    reason: Schema.Literals(["state_unavailable", "switch_refused", "continuation_refused"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return "Codex account failover could not be admitted.";
  }
}
export interface CodexFailoverCommitContext {
  readonly revalidate?: (
    input: Envelope,
  ) => Effect.Effect<ReadonlyArray<RecordedLifecycleEvent>, CodexFailoverError>;
  /** Runs after projection application, within the same transaction as the receipt. */
  readonly accept?: (input: Envelope) => Effect.Effect<void, CodexFailoverError>;
}
export interface CodexFailoverHooks {
  readonly afterFailedRun: (
    threadId: ThreadId,
    runId: RunId,
  ) => Effect.Effect<void, CodexFailoverError>;
  readonly afterChildWorkSettled: (threadId: ThreadId) => Effect.Effect<void, CodexFailoverError>;
  readonly queuedSelection: (
    threadId: ThreadId,
    run: RecordedRun,
  ) => Effect.Effect<ModelSelection | null, CodexFailoverError>;
  readonly continuationSourceRunId: (command: {
    readonly commandId: CommandId;
    readonly threadId: ThreadId;
  }) => Effect.Effect<RunId | undefined, CodexFailoverError>;
  readonly holdsChildResult: (
    threadId: ThreadId,
    run: RecordedRun | null | undefined,
  ) => Effect.Effect<boolean, CodexFailoverError>;
  readonly commitContext: (command: {
    readonly type: string;
    readonly commandId: CommandId;
    readonly threadId?: ThreadId;
  }) => CodexFailoverCommitContext | undefined;
  readonly recoverCodexFailover: Effect.Effect<void>;
}
export interface CodexFailoverPorts {
  readonly recoverCodexFailover?: Effect.Effect<void>;
}
const isCodexFailoverError = Schema.is(CodexFailoverError);
const unavailable = (cause: unknown) =>
  new CodexFailoverError({ reason: "state_unavailable", cause });
const blocking = (run: RecordedRun) =>
  ["preparing", "starting", "running", "waiting"].includes(run.status);
const fields = [
  "runs",
  "attempts",
  "providerThreads",
  "providerTurns",
  "providerSessions",
  "runtimeRequests",
  "turnItems",
  "subagents",
  "messages",
] as const;
type Projection = Projections.ProjectionRecords<(typeof fields)[number]>;

/** All dispatch callbacks skip lock acquisition; the caller already owns the thread lock. */
export const make = Effect.fn("CodexFailover.make")(function* <DispatchError, QueueError>(input: {
  readonly dispatch: (
    command: OrchestrationV2ServerCommand,
  ) => Effect.Effect<unknown, DispatchError>;
  readonly queueAfterFailure: (threadId: ThreadId, runId: RunId) => Effect.Effect<void, QueueError>;
}) {
  const projections = yield* Projections.ProjectionStoreV2;
  const sink = yield* Sink.EventSinkV2;
  const receipts = yield* Receipts.CommandReceiptStoreV2;
  const locks = yield* Locks.ThreadCommandExecutor;
  const transitions = yield* Switch.ProviderSwitchServiceV2;
  const settings = yield* Settings.ServerSettingsService;
  const adapters = yield* Adapters.ProviderAdapterRegistryV2;
  const registry = yield* Effect.serviceOption(Registry.ProviderRegistry);
  const fs = yield* FileSystem.FileSystem;
  const store = yield* Store.make;
  const lineage = yield* Lineage.make();
  const enabled = Option.isSome(registry);
  const snapshot = (threadId: ThreadId) => projections.getThreadRecords(threadId, fields);
  const metadata = Effect.fn("CodexFailover.metadata")(function* () {
    return yield* Effect.forEach(yield* adapters.list(), (instanceId) =>
      Effect.gen(function* () {
        // Missing continuation metadata is unknown, never proof of shared native history.
        if (adapters.getMetadata === undefined) return null;
        const value = yield* Effect.option(adapters.getMetadata(instanceId));
        return Option.isSome(value) ? { instanceId, ...value.value } : null;
      }),
    ).pipe(Effect.map((rows) => rows.filter((row) => row !== null)));
  });
  const facts = Effect.fn("CodexFailover.facts")(function* (
    projection: Projection,
    runId: RunId,
    startup = false,
  ) {
    const origin = projection.runs.find((run) => run.id === runId);
    const provider = projection.providerThreads.find((row) => row.id === origin?.providerThreadId);
    const worktreePresent =
      projection.thread.worktreePath !== null
        ? yield* fs.exists(projection.thread.worktreePath)
        : origin?.workspacePreparation?.type !== "worktree";
    return {
      driver: provider?.driver,
      stopped: projection.turnItems.some(
        (item) => item.runId === runId && item.type === "run_interrupt_request",
      ),
      pendingRequest: projection.runtimeRequests.some((request) => request.status === "pending"),
      sourceQuiet: yield* lineage.sourceQuiet(projection, origin?.providerThreadId),
      worktreePresent,
      now: DateTime.toEpochMillis(yield* DateTime.now),
      startup,
    };
  });
  const admission = Effect.fn("CodexFailover.admission")(function* (
    projection: Projection,
    savedSelection: ModelSelection,
    continuationSelection: ModelSelection,
  ) {
    return {
      providers: Option.isSome(registry) ? yield* registry.value.getProviders : [],
      settings: yield* settings.getSettings,
      projectId: projection.thread.projectId,
      metadata: yield* metadata(),
      savedSelection,
      continuationSelection,
      now: DateTime.toEpochMillis(yield* DateTime.now),
      plan: (selection: ModelSelection) =>
        transitions.plan({ projection, targetModelSelection: selection }),
    };
  });
  const postSwitch = Effect.fn("CodexFailover.postSwitch")(function* (
    step: Store.CodexFailoverStep,
    projection: Projection,
  ) {
    const events = yield* sink
      .readByCommandId({ commandId: step.switchCommandId })
      .pipe(Stream.runCollect);
    const switched = events.find(
      (stored) =>
        stored.event.type === "thread.provider-switched" && stored.event.threadId === step.threadId,
    );
    const target = yield* readmitCodexFailoverTarget(
      yield* admission(projection, step.savedSelection, step.continuationSelection),
    );
    return codexFailoverPostSwitchEligible({
      projection,
      step,
      selectionRevision: yield* store.selectionRevision(step.threadId),
      switchSequence: switched?.sequence ?? null,
      facts: {
        ...(yield* facts(projection, step.originRunId)),
        blockingRun: projection.runs.some(blocking),
      },
      targetAdmitted: target !== null,
    });
  });
  const continuation = (step: Store.CodexFailoverStep): Command => ({
    type: "message.dispatch",
    commandId: step.continueCommandId,
    threadId: step.threadId,
    messageId: step.continueMessageId,
    text: "Continue where you left off.",
    attachments: [],
    dispatchMode: { type: "start_immediately" },
    createdBy: "user",
    creationSource: "server",
    modelSelection: step.continuationSelection,
    modelSelectionExplicit: true,
  });
  const abandon = Effect.fn("CodexFailover.abandon")(function* (step: Store.CodexFailoverStep) {
    yield* store.setStepStatus(
      step.switchCommandId,
      "abandoned",
      DateTime.formatIso(yield* DateTime.now),
    );
  });
  const dispatchContinuation = Effect.fn("CodexFailover.dispatchContinuation")(function* (
    step: Store.CodexFailoverStep,
  ) {
    const receipt = yield* receipts.getByCommandId(step.continueCommandId);
    if (Option.isSome(receipt)) {
      if (receipt.value.threadId === step.threadId && receipt.value.status === "accepted")
        yield* store.setStepStatus(
          step.switchCommandId,
          "continued",
          DateTime.formatIso(yield* DateTime.now),
        );
      else yield* abandon(step);
      return;
    }
    const result = yield* Effect.exit(input.dispatch(continuation(step)));
    if (result._tag === "Failure") yield* abandon(step);
  });
  const evaluate = Effect.fn("CodexFailover.evaluate")(function* (
    evidence: Store.CodexFailoverEvidenceRecord,
    startup = false,
  ) {
    const projection = yield* snapshot(evidence.threadId);
    const result = codexFailoverEligibility({
      projection,
      evidence,
      facts: yield* facts(projection, evidence.runId, startup),
    });
    const now = yield* DateTime.now;
    const updatedAt = DateTime.formatIso(now);
    if (result.type === "defer") {
      yield* store.markEvidence(evidence.providerTurnId, { state: "deferred", updatedAt });
      return;
    }
    if (result.type === "skip") {
      yield* store.markEvidence(evidence.providerTurnId, {
        state: "decided",
        decision: result.reason,
        updatedAt,
      });
      return;
    }
    const origin = projection.runs.find((run) => run.id === evidence.runId)!;
    const predecessor = yield* store.getStepByContinueMessage(origin.userMessageId);
    const steps = yield* store.listChainSteps(predecessor?.chainRootRunId ?? origin.id);
    const chain = codexFailoverChain({
      threadId: evidence.threadId,
      origin,
      steps,
      usageResets: yield* store.listUnexpiredUsageResets(evidence.threadId, updatedAt),
      now: DateTime.toEpochMillis(now),
    });
    const target = yield* selectCodexFailoverTarget({
      ...(yield* admission(projection, projection.thread.modelSelection, origin.modelSelection)),
      visited: chain.visited,
    });
    if (target === null) {
      yield* store.markEvidence(evidence.providerTurnId, {
        state: "decided",
        decision: "exhausted",
        updatedAt,
      });
      return;
    }
    const step: Store.CodexFailoverStep = {
      switchCommandId: CommandId.make(`${prefix}${origin.id}:switch`),
      threadId: evidence.threadId,
      chainRootRunId: chain.chainRootRunId,
      originRunId: origin.id,
      originProviderTurnId: evidence.providerTurnId,
      sourceInstanceId: evidence.providerInstanceId,
      sourceFailure: evidence.disposition,
      sourceResetAt:
        evidence.disposition === "usage_limit"
          ? (latestRootProviderFailure(origin, projection.turnItems)?.resetAt ?? null)
          : null,
      targetInstanceId: target.savedSelection.instanceId,
      ...target,
      continueCommandId: CommandId.make(`${prefix}${origin.id}:continue`),
      continueMessageId: MessageId.make(`codex-failover:${origin.id}`),
      status: "switched",
      createdAt: updatedAt,
      updatedAt,
    };
    const switched = yield* sink.commitCommand({
      commandId: step.switchCommandId,
      threadId: step.threadId,
      commandType: "jones.codex-failover.switch",
      acceptedAt: now,
      events: [],
      effects: [],
      codexFailover: {
        revalidate: Effect.fn("CodexFailover.revalidateSwitch")(
          function* () {
            const currentEvidence = yield* store.getEvidence(evidence.providerTurnId);
            const current = yield* snapshot(step.threadId);
            if (
              currentEvidence === null ||
              currentEvidence.state === "decided" ||
              codexFailoverEligibility({
                projection: current,
                evidence: currentEvidence,
                facts: yield* facts(current, origin.id, startup),
              }).type !== "eligible" ||
              !modelSelectionsEqual(
                current.thread.modelSelection,
                projection.thread.modelSelection,
              ) ||
              !(yield* readmitCodexFailoverTarget(
                yield* admission(current, step.savedSelection, step.continuationSelection),
              ))
            )
              return yield* new CodexFailoverError({ reason: "switch_refused" });
            if (
              !(yield* store.insertStep(step)) ||
              !(yield* store.markEvidence(evidence.providerTurnId, {
                state: "decided",
                decision: "switched",
                updatedAt,
              }))
            )
              return yield* new CodexFailoverError({ reason: "switch_refused" });
            return [
              {
                id: EventId.make(`${step.switchCommandId}:event`),
                type: "thread.provider-switched" as const,
                threadId: step.threadId,
                providerInstanceId: step.targetInstanceId,
                occurredAt: now,
                payload: {
                  ...current.thread,
                  modelSelection: step.savedSelection,
                  providerInstanceId: step.targetInstanceId,
                  updatedAt: now,
                },
              },
            ];
          },
          Effect.mapError((cause) => (isCodexFailoverError(cause) ? cause : unavailable(cause))),
        ),
      },
    });
    if (switched.receipt.status === "accepted") yield* dispatchContinuation(step);
  });
  const react = (evidence: Store.CodexFailoverEvidenceRecord, startup = false) =>
    evaluate(evidence, startup).pipe(
      Effect.catchCause((cause) =>
        Effect.gen(function* () {
          yield* Effect.logWarning("Codex failover admission failed", {
            threadId: evidence.threadId,
            cause,
          });
          // Close the durable hold on a refused switch; the normal failure queue remains held.
          if ((yield* store.getEvidence(evidence.providerTurnId))?.state !== "decided")
            yield* store.markEvidence(evidence.providerTurnId, {
              state: "decided",
              decision: "admission_error",
              updatedAt: DateTime.formatIso(yield* DateTime.now),
            });
        }),
      ),
    );
  const afterFailedRun = Effect.fn("CodexFailover.afterFailedRun")(function* (
    threadId: ThreadId,
    runId: RunId,
  ) {
    if (!enabled) return;
    for (const evidence of yield* store.listOpenEvidenceForThread(threadId)) {
      if (evidence.runId !== runId) continue;
      yield* react(evidence);
    }
  });
  const afterChildWorkSettled = Effect.fn("CodexFailover.afterChildWorkSettled")(function* (
    threadId: ThreadId,
  ) {
    if (!enabled) return;
    // Discover ownership without locks, then release each lock before taking an ancestor's.
    // Native descendants publish independently; nesting parent under child would invert delegation.
    for (const owner of yield* lineage.settleThreadIds(threadId))
      yield* locks.withLock(
        owner,
        Effect.gen(function* () {
          for (const evidence of yield* store.listOpenEvidenceForThread(owner)) {
            // Recorded evidence can precede the root terminal projection. Only the
            // failed-run reactor (or startup reconciliation) admits that first reaction.
            if (evidence.state !== "deferred") continue;
            yield* react(evidence);
            yield* input.queueAfterFailure(owner, evidence.runId);
          }
        }),
      );
  });
  const queuedSelection = (threadId: ThreadId, run: RecordedRun) =>
    !enabled || run.threadId !== threadId || run.status !== "queued"
      ? Effect.succeed(null)
      : store.getQueuedOverride(run.id);
  const continuationSourceRunId = (command: {
    readonly commandId: CommandId;
    readonly threadId: ThreadId;
  }) =>
    !enabled || !command.commandId.startsWith(prefix)
      ? Effect.sync(() => undefined)
      : store
          .getStepByContinueCommand(command.commandId)
          .pipe(
            Effect.map((step) =>
              step?.threadId === command.threadId ? step.originRunId : undefined,
            ),
          );
  const holdsChildResult = Effect.fn("CodexFailover.holdsChildResult")(function* (
    threadId: ThreadId,
    run: RecordedRun | null | undefined,
  ) {
    if (!enabled || run?.threadId !== threadId) return false;
    const projection = yield* projections.getThreadRecords(threadId, ["runs", "attempts"]);
    const taskRuns =
      run.delegatedTaskId === undefined
        ? projection.runs
        : projection.runs.filter((row) => row.delegatedTaskId === run.delegatedTaskId);
    const latest = latestCodexExecutedRun(taskRuns);
    if (latest?.status !== "failed") return false;
    const step = yield* store.getStepByOrigin(latest.id);
    if (step?.threadId === threadId && step.status === "switched") return true;
    const turn = projection.attempts.find((row) => row.id === latest.activeAttemptId)
      ?.providerSettlement?.providerTurnId;
    if (turn === undefined) return false;
    const evidence = yield* store.getEvidence(turn);
    return (
      evidence?.threadId === threadId &&
      evidence.runId === latest.id &&
      evidence.state !== "decided"
    );
  });
  const commitContext = (command: {
    readonly type: string;
    readonly commandId: CommandId;
    readonly threadId?: ThreadId;
  }): CodexFailoverCommitContext | undefined => {
    if (!enabled || command.type !== "message.dispatch" || command.threadId === undefined)
      return undefined;
    const message = command as Command;
    const isContinuation = command.commandId.startsWith(prefix);
    const inherited =
      message.modelSelectionExplicit === false || message.modelSelection === undefined;
    if (!isContinuation && !inherited) return undefined;
    // The pre-event snapshot belongs to this one EventSink transaction. Reading the
    // newly inserted continuation would make its own run look like a competing Send.
    let before: Projection | undefined;
    return {
      ...(isContinuation
        ? {
            revalidate: Effect.fn("CodexFailover.revalidateContinuation")(
              function* (envelope: Envelope) {
                const step = yield* store.getStepByContinueCommand(message.commandId);
                before = yield* snapshot(message.threadId);
                if (
                  step === null ||
                  step.threadId !== message.threadId ||
                  message.messageId !== step.continueMessageId ||
                  message.dispatchMode.type !== "start_immediately" ||
                  message.modelSelection === undefined ||
                  !modelSelectionsEqual(message.modelSelection, step.continuationSelection) ||
                  !(yield* postSwitch(step, before))
                )
                  return yield* new CodexFailoverError({ reason: "continuation_refused" });
                return envelope.events;
              },
              Effect.mapError((cause) =>
                isCodexFailoverError(cause) ? cause : unavailable(cause),
              ),
            ),
          }
        : {}),
      accept: Effect.fn("CodexFailover.accept")(
        function* (envelope: Envelope) {
          const updatedAt = DateTime.formatIso(yield* DateTime.now);
          if (isContinuation) {
            const step = yield* store.getStepByContinueCommand(message.commandId);
            if (step === null || before === undefined || !(yield* postSwitch(step, before)))
              return yield* new CodexFailoverError({ reason: "continuation_refused" });
            const live = yield* admission(before, step.savedSelection, step.continuationSelection);
            const markers = yield* store.listInheritedQueuedRuns(step.threadId);
            for (const run of before.runs) {
              const queuedMessage = before.messages.find((row) => row.id === run.userMessageId);
              const effectiveSelection =
                (yield* store.getQueuedOverride(run.id)) ??
                (queuedMessage?.notification?.source.kind === "monitor" &&
                !isBufferedProviderNotification(queuedMessage)
                  ? before.thread.modelSelection
                  : run.modelSelection);
              const selection = codexFailoverFollowerSelection({
                run,
                marker: markers.find((row) => row.runId === run.id) ?? null,
                threadId: step.threadId,
                activeProviderThreadId: before.thread.activeProviderThreadId,
                sourceInstanceId: step.sourceInstanceId,
                targetInstanceId: step.targetInstanceId,
                effectiveSelection,
                targetAdmissible: (selection) =>
                  selectionAdmissionReason({ ...live, selection }) === null,
              });
              if (
                selection !== null &&
                !(yield* store.setQueuedOverride({
                  runId: run.id,
                  threadId: step.threadId,
                  selection,
                  switchCommandId: step.switchCommandId,
                  updatedAt,
                }))
              )
                return yield* new CodexFailoverError({ reason: "continuation_refused" });
            }
            if (!(yield* store.setStepStatus(step.switchCommandId, "continued", updatedAt)))
              return yield* new CodexFailoverError({ reason: "continuation_refused" });
          }
          if (!inherited) return;
          const queuedEvents = envelope.events.filter(
            (event) => event.type === "run.created" && event.payload.status === "queued",
          );
          if (queuedEvents.length === 0) return;
          const { providerThreads } = yield* projections.getThreadRecords(message.threadId, [
            "providerThreads",
          ]);
          for (const event of queuedEvents) {
            if (
              event.type !== "run.created" ||
              event.threadId !== message.threadId ||
              event.payload.status !== "queued"
            )
              continue;
            if (
              !providerThreads.some(
                (row) =>
                  row.id === event.payload.providerThreadId &&
                  row.providerInstanceId === event.payload.providerInstanceId &&
                  row.driver === "codex",
              )
            )
              continue;
            yield* store.insertInheritedQueuedRun({
              runId: event.payload.id,
              threadId: message.threadId,
              inheritedAt: updatedAt,
            });
          }
        },
        Effect.mapError((cause) => (isCodexFailoverError(cause) ? cause : unavailable(cause))),
      ),
    };
  };
  const recoverCodexFailover = Effect.gen(function* () {
    if (!enabled) return;
    const markers = yield* store.listInheritedQueuedRuns();
    const byThread = new Map<ThreadId, (typeof markers)[number][]>();
    for (const marker of markers)
      byThread.set(marker.threadId, [...(byThread.get(marker.threadId) ?? []), marker]);
    for (const [threadId, rows] of byThread)
      yield* locks.withLock(
        threadId,
        Effect.gen(function* () {
          const current = yield* projections
            .getThreadRecords(threadId, ["runs"])
            .pipe(
              Effect.catchTags({ ProjectionStoreThreadNotFoundError: () => Effect.succeed(null) }),
            );
          const queued = new Set(
            current === null
              ? []
              : current.runs.filter((run) => run.status === "queued").map((run) => run.id),
          );
          yield* store.deleteQueuedRuns(
            rows.filter((row) => !queued.has(row.runId)).map((row) => row.runId),
          );
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("Codex failover queue-marker recovery failed", { threadId, cause }),
          ),
        ),
      );
    // Snapshot the open list: newly switched steps are dispatched by evaluate itself.
    const switched = yield* store.listSwitchedSteps();
    for (const evidence of yield* store.listOpenEvidence())
      yield* locks.withLock(
        evidence.threadId,
        Effect.gen(function* () {
          yield* react(evidence, true);
          yield* input.queueAfterFailure(evidence.threadId, evidence.runId);
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("Codex failover evidence recovery failed", {
              threadId: evidence.threadId,
              cause,
            }),
          ),
        ),
      );
    for (const step of switched)
      yield* locks.withLock(
        step.threadId,
        Effect.gen(function* () {
          const receipt = yield* receipts.getByCommandId(step.continueCommandId);
          if (Option.isSome(receipt)) yield* dispatchContinuation(step);
          else {
            const projection = yield* snapshot(step.threadId).pipe(
              Effect.catchTags({ ProjectionStoreThreadNotFoundError: () => Effect.succeed(null) }),
            );
            if (projection !== null && (yield* postSwitch(step, projection)))
              yield* dispatchContinuation(step);
            else yield* abandon(step);
          }
          yield* input.queueAfterFailure(step.threadId, step.originRunId);
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("Codex failover continuation recovery failed", {
              threadId: step.threadId,
              cause,
            }),
          ),
        ),
      );
  }).pipe(
    Effect.catchCause((cause) =>
      Effect.logWarning("Codex failover recovery is unavailable", { cause }),
    ),
  );
  const hooks: CodexFailoverHooks = {
    afterFailedRun: (threadId, runId) =>
      afterFailedRun(threadId, runId).pipe(Effect.mapError(unavailable)),
    afterChildWorkSettled: (threadId) =>
      afterChildWorkSettled(threadId).pipe(Effect.mapError(unavailable)),
    queuedSelection: (threadId, run) =>
      queuedSelection(threadId, run).pipe(Effect.mapError(unavailable)),
    continuationSourceRunId: (command) =>
      continuationSourceRunId(command).pipe(Effect.mapError(unavailable)),
    holdsChildResult: (threadId, run) =>
      holdsChildResult(threadId, run).pipe(Effect.mapError(unavailable)),
    commitContext,
    recoverCodexFailover,
  };
  return hooks;
});
