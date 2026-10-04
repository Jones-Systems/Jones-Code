import { modelSelectionsEqual } from "@t3tools/shared/model";
import { projectComposerContextForProvider } from "@t3tools/shared/composerContextReferences";
import {
  CommandId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
  type OrchestrationV2TurnItem,
  RunId,
  type RunAttemptId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as NodeCrypto from "node:crypto";
import { OrchestrationV2StoredEventJson } from "@t3tools/contracts";
import { nativeCreationCanonicalJson, nativeCreationSha256 } from "./NativeCreationPreparation.ts";

import * as GitWorkflowService from "../git/GitWorkflowService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProviderAuthService from "../provider/Services/ProviderAuthService.ts";
import * as EventSink from "./EventSink.ts";
import * as ContextHandoffService from "./ContextHandoffService.ts";
import * as CheckpointService from "./CheckpointService.ts";
import * as ProjectStore from "./ProjectStore.ts";
import type { ImportedHistoryStartExecutionReferenceV2 } from "./Orchestrator.ts";
import {
  DEFAULT_HANDOFF_TOKEN_CAP,
  handoffTokenCapConfig,
  handoffBudget,
  attachmentTokenAllowance,
  contextUsageForHandoff,
  historicalMessage,
  latestNativeContextUsage,
} from "./ContextHandoffBudget.ts";
import { deliverContextHandoffs } from "./ContextHandoffDelivery.ts";
import {
  ProviderAdapterTurnStartError,
  ProviderAdapterResumeThreadError,
  type ProviderAdapterV2Error,
  type ProviderAdapterV2HistoricalContext,
  type ProviderAdapterV2SessionRuntime,
  type ProviderAdapterV2OpenSessionInput,
  type ProviderNativeOperationContext,
  type ProviderNativeEffectEvidence,
  type ProviderRuntimeBinding,
  withProviderNativeEffect,
} from "./ProviderAdapter.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import {
  makeProviderFailure,
  nativeEffectEvidenceFor,
  ProviderNativeOperationUnknownError,
} from "./ProviderFailure.ts";
import * as RunExecutionService from "./RunExecutionService.ts";
import * as OrdinaryCheckout from "./OrdinaryCheckoutOwnership.ts";
import * as RuntimePolicy from "./RuntimePolicy.ts";
import {
  authorizeNativeCreationExecution,
  getNativeCreationExecutionReference,
  type NativeCreationExecutionContextV2,
} from "./NativeCreationAuthority.ts";
import {
  isRestartNoteContinuation,
  pendingRestartCancelledBackgroundWork,
  restartCancelledBackgroundWorkNote,
} from "./RestartBackgroundNote.ts";

export class ProviderTurnStartError extends Schema.TaggedError<ProviderTurnStartError>()(
  "ProviderTurnStartError",
  {
    runId: RunId,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

const isProviderTurnStartError = Schema.is(ProviderTurnStartError);

export const StartOutcomeSink = Context.Reference<
  ((observation: EventSink.StartFailedBeforeOpenObservationV1) => Effect.Effect<void>) | undefined
>("t3/orchestration-v2/ProviderTurnStartService/StartOutcomeSink", {
  defaultValue: () => undefined,
});
const issuedFailedStarts = new WeakMap<
  object,
  { readonly observation: EventSink.StartFailedBeforeOpenObservationV1; readonly snapshot: string }
>();
export function readIssuedStartFailedBeforeOpenObservation(
  value: unknown,
): EventSink.StartFailedBeforeOpenObservationV1 | null {
  if (typeof value !== "object" || value === null) return null;
  const issued = issuedFailedStarts.get(value);
  if (issued === undefined) return null;
  try {
    return nativeCreationCanonicalJson(
      Schema.encodeSync(EventSink.StartFailedBeforeOpenObservationV1)(issued.observation),
    ) === issued.snapshot
      ? issued.observation
      : null;
  } catch {
    return null;
  }
}

export const StartRetryOutcomeSink = Context.Reference<
  ((observation: EventSink.StartRetryBeforeOpenObservationV1) => Effect.Effect<void>) | undefined
>("t3/orchestration-v2/ProviderTurnStartService/StartRetryOutcomeSink", {
  defaultValue: () => undefined,
});
const issuedRetryStarts = new WeakMap<
  object,
  { readonly observation: EventSink.StartRetryBeforeOpenObservationV1; readonly snapshot: string }
>();
export function readIssuedStartRetryBeforeOpenObservation(
  value: unknown,
): EventSink.StartRetryBeforeOpenObservationV1 | null {
  if (typeof value !== "object" || value === null) return null;
  const issued = issuedRetryStarts.get(value);
  if (issued === undefined) return null;
  try {
    return nativeCreationCanonicalJson(
      Schema.encodeSync(EventSink.StartRetryBeforeOpenObservationV1)(issued.observation),
    ) === issued.snapshot
      ? issued.observation
      : null;
  } catch {
    return null;
  }
}

export interface ProviderNativeStartConfirmation {
  readonly status: "confirmed_start";
  readonly binding: ProviderRuntimeBinding;
  readonly evidenceRevision: number;
  readonly attemptId: RunAttemptId;
  readonly nativeEffect: ProviderNativeEffectEvidence & { readonly outcome: "confirmed_success" };
}

export type ImportedHistoryStartExecution = RunExecutionService.ImportedHistoryStartExecution;
export type QueuedRunStartExecution = RunExecutionService.QueuedRunStartExecution;

export interface ProviderTurnStartServiceV2Shape {
  readonly prepareImportedHistoryStart: (input: {
    readonly reference: ImportedHistoryStartExecutionReferenceV2;
    readonly choice: EventSink.ImportedHistoryStartOutcomeV2;
  }) => Effect.Effect<ReadonlyArray<OrchestrationV2DomainEvent>, ProviderTurnStartError>;
  /**
   * Starts the run's provider turn. Unknown native outcomes remain held on
   * every attempt; only a proven no-effect failure can retry or settle.
   */
  readonly start: (input: {
    readonly threadId: ThreadId;
    readonly runId: RunId;
    readonly willRetry?: boolean;
    readonly ordinaryCheckoutUse?: OrdinaryCheckout.OrdinaryCheckoutUseV1;
    readonly ordinaryCheckoutExecution?: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1;
    readonly prepareOrdinaryManagedActorRun?: RunExecutionService.RunExecutionServiceV2StartRootRunInput["prepareOrdinaryManagedActorRun"];
    readonly onOrdinaryManagedRunStarted?: (
      handle: RunExecutionService.OrdinaryManagedRunExecutionHandleV1,
      confirmation?: ProviderNativeStartConfirmation,
    ) => Effect.Effect<void, unknown>;
    readonly nativeCreationExecutionContext?: NativeCreationExecutionContextV2;
    readonly importedHistoryStartExecution?: ImportedHistoryStartExecution;
    readonly queuedRunStartExecution?: QueuedRunStartExecution;
  }) => Effect.Effect<void | ProviderNativeStartConfirmation, ProviderTurnStartError>;
}

export class ProviderTurnStartServiceV2 extends Context.Service<
  ProviderTurnStartServiceV2,
  ProviderTurnStartServiceV2Shape
>()("t3/orchestration-v2/ProviderTurnStartService/ProviderTurnStartServiceV2") {}

export const layer: Layer.Layer<
  ProviderTurnStartServiceV2,
  never,
  | EventSink.EventSinkV2
  | ContextHandoffService.ContextHandoffServiceV2
  | IdAllocator.IdAllocatorV2
  | FileSystem.FileSystem
  | GitWorkflowService.GitWorkflowService
  | ProjectService.ProjectService
  | ProviderAuthService.ProviderAuthService
  | ProjectionStore.ProjectionStoreV2
  | ProviderSessionManager.ProviderSessionManagerV2
  | RunExecutionService.RunExecutionServiceV2
  | RuntimePolicy.RuntimePolicyV2
> = Layer.effect(
  ProviderTurnStartServiceV2,
  Effect.gen(function* () {
    const eventSink = yield* EventSink.EventSinkV2;
    const contextHandoffService = yield* ContextHandoffService.ContextHandoffServiceV2;
    const idAllocator = yield* IdAllocator.IdAllocatorV2;
    const fileSystem = yield* FileSystem.FileSystem;
    const gitWorkflow = yield* GitWorkflowService.GitWorkflowService;
    const projects = yield* ProjectService.ProjectService;
    const providerAuth = yield* ProviderAuthService.ProviderAuthService;
    const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
    const providerSessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
    const runExecution = yield* RunExecutionService.RunExecutionServiceV2;
    const runtimePolicy = yield* RuntimePolicy.RuntimePolicyV2;
    const importedCheckpointService = yield* Effect.serviceOption(
      CheckpointService.CheckpointServiceV2,
    );
    const importedProjectStore = yield* Effect.serviceOption(ProjectStore.ProjectStoreV2);

    const prepareImportedHistoryStart: ProviderTurnStartServiceV2Shape["prepareImportedHistoryStart"] =
      (input) =>
        Effect.gen(function* () {
          const { reference, choice } = input;
          const reject = (cause: unknown) =>
            new ProviderTurnStartError({ runId: reference.runId, cause });
          if (
            choice.receipt.status !== "accepted" ||
            choice.commandId !== reference.commandId ||
            choice.threadId !== reference.threadId ||
            choice.runId !== reference.runId ||
            (choice.effectId !== null && choice.effectId !== reference.effectId) ||
            Option.isNone(importedCheckpointService) ||
            Option.isNone(importedProjectStore)
          )
            return yield* reject(
              "The accepted imported choice or its SQL preparation services are unavailable.",
            );
          const projection = yield* projectionStore.getTurnStartContext(
            reference.threadId,
            reference.runId,
          );
          const run = projection.runs.find((candidate) => candidate.id === reference.runId);
          const attempt = projection.attempts.find(
            (candidate) => candidate.id === run?.activeAttemptId,
          );
          const rootNode = projection.nodes.find((candidate) => candidate.id === run?.rootNodeId);
          const providerThread = projection.providerThreads.find(
            (candidate) => candidate.id === run?.providerThreadId,
          );
          const queuedDelivery = choice.command.delivery.type === "queued_run";
          if (
            run === undefined ||
            attempt === undefined ||
            rootNode === undefined ||
            providerThread === undefined ||
            run.userMessageId !== choice.messageId ||
            (run.status !== "starting" && run.status !== "queued") ||
            attempt.status !== "pending" ||
            attempt.runId !== run.id ||
            attempt.rootNodeId !== rootNode.id ||
            attempt.providerThreadId !== providerThread.id ||
            providerThread.appThreadId !== reference.threadId ||
            (!queuedDelivery &&
              (rootNode.checkpointScopeId !== null ||
                providerThread.providerSessionId !== null ||
                providerThread.nativeThreadRef !== null ||
                providerThread.nativeConversationHeadRef !== null))
          )
            return yield* reject(
              "The accepted imported choice no longer owns its fresh execution projection.",
            );
          const project = yield* importedProjectStore.value.get(projection.thread.projectId);
          if (Option.isNone(project))
            return yield* reject("The imported choice project is unavailable.");
          const cwd = projection.thread.worktreePath ?? project.value.workspaceRoot;
          const now = yield* DateTime.now;
          const preparedProviderThreadId = queuedDelivery
            ? idAllocator.derive.providerThread({
                driver: providerThread.driver,
                providerInstanceId: run.providerInstanceId,
                nativeThreadId: `pending:${reference.effectId}:${NodeCrypto.randomUUID()}`,
              })
            : providerThread.id;
          const handoff = queuedDelivery
            ? yield* Effect.gen(function* () {
                const records = yield* projectionStore.getThreadRecords(
                  reference.threadId,
                  ["turnItems"],
                  { turnItemRunIds: [null] },
                );
                if (records.turnItems.length === 0)
                  return yield* reject(
                    "The accepted imported transcript has no readable handoff items.",
                  );
                return yield* contextHandoffService.prepareLegacyImport({
                  threadId: reference.threadId,
                  targetRunId: run.id,
                  toProviderThreadId: preparedProviderThreadId,
                  toProviderInstanceId: run.providerInstanceId,
                  items: records.turnItems,
                  createdAt: now,
                });
              })
            : undefined;
          const providerSessionId = yield* idAllocator.allocate.providerSession({
            providerInstanceId: run.providerInstanceId,
            threadId: reference.threadId,
          });
          const scope = yield* importedCheckpointService.value.prepareRootRunScope({
            threadId: reference.threadId,
            runId: run.id,
            rootNodeId: rootNode.id,
            providerThreadId: preparedProviderThreadId,
            cwd,
            createdAt: now,
          });
          const preparedProvider: OrchestrationV2ProviderThread = queuedDelivery
            ? {
                id: preparedProviderThreadId,
                driver: providerThread.driver,
                providerInstanceId: run.providerInstanceId,
                providerSessionId,
                appThreadId: reference.threadId,
                ownerNodeId: null,
                nativeThreadRef: null,
                nativeConversationHeadRef: null,
                status: "not_loaded",
                firstRunOrdinal: run.ordinal,
                lastRunOrdinal: run.ordinal,
                handoffIds: handoff === undefined ? [] : [handoff.id],
                forkedFrom: null,
                createdAt: now,
                updatedAt: now,
              }
            : { ...providerThread, providerSessionId, status: "not_loaded", updatedAt: now };
          const payloads = [
            { type: "provider-thread.updated", payload: preparedProvider },
            ...(handoff === undefined
              ? []
              : [{ type: "context-handoff.updated" as const, payload: handoff }]),
            { type: "checkpoint-scope.created", payload: scope },
            {
              type: "node.updated",
              payload: {
                ...rootNode,
                providerThreadId: preparedProviderThreadId,
                checkpointScopeId: scope.id,
              },
            },
            {
              type: "thread.metadata-updated",
              payload: {
                ...projection.thread,
                activeProviderThreadId: preparedProviderThreadId,
                providerInstanceId: run.providerInstanceId,
                modelSelection: run.modelSelection,
                updatedAt: now,
              },
            },
            {
              type: "run.updated",
              payload: {
                ...run,
                providerThreadId: preparedProviderThreadId,
                status: "starting",
                ...(handoff === undefined ? {} : { contextHandoffId: handoff.id }),
              },
            },
            ...(queuedDelivery
              ? [
                  {
                    type: "run-attempt.updated" as const,
                    payload: { ...attempt, providerThreadId: preparedProviderThreadId },
                  },
                ]
              : []),
          ] as const;
          return yield* Effect.forEach(payloads, (event) =>
            Effect.gen(function* () {
              return {
                ...event,
                id: yield* idAllocator.allocate.event({
                  threadId: reference.threadId,
                  commandId: reference.commandId,
                }),
                threadId: reference.threadId,
                runId: run.id,
                nodeId: rootNode.id,
                providerInstanceId: run.providerInstanceId,
                occurredAt: now,
              } satisfies OrchestrationV2DomainEvent;
            }),
          );
        }).pipe(
          Effect.mapError((cause) =>
            isProviderTurnStartError(cause)
              ? cause
              : new ProviderTurnStartError({ runId: input.reference.runId, cause }),
          ),
        );

    // These callbacks outlive startup while a run drains background work. Build
    // them outside start's scope so they cannot retain its full thread history.
    const makeRunControls = (input: {
      readonly threadId: ThreadId;
      readonly runId: RunId;
      readonly attemptId: OrchestrationV2RunAttempt["id"];
      readonly providerThreadId: OrchestrationV2ProviderThread["id"];
      readonly runOrdinal: number;
      readonly inheritedBackgroundTurnItems: ReturnType<
        typeof RunExecutionService.selectInheritedBackgroundTurnItems
      >;
    }) => {
      // Guards and background routing need live execution state, not a fresh
      // allocation of every completed message and tool output in the thread.
      const isCurrentAttemptInStatus = (expectedStatus: OrchestrationV2Run["status"]) =>
        projectionStore.getRuntimeRecoveryProjection(input.threadId).pipe(
          Effect.map((current) => {
            const run = current.runs.find((candidate) => candidate.id === input.runId);
            return run?.activeAttemptId === input.attemptId && run.status === expectedStatus;
          }),
          Effect.catchCause(() => Effect.succeed(false)),
        );
      return {
        isCurrentAttemptInStatus,
        loadInheritedBackgroundTurnItems: () =>
          projectionStore.getRuntimeRecoveryProjection(input.threadId).pipe(
            Effect.map((current) =>
              RunExecutionService.selectInheritedBackgroundTurnItems({
                threadId: input.threadId,
                currentProviderThreadId: input.providerThreadId,
                currentRunOrdinal: input.runOrdinal,
                runs: current.runs,
                turnItems: current.turnItems,
              }),
            ),
            Effect.catchCause(() => Effect.succeed(input.inheritedBackgroundTurnItems)),
          ),
        shouldStartProviderTurn: () => isCurrentAttemptInStatus("running"),
        shouldFinalizeRun: () =>
          projectionStore.getRuntimeRecoveryProjection(input.threadId).pipe(
            Effect.map((current) => {
              const run = current.runs.find((candidate) => candidate.id === input.runId);
              return (
                run?.activeAttemptId === input.attemptId &&
                (run.status === "starting" || run.status === "running")
              );
            }),
            Effect.catchCause(() => Effect.succeed(false)),
          ),
        hasUnpairedRunInterruptRequest: () =>
          projectionStore
            .hasUnpairedRunInterruptRequest(
              input.threadId,
              idAllocator.derive.runSignalTurnItem({
                runId: input.runId,
                signal: "interrupt-request",
              }),
              idAllocator.derive.runSignalTurnItem({
                runId: input.runId,
                signal: "interrupt-result",
              }),
            )
            .pipe(Effect.catchCause(() => Effect.succeed(false))),
      };
    };

    const makeDeliverySession = (
      session: ProviderAdapterV2SessionRuntime,
      startWithHandoffs: (
        input: Parameters<ProviderAdapterV2SessionRuntime["startTurn"]>[0],
        compact?: boolean,
      ) => ReturnType<ProviderAdapterV2SessionRuntime["startTurn"]>,
    ) => {
      let deliver: typeof startWithHandoffs | undefined = startWithHandoffs;
      const start = (
        input: Parameters<ProviderAdapterV2SessionRuntime["startTurn"]>[0],
        compact = false,
      ) =>
        Effect.suspend(() => {
          if (deliver !== undefined) return deliver(input, compact);
          return compact && session.compactThread !== undefined
            ? session.compactThread(input)
            : session.startTurn(input);
        }).pipe(
          // Only startup needs the handoff history. The event worker keeps this
          // session alive afterward, including when background work remains.
          Effect.ensuring(
            Effect.sync(() => {
              deliver = undefined;
            }),
          ),
        );
      const deliverySession: ProviderAdapterV2SessionRuntime = {
        ...session,
        get providerSession() {
          return session.providerSession;
        },
        get continuationSourceIdentity() {
          return session.continuationSourceIdentity;
        },
        startTurn: (input: Parameters<typeof session.startTurn>[0]) => start(input),
        ...(session.compactThread === undefined
          ? {}
          : {
              compactThread: (input: Parameters<typeof session.startTurn>[0]) => start(input, true),
            }),
      };
      if ("runtimeGeneration" in session) {
        Object.defineProperty(deliverySession, "runtimeGeneration", {
          configurable: true,
          enumerable: true,
          get: () => session.runtimeGeneration,
        });
      }
      return deliverySession;
    };

    const start = Effect.fn("orchestrationV2.providerTurnStart.start")(function* (input: {
      readonly threadId: ThreadId;
      readonly runId: RunId;
      readonly willRetry?: boolean;
      readonly ordinaryCheckoutUse?: OrdinaryCheckout.OrdinaryCheckoutUseV1;
      readonly ordinaryCheckoutExecution?: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1;
      readonly prepareOrdinaryManagedActorRun?: RunExecutionService.RunExecutionServiceV2StartRootRunInput["prepareOrdinaryManagedActorRun"];
      readonly onOrdinaryManagedRunStarted?: (
        handle: RunExecutionService.OrdinaryManagedRunExecutionHandleV1,
        confirmation?: ProviderNativeStartConfirmation,
      ) => Effect.Effect<void, unknown>;
      readonly nativeCreationExecutionContext?: NativeCreationExecutionContextV2;
      readonly importedHistoryStartExecution?: ImportedHistoryStartExecution;
      readonly queuedRunStartExecution?: QueuedRunStartExecution;
    }) {
      const { runId } = input;
      const ordinaryExecution = input.ordinaryCheckoutExecution;
      const revalidateOrdinaryExecution =
        ordinaryExecution === undefined
          ? Effect.void
          : eventSink.revalidateOrdinaryCheckoutExecution(ordinaryExecution).pipe(Effect.asVoid);
      if (ordinaryExecution !== undefined) {
        if (
          ordinaryExecution.executor.kind !== "actual_outbox_claim" ||
          ordinaryExecution.originalUse.lease.ownerThreadId !== input.threadId ||
          (input.ordinaryCheckoutUse !== undefined &&
            (yield* Schema.encodeEffect(
              Schema.fromJsonString(OrdinaryCheckout.OrdinaryCheckoutUseV1),
            )(input.ordinaryCheckoutUse).pipe(Effect.orDie)) !==
              (yield* Schema.encodeEffect(
                Schema.fromJsonString(OrdinaryCheckout.OrdinaryCheckoutUseV1),
              )(ordinaryExecution.originalUse).pipe(Effect.orDie)))
        )
          return yield* new ProviderTurnStartError({
            runId,
            cause: "The start has no matching original checkout execution actor.",
          });
        yield* revalidateOrdinaryExecution;
      }
      const executionReference =
        input.nativeCreationExecutionContext === undefined
          ? null
          : getNativeCreationExecutionReference(input.nativeCreationExecutionContext);
      if (input.nativeCreationExecutionContext !== undefined && executionReference === null) {
        return yield* new ProviderTurnStartError({
          runId,
          cause: "The native execution context has no issued effect reference.",
        });
      }
      if (input.importedHistoryStartExecution !== undefined) {
        if (
          executionReference !== null ||
          input.importedHistoryStartExecution.reference.threadId !== input.threadId ||
          input.importedHistoryStartExecution.reference.runId !== runId
        )
          return yield* new ProviderTurnStartError({
            runId,
            cause: "The prepared application choice differs from this provider start.",
          });
        yield* RunExecutionService.readImportedHistoryStartExecution(
          input.importedHistoryStartExecution,
          eventSink,
        );
      }
      if (input.queuedRunStartExecution !== undefined) {
        if (
          executionReference !== null ||
          input.importedHistoryStartExecution !== undefined ||
          input.queuedRunStartExecution.threadId !== input.threadId ||
          input.queuedRunStartExecution.runId !== runId
        )
          return yield* new ProviderTurnStartError({
            runId,
            cause: "The claimed queued source differs from this provider start.",
          });
        yield* RunExecutionService.readClaimedQueuedRunStartExecution(
          input.queuedRunStartExecution,
          eventSink,
        );
      }
      const projection = yield* projectionStore.getTurnStartContext(input.threadId, runId);
      const run = projection.runs.find((candidate) => candidate.id === runId);
      if (run === undefined) {
        return yield* new ProviderTurnStartError({ runId, cause: `Run ${runId} was not found.` });
      }
      const ordinaryAdmission = yield* eventSink.readOrdinaryCheckoutAdmissionForRun({
        threadId: input.threadId,
        runId,
      });
      if (
        ordinaryAdmission !== null &&
        (ordinaryExecution === undefined ||
          ordinaryExecution.originalUse.admission.admissionId !== ordinaryAdmission.admissionId ||
          ordinaryExecution.originalUse.admission.admissionSha256 !==
            OrdinaryCheckout.ordinaryCheckoutAdmissionRefV1(ordinaryAdmission).admissionSha256)
      )
        return yield* new ProviderTurnStartError({
          runId,
          cause: "The admitted ordinary run is missing its actual original checkout actor.",
        });
      if (run.status !== "starting") {
        // Ordinary effects can observe a run that already advanced or terminalized.
        return;
      }
      const rootNode = projection.nodes.find((candidate) => candidate.id === run.rootNodeId);
      const attempt = projection.attempts.find((candidate) => candidate.id === run.activeAttemptId);
      const providerThread = projection.providerThreads.find(
        (candidate) => candidate.id === run.providerThreadId,
      );
      const message = projection.messages.find((candidate) => candidate.id === run.userMessageId);
      const checkpointScope = projection.checkpointScopes.find(
        (candidate) => candidate.id === rootNode?.checkpointScopeId,
      );
      const handoffs = projection.contextHandoffs.filter(
        (handoff) =>
          handoff.status === "ready" &&
          (handoff.targetRunId === run.id ||
            (handoff.toProviderThreadId === run.providerThreadId &&
              projection.runs.some(
                (source) =>
                  source.id === handoff.targetRunId &&
                  (source.status === "failed" ||
                    source.status === "interrupted" ||
                    (source.status === "completed" &&
                      handoff.delivery === undefined &&
                      projection.messages.some(
                        (message) =>
                          message.id === source.userMessageId &&
                          message.attachments.length === 0 &&
                          message.text.trim().toLowerCase() === "/compact",
                      ))),
              ))),
      );
      const nativeForkTransfer = projection.contextTransfers.find(
        (transfer) =>
          transfer.type === "fork" &&
          transfer.targetThreadId === input.threadId &&
          transfer.targetRunId === run.id &&
          transfer.status === "pending" &&
          transfer.resolution === null,
      );
      if (
        rootNode === undefined ||
        attempt === undefined ||
        providerThread === undefined ||
        providerThread.providerSessionId === null ||
        message === undefined ||
        checkpointScope === undefined
      ) {
        return yield* new ProviderTurnStartError({
          runId,
          cause: `Run ${runId} is missing its execution projection state.`,
        });
      }
      // Settles a run that never reached the provider: one signal turn item plus
      // terminal run, attempt and root node, written only while the run is still
      // the current starting attempt.
      const settleRunBeforeStart = Effect.fn("orchestrationV2.providerTurnStart.settleBeforeStart")(
        function* (input: {
          readonly signal: string;
          readonly status: "completed" | "failed";
          readonly now: DateTime.Utc;
          /** Omitted when the run never started, so `startedAt` stays as projected. */
          readonly startedAt?: DateTime.Utc;
          readonly providerInstanceId: OrchestrationV2Run["providerInstanceId"];
          readonly itemProviderThreadId: OrchestrationV2ProviderThread["id"];
          readonly item:
            | Pick<
                Extract<OrchestrationV2TurnItem, { type: "error" }>,
                "type" | "title" | "failure"
              >
            | Pick<
                Extract<OrchestrationV2TurnItem, { type: "command_execution" }>,
                "type" | "title" | "input" | "output" | "exitCode"
              >;
          /** Emitted after the run events when the provider thread should go idle. */
          readonly providerThreadUpdate?: OrchestrationV2ProviderThread;
        }) {
          const { now, status } = input;
          const started = input.startedAt === undefined ? {} : { startedAt: input.startedAt };
          const item: OrchestrationV2TurnItem = {
            id: idAllocator.derive.runSignalTurnItem({ runId, signal: input.signal }),
            threadId: projection.thread.id,
            runId,
            nodeId: rootNode.id,
            providerThreadId: input.itemProviderThreadId,
            providerTurnId: null,
            nativeItemRef: null,
            parentItemId: null,
            ordinal:
              Math.max(
                0,
                ...projection.turnItems
                  .filter((item) => item.runId === runId)
                  .map((item) => item.ordinal),
              ) + 1,
            status,
            startedAt: now,
            completedAt: now,
            updatedAt: now,
            ...input.item,
          };
          const eventPayloads = [
            { type: "turn-item.updated", payload: item },
            { type: "run.updated", payload: { ...run, status, ...started, completedAt: now } },
            {
              type: "run-attempt.updated",
              payload: { ...attempt, status, ...started, completedAt: now },
            },
            {
              type: "node.updated",
              payload: { ...rootNode, status, ...started, completedAt: now },
            },
            ...(input.providerThreadUpdate === undefined
              ? []
              : [
                  {
                    type: "provider-thread.updated" as const,
                    payload: input.providerThreadUpdate,
                  },
                ]),
          ] as const;
          const events = yield* Effect.forEach(eventPayloads, (event) =>
            Effect.gen(function* () {
              return {
                ...event,
                id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
                threadId: projection.thread.id,
                runId,
                nodeId: rootNode.id,
                providerInstanceId: input.providerInstanceId,
                occurredAt: now,
              } satisfies OrchestrationV2DomainEvent;
            }),
          );
          return yield* eventSink.writeIfRunCurrent({
            threadId: projection.thread.id,
            runId,
            activeAttemptId: attempt.id,
            expectedStatus: "starting",
            events,
          });
        },
      );
      if (message.attachments.length === 0 && message.text.trimStart().startsWith("/")) {
        const isEmptyCompaction =
          message.text.trim().toLowerCase() === "/compact" && !projection.hasConversation;
        // Preparing a run may already point the thread at a newly selected
        // provider. Account commands still belong to its last native session.
        const nativeThreads = new Map(
          projection.providerThreads
            .filter(
              (candidate) => candidate.ownerNodeId === null && candidate.nativeThreadRef !== null,
            )
            .map((candidate) => [candidate.id, candidate]),
        );
        const previousNativeRun = projection.runs.reduce<OrchestrationV2Run | undefined>(
          (previous, candidate) =>
            candidate.ordinal < run.ordinal &&
            candidate.providerThreadId !== null &&
            nativeThreads.has(candidate.providerThreadId) &&
            (previous === undefined || candidate.ordinal > previous.ordinal)
              ? candidate
              : previous,
          undefined,
        );
        const nativeThread = nativeThreads.get(
          previousNativeRun?.providerThreadId ??
            projection.thread.activeProviderThreadId ??
            providerThread.id,
        );
        const authInstanceId = nativeThread?.providerInstanceId ?? run.providerInstanceId;
        yield* revalidateOrdinaryExecution;
        const authResult = isEmptyCompaction
          ? null
          : yield* Effect.result(
              providerAuth.tryHandlePromptCommand({
                instanceId: authInstanceId,
                text: projectComposerContextForProvider({
                  text: message.text,
                  records: message.context?.records ?? [],
                }),
                hasAttachments: false,
              }),
            );
        if (isEmptyCompaction || authResult?._tag === "Failure" || authResult?.success) {
          const now = yield* DateTime.now;
          const failure = isEmptyCompaction
            ? makeProviderFailure({
                class: "validation_error",
                message: "Start a conversation before compacting this thread.",
              })
            : authResult?._tag === "Failure"
              ? makeProviderFailure({
                  class: "permission_error",
                  message: authResult.failure.detail,
                })
              : undefined;
          const status = failure === undefined ? "completed" : "failed";
          yield* settleRunBeforeStart({
            signal: isEmptyCompaction ? "empty-compaction" : "provider-sign-out",
            status,
            now,
            startedAt: now,
            providerInstanceId: authInstanceId,
            itemProviderThreadId: nativeThread?.id ?? providerThread.id,
            item:
              failure !== undefined
                ? {
                    type: "error",
                    title: isEmptyCompaction
                      ? "Cannot compact an empty thread"
                      : "Provider sign-out failed",
                    failure,
                  }
                : {
                    type: "command_execution",
                    title: "Provider signed out",
                    input: message.text.trim(),
                    output: "Provider signed out",
                    exitCode: 0,
                  },
            providerThreadUpdate: {
              ...providerThread,
              status: providerThread.nativeThreadRef === null ? "not_loaded" : "idle",
              updatedAt: now,
            },
          });
          return;
        }
      }
      const { worktreePath, branch } = projection.thread;
      if (worktreePath !== null && branch !== null) {
        const exists = yield* fileSystem
          .exists(worktreePath)
          .pipe(Effect.orElseSucceed(() => true));
        if (!exists) {
          if (ordinaryExecution !== undefined) {
            const project = yield* projects.getById(projection.thread.projectId);
            if (Option.isNone(project))
              return yield* new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
                reason: "unavailable",
                threadId: projection.thread.id,
                path: worktreePath,
                message: "The admitted checkout project is unavailable.",
              });
            const projectCwd = project.value.workspaceRoot;
            const revalidateCreation = Effect.gen(function* () {
              yield* revalidateOrdinaryExecution;
              const original = yield* eventSink.readOrdinaryCheckoutUse(
                ordinaryExecution.originalUse.operationId,
              );
              const current = yield* projectionStore.getTurnStartContext(input.threadId, runId);
              const currentRun = current.runs.find((candidate) => candidate.id === runId);
              if (
                original === null ||
                original.subject.source.projectWorkspaceRoot !== projectCwd ||
                original.subject.source.worktreePath !== worktreePath ||
                (yield* Schema.encodeEffect(
                  Schema.fromJsonString(OrdinaryCheckout.OrdinaryCheckoutUseV1),
                )(original.subject.use).pipe(Effect.orDie)) !==
                  (yield* Schema.encodeEffect(
                    Schema.fromJsonString(OrdinaryCheckout.OrdinaryCheckoutUseV1),
                  )(ordinaryExecution.originalUse).pipe(Effect.orDie)) ||
                current.thread.worktreePath !== worktreePath ||
                current.thread.branch !== branch ||
                currentRun?.status !== "starting" ||
                currentRun.activeAttemptId !== attempt.id
              )
                return yield* new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
                  reason: "target_changed",
                  threadId: projection.thread.id,
                  path: worktreePath,
                  message: "The captured ordinary checkout creation target changed.",
                });
            });
            yield* revalidateCreation;
            // The exact checkout grant covers creation; repository-wide metadata pruning has separate ownership.
            const created = yield* gitWorkflow
              .createWorktree(
                { cwd: projectCwd, refName: branch, path: worktreePath },
                { revalidateMutation: revalidateCreation },
              )
              .pipe(Effect.mapError((cause) => new ProviderTurnStartError({ runId, cause })));
            if (
              (created.worktree.path !== worktreePath &&
                created.worktree.path !== ordinaryExecution.originalUse.lease.resourcePath) ||
              created.worktree.refName !== branch
            )
              return yield* new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
                reason: "unknown_use",
                threadId: projection.thread.id,
                path: worktreePath,
                message: "Ordinary checkout creation returned a different physical target.",
              });
            yield* revalidateCreation;
          } else {
            if (input.nativeCreationExecutionContext !== undefined) {
              return yield* new ProviderNativeOperationUnknownError({
                nativeEffect: {
                  operationId: `native-worktree:${attempt.id}:${NodeCrypto.randomUUID()}`,
                  operation: "open_session",
                  threadId: projection.thread.id,
                  instanceId: run.providerInstanceId,
                  providerSessionId: providerThread.providerSessionId,
                  providerThreadId: providerThread.id,
                  attemptId: attempt.id,
                  outcome: "unknown",
                },
                cause:
                  "Missing native worktree requires its own durable prune and create stage starts.",
              });
            }
            const project = yield* projects.getById(projection.thread.projectId).pipe(
              Effect.map(Option.getOrUndefined),
              Effect.orElseSucceed(() => undefined),
            );
            if (project !== undefined) {
              yield* Effect.logWarning("provider turn start recreating missing worktree", {
                threadId: projection.thread.id,
                worktreePath,
                branch,
              });
              yield* gitWorkflow.pruneWorktrees({ cwd: project.workspaceRoot }).pipe(
                Effect.andThen(
                  gitWorkflow.createWorktree({
                    cwd: project.workspaceRoot,
                    refName: branch,
                    path: worktreePath,
                  }),
                ),
                Effect.catchCause((cause) =>
                  Cause.hasInterruptsOnly(cause)
                    ? Effect.failCause(cause)
                    : Effect.logWarning("provider turn start failed to recreate worktree", {
                        threadId: projection.thread.id,
                        worktreePath,
                        cause: Cause.pretty(cause),
                      }),
                ),
              );
            }
          }
        }
      }
      const selectInheritedBackgroundItems = (
        current: ProjectionStore.ProjectionRuntimeRecoveryState,
      ): ReturnType<typeof RunExecutionService.selectInheritedBackgroundTurnItems> =>
        RunExecutionService.selectInheritedBackgroundTurnItems({
          threadId: current.thread.id,
          currentProviderThreadId: providerThread.id,
          currentRunOrdinal: run.ordinal,
          runs: current.runs,
          turnItems: current.turnItems,
        });
      const inheritedBackgroundTurnItems = yield* projectionStore
        .getRuntimeRecoveryProjection(projection.thread.id)
        .pipe(Effect.map(selectInheritedBackgroundItems));
      const providerSessionId = providerThread.providerSessionId;
      const runControls = makeRunControls({
        threadId: projection.thread.id,
        runId: run.id,
        attemptId: attempt.id,
        providerThreadId: providerThread.id,
        runOrdinal: run.ordinal,
        inheritedBackgroundTurnItems,
      });
      const { isCurrentAttemptInStatus } = runControls;

      const resolvedRuntimePolicy = yield* runtimePolicy.resolve({
        thread: projection.thread,
        modelSelection: run.modelSelection,
      });
      const existingSessionProjection = projection.providerSessions.find(
        (candidate) => candidate.id === providerSessionId,
      );
      const nativeOperation = (
        operation: ProviderNativeOperationContext["operation"],
        runtime?: ProviderAdapterV2SessionRuntime,
      ): ProviderNativeOperationContext => ({
        operationId: `provider-turn:${attempt.id}:${operation}:${NodeCrypto.randomUUID()}`,
        operation,
        instanceId: run.providerInstanceId,
        threadId: projection.thread.id,
        providerSessionId,
        providerThreadId: providerThread.id,
        attemptId: attempt.id,
        ...(runtime?.runtimeGeneration === undefined
          ? {}
          : { runtimeGeneration: runtime.runtimeGeneration }),
      });
      const openOperation = nativeOperation("open_session");
      const nativeThreadIdBeforeOpen = providerThread.nativeThreadRef?.nativeId;
      let strictResumeSource:
        | { readonly driverKind: typeof providerThread.driver; readonly continuationKey: string }
        | undefined;
      if (nativeThreadIdBeforeOpen != null) {
        const disposition = yield* eventSink
          .readLegacyContinuationDisposition(projection.thread.id)
          .pipe(
            Effect.mapError(
              (cause) =>
                new ProviderNativeOperationUnknownError({
                  nativeEffect: { ...openOperation, outcome: "unknown" },
                  cause,
                }),
            ),
          );
        const confirmedChoiceLineage =
          disposition === null && projection.thread.historyOrigin !== "v1_import"
            ? null
            : yield* Effect.gen(function* () {
                const owner = yield* eventSink.readCurrentProviderRuntimeOwner(
                  projection.thread.id,
                );
                if (
                  owner === null ||
                  owner.binding.providerThreadId !== providerThread.id ||
                  owner.binding.providerSessionId !== providerSessionId ||
                  owner.binding.instanceId !== run.providerInstanceId ||
                  owner.binding.driver !== providerThread.driver ||
                  owner.binding.nativeThreadId !== nativeThreadIdBeforeOpen
                )
                  return null;
                return yield* eventSink.readConfirmedImportedHistoryContinuation({
                  expectedBinding: owner.binding,
                  expectedEvidenceRevision: owner.evidenceRevision,
                });
              }).pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderNativeOperationUnknownError({
                      nativeEffect: { ...openOperation, outcome: "unknown" },
                      cause,
                    }),
                ),
              );
        if (confirmedChoiceLineage !== null) {
          strictResumeSource = {
            driverKind: confirmedChoiceLineage.currentSource.driverKind,
            continuationKey: confirmedChoiceLineage.currentSource.continuationKey,
          };
        } else if (disposition !== null) {
          const evidence = disposition.evidence;
          if (
            disposition.qualification.type !== "qualified" ||
            evidence === null ||
            evidence.driver !== providerThread.driver ||
            evidence.nativeThreadId !== nativeThreadIdBeforeOpen ||
            disposition.qualification.nativeThreadId !== nativeThreadIdBeforeOpen ||
            evidence.stoppedProof === null ||
            evidence.historicalSourceIdentity === null ||
            evidence.accessibility === null ||
            evidence.accessibility.providerInstanceId !== run.providerInstanceId ||
            evidence.accessibility.continuationKey !== disposition.qualification.continuationKey
          ) {
            return yield* new ProviderNativeOperationUnknownError({
              nativeEffect: { ...openOperation, outcome: "unknown" },
              cause:
                "Imported native continuation lacks its exact immutable historical qualification.",
            });
          }
          strictResumeSource = {
            driverKind: evidence.driver,
            continuationKey: disposition.qualification.continuationKey,
          };
        } else if (
          confirmedChoiceLineage === null &&
          projection.thread.historyOrigin === "v1_import"
        ) {
          return yield* new ProviderNativeOperationUnknownError({
            nativeEffect: { ...openOperation, outcome: "unknown" },
            cause: "Imported continuation disposition is missing.",
          });
        }
        if (
          input.nativeCreationExecutionContext !== undefined &&
          strictResumeSource === undefined
        ) {
          const historical = yield* eventSink.readProviderRuntimeEvidence(projection.thread.id);
          const source =
            historical === null
              ? null
              : yield* eventSink.readProviderContinuationSourceIdentity(historical.binding);
          if (
            historical === null ||
            source === null ||
            historical.binding.threadId !== projection.thread.id ||
            historical.binding.providerThreadId !== providerThread.id ||
            historical.binding.driver !== providerThread.driver ||
            historical.binding.nativeThreadId !== nativeThreadIdBeforeOpen ||
            source.driverKind !== historical.binding.driver ||
            source.runtimeGeneration !== historical.binding.runtimeGeneration
          ) {
            return yield* new ProviderNativeOperationUnknownError({
              nativeEffect: { ...openOperation, outcome: "unknown" },
              cause:
                "Native continuation has no exact retained source identity from its actual prior incarnation.",
            });
          }
          strictResumeSource = source;
        }
      }
      const capturedResumeSource = strictResumeSource;
      const nativeCreationExecution: ProviderAdapterV2OpenSessionInput["nativeCreationExecution"] =
        input.nativeCreationExecutionContext === undefined
          ? undefined
          : yield* Effect.gen(function* () {
              if (
                worktreePath === null ||
                branch === null ||
                resolvedRuntimePolicy.cwd !== worktreePath
              ) {
                return yield* new ProviderNativeOperationUnknownError({
                  nativeEffect: { ...openOperation, outcome: "unknown" },
                  cause: "Native execution lacks its exact current worktree resources.",
                });
              }
              const project = yield* projects.getById(projection.thread.projectId);
              if (Option.isNone(project)) {
                return yield* new ProviderNativeOperationUnknownError({
                  nativeEffect: { ...openOperation, outcome: "unknown" },
                  cause: "Native execution project resources are unavailable.",
                });
              }
              const execution = {
                context: input.nativeCreationExecutionContext!,
                resources: { projectCwd: project.value.workspaceRoot, branch, worktreePath },
              };
              yield* authorizeNativeCreationExecution(execution.context, {
                stage: "native_command",
                resources: execution.resources,
              });
              return execution;
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderNativeOperationUnknownError({
                    nativeEffect: { ...openOperation, outcome: "unknown" },
                    cause,
                  }),
              ),
            );
      if (ordinaryExecution !== undefined && input.onOrdinaryManagedRunStarted === undefined)
        return yield* new ProviderTurnStartError({
          runId,
          cause: "The ordinary start has no scoped activation handoff.",
        });
      yield* revalidateOrdinaryExecution;
      const sessionResult = yield* Effect.result(
        providerSessions.open({
          threadId: projection.thread.id,
          providerSessionId,
          modelSelection: run.modelSelection,
          runtimePolicy: resolvedRuntimePolicy,
          nativeOperation: openOperation,
          ...(nativeCreationExecution === undefined ? {} : { nativeCreationExecution }),
          ...(existingSessionProjection === undefined
            ? {}
            : { resumeFromSession: existingSessionProjection }),
          ...(nativeThreadIdBeforeOpen == null || strictResumeSource !== undefined
            ? {}
            : { initialNativeThreadId: nativeThreadIdBeforeOpen }),
          ...(providerThread.nativeMetadata?.itemIdentityVersion === undefined
            ? {}
            : {
                initialProviderItemIdentityVersion:
                  providerThread.nativeMetadata.itemIdentityVersion,
              }),
        }),
      );
      // The last start attempt fails the run with the provider's own reason
      // instead of leaving it `starting` after the effect gives up. A run that
      // already left `starting` is not overwritten, and a failed write returns
      // its error to the effect worker.
      const settleStartFailure = (failed: {
        readonly signal: string;
        readonly title: string;
        readonly error: Error;
      }) =>
        Effect.gen(function* () {
          const nestedCause = "cause" in failed.error ? failed.error.cause : undefined;
          return yield* settleRunBeforeStart({
            signal: failed.signal,
            status: "failed",
            now: yield* DateTime.now,
            providerInstanceId: run.providerInstanceId,
            itemProviderThreadId: providerThread.id,
            item: {
              type: "error",
              title: failed.title,
              failure: makeProviderFailure({
                cause: failed.error,
                message:
                  nestedCause instanceof Error
                    ? nestedCause.message
                    : typeof nestedCause === "string"
                      ? nestedCause
                      : failed.error.message,
                class: "provider_error",
              }),
            },
          });
        });
      if (sessionResult._tag === "Failure") {
        const evidence = nativeEffectEvidenceFor(sessionResult.failure, openOperation);
        if (evidence.outcome !== "known_no_effect")
          return yield* new ProviderNativeOperationUnknownError({
            nativeEffect: { ...evidence, outcome: "unknown" },
            cause: sessionResult.failure,
          });
        if (input.willRetry === true) {
          const retrySink = yield* StartRetryOutcomeSink;
          if (
            retrySink !== undefined &&
            ordinaryExecution !== undefined &&
            openOperation.operation === "open_session" &&
            (yield* isCurrentAttemptInStatus("starting"))
          ) {
            yield* revalidateOrdinaryExecution;
            const observation: EventSink.StartRetryBeforeOpenObservationV1 = Object.freeze({
              version: 1,
              schema: "t3.start-retry-before-open/v1",
              execution: ordinaryExecution,
              run: {
                runId: run.id,
                runAttemptId: attempt.id,
                nodeId: rootNode.id,
                messageId: message.id,
              },
              providerInstanceId: run.providerInstanceId,
              providerThreadId: providerThread.id,
              providerSessionId,
              checkpointScopeId: rootNode.checkpointScopeId,
              attemptedOperation: openOperation,
              nativeEffect: evidence,
              completedAt: DateTime.formatIso(yield* DateTime.now),
            });
            const snapshot = nativeCreationCanonicalJson(
              yield* Schema.encodeEffect(EventSink.StartRetryBeforeOpenObservationV1)(observation),
            );
            issuedRetryStarts.set(observation, { observation, snapshot });
            yield* retrySink(observation);
          }
          return yield* sessionResult.failure;
        }
        const settled = yield* settleStartFailure({
          signal: "provider-session-open-failure",
          title: "Provider session failed to open",
          error: sessionResult.failure,
        });
        const sink = yield* StartOutcomeSink;
        if (
          sink !== undefined &&
          ordinaryExecution !== undefined &&
          input.willRetry === false &&
          settled.committed &&
          settled.storedEvents.length === 4 &&
          openOperation.operation === "open_session"
        ) {
          const observation: EventSink.StartFailedBeforeOpenObservationV1 = Object.freeze({
            version: 1,
            schema: "t3.start-failed-before-open/v1",
            execution: ordinaryExecution,
            run: {
              runId: run.id,
              runAttemptId: attempt.id,
              nodeId: rootNode.id,
              messageId: message.id,
            },
            providerInstanceId: run.providerInstanceId,
            providerThreadId: providerThread.id,
            providerSessionId,
            checkpointScopeId: rootNode.checkpointScopeId,
            attemptedOperation: openOperation,
            nativeEffect: evidence,
            completedAt: DateTime.formatIso(yield* DateTime.now),
            terminalEvents: settled.storedEvents.map((stored) => ({
              eventId: stored.event.id,
              sequence: stored.sequence,
            })),
            terminalPayloadSha256: nativeCreationSha256(
              nativeCreationCanonicalJson(
                settled.storedEvents.map(
                  (stored) => Schema.encodeSync(OrchestrationV2StoredEventJson)(stored).event,
                ),
              ),
            ),
          });
          const snapshot = nativeCreationCanonicalJson(
            yield* Schema.encodeEffect(EventSink.StartFailedBeforeOpenObservationV1)(
              observation,
            ).pipe(Effect.orDie),
          );
          yield* eventSink.onCommit(
            Effect.sync(() => {
              issuedFailedStarts.set(observation, { observation, snapshot });
            }),
          );
          yield* sink(observation);
        }
        return;
      }
      const session = sessionResult.success;
      let startupAttachment:
        | ProviderSessionManager.ProviderOrdinaryExecutionAttachmentV1
        | undefined;
      const captureOrdinaryRuntime = (target: OrchestrationV2ProviderThread) =>
        providerSessions
          .captureOrdinaryExecutionAttachment({
            runtime: session,
            threadId: projection.thread.id,
            providerThread: target,
            runId: run.id,
            attemptId: attempt.id,
          })
          .pipe(
            Effect.tap((captured) =>
              Effect.sync(() => {
                startupAttachment = captured;
              }),
            ),
          );
      const stopCapturedStartup =
        ordinaryExecution === undefined
          ? Effect.void
          : Effect.suspend(() =>
              startupAttachment === undefined
                ? Effect.void
                : startupAttachment
                    .stopCaptured({
                      operationId: `${ordinaryExecution.originalUse.operationId}:captured-startup-stop`,
                    })
                    .pipe(Effect.asVoid),
            );
      if (ordinaryExecution !== undefined) yield* captureOrdinaryRuntime(providerThread);
      let lastSetupOperation = openOperation;
      let setupEffectOccurred = false;
      // Only the provider's own thread load fails the run on the last attempt;
      // store, id and handoff failures around it keep their typed errors.
      const loadFromProvider = (
        load: Effect.Effect<OrchestrationV2ProviderThread, ProviderAdapterV2Error>,
        operation: ProviderNativeOperationContext,
      ) =>
        Effect.gen(function* () {
          lastSetupOperation = operation;
          yield* revalidateOrdinaryExecution;
          const loaded = yield* Effect.result(withProviderNativeEffect(load, operation));
          if (loaded._tag === "Success") {
            setupEffectOccurred = true;
            return loaded.success;
          }
          const evidence = nativeEffectEvidenceFor(loaded.failure, operation);
          if (evidence.outcome !== "known_no_effect")
            return yield* new ProviderNativeOperationUnknownError({
              nativeEffect: { ...evidence, outcome: "unknown" },
              cause: loaded.failure,
            });
          if (input.willRetry === true) return yield* loaded.failure;
          yield* settleStartFailure({
            signal: "provider-thread-load-failure",
            title: "Provider turn failed to start",
            error: loaded.failure,
          });
          return undefined;
        });
      let effectiveHandoffs = handoffs;
      const loadedProviderThread = yield* Effect.gen(function* () {
        if (nativeForkTransfer !== undefined) {
          if (nativeCreationExecution !== undefined) {
            return yield* new ProviderNativeOperationUnknownError({
              nativeEffect: { ...nativeOperation("fork_thread", session), outcome: "unknown" },
              cause:
                "Native fork lacks a current operation authority carrier at its actual callee.",
            });
          }
          const sourceProjection = yield* projectionStore.getThreadRecords(
            nativeForkTransfer.sourceThreadId,
            ["runs", "providerThreads", "attempts", "providerTurns"],
          );
          const sourceRun = sourceProjection.runs.find(
            (candidate) => candidate.id === nativeForkTransfer.sourcePoint.runId,
          );
          const sourceProviderThread = sourceProjection.providerThreads.find(
            (candidate) => candidate.id === sourceRun?.providerThreadId,
          );
          const sourceAttempt = sourceProjection.attempts.find(
            (candidate) => candidate.id === sourceRun?.activeAttemptId,
          );
          const sourceProviderTurn = sourceProjection.providerTurns.find(
            (candidate) =>
              candidate.id === sourceAttempt?.providerTurnId ||
              candidate.runAttemptId === sourceAttempt?.id,
          );
          if (sourceRun === undefined || sourceProviderThread === undefined) {
            return yield* new ProviderTurnStartError({
              runId,
              cause: `Native fork transfer ${nativeForkTransfer.id} has no source provider execution.`,
            });
          }
          const operation = nativeOperation("fork_thread", session);
          return yield* loadFromProvider(
            session.forkThread({
              nativeOperation: operation,
              sourceProviderThread,
              sourceProviderTurns: sourceProjection.providerTurns,
              targetThreadId: projection.thread.id,
              modelSelection: run.modelSelection,
              runtimePolicy: resolvedRuntimePolicy,
              ...(sourceProviderTurn === undefined
                ? {}
                : { providerTurnId: sourceProviderTurn.id }),
            }),
            operation,
          );
        }
        if (providerThread.nativeThreadRef === null) {
          // Hand the run's provider thread to the adapter so it adopts this
          // row's identity when attaching native state. An adapter that mints
          // its own row instead leaves two live rows per app thread, and
          // `activeProviderThreadId` then flaps between them on every update.
          const operation = nativeOperation("ensure_thread", session);
          return yield* loadFromProvider(
            session.ensureThread({
              nativeOperation: operation,
              ...(nativeCreationExecution === undefined ? {} : { nativeCreationExecution }),
              threadId: projection.thread.id,
              modelSelection: run.modelSelection,
              runtimePolicy: resolvedRuntimePolicy,
              providerSessionId,
              existingProviderThread: providerThread,
            }),
            operation,
          );
        }
        const uncertainDelivery = projection.contextHandoffs.some(
          (handoff) =>
            handoff.toProviderThreadId === providerThread.id &&
            handoff.delivery?.nativeThreadId === providerThread.nativeThreadRef?.nativeId &&
            handoff.delivery?.status === "pending",
        );
        const resumeOperation = nativeOperation("resume_thread", session);
        lastSetupOperation = resumeOperation;
        if (uncertainDelivery)
          return yield* new ProviderNativeOperationUnknownError({
            nativeEffect: { ...resumeOperation, outcome: "unknown" },
            cause: "Uncertain native history injection",
          });
        yield* revalidateOrdinaryExecution;
        const resumed = yield* Effect.result(
          withProviderNativeEffect(
            session.resumeThread({
              nativeOperation: resumeOperation,
              ...(nativeCreationExecution === undefined ? {} : { nativeCreationExecution }),
              providerThread,
              threadId: projection.thread.id,
              modelSelection: run.modelSelection,
              runtimePolicy: resolvedRuntimePolicy,
              ...(capturedResumeSource === undefined
                ? {}
                : {
                    beforeNativeResume: (actual) =>
                      Effect.gen(function* () {
                        const generation = session.runtimeGeneration;
                        const resident = yield* providerSessions.get(providerSessionId);
                        const current = yield* projectionStore.getRuntimeRecoveryProjection(
                          projection.thread.id,
                        );
                        const currentRun = current.runs.find(
                          (candidate) => candidate.id === run.id,
                        );
                        const currentThread = current.providerThreads.find(
                          (candidate) => candidate.id === providerThread.id,
                        );
                        const after = yield* providerSessions.get(providerSessionId);
                        if (
                          actual === undefined ||
                          generation === undefined ||
                          actual.runtimeGeneration !== generation ||
                          actual.driverKind !== capturedResumeSource.driverKind ||
                          actual.continuationKey !== capturedResumeSource.continuationKey ||
                          session.driver !== actual.driverKind ||
                          session.instanceId !== run.providerInstanceId ||
                          Option.isNone(resident) ||
                          resident.value !== session ||
                          Option.isNone(after) ||
                          after.value !== session ||
                          currentRun?.activeAttemptId !== attempt.id ||
                          currentRun.status !== "starting" ||
                          currentRun.providerThreadId !== providerThread.id ||
                          !modelSelectionsEqual(currentRun.modelSelection, run.modelSelection) ||
                          current.thread.activeProviderThreadId !== providerThread.id ||
                          current.thread.modelSelection.instanceId !== run.providerInstanceId ||
                          currentThread?.providerSessionId !== providerSessionId ||
                          currentThread.nativeThreadRef?.nativeId !== nativeThreadIdBeforeOpen ||
                          session.runtimeGeneration !== generation
                        ) {
                          return yield* new ProviderAdapterResumeThreadError({
                            driver: session.driver,
                            providerSessionId,
                            providerThreadId: providerThread.id,
                            nativeEffect: {
                              ...resumeOperation,
                              ...(generation === undefined
                                ? {}
                                : { runtimeGeneration: generation }),
                              outcome: "unknown",
                            },
                            cause:
                              "The initialized target source identity or current continuation binding is unproved or changed.",
                          });
                        }
                      }).pipe(
                        Effect.mapError((cause) =>
                          Schema.is(ProviderAdapterResumeThreadError)(cause)
                            ? cause
                            : new ProviderAdapterResumeThreadError({
                                driver: session.driver,
                                providerSessionId,
                                providerThreadId: providerThread.id,
                                nativeEffect: nativeEffectEvidenceFor(cause, resumeOperation),
                                cause,
                              }),
                        ),
                      ),
                  }),
            }),
            resumeOperation,
          ),
        );
        if (resumed._tag === "Success") {
          setupEffectOccurred = true;
          return resumed.success;
        }
        const resumeEvidence = nativeEffectEvidenceFor(resumed.failure, resumeOperation);
        if (resumeEvidence.outcome !== "known_no_effect" || strictResumeSource !== undefined)
          return yield* new ProviderNativeOperationUnknownError({
            nativeEffect: { ...resumeEvidence, outcome: "unknown" },
            cause: resumed.failure,
          });

        yield* Effect.logWarning("Provider resume failed; attempting a fresh native session", {
          driver: session.driver,
          providerThreadId: providerThread.id,
          runId,
          reason: "resume_proven_no_effect",
          errorTag: resumed.failure._tag,
        });
        const replacementOperation = nativeOperation("ensure_thread", session);
        const replacement = yield* loadFromProvider(
          session.ensureThread({
            nativeOperation: replacementOperation,
            ...(nativeCreationExecution === undefined ? {} : { nativeCreationExecution }),
            threadId: projection.thread.id,
            modelSelection: run.modelSelection,
            runtimePolicy: resolvedRuntimePolicy,
            providerSessionId,
            // The native ref is dropped so the adapter binds a fresh native
            // session instead of retrying the resume that just failed, while
            // still adopting this row's identity.
            existingProviderThread: { ...providerThread, nativeThreadRef: null },
          }),
          replacementOperation,
        );
        if (replacement === undefined) return undefined;
        const transferId = yield* idAllocator.allocate.contextTransfer({
          sourceThreadId: projection.thread.id,
          targetThreadId: projection.thread.id,
          type: "provider_resume_fallback",
        });
        const createdAt = yield* DateTime.now;
        const handoff = yield* contextHandoffService.prepareProviderHandoff({
          threadId: projection.thread.id,
          targetRunId: run.id,
          transferId,
          fromProviderThreadIds: [providerThread.id],
          toProviderThreadId: providerThread.id,
          fromProviderInstanceId: providerThread.providerInstanceId,
          toProviderInstanceId: run.providerInstanceId,
          coveredRunOrdinals: { from: 1, to: Math.max(1, run.ordinal - 1) },
          strategy: "full_thread_summary",
          runs: projection.runs,
          items: (yield* projectionStore.getTurnStartHistory(input.threadId)).filter(
            (item) =>
              item.runId === null ||
              projection.runs.some(
                (source) => source.id === item.runId && source.ordinal < run.ordinal,
              ),
          ),
          createdAt,
        });
        effectiveHandoffs = [handoff, ...effectiveHandoffs];
        yield* eventSink.write({
          events: [
            {
              id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
              type: "context-handoff.updated",
              threadId: projection.thread.id,
              runId: run.id,
              providerInstanceId: run.providerInstanceId,
              occurredAt: createdAt,
              payload: handoff,
            },
            {
              id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
              type: "context-transfer.updated",
              threadId: projection.thread.id,
              runId: run.id,
              providerInstanceId: run.providerInstanceId,
              occurredAt: createdAt,
              payload: {
                id: transferId,
                type: "provider_handoff",
                sourceThreadId: projection.thread.id,
                targetThreadId: projection.thread.id,
                sourcePoint: { threadId: projection.thread.id },
                basePoint: null,
                sourceProviderInstanceId: providerThread.providerInstanceId,
                targetProviderInstanceId: run.providerInstanceId,
                targetRunId: run.id,
                status: "resolved_portable",
                resolution: { strategy: "portable_context", contextHandoffId: handoff.id },
                createdBy: "system",
                error: null,
                createdAt,
                updatedAt: createdAt,
                consumedAt: null,
              },
            },
          ],
        });
        return replacement;
      }).pipe(
        Effect.onError(() => stopCapturedStartup),
        Effect.mapError((cause) =>
          setupEffectOccurred
            ? new ProviderNativeOperationUnknownError({
                nativeEffect: { ...lastSetupOperation, outcome: "unknown" },
                cause,
              })
            : cause,
        ),
      );
      // The last attempt already failed the run.
      if (loadedProviderThread === undefined) return;
      if (ordinaryExecution !== undefined) yield* captureOrdinaryRuntime(loadedProviderThread);
      if (!(yield* isCurrentAttemptInStatus("starting"))) {
        if (ordinaryExecution !== undefined) {
          yield* stopCapturedStartup;
          return yield* new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
            reason: "unknown_use",
            threadId: projection.thread.id,
            path: ordinaryExecution.originalUse.lease.resourcePath,
            message: "The actual setup target was captured after this run attempt was superseded.",
          });
        }
        return;
      }
      const now = yield* DateTime.now;
      // Only started runs reached the provider-thread update below. Queued runs and
      // failures during session setup cannot establish a new telemetry selection.
      const measuredContext = latestNativeContextUsage(projection, providerThread);
      const previousSelection =
        measuredContext?.modelSelection ??
        projection.runs.findLast(
          (source) =>
            source.ordinal < run.ordinal &&
            source.startedAt !== null &&
            source.providerThreadId === providerThread.id,
        )?.modelSelection;
      const sameSelection =
        previousSelection === undefined ||
        modelSelectionsEqual(previousSelection, run.modelSelection);
      const sameNativeThread =
        loadedProviderThread.nativeThreadRef?.nativeId === providerThread.nativeThreadRef?.nativeId;
      const threadUsage = loadedProviderThread.contextUsage ?? providerThread.contextUsage;
      const previousUsage = measuredContext
        ? { ...threadUsage, ...measuredContext.usage }
        : threadUsage;
      const reuseTelemetry =
        sameSelection ||
        (previousSelection !== undefined &&
          session.canReuseContextUsage?.(previousSelection, run.modelSelection) === true);
      const knownModelWindow = session.getModelContextWindow?.(
        run.modelSelection,
        resolvedRuntimePolicy.cwd,
      );
      // Persist before delivery. Keep this native transcript's measured
      // occupancy. A different model drops compaction telemetry and uses the
      // new window when that window is known.
      const handoffUsage = contextUsageForHandoff({
        sameNativeThread,
        sameSelection,
        reuseTelemetry,
        previousUsage,
        knownModelWindow,
      });
      const runningProviderThread: OrchestrationV2ProviderThread = {
        ...loadedProviderThread,
        contextUsage: handoffUsage,
        id: providerThread.id,
        driver: session.driver,
        providerInstanceId: run.providerInstanceId,
        providerSessionId,
        appThreadId: projection.thread.id,
        ownerNodeId: providerThread.ownerNodeId,
        firstRunOrdinal: providerThread.firstRunOrdinal ?? run.ordinal,
        lastRunOrdinal: run.ordinal,
        handoffIds: providerThread.handoffIds,
        forkedFrom: providerThread.forkedFrom,
        status: "active",
        createdAt: providerThread.createdAt,
        updatedAt: now,
      };
      const runningRun: OrchestrationV2Run = {
        ...run,
        status: "running",
        startedAt: now,
      };
      const runningAttempt: OrchestrationV2RunAttempt = {
        ...attempt,
        ...(runningProviderThread.nativeThreadRef?.nativeId == null
          ? {}
          : { nativeThreadId: runningProviderThread.nativeThreadRef.nativeId }),
        status: "running",
        startedAt: now,
      };
      const runningRootNode: OrchestrationV2ExecutionNode = {
        ...rootNode,
        status: "running",
        startedAt: now,
      };
      const events: Array<OrchestrationV2DomainEvent> = [
        {
          id: yield* idAllocator.allocate.event({
            threadId: projection.thread.id,
            providerSessionId,
          }),
          type: "provider-session.updated",
          threadId: projection.thread.id,
          driver: session.driver,
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: session.providerSession,
        },
        {
          id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
          type: "provider-thread.updated",
          threadId: projection.thread.id,
          driver: session.driver,
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: runningProviderThread,
        },
        ...(nativeForkTransfer === undefined || runningProviderThread.nativeThreadRef === null
          ? []
          : [
              {
                id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
                type: "context-transfer.updated" as const,
                threadId: projection.thread.id,
                runId: run.id,
                driver: session.driver,
                providerInstanceId: run.providerInstanceId,
                occurredAt: now,
                payload: {
                  ...nativeForkTransfer,
                  targetProviderInstanceId: run.providerInstanceId,
                  targetRunId: run.id,
                  status: "consumed" as const,
                  resolution: {
                    strategy: "native_fork" as const,
                    providerThreadRef: runningProviderThread.nativeThreadRef,
                  },
                  error: null,
                  updatedAt: now,
                  consumedAt: now,
                },
              },
            ]),
        {
          id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
          type: "run.updated",
          threadId: projection.thread.id,
          runId: run.id,
          nodeId: rootNode.id,
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: runningRun,
        },
        {
          id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
          type: "run-attempt.updated",
          threadId: projection.thread.id,
          runId: run.id,
          nodeId: rootNode.id,
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: runningAttempt,
        },
        {
          id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
          type: "node.updated",
          threadId: projection.thread.id,
          runId: run.id,
          nodeId: rootNode.id,
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: runningRootNode,
        },
      ];
      const runningWrite = yield* eventSink
        .writeIfRunCurrent({
          threadId: projection.thread.id,
          runId: run.id,
          activeAttemptId: attempt.id,
          expectedStatus: "starting",
          events,
        })
        .pipe(
          Effect.mapError(
            (cause) =>
              new ProviderNativeOperationUnknownError({
                nativeEffect: { ...lastSetupOperation, outcome: "unknown" },
                cause,
              }),
          ),
        );
      if (!runningWrite.committed) {
        if (ordinaryExecution !== undefined) {
          yield* stopCapturedStartup;
          return yield* new ProviderNativeOperationUnknownError({
            nativeEffect: { ...lastSetupOperation, outcome: "unknown" },
            cause: "The actual setup target lost its run attempt before running publication.",
          });
        }
        return;
      }
      yield* providerSessions
        .registerRuntimeBinding({
          threadId: projection.thread.id,
          providerSessionId,
          providerThreadId: runningProviderThread.id,
          runId: run.id,
          attemptId: attempt.id,
        })
        .pipe(
          Effect.mapError(
            (cause) =>
              new ProviderNativeOperationUnknownError({
                nativeEffect: { ...lastSetupOperation, outcome: "unknown" },
                cause,
              }),
          ),
        );
      const routableSubagents = projection.subagents.filter((subagent) =>
        RunExecutionService.canRouteRelatedSubagent(subagent.status),
      );
      const userText = projectComposerContextForProvider({
        text: message.text,
        records: message.context?.records ?? [],
      });
      // Delivered once: this run's provider turn marks the work as told. A
      // restart continuation is prompted by its own text or resumes natively.
      const noteContinuation = isRestartNoteContinuation(
        run,
        projection.runs,
        projection.providerTurns,
      );
      const restartCancelledWork = pendingRestartCancelledBackgroundWork({
        runs: projection.runs,
        providerTurns: projection.providerTurns,
        compactionMessageIds: new Set(
          projection.messages
            .filter(
              (candidate) =>
                candidate.attachments.length === 0 &&
                candidate.text.trim().toLowerCase() === "/compact",
            )
            .map((candidate) => candidate.id),
        ),
        run,
        runAttemptIds: projection.attempts
          .filter((candidate) => candidate.runId === run.id)
          .map((candidate) => candidate.id),
      });
      const restartNote =
        restartCancelledWork.length === 0
          ? ""
          : restartCancelledBackgroundWorkNote(restartCancelledWork);
      const tokenCap = yield* handoffTokenCapConfig.pipe(
        Effect.orElseSucceed(() => DEFAULT_HANDOFF_TOKEN_CAP),
      );
      const settledHandoffs = projection.contextHandoffs.filter(
        (handoff) =>
          handoff.toProviderThreadId === providerThread.id &&
          handoff.delivery?.nativeThreadId === runningProviderThread.nativeThreadRef?.nativeId &&
          handoff.delivery?.status !== "pending",
      );
      const deliveredItemIds = new Set(
        settledHandoffs.flatMap((handoff) => handoff.delivery?.itemIds ?? []),
      );
      const coveredItemIds = new Set([
        ...deliveredItemIds,
        ...settledHandoffs.flatMap((handoff) => handoff.delivery?.omittedItemIds ?? []),
      ]);
      const deliveredAttemptIds = new Set(
        projection.providerTurns.map((turn) => turn.runAttemptId),
      );
      const acceptedAttempts = projection.attempts.filter(
        (source) =>
          source.providerThreadId === providerThread.id && deliveredAttemptIds.has(source.id),
      );
      const nativeInputRunIds = new Set(
        acceptedAttempts
          .filter(
            (source) =>
              source.nativeThreadId !== undefined &&
              source.nativeThreadId === runningProviderThread.nativeThreadRef?.nativeId,
          )
          .map((source) => source.runId),
      );
      const legacyInputRunIds = new Set(
        acceptedAttempts
          .filter((source) => source.nativeThreadId === undefined)
          .map((source) => source.runId),
      );
      const legacyRecoveredRunIds = new Set(
        projection.runs
          .filter(
            (source) =>
              source.providerThreadId === providerThread.id &&
              settledHandoffs.some(
                (handoff) =>
                  handoff.strategy === "full_thread_summary" &&
                  handoff.fromProviderThreadIds.includes(providerThread.id) &&
                  source.ordinal >= handoff.coveredRunOrdinals.from &&
                  source.ordinal <= handoff.coveredRunOrdinals.to,
              ),
          )
          .map((source) => source.id),
      );
      // Use saved text and actual native attachments when telemetry is absent.
      // Legacy attempts lack native identity; exclude their explicitly recovered
      // history, whose attachments were not replayed into the replacement thread.
      const nativeContextEstimate = Effect.gen(function* () {
        return sameNativeThread
          ? (yield* projectionStore.getTurnStartHistory(input.threadId)).reduce((sum, item) => {
              if (
                item.runId === run.id ||
                (item.runId !== null &&
                  missedRunIds.has(item.runId) &&
                  !deliveredItemIds.has(item.id)) ||
                (item.providerThreadId !== providerThread.id && !deliveredItemIds.has(item.id))
              )
                return sum;
              const historical = historicalMessage(item);
              const nativeAttachments =
                item.type === "user_message" &&
                item.providerThreadId === providerThread.id &&
                item.runId !== null &&
                (nativeInputRunIds.has(item.runId) ||
                  (legacyInputRunIds.has(item.runId) &&
                    !coveredItemIds.has(item.id) &&
                    !legacyRecoveredRunIds.has(item.runId)))
                  ? attachmentTokenAllowance(item.attachments)
                  : 0;
              return (
                sum +
                (historical === null ? 0 : Buffer.byteLength(historical.text)) +
                nativeAttachments
              );
            }, 0)
          : 0;
      });
      const modelContextWindow =
        knownModelWindow ??
        (handoffUsage !== null || reuseTelemetry ? previousUsage?.maxTokens : undefined);
      // Replacing a native thread clears its usage, not the selected model's capacity.
      const budgetProviderThread = {
        ...runningProviderThread,
        contextUsage: handoffUsage,
      };
      const missedRuns = projection.runs.filter(
        (source) =>
          source.ordinal < run.ordinal &&
          source.providerThreadId === providerThread.id &&
          (source.status === "failed" || source.status === "interrupted") &&
          !deliveredAttemptIds.has(source.activeAttemptId),
      );
      const missedRunIds = new Set(missedRuns.map((source) => source.id));
      const missedItems =
        missedRunIds.size === 0
          ? []
          : (yield* projectionStore.getTurnStartHistory(input.threadId, [...missedRunIds])).filter(
              (item) =>
                item.runId !== null &&
                missedRunIds.has(item.runId) &&
                !coveredItemIds.has(item.id) &&
                historicalMessage(item) !== null,
            );
      const startWithHandoffs = (
        turnInput: Parameters<typeof session.startTurn>[0],
        compact = false,
      ) => {
        const completeOperation =
          turnInput.nativeOperation ??
          nativeOperation(compact ? "compact_thread" : "start_turn", session);
        return withProviderNativeEffect(
          Effect.gen(function* () {
            // A failed turn/start can leave the requested turn absent from
            // native history even when its preceding handoff was injected.
            const retryHandoff =
              missedItems.length === 0
                ? []
                : [
                    yield* contextHandoffService.prepareProviderHandoff({
                      threadId: projection.thread.id,
                      targetRunId: run.id,
                      transferId: null,
                      fromProviderThreadIds: [providerThread.id],
                      toProviderThreadId: providerThread.id,
                      fromProviderInstanceId: run.providerInstanceId,
                      toProviderInstanceId: run.providerInstanceId,
                      coveredRunOrdinals: {
                        from: missedRuns[0]!.ordinal,
                        to: missedRuns.at(-1)!.ordinal,
                      },
                      strategy: "delta_since_target_last_seen",
                      items: missedItems,
                      runs: projection.runs,
                      createdAt: yield* DateTime.now,
                    }),
                  ];
            const delivery = yield* deliverContextHandoffs({
              handoffs: [...effectiveHandoffs, ...retryHandoff],
              deferInline: compact,
              providerThread: runningProviderThread,
              budget: Effect.gen(function* () {
                return handoffBudget({
                  tokenCap,
                  modelContextWindow,
                  // The note is sent with the user text, so it spends the same allowance.
                  userText: restartNote === "" ? userText : `${restartNote}\n\n${userText}`,
                  attachments: message.attachments,
                  providerThread: budgetProviderThread,
                  nativeContextEstimate:
                    budgetProviderThread.contextUsage?.usedTokens === undefined
                      ? yield* nativeContextEstimate
                      : 0,
                });
              }),
              alreadyDeliveredItemIds: deliveredItemIds,
              ...(session.injectHistory === undefined
                ? {}
                : {
                    inject: (history: ProviderAdapterV2HistoricalContext) =>
                      revalidateOrdinaryExecution.pipe(
                        Effect.andThen(
                          session.injectHistory!({
                            ...(nativeCreationExecution === undefined
                              ? {}
                              : { nativeCreationExecution }),
                            nativeOperation: nativeOperation("inject_history", session),
                            providerThread: runningProviderThread,
                            ...history,
                          }),
                        ),
                      ),
                  }),
              persist: (handoff) =>
                Effect.gen(function* () {
                  const updatedAt = yield* DateTime.now;
                  yield* eventSink.write({
                    events: [
                      {
                        id: yield* idAllocator.allocate.event({
                          threadId: projection.thread.id,
                        }),
                        type: "context-handoff.updated",
                        threadId: projection.thread.id,
                        runId: run.id,
                        providerInstanceId: run.providerInstanceId,
                        occurredAt: updatedAt,
                        payload: { ...handoff, updatedAt },
                      },
                    ],
                  });
                }),
            });
            if (!(yield* isCurrentAttemptInStatus("running"))) {
              if (
                ordinaryExecution !== undefined ||
                nativeCreationExecution !== undefined ||
                input.importedHistoryStartExecution !== undefined
              )
                return yield* new ProviderNativeOperationUnknownError({
                  nativeEffect: { ...completeOperation, outcome: "unknown" },
                  cause:
                    "The current attempt changed after handoff preparation and before native dispatch.",
                });
              return;
            }
            const start = compact ? session.compactThread! : session.startTurn;
            const context = [delivery.context, restartNote]
              .filter((part) => part !== "")
              .join("\n\n");
            // A note continuation has no turn to resume; its text is the prompt.
            const { restartContinuationOfRunId: _resumedRunId, ...promptedInput } = turnInput;
            yield* revalidateOrdinaryExecution;
            yield* start({
              ...(noteContinuation ? promptedInput : turnInput),
              nativeOperation: completeOperation,
              message: {
                ...turnInput.message,
                text: context === "" ? userText : `${context}\n\nUser message:\n${userText}`,
              },
            });
            // Ingestion keeps running while failed delivery persistence holds
            // the complete operation; the accepted prompt must not be resent.
            yield* delivery.delivered;
          }).pipe(
            Effect.mapError((cause) =>
              cause._tag === "ProviderAdapterTurnStartError"
                ? cause
                : new ProviderAdapterTurnStartError({
                    driver: session.driver,
                    threadId: projection.thread.id,
                    providerThreadId: providerThread.id,
                    runId: run.id,
                    nativeEffect: {
                      ...nativeEffectEvidenceFor(cause, completeOperation),
                      outcome: "unknown",
                    },
                    cause,
                  }),
            ),
          ),
          completeOperation,
        );
      };
      const deliverySession =
        effectiveHandoffs.length === 0 &&
        missedItems.length === 0 &&
        restartNote === "" &&
        !noteContinuation
          ? session
          : makeDeliverySession(session, startWithHandoffs);
      let confirmedNativeEffect: ProviderNativeStartConfirmation["nativeEffect"] | undefined;
      const confirmNativeStart = (operation: ProviderNativeOperationContext) =>
        Effect.sync(() => {
          if (session.driver !== "codex") return;
          confirmedNativeEffect = {
            ...operation,
            ...(session.runtimeGeneration === undefined
              ? {}
              : { runtimeGeneration: session.runtimeGeneration }),
            outcome: "confirmed_success",
          };
        });
      const nativeStart = (turnInput: Parameters<typeof session.startTurn>[0], compact = false) => {
        const operation =
          turnInput.nativeOperation ??
          nativeOperation(compact ? "compact_thread" : "start_turn", session);
        const actualInput = {
          ...turnInput,
          ...(nativeCreationExecution === undefined ? {} : { nativeCreationExecution }),
        };
        const startEffect = compact
          ? deliverySession.compactThread!(actualInput)
          : deliverySession.startTurn(actualInput);
        return startEffect.pipe(
          Effect.tap(() => confirmNativeStart(operation)),
          Effect.catchCause((cause) =>
            nativeEffectEvidenceFor(cause, operation).outcome === "confirmed_success"
              ? confirmNativeStart(operation).pipe(Effect.andThen(Effect.failCause(cause)))
              : Effect.failCause(cause),
          ),
        );
      };
      const requiresStartConfirmation =
        nativeCreationExecution !== undefined || input.importedHistoryStartExecution !== undefined;
      const executionSession: ProviderAdapterV2SessionRuntime = !requiresStartConfirmation
        ? deliverySession
        : {
            ...deliverySession,
            get providerSession() {
              return session.providerSession;
            },
            get continuationSourceIdentity() {
              return session.continuationSourceIdentity;
            },
            startTurn: (turnInput: Parameters<typeof session.startTurn>[0]) =>
              nativeStart(turnInput),
            ...(deliverySession.compactThread === undefined
              ? {}
              : {
                  compactThread: (turnInput: Parameters<typeof session.startTurn>[0]) =>
                    nativeStart(turnInput, true),
                }),
          };
      if (requiresStartConfirmation && "runtimeGeneration" in session) {
        Object.defineProperty(executionSession, "runtimeGeneration", {
          configurable: true,
          enumerable: true,
          get: () => session.runtimeGeneration,
        });
      }
      const managedHandle = yield* runExecution
        .startRootRun({
          ...(ordinaryExecution === undefined
            ? {}
            : {
                ordinaryCheckoutUse: ordinaryExecution.originalUse,
                ordinaryCheckoutExecution: ordinaryExecution,
                captureOrdinaryAttachment: () => captureOrdinaryRuntime(runningProviderThread),
                ...(input.prepareOrdinaryManagedActorRun === undefined
                  ? {}
                  : {
                      prepareOrdinaryManagedActorRun: input.prepareOrdinaryManagedActorRun,
                    }),
              }),
          commandId: CommandId.make(`command:effect:provider-turn.start:${run.id}`),
          ...(input.nativeCreationExecutionContext === undefined
            ? {}
            : {
                nativeCreationExecutionContext: input.nativeCreationExecutionContext,
              }),
          ...(input.importedHistoryStartExecution === undefined
            ? {}
            : {
                importedHistoryStartExecution: input.importedHistoryStartExecution,
              }),
          appThread: projection.thread,
          providerSessionId,
          session: executionSession,
          run: runningRun,
          rootNode: runningRootNode,
          checkpointScope,
          providerThread: runningProviderThread,
          attempt: runningAttempt,
          attemptId: attempt.id,
          loadInheritedBackgroundTurnItems: runControls.loadInheritedBackgroundTurnItems,
          relatedThreadIds: routableSubagents.flatMap((subagent) =>
            subagent.childThreadId === null ? [] : [subagent.childThreadId],
          ),
          relatedProviderThreadIds: routableSubagents.flatMap((subagent) =>
            subagent.providerThreadId === null ? [] : [subagent.providerThreadId],
          ),
          providerTurnOrdinal:
            Math.max(
              0,
              ...projection.providerTurns
                .filter((turn) => turn.providerThreadId === providerThread.id)
                .map((turn) => turn.ordinal),
            ) + 1,
          shouldStartProviderTurn: runControls.shouldStartProviderTurn,
          shouldFinalizeRun: runControls.shouldFinalizeRun,
          hasUnpairedRunInterruptRequest: runControls.hasUnpairedRunInterruptRequest,
          message: {
            messageId: message.id,
            text: userText,
            attachments: message.attachments,
            createdBy: message.createdBy,
            creationSource: message.creationSource,
            ...(message.scheduledTaskId === undefined
              ? {}
              : { scheduledTaskId: message.scheduledTaskId }),
            ...(message.senderThreadId === undefined
              ? {}
              : { senderThreadId: message.senderThreadId }),
          },
          modelSelection: run.modelSelection,
          runtimePolicy: resolvedRuntimePolicy,
        })
        .pipe(Effect.onError(() => stopCapturedStartup));
      if (ordinaryExecution !== undefined && managedHandle === undefined) {
        yield* stopCapturedStartup;
        return yield* new ProviderNativeOperationUnknownError({
          nativeEffect: { ...lastSetupOperation, outcome: "unknown" },
          cause: "The ordinary start returned without its actual managed activation handle.",
        });
      }
      const registerManagedHandle =
        managedHandle === undefined
          ? Effect.void
          : input.onOrdinaryManagedRunStarted!(managedHandle).pipe(
              Effect.andThen(managedHandle.requireActivatedExecution),
              Effect.asVoid,
              Effect.onError(() =>
                managedHandle
                  .lose("Ordinary managed activation or registration failed.")
                  .pipe(Effect.ignore),
              ),
            );
      if (!requiresStartConfirmation) {
        yield* registerManagedHandle;
        return;
      }
      const confirmation = confirmedNativeEffect;
      const generation = session.runtimeGeneration;
      const nativeThreadId = runningProviderThread.nativeThreadRef?.nativeId;
      const currentRuntime = yield* providerSessions.get(providerSessionId);
      const current = yield* projectionStore.getRuntimeRecoveryProjection(projection.thread.id);
      const currentRun = current.runs.find((candidate) => candidate.id === run.id);
      const currentAttempt = current.attempts.find((candidate) => candidate.id === attempt.id);
      const currentThread = current.providerThreads.find(
        (candidate) => candidate.id === providerThread.id,
      );
      const registered = yield* eventSink.readProviderRuntimeEvidence(projection.thread.id);
      if (
        confirmation === undefined ||
        generation === undefined ||
        nativeThreadId == null ||
        confirmation.operationId !==
          (executionReference?.effectId ??
            input.importedHistoryStartExecution?.reference.effectId) ||
        Option.isNone(currentRuntime) ||
        currentRuntime.value !== session ||
        currentRun?.activeAttemptId !== attempt.id ||
        currentRun.providerThreadId !== providerThread.id ||
        currentRun.providerInstanceId !== run.providerInstanceId ||
        currentAttempt?.runId !== run.id ||
        currentAttempt.providerThreadId !== providerThread.id ||
        currentThread?.providerSessionId !== providerSessionId ||
        currentThread.nativeThreadRef?.nativeId !== nativeThreadId ||
        current.thread.activeProviderThreadId !== providerThread.id ||
        current.thread.modelSelection.instanceId !== run.providerInstanceId ||
        registered === null ||
        registered.binding.threadId !== projection.thread.id ||
        registered.binding.providerThreadId !== providerThread.id ||
        registered.binding.providerSessionId !== providerSessionId ||
        registered.binding.instanceId !== run.providerInstanceId ||
        registered.binding.driver !== session.driver ||
        registered.binding.nativeThreadId !== nativeThreadId ||
        registered.binding.runtimeGeneration !== generation ||
        confirmation.runtimeGeneration !== generation ||
        session.runtimeGeneration !== generation
      ) {
        if (ordinaryExecution !== undefined && managedHandle !== undefined)
          yield* managedHandle
            .lose("The required native acknowledgment lost its actual current binding.")
            .pipe(Effect.ignore);
        return yield* new ProviderNativeOperationUnknownError({
          nativeEffect: {
            ...(confirmation ?? nativeOperation("start_turn", session)),
            outcome: "unknown",
          },
          cause:
            "The complete native start or its current registered binding could not be confirmed.",
        });
      }
      const returnedConfirmation = {
        status: "confirmed_start" as const,
        binding: {
          threadId: projection.thread.id,
          providerThreadId: providerThread.id,
          providerSessionId,
          instanceId: run.providerInstanceId,
          runtimeGeneration: generation,
          nativeThreadId,
        },
        evidenceRevision: registered.evidenceRevision,
        attemptId: attempt.id,
        nativeEffect: confirmation,
      } satisfies ProviderNativeStartConfirmation;
      if (managedHandle !== undefined)
        yield* input.onOrdinaryManagedRunStarted!(managedHandle, returnedConfirmation).pipe(
          Effect.andThen(managedHandle.requireActivatedExecution),
          Effect.asVoid,
          Effect.onError(() =>
            managedHandle
              .lose("Ordinary acknowledged activation or registration failed.")
              .pipe(Effect.ignore),
          ),
        );
      return returnedConfirmation;
    });

    return ProviderTurnStartServiceV2.of({
      prepareImportedHistoryStart,
      start: (input) =>
        start(input).pipe(
          Effect.mapError((cause) =>
            isProviderTurnStartError(cause)
              ? cause
              : new ProviderTurnStartError({ runId: input.runId, cause }),
          ),
        ),
    });
  }),
);
