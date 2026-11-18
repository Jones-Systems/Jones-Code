import { withStartFailureDiagnostic } from "../jones/provider/startFailureDiagnostics.ts";
import * as WorkspaceProvenance from "../jones/workspaceLifecycle/Provenance.ts";
import * as ConversationPolicy from "../jones/conversationWorkspace/ConversationPolicy.ts";
import * as PlannedUpdateContinuity from "../jones/updates/PlannedUpdateContinuityService.ts";
import * as ProviderInstanceRegistry from "../provider/ProviderInstanceRegistry.ts";
import * as ProviderSwitchService from "./ProviderSwitchService.ts";
import { findEffectClaimLost, guardClaim } from "../jones/workspaceLifecycle/ActiveEffectClaim.ts";
import { WorkspaceLifecycle as WorkspaceLifecycleService } from "../jones/workspaceLifecycle/WorkspaceLifecycle.ts";
import { withOperation } from "../jones/workspaceLifecycle/NativeOperation.ts";
import * as WorkspaceLifecycle from "../jones/workspaceLifecycle/NativeBoundary.ts";
import { findProviderControlOutcomeUnknown } from "@t3tools/provider-core/server/jones/providerControlOutcome";
import { appFailureCodeFromCause } from "../jones/orchestration/appFailureReason.ts";
import { findSteerFailure } from "../jones/orchestration/LateSteer.ts";
import * as PreSessionFailure from "../jones/workspaceRecreation/PreSessionFailure.ts";
import * as CheckoutRecreation from "../jones/workspaceRecreation/Runtime.ts";
import * as WorkspaceDuty from "../jones/workspaceRetirement/Duty.ts";
import * as TaskContinuationStore from "../jones/taskContinuation/TaskContinuationStore.ts";
import { requirementFor, loadedSessionMatches } from "../jones/taskContinuation/guard.ts";
import {
  ExecutionSessionRevalidationError,
  ExecutionSessionRevalidationUnavailableError,
  prepareExecutionSession,
} from "../jones/threadConfiguration/prepareExecutionSession.ts";
import { isWorkModeKeepWarm, workModeProviderPrompt } from "../jones/provider/workModePrompt.ts";
import { continuationPrompt } from "../jones/updates/continuationPrompt.ts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import * as NativeProvider from "../jones/nativeCreation/NativeCreationProviderGuard.ts";
import type { NativeCreationWholeOperationEvidence } from "../jones/nativeCreation/NativeCreationExecutionTypes.ts";
import * as StartRefusals from "../jones/startRefusal/Store.ts";
import * as RestartPreparation from "../jones/orchestration/RestartPreparation.ts";
import * as StartAdmission from "../jones/provider/StartAdmission.ts";
import { continuityRecoveryCapability } from "../jones/goals/continuity/admission.ts";
import { RequiredNativeSession } from "../jones/taskContinuation/requiredNativeSession.ts";
import * as GoalContinuity from "../jones/goals/continuity/GoalContinuity.ts";
import { continuityAdmissionRefusal } from "../jones/goals/continuity/admission.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ControlAdmission from "../jones/orchestration/ControlAdmission.ts";
import { nativeConversationMustBePreserved } from "../jones/goals/preserveGoalConversation.ts";
import { startFailureCauseMessage } from "../jones/provider/startFailureMessage.ts";
import { promptWasNotApplied } from "../jones/provider/codex/HistoryDeliveryOutcome.ts";
import {
  modelSelectionsEqual,
  codexModelFamily,
  normalizeModelSlug,
  getConfiguredReasoningEffort,
} from "@t3tools/shared/model";
import { projectComposerContextForProvider } from "@t3tools/shared/composerContextReferences";
import {
  CommandId,
  ProviderRuntimeBinding,
  latestProviderTurnForAttempt,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ContextHandoff,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
  type OrchestrationV2TurnItem,
  isCompactionCommandMessage,
  parseGoalCommand,
  RunId,
  RunAttemptId,
  NodeId,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";

import * as ServerConfig from "../config.ts";
import {
  CLAUDE_PROVIDER,
  CLAUDE_QUERY_ENDED_BEFORE_TURN_DETAIL,
} from "./Adapters/ClaudeAdapterV2.ts";
import { withTurnStartReservation, withWorkspaceLease } from "../workspace/workspaceLease.ts";
import * as GitWorkflowService from "../git/GitWorkflowService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProviderAuthService from "../provider/ProviderAuthService.ts";
import * as ServerSettings from "../serverSettings.ts";
import { makeRestoreMissingWorktree } from "../workspace/restoreMissingWorktree.ts";
import { makeIsManagedWorktreePath } from "../worktreesDirectory.ts";
import * as EventSink from "./EventSink.ts";
import * as ProviderUsageAnalytics from "../telemetry/ProviderUsageAnalytics.ts";
import * as ContextHandoffService from "./ContextHandoffService.ts";
import { isActiveRun } from "./CommandPolicy.ts";
import {
  DEFAULT_HANDOFF_TOKEN_CAP,
  handoffTokenCapConfig,
  handoffBudget,
  attachmentTokenAllowance,
  estimateTextTokens,
  contextUsageForHandoff,
  historicalMessage,
  latestNativeContextUsage,
} from "@t3tools/provider-core/server/handoffBudget";
import { deliverContextHandoffs, withInlineContext } from "./ContextHandoffDelivery.ts";
import { deliverWithHandoffRecovery } from "../jones/provider/codex/ContextHandoffRecovery.ts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import {
  ContextHandoffBackgroundWorkError,
  knownProviderFailureMessage,
  makeProviderFailure,
} from "@t3tools/provider-core/server/failure";
import * as RunExecutionService from "./RunExecutionService.ts";
import * as RuntimePolicy from "./RuntimePolicy.ts";
import * as TurnItemPositionStore from "./TurnItemPositionStore.ts";
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
    attemptId: Schema.optional(RunAttemptId),
    rootNodeId: Schema.optional(NodeId),
  },
) {}

/** Runs that ended without completing: their provider turn may still project as running. */
const isProviderAdapterTurnStartError = Schema.is(ProviderAdapter.ProviderAdapterTurnStartError);

const NON_COMPLETING_TERMINAL_RUN_STATUSES: ReadonlySet<OrchestrationV2Run["status"]> = new Set([
  "failed",
  "interrupted",
  "cancelled",
  "rolled_back",
]);

export const isProviderTurnStartError = Schema.is(ProviderTurnStartError);
const isProviderAdapterProtocolError = Schema.is(ProviderAdapter.ProviderAdapterProtocolError);
const isProviderAdapterOpenSessionError = Schema.is(
  ProviderAdapter.ProviderAdapterOpenSessionError,
);

/**
 * Claude refuses these before it reads the prompt: replacing a process running background work,
 * launch arguments whose appended system prompt or settings cannot be read, or MCP registration
 * that never answers while the query opens. A query that ends before the turn attaches (such as a
 * rejected resume point) never received the prompt either.
 */
const PROMPT_REFUSAL_TAGS = [
  "ClaudeBackgroundWorkBlocksQueryReplacementError",
  "ClaudeAppendSystemPromptFileError",
  "ClaudeAppendSystemPromptMissingValueError",
  "ClaudeSettingsLaunchArgError",
  "ClaudeMcpRegistrationTimeoutError",
] as const;

const refusedBeforePrompt = (error: unknown): boolean =>
  PROMPT_REFUSAL_TAGS.some((tag) => Predicate.isTagged(error, tag)) ||
  (isProviderAdapterProtocolError(error) &&
    error.driver === CLAUDE_PROVIDER &&
    error.detail === CLAUDE_QUERY_ENDED_BEFORE_TURN_DETAIL) ||
  (Predicate.hasProperty(error, "cause") && refusedBeforePrompt(error.cause));

const isExecutionSessionRevalidationUnavailable = Schema.is(
  ExecutionSessionRevalidationUnavailableError,
);
const isExecutionSessionRevalidationError = Schema.is(ExecutionSessionRevalidationError);

// Jones: lost worker custody and an unknown provider-control outcome are never
// retried by the start effect; retrying could reissue work the provider may own.
const terminalStartFailure = (error: unknown): boolean =>
  findEffectClaimLost(error) !== undefined ||
  findProviderControlOutcomeUnknown(error) !== undefined;

const isRestartThreadHeld = (cause: unknown): cause is RestartPreparation.RestartThreadHeld =>
  Predicate.isTagged(cause, "RestartThreadHeld");

/** Missing execution records cannot be repaired by retrying a start. */
export class ProviderTurnStartRunStateMissingError extends Schema.TaggedError<ProviderTurnStartRunStateMissingError>()(
  "ProviderTurnStartRunStateMissingError",
  {
    runId: RunId,
  },
) {}

export const isProviderTurnStartRunStateMissingError = Schema.is(
  ProviderTurnStartRunStateMissingError,
);

const START_GAVE_UP_MESSAGE =
  "T3 Code could not start this turn. Send the message again; if it keeps failing, check the server logs.";

const startFailureMessage = (error: Error): string => {
  const nestedCause = "cause" in error ? error.cause : undefined;
  return knownProviderFailureMessage(error) ?? startFailureCauseMessage(error, nestedCause);
};

export const probeStartAdmission = Effect.fn("ProviderTurnStartService.probeStartAdmission")(
  function* (threadId: ThreadId) {
    const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
    const goalContinuity = yield* GoalContinuity.GoalContinuity;
    const environment = yield* ServerEnvironment.ServerEnvironmentIdentity;
    const controlAdmission = yield* ControlAdmission.ControlAdmission;
    const projection = yield* projectionStore.getThreadRecords(threadId, ["providerThreads"]);
    const provider = projection.providerThreads.find(
      (candidate) =>
        candidate.id === projection.thread.activeProviderThreadId &&
        candidate.providerInstanceId === projection.thread.providerInstanceId,
    );
    if (
      provider === undefined ||
      projection.thread.archivedAt !== null ||
      projection.thread.deletedAt !== null ||
      projection.thread.settledAt !== null
    )
      return false;
    const environmentId = yield* environment.getEnvironmentId;
    const requirement = yield* goalContinuity.requirementFor(environmentId, threadId);
    const owner =
      provider.nativeThreadRef?.nativeId == null
        ? null
        : {
            environmentId,
            threadId,
            providerInstanceId: provider.providerInstanceId,
            providerThreadId: provider.id,
            driver: provider.nativeThreadRef.driver,
            nativeThreadId: provider.nativeThreadRef.nativeId,
          };
    if (continuityAdmissionRefusal(requirement, owner, false) !== undefined) return false;
    yield* controlAdmission.assertOpen({
      instanceId: provider.providerInstanceId,
      threadId,
      nativeThreadId: provider.nativeThreadRef?.nativeId ?? null,
      producer:
        provider.providerSessionId === null ||
        provider.runtimeIdentity?.runtimeGeneration === undefined
          ? null
          : {
              instanceId: provider.providerInstanceId,
              providerSessionId: provider.providerSessionId,
              runtimeGeneration: provider.runtimeIdentity.runtimeGeneration,
            },
    });
    return true;
  },
  Effect.catchCause(() => Effect.succeed(false)),
);

export interface ProviderTurnStartServiceV2Shape {
  readonly captureStartFailure?: (input: {
    readonly threadId: ThreadId;
    readonly runId: RunId;
  }) => Effect.Effect<
    (cause: unknown) => Effect.Effect<void, ProviderTurnStartError>,
    ProviderTurnStartError
  >;
  readonly startNative?: (input: {
    readonly threadId: ThreadId;
    readonly runId: RunId;
    readonly guard: NativeProvider.NativeProviderExecutionGuard;
  }) => Effect.Effect<NativeCreationWholeOperationEvidence, ProviderTurnStartError>;

  readonly start: (input: {
    readonly threadId: ThreadId;
    readonly runId: RunId;
    readonly willRetry?: boolean;
  }) => Effect.Effect<void, ProviderTurnStartError>;
  readonly failStartingRun: (input: {
    readonly threadId: ThreadId;
    readonly runId: RunId;
    readonly attemptId: RunAttemptId;
    readonly rootNodeId: NodeId;
  }) => Effect.Effect<void, ProviderTurnStartError | ProviderTurnStartRunStateMissingError>;
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
  | Path.Path
  | ServerConfig.ServerConfig
  | ServerSettings.ServerSettingsService
  | GitWorkflowService.GitWorkflowService
  | ProjectService.ProjectService
  | ProviderAuthService.ProviderAuthService
  | ProjectionStore.ProjectionStoreV2
  | ProviderSessionManager.ProviderSessionManagerV2
  | RunExecutionService.RunExecutionServiceV2
  | RuntimePolicy.RuntimePolicyV2
  | TurnItemPositionStore.TurnItemPositionStoreV2
> = Layer.effect(
  ProviderTurnStartServiceV2,
  Effect.gen(function* () {
    const eventSink = yield* EventSink.EventSinkV2;
    const turnAnalytics = yield* ProviderUsageAnalytics.ProviderTurnAnalytics;
    const contextHandoffService = yield* ContextHandoffService.ContextHandoffServiceV2;
    const idAllocator = yield* IdAllocator.IdAllocatorV2;
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const projects = yield* ProjectService.ProjectService;
    const providerAuth = yield* ProviderAuthService.ProviderAuthService;
    const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
    const continuationStore = yield* Effect.serviceOption(
      TaskContinuationStore.TaskContinuationStore,
    );
    const providerSessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
    const runExecution = yield* RunExecutionService.RunExecutionServiceV2;
    const runtimePolicy = yield* RuntimePolicy.RuntimePolicyV2;
    const restoreMissingWorktree = yield* makeRestoreMissingWorktree;
    const isManagedWorktreePath = yield* makeIsManagedWorktreePath;
    const turnItemPositions = yield* TurnItemPositionStore.TurnItemPositionStoreV2;
    const selectionTransitions = yield* Effect.serviceOption(
      ProviderSwitchService.ProviderSwitchServiceV2,
    );
    const plannedContinuity = yield* Effect.serviceOption(
      PlannedUpdateContinuity.PlannedUpdateContinuity,
    );
    const conversations = yield* Effect.serviceOption(ConversationPolicy.ConversationPolicy);
    const instanceRegistry = yield* Effect.serviceOption(
      ProviderInstanceRegistry.ProviderInstanceRegistry,
    );
    const admissionService = yield* Effect.serviceOption(StartAdmission.StartAdmission);
    const continuityService = yield* Effect.serviceOption(GoalContinuity.GoalContinuity);
    const environmentService = yield* Effect.serviceOption(
      ServerEnvironment.ServerEnvironmentIdentity,
    );
    const controlService = yield* Effect.serviceOption(ControlAdmission.ControlAdmission);
    const restartPreparation = yield* Effect.serviceOption(RestartPreparation.RestartPreparation);
    const startRefusals = yield* Effect.serviceOption(StartRefusals.StartRefusals);
    const captureProvenance = yield* WorkspaceProvenance.optionalCapture;
    const currentSettings = yield* ServerSettings.ServerSettingsService;

    // Build draining callbacks outside startup scope so they cannot retain full thread history.
    const makeRunControls = (input: {
      readonly threadId: ThreadId;
      readonly runId: RunId;
      readonly attemptId: OrchestrationV2RunAttempt["id"];
      readonly providerThreadId: OrchestrationV2ProviderThread["id"];
      readonly runOrdinal: number;
      readonly worktreePath: string | null;
      readonly inheritedBackgroundTurnItems: ReturnType<
        typeof RunExecutionService.selectInheritedBackgroundTurnItems
      >;
    }) => {
      const isCurrentAttemptIn = (statuses: ReadonlyArray<OrchestrationV2Run["status"]>) =>
        projectionStore
          .getRun(input.threadId, input.runId)
          .pipe(
            Effect.map(
              (run) => run?.activeAttemptId === input.attemptId && statuses.includes(run.status),
            ),
          );
      const isCurrentAttemptInStatus = (expectedStatus: OrchestrationV2Run["status"]) =>
        isCurrentAttemptIn([expectedStatus]);
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
        shouldFinalizeRun: () => isCurrentAttemptIn(["starting", "running"]),
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
        hasWorkspaceMoved: () =>
          projectionStore.getRuntimeRecoveryProjection(input.threadId).pipe(
            Effect.map((current) => current.thread.worktreePath !== input.worktreePath),
            Effect.catchCause(() => Effect.succeed(false)),
          ),
        loadUnsettledWork: (threadId: ThreadId, nodeIds: ReadonlyArray<NodeId>) =>
          projectionStore
            .getThreadRecords(threadId, ["subagents", "nodes", "turnItems"], {
              nodeIds,
              subagentIds: nodeIds,
              turnItemStatuses: ["pending", "running", "waiting"],
            })
            .pipe(
              Effect.catchTags({
                ProjectionStoreThreadNotFoundError: () =>
                  Effect.succeed({ subagents: [], nodes: [], turnItems: [] }),
              }),
            ),
        loadRunInterruptReason: () =>
          projectionStore
            .getThreadRecords(input.threadId, ["turnItems"], {
              turnItemTypes: ["run_interrupt_request"],
              turnItemRunIds: [input.runId],
            })
            .pipe(
              Effect.map(({ turnItems }) => {
                const request = turnItems.find((item) => item.type === "run_interrupt_request");
                return request?.type === "run_interrupt_request" ? request.message : undefined;
              }),
              Effect.catchCause(() => Effect.undefined),
            ),
      };
    };

    const makeDeliverySession = (
      session: ProviderAdapter.ProviderAdapterV2SessionRuntime,
      startWithHandoffs: (
        input: Parameters<ProviderAdapter.ProviderAdapterV2SessionRuntime["startTurn"]>[0],
        compact?: boolean,
      ) => ReturnType<ProviderAdapter.ProviderAdapterV2SessionRuntime["startTurn"]>,
    ) => {
      let deliver: typeof startWithHandoffs | undefined = startWithHandoffs;
      const start = (
        input: Parameters<ProviderAdapter.ProviderAdapterV2SessionRuntime["startTurn"]>[0],
        compact = false,
      ) =>
        Effect.suspend(() => {
          if (deliver !== undefined) return deliver(input, compact);
          return compact && session.compactThread !== undefined
            ? session.compactThread(input)
            : session.startTurn(input);
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              deliver = undefined;
            }),
          ),
        );
      return {
        ...session,
        startTurn: (input: Parameters<typeof session.startTurn>[0]) => start(input),
        ...(session.resumeTurn === undefined
          ? {}
          : {
              resumeTurn: (input: ProviderAdapter.ProviderAdapterV2ResumeTurnInput) =>
                start(ProviderAdapter.resumeTurnInput(input)),
            }),
        ...(session.compactThread === undefined
          ? {}
          : {
              compactThread: (input: Parameters<typeof session.startTurn>[0]) => start(input, true),
            }),
      };
    };

    const settleRunBeforeStart = Effect.fn("orchestrationV2.providerTurnStart.settleBeforeStart")(
      function* (input: {
        readonly run: OrchestrationV2Run;
        readonly rootNode: OrchestrationV2ExecutionNode;
        readonly attempt: OrchestrationV2RunAttempt;
        readonly turnItems: ReadonlyArray<OrchestrationV2TurnItem>;
        readonly signal: string;
        readonly status: "completed" | "failed";
        readonly now: DateTime.Utc;
        readonly startedAt?: DateTime.Utc;
        readonly providerInstanceId: OrchestrationV2Run["providerInstanceId"];
        readonly itemProviderThreadId: OrchestrationV2ProviderThread["id"] | null;
        readonly item:
          | Pick<Extract<OrchestrationV2TurnItem, { type: "error" }>, "type" | "title" | "failure">
          | Pick<
              Extract<OrchestrationV2TurnItem, { type: "command_execution" }>,
              "type" | "title" | "input" | "output" | "exitCode"
            >;
        readonly startRefusalReason?:
          | "restart_admission"
          | "continuity_unavailable"
          | "continuity_held"
          | "control_held";
        readonly providerThreadUpdate?: OrchestrationV2ProviderThread;
      }) {
        const { run, rootNode, attempt, turnItems, now, status } = input;
        const runId = run.id;
        const started = input.startedAt === undefined ? {} : { startedAt: input.startedAt };
        const item: OrchestrationV2TurnItem = {
          id: idAllocator.derive.runSignalTurnItem({ runId, signal: input.signal }),
          threadId: run.threadId,
          runId,
          nodeId: rootNode.id,
          providerThreadId: input.itemProviderThreadId,
          providerTurnId: null,
          nativeItemRef: null,
          parentItemId: null,
          ordinal:
            Math.max(
              0,
              ...turnItems.filter((item) => item.runId === runId).map((item) => item.ordinal),
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
              id: yield* idAllocator.allocate.event({ threadId: run.threadId }),
              threadId: run.threadId,
              runId,
              nodeId: rootNode.id,
              providerInstanceId: input.providerInstanceId,
              occurredAt: now,
            } satisfies OrchestrationV2DomainEvent;
          }),
        );
        const written = yield* eventSink.writeIfRunCurrent({
          threadId: run.threadId,
          runId,
          activeAttemptId: attempt.id,
          expectedStatus: "starting",
          events,
          ...(input.startRefusalReason === undefined
            ? {}
            : {
                startRefusalReason: input.startRefusalReason,
                effects: [],
              }),
        });
        // A steering restart that cannot start still ends a run whose earlier turns counted.
        if (written.committed) {
          yield* turnAnalytics.runSettled({
            runId,
            status,
            completedAt: now,
            childrenRunning: false,
          });
        }
      },
    );

    const start = Effect.fn("orchestrationV2.providerTurnStart.start")(function* (
      input: {
        readonly threadId: ThreadId;
        readonly runId: RunId;
        readonly willRetry?: boolean;
        readonly nativeCreationGuard?: NativeProvider.NativeProviderExecutionGuard;
      },
      captureStartingRun: (run: OrchestrationV2Run) => void,
    ) {
      const { runId } = input;
      // Reassigned only after checkout recreation rebinds this thread's workspace.
      let projection = yield* projectionStore.getTurnStartContext(input.threadId, runId);
      const run = projection.runs.find((candidate) => candidate.id === runId);
      if (run === undefined) {
        return yield* new ProviderTurnStartError({ runId, cause: `Run ${runId} was not found.` });
      }
      if (run.status !== "starting") {
        return;
      }
      captureStartingRun(run);
      const rootNode = projection.nodes.find((candidate) => candidate.id === run.rootNodeId);
      const attempt = projection.attempts.find((candidate) => candidate.id === run.activeAttemptId);
      const selectedProviderThread = projection.providerThreads.find(
        (candidate) => candidate.id === run.providerThreadId,
      );
      const message = projection.messages.find((candidate) => candidate.id === run.userMessageId);
      const checkpointScope = projection.checkpointScopes.find(
        (candidate) => candidate.id === rootNode?.checkpointScopeId,
      );
      // Older versions also stored subagent replies as history handoffs. Their
      // queued completion messages carry the results without a history budget.
      const subagentResultTransfers = new Set(
        projection.contextTransfers
          .filter((transfer) => transfer.type === "subagent_result")
          .map((transfer) => transfer.id),
      );
      const isPendingForRun = (handoff: OrchestrationV2ContextHandoff, anyProviderThread = false) =>
        handoff.status === "ready" &&
        (handoff.targetRunId === run.id ||
          ((anyProviderThread || handoff.toProviderThreadId === run.providerThreadId) &&
            projection.runs.some(
              (source) =>
                source.id === handoff.targetRunId &&
                (source.status === "failed" ||
                  source.status === "interrupted" ||
                  source.status === "cancelled" ||
                  (source.status === "completed" &&
                    handoff.delivery === undefined &&
                    // Compaction and /goal turns defer history to the next turn.
                    projection.messages.some(
                      (message) =>
                        message.id === source.userMessageId &&
                        (isCompactionCommandMessage(message) || parseGoalCommand(message) !== null),
                    ))),
            )));
      const isSideChatHandoff = (handoff: OrchestrationV2ContextHandoff) =>
        projection.thread.lineage.sideChat === true &&
        ContextHandoffService.isSideChatContextHandoff(handoff);
      const handoffs = projection.contextHandoffs.filter(
        (handoff) =>
          !isSideChatHandoff(handoff) &&
          (handoff.transferId == null || !subagentResultTransfers.has(handoff.transferId)) &&
          isPendingForRun(handoff),
      );
      // Carried forward until a provider turn accepts it, also after a provider switch.
      const sideChatHandoff = projection.contextHandoffs.find(
        (handoff) =>
          isSideChatHandoff(handoff) &&
          handoff.delivery === undefined &&
          isPendingForRun(handoff, true),
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
        selectedProviderThread === undefined ||
        selectedProviderThread.providerSessionId === null ||
        message === undefined ||
        checkpointScope === undefined
      ) {
        return yield* new ProviderTurnStartError({
          runId,
          cause: `Run ${runId} is missing its execution projection state.`,
        });
      }
      let providerThread = selectedProviderThread;
      const admissionError = (cause: unknown) =>
        new ProviderAdapter.ProviderAdapterTurnStartError({
          driver: providerThread.driver,
          threadId: input.threadId,
          providerThreadId: providerThread.id,
          runId,
          cause,
        });
      type AdmissionRefusal =
        | "restart_admission"
        | "continuity_unavailable"
        | "continuity_held"
        | "control_held";
      // Jones: an evidenced refusal keeps the receiving signal, persisted copy and
      // failure class; a missing admission owner keeps the fail-closed notice.
      const refusalNotice = (
        reason: AdmissionRefusal,
        cause: unknown,
      ): {
        readonly reason: AdmissionRefusal;
        readonly signal: string;
        readonly title: string;
        readonly failure: ReturnType<typeof makeProviderFailure>;
      } => {
        if (typeof cause === "string")
          return {
            reason,
            signal: "start-admission-refused",
            title: "Provider start admission refused",
            failure: makeProviderFailure({ cause, message: "Provider start admission is held." }),
          };
        if (reason === "restart_admission") {
          const held = isRestartThreadHeld(cause) ? cause : undefined;
          return {
            reason,
            signal:
              held === undefined
                ? "provider-restart-admission-failure"
                : `provider-restart-${held.stage}-held`,
            title: "Provider restart is held",
            failure: {
              class: "provider_error",
              message: held?.message ?? "Provider restart preparation could not be verified.",
              code: null,
              retryable: false,
            },
          };
        }
        if (reason === "control_held")
          return {
            reason,
            signal: "provider-control-held",
            title: "Provider work is held",
            failure: {
              class: "provider_error",
              message:
                "Earlier provider control has an unresolved outcome for this conversation. This turn was not started. Stop the exact runtime or wait for its control result before trying again.",
              code: null,
              retryable: false,
            },
          };
        const code =
          reason === "continuity_unavailable" ? appFailureCodeFromCause(cause) : undefined;
        return code !== undefined
          ? {
              reason,
              signal: "goal-continuity-unavailable",
              title: "Continuity check unavailable",
              failure: makeProviderFailure({
                class: "unknown",
                code,
                retryable: true,
                message: "The continuity check could not be read. No provider work was started.",
              }),
            }
          : {
              reason: "continuity_held",
              signal: "goal-continuity-held",
              title: "Goal continuity requires attention",
              failure: makeProviderFailure({
                class: "provider_error",
                message:
                  "The original conversation or a recovery delivery is held. No provider work was started.",
              }),
            };
      };
      const refuseAdmission = (reason: AdmissionRefusal, cause: unknown) => {
        const notice = refusalNotice(reason, cause);
        return DateTime.now.pipe(
          Effect.flatMap((now) =>
            settleRunBeforeStart({
              run,
              rootNode,
              attempt,
              turnItems: projection.turnItems,
              signal: notice.signal,
              status: "failed",
              now,
              startRefusalReason: notice.reason,
              providerInstanceId: run.providerInstanceId,
              itemProviderThreadId: providerThread.id,
              item: { type: "error", title: notice.title, failure: notice.failure },
            }),
          ),
        );
      };
      if (Option.isNone(restartPreparation) || Option.isNone(startRefusals))
        return yield* refuseAdmission(
          "restart_admission",
          "Original restart admission services are unavailable.",
        );
      const restartAdmission = yield* restartPreparation.value
        .assertThreadOpen(input.threadId)
        .pipe(Effect.result);
      if (restartAdmission._tag === "Failure")
        return yield* refuseAdmission("restart_admission", restartAdmission.failure);
      // Jones: the manager may finish its retained local publication of this exact
      // binding first; admission is then captured against the recovered projection.
      if (providerSessions.reconcileThreadBindingPublication !== undefined) {
        const recovered = yield* guardClaim(
          providerSessions.reconcileThreadBindingPublication({
            run,
            providerThread: selectedProviderThread,
          }),
        ).pipe(Effect.mapError((cause) => new ProviderTurnStartError({ runId, cause })));
        if (recovered !== null) {
          projection = yield* projectionStore.getTurnStartContext(input.threadId, runId);
          const currentRun = projection.runs.find((candidate) => candidate.id === runId);
          const currentProvider = projection.providerThreads.find(
            (candidate) => candidate.id === selectedProviderThread.id,
          );
          const currentAttempt = projection.attempts.find(
            (candidate) => candidate.id === run.activeAttemptId,
          );
          if (
            currentRun?.status !== "starting" ||
            currentRun.activeAttemptId !== run.activeAttemptId ||
            currentRun.providerThreadId !== selectedProviderThread.id ||
            currentRun.providerInstanceId !== run.providerInstanceId ||
            !modelSelectionsEqual(currentRun.modelSelection, run.modelSelection) ||
            currentAttempt?.status !== "pending" ||
            currentAttempt.runId !== runId ||
            currentAttempt.providerThreadId !== selectedProviderThread.id ||
            currentAttempt.providerInstanceId !== run.providerInstanceId ||
            currentProvider?.providerSessionId !== recovered.providerSessionId ||
            currentProvider.nativeThreadRef?.nativeId !== recovered.nativeThreadRef?.nativeId ||
            currentProvider.runtimeIdentity?.runtimeGeneration !==
              recovered.runtimeIdentity?.runtimeGeneration
          )
            return yield* new ProviderTurnStartError({
              runId,
              cause: "The new run or recovered binding changed during local reconciliation.",
            });
          providerThread = currentProvider;
        }
      }
      if (
        Option.isNone(admissionService) ||
        Option.isNone(continuityService) ||
        Option.isNone(environmentService) ||
        Option.isNone(controlService)
      )
        return yield* refuseAdmission(
          "continuity_unavailable",
          "Original durable start admission services are unavailable.",
        );
      let capturedAdmission = yield* admissionService.value.capture({
        threadId: input.threadId,
        run,
        attemptId: attempt.id,
        providerThread,
      });
      if (
        input.nativeCreationGuard !== undefined &&
        capturedAdmission.nativeCreationGuard !== input.nativeCreationGuard
      )
        return yield* admissionError(
          "Native admission did not retain the original executor-issued guard.",
        );
      const goalContinuity = continuityService.value;
      const controlAdmission = controlService.value;
      const environmentId = yield* environmentService.value.getEnvironmentId;
      let admissionRefusal: AdmissionRefusal | undefined;
      // Jones: once this attempt observed a required continuity owner, its
      // native conversation is never replaced by a fresh one.
      let continuityRequired = false;
      const stopAdmission = eventSink as typeof eventSink & {
        readonly assertRuntimeStopStartAllowed?: (input: {
          readonly threadId: ThreadId;
          readonly runId: RunId;
        }) => Effect.Effect<void, unknown>;
      };
      // A delegated task continuation must resume its recorded native session. The
      // requirement is captured once from the continuation owner and travels with
      // this attempt; it is never re-derived from ambient context.
      const continuation = yield* Effect.result(
        requirementFor(Option.getOrUndefined(continuationStore), projection, run, attempt),
      );
      if (
        continuation._tag === "Failure" ||
        (continuation.success !== undefined && nativeForkTransfer !== undefined)
      ) {
        yield* settleRunBeforeStart({
          run,
          rootNode,
          attempt,
          turnItems: projection.turnItems,
          signal: "task-continuation-native-identity",
          status: "failed",
          now: yield* DateTime.now,
          providerInstanceId: run.providerInstanceId,
          itemProviderThreadId: providerThread.id,
          item: {
            type: "error",
            title:
              "Delegated task continuation could not resume its original provider conversation",
            failure: makeProviderFailure({
              class: "provider_error",
              message: "The required task continuation binding is unavailable or changed.",
              ...(continuation._tag === "Failure" ? { cause: continuation.failure } : {}),
            }),
          },
        });
        return;
      }
      const requiredNativeSession = continuation.success;
      const guarded = requiredNativeSession !== undefined;
      const admittedSuccessors = new Map<string, typeof ProviderRuntimeBinding.Type>();
      const equivalentAdmissionBinding = Schema.toEquivalence(ProviderRuntimeBinding);
      const assertAdmission = Effect.fn("ProviderTurnStartService.assertAdmission")(
        (expected: OrchestrationV2ProviderThread) =>
          Effect.gen(function* () {
            yield* capturedAdmission.revalidate;
            if (input.nativeCreationGuard !== undefined)
              yield* NativeProvider.assertNativeProviderStartOperation(
                input.nativeCreationGuard,
                input.threadId,
                runId,
              );
            yield* restartPreparation.value.assertThreadOpen(input.threadId);
            if (stopAdmission.assertRuntimeStopStartAllowed === undefined)
              return yield* admissionError("Durable runtime Stop start admission is unavailable.");
            yield* stopAdmission.assertRuntimeStopStartAllowed({ threadId: input.threadId, runId });
            const fresh = yield* projectionStore.getTurnStartContext(input.threadId, runId);
            const currentRun = fresh.runs.find((candidate) => candidate.id === runId);
            const current = fresh.providerThreads.find((candidate) => candidate.id === expected.id);
            const publication = yield* StartAdmission.CurrentBindingPublication;
            const currentGeneration = current?.runtimeIdentity?.runtimeGeneration;
            const currentBinding =
              current === undefined || currentGeneration === undefined
                ? undefined
                : ProviderAdapter.runtimeBinding(current, currentGeneration);
            const retainedSuccessor = admittedSuccessors.get(expected.id);
            const fromPublication =
              currentBinding !== undefined &&
              publication !== undefined &&
              publication.previousGeneration ===
                (expected.runtimeIdentity?.runtimeGeneration ?? null) &&
              equivalentAdmissionBinding(currentBinding, publication.binding) &&
              (expected.nativeThreadRef == null ||
                expected.nativeThreadRef.nativeId === currentBinding.nativeThreadId);
            const isSuccessor =
              fromPublication ||
              (currentBinding !== undefined &&
                retainedSuccessor !== undefined &&
                equivalentAdmissionBinding(currentBinding, retainedSuccessor));
            if (
              currentRun?.activeAttemptId !== attempt.id ||
              !["starting", "running"].includes(currentRun.status) ||
              currentRun.providerThreadId !== expected.id ||
              !modelSelectionsEqual(currentRun.modelSelection, run.modelSelection) ||
              current?.providerSessionId !== expected.providerSessionId ||
              current?.providerInstanceId !== expected.providerInstanceId ||
              (!isSuccessor &&
                (current?.nativeThreadRef?.nativeId !== expected.nativeThreadRef?.nativeId ||
                  current?.runtimeIdentity?.runtimeGeneration !==
                    expected.runtimeIdentity?.runtimeGeneration)) ||
              fresh.thread.archivedAt !== null ||
              fresh.thread.deletedAt !== null ||
              fresh.thread.settledAt !== null
            )
              return yield* admissionError("The original attempt or native binding changed.");
            const admittedProvider = isSuccessor ? current! : expected;
            // Only the requirement captured for this attempt may be in scope.
            const contextualNative = yield* RequiredNativeSession;
            if (contextualNative !== undefined && contextualNative !== requiredNativeSession)
              return yield* admissionError(
                "Required native session does not match the original admission.",
              );
            if (
              requiredNativeSession !== undefined &&
              (requiredNativeSession.runId !== runId ||
                requiredNativeSession.attemptId !== attempt.id ||
                admittedProvider.nativeThreadRef?.nativeId !==
                  requiredNativeSession.nativeSessionId)
            )
              return yield* admissionError(
                "Required native session does not match the original admission.",
              );
            const requirementResult = yield* goalContinuity
              .requirementFor(environmentId, input.threadId)
              .pipe(Effect.result);
            if (requirementResult._tag === "Failure") {
              admissionRefusal = "continuity_unavailable";
              return yield* requirementResult.failure;
            }
            const requirement = requirementResult.success;
            const owner =
              admittedProvider.nativeThreadRef?.nativeId == null
                ? null
                : {
                    environmentId,
                    threadId: input.threadId,
                    providerInstanceId: admittedProvider.providerInstanceId,
                    providerThreadId: admittedProvider.id,
                    driver: admittedProvider.nativeThreadRef.driver,
                    nativeThreadId: admittedProvider.nativeThreadRef.nativeId,
                  };
            if (
              continuityAdmissionRefusal(requirement, owner) !== undefined ||
              (requirement.outcome === "required" &&
                continuityRecoveryCapability(
                  admittedProvider.driver,
                  admittedProvider.nativeThreadRef?.strength === "strong",
                ).turnStart === "unsupported")
            ) {
              admissionRefusal = "continuity_held";
              return yield* admissionError("Goal continuity delivery or native ownership is held.");
            }
            if (requirement.outcome === "required") continuityRequired = true;
            const publishing = yield* ControlAdmission.startupPublicationFor(
              publication?.binding,
              admittedProvider,
            );
            const controlResult = yield* controlAdmission
              .assertOpen(
                {
                  instanceId: admittedProvider.providerInstanceId,
                  threadId: input.threadId,
                  nativeThreadId: admittedProvider.nativeThreadRef?.nativeId ?? null,
                  producer:
                    admittedProvider.providerSessionId === null ||
                    admittedProvider.runtimeIdentity?.runtimeGeneration === undefined
                      ? null
                      : {
                          instanceId: admittedProvider.providerInstanceId,
                          providerSessionId: admittedProvider.providerSessionId,
                          runtimeGeneration: admittedProvider.runtimeIdentity.runtimeGeneration,
                        },
                },
                publishing,
              )
              .pipe(Effect.result);
            if (controlResult._tag === "Failure") {
              admissionRefusal = "control_held";
              return yield* controlResult.failure;
            }
            yield* capturedAdmission.revalidate;
            if (fromPublication && currentBinding !== undefined)
              admittedSuccessors.set(expected.id, currentBinding);
          }).pipe(Effect.mapError(admissionError)),
      );
      const initialAdmission = yield* assertAdmission(providerThread).pipe(Effect.result);
      if (initialAdmission._tag === "Failure") {
        // Jones: a transient reader-admission failure keeps retry ownership and
        // settles only when the worker has no retry left. The refusal is
        // assigned inside the admission closure, so it is widened here.
        const observedRefusal = admissionRefusal as AdmissionRefusal | undefined;
        if (
          observedRefusal === "continuity_unavailable" &&
          input.willRetry === true &&
          appFailureCodeFromCause(initialAdmission.failure) !== undefined
        )
          return yield* initialAdmission.failure;
        if (admissionRefusal !== undefined)
          return yield* refuseAdmission(admissionRefusal, initialAdmission.failure);
        return yield* initialAdmission.failure;
      }
      const manualResume =
        run.manualContinuationOfRunId !== undefined && attempt.reason !== "steering_restart";
      const resumeDisposition = manualResume ? yield* capturedAdmission.classifyResume : undefined;
      if (resumeDisposition === "held")
        return yield* admissionError("The original native outcome is unknown; Resume is held.");
      const resumeSource =
        manualResume && resumeDisposition === "unsent"
          ? projection.runs.find((source) => source.id === run.manualContinuationOfRunId)
          : undefined;
      if (resumeSource?.manualContinuationOfRunId !== undefined)
        return yield* admissionError(
          "A chained Resume requires exact original input certification; replay is held.",
        );
      const unsentResumeMessage =
        resumeSource !== undefined
          ? projection.messages.find((source) => source.id === resumeSource.userMessageId)
          : undefined;
      if (resumeDisposition === "unsent" && unsentResumeMessage === undefined)
        return yield* admissionError("Original unsent input is unavailable; replay is refused.");
      const providerMessage = unsentResumeMessage ?? message;
      const preserveNativeConversation =
        nativeForkTransfer === undefined &&
        nativeConversationMustBePreserved(providerThread, providerMessage);
      const settleStartFailure = Effect.fn("ProviderTurnStartService.settleStartFailure")(
        function* (failed: {
          readonly signal: string;
          readonly title: string;
          readonly error: Error;
          readonly message?: string;
        }) {
          // Jones: a run-state read that could not complete and a real selection
          // change keep their receiving copy and reader-admission failure code.
          const nestedCause = "cause" in failed.error ? failed.error.cause : undefined;
          const unavailable = isExecutionSessionRevalidationUnavailable(failed.error)
            ? failed.error
            : isExecutionSessionRevalidationUnavailable(nestedCause)
              ? nestedCause
              : undefined;
          const changed =
            isExecutionSessionRevalidationError(failed.error) ||
            isExecutionSessionRevalidationError(nestedCause);
          yield* settleRunBeforeStart({
            run,
            rootNode,
            attempt,
            turnItems: projection.turnItems,
            signal:
              unavailable !== undefined
                ? "provider-run-state-unavailable"
                : changed
                  ? "provider-selection-changed-before-open"
                  : failed.signal,
            status: "failed",
            now: yield* DateTime.now,
            providerInstanceId: run.providerInstanceId,
            itemProviderThreadId: providerThread.id,
            item: {
              type: "error",
              title:
                unavailable !== undefined
                  ? "Provider start could not verify its run state"
                  : changed
                    ? "Provider selection changed before opening"
                    : failed.title,
              failure:
                unavailable !== undefined
                  ? makeProviderFailure({
                      class: "unknown",
                      code: appFailureCodeFromCause(unavailable),
                      retryable: true,
                      message: unavailable.message,
                    })
                  : withStartFailureDiagnostic(
                      makeProviderFailure({
                        cause: failed.error,
                        message: failed.message ?? startFailureMessage(failed.error),
                        class: "provider_error",
                      }),
                      failed.error,
                    ),
            },
          });
          if (terminalStartFailure(failed.error)) return yield* Effect.fail(failed.error);
        },
      );
      yield* startRefusals.value.cross({ threadId: input.threadId, runId, attemptId: attempt.id });
      if (
        providerMessage.attachments.length === 0 &&
        providerMessage.text.trimStart().startsWith("/")
      ) {
        const isEmptyCompaction =
          providerMessage.text.trim().toLowerCase() === "/compact" && !projection.hasConversation;
        // Preparation may select a new provider; account commands still target the last native session.
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
        const authResult = isEmptyCompaction
          ? null
          : yield* Effect.result(
              providerAuth.tryHandlePromptCommand({
                instanceId: authInstanceId,
                text: projectComposerContextForProvider({
                  text: providerMessage.text,
                  records: providerMessage.context?.records ?? [],
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
            run,
            rootNode,
            attempt,
            turnItems: projection.turnItems,
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
                    input: providerMessage.text.trim(),
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
      const preSessionProviderThreadId = providerThread.id;
      const beforeSession = <A, E, R>(
        operation: PreSessionFailure.Operation,
        effect: Effect.Effect<A, E, R>,
      ) =>
        PreSessionFailure.protect(operation, effect, {
          willRetry: input.willRetry === true,
          nativeGuarded: input.nativeCreationGuard !== undefined,
          settle: (notice) =>
            Effect.flatMap(DateTime.now, (now) =>
              settleRunBeforeStart({
                run,
                rootNode,
                attempt,
                turnItems: projection.turnItems,
                signal: notice.signal,
                status: "failed",
                now,
                providerInstanceId: run.providerInstanceId,
                itemProviderThreadId: preSessionProviderThreadId,
                item: { type: "error", title: notice.title, failure: notice.failure },
              }),
            ),
        });
      // Jones exact checkout recreation owns retired and missing managed checkouts;
      // it rebinds the thread only through EventSink's checkout compare-and-bind.
      let recreatedWorkspace = false;
      const { worktreePath } = projection.thread;
      if (worktreePath !== null) {
        if (input.nativeCreationGuard !== undefined && !(yield* fileSystem.exists(worktreePath)))
          return yield* new ProviderTurnStartError({
            runId,
            cause: "Native verified workspace disappeared; ordinary recreation is unavailable.",
          });
        const project = yield* beforeSession(
          "preflight",
          projects.getById(projection.thread.projectId).pipe(Effect.map(Option.getOrUndefined)),
        );
        if (project !== undefined) {
          const managed = yield* beforeSession("preflight", isManagedWorktreePath(worktreePath));
          const recreated = yield* beforeSession(
            "recreation",
            CheckoutRecreation.prepareTurn({
              threadId: input.threadId,
              runId,
              projectRoot: project.workspaceRoot,
              worktree: worktreePath,
              managed,
              nativeGuarded: input.nativeCreationGuard !== undefined,
              ...(message.context?.checkoutRecreation === undefined
                ? {}
                : { detached: message.context.checkoutRecreation }),
            }).pipe(Effect.mapError((cause) => new ProviderTurnStartError({ runId, cause }))),
          );
          if (recreated !== null) {
            const recreatedBranch = recreated.branch?.replace(/^refs\/heads\//, "") ?? null;
            const now = yield* DateTime.now;
            yield* eventSink.writeIfRunCurrent({
              threadId: input.threadId,
              runId,
              activeAttemptId: attempt.id,
              expectedStatus: "starting",
              checkoutRecreation: { worktreePath, branch: projection.thread.branch },
              events: [
                {
                  type: "thread.metadata-updated",
                  id: yield* idAllocator.allocate.event({ threadId: input.threadId }),
                  threadId: input.threadId,
                  occurredAt: now,
                  payload: {
                    ...projection.thread,
                    worktreePath: recreated.path,
                    branch: recreatedBranch,
                    updatedAt: now,
                  },
                },
              ],
            });
            projection = yield* beforeSession(
              "preflight",
              projectionStore.getTurnStartContext(input.threadId, runId),
            );
            if (
              projection.thread.worktreePath !== recreated.path ||
              projection.thread.branch !== recreatedBranch
            )
              return yield* beforeSession(
                "preflight",
                Effect.fail(
                  new ProviderTurnStartError({
                    runId,
                    cause: "Checkout recreation binding changed before use.",
                  }),
                ),
              );
            recreatedWorkspace = true;
          }
        }
      }
      // Upstream restore remains for what the Jones owner leaves alone: pruned
      // No project folders and leftover-only folders. A native-verified
      // workspace is never restored by ordinary means.
      const recreateResult = yield* Effect.result(
        input.nativeCreationGuard === undefined
          ? restoreMissingWorktree(projection.thread)
          : Effect.void,
      );
      if (recreateResult._tag === "Failure") {
        if (input.willRetry === true) return yield* recreateResult.failure;
        yield* settleStartFailure({
          signal: "worktree-recreate-failure",
          title: "Couldn't recreate thread workspace",
          error: recreateResult.failure,
          message: recreateResult.failure.message,
        });
        return;
      }
      // Jones: a lifecycle-managed workspace resumes its original native
      // transition at the start boundary before any provider use.
      yield* WorkspaceLifecycle.beforeUse({
        threadId: input.threadId,
        boundary: "start",
        fallback: { id: `turn-start:${runId}`, request: { runId, threadId: input.threadId } },
      }).pipe(Effect.mapError((cause) => new ProviderTurnStartError({ runId, cause })));
      yield* captureProvenance({
        threadId: input.threadId,
        reason: recreatedWorkspace ? "recreate" : "reference",
        createdBy: "system",
        runId,
      });
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
      const inheritedBackgroundTurnItems = yield* beforeSession(
        "preflight",
        projectionStore
          .getRuntimeRecoveryProjection(projection.thread.id)
          .pipe(Effect.map(selectInheritedBackgroundItems)),
      );
      const revalidateReplacement = Effect.gen(function* () {
        const current = yield* projectionStore
          .getTurnStartContext(run.threadId, runId)
          .pipe(
            Effect.mapError(
              (cause) => new ExecutionSessionRevalidationUnavailableError({ runId, cause }),
            ),
          );
        const latest = current.runs.find((candidate) => candidate.id === runId);
        if (
          latest?.activeAttemptId !== attempt.id ||
          latest.status !== "starting" ||
          latest.providerThreadId !== providerThread.id ||
          latest.providerInstanceId !== run.providerInstanceId ||
          !modelSelectionsEqual(latest.modelSelection, run.modelSelection) ||
          current.thread.archivedAt !== null ||
          current.thread.deletedAt !== null ||
          current.thread.settledAt !== null
        )
          return yield* new ExecutionSessionRevalidationError({
            runId,
            cause: "The original attempt or selection changed during retirement.",
          });
      });
      yield* assertAdmission(providerThread);
      if (Option.isNone(selectionTransitions))
        return yield* new ProviderTurnStartError({
          runId,
          cause: "Provider session preparation is unavailable.",
        });
      const preparation = yield* prepareExecutionSession(
        run,
        providerThread,
        projection,
        revalidateReplacement.pipe(
          Effect.andThen(
            capturedAdmission.revalidate.pipe(
              Effect.mapError((cause) => new ExecutionSessionRevalidationError({ runId, cause })),
            ),
          ),
        ),
      ).pipe(
        Effect.provideService(ProviderSessionManager.ProviderSessionManagerV2, providerSessions),
        Effect.provideService(EventSink.EventSinkV2, eventSink),
        Effect.provideService(
          ProviderSwitchService.ProviderSwitchServiceV2,
          selectionTransitions.value,
        ),
        guardClaim,
        Effect.result,
      );
      if (preparation._tag === "Failure") {
        if (input.willRetry === true && !terminalStartFailure(preparation.failure))
          return yield* preparation.failure;
        yield* settleStartFailure({
          signal: "provider-writer-retirement-failure",
          title: "Previous provider writer could not retire",
          error: preparation.failure,
        });
        return;
      }
      if (preparation.success.providerSessionId !== providerThread.providerSessionId) {
        // Capture again only after the helper's guarded durable replacement. The
        // same worker claim and original accepted receipt must still authorize it.
        const replacementAdmission = yield* admissionService.value.capture({
          threadId: input.threadId,
          run,
          attemptId: attempt.id,
          providerThread: preparation.success,
        });
        if (replacementAdmission.nativeCreationGuard !== capturedAdmission.nativeCreationGuard)
          return yield* admissionError("Replacement admission changed the original native guard.");
        capturedAdmission = replacementAdmission;
      }
      providerThread = preparation.success;
      const providerSessionId = providerThread.providerSessionId;
      if (providerSessionId === null)
        return yield* new ProviderTurnStartError({
          runId,
          cause: "Prepared execution session has no session identity.",
        });
      const runControls = makeRunControls({
        threadId: projection.thread.id,
        runId: run.id,
        attemptId: attempt.id,
        providerThreadId: providerThread.id,
        runOrdinal: run.ordinal,
        worktreePath: projection.thread.worktreePath,
        inheritedBackgroundTurnItems,
      });
      const { isCurrentAttemptInStatus } = runControls;

      if (
        Option.isSome(conversations) &&
        projection.thread.lineage.parentThreadId !== null &&
        run.workspacePreparation === undefined &&
        projection.thread.worktreePath === null
      ) {
        yield* conversations.value.inherit(
          projection.thread.id,
          projection.thread.lineage.parentThreadId,
        );
      }
      if (Option.isSome(conversations) && projection.thread.worktreePath === null) {
        const shared = yield* conversations.value.lookup(projection.thread.id);
        if (shared?.state === "attached" && shared.ownerThreadId !== projection.thread.id) {
          yield* assertAdmission(providerThread);
          yield* conversations.value.attachForTurn(
            projection.thread.id,
            run.id,
            providerMessage.text,
            providerMessage.attachments,
          );
          return;
        }
      }
      const conversationTurn =
        Option.isSome(conversations) && projection.thread.worktreePath === null
          ? yield* conversations.value.startTurn(projection.thread.id, run.id)
          : null;
      if (conversationTurn !== null) {
        const instance = Option.isSome(instanceRegistry)
          ? yield* instanceRegistry.value.getInstance(run.modelSelection.instanceId)
          : undefined;
        if (instance?.driverKind !== "codex" && instance?.driverKind !== "claudeAgent") {
          if (Option.isNone(conversations))
            return yield* admissionError("Conversation attachment is unavailable.");
          yield* ConversationPolicy.observeEagerFallback({
            threadId: projection.thread.id,
            runId,
            driver: instance?.driverKind ?? "unavailable",
            reason: "eager: resumed adapter has no conversation proof",
          });
          yield* assertAdmission(providerThread);
          yield* conversations.value.attachForTurn(
            projection.thread.id,
            run.id,
            providerMessage.text,
            providerMessage.attachments,
          );
          return;
        }
      }
      const resolvedPolicy = yield* runtimePolicy
        .resolve({
          thread: projection.thread,
          modelSelection: run.modelSelection,
        })
        .pipe(Effect.result);
      if (resolvedPolicy._tag === "Failure") {
        const error = resolvedPolicy.failure;
        if (error._tag !== "RuntimePolicyAboveCeilingError") return yield* error;
        yield* settleStartFailure({
          signal: "runtime-mode-above-ceiling",
          title: "Thread mode is above the policy limit",
          error,
          message: error.message,
        });
        return;
      }
      const resolvedRuntimePolicy =
        conversationTurn === null
          ? resolvedPolicy.success
          : { ...resolvedPolicy.success, cwd: conversationTurn.binding.path };
      if (!(yield* isCurrentAttemptInStatus("starting"))) return;
      const existingSessionProjection = projection.providerSessions.find(
        (candidate) => candidate.id === providerSessionId,
      );
      // A run that ended without completing can leave its provider turn projected as
      // running when the agent ignored Stop or an interrupt timed out. Every attempt
      // of such a run owns a possible abandoned turn, including attempts a steering
      // restart superseded, and the session cannot be reused until those turns stop.
      const abandonedRuns = new Map<string | null, OrchestrationV2Run>();
      for (const previous of projection.runs) {
        if (
          previous.ordinal >= run.ordinal ||
          previous.providerThreadId !== providerThread.id ||
          !NON_COMPLETING_TERMINAL_RUN_STATUSES.has(previous.status)
        ) {
          continue;
        }
        if (previous.activeAttemptId !== null) {
          abandonedRuns.set(previous.activeAttemptId, previous);
        }
        for (const attempt of projection.attempts) {
          if (attempt.runId === previous.id) abandonedRuns.set(attempt.id, previous);
        }
      }
      const abandonedTurns = projection.providerTurns.flatMap((turn) => {
        const previous = abandonedRuns.get(turn.runAttemptId);
        return turn.providerThreadId === providerThread.id &&
          turn.status === "running" &&
          previous !== undefined
          ? [{ turn, run: previous }]
          : [];
      });
      const abandonedTurn = abandonedTurns.at(-1)?.turn;
      if (abandonedTurn !== undefined) {
        const liveSession = yield* providerSessions.get(providerSessionId);
        if (Option.isSome(liveSession)) {
          // The run already ended without stopping its provider — a failed start, an
          // ignored Stop, or an interrupt that never confirmed. Recover that exact
          // turn before reusing its native conversation.
          const recovered = yield* Effect.result(
            liveSession.value
              .interruptTurn({
                providerThread,
                providerTurnId: abandonedTurn.id,
                requestRuntimeRestart: true,
                // Jones: recovering an abandoned turn keeps its native goal.
                preserveGoal: true,
              })
              .pipe(guardClaim),
          );
          if (recovered._tag === "Failure") {
            if (input.willRetry === true && !terminalStartFailure(recovered.failure))
              return yield* recovered.failure;
            yield* settleStartFailure({
              signal: "provider-turn-recovery-failure",
              title: "Provider turn recovery failed",
              error: recovered.failure,
            });
            return;
          }
        }
        const completedAt = yield* DateTime.now;
        const repaired = yield* eventSink.writeIfRunCurrent({
          threadId: projection.thread.id,
          runId: run.id,
          activeAttemptId: attempt.id,
          expectedStatus: "starting",
          events: yield* Effect.forEach(abandonedTurns, ({ turn, run: abandonedRun }) =>
            Effect.gen(function* () {
              return {
                id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
                type: "provider-turn.updated" as const,
                threadId: projection.thread.id,
                runId: abandonedRun.id,
                nodeId: turn.nodeId,
                providerInstanceId: abandonedRun.providerInstanceId,
                occurredAt: completedAt,
                payload: { ...turn, status: "interrupted" as const, completedAt },
              };
            }),
          ),
        });
        if (!repaired.committed) return;
      }
      // Opening a session does not prompt the provider, so the workspace is still unchanged.
      const baseline = yield* runExecution.forkBaseline({
        appThread: projection.thread,
        run,
        checkpointScope,
      });
      const keepWarm = isWorkModeKeepWarm(message);
      const plannedWorkResume =
        keepWarm &&
        Option.isSome(plannedContinuity) &&
        (yield* plannedContinuity.value
          .allowWorkStart(projection.thread.id, message.id)
          .pipe(Effect.orElseSucceed(() => false)));
      const replacementCurrent = yield* guardClaim(revalidateReplacement).pipe(Effect.result);
      if (replacementCurrent._tag === "Failure") {
        if (input.willRetry === true && !terminalStartFailure(replacementCurrent.failure))
          return yield* replacementCurrent.failure;
        yield* settleStartFailure({
          signal: "provider-selection-changed-before-open",
          title: "Provider selection changed before opening",
          error: replacementCurrent.failure,
        });
        return;
      }
      yield* assertAdmission(providerThread);
      const sessionResult = yield* Effect.result(
        keepWarm && !plannedWorkResume
          ? providerSessions.get(providerSessionId).pipe(
              Effect.flatMap((live) =>
                Option.isSome(live) && providerThread.nativeThreadRef !== null
                  ? Effect.succeed(live.value)
                  : Effect.fail(
                      new ProviderSessionManager.ProviderSessionOpenError({
                        instanceId: run.providerInstanceId,
                        providerSessionId,
                        cause: "Keep-warm requires the existing live provider conversation.",
                      }),
                    ),
              ),
            )
          : providerSessions.open({
              threadId: projection.thread.id,
              providerSessionId,
              revalidateStartAdmission: assertAdmission(providerThread),
              ...(capturedAdmission.nativeCreationGuard === undefined
                ? {}
                : {
                    nativeCreationGuard: capturedAdmission.nativeCreationGuard,
                  }),
              modelSelection: run.modelSelection,
              runtimePolicy: resolvedRuntimePolicy,
              ...(existingSessionProjection === undefined
                ? {}
                : { resumeFromSession: existingSessionProjection }),
              ...(providerThread.nativeThreadRef?.nativeId == null
                ? {}
                : { initialNativeThreadId: providerThread.nativeThreadRef.nativeId }),
              ...(providerThread.nativeMetadata?.itemIdentityVersion === undefined
                ? {}
                : {
                    initialProviderItemIdentityVersion:
                      providerThread.nativeMetadata.itemIdentityVersion,
                  }),
            }),
      );
      if (sessionResult._tag === "Failure") {
        const failure = sessionResult.failure;
        const requiresSignIn =
          "cause" in failure &&
          isProviderAdapterOpenSessionError(failure.cause) &&
          failure.cause.requiresSignIn === true;
        if (
          input.willRetry === true &&
          !requiresSignIn &&
          findSteerFailure(failure) === undefined &&
          !terminalStartFailure(failure)
        )
          return yield* failure;
        yield* settleStartFailure({
          signal: "provider-session-open-failure",
          title: "Provider session failed to open",
          error: sessionResult.failure,
        });
        return;
      }
      const rawSession = sessionResult.success;
      const session = {
        ...rawSession,
        get providerSession() {
          return rawSession.providerSession;
        },
        ensureThread: (value: Parameters<typeof rawSession.ensureThread>[0]) =>
          assertAdmission(providerThread).pipe(
            Effect.andThen(
              rawSession.ensureThread({
                ...value,
                ...(capturedAdmission.nativeCreationGuard === undefined
                  ? {}
                  : { nativeCreationGuard: capturedAdmission.nativeCreationGuard }),
              }),
            ),
            Effect.provideService(
              StartAdmission.CurrentRevalidation,
              assertAdmission(providerThread),
            ),
          ),
        resumeThread: (value: Parameters<typeof rawSession.resumeThread>[0]) =>
          assertAdmission(providerThread).pipe(
            Effect.andThen(rawSession.resumeThread(value)),
            Effect.provideService(
              StartAdmission.CurrentRevalidation,
              assertAdmission(providerThread),
            ),
          ),
        forkThread: (value: Parameters<typeof rawSession.forkThread>[0]) =>
          assertAdmission(providerThread).pipe(
            Effect.andThen(rawSession.forkThread(value)),
            Effect.provideService(
              StartAdmission.CurrentRevalidation,
              assertAdmission(providerThread),
            ),
          ),
      };
      if (!(yield* isCurrentAttemptInStatus("starting"))) return;
      const loadFromProvider = Effect.fn("ProviderTurnStartService.loadFromProvider")(function* (
        load: Effect.Effect<OrchestrationV2ProviderThread, ProviderAdapter.ProviderAdapterV2Error>,
      ) {
        const loaded = yield* Effect.result(guardClaim(load));
        if (loaded._tag === "Success") return loaded.success;
        if (
          input.willRetry === true &&
          !terminalStartFailure(loaded.failure) &&
          findSteerFailure(loaded.failure) === undefined &&
          !ProviderAdapter.hasUnknownRuntimeBinding(loaded.failure)
        )
          return yield* loaded.failure;
        yield* settleStartFailure({
          signal: "provider-thread-load-failure",
          title: "Provider turn failed to start",
          error: loaded.failure,
        });
        return undefined;
      });
      let effectiveHandoffs = handoffs;
      let resumeFellBack = false;
      const userText =
        manualResume && unsentResumeMessage === undefined
          ? ""
          : (conversationTurn === null ? "" : `${conversationTurn.context}\n\n`) +
            workModeProviderPrompt(
              projectComposerContextForProvider({
                text: providerMessage.text,
                records: providerMessage.context?.records ?? [],
              }),
            );
      // Native goal commands go verbatim; handoff history waits a turn.
      const goalDriver =
        session.driver === "codex" ||
        session.driver === "claudeAgent" ||
        session.providerSession.capabilities.turns.supportsNativeGoals === true;
      const goalCommand = goalDriver && parseGoalCommand(providerMessage) !== null;
      // The provider turn marks cancellation work delivered; restart continuations use their own text or resume natively.
      const noteContinuation = isRestartNoteContinuation(
        run,
        projection.runs,
        projection.providerTurns,
        projection.attempts,
      );
      const restartCancelledWork = pendingRestartCancelledBackgroundWork({
        runs: projection.runs,
        providerTurns: projection.providerTurns,
        compactionMessageIds: new Set(
          projection.messages
            .filter(
              (candidate) =>
                isCompactionCommandMessage(candidate) ||
                (goalDriver && parseGoalCommand(candidate) !== null),
            )
            .map((candidate) => candidate.id),
        ),
        run,
        attempts: projection.attempts,
      });
      const restartNote =
        restartCancelledWork.length === 0
          ? ""
          : restartCancelledBackgroundWorkNote(restartCancelledWork);
      const tokenCap = yield* handoffTokenCapConfig.pipe(
        Effect.orElseSucceed(() => DEFAULT_HANDOFF_TOKEN_CAP),
      );
      const handoffContextForThread = (loadedProviderThread: OrchestrationV2ProviderThread) => {
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
          loadedProviderThread.nativeThreadRef?.nativeId ===
          providerThread.nativeThreadRef?.nativeId;
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
        const handoffUsage = contextUsageForHandoff({
          sameNativeThread,
          sameSelection,
          reuseTelemetry,
          previousUsage,
          knownModelWindow,
        });
        const settledHandoffs = projection.contextHandoffs.filter(
          (handoff) =>
            handoff.toProviderThreadId === providerThread.id &&
            handoff.delivery?.nativeThreadId === loadedProviderThread.nativeThreadRef?.nativeId &&
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
                source.nativeThreadId === loadedProviderThread.nativeThreadRef?.nativeId,
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
        // Row identity survives recovery; only accepted native input and delivered history occupy the new session.
        const nativeContextEstimate = Effect.gen(function* () {
          return sameNativeThread
            ? (yield* projectionStore.getTurnStartHistory(input.threadId)).reduce((sum, item) => {
                const acceptedNatively =
                  item.providerThreadId === providerThread.id &&
                  item.runId !== null &&
                  (nativeInputRunIds.has(item.runId) ||
                    (legacyInputRunIds.has(item.runId) &&
                      !coveredItemIds.has(item.id) &&
                      !legacyRecoveredRunIds.has(item.runId)));
                if (item.runId === run.id || (!acceptedNatively && !deliveredItemIds.has(item.id)))
                  return sum;
                const historical = historicalMessage(item);
                const nativeAttachments =
                  item.type === "user_message" && acceptedNatively
                    ? attachmentTokenAllowance(item.attachments)
                    : 0;
                return (
                  sum +
                  (historical === null ? 0 : estimateTextTokens(historical.text)) +
                  nativeAttachments
                );
              }, 0)
            : 0;
        });
        const modelContextWindow =
          knownModelWindow ??
          (handoffUsage !== null || reuseTelemetry ? previousUsage?.maxTokens : undefined);
        const budgetProviderThread = {
          ...loadedProviderThread,
          contextUsage: handoffUsage,
        };
        // The resume fallback summary already covers every earlier run in order. A
        // retry copy after it would rank older missed requests as the newest history.
        const missedRuns = projection.runs.filter(
          (source) =>
            !resumeFellBack &&
            source.ordinal < run.ordinal &&
            source.providerThreadId === providerThread.id &&
            (source.status === "failed" || source.status === "interrupted") &&
            !deliveredAttemptIds.has(source.activeAttemptId),
        );
        const missedRunIds = new Set(missedRuns.map((source) => source.id));
        // Jones: the budget after a native compaction reports this usage; zero
        // occupancy bounds what compaction could reclaim.
        const budgetAfter = (
          contextUsage: NonNullable<OrchestrationV2ProviderThread["contextUsage"]>,
        ) =>
          handoffBudget({
            tokenCap,
            modelContextWindow,
            userText: restartNote === "" ? userText : `${restartNote}\n\n${userText}`,
            attachments: providerMessage.attachments,
            providerThread: { ...budgetProviderThread, contextUsage },
            nativeContextEstimate: 0,
          });
        return {
          budgetAfter,
          reclaimableBudget:
            budgetProviderThread.contextUsage === null ||
            budgetProviderThread.contextUsage === undefined
              ? undefined
              : budgetAfter({ ...budgetProviderThread.contextUsage, usedTokens: 0 }),
          sameNativeThread,
          handoffUsage,
          deliveredItemIds,
          coveredItemIds,
          acceptedAttempts,
          nativeInputRunIds,
          legacyInputRunIds,
          missedRuns,
          missedRunIds,
          budget: Effect.gen(function* () {
            return handoffBudget({
              tokenCap,
              modelContextWindow,
              userText: restartNote === "" ? userText : `${restartNote}\n\n${userText}`,
              attachments: providerMessage.attachments,
              providerThread: budgetProviderThread,
              nativeContextEstimate:
                budgetProviderThread.contextUsage?.usedTokens === undefined
                  ? yield* nativeContextEstimate
                  : 0,
            });
          }),
        };
      };
      const prepareRetryHandoff = Effect.fn("ProviderTurnStartService.prepareRetryHandoff")(
        function* (context: ReturnType<typeof handoffContextForThread>) {
          const { missedRunIds, coveredItemIds, missedRuns } = context;
          const missedItems =
            missedRunIds.size === 0
              ? []
              : (yield* projectionStore.getTurnStartHistory(input.threadId, [
                  ...missedRunIds,
                ])).filter(
                  (item) =>
                    item.runId !== null &&
                    missedRunIds.has(item.runId) &&
                    !coveredItemIds.has(item.id) &&
                    historicalMessage(item) !== null,
                );
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
          return retryHandoff;
        },
      );
      const prepareRecoveryHandoff = Effect.fn("ProviderTurnStartService.prepareRecoveryHandoff")(
        function* () {
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
          return { handoff, transferId, createdAt };
        },
      );
      const recoveryHandoff = yield* Effect.cached(prepareRecoveryHandoff());
      const withRecoveryHandoff = (handoff: OrchestrationV2ContextHandoff) => {
        // Delivery ranks later handoffs as newer history. Pending handoffs from
        // earlier runs predate this summary; handoffs for this run (provider-switch
        // delta, merge-back) are newer and must stay last.
        return [
          ...effectiveHandoffs.filter((candidate) => candidate.targetRunId !== run.id),
          handoff,
          ...effectiveHandoffs.filter((candidate) => candidate.targetRunId === run.id),
        ];
      };
      let resumedRetryHandoff: ReadonlyArray<OrchestrationV2ContextHandoff> | undefined;
      let resumedHandoffContext: ReturnType<typeof handoffContextForThread> | undefined;
      let portableForkFallback: OrchestrationV2ContextHandoff | undefined;
      let recoveryTurnItem: OrchestrationV2TurnItem | undefined;
      const loadedProviderThread = yield* Effect.gen(function* () {
        if (nativeForkTransfer !== undefined) {
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
          const sourceProviderTurn =
            latestProviderTurnForAttempt(sourceProjection.providerTurns, sourceAttempt?.id) ??
            sourceProjection.providerTurns.find(
              (candidate) => candidate.id === sourceAttempt?.providerTurnId,
            );
          if (sourceRun === undefined || sourceProviderThread === undefined) {
            return yield* new ProviderTurnStartError({
              runId,
              cause: `Native fork transfer ${nativeForkTransfer.id} has no source provider execution.`,
            });
          }
          // Providers refuse to fork a busy thread, so a source that started a turn after
          // dispatch planned this fork gets the portable context dispatch gives a busy source.
          const sourceBusy = sourceProjection.runs.some(
            (candidate) => candidate.ordinal > sourceRun.ordinal && isActiveRun(candidate),
          );
          let nativeForkFailure: string | undefined;
          if (!sourceBusy) {
            // Rolled-back turns are no longer in the source conversation, so they cannot bound a fork.
            const rolledBackAttemptIds = new Set(
              sourceProjection.attempts
                .filter((attempt) =>
                  sourceProjection.runs.some(
                    (candidate) =>
                      candidate.id === attempt.runId && candidate.status === "rolled_back",
                  ),
                )
                .map((attempt) => attempt.id),
            );
            const forked = yield* Effect.result(
              session.forkThread({
                sourceProviderThread,
                sourceProviderTurns: sourceProjection.providerTurns.filter(
                  (turn) =>
                    turn.runAttemptId === null || !rolledBackAttemptIds.has(turn.runAttemptId),
                ),
                targetThreadId: projection.thread.id,
                targetProviderThread: providerThread,
                modelSelection: run.modelSelection,
                runtimePolicy: resolvedRuntimePolicy,
                ...(sourceProviderTurn === undefined
                  ? {}
                  : { providerTurnId: sourceProviderTurn.id }),
              }),
            );
            if (forked._tag === "Success") return forked.success;
            if (
              input.willRetry === true ||
              ProviderAdapter.hasUnknownRuntimeBinding(forked.failure)
            )
              return yield* forked.failure;
            if (!(yield* isCurrentAttemptInStatus("starting"))) return undefined;
            // Retries are spent (e.g. the source's native session is gone): start fresh with portable history.
            yield* Effect.logWarning("Starting a native fork in a fresh native session", {
              driver: session.driver,
              providerThreadId: providerThread.id,
              sourceProviderThreadId: sourceProviderThread.id,
              runId,
              errorTag: forked.failure._tag,
            });
            nativeForkFailure = makeProviderFailure({
              cause: forked.failure,
              message: startFailureMessage(forked.failure),
              class: "provider_error",
            }).message;
          }
          const nativeForkFailed = nativeForkFailure !== undefined;
          const sourceItems = yield* ContextHandoffService.readForkHistoryItems(
            projectionStore,
            sourceProjection,
            sourceRun,
          );
          const createdAt = yield* DateTime.now;
          const handoff = yield* contextHandoffService.prepareProviderHandoff({
            threadId: projection.thread.id,
            targetRunId: run.id,
            transferId: nativeForkTransfer.id,
            fromProviderThreadIds: [sourceProviderThread.id],
            toProviderThreadId: providerThread.id,
            fromProviderInstanceId: sourceRun.providerInstanceId,
            toProviderInstanceId: run.providerInstanceId,
            coveredRunOrdinals: ContextHandoffService.visibleDeltaRunOrdinals(
              sourceProjection,
              sourceItems,
            ),
            strategy: "full_thread_summary",
            items: sourceItems,
            runs: sourceProjection.runs,
            createdAt,
          });
          const loaded = yield* loadFromProvider(
            session.ensureThread({
              threadId: projection.thread.id,
              title: projection.thread.title,
              modelSelection: run.modelSelection,
              runtimePolicy: resolvedRuntimePolicy,
              providerSessionId,
              existingProviderThread: nativeForkFailed
                ? { ...providerThread, nativeThreadRef: null, forkedFrom: null }
                : { ...providerThread, forkedFrom: null },
              // The failed fork may have left the adapter's session half-switched.
              ...(nativeForkFailed ? { forceNewNativeThread: true } : {}),
            }),
          );
          if (loaded === undefined) return undefined;
          portableForkFallback = handoff;
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
                  ...nativeForkTransfer,
                  targetProviderInstanceId: run.providerInstanceId,
                  status: "resolved_portable",
                  resolution: { strategy: "portable_context", contextHandoffId: handoff.id },
                  error: null,
                  updatedAt: createdAt,
                },
              },
              ...(nativeForkFailure === undefined
                ? []
                : [
                    {
                      id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
                      type: "turn-item.updated" as const,
                      threadId: projection.thread.id,
                      runId: run.id,
                      providerInstanceId: run.providerInstanceId,
                      occurredAt: createdAt,
                      payload: {
                        id: idAllocator.derive.runSignalTurnItem({
                          runId,
                          signal: "native-fork-fallback",
                        }),
                        threadId: projection.thread.id,
                        runId,
                        nodeId: rootNode.id,
                        providerThreadId: providerThread.id,
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
                        status: "completed" as const,
                        title: "Native fork failed",
                        startedAt: createdAt,
                        completedAt: createdAt,
                        updatedAt: createdAt,
                        type: "system_notice" as const,
                        message: `Could not fork the source conversation natively (${nativeForkFailure}). Started a fresh session with its history.`,
                      },
                    },
                  ]),
            ],
          });
          return loaded;
        }
        if (providerThread.nativeThreadRef === null) {
          if (
            keepWarm ||
            preserveNativeConversation ||
            manualResume ||
            guarded ||
            continuityRequired
          ) {
            return yield* loadFromProvider(
              Effect.fail(
                new ProviderAdapter.ProviderAdapterTurnStartError({
                  driver: session.driver,
                  threadId: projection.thread.id,
                  providerThreadId: providerThread.id,
                  runId,
                  cause:
                    "The required native conversation is unavailable; a new conversation cannot replace it.",
                }),
              ),
            );
          }
          // Pass the stored provider row for adoption; adapter-minted duplicates make activeProviderThreadId flap.
          return yield* loadFromProvider(
            session.ensureThread({
              threadId: projection.thread.id,
              title: projection.thread.title,
              modelSelection: run.modelSelection,
              runtimePolicy: resolvedRuntimePolicy,
              providerSessionId,
              existingProviderThread: providerThread,
            }),
          );
        }
        const uncertainDelivery = projection.contextHandoffs.some(
          (handoff) =>
            handoff.toProviderThreadId === providerThread.id &&
            handoff.delivery?.nativeThreadId === providerThread.nativeThreadRef?.nativeId &&
            handoff.delivery?.status === "pending",
        );
        const resumed = yield* Effect.result(
          uncertainDelivery
            ? Effect.fail(
                new ProviderAdapter.ProviderAdapterTurnStartError({
                  driver: session.driver,
                  threadId: projection.thread.id,
                  providerThreadId: providerThread.id,
                  runId,
                  cause: "Uncertain native history injection",
                }),
              )
            : session.resumeThread({
                providerThread,
                threadId: projection.thread.id,
                modelSelection: run.modelSelection,
                runtimePolicy: resolvedRuntimePolicy,
              }),
        );
        if (resumed._tag === "Success") {
          if (
            (preserveNativeConversation || manualResume || guarded || continuityRequired) &&
            (resumed.success.nativeThreadRef?.nativeId !==
              providerThread.nativeThreadRef.nativeId ||
              resumed.success.nativeThreadRef?.driver !== providerThread.nativeThreadRef.driver)
          ) {
            return yield* loadFromProvider(
              Effect.fail(
                new ProviderAdapter.ProviderRuntimeBindingError({
                  driver: session.driver,
                  detail: "The loaded native conversation differs from the required conversation",
                }),
              ),
            );
          }
          const context = handoffContextForThread(resumed.success);
          context.budget = yield* Effect.cached(context.budget);
          resumedHandoffContext = context;
          resumedRetryHandoff = yield* prepareRetryHandoff(context);
          const fits = yield* deliverContextHandoffs({
            handoffs: [...handoffs, ...resumedRetryHandoff],
            providerThread: { ...resumed.success, id: providerThread.id },
            budget: context.budget,
            deferInline: isCompactionCommandMessage(providerMessage) || goalCommand,
            alreadyDeliveredItemIds: context.deliveredItemIds,
            persist: () => Effect.void,
          }).pipe(
            Effect.as(true),
            Effect.catchTags({ ContextHandoffBudgetError: () => Effect.succeed(false) }),
          );
          if (fits) return resumed.success;
          // Jones: a compaction-capable provider keeps its admitted native
          // conversation; dispatch compacts once and sends the intact request.
          if (session.compactForContextHandoff !== undefined) return resumed.success;
          if (
            keepWarm ||
            preserveNativeConversation ||
            manualResume ||
            guarded ||
            continuityRequired
          ) {
            return yield* loadFromProvider(
              Effect.fail(
                new ProviderAdapter.ProviderAdapterTurnStartError({
                  driver: session.driver,
                  threadId: projection.thread.id,
                  providerThreadId: providerThread.id,
                  runId,
                  cause:
                    "The native conversation must be retained; its handoff exceeds the available budget.",
                }),
              ),
            );
          }
          const freshThread = { ...resumed.success, nativeThreadRef: null };
          const freshContext = handoffContextForThread(freshThread);
          const { handoff } = yield* recoveryHandoff;
          const fitsFresh = yield* deliverContextHandoffs({
            handoffs: withRecoveryHandoff(handoff),
            providerThread: freshThread,
            budget: freshContext.budget,
            alreadyDeliveredItemIds: freshContext.deliveredItemIds,
            persist: () => Effect.void,
          }).pipe(
            Effect.as(true),
            Effect.catchTags({ ContextHandoffBudgetError: () => Effect.succeed(false) }),
          );
          if (!fitsFresh) return resumed.success;
          const hasPendingWork = yield* (
            session.hasPendingBackgroundWorkForThread?.(resumed.success, {
              includeSubagents: true,
            }) ??
              session.hasPendingBackgroundWork ??
              Effect.succeed(false)
          );
          if (hasPendingWork) {
            return yield* loadFromProvider(
              Effect.fail(
                new ProviderAdapter.ProviderAdapterTurnStartError({
                  driver: session.driver,
                  threadId: projection.thread.id,
                  providerThreadId: providerThread.id,
                  runId,
                  cause: new ContextHandoffBackgroundWorkError(),
                }),
              ),
            );
          }
        }
        if (
          keepWarm ||
          preserveNativeConversation ||
          manualResume ||
          guarded ||
          continuityRequired ||
          (resumed._tag === "Failure" &&
            (ProviderAdapter.hasUnknownRuntimeBinding(resumed.failure) ||
              terminalStartFailure(resumed.failure) ||
              findSteerFailure(resumed.failure) !== undefined))
        ) {
          return yield* loadFromProvider(
            Effect.fail(
              resumed._tag === "Failure"
                ? resumed.failure
                : new ProviderAdapter.ProviderAdapterTurnStartError({
                    driver: session.driver,
                    threadId: projection.thread.id,
                    providerThreadId: providerThread.id,
                    runId,
                    cause: "Native conversation recovery cannot create a replacement conversation.",
                  }),
            ),
          );
        }
        resumedHandoffContext = undefined;
        resumedRetryHandoff = [];
        if (!(yield* isCurrentAttemptInStatus("starting"))) return undefined;
        // Another client is still writing to the native thread. A fresh
        // session here would silently split the conversation from it.
        if (
          resumed._tag === "Failure" &&
          resumed.failure._tag === "ProviderAdapterThreadInUseError"
        ) {
          return yield* loadFromProvider(Effect.fail(resumed.failure));
        }

        yield* Effect.logWarning("Recovering provider context in a fresh native session", {
          driver: session.driver,
          providerThreadId: providerThread.id,
          runId,
          reason: uncertainDelivery
            ? "uncertain_history_delivery"
            : resumed._tag === "Success"
              ? "handoff_budget_exhausted"
              : "resume_failed",
          errorTag: resumed._tag === "Failure" ? resumed.failure._tag : "ContextHandoffBudgetError",
        });
        const replacement = yield* loadFromProvider(
          session.ensureThread({
            threadId: projection.thread.id,
            title: projection.thread.title,
            modelSelection: run.modelSelection,
            runtimePolicy: resolvedRuntimePolicy,
            providerSessionId,
            // Clear the native ref to force a fresh session while preserving row identity.
            existingProviderThread: { ...providerThread, nativeThreadRef: null },
            forceNewNativeThread: true,
          }),
        );
        if (replacement === undefined) return undefined;
        const { handoff, transferId, createdAt } = yield* recoveryHandoff;
        effectiveHandoffs = withRecoveryHandoff(handoff);
        resumeFellBack = true;
        const previousRun = projection.runs.findLast(
          (candidate) =>
            candidate.providerThreadId === providerThread.id &&
            candidate.ordinal < run.ordinal &&
            candidate.startedAt !== null,
        );
        // Stable per run so a retried start reuses the reserved run-start position.
        const handoffTurnItemId = idAllocator.derive.runSignalTurnItem({
          runId,
          signal: "resume-fallback-handoff",
        });
        const handoffTurnItem: OrchestrationV2TurnItem = {
          id: handoffTurnItemId,
          threadId: projection.thread.id,
          runId,
          nodeId: rootNode.id,
          providerThreadId: providerThread.id,
          providerTurnId: null,
          nativeItemRef: null,
          parentItemId: null,
          // The summary is delivered ahead of this run's own handoffs and message.
          ordinal: yield* turnItemPositions.allocate({
            threadId: projection.thread.id,
            turnItemId: handoffTurnItemId,
            runId,
            runOrdinal: run.ordinal,
            placement: "run_start",
          }),
          status: "completed",
          title: "Recovered context",
          startedAt: createdAt,
          completedAt: createdAt,
          updatedAt: createdAt,
          type: "handoff",
          contextHandoffId: handoff.id,
          fromProviderThreadIds: handoff.fromProviderThreadIds,
          toProviderThreadId: handoff.toProviderThreadId,
          fromProviderInstanceIds: [providerThread.providerInstanceId],
          toProviderInstanceId: run.providerInstanceId,
          fromModelSelections: previousRun === undefined ? [] : [previousRun.modelSelection],
          toModel: run.modelSelection.model,
          strategy: handoff.strategy,
          summary: handoff.summaryText,
        };
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
        recoveryTurnItem = handoffTurnItem;
        return replacement;
      });
      if (loadedProviderThread === undefined) return;
      if (
        plannedWorkResume &&
        (loadedProviderThread.nativeThreadRef?.nativeId !==
          providerThread.nativeThreadRef?.nativeId ||
          loadedProviderThread.nativeThreadRef?.driver !== providerThread.nativeThreadRef?.driver)
      )
        return yield* admissionError(
          "Planned work cannot replace its original native conversation.",
        );
      if (
        requiredNativeSession !== undefined &&
        !loadedSessionMatches(requiredNativeSession, providerThread, loadedProviderThread)
      ) {
        yield* settleStartFailure({
          signal: "task-continuation-native-identity",
          title: "Delegated task continuation could not resume its original provider conversation",
          error: new ProviderTurnStartError({
            runId,
            cause:
              "The provider loaded a different native conversation for this task continuation.",
          }),
        });
        return;
      }
      if (!(yield* isCurrentAttemptInStatus("starting"))) {
        return;
      }
      const now = yield* DateTime.now;
      const handoffContext = resumedHandoffContext ?? handoffContextForThread(loadedProviderThread);
      const {
        sameNativeThread,
        handoffUsage,
        deliveredItemIds,
        acceptedAttempts,
        nativeInputRunIds,
        legacyInputRunIds,
        budget,
      } = handoffContext;
      if (manualResume && unsentResumeMessage === undefined && session.resumeTurn === undefined) {
        yield* settleStartFailure({
          signal: "resume-unsupported",
          title: "Native resume is unavailable",
          error: new ProviderAdapter.ProviderResumeUnsupportedError({
            driver: session.driver,
            providerThreadId: providerThread.id,
          }),
        });
        return;
      }
      const retryHandoff = resumedRetryHandoff ?? (yield* prepareRetryHandoff(handoffContext));
      const runningProviderThread: OrchestrationV2ProviderThread = {
        ...loadedProviderThread,
        runtimeIdentity: ProviderAdapter.identityForRequest(
          ProviderAdapter.requestedRuntimeIdentity(run.modelSelection, session.driver),
          loadedProviderThread.runtimeIdentity,
        ),
        contextUsage: handoffUsage,
        id: providerThread.id,
        driver: session.driver,
        providerInstanceId: run.providerInstanceId,
        providerSessionId,
        appThreadId: projection.thread.id,
        ownerNodeId: providerThread.ownerNodeId,
        firstRunOrdinal: providerThread.firstRunOrdinal ?? run.ordinal,
        lastRunOrdinal: run.ordinal,
        handoffIds:
          portableForkFallback === undefined
            ? providerThread.handoffIds
            : [...providerThread.handoffIds, portableForkFallback.id],
        forkedFrom: portableForkFallback === undefined ? providerThread.forkedFrom : null,
        status: "active",
        createdAt: providerThread.createdAt,
        updatedAt: now,
      };
      const runningRun: OrchestrationV2Run = {
        ...run,
        status: "running",
        startedAt: run.startedAt ?? now,
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
        ...(nativeForkTransfer === undefined ||
        portableForkFallback !== undefined ||
        runningProviderThread.nativeThreadRef === null
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
        // Shown only once the replacement session commits with the running transition.
        ...(recoveryTurnItem === undefined
          ? []
          : [
              {
                id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
                type: "turn-item.updated" as const,
                threadId: projection.thread.id,
                runId: run.id,
                providerInstanceId: run.providerInstanceId,
                occurredAt: now,
                payload: recoveryTurnItem,
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
      // Claude allocates an ID while loading a fresh thread, before the first
      // native query has a generation to publish.
      const recordsFreshClaudeId =
        session.driver === "claudeAgent" &&
        nativeForkTransfer === undefined &&
        requiredNativeSession === undefined &&
        capturedAdmission.nativeCreationGuard === undefined &&
        providerThread.nativeThreadRef === null &&
        providerThread.runtimeIdentity?.runtimeGeneration === undefined &&
        runningProviderThread.nativeThreadRef?.driver === session.driver &&
        runningProviderThread.runtimeIdentity?.runtimeGeneration === undefined;
      if (recordsFreshClaudeId) yield* capturedAdmission.revalidate;
      const runningWrite = yield* eventSink.writeIfRunCurrent({
        threadId: projection.thread.id,
        runId: run.id,
        activeAttemptId: attempt.id,
        expectedStatus: "starting",
        ...(recordsFreshClaudeId
          ? {
              runtimeIdentityBoundary: {
                expectedGeneration: null,
                expectedProviderThreadIdentity: providerThread,
              },
            }
          : {}),
        events,
      });
      if (!runningWrite.committed) {
        return;
      }
      if (recordsFreshClaudeId) {
        // The same claim and accepted receipt now authorize the ID recorded
        // by this attempt's guarded write. Native publication still binds
        // the actual query generation when the provider starts.
        const recordedAdmission = yield* admissionService.value.capture({
          threadId: input.threadId,
          run,
          attemptId: attempt.id,
          providerThread: runningProviderThread,
        });
        if (recordedAdmission.nativeCreationGuard !== capturedAdmission.nativeCreationGuard)
          return yield* admissionError(
            "Recording the allocated ID changed the original native guard.",
          );
        capturedAdmission = recordedAdmission;
      }
      // Delegated (app_owned) children run their own runs; routing them here ingests their items twice.
      const routableSubagents = projection.subagents.filter(
        (subagent) =>
          subagent.origin === "provider_native" &&
          RunExecutionService.canRouteRelatedSubagent(subagent.status),
      );
      const persistHandoff = Effect.fn("ProviderTurnStartService.persist")(function* (
        handoff: OrchestrationV2ContextHandoff,
      ) {
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
      });
      const startWithHandoffs = Effect.fn("ProviderTurnStartService.startWithHandoffs")(
        function* (turnInput: Parameters<typeof session.startTurn>[0], compact = false) {
          if (turnInput.resume === true) {
            yield* assertAdmission(runningProviderThread);
            if (session.resumeTurn === undefined)
              return yield* admissionError(
                "Native Resume is unavailable; prompt replay is refused.",
              );
            const { message: resumeMessage, resume: _resume, ...rest } = turnInput;
            // Pending context stays pending; Resume never becomes a synthetic prompted start.
            return yield* session.resumeTurn({ ...rest, messageId: resumeMessage.messageId });
          }
          // Jones: every delivery effect revalidates this attempt's admission, and
          // only a local budget failure may compact once before redelivery.
          const revalidateHandoff = Effect.gen(function* () {
            if (!(yield* isCurrentAttemptInStatus("running")))
              return yield* admissionError("The original attempt stopped before handoff delivery.");
            yield* assertAdmission(runningProviderThread);
          }).pipe(
            Effect.mapError((cause) =>
              isProviderAdapterTurnStartError(cause) ? cause : admissionError(cause),
            ),
          );
          const { reclaimableBudget } = handoffContext;
          const delivery = yield* deliverWithHandoffRecovery(
            {
              handoffs: [...effectiveHandoffs, ...retryHandoff],
              deferInline: compact || goalCommand,
              providerThread: runningProviderThread,
              budget,
              alreadyDeliveredItemIds: deliveredItemIds,
              ...(session.injectHistory === undefined
                ? {}
                : {
                    inject: (history: ProviderAdapter.ProviderAdapterV2HistoricalContext) =>
                      session.injectHistory!({
                        providerThread: runningProviderThread,
                        ...history,
                      }),
                  }),
              persist: persistHandoff,
            },
            {
              input: turnInput,
              ...(compact || goalCommand || reclaimableBudget === undefined
                ? {}
                : { compact: session.compactForContextHandoff }),
              revalidate: revalidateHandoff,
              budgetAfter: handoffContext.budgetAfter,
              reclaimableBudget: reclaimableBudget ?? 0,
            },
          );
          if (!(yield* isCurrentAttemptInStatus("running"))) return;
          const start = (value: Parameters<typeof session.startTurn>[0]) =>
            startWithConfiguredEffort(value, compact, delivery.unsent);
          // Like history, the side chat context waits for a turn that sends a prompt.
          const sideChatContext =
            sideChatHandoff === undefined || compact || goalCommand
              ? ""
              : sideChatHandoff.summaryText;
          const context = [delivery.context, sideChatContext, restartNote]
            .filter((part) => part !== "")
            .join("\n\n");
          const { restartContinuationOfRunId: _resumedRunId, ...promptedInput } = turnInput;
          const text = goalCommand ? userText : withInlineContext(context, userText);
          // Jones: Codex sends no input for a bare restart continuation, so Codex,
          // a fresh native conversation, a restart note or inline context keeps the
          // prompt. The continuation owner decides; upstream keeps the text format.
          const startInput =
            goalCommand && !noteContinuation
              ? turnInput
              : continuationPrompt(turnInput, {
                    sameNativeThread,
                    noteContinuation,
                    context,
                    userText,
                  }).restartContinuationOfRunId === undefined
                ? promptedInput
                : turnInput;
          const { resume: _resume, ...contextInput } = startInput;
          const { message: resumeMessage, resume: _resumeIntent, ...resumeInput } = startInput;
          const operation =
            turnInput.resume && context === "" && session.resumeTurn !== undefined
              ? session.resumeTurn({ ...resumeInput, messageId: resumeMessage.messageId })
              : start({
                  ...(context === "" ? startInput : contextInput),
                  message: {
                    ...turnInput.message,
                    text,
                  },
                  ...(text === userText
                    ? {}
                    : { inlineContextLength: text.length - userText.length }),
                });
          yield* assertAdmission(runningProviderThread);
          yield* operation.pipe(
            // Clear pending handoff markers for refused prompts so the next turn retains the native session.
            // Jones: a current-attempt proof that Codex did not apply the prompt also restores them.
            Effect.tapError((error) =>
              refusedBeforePrompt(error) || promptWasNotApplied(error)
                ? delivery.unsent.pipe(
                    Effect.catchCause(() =>
                      Effect.logWarning("Failed to restore unsent context handoffs", {
                        runId: run.id,
                      }),
                    ),
                  )
                : Effect.void,
            ),
          );
          // After provider acceptance, failed delivery bookkeeping must not stop live ingestion.
          const nativeThreadId = runningProviderThread.nativeThreadRef?.nativeId;
          yield* Effect.forEach(
            [
              delivery.delivered,
              ...(sideChatContext === "" || sideChatHandoff === undefined || nativeThreadId == null
                ? []
                : [
                    persistHandoff({
                      ...sideChatHandoff,
                      toProviderThreadId: runningProviderThread.id,
                      delivery: { nativeThreadId, status: "inline" as const, itemIds: [] },
                    }),
                  ]),
            ],
            (record) =>
              record.pipe(
                Effect.catchCause(() =>
                  Effect.logWarning("Failed to record accepted context handoff delivery", {
                    runId: run.id,
                    deliveryStatus: "pending",
                  }),
                ),
              ),
            { discard: true },
          );
        },
        Effect.scoped,
        Effect.mapError((cause) =>
          isProviderAdapterTurnStartError(cause)
            ? cause
            : new ProviderAdapter.ProviderAdapterTurnStartError({
                driver: session.driver,
                threadId: projection.thread.id,
                providerThreadId: providerThread.id,
                runId: run.id,
                cause,
              }),
        ),
      );
      const startWithConfiguredEffort = Effect.fn(
        "ProviderTurnStartService.startWithConfiguredEffort",
      )(
        (
          turnInput: Parameters<typeof session.startTurn>[0],
          compact: boolean = false,
          unsent: Effect.Effect<
            void,
            Effect.Error<ReturnType<typeof persistHandoff>> | ProviderAdapter.ProviderAdapterV2Error
          > = Effect.void,
        ) =>
          Effect.gen(function* () {
            let dispatchInput = turnInput;
            if (
              session.driver === "codex" &&
              !turnInput.modelSelection.options?.some((option) => option.id === "reasoningEffort")
            ) {
              const settingsResult = yield* currentSettings.getSettings.pipe(Effect.option);
              if (Option.isSome(settingsResult)) {
                const project = yield* projects
                  .getById(projection.thread.projectId)
                  .pipe(Effect.catch(() => Effect.succeedNone));
                const settings = resolveProjectSettings(
                  settingsResult.value,
                  projection.thread.projectId,
                  Option.getOrUndefined(project),
                ).settings;
                const defaultSelection = settings.defaultModelSelection ?? undefined;
                const selected = Option.isSome(instanceRegistry)
                  ? yield* instanceRegistry.value.getInstance(turnInput.modelSelection.instanceId)
                  : undefined;
                const snapshot =
                  selected === undefined ? undefined : yield* selected.snapshot.getSnapshot;
                const defaultInstance =
                  defaultSelection === undefined || Option.isNone(instanceRegistry)
                    ? undefined
                    : yield* instanceRegistry.value.getInstance(defaultSelection.instanceId);
                const canonical = normalizeModelSlug(
                  codexModelFamily(turnInput.modelSelection.model),
                  session.driver,
                );
                const model =
                  snapshot?.models.find(
                    (candidate) => candidate.slug === turnInput.modelSelection.model,
                  ) ??
                  snapshot?.models.find((candidate) =>
                    [candidate.slug, ...(candidate.aliases ?? [])].some(
                      (slug) =>
                        normalizeModelSlug(codexModelFamily(slug), session.driver) === canonical,
                    ),
                  );
                const effort = getConfiguredReasoningEffort({
                  modelSelection: turnInput.modelSelection,
                  driverKind: session.driver,
                  capabilities: model?.capabilities ?? undefined,
                  defaultModelSelection: defaultSelection,
                  defaultDriverKind:
                    defaultSelection === undefined
                      ? undefined
                      : (settings.providerInstances[defaultSelection.instanceId]?.driver ??
                        defaultInstance?.driverKind),
                });
                if (effort !== undefined)
                  dispatchInput = { ...turnInput, configuredReasoningEffort: effort };
              }
            }
            if (!(yield* isCurrentAttemptInStatus("running"))) return yield* unsent;
            yield* assertAdmission(runningProviderThread);
            // The adapter confirms the captured continuation session at its native boundary.
            yield* (
              compact ? session.compactThread!(dispatchInput) : session.startTurn(dispatchInput)
            ).pipe(Effect.provideService(RequiredNativeSession, requiredNativeSession));
          }),
        Effect.mapError((cause) =>
          isProviderAdapterTurnStartError(cause)
            ? cause
            : new ProviderAdapter.ProviderAdapterTurnStartError({
                driver: session.driver,
                threadId: projection.thread.id,
                providerThreadId: runningProviderThread.id,
                runId: run.id,
                cause,
              }),
        ),
      );
      const configuredSession = {
        ...session,
        get providerSession() {
          return session.providerSession;
        },
        startTurn: (value: Parameters<typeof session.startTurn>[0]) =>
          startWithConfiguredEffort(value),
        ...(session.compactThread === undefined
          ? {}
          : {
              compactThread: (value: Parameters<typeof session.startTurn>[0]) =>
                startWithConfiguredEffort(value, true),
            }),
      };
      const admittedSession = StartAdmission.withStartAdmission(
        configuredSession,
        assertAdmission(runningProviderThread),
        goalContinuity.context(
          continuityRecoveryCapability(
            session.driver,
            runningProviderThread.nativeThreadRef?.strength === "strong" &&
              runningProviderThread.runtimeIdentity?.evidenceRevision !== undefined,
          ),
        ),
        capturedAdmission.nativeCreationGuard,
      );
      const deliverySession =
        effectiveHandoffs.length === 0 &&
        retryHandoff.length === 0 &&
        sideChatHandoff === undefined &&
        restartNote === "" &&
        !noteContinuation
          ? admittedSession
          : StartAdmission.withStartAdmission(
              makeDeliverySession(admittedSession, startWithHandoffs),
              assertAdmission(runningProviderThread),
              goalContinuity.context(
                continuityRecoveryCapability(
                  session.driver,
                  runningProviderThread.nativeThreadRef?.strength === "strong" &&
                    runningProviderThread.runtimeIdentity?.evidenceRevision !== undefined,
                ),
              ),
              capturedAdmission.nativeCreationGuard,
            );
      return runExecution.startRootRun({
        goalContinuityContext: goalContinuity.context(
          continuityRecoveryCapability(
            session.driver,
            runningProviderThread.nativeThreadRef?.strength === "strong" &&
              runningProviderThread.runtimeIdentity?.evidenceRevision !== undefined,
          ),
        ),
        checkContinuity: () =>
          assertAdmission(runningProviderThread).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterTurnStartError({
                  driver: session.driver,
                  threadId: projection.thread.id,
                  providerThreadId: runningProviderThread.id,
                  runId,
                  cause,
                }),
            ),
          ),
        ...(capturedAdmission.nativeCreationGuard === undefined
          ? {}
          : {
              nativeCreationGuard: capturedAdmission.nativeCreationGuard,
            }),
        commandId: CommandId.make(`command:effect:provider-turn.start:${run.id}`),
        appThread: projection.thread,
        providerSessionId,
        session: deliverySession,
        run: runningRun,
        rootNode: runningRootNode,
        checkpointScope,
        providerThread: runningProviderThread,
        attempt: runningAttempt,
        attemptId: attempt.id,
        baseline,
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
        resume: manualResume && unsentResumeMessage === undefined,
        nativeThreadHasTurns:
          nativeInputRunIds.size > 0 ||
          (legacyInputRunIds.size > 0 &&
            sameNativeThread &&
            !acceptedAttempts.some((source) => source.nativeThreadId !== undefined)),
        shouldStartProviderTurn: runControls.shouldStartProviderTurn,
        shouldFinalizeRun: runControls.shouldFinalizeRun,
        hasUnpairedRunInterruptRequest: runControls.hasUnpairedRunInterruptRequest,
        loadRunInterruptReason: runControls.loadRunInterruptReason,
        hasWorkspaceMoved: runControls.hasWorkspaceMoved,
        loadUnsettledWork: runControls.loadUnsettledWork,
        message: {
          messageId: message.id,
          text: userText,
          attachments: providerMessage.attachments,
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
      });
    });

    const runStart = Effect.fn("ProviderTurnStartService.start")(
      function* (input: {
        readonly threadId: ThreadId;
        readonly runId: RunId;
        readonly willRetry?: boolean;
        readonly nativeCreationGuard?: NativeProvider.NativeProviderExecutionGuard;
      }) {
        let startingRun: OrchestrationV2Run | undefined;
        const captureStartingRun = (run: OrchestrationV2Run) => {
          startingRun = run;
        };
        return yield* Effect.gen(function* () {
          // Storage cleanup removes worktrees under the same lease. Only startup
          // holds it; the provider turn runs after it is released.
          const { thread } = yield* projectionStore.getThreadRecords(input.threadId, []);
          const cwd =
            thread.worktreePath ??
            (yield* projects
              .getById(thread.projectId, { includeDeleted: true })
              .pipe(Effect.map((project) => Option.getOrUndefined(project)?.workspaceRoot)));
          // Jones: the turn duty keeps this checkout from retirement for the whole
          // turn, and turn claims let checkout recreation rebind its hold.
          yield* CheckoutRecreation.withTurnClaims(
            input.runId,
            WorkspaceDuty.withDuty(
              thread.worktreePath,
              "turn",
              input.runId,
              Effect.gen(function* () {
                const providerTurn = yield* cwd === undefined
                  ? start(input, captureStartingRun)
                  : withTurnStartReservation(
                      fileSystem,
                      path,
                      path.resolve(cwd),
                      thread.worktreePath === null
                        ? start(input, captureStartingRun)
                        : withWorkspaceLease(
                            path.resolve(thread.worktreePath),
                            start(input, captureStartingRun),
                          ),
                    );
                if (providerTurn !== undefined) {
                  // Run execution owns finalization after the running write. A later
                  // failure must not settle a replacement attempt committed by steering.
                  yield* providerTurn.pipe(
                    Effect.catchCauseIf(
                      (cause) => !Cause.hasInterruptsOnly(cause) && !terminalStartFailure(cause),
                      (cause) =>
                        Effect.logWarning(
                          "Run execution failed after starting",
                          { runId: input.runId },
                          cause,
                        ),
                    ),
                  );
                }
              }),
            ),
          );
        }).pipe(
          Effect.catchCauseIf(
            (cause) => !Cause.hasInterruptsOnly(cause),
            (cause) => {
              const failure = Cause.squash(cause);
              const owner =
                startingRun?.activeAttemptId == null || startingRun.rootNodeId == null
                  ? {}
                  : {
                      attemptId: startingRun.activeAttemptId,
                      rootNodeId: startingRun.rootNodeId,
                    };
              // Jones: a start error keeps its own typed cause (for example a
              // checkout RecreationHold), as the receiving owner did; only the
              // captured attempt ownership is added.
              if (isProviderTurnStartError(failure))
                return Effect.fail(
                  failure.attemptId !== undefined
                    ? failure
                    : new ProviderTurnStartError({
                        runId: failure.runId,
                        ...(failure.cause === undefined ? {} : { cause: failure.cause }),
                        ...owner,
                      }),
                );
              return Effect.fail(
                new ProviderTurnStartError({ runId: input.runId, cause: failure, ...owner }),
              );
            },
          ),
        );
      },
      (effect, input) =>
        effect.pipe(
          Effect.mapError((cause) =>
            isProviderTurnStartError(cause)
              ? cause
              : new ProviderTurnStartError({ runId: input.runId, cause }),
          ),
        ),
    );

    const captureStartFailure = Effect.fn("ProviderTurnStartService.captureStartFailure")(
      function* (input: { readonly threadId: ThreadId; readonly runId: RunId }) {
        const projection = yield* projectionStore.getTurnStartContext(input.threadId, input.runId);
        const run = projection.runs.find((row) => row.id === input.runId);
        if (run !== undefined && run.status !== "starting") return (_cause: unknown) => Effect.void;
        const attempt = projection.attempts.find((row) => row.id === run?.activeAttemptId);
        const rootNode = projection.nodes.find((row) => row.id === run?.rootNodeId);
        if (
          run === undefined ||
          attempt === undefined ||
          rootNode === undefined ||
          run.providerThreadId === null
        )
          return yield* new ProviderTurnStartError({
            runId: input.runId,
            cause: "Prepared run failure ownership is unavailable.",
          });
        return (cause: unknown) =>
          Effect.gen(function* () {
            yield* settleRunBeforeStart({
              run,
              attempt,
              rootNode,
              turnItems: projection.turnItems,
              signal: "provider-start-execution-failure",
              status: "failed",
              now: yield* DateTime.now,
              providerInstanceId: run.providerInstanceId,
              itemProviderThreadId: run.providerThreadId,
              item: {
                type: "error",
                title: "Provider turn failed to start",
                failure: makeProviderFailure({
                  cause,
                  class: "provider_error",
                  message: "Provider start could not retain its original execution authority.",
                }),
              },
            });
          }).pipe(
            Effect.mapError((cause) => new ProviderTurnStartError({ runId: input.runId, cause })),
          );
      },
      (effect, input) =>
        effect.pipe(
          Effect.mapError((cause) =>
            isProviderTurnStartError(cause)
              ? cause
              : new ProviderTurnStartError({ runId: input.runId, cause }),
          ),
        ),
    );

    // Jones: with workspace lifecycle configured, the start runs as one issued
    // native operation that rechecks run ownership at each lifecycle boundary.
    const attestStart = Effect.fn("ProviderTurnStartService.attestWorkspaceStart")(function* <
      A,
      E,
      R,
    >(
      input: {
        readonly threadId: ThreadId;
        readonly runId: RunId;
        readonly guard?: NativeProvider.NativeProviderExecutionGuard;
      },
      effect: Effect.Effect<A, E, R>,
    ) {
      if (Option.isNone(yield* Effect.serviceOption(WorkspaceLifecycleService)))
        return yield* effect;
      const admitted = yield* projectionStore.getTurnStartContext(input.threadId, input.runId);
      const originalAttempt = admitted.runs.find((run) => run.id === input.runId)?.activeAttemptId;
      return yield* withOperation(
        {
          id: `run:${input.runId}`,
          kind: input.guard ? "native-start" : "provider-start",
          threadId: input.threadId,
          boundaries: ["start", "resume", "recreate"],
          sqlOnly: input.guard === undefined,
          check: Effect.gen(function* () {
            if (input.guard)
              yield* NativeProvider.assertNativeProviderStartOperation(
                input.guard,
                input.threadId,
                input.runId,
              );
            const current = yield* projectionStore.getTurnStartContext(input.threadId, input.runId);
            const active = current.runs.find((run) => run.id === input.runId);
            if (
              !active ||
              !["starting", "running"].includes(active.status) ||
              !active.activeAttemptId ||
              (originalAttempt !== undefined && originalAttempt !== active.activeAttemptId) ||
              !current.attempts.some(
                (attempt) => attempt.id === active.activeAttemptId && attempt.runId === active.id,
              ) ||
              !current.nodes.some((node) => node.id === active.rootNodeId)
            )
              return yield* new ProviderTurnStartError({
                runId: input.runId,
                cause: "Native run ownership changed before workspace use.",
              });
          }),
        },
        effect,
      );
    });
    const attestedStart = <A>(
      input: {
        readonly threadId: ThreadId;
        readonly runId: RunId;
        readonly guard?: NativeProvider.NativeProviderExecutionGuard;
      },
      effect: Effect.Effect<A, ProviderTurnStartError>,
    ) =>
      attestStart(
        input,
        WorkspaceLifecycle.inheritOriginalTransition(
          `turn-start:${input.runId}`,
          { runId: input.runId, threadId: input.threadId },
          effect,
        ),
      ).pipe(
        Effect.mapError((cause) =>
          isProviderTurnStartError(cause)
            ? cause
            : new ProviderTurnStartError({ runId: input.runId, cause }),
        ),
      );

    return ProviderTurnStartServiceV2.of({
      captureStartFailure,
      start: Effect.fn("ProviderTurnStartService.attestedStart")(function* (input) {
        return yield* attestedStart(input, runStart(input));
      }),
      startNative: Effect.fn("ProviderTurnStartService.startNative")(
        function* (input) {
          yield* NativeProvider.assertNativeProviderStartOperation(
            input.guard,
            input.threadId,
            input.runId,
          );
          yield* runStart({
            threadId: input.threadId,
            runId: input.runId,
            nativeCreationGuard: input.guard,
          });
          const evidence = NativeProvider.readNativeProviderAcknowledgement(input.guard);
          if (evidence === undefined)
            return yield* new ProviderTurnStartError({
              runId: input.runId,
              cause: "Native start did not receive its direct whole-operation acknowledgement.",
            });
          return evidence;
        },
        (effect, input) =>
          attestedStart(
            input,
            effect.pipe(
              Effect.mapError((cause) =>
                isProviderTurnStartError(cause)
                  ? cause
                  : new ProviderTurnStartError({ runId: input.runId, cause }),
              ),
            ),
          ),
      ),
      failStartingRun: Effect.fn("ProviderTurnStartService.failStartingRun")(
        function* ({ threadId, runId, attemptId, rootNodeId }) {
          const projection = yield* projectionStore.getTurnStartContext(threadId, runId);
          const run = projection.runs.find((candidate) => candidate.id === runId);
          if (
            run === undefined ||
            run.status !== "starting" ||
            run.activeAttemptId !== attemptId ||
            run.rootNodeId !== rootNodeId
          )
            return;
          const rootNode = projection.nodes.find((candidate) => candidate.id === run.rootNodeId);
          const attempt = projection.attempts.find(
            (candidate) => candidate.id === run.activeAttemptId,
          );
          if (rootNode === undefined || attempt === undefined) {
            return yield* new ProviderTurnStartRunStateMissingError({ runId });
          }
          yield* settleRunBeforeStart({
            run,
            rootNode,
            attempt,
            turnItems: projection.turnItems,
            signal: "provider-turn-start-failure",
            status: "failed",
            now: yield* DateTime.now,
            providerInstanceId: run.providerInstanceId,
            itemProviderThreadId: run.providerThreadId,
            item: {
              type: "error",
              title: "Turn failed to start",
              failure: makeProviderFailure({ message: START_GAVE_UP_MESSAGE }),
            },
          });
        },
        (effect, { runId }) =>
          effect.pipe(
            Effect.mapError((cause) =>
              isProviderTurnStartRunStateMissingError(cause)
                ? cause
                : new ProviderTurnStartError({ runId, cause }),
            ),
          ),
      ),
    });
  }),
);
