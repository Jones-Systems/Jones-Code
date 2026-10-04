import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import {
  CommandId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type ProviderThreadId,
  type OrchestrationV2RestartCancelledBackgroundWork,
  type OrchestrationV2ThreadProjection,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlError from "effect/unstable/sql/SqlError";
import * as NodeCrypto from "node:crypto";

import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ServerSettings from "../serverSettings.ts";
import { PersistenceSqlError } from "../persistence/Errors.ts";
import { restartContinuationRun } from "./RestartContinuation.ts";
import {
  cancelledRosterTaskWork,
  cancelledTurnItemWork,
  mergeRestartCancelledBackgroundWork,
} from "./RestartBackgroundNote.ts";

export class ProviderRuntimeRecoveryError extends Schema.TaggedError<ProviderRuntimeRecoveryError>()(
  "ProviderRuntimeRecoveryError",
  {
    operation: Schema.Literals(["read-projections", "reconcile", "drain-outbox"]),
    threadId: Schema.optional(ThreadId),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Provider runtime recovery failed during ${this.operation}.`;
  }
}

export interface ProviderRuntimeRecoverySummary {
  readonly terminalizedRuns: number;
  readonly stoppedSessions: number;
  readonly closedRequests: number;
  readonly retiredEffects: number;
  readonly requeuedEffects: number;
  readonly failedThreadIds: ReadonlyArray<ThreadId>;
}

export interface ProviderRuntimeReconciliationSummary {
  readonly terminalizedRuns: number;
  readonly stoppedSessions: number;
  readonly closedRequests: number;
  readonly retiredEffects: number;
  readonly requeuedEffects: number;
  readonly failedThreadIds: ReadonlyArray<ThreadId>;
}
export interface ProviderStartupRecoveryStage {
  readonly continuationMarkers: ReadonlyArray<EventSink.RestartContinuationMarkerV2>;
}
export interface ProviderStartupRecoveryResult extends ProviderRuntimeReconciliationSummary {
  readonly releasedContinuationMarkerIds: ReadonlyArray<string>;
  readonly heldContinuationMarkers: ReadonlyArray<{
    readonly marker: EventSink.RestartContinuationMarkerV2;
    readonly reason: string;
  }>;
}

export class ProviderRuntimeRecoveryService extends Context.Service<
  ProviderRuntimeRecoveryService,
  {
    readonly reconcile: (
      trigger: "startup" | "shutdown",
    ) => Effect.Effect<ProviderRuntimeReconciliationSummary, ProviderRuntimeRecoveryError>;
    readonly prepareForShutdown: Effect.Effect<void, ProviderRuntimeRecoveryError>;
    readonly stageStartupRecovery: Effect.Effect<
      ProviderStartupRecoveryStage,
      ProviderRuntimeRecoveryError
    >;
    readonly reconcileAfterStartupTrial: (
      stage: ProviderStartupRecoveryStage,
    ) => Effect.Effect<ProviderStartupRecoveryResult, ProviderRuntimeRecoveryError>;
    readonly prepareForServerUpdate: (input: {
      readonly respectProjectPreference: boolean;
    }) => Effect.Effect<
      ReadonlyArray<EventSink.RestartContinuationMarkerV2>,
      ProviderRuntimeRecoveryError
    >;
    readonly clearServerUpdatePreparation: (
      markers: ReadonlyArray<EventSink.RestartContinuationMarkerV2>,
    ) => Effect.Effect<void, ProviderRuntimeRecoveryError>;
    readonly recover: Effect.Effect<ProviderRuntimeRecoverySummary, ProviderRuntimeRecoveryError>;
  }
>()("t3/orchestration-v2/ProviderRuntimeRecoveryService") {}

function isTransientProjectionReadFailure(error: unknown): boolean {
  if (!Schema.is(ProjectionStore.ProjectionStoreReadError)(error)) return false;
  let cause = error.cause;
  for (let depth = 0; depth < 4; depth++) {
    if (SqlError.isSqlError(cause)) return Schema.is(SqlError.LockTimeoutError)(cause.reason);
    if (!Schema.is(PersistenceSqlError)(cause)) return false;
    cause = cause.cause;
  }
  return false;
}

function nonterminalRuns(projection: ProjectionStore.ProjectionRuntimeRecoveryState) {
  return projection.runs.filter((run) => {
    const status: string = run.status;
    return (
      status === "preparing" ||
      run.status === "starting" ||
      run.status === "running" ||
      run.status === "waiting"
    );
  });
}

function isBackgroundCapableTurnItemType(type: string): boolean {
  return type === "command_execution" || type === "dynamic_tool" || type === "subagent";
}

function isNonterminalTurnItemStatus(status: string): boolean {
  return status === "pending" || status === "running" || status === "waiting";
}

function isNonterminalSubagentStatus(status: string): boolean {
  return status === "pending" || status === "running" || status === "waiting";
}

function isNonterminalNodeStatus(status: string): boolean {
  return status === "pending" || status === "running" || status === "waiting";
}

function providerThreadHasPendingBackgroundTasks(
  providerThread: OrchestrationV2ThreadProjection["providerThreads"][number],
): boolean {
  return (providerThread.pendingBackgroundTasks?.length ?? 0) > 0;
}

/**
 * Resolve providerInstanceId for a stale background-capable turn item whose
 * run is missing/null (or not found). Prefer an existing run, then a subagent
 * item's own instance id, then the item's provider thread, then a last-resort
 * first provider thread, then the thread's selected provider.
 */
function resolveStaleBackgroundItemProviderInstanceId(
  item: OrchestrationV2ThreadProjection["turnItems"][number],
  projection: ProjectionStore.ProjectionRuntimeRecoveryState,
): OrchestrationV2ThreadProjection["thread"]["providerInstanceId"] {
  if (item.runId !== null) {
    const run = projection.runs.find((candidate) => candidate.id === item.runId);
    if (run !== undefined) {
      return run.providerInstanceId;
    }
  }
  if (item.type === "subagent") {
    return item.providerInstanceId;
  }
  if (item.providerThreadId !== null && item.providerThreadId !== undefined) {
    const providerThread = projection.providerThreads.find(
      (candidate) => candidate.id === item.providerThreadId,
    );
    if (providerThread !== undefined) {
      return providerThread.providerInstanceId;
    }
  }
  return projection.providerThreads[0]?.providerInstanceId ?? projection.thread.providerInstanceId;
}

/**
 * Provider threads with background work reconciliation would cancel and
 * record for their next turn.
 */
function providerThreadsWithOpenBackgroundWork(
  projection: ProjectionStore.ProjectionRuntimeRecoveryState,
): ReadonlySet<ProviderThreadId> {
  const ids = new Set<ProviderThreadId>();
  for (const item of projection.turnItems ?? []) {
    if (!isBackgroundCapableTurnItemType(item.type) || !isNonterminalTurnItemStatus(item.status))
      continue;
    const providerThreadId =
      item.providerThreadId ??
      projection.runs.find((run) => run.id === item.runId)?.providerThreadId;
    if (providerThreadId != null) ids.add(providerThreadId);
  }
  for (const thread of projection.providerThreads ?? []) {
    if (thread.ownerNodeId === null && providerThreadHasPendingBackgroundTasks(thread))
      ids.add(thread.id);
  }
  return ids;
}

/**
 * A provider thread's latest started run: the last turn that provider saw.
 * Restart recovery records the thread's cancelled background work on it, and
 * the next run on the same provider thread delivers it with its input.
 */
function latestStartedRun(
  projection: ProjectionStore.ProjectionRuntimeRecoveryState,
  providerThreadId: ProviderThreadId,
) {
  return projection.runs.reduce<OrchestrationV2ThreadProjection["runs"][number] | undefined>(
    (latest, run) =>
      run.providerThreadId === providerThreadId &&
      run.status !== "queued" &&
      run.status !== "rolled_back" &&
      (latest === undefined || run.ordinal > latest.ordinal)
        ? run
        : latest,
    undefined,
  );
}

export const make = Effect.gen(function* () {
  const settings = yield* ServerSettings.ServerSettingsService;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const eventSink = yield* EventSink.EventSinkV2;
  const ids = yield* IdAllocator.IdAllocatorV2;
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const providerSessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
  const reconcileProjection = Effect.fn("ProviderRuntimeRecoveryService.reconcileProjection")(
    function* (
      projection: ProjectionStore.ProjectionRuntimeRecoveryState,
      trigger: "startup" | "shutdown",
      continueAfterRestart: boolean,
    ) {
      const held = yield* outbox.listHeldByThreadId(projection.thread.id).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderRuntimeRecoveryError({
              operation: "reconcile",
              threadId: projection.thread.id,
              cause,
            }),
        ),
      );
      if (held.length > 0) {
        yield* Effect.logWarning("orchestration-v2.runtime-recovery.unknown-operation-held", {
          threadId: projection.thread.id,
          operationIds: held.map((operation) => operation.operationId),
        });
        return { terminalizedRuns: 0, stoppedSessions: 0, closedRequests: 0, retiredEffects: 0 };
      }
      const now = yield* DateTime.now;
      const runs = [] as Array<OrchestrationV2ThreadProjection["runs"][number]>;
      for (const run of nonterminalRuns(projection)) {
        if (run.status === "waiting") {
          const checkpointEffects = yield* outbox
            .listByCommandId(CommandId.make(`command:effect:checkpoint.capture:${run.id}`))
            .pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderRuntimeRecoveryError({
                    operation: "reconcile",
                    threadId: projection.thread.id,
                    cause,
                  }),
              ),
            );
          const hasReplayableCheckpoint = checkpointEffects.some(
            (effect) =>
              effect.request.type === "checkpoint.capture" &&
              effect.request.runId === run.id &&
              (effect.status === "pending" || effect.status === "running"),
          );
          if (hasReplayableCheckpoint) continue;
        }
        runs.push(run);
      }
      const messageRequestNodeIds = new Set(
        projection.runtimeRequests
          .filter(
            (request) =>
              request.status === "pending" && request.responseCapability.type === "message",
          )
          .map((request) => request.nodeId),
      );
      const requests = projection.runtimeRequests.filter(
        (request) => request.status === "pending" && request.responseCapability.type !== "message",
      );
      const detail = `Cancelled because the server ${trigger === "startup" ? "restarted" : "shut down"} before the provider work completed.`;
      const commandId = CommandId.make(
        `command:runtime-reconcile:${trigger}:${projection.thread.id}:${DateTime.formatIso(now)}`,
      );
      const allocateEventId = () =>
        ids.allocate.event({ threadId: projection.thread.id, commandId }).pipe(
          Effect.mapError(
            (cause) =>
              new ProviderRuntimeRecoveryError({
                operation: "reconcile",
                threadId: projection.thread.id,
                cause,
              }),
          ),
        );
      const events: Array<OrchestrationV2DomainEvent> = [];
      // Background work that outlived its settled turn. The provider transcript
      // cannot record its death, so the next provider turn is told instead.
      // Shutdown records it too: a graceful restart cancels it there first.
      // Keyed by the provider thread that lost the work: only its turns are told.
      const cancelledBackgroundWork = new Map<
        ProviderThreadId,
        Array<OrchestrationV2RestartCancelledBackgroundWork>
      >();
      const cancelledBackgroundNativeIds = new Set<string>();
      const recordCancelledBackgroundWork = (
        providerThreadId: ProviderThreadId | null | undefined,
        work: OrchestrationV2RestartCancelledBackgroundWork,
      ) => {
        if (providerThreadId == null) return;
        const existing = cancelledBackgroundWork.get(providerThreadId);
        if (existing === undefined) cancelledBackgroundWork.set(providerThreadId, [work]);
        else existing.push(work);
      };
      const recordCancelledBackgroundItem = (
        item: OrchestrationV2ThreadProjection["turnItems"][number],
      ) => {
        if (!isBackgroundCapableTurnItemType(item.type)) return;
        const work = cancelledTurnItemWork(item);
        if (work === undefined) return;
        recordCancelledBackgroundWork(
          item.providerThreadId ??
            projection.runs.find((run) => run.id === item.runId)?.providerThreadId,
          work,
        );
        if (item.nativeItemRef?.nativeId != null) {
          cancelledBackgroundNativeIds.add(item.nativeItemRef.nativeId);
        }
      };
      // Queued runs have not started provider work. Preserve their execution
      // identities and order, but require explicit consent before draining them.
      for (const run of projection.runs) {
        if (run.status !== "queued" || run.queueHeld === true) continue;
        events.push({
          id: yield* allocateEventId(),
          type: "run.updated",
          threadId: projection.thread.id,
          runId: run.id,
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: { ...run, queueHeld: true },
        });
      }
      for (const request of requests) {
        events.push({
          id: yield* allocateEventId(),
          type: "runtime-request.updated",
          threadId: projection.thread.id,
          nodeId: request.nodeId,
          occurredAt: now,
          payload: {
            ...request,
            status: trigger === "startup" ? "expired" : "cancelled",
            responseCapability: {
              type: "not_resumable",
              reason: `The server ${trigger === "startup" ? "restarted" : "shut down"} before this runtime request was resolved.`,
            },
            resolvedAt: now,
          },
        });
      }
      for (const run of runs) {
        events.push({
          id: yield* allocateEventId(),
          type: "run.updated",
          threadId: projection.thread.id,
          runId: run.id,
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: { ...run, status: "cancelled", queuePosition: null, completedAt: now },
        });
        for (const attempt of projection.attempts.filter(
          (candidate) =>
            candidate.runId === run.id &&
            (candidate.status === "pending" || candidate.status === "running"),
        )) {
          events.push({
            id: yield* allocateEventId(),
            type: "run-attempt.updated",
            threadId: projection.thread.id,
            runId: run.id,
            nodeId: attempt.rootNodeId,
            providerInstanceId: run.providerInstanceId,
            occurredAt: now,
            payload: { ...attempt, status: "cancelled", completedAt: now },
          });
        }
        for (const node of projection.nodes.filter(
          (candidate) =>
            candidate.runId === run.id &&
            !messageRequestNodeIds.has(candidate.id) &&
            (candidate.status === "pending" ||
              candidate.status === "running" ||
              candidate.status === "waiting"),
        )) {
          events.push({
            id: yield* allocateEventId(),
            type: "node.updated",
            threadId: projection.thread.id,
            runId: run.id,
            nodeId: node.id,
            providerInstanceId: run.providerInstanceId,
            occurredAt: now,
            payload: { ...node, status: "cancelled", completedAt: now },
          });
        }
        for (const subagent of projection.subagents.filter(
          (candidate) =>
            candidate.runId === run.id &&
            (candidate.status === "pending" ||
              candidate.status === "running" ||
              candidate.status === "waiting"),
        )) {
          events.push({
            id: yield* allocateEventId(),
            type: "subagent.updated",
            threadId: projection.thread.id,
            runId: run.id,
            nodeId: subagent.id,
            driver: subagent.driver,
            providerInstanceId: subagent.providerInstanceId,
            occurredAt: now,
            payload: { ...subagent, status: "cancelled", completedAt: now, updatedAt: now },
          });
        }
        for (const providerTurn of projection.providerTurns.filter(
          (candidate) =>
            candidate.runAttemptId !== null &&
            projection.attempts.some(
              (attempt) => attempt.id === candidate.runAttemptId && attempt.runId === run.id,
            ) &&
            (candidate.status === "pending" || candidate.status === "running"),
        )) {
          events.push({
            id: yield* allocateEventId(),
            type: "provider-turn.updated",
            threadId: projection.thread.id,
            runId: run.id,
            nodeId: providerTurn.nodeId,
            providerInstanceId: run.providerInstanceId,
            occurredAt: now,
            payload: { ...providerTurn, status: "cancelled", completedAt: now },
          });
        }
        for (const message of projection.messages.filter(
          (candidate) => candidate.runId === run.id && candidate.streaming,
        )) {
          events.push({
            id: yield* allocateEventId(),
            type: "message.updated",
            threadId: projection.thread.id,
            runId: run.id,
            ...(message.nodeId === null ? {} : { nodeId: message.nodeId }),
            providerInstanceId: run.providerInstanceId,
            occurredAt: now,
            payload: { ...message, streaming: false, updatedAt: now },
          });
        }
        for (const item of projection.turnItems.filter(
          (candidate) =>
            candidate.runId === run.id &&
            (candidate.nodeId === null || !messageRequestNodeIds.has(candidate.nodeId)) &&
            (candidate.status === "pending" ||
              candidate.status === "running" ||
              candidate.status === "waiting"),
        )) {
          // A waiting run's provider turn already settled; its open items are
          // background work. A running run's items die with its turn.
          if (run.status === "waiting") recordCancelledBackgroundItem(item);
          events.push({
            id: yield* allocateEventId(),
            type: "turn-item.updated",
            threadId: projection.thread.id,
            runId: run.id,
            ...(item.nodeId === null ? {} : { nodeId: item.nodeId }),
            providerInstanceId: run.providerInstanceId,
            occurredAt: now,
            payload: { ...item, status: "cancelled", completedAt: now, updatedAt: now },
          });
        }
      }
      // Process loss also orphans background-capable turn items on already-
      // settled runs (e.g. post-settle Waiting work). Skip items already
      // cancelled above for recovered nonterminal runs to avoid duplicate
      // cancellation events.
      const recoveredNonterminalRunIds = new Set(runs.map((run) => run.id));
      const cancelledStaleNodeIds = new Set<string>();
      for (const item of projection.turnItems ?? []) {
        if (item.runId !== null && recoveredNonterminalRunIds.has(item.runId)) {
          continue;
        }
        if (!isBackgroundCapableTurnItemType(item.type)) {
          continue;
        }
        if (!isNonterminalTurnItemStatus(item.status)) {
          continue;
        }
        const providerInstanceId = resolveStaleBackgroundItemProviderInstanceId(item, projection);
        recordCancelledBackgroundItem(item);
        events.push({
          id: yield* allocateEventId(),
          type: "turn-item.updated",
          threadId: projection.thread.id,
          ...(item.runId === null ? {} : { runId: item.runId }),
          ...(item.nodeId === null || item.nodeId === undefined ? {} : { nodeId: item.nodeId }),
          providerInstanceId,
          occurredAt: now,
          payload: { ...item, status: "cancelled", completedAt: now, updatedAt: now },
        });
        if (item.nodeId !== null && item.nodeId !== undefined) {
          const staleItemNode = projection.nodes.find(
            (candidate) =>
              candidate.id === item.nodeId && isNonterminalNodeStatus(candidate.status),
          );
          if (staleItemNode !== undefined && !cancelledStaleNodeIds.has(staleItemNode.id)) {
            cancelledStaleNodeIds.add(staleItemNode.id);
            events.push({
              id: yield* allocateEventId(),
              type: "node.updated",
              threadId: projection.thread.id,
              ...(item.runId === null ? {} : { runId: item.runId }),
              nodeId: staleItemNode.id,
              providerInstanceId,
              occurredAt: now,
              payload: { ...staleItemNode, status: "cancelled", completedAt: now },
            });
          }
        }
        if (item.type !== "subagent") {
          continue;
        }
        // Cancelling only the turn item would leave the linked subagent entity
        // non-terminal forever, since the dead provider process can no longer
        // emit its terminal event. Match the exact linked id so a subagent
        // that already finished is never overwritten.
        const staleSubagent = projection.subagents.find(
          (candidate) =>
            candidate.id === item.subagentId && isNonterminalSubagentStatus(candidate.status),
        );
        if (staleSubagent !== undefined) {
          events.push({
            id: yield* allocateEventId(),
            type: "subagent.updated",
            threadId: projection.thread.id,
            ...(item.runId === null ? {} : { runId: item.runId }),
            nodeId: staleSubagent.id,
            driver: staleSubagent.driver,
            providerInstanceId: staleSubagent.providerInstanceId,
            occurredAt: now,
            payload: { ...staleSubagent, status: "cancelled", completedAt: now, updatedAt: now },
          });
        }
        const staleSubagentNode = projection.nodes.find(
          (candidate) =>
            candidate.id === item.subagentId && isNonterminalNodeStatus(candidate.status),
        );
        if (staleSubagentNode !== undefined && !cancelledStaleNodeIds.has(staleSubagentNode.id)) {
          cancelledStaleNodeIds.add(staleSubagentNode.id);
          events.push({
            id: yield* allocateEventId(),
            type: "node.updated",
            threadId: projection.thread.id,
            ...(item.runId === null ? {} : { runId: item.runId }),
            nodeId: staleSubagentNode.id,
            providerInstanceId,
            occurredAt: now,
            payload: { ...staleSubagentNode, status: "cancelled", completedAt: now },
          });
        }
      }
      // A provider-native subagent thread has no runs: its work is a runless
      // root turn, plus items under it (Claude's live progress item), that
      // only the dead provider process could settle. Left running, the child
      // would show as working forever.
      const cancelledStaleItemIds = new Set(
        events.flatMap((event) => (event.type === "turn-item.updated" ? [event.payload.id] : [])),
      );
      for (const node of projection.nodes) {
        if (
          node.kind !== "root_turn" ||
          node.runId !== null ||
          !isNonterminalNodeStatus(node.status) ||
          cancelledStaleNodeIds.has(node.id)
        ) {
          continue;
        }
        cancelledStaleNodeIds.add(node.id);
        events.push({
          id: yield* allocateEventId(),
          type: "node.updated",
          threadId: projection.thread.id,
          nodeId: node.id,
          providerInstanceId: projection.thread.providerInstanceId,
          occurredAt: now,
          payload: { ...node, status: "cancelled", completedAt: now },
        });
        for (const item of projection.turnItems) {
          if (
            item.nodeId !== node.id ||
            item.runId !== null ||
            !isNonterminalTurnItemStatus(item.status) ||
            cancelledStaleItemIds.has(item.id)
          ) {
            continue;
          }
          cancelledStaleItemIds.add(item.id);
          events.push({
            id: yield* allocateEventId(),
            type: "turn-item.updated",
            threadId: projection.thread.id,
            nodeId: node.id,
            providerInstanceId: projection.thread.providerInstanceId,
            occurredAt: now,
            payload: {
              ...item,
              status: "cancelled",
              completedAt: now,
              updatedAt: now,
              ...(item.type === "reasoning" || item.type === "assistant_message"
                ? { streaming: false }
                : {}),
            },
          });
        }
      }
      // All provider processes are gone on startup/shutdown: clear any
      // persisted Waiting roster (including idle threads from settled roots)
      // and idle active threads without resurrecting active status.
      for (const providerThread of projection.providerThreads ?? []) {
        const needsIdle = providerThread.status === "active";
        const needsRosterClear = providerThreadHasPendingBackgroundTasks(providerThread);
        if (!needsIdle && !needsRosterClear) {
          continue;
        }
        if (providerThread.ownerNodeId === null) {
          for (const task of providerThread.pendingBackgroundTasks ?? []) {
            if (cancelledBackgroundNativeIds.has(task.taskId)) continue;
            cancelledBackgroundNativeIds.add(task.taskId);
            recordCancelledBackgroundWork(providerThread.id, cancelledRosterTaskWork(task));
          }
        }
        events.push({
          id: yield* allocateEventId(),
          type: "provider-thread.updated",
          threadId: projection.thread.id,
          driver: providerThread.driver,
          providerInstanceId: providerThread.providerInstanceId,
          occurredAt: now,
          payload: {
            ...providerThread,
            status: needsIdle ? "idle" : providerThread.status,
            pendingBackgroundTasks: [],
            updatedAt: now,
          },
        });
      }
      for (const session of projection.providerSessions.filter(
        (candidate) => candidate.status !== "stopped" && candidate.status !== "error",
      )) {
        events.push({
          id: yield* allocateEventId(),
          type: "provider-session.updated",
          threadId: projection.thread.id,
          driver: session.driver,
          providerInstanceId: session.providerInstanceId,
          occurredAt: now,
          payload: { ...session, status: "stopped", updatedAt: now, lastError: null },
        });
      }
      for (const [providerThreadId, work] of cancelledBackgroundWork) {
        const noteRun = latestStartedRun(projection, providerThreadId);
        if (noteRun === undefined) continue;
        // Its own event: a run snapshot read before this commit could regress
        // a lifecycle change (e.g. a checkpoint completing the run) made since.
        events.push({
          id: yield* allocateEventId(),
          type: "run.background-work-cancelled",
          threadId: projection.thread.id,
          runId: noteRun.id,
          providerInstanceId: noteRun.providerInstanceId,
          occurredAt: now,
          payload: {
            runId: noteRun.id,
            restartCancelledBackgroundWork: mergeRestartCancelledBackgroundWork(
              noteRun.restartCancelledBackgroundWork ?? [],
              work,
            ),
          },
        });
      }
      const continuationRun =
        continueAfterRestart && trigger === "startup"
          ? restartContinuationRun(projection, new Set(cancelledBackgroundWork.keys()))
          : undefined;
      const effects: Array<EffectOutbox.PendingOrchestrationEffectV2> = continuationRun
        ? [
            {
              id: `effect:restart-continuation:${continuationRun.id}`,
              commandId,
              threadId: projection.thread.id,
              request: { type: "provider-runtime.continue", sourceRunId: continuationRun.id },
            },
          ]
        : [];
      const stoppedSessions = projection.providerSessions.filter(
        (candidate) => candidate.status !== "stopped" && candidate.status !== "error",
      ).length;
      let retiredEffects: number;
      if (events.length === 0) {
        const retiredEffectIds = yield* outbox
          .cancelUnsettled({
            threadId: projection.thread.id,
            effectTypes: EffectOutbox.PROCESS_BOUND_EFFECT_TYPES,
            reason: detail,
          })
          .pipe(
            Effect.mapError(
              (cause) =>
                new ProviderRuntimeRecoveryError({
                  operation: "reconcile",
                  threadId: projection.thread.id,
                  cause: { detail, cause },
                }),
            ),
          );
        yield* outbox.signalCancellations(retiredEffectIds);
        retiredEffects = retiredEffectIds.length;
      } else {
        const result = yield* eventSink
          .commitCommand({
            commandId,
            threadId: projection.thread.id,
            commandType: "provider-runtime.reconcile",
            acceptedAt: now,
            events,
            effects,
            cancelUnsettledEffects: {
              effectTypes: EffectOutbox.PROCESS_BOUND_EFFECT_TYPES,
              reason: detail,
            },
          })
          .pipe(
            Effect.mapError(
              (cause) =>
                new ProviderRuntimeRecoveryError({
                  operation: "reconcile",
                  threadId: projection.thread.id,
                  cause,
                }),
            ),
          );
        retiredEffects = result.cancelledEffectCount;
      }
      return {
        terminalizedRuns: runs.length,
        stoppedSessions,
        closedRequests: requests.length,
        retiredEffects,
      };
    },
  );

  const reconcile = (
    trigger: "startup" | "shutdown",
    reconcileOutbox = true,
    prepareAutomaticContinuations = true,
  ) =>
    Effect.gen(function* () {
      const continueAfterRestart = yield* settings.getSettings.pipe(
        Effect.orElseSucceed(() => null),
      );
      const threadIds = yield* projections
        .getRecoveryThreadIds("runtime")
        .pipe(
          Effect.mapError(
            (cause) => new ProviderRuntimeRecoveryError({ operation: "read-projections", cause }),
          ),
        );
      let terminalizedRuns = 0;
      let stoppedSessions = 0;
      let closedRequests = 0;
      let retiredEffects = 0;
      const failedThreadIds: Array<ThreadId> = [];
      for (const threadId of threadIds) {
        const read = Effect.suspend(() => projections.getRuntimeRecoveryProjection(threadId));
        let result = yield* Effect.result(read);
        let attempts = 1;
        if (result._tag === "Failure" && isTransientProjectionReadFailure(result.failure)) {
          attempts++;
          result = yield* Effect.result(read);
        }
        if (result._tag === "Failure") {
          failedThreadIds.push(threadId);
          yield* Effect.logWarning("orchestration-v2.runtime-recovery.projection-read-held", {
            threadId,
            attempts,
          });
          continue;
        }
        const projection = result.success;
        const enabled =
          prepareAutomaticContinuations &&
          continueAfterRestart !== null &&
          resolveProjectSettings(continueAfterRestart, projection.thread.projectId).settings
            .continueThreadsAfterServerUpdate;
        const reconciled = yield* reconcileProjection(projection, trigger, enabled);
        terminalizedRuns += reconciled.terminalizedRuns;
        stoppedSessions += reconciled.stoppedSessions;
        closedRequests += reconciled.closedRequests;
        retiredEffects += reconciled.retiredEffects;
      }
      // An unreadable thread retains its effects and projection while unrelated
      // threads still complete process-loss reconciliation.
      const outboxReconciliation = yield* (
        reconcileOutbox
          ? failedThreadIds.length === 0
            ? outbox.reconcileAfterProcessLoss
            : outbox.reconcileAfterProcessLossExcluding({ excludeThreadIds: failedThreadIds })
          : Effect.succeed({ requeued: 0, cancelled: 0 })
      ).pipe(
        Effect.mapError(
          (cause) => new ProviderRuntimeRecoveryError({ operation: "drain-outbox", cause }),
        ),
      );
      return {
        terminalizedRuns,
        stoppedSessions,
        closedRequests,
        retiredEffects: retiredEffects + outboxReconciliation.cancelled,
        requeuedEffects: outboxReconciliation.requeued,
        failedThreadIds,
      } satisfies ProviderRuntimeReconciliationSummary;
    });

  const clearServerUpdatePreparation = (
    markers: ReadonlyArray<EventSink.RestartContinuationMarkerV2>,
  ) =>
    Effect.forEach(markers, (marker) => eventSink.clearRestartContinuation(marker), {
      discard: true,
    }).pipe(
      Effect.mapError(
        (cause) => new ProviderRuntimeRecoveryError({ operation: "reconcile", cause }),
      ),
    );
  const prepareForServerUpdate = (input: { readonly respectProjectPreference: boolean }) =>
    Effect.gen(function* () {
      const preferences = input.respectProjectPreference ? yield* settings.getSettings : undefined;
      const marked: Array<EventSink.RestartContinuationMarkerV2> = [];
      const allocated: Array<EventSink.RestartContinuationMarkerV2> = [];
      return yield* Effect.gen(function* () {
        const threadIds = yield* projections.getRecoveryThreadIds("runtime");
        for (const threadId of threadIds) {
          const projection = yield* projections.getRuntimeRecoveryProjection(threadId);
          const registered = yield* eventSink.readProviderRuntimeEvidence(threadId);
          const currentSource = projection.runs.reduce<OrchestrationV2Run | undefined>(
            (latest, candidate) =>
              latest === undefined || candidate.ordinal > latest.ordinal ? candidate : latest,
            undefined,
          );
          const currentSourceAttemptId = currentSource?.activeAttemptId ?? null;
          // A dormant marker binds its source attempt, so an attempt-less source cannot own one.
          if (
            registered !== null &&
            currentSource !== undefined &&
            currentSourceAttemptId !== null
          ) {
            const existing = yield* eventSink.findDormantRestartContinuation({
              threadId,
              projectId: projection.thread.projectId,
              sourceRunId: currentSource.id,
              sourceRunAttemptId: currentSourceAttemptId,
              expectedBinding: registered.binding,
              expectedEvidenceRevision: registered.evidenceRevision,
            });
            if (existing !== null) {
              marked.push(existing);
              continue;
            }
          }
          if (
            preferences !== undefined &&
            !resolveProjectSettings(preferences, projection.thread.projectId).settings
              .continueThreadsAfterServerUpdate
          )
            continue;
          const native = yield* providerSessions.observeCurrentThreadRuntime(threadId);
          if (native.status === "unknown") {
            if (
              native.reason !== "runtime_not_resident" &&
              restartContinuationRun(
                projection,
                providerThreadsWithOpenBackgroundWork(projection),
              ) !== undefined
            )
              return yield* new ProviderRuntimeRecoveryError({
                operation: "reconcile",
                threadId,
                cause: `Current native activity is unknown: ${native.reason}`,
              });
            continue;
          }
          if (native.status === "idle") continue;
          const backgroundThreads = new Set<ProviderThreadId>();
          if (native.status === "working" || native.status === "monitoring")
            backgroundThreads.add(native.binding.providerThreadId);
          const run = restartContinuationRun(projection, backgroundThreads);
          if (!run) continue;
          const evidence = yield* eventSink.readProviderRuntimeEvidence(threadId);
          if (
            evidence === null ||
            evidence.binding.runtimeGeneration !== native.binding.runtimeGeneration ||
            evidence.binding.providerThreadId !== native.binding.providerThreadId ||
            evidence.binding.providerSessionId !== native.binding.providerSessionId ||
            evidence.binding.instanceId !== native.binding.instanceId ||
            evidence.binding.nativeThreadId !== (native.binding.nativeThreadId ?? null)
          )
            return yield* new ProviderRuntimeRecoveryError({
              operation: "reconcile",
              threadId,
              cause: "The current runtime evidence changed during update preparation.",
            });
          const runAttemptId = run.activeAttemptId;
          // The store rejects a continuation source without an attempt as a changed binding.
          if (runAttemptId === null)
            return yield* new EventSink.RestartContinuationMarkerError({
              reason: "binding_changed",
            });
          let allocatedHere = false;
          const marker = yield* eventSink.prepareRestartContinuation({
            markerId: Effect.sync(() => {
              allocatedHere = true;
              return `restart-preparation:${NodeCrypto.randomUUID()}`;
            }),
            threadId,
            projectId: projection.thread.projectId,
            sourceRunId: run.id,
            sourceRunAttemptId: runAttemptId,
            expectedBinding: evidence.binding,
            expectedEvidenceRevision: evidence.evidenceRevision,
          });
          marked.push(marker);
          if (allocatedHere) allocated.push(marker);
        }
        return marked;
      }).pipe(
        Effect.catchCause((cause) =>
          clearServerUpdatePreparation(allocated).pipe(Effect.andThen(Effect.failCause(cause))),
        ),
      );
    }).pipe(
      Effect.mapError(
        (cause) => new ProviderRuntimeRecoveryError({ operation: "reconcile", cause }),
      ),
    );
  const prepareForShutdown = prepareForServerUpdate({ respectProjectPreference: true }).pipe(
    Effect.asVoid,
  );

  const reconcileAfterStartupTrial = (stage: ProviderStartupRecoveryStage) =>
    Effect.gen(function* () {
      const summary = yield* reconcile("startup", true, false);
      const releasedContinuationMarkerIds: Array<string> = [];
      const heldContinuationMarkers: Array<
        ProviderStartupRecoveryResult["heldContinuationMarkers"][number]
      > = [];
      for (const marker of stage.continuationMarkers) {
        if (summary.failedThreadIds.includes(marker.threadId)) {
          heldContinuationMarkers.push({ marker, reason: "projection_unavailable" });
          continue;
        }
        const commandId = CommandId.make(`command:restart-continuation:${marker.markerId}`);
        const revalidateAfterTrial = Effect.gen(function* () {
          const projection = yield* projections.getRuntimeRecoveryProjection(marker.threadId);
          const run = projection.runs.find((candidate) => candidate.id === marker.sourceRunId);
          const attempt = projection.attempts.find(
            (candidate) => candidate.id === marker.sourceRunAttemptId,
          );
          const providerThread = projection.providerThreads.find(
            (candidate) => candidate.id === marker.binding.providerThreadId,
          );
          const registered = yield* eventSink.readProviderRuntimeEvidence(marker.threadId);
          if (
            projection.thread.projectId !== marker.projectId ||
            projection.thread.archivedAt !== null ||
            projection.thread.deletedAt !== null ||
            projection.thread.activeProviderThreadId !== marker.binding.providerThreadId ||
            projection.thread.modelSelection.instanceId !== marker.binding.instanceId ||
            run?.activeAttemptId !== marker.sourceRunAttemptId ||
            run.providerThreadId !== marker.binding.providerThreadId ||
            run.providerInstanceId !== marker.binding.instanceId ||
            attempt?.runId !== marker.sourceRunId ||
            attempt.providerThreadId !== marker.binding.providerThreadId ||
            projection.runs.some(
              (candidate) => candidate.ordinal > run.ordinal && candidate.status !== "rolled_back",
            ) ||
            providerThread?.appThreadId !== marker.threadId ||
            providerThread.providerSessionId !== marker.binding.providerSessionId ||
            providerThread.providerInstanceId !== marker.binding.instanceId ||
            providerThread.driver !== marker.binding.driver ||
            providerThread.nativeThreadRef?.nativeId !== marker.binding.nativeThreadId ||
            registered === null ||
            registered.evidenceRevision !== marker.evidenceRevision ||
            (
              [
                "threadId",
                "providerThreadId",
                "providerSessionId",
                "instanceId",
                "driver",
                "nativeThreadId",
                "runtimeGeneration",
              ] as const
            ).some((key) => registered.binding[key] !== marker.binding[key]) ||
            (yield* outbox.listHeldByThreadId(marker.threadId)).length > 0
          ) {
            return yield* new EventSink.RestartContinuationMarkerError({
              reason: "source_changed",
            });
          }
          const disposition = yield* eventSink.readLegacyContinuationDisposition(marker.threadId);
          if (disposition !== null) {
            const evidence = disposition.evidence;
            if (
              disposition.qualification.type !== "qualified" ||
              evidence === null ||
              evidence.stoppedProof === null ||
              evidence.historicalSourceIdentity === null ||
              evidence.accessibility === null ||
              disposition.qualification.nativeThreadId !== marker.binding.nativeThreadId ||
              evidence.driver !== marker.binding.driver ||
              evidence.nativeThreadId !== marker.binding.nativeThreadId ||
              evidence.accessibility.providerInstanceId !== marker.binding.instanceId ||
              evidence.accessibility.continuationKey !== disposition.qualification.continuationKey
            )
              return yield* new EventSink.RestartContinuationMarkerError({
                reason: "qualification_unavailable",
              });
          } else if (projection.thread.historyOrigin === "v1_import") {
            return yield* new EventSink.RestartContinuationMarkerError({
              reason: "qualification_unavailable",
            });
          }
        });
        const release = yield* Effect.result(
          Effect.gen(function* () {
            yield* revalidateAfterTrial;
            const facts = yield* eventSink.readNativeCommandFacts({
              threadId: marker.threadId,
              commandId,
            });
            return yield* eventSink.releaseRestartContinuation({
              marker,
              currentSnapshot: facts.commitSnapshot,
              revalidateAfterTrial,
            });
          }),
        );
        if (release._tag === "Success" && release.success) {
          releasedContinuationMarkerIds.push(marker.markerId);
        } else {
          const reason =
            release._tag === "Failure" &&
            Schema.is(EventSink.RestartContinuationMarkerError)(release.failure)
              ? release.failure.reason
              : release._tag === "Failure"
                ? "qualification_unavailable"
                : "marker_not_dormant";
          heldContinuationMarkers.push({ marker, reason });
          yield* Effect.logWarning("orchestration-v2.runtime-recovery.continuation-held", {
            threadId: marker.threadId,
            markerId: marker.markerId,
            reason,
          });
        }
      }
      return {
        ...summary,
        releasedContinuationMarkerIds,
        heldContinuationMarkers,
      } satisfies ProviderStartupRecoveryResult;
    });

  const recover = Effect.gen(function* () {
    return (yield* reconcile("startup")) satisfies ProviderRuntimeRecoverySummary;
  });

  return ProviderRuntimeRecoveryService.of({
    reconcile,
    prepareForShutdown,
    prepareForServerUpdate,
    clearServerUpdatePreparation,
    reconcileAfterStartupTrial,
    stageStartupRecovery: eventSink.readDormantRestartContinuations.pipe(
      Effect.map((continuationMarkers) => ({ continuationMarkers })),
      Effect.mapError(
        (cause) => new ProviderRuntimeRecoveryError({ operation: "read-projections", cause }),
      ),
    ),
    recover,
  });
});

export const layer = Layer.effect(ProviderRuntimeRecoveryService, make);
