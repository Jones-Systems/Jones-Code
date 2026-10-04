import {
  CheckpointId,
  CheckpointScopeId,
  CommandId,
  OrchestrationV2DomainEvent,
  OrchestrationV2StoredEvent,
  ProviderThreadId,
  ThreadId,
  WorktreeOwnershipConflictError,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import {
  isCheckpointRestoreIsolated,
  SHARED_WORKSPACE_RESTORE_MESSAGE,
} from "./CheckpointRestoreSafety.ts";
import { CheckpointServiceV2 } from "./CheckpointService.ts";
import { EventSinkV2 } from "./EventSink.ts";
import { OrchestrationEffectRequestV2 } from "./EffectOutbox.ts";
import { IdAllocatorV2 } from "./IdAllocator.ts";
import { nativeCreationCanonicalJson, nativeCreationSha256 } from "./NativeCreationPreparation.ts";
import * as OrdinaryCheckout from "./OrdinaryCheckoutOwnership.ts";
import { ProjectionStoreV2 } from "./ProjectionStore.ts";
import * as ProjectStore from "./ProjectStore.ts";
import type { ProviderAdapterV2RollbackTarget } from "./ProviderAdapter.ts";
import { ProviderSessionManagerV2 } from "./ProviderSessionManager.ts";
import { RuntimePolicyV2 } from "./RuntimePolicy.ts";

export const ROLLBACK_FAILED_MESSAGE =
  "The provider could not roll back this conversation. Try again; if it keeps failing, check the provider and server logs.";

export class CheckpointRollbackExecutionError extends Schema.TaggedError<CheckpointRollbackExecutionError>()(
  "CheckpointRollbackExecutionError",
  {
    reason: Schema.Literals([
      "rollback-target-invalid",
      "active-provider-changed",
      "provider-turn-unavailable",
      "unexpected-failure",
      "shared-workspace",
    ]),
    threadId: ThreadId,
    providerThreadId: ProviderThreadId,
    checkpointId: CheckpointId,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    switch (this.reason) {
      case "rollback-target-invalid":
        return `Rollback target ${this.checkpointId} for provider thread ${this.providerThreadId} on thread ${this.threadId} is incomplete or invalid.`;
      case "active-provider-changed":
        return `Active provider changed before rollback target ${this.checkpointId} could execute on thread ${this.threadId}.`;
      case "provider-turn-unavailable":
        return `Provider turn for rollback target ${this.checkpointId} is unavailable on provider thread ${this.providerThreadId}.`;
      case "shared-workspace":
        return SHARED_WORKSPACE_RESTORE_MESSAGE;
      case "unexpected-failure":
        return ROLLBACK_FAILED_MESSAGE;
    }
  }
}

const isCheckpointRollbackExecutionError = Schema.is(CheckpointRollbackExecutionError);

export interface CheckpointRollbackObservationV1 {
  readonly version: 1;
  readonly kind: "rolled_back";
  readonly sourceEffect: { readonly effectId: string; readonly commandId: CommandId };
  readonly threadId: ThreadId;
  readonly providerThreadId: ProviderThreadId;
  readonly checkpointId: CheckpointId;
  readonly scopeId: CheckpointScopeId;
  readonly completedAt: string;
  readonly pruneEffectId: string;
  readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
  readonly execution: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1;
}
const issuedRollbackObservations = new WeakMap<
  object,
  { readonly observation: CheckpointRollbackObservationV1; readonly snapshot: string }
>();
const issuedExecutionObservations = new WeakMap<
  OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1,
  CheckpointRollbackObservationV1
>();
const rollbackObservationSnapshot = (observation: CheckpointRollbackObservationV1) =>
  nativeCreationCanonicalJson({
    ...observation,
    storedEvents: observation.storedEvents.map((event) =>
      Schema.encodeSync(OrchestrationV2StoredEvent)(event),
    ),
    execution: Schema.encodeSync(OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1)(
      observation.execution,
    ),
  });

// Only the completed physical path and its returned durable batch can issue this observation.
export function readIssuedCheckpointRollbackObservation(
  observation: unknown,
): CheckpointRollbackObservationV1 | null {
  if (typeof observation !== "object" || observation === null) return null;
  const issued = issuedRollbackObservations.get(observation);
  if (issued === undefined) return null;
  try {
    return rollbackObservationSnapshot(issued.observation) === issued.snapshot
      ? issued.observation
      : null;
  } catch {
    return null;
  }
}

export function readIssuedCheckpointRollbackObservationForExecution(
  execution: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1,
): CheckpointRollbackObservationV1 | null {
  const observation = issuedExecutionObservations.get(execution);
  return observation === undefined ? null : readIssuedCheckpointRollbackObservation(observation);
}

export interface CheckpointRollbackServiceV2Shape {
  readonly execute: (input: {
    readonly threadId: ThreadId;
    readonly providerThreadId: ProviderThreadId;
    readonly checkpointId: CheckpointId;
    readonly scopeId: CheckpointScopeId;
    readonly sourceEffect: { readonly effectId: string; readonly commandId: CommandId };
    readonly ordinaryCheckoutExecution?: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1;
    readonly restoreFiles?: boolean;
  }) => Effect.Effect<
    void,
    | CheckpointRollbackExecutionError
    | OrdinaryCheckout.OrdinaryCheckoutOwnershipError
    | WorktreeOwnershipConflictError
  >;
}

export class CheckpointRollbackServiceV2 extends Context.Service<
  CheckpointRollbackServiceV2,
  CheckpointRollbackServiceV2Shape
>()("t3/orchestration-v2/CheckpointRollbackService/CheckpointRollbackServiceV2") {}

export const layer: Layer.Layer<
  CheckpointRollbackServiceV2,
  never,
  | CheckpointServiceV2
  | EventSinkV2
  | IdAllocatorV2
  | ProjectionStoreV2
  | ProviderSessionManagerV2
  | RuntimePolicyV2
  | FileSystem.FileSystem
  | Path.Path
  | ProjectStore.ProjectStoreV2
> = Layer.effect(
  CheckpointRollbackServiceV2,
  Effect.gen(function* () {
    const checkpoints = yield* CheckpointServiceV2;
    const eventSink = yield* EventSinkV2;
    const ids = yield* IdAllocatorV2;
    const projections = yield* ProjectionStoreV2;
    const sessions = yield* ProviderSessionManagerV2;
    const runtimePolicy = yield* RuntimePolicyV2;
    const fileSystem = yield* FileSystem.FileSystem;
    const projects = yield* ProjectStore.ProjectStoreV2;
    const path = yield* Path.Path;

    const execute = Effect.fn("orchestrationV2.checkpointRollback.execute")(function* (input: {
      readonly threadId: ThreadId;
      readonly providerThreadId: ProviderThreadId;
      readonly checkpointId: CheckpointId;
      readonly scopeId: CheckpointScopeId;
      readonly sourceEffect: { readonly effectId: string; readonly commandId: CommandId };
      readonly ordinaryCheckoutExecution?: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1;
      readonly restoreFiles?: boolean;
    }) {
      const sourceEffect = yield* Schema.decodeUnknownEffect(
        Schema.Struct({
          effectId: Schema.NonEmptyString,
          commandId: CommandId,
        }),
      )(input.sourceEffect, { onExcessProperty: "error" });
      if (
        sourceEffect.effectId !== input.sourceEffect.effectId ||
        sourceEffect.commandId !== input.sourceEffect.commandId
      ) {
        return yield* new CheckpointRollbackExecutionError({
          reason: "rollback-target-invalid",
          threadId: input.threadId,
          providerThreadId: input.providerThreadId,
          checkpointId: input.checkpointId,
        });
      }
      const pruneEffectId = `${sourceEffect.effectId}:attachment.cleanup:prune`;
      const previousTask = yield* eventSink.readAttachmentNamespaceCleanupTask(pruneEffectId);
      if (previousTask !== null) {
        if (
          previousTask.effectId !== pruneEffectId ||
          previousTask.commandId !== sourceEffect.commandId ||
          previousTask.threadId !== input.threadId ||
          previousTask.reference.mode !== "prune_thread" ||
          previousTask.reference.rollbackEffectId !== sourceEffect.effectId ||
          previousTask.reference.ownerBirth.threadId !== input.threadId
        ) {
          return yield* new CheckpointRollbackExecutionError({
            reason: "rollback-target-invalid",
            threadId: input.threadId,
            providerThreadId: input.providerThreadId,
            checkpointId: input.checkpointId,
          });
        }
        // A qualified task proves completion already committed; retries must not repeat physical work.
        return;
      }
      const projection = yield* projections.getThreadRecords(input.threadId, [
        "providerThreads",
        "providerSessions",
        "checkpoints",
        "checkpointScopes",
        "runs",
        "attempts",
        "nodes",
        "providerTurns",
      ]);
      const providerThread = projection.providerThreads.find(
        (candidate) => candidate.id === input.providerThreadId,
      );
      const checkpoint = projection.checkpoints.find(
        (candidate) => candidate.id === input.checkpointId,
      );
      const scope = projection.checkpointScopes.find((candidate) => candidate.id === input.scopeId);
      if (
        providerThread === undefined ||
        providerThread.providerSessionId === null ||
        checkpoint === undefined ||
        scope === undefined ||
        checkpoint.scopeId !== scope.id ||
        checkpoint.status !== "ready"
      ) {
        return yield* new CheckpointRollbackExecutionError({
          reason: "rollback-target-invalid",
          threadId: input.threadId,
          providerThreadId: input.providerThreadId,
          checkpointId: input.checkpointId,
        });
      }
      if (
        providerThread.id !== projection.thread.activeProviderThreadId ||
        providerThread.providerInstanceId !== projection.thread.modelSelection.instanceId
      ) {
        return yield* new CheckpointRollbackExecutionError({
          reason: "active-provider-changed",
          threadId: input.threadId,
          providerThreadId: input.providerThreadId,
          checkpointId: input.checkpointId,
        });
      }

      if (
        input.restoreFiles !== false &&
        !(yield* isCheckpointRestoreIsolated(projection.thread, scope, {
          fileSystem,
          projections,
          projects,
          path,
        }))
      ) {
        return yield* new CheckpointRollbackExecutionError({
          reason: "shared-workspace",
          threadId: input.threadId,
          providerThreadId: input.providerThreadId,
          checkpointId: input.checkpointId,
        });
      }

      const birth = yield* eventSink.readApplicationBirthRecord(input.threadId);
      const original = yield* eventSink.readCommandReceiptIdentity(sourceEffect.commandId);
      const receipt = original.receipt;
      if (
        birth === null ||
        birth.threadId !== input.threadId ||
        receipt === null ||
        receipt.status !== "accepted" ||
        receipt.commandId !== sourceEffect.commandId ||
        receipt.threadId !== input.threadId ||
        receipt.commandType !== "checkpoint.rollback" ||
        birth.sequence > receipt.resultSequence
      ) {
        return yield* new CheckpointRollbackExecutionError({
          reason: "rollback-target-invalid",
          threadId: input.threadId,
          providerThreadId: input.providerThreadId,
          checkpointId: input.checkpointId,
        });
      }

      const ordinaryLink = yield* eventSink.readOrdinaryCheckoutEffectLink(sourceEffect.effectId);
      const execution =
        input.ordinaryCheckoutExecution === undefined
          ? undefined
          : yield* Schema.decodeUnknownEffect(
              Schema.toType(OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1),
            )(input.ordinaryCheckoutExecution, { onExcessProperty: "error" });
      const ordinaryRequest = {
        type: "provider-thread.rollback" as const,
        providerThreadId: input.providerThreadId,
        checkpointId: input.checkpointId,
        scopeId: input.scopeId,
        ...(input.restoreFiles === undefined ? {} : { restoreFiles: input.restoreFiles }),
      };
      const requiresExecution =
        ordinaryLink !== null ||
        original.ordinaryCheckoutCommands.some((binding) => binding.threadId === input.threadId);
      if (
        (requiresExecution && execution === undefined) ||
        (execution !== undefined &&
          (ordinaryLink === null ||
            execution.executor.kind !== "actual_outbox_claim" ||
            ordinaryLink.commandId !== sourceEffect.commandId ||
            ordinaryLink.threadId !== input.threadId ||
            ordinaryLink.requestSha256 !==
              nativeCreationSha256(
                nativeCreationCanonicalJson(
                  yield* Schema.encodeEffect(OrchestrationEffectRequestV2)(ordinaryRequest).pipe(
                    Effect.orDie,
                  ),
                ),
              ) ||
            nativeCreationCanonicalJson(
              yield* Schema.encodeEffect(OrdinaryCheckout.OrdinaryCheckoutEffectLinkV1)(
                execution.executor.source.link,
              ).pipe(Effect.orDie),
            ) !==
              nativeCreationCanonicalJson(
                yield* Schema.encodeEffect(OrdinaryCheckout.OrdinaryCheckoutEffectLinkV1)(
                  ordinaryLink,
                ).pipe(Effect.orDie),
              ) ||
            execution.originalUse.lease.ownerThreadId !== input.threadId ||
            execution.originalUse.lease.resourcePath !== scope.cwd))
      ) {
        return yield* new CheckpointRollbackExecutionError({
          reason: "rollback-target-invalid",
          threadId: input.threadId,
          providerThreadId: input.providerThreadId,
          checkpointId: input.checkpointId,
        });
      }
      const revalidateExecution =
        execution === undefined
          ? Effect.void
          : eventSink.revalidateOrdinaryCheckoutExecution(execution).pipe(Effect.asVoid);
      yield* revalidateExecution;

      const modelSelection = projection.thread.modelSelection;
      const resolvedRuntimePolicy = yield* runtimePolicy.resolve({
        thread: projection.thread,
        modelSelection,
      });
      const existingSession = projection.providerSessions.find(
        (candidate) => candidate.id === providerThread.providerSessionId,
      );
      yield* revalidateExecution;
      const session = yield* sessions.open({
        threadId: input.threadId,
        providerSessionId: providerThread.providerSessionId,
        modelSelection,
        runtimePolicy: resolvedRuntimePolicy,
        ...(existingSession === undefined ? {} : { resumeFromSession: existingSession }),
        ...(providerThread.nativeThreadRef?.nativeId == null
          ? {}
          : { initialNativeThreadId: providerThread.nativeThreadRef.nativeId }),
        ...(providerThread.nativeMetadata?.itemIdentityVersion === undefined
          ? {}
          : {
              initialProviderItemIdentityVersion: providerThread.nativeMetadata.itemIdentityVersion,
            }),
      });
      const attachment =
        execution === undefined
          ? undefined
          : yield* sessions.captureOrdinaryExecutionAttachment({
              runtime: session,
              threadId: input.threadId,
              providerThread,
            });
      const revalidateCapturedExecution = Effect.gen(function* () {
        yield* revalidateExecution;
        if (attachment !== undefined) yield* attachment.revalidateCaptured;
      });

      const targetOrdinal = checkpoint.appRunOrdinal ?? 0;
      // Stopped and failed runs after the target leave the provider
      // conversation too, so they must not stay visible.
      const runsToRollback = projection.runs.filter(
        (run) =>
          run.ordinal > targetOrdinal &&
          (run.status === "completed" ||
            run.status === "interrupted" ||
            run.status === "failed" ||
            run.status === "cancelled"),
      );
      // Rolled-back turns stay in the audit history, but no longer exist in
      // the provider conversation and must not be counted by a later rewind.
      const rolledBackRunIds = new Set(
        projection.runs.filter((run) => run.status === "rolled_back").map((run) => run.id),
      );
      const rolledBackAttemptIds = new Set(
        projection.attempts
          .filter((attempt) => rolledBackRunIds.has(attempt.runId))
          .map((attempt) => attempt.id),
      );
      const providerThreadTurns = projection.providerTurns.filter(
        (turn) =>
          turn.providerThreadId === providerThread.id &&
          (turn.runAttemptId === null || !rolledBackAttemptIds.has(turn.runAttemptId)),
      );
      const rollbackTarget: ProviderAdapterV2RollbackTarget =
        targetOrdinal === 0
          ? {
              type: "thread_start",
              checkpointId: checkpoint.id,
              appRunOrdinal: 0,
            }
          : yield* Effect.gen(function* () {
              const targetRun = projection.runs.find((run) => run.ordinal === targetOrdinal);
              const targetAttempt = projection.attempts.find(
                (attempt) => attempt.id === targetRun?.activeAttemptId,
              );
              const targetTurn = projection.providerTurns.find(
                (turn) =>
                  turn.id === targetAttempt?.providerTurnId ||
                  turn.runAttemptId === targetAttempt?.id,
              );
              if (targetTurn === undefined || targetTurn.providerThreadId !== providerThread.id) {
                return yield* new CheckpointRollbackExecutionError({
                  reason: "provider-turn-unavailable",
                  threadId: input.threadId,
                  providerThreadId: input.providerThreadId,
                  checkpointId: input.checkpointId,
                });
              }
              return {
                type: "provider_turn" as const,
                checkpointId: checkpoint.id,
                appRunOrdinal: targetOrdinal,
                providerTurn: targetTurn,
              };
            });

      yield* revalidateCapturedExecution;
      const snapshot =
        runsToRollback.length === 0
          ? { providerThread }
          : yield* session.rollbackThread({
              providerThread,
              target: rollbackTarget,
              providerThreadTurns,
            });
      const checkoutContext =
        execution === undefined
          ? {}
          : {
              ordinaryCheckoutExecution: execution,
              ordinaryCheckoutUse: execution.originalUse,
            };
      if (input.restoreFiles !== false)
        yield* checkpoints.restore({ scope, checkpoint, ...checkoutContext });
      const staleCheckpoints = projection.checkpoints.filter(
        (candidate) =>
          candidate.scopeId === scope.id &&
          candidate.appRunOrdinal !== null &&
          candidate.appRunOrdinal > targetOrdinal &&
          candidate.status === "ready",
      );
      if (staleCheckpoints.length > 0) {
        yield* checkpoints.deleteStaleRefs({
          scope,
          checkpoints: staleCheckpoints,
          ...checkoutContext,
        });
      }
      yield* revalidateCapturedExecution;

      const now = yield* DateTime.now;
      const makeEvent = <Event extends OrchestrationV2DomainEvent>(event: Omit<Event, "id">) =>
        Effect.map(
          ids.allocate.event({ threadId: event.threadId }),
          (id) =>
            ({
              ...event,
              id,
            }) as Event,
        );
      const events: Array<OrchestrationV2DomainEvent> = [];
      const rollbackEvent = yield* makeEvent({
        type: "provider-thread.updated",
        threadId: input.threadId,
        driver: providerThread.driver,
        providerInstanceId: providerThread.providerInstanceId,
        occurredAt: now,
        payload: {
          ...snapshot.providerThread,
          lastRunOrdinal: targetOrdinal === 0 ? null : targetOrdinal,
          updatedAt: now,
        },
      });
      events.push(rollbackEvent);
      for (const staleCheckpoint of staleCheckpoints) {
        events.push(
          yield* makeEvent({
            type: "checkpoint.captured",
            threadId: input.threadId,
            ...(staleCheckpoint.runId === null ? {} : { runId: staleCheckpoint.runId }),
            nodeId: staleCheckpoint.nodeId,
            providerInstanceId: providerThread.providerInstanceId,
            occurredAt: now,
            payload: { ...staleCheckpoint, status: "stale" },
          }),
        );
      }
      for (const run of runsToRollback) {
        const rootNode = projection.nodes.find((candidate) => candidate.id === run.rootNodeId);
        events.push(
          yield* makeEvent({
            type: "run.updated",
            threadId: input.threadId,
            runId: run.id,
            ...(rootNode === undefined ? {} : { nodeId: rootNode.id }),
            providerInstanceId: run.providerInstanceId,
            occurredAt: now,
            payload: { ...run, status: "rolled_back", completedAt: now },
          }),
        );
        if (rootNode !== undefined) {
          events.push(
            yield* makeEvent({
              type: "node.updated",
              threadId: input.threadId,
              runId: run.id,
              nodeId: rootNode.id,
              providerInstanceId: run.providerInstanceId,
              occurredAt: now,
              payload: { ...rootNode, status: "rolled_back", completedAt: now },
            }),
          );
        }
      }
      const storedEvents = yield* eventSink.writeWithEffects({
        commandId: sourceEffect.commandId,
        events,
        effects: [
          {
            id: pruneEffectId,
            commandId: sourceEffect.commandId,
            threadId: input.threadId,
            request: { type: "attachment.cleanup", attachmentIds: [] },
            attachmentNamespaceCleanup: {
              version: 1,
              mode: "prune_thread",
              ownerBirth: birth,
              triggerEventId: rollbackEvent.id,
              rollbackEffectId: sourceEffect.effectId,
            },
          },
        ],
      });
      const matchingCompletion =
        storedEvents.length === events.length &&
        storedEvents.every(
          (stored, index) =>
            stored.commandId === sourceEffect.commandId &&
            stored.sequence > 0 &&
            (index === 0 || stored.sequence > storedEvents[index - 1]!.sequence) &&
            nativeCreationCanonicalJson(
              Schema.encodeSync(OrchestrationV2DomainEvent)(stored.event),
            ) ===
              nativeCreationCanonicalJson(
                Schema.encodeSync(OrchestrationV2DomainEvent)(events[index]!),
              ),
        );
      if (
        execution !== undefined &&
        input.ordinaryCheckoutExecution !== undefined &&
        matchingCompletion
      ) {
        const observation = Object.freeze({
          version: 1 as const,
          kind: "rolled_back" as const,
          sourceEffect: Object.freeze({ ...sourceEffect }),
          threadId: input.threadId,
          providerThreadId: input.providerThreadId,
          checkpointId: input.checkpointId,
          scopeId: input.scopeId,
          completedAt: DateTime.formatIso(now),
          pruneEffectId,
          storedEvents,
          execution,
        });
        const snapshot = rollbackObservationSnapshot(observation);
        const originalExecution = input.ordinaryCheckoutExecution;
        yield* eventSink.onCommit(
          Effect.sync(() => {
            issuedRollbackObservations.set(observation, { observation, snapshot });
            issuedExecutionObservations.set(originalExecution, observation);
          }),
        );
      }
    });

    return CheckpointRollbackServiceV2.of({
      execute: (input) =>
        execute(input).pipe(
          Effect.mapError((cause) =>
            isCheckpointRollbackExecutionError(cause) ||
            Schema.is(OrdinaryCheckout.OrdinaryCheckoutOwnershipError)(cause) ||
            Schema.is(WorktreeOwnershipConflictError)(cause)
              ? cause
              : new CheckpointRollbackExecutionError({
                  reason: "unexpected-failure",
                  threadId: input.threadId,
                  providerThreadId: input.providerThreadId,
                  checkpointId: input.checkpointId,
                  cause,
                }),
          ),
        ),
    });
  }),
);
