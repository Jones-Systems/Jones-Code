import {
  CheckpointScopeId,
  CommandId,
  OrchestrationV2Checkpoint,
  OrchestrationV2StoredEvent,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  type OrchestrationV2TurnItem,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import * as CheckpointService from "./CheckpointService.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as OrdinaryCheckout from "./OrdinaryCheckoutOwnership.ts";
import * as EventSink from "./EventSink.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import type {
  OrdinaryCheckoutUseV1,
  OrdinaryCheckoutExecutionRefV1,
} from "./OrdinaryCheckoutOwnership.ts";

export class CheckpointCaptureExecutionError extends Schema.TaggedError<CheckpointCaptureExecutionError>()(
  "CheckpointCaptureExecutionError",
  {
    threadId: ThreadId,
    runId: RunId,
    scopeId: CheckpointScopeId,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

const isCheckpointCaptureExecutionError = Schema.is(CheckpointCaptureExecutionError);

export type CheckpointCaptureCommitResultV1 = Effect.Success<
  ReturnType<EventSink.EventSinkV2Shape["commitCommand"]>
>;
export type CheckpointCaptureObservationV1 =
  | {
      readonly version: 1;
      readonly kind: "captured";
      readonly checkpoint: OrchestrationV2Checkpoint;
      readonly commit: CheckpointCaptureCommitResultV1;
      readonly ordinaryCheckoutExecution?: OrdinaryCheckoutExecutionRefV1;
      readonly ordinaryFinalCheckpointBasis?: EventSink.OrdinaryFinalCheckpointCompletionBasisV1;
    }
  | { readonly version: 1; readonly kind: "skipped"; readonly reason: "settled" | "rolled_back" };

type CapturedCheckpointObservationV1 = Extract<
  CheckpointCaptureObservationV1,
  { readonly kind: "captured" }
>;
const issuedCheckpointObservations = new WeakMap<
  object,
  { readonly observation: CapturedCheckpointObservationV1; readonly snapshot: string }
>();
const issuedExecutionObservations = new WeakMap<
  OrdinaryCheckoutExecutionRefV1,
  CapturedCheckpointObservationV1
>();

function checkpointObservationSnapshot(observation: CapturedCheckpointObservationV1): string {
  return JSON.stringify({
    checkpoint: Schema.encodeSync(OrchestrationV2Checkpoint)(observation.checkpoint),
    receipt: Schema.encodeSync(CommandReceiptStore.CommandReceiptV2)(observation.commit.receipt),
    storedEvents: observation.commit.storedEvents.map((event) =>
      Schema.encodeSync(OrchestrationV2StoredEvent)(event),
    ),
    committed: observation.commit.committed,
    cancelledEffectCount: observation.commit.cancelledEffectCount,
    ordinaryCheckoutExecution:
      observation.ordinaryCheckoutExecution === undefined
        ? null
        : Schema.encodeSync(OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1)(
            observation.ordinaryCheckoutExecution,
          ),
    ordinaryFinalCheckpointBasis:
      observation.ordinaryFinalCheckpointBasis === undefined
        ? null
        : Schema.encodeSync(EventSink.OrdinaryFinalCheckpointCompletionBasisV1)(
            observation.ordinaryFinalCheckpointBasis,
          ),
  });
}

// Only this native-result path publishes after durable commit. Projection records and object copies cannot issue a proof.
export function readIssuedCheckpointCaptureObservation(
  observation: unknown,
): CapturedCheckpointObservationV1 | null {
  if (typeof observation !== "object" || observation === null) return null;
  const issued = issuedCheckpointObservations.get(observation);
  if (issued === undefined) return null;
  try {
    return checkpointObservationSnapshot(issued.observation) === issued.snapshot
      ? issued.observation
      : null;
  } catch {
    return null;
  }
}

// Retain the real result across a later readonly refresh failure; a reconstructed execution ref has no entry.
export function readIssuedCheckpointCaptureObservationForExecution(
  execution: OrdinaryCheckoutExecutionRefV1,
): CapturedCheckpointObservationV1 | null {
  const observation = issuedExecutionObservations.get(execution);
  return observation === undefined ? null : readIssuedCheckpointCaptureObservation(observation);
}

export interface CheckpointCaptureServiceV2Shape {
  readonly execute: (input: {
    readonly threadId: ThreadId;
    readonly runId: RunId;
    readonly scopeId: CheckpointScopeId;
    readonly ordinaryCheckoutUse?: OrdinaryCheckoutUseV1;
    readonly ordinaryCheckoutExecution?: OrdinaryCheckoutExecutionRefV1;
    readonly ordinaryFinalCheckpointBasis?: EventSink.OrdinaryFinalCheckpointCompletionBasisV1;
  }) => Effect.Effect<
    CheckpointCaptureObservationV1,
    CheckpointCaptureExecutionError | CheckpointService.OrdinaryCheckoutMutationError
  >;
}

export class CheckpointCaptureServiceV2 extends Context.Service<
  CheckpointCaptureServiceV2,
  CheckpointCaptureServiceV2Shape
>()("t3/orchestration-v2/CheckpointCaptureService/CheckpointCaptureServiceV2") {}

export const layer: Layer.Layer<
  CheckpointCaptureServiceV2,
  never,
  | CheckpointService.CheckpointServiceV2
  | EventSink.EventSinkV2
  | IdAllocator.IdAllocatorV2
  | ProjectionStore.ProjectionStoreV2
> = Layer.effect(
  CheckpointCaptureServiceV2,
  Effect.gen(function* () {
    const checkpoints = yield* CheckpointService.CheckpointServiceV2;
    const eventSink = yield* EventSink.EventSinkV2;
    const ids = yield* IdAllocator.IdAllocatorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;

    const execute = Effect.fn("orchestrationV2.checkpointCapture.execute")(function* (input: {
      readonly threadId: ThreadId;
      readonly runId: RunId;
      readonly scopeId: CheckpointScopeId;
      readonly ordinaryCheckoutUse?: OrdinaryCheckoutUseV1;
      readonly ordinaryCheckoutExecution?: OrdinaryCheckoutExecutionRefV1;
      readonly ordinaryFinalCheckpointBasis?: EventSink.OrdinaryFinalCheckpointCompletionBasisV1;
    }) {
      if (input.ordinaryFinalCheckpointBasis !== undefined) {
        const basis = input.ordinaryFinalCheckpointBasis;
        if (
          input.ordinaryCheckoutExecution === undefined ||
          basis.runId !== input.runId ||
          basis.scopeId !== input.scopeId ||
          (yield* Schema.encodeEffect(
            Schema.fromJsonString(OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1),
          )(basis.checkpointExecution).pipe(Effect.orDie)) !==
            (yield* Schema.encodeEffect(
              Schema.fromJsonString(OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1),
            )(input.ordinaryCheckoutExecution).pipe(Effect.orDie))
        )
          return yield* new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
            reason: "claim_mismatch",
            threadId: input.threadId,
            path: basis.checkpointExecution.originalUse.lease.resourcePath,
            message: "Final physical capture differs from its actual joined checkpoint basis.",
          });
        yield* eventSink.revalidateOrdinaryFinalCheckpointBasis(basis);
      }
      const { run, rootNode, scope, providerThread, readyCheckpointOrdinals } =
        yield* projections.getCheckpointCaptureContext(input.threadId, input);
      // A stopped or failed admitted run is already terminal. Its checkpoint
      // is the rollback point for the next message, so its status stays intact.
      const stopped =
        run?.status === "interrupted" ||
        run?.status === "cancelled" ||
        (run?.status === "failed" && input.ordinaryCheckoutExecution !== undefined);

      // The effect is at-least-once. A settled run with a checkpoint proves
      // that an earlier execution committed its result.
      if (
        run !== undefined &&
        run.checkpointId !== null &&
        (run.status === "completed" || stopped)
      ) {
        if (input.ordinaryFinalCheckpointBasis !== undefined)
          return yield* new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
            reason: "unknown_use",
            threadId: input.threadId,
            path: input.ordinaryFinalCheckpointBasis.checkpointExecution.originalUse.lease
              .resourcePath,
            message:
              "An earlier settled checkpoint cannot prove this final native cohort's physical capture.",
          });
        return { version: 1, kind: "skipped", reason: "settled" } as const;
      }
      // Rollback shares this effect lane, so it can only land before a capture
      // runs, e.g. while a failed capture waits to retry. The workspace now
      // holds the rollback target, and the run must stay discarded.
      if (run?.status === "rolled_back") {
        if (input.ordinaryFinalCheckpointBasis !== undefined)
          return yield* new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
            reason: "unknown_use",
            threadId: input.threadId,
            path: input.ordinaryFinalCheckpointBasis.checkpointExecution.originalUse.lease
              .resourcePath,
            message:
              "A rolled-back run cannot produce this final native cohort's physical capture.",
          });
        return { version: 1, kind: "skipped", reason: "rolled_back" } as const;
      }

      if (
        run === undefined ||
        (run.status !== "waiting" &&
          !stopped &&
          !(input.ordinaryFinalCheckpointBasis !== undefined && run.status === "completed")) ||
        rootNode === undefined ||
        scope === undefined ||
        rootNode.checkpointScopeId !== scope.id ||
        providerThread === undefined
      ) {
        return yield* new CheckpointCaptureExecutionError({
          threadId: input.threadId,
          runId: input.runId,
          scopeId: input.scopeId,
          cause: "The persisted checkpoint capture target is incomplete or no longer waiting.",
        });
      }

      const capturedAt = yield* DateTime.now;
      const baselineOrdinalWithinScope = Math.max(0, run.ordinal - 1);
      const hasReadyCheckpoint = (ordinalWithinScope: number) =>
        readyCheckpointOrdinals.includes(ordinalWithinScope);
      const threadStartCheckpoint =
        baselineOrdinalWithinScope === 0 || hasReadyCheckpoint(0)
          ? null
          : yield* checkpoints.materializeBaselineCheckpoint({
              scope,
              ordinalWithinScope: 0,
            });
      const baselineCheckpoint = hasReadyCheckpoint(baselineOrdinalWithinScope)
        ? null
        : yield* checkpoints.materializeBaselineCheckpoint({
            scope,
            ordinalWithinScope: baselineOrdinalWithinScope,
          });
      const checkpoint = yield* checkpoints
        .capture({
          scope,
          runId: run.id,
          nodeId: rootNode.id,
          ordinalWithinScope: run.ordinal,
          appRunOrdinal: run.ordinal,
          capturedAt,
          ...(input.ordinaryCheckoutUse === undefined
            ? {}
            : { ordinaryCheckoutUse: input.ordinaryCheckoutUse }),
          ...(input.ordinaryCheckoutExecution === undefined
            ? {}
            : { ordinaryCheckoutExecution: input.ordinaryCheckoutExecution }),
          ...(input.ordinaryFinalCheckpointBasis === undefined
            ? {}
            : { ordinaryFinalCheckpointBasis: input.ordinaryFinalCheckpointBasis }),
        })
        .pipe(Effect.provideService(EventSink.EventSinkV2, eventSink));
      // Match RunExecutionService: capture loaded the waiting run before
      // materializing baselines. Omit delegatedCompletion so a newer cohort
      // write during capture is not overwritten by this stale snapshot
      // (ProjectionStore preserves the field when absent from the payload).
      const { delegatedCompletion: _delegatedCompletion, ...runWithoutDelegatedCompletion } = run;
      const commandId = CommandId.make(`command:effect:checkpoint.capture:${run.id}`);
      return yield* eventSink.withTransaction(
        Effect.gen(function* () {
          const commit = yield* eventSink.commitCommand({
            commandId,
            threadId: input.threadId,
            commandType: "checkpoint.capture",
            acceptedAt: capturedAt,
            effects: [],
            ...(input.ordinaryCheckoutUse === undefined
              ? {}
              : { ordinaryCheckoutUse: input.ordinaryCheckoutUse }),
            ...(input.ordinaryCheckoutExecution === undefined
              ? {}
              : { ordinaryCheckoutExecution: input.ordinaryCheckoutExecution }),
            ...(input.ordinaryFinalCheckpointBasis === undefined
              ? {}
              : { ordinaryFinalCheckpointBasis: input.ordinaryFinalCheckpointBasis }),
            events: [
              ...(threadStartCheckpoint === null
                ? []
                : [
                    {
                      id: yield* ids.allocate.event({ threadId: input.threadId, commandId }),
                      type: "checkpoint.captured" as const,
                      threadId: input.threadId,
                      nodeId: threadStartCheckpoint.nodeId,
                      driver: providerThread.driver,
                      providerInstanceId: run.providerInstanceId,
                      occurredAt: capturedAt,
                      payload: threadStartCheckpoint,
                    },
                  ]),
              ...(baselineCheckpoint === null
                ? []
                : [
                    {
                      id: yield* ids.allocate.event({ threadId: input.threadId, commandId }),
                      type: "checkpoint.captured" as const,
                      threadId: input.threadId,
                      nodeId: baselineCheckpoint.nodeId,
                      driver: providerThread.driver,
                      providerInstanceId: run.providerInstanceId,
                      occurredAt: capturedAt,
                      payload: baselineCheckpoint,
                    },
                  ]),
              {
                id: yield* ids.allocate.event({ threadId: input.threadId, commandId }),
                type: "checkpoint.captured",
                threadId: input.threadId,
                runId: run.id,
                nodeId: rootNode.id,
                driver: providerThread.driver,
                providerInstanceId: run.providerInstanceId,
                occurredAt: capturedAt,
                payload: checkpoint,
              },
              {
                id: yield* ids.allocate.event({ threadId: input.threadId, commandId }),
                type: "turn-item.updated",
                threadId: input.threadId,
                runId: run.id,
                nodeId: rootNode.id,
                driver: providerThread.driver,
                providerInstanceId: run.providerInstanceId,
                occurredAt: capturedAt,
                payload: makeCheckpointTurnItem({
                  idAllocator: ids,
                  run,
                  rootNode,
                  providerThread,
                  checkpoint,
                  completedAt: capturedAt,
                }),
              },
              {
                id: yield* ids.allocate.event({ threadId: input.threadId, commandId }),
                type: "run.updated",
                threadId: input.threadId,
                runId: run.id,
                nodeId: rootNode.id,
                providerInstanceId: run.providerInstanceId,
                occurredAt: capturedAt,
                payload: stopped
                  ? { ...runWithoutDelegatedCompletion, checkpointId: checkpoint.id }
                  : {
                      ...runWithoutDelegatedCompletion,
                      status: "completed",
                      completedAt: capturedAt,
                      checkpointId: checkpoint.id,
                    },
              },
              ...(stopped
                ? []
                : [
                    {
                      id: yield* ids.allocate.event({ threadId: input.threadId, commandId }),
                      type: "node.updated" as const,
                      threadId: input.threadId,
                      runId: run.id,
                      nodeId: rootNode.id,
                      providerInstanceId: run.providerInstanceId,
                      occurredAt: capturedAt,
                      payload: {
                        ...rootNode,
                        status: "completed" as const,
                        completedAt: capturedAt,
                        checkpointScopeId: scope.id,
                      },
                    },
                  ]),
            ],
          });
          const observation: CapturedCheckpointObservationV1 = Object.freeze({
            version: 1,
            kind: "captured",
            checkpoint,
            commit,
            ...(input.ordinaryCheckoutExecution === undefined
              ? {}
              : { ordinaryCheckoutExecution: input.ordinaryCheckoutExecution }),
            ...(input.ordinaryFinalCheckpointBasis === undefined
              ? {}
              : { ordinaryFinalCheckpointBasis: input.ordinaryFinalCheckpointBasis }),
          });
          const matchingEvents = (commit.storedEvents ?? []).filter(
            ({ event, commandId: storedCommandId, sequence }) =>
              storedCommandId === commandId &&
              sequence > 0 &&
              commit.receipt !== undefined &&
              sequence <= commit.receipt.resultSequence &&
              event.type === "checkpoint.captured" &&
              event.threadId === input.threadId &&
              event.runId === run.id &&
              event.nodeId === rootNode.id &&
              JSON.stringify(Schema.encodeSync(OrchestrationV2Checkpoint)(event.payload)) ===
                JSON.stringify(Schema.encodeSync(OrchestrationV2Checkpoint)(checkpoint)),
          );
          if (
            checkpoint.status === "ready" &&
            commit.committed &&
            commit.receipt?.status === "accepted" &&
            commit.receipt.error === null &&
            commit.receipt.commandId === commandId &&
            commit.receipt.commandType === "checkpoint.capture" &&
            commit.receipt.threadId === input.threadId &&
            matchingEvents.length === 1
          ) {
            const snapshot = checkpointObservationSnapshot(observation);
            yield* eventSink.onCommit(
              Effect.sync(() => {
                issuedCheckpointObservations.set(observation, { observation, snapshot });
                if (observation.ordinaryCheckoutExecution !== undefined)
                  issuedExecutionObservations.set(
                    observation.ordinaryCheckoutExecution,
                    observation,
                  );
              }),
            );
          }
          return observation;
        }),
      );
    });

    return CheckpointCaptureServiceV2.of({
      execute: (input) =>
        execute(input).pipe(
          Effect.mapError((cause) =>
            isCheckpointCaptureExecutionError(cause) ||
            CheckpointService.isOrdinaryCheckoutMutationError(cause)
              ? cause
              : new CheckpointCaptureExecutionError({ ...input, cause }),
          ),
        ),
    });
  }),
);

function makeCheckpointTurnItem(input: {
  readonly idAllocator: IdAllocator.IdAllocatorV2Shape;
  readonly run: OrchestrationV2Run;
  readonly rootNode: OrchestrationV2ExecutionNode;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly checkpoint: OrchestrationV2Checkpoint;
  readonly completedAt: DateTime.Utc;
}): OrchestrationV2TurnItem {
  return {
    id: input.idAllocator.derive.turnItemFromProviderItem({
      driver: input.providerThread.driver,
      nativeItemId: `checkpoint:${input.checkpoint.id}`,
    }),
    threadId: input.run.threadId,
    runId: input.run.id,
    nodeId: input.rootNode.id,
    providerThreadId: input.providerThread.id,
    providerTurnId: input.rootNode.providerTurnId,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: input.run.ordinal * 100 + 99,
    status: "completed",
    title: null,
    startedAt: input.completedAt,
    completedAt: input.completedAt,
    updatedAt: input.completedAt,
    type: "checkpoint",
    checkpointId: input.checkpoint.id,
    scopeId: input.checkpoint.scopeId,
    files: input.checkpoint.files,
  };
}
