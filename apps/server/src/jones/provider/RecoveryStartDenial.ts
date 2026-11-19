import {
  CommandId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Context from "effect/Context";
import type * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { CommandReceiptStoreV2 } from "../../orchestration-v2/CommandReceiptStore.ts";
import {
  EffectOutboxError,
  EffectOutboxV2,
  type OrchestrationEffectV2,
} from "../../orchestration-v2/EffectOutbox.ts";
import { EventSinkV2 } from "../../orchestration-v2/EventSink.ts";
import { ProjectionStoreV2 } from "../../orchestration-v2/ProjectionStore.ts";
import { sameOperationRequest } from "../workspaceLifecycle/NativeOperation.ts";
import { recoveryResendEffect } from "./UnsentStartProof.ts";

const decodePayload = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Unknown));

export class RecoveryStartDenial extends Context.Service<
  RecoveryStartDenial,
  {
    readonly withTransaction: EventSinkV2["Service"]["withProviderBatch"];
    readonly settle: (
      effect: OrchestrationEffectV2,
      now: DateTime.Utc,
    ) => Effect.Effect<void, unknown>;
  }
>()("t3/jones/provider/RecoveryStartDenial") {}

const make = Effect.gen(function* () {
  const sink = yield* EventSinkV2;
  const receipts = yield* CommandReceiptStoreV2;
  const projections = yield* ProjectionStoreV2;
  const outbox = yield* EffectOutboxV2;
  const ids = yield* IdAllocator.IdAllocatorV2;

  const settle = Effect.fn("RecoveryStartDenial.settle")(function* (
    effect: OrchestrationEffectV2,
    now: DateTime.Utc,
  ) {
    const { request, threadId } = effect;
    if (
      request.type !== "provider-turn.start" ||
      effect.nativeCreationExecutionReference !== undefined
    )
      return;
    const { runId } = request;
    const prefix = `effect:restart-resend:${runId}:`;
    const payload = decodePayload(effect.payloadIdentity);
    if (
      !effect.id.startsWith(prefix) ||
      Option.isNone(payload) ||
      !sameOperationRequest(payload.value, request)
    )
      return;
    const commandId = CommandId.make(effect.id.slice(prefix.length));
    const receipt = Option.getOrUndefined(yield* receipts.getByCommandId(commandId));
    if (
      receipt?.threadId !== threadId ||
      receipt.status !== "accepted" ||
      receipt.error !== null ||
      receipt.commandType !== "provider-runtime.reconcile"
    )
      return;
    const events = yield* sink.readByCommandId({ commandId }).pipe(Stream.runCollect);
    if (events.at(-1)?.sequence !== receipt.resultSequence) return;
    const [origin, duplicate] = events.filter(
      ({ event }) =>
        event.type === "run.updated" && event.threadId === threadId && event.runId === runId,
    );
    const recorded = origin?.event;
    if (
      duplicate !== undefined ||
      recorded?.type !== "run.updated" ||
      recorded.payload.rootNodeId === null
    )
      return;
    const expected = recoveryResendEffect(recorded.payload, commandId);
    if (expected.commandId !== effect.commandId || expected.id !== effect.id) return;

    const { runs, attempts, nodes, providerTurns } = yield* projections.getRunProviderRecords(
      threadId,
      runId,
      recorded.payload.rootNodeId,
    );
    const run = runs[0];
    const attempt = attempts[0];
    const root = nodes[0];
    if (
      run?.status !== "starting" ||
      run.startedAt !== null ||
      run.completedAt !== null ||
      run.restartContinuationOfRunId !== undefined ||
      !sameOperationRequest(run, recorded.payload) ||
      attempt?.id !== run.activeAttemptId ||
      attempt.runId !== run.id ||
      attempt.reason !== "initial" ||
      attempt.attemptOrdinal !== 1 ||
      attempt.status !== "pending" ||
      attempt.startedAt !== null ||
      attempt.completedAt !== null ||
      attempt.providerTurnId !== null ||
      attempt.providerSettlement != null ||
      attempt.nativeThreadId !== undefined ||
      attempt.providerThreadId !== run.providerThreadId ||
      attempt.providerInstanceId !== run.providerInstanceId ||
      attempt.rootNodeId !== run.rootNodeId ||
      root?.id !== run.rootNodeId ||
      root.threadId !== threadId ||
      root.runId !== run.id ||
      root.rootNodeId !== root.id ||
      root.kind !== "root_turn" ||
      root.status !== "pending" ||
      root.startedAt !== null ||
      root.completedAt !== null ||
      root.providerTurnId !== null ||
      providerTurns.length !== 0
    )
      return;

    // The reconciliation receipt certifies the original input. Recheck custody in
    // case another start or restart took over the same run after reconciliation.
    const predecessors = (yield* outbox.listByThreadId(threadId)).filter(
      (row) =>
        row.id !== effect.id &&
        (row.request.type === "provider-turn.start" ||
          row.request.type === "provider-turn.restart") &&
        row.request.runId === runId,
    );
    if (
      predecessors.length === 0 ||
      predecessors.some(
        (row) =>
          row.status !== "cancelled" ||
          row.attemptCount !== 0 ||
          row.leaseOwner !== null ||
          row.completedAt === null ||
          row.nativeCreationExecutionReference !== undefined,
      )
    )
      return;

    const notice: OrchestrationV2TurnItem = {
      id: ids.derive.runSignalTurnItem({ runId, signal: "recovery-start-denied" }),
      type: "error",
      title: "Recovered turn did not start",
      failure: makeProviderFailure({
        class: "provider_error",
        message: "The recovered start was blocked before contacting the provider.",
        retryable: false,
      }),
      threadId,
      runId,
      nodeId: root.id,
      providerThreadId: run.providerThreadId,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: yield* projections.getNextTurnItemOrdinal(threadId),
      status: "failed",
      startedAt: now,
      completedAt: now,
      updatedAt: now,
    };
    const updates = [
      { type: "turn-item.updated", payload: notice },
      { type: "run.updated", payload: { ...run, status: "failed", completedAt: now } },
      { type: "run-attempt.updated", payload: { ...attempt, status: "failed", completedAt: now } },
      { type: "node.updated", payload: { ...root, status: "failed", completedAt: now } },
    ] as const;
    const written = yield* sink.writeIfRunCurrent({
      threadId,
      runId,
      activeAttemptId: attempt.id,
      expectedStatus: "starting",
      events: yield* Effect.forEach(updates, (update) =>
        ids.allocate.event({ threadId }).pipe(
          Effect.map((id): OrchestrationV2DomainEvent => ({
            ...update,
            id,
            threadId,
            runId,
            nodeId: root.id,
            providerInstanceId: run.providerInstanceId,
            occurredAt: now,
          })),
        ),
      ),
    });
    if (!written.committed)
      return yield* new EffectOutboxError({
        operation: "claim",
        effectId: effect.id,
        cause: "Recovered start denial could not settle the exact active attempt.",
      });
  });
  return RecoveryStartDenial.of({ withTransaction: sink.withProviderBatch, settle });
});

export const layer = Layer.effect(RecoveryStartDenial, make);
