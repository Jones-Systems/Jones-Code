import { sameOperationRequest } from "../workspaceLifecycle/NativeOperation.ts";
import type { OrchestrationV2Run, OrchestrationV2RunAttempt } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import type * as Outbox from "../../orchestration-v2/EffectOutbox.ts";
import type * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import type * as Sink from "../../orchestration-v2/EventSink.ts";
import type * as Refusals from "../startRefusal/Store.ts";

// A cancelled original row with a monotonic zero claim count is positive custody
// evidence. A missing turn, null startedAt, retry row or held control is not.
export function makeUnsentStartProof(ports: {
  readonly outbox: Pick<Outbox.EffectOutboxV2Shape, "listByThreadId" | "get">;
  readonly receipts: Pick<Receipts.CommandReceiptStoreV2Shape, "getByCommandId">;
  readonly sink: Pick<Sink.EventSinkV2Shape, "readByCommandId">;
  readonly refusals: Refusals.StartRefusals["Service"];
}) {
  return (run: OrchestrationV2Run, attempts: ReadonlyArray<OrchestrationV2RunAttempt>) =>
    Effect.gen(function* () {
      const attempt = attempts.find((row) => row.id === run.activeAttemptId && row.runId === run.id);
      if (attempt === undefined || run.manualContinuationOfRunId !== undefined) return false;
      const rows = (yield* ports.outbox.listByThreadId(run.threadId)).filter((row) =>
        (row.request.type === "provider-turn.start" || row.request.type === "provider-turn.restart") &&
        row.request.runId === run.id);
      if (rows.length === 0 || rows.some((row) => row.nativeCreationExecutionReference !== undefined)) return false;
      const prefix = yield* ports.refusals.readPrefix({ threadId: run.threadId,
        runId: run.id, attemptId: attempt.id });
      if (prefix.state === "refused") return rows.some((row) => row.commandId === prefix.commandId);

      if (prefix.state !== "unknown" || run.startedAt !== null || attempt.reason !== "initial" ||
          attempt.attemptOrdinal !== 1 || attempts.filter((row) => row.runId === run.id).length !== 1 ||
          rows.length !== 1) return false;
      const original = rows[0]!;
      if (original.request.type !== "provider-turn.start" ||
          original.id !== `effect:${original.commandId}:provider-turn.start:${run.id}` || original.status !== "cancelled" ||
          original.attemptCount !== 0 || original.leaseOwner !== null || original.payloadIdentity === undefined) return false;
      const receipt = yield* ports.receipts.getByCommandId(original.commandId);
      const events = yield* ports.sink.readByCommandId({ commandId: original.commandId }).pipe(Stream.runCollect);
      const bornRun = events.find(({ event }) => event.type === "run.created" && event.payload.id === run.id);
      const bornAttempt = events.find(({ event }) => event.type === "run-attempt.created" && event.payload.id === attempt.id);
      const current = yield* ports.outbox.get(original.id);
      return Option.isSome(receipt) && receipt.value.status === "accepted" &&
        receipt.value.threadId === run.threadId && receipt.value.resultSequence === events.at(-1)?.sequence &&
        events.every((row) => row.commandId === original.commandId && row.event.threadId === run.threadId) &&
        bornRun?.event.type === "run.created" && bornRun.event.payload.userMessageId === run.userMessageId &&
        bornRun.event.payload.activeAttemptId === attempt.id && bornRun.event.payload.startedAt === null &&
        bornAttempt?.event.type === "run-attempt.created" && bornAttempt.event.payload.runId === run.id &&
        bornAttempt.event.payload.attemptOrdinal === 1 && bornAttempt.event.payload.startedAt === null &&
        Option.isSome(current) && sameOperationRequest(current.value, original);
    }).pipe(Effect.catchCause(() => Effect.succeed(false)));
}
