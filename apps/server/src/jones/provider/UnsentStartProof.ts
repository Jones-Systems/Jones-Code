import { sameOperationRequest } from "../workspaceLifecycle/NativeOperation.ts";
import type { OrchestrationV2Run, OrchestrationV2RunAttempt } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import type * as SqlClient from "effect/sql/SqlClient";
import { EVENT_LOG_PRUNED_THROUGH_PROJECTION } from "../persistence/EventLogPruneBoundary.ts";
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
      const attempt = attempts.find(
        (row) => row.id === run.activeAttemptId && row.runId === run.id,
      );
      if (attempt === undefined || run.manualContinuationOfRunId !== undefined) return false;
      const rows = (yield* ports.outbox.listByThreadId(run.threadId)).filter(
        (row) =>
          (row.request.type === "provider-turn.start" ||
            row.request.type === "provider-turn.restart") &&
          row.request.runId === run.id,
      );
      if (
        rows.length === 0 ||
        rows.some((row) => row.nativeCreationExecutionReference !== undefined)
      )
        return false;
      const prefix = yield* ports.refusals.readPrefix({
        threadId: run.threadId,
        runId: run.id,
        attemptId: attempt.id,
      });
      if (prefix.state === "refused") return rows.some((row) => row.commandId === prefix.commandId);

      if (
        prefix.state !== "unknown" ||
        run.startedAt !== null ||
        attempt.reason !== "initial" ||
        attempt.attemptOrdinal !== 1 ||
        attempts.filter((row) => row.runId === run.id).length !== 1 ||
        rows.length !== 1
      )
        return false;
      const original = rows[0]!;
      if (
        original.request.type !== "provider-turn.start" ||
        original.id !== `effect:${original.commandId}:provider-turn.start:${run.id}` ||
        original.status !== "cancelled" ||
        original.attemptCount !== 0 ||
        original.leaseOwner !== null ||
        original.payloadIdentity === undefined
      )
        return false;
      const receipt = yield* ports.receipts.getByCommandId(original.commandId);
      const events = yield* ports.sink
        .readByCommandId({ commandId: original.commandId })
        .pipe(Stream.runCollect);
      const bornRun = events.find(
        ({ event }) => event.type === "run.created" && event.payload.id === run.id,
      );
      const bornAttempt = events.find(
        ({ event }) => event.type === "run-attempt.created" && event.payload.id === attempt.id,
      );
      const current = yield* ports.outbox.get(original.id);
      if (
        Option.isNone(receipt) ||
        receipt.value.status !== "accepted" ||
        receipt.value.threadId !== run.threadId ||
        receipt.value.resultSequence !== events.at(-1)?.sequence ||
        events.some(
          (row) => row.commandId !== original.commandId || row.event.threadId !== run.threadId,
        ) ||
        Option.isNone(current) ||
        !sameOperationRequest(current.value, original)
      )
        return false;
      if (receipt.value.commandType === "queue.start") {
        const promotion = events.at(-1)?.event;
        return (
          promotion?.type === "run.updated" &&
          promotion.runId === run.id &&
          promotion.payload.id === run.id &&
          promotion.payload.status === "starting" &&
          promotion.payload.activeAttemptId === attempt.id &&
          promotion.payload.userMessageId === run.userMessageId &&
          promotion.payload.startedAt === null &&
          attempt.startedAt === null
        );
      }
      return (
        bornRun?.event.type === "run.created" &&
        bornRun.event.payload.userMessageId === run.userMessageId &&
        bornRun.event.payload.activeAttemptId === attempt.id &&
        bornRun.event.payload.startedAt === null &&
        bornAttempt?.event.type === "run-attempt.created" &&
        bornAttempt.event.payload.runId === run.id &&
        bornAttempt.event.payload.attemptOrdinal === 1 &&
        bornAttempt.event.payload.startedAt === null
      );
    }).pipe(Effect.catchCause(() => Effect.succeed(false)));
}

// Deferred preparation and queued messages have no start effect until release or
// promotion. Require a complete retained record of their original admission.
export function readUnreleasedStartBirth(
  sql: SqlClient.SqlClient,
  run: OrchestrationV2Run,
  attempt: OrchestrationV2RunAttempt,
) {
  return sql<{ readonly sequence: number }>`
    SELECT birth.sequence
    FROM orchestration_events AS birth
    JOIN orchestration_command_receipts AS receipt ON receipt.command_id = birth.command_id
    WHERE birth.application_event_version = 2
      AND birth.aggregate_kind = 'thread' AND birth.stream_id = ${run.threadId}
      AND birth.event_type = 'run.created' AND json_extract(birth.payload_json, '$.id') = ${run.id}
      AND birth.sequence > COALESCE((
        SELECT last_sequence FROM orchestration_v2_projection_metadata
        WHERE projection_name = ${EVENT_LOG_PRUNED_THROUGH_PROJECTION}
      ), 0)
      AND json_extract(birth.payload_json, '$.status') = ${run.status}
      AND json_extract(birth.payload_json, '$.activeAttemptId') = ${attempt.id}
      AND json_extract(birth.payload_json, '$.rootNodeId') = ${run.rootNodeId}
      AND json_extract(birth.payload_json, '$.userMessageId') = ${run.userMessageId}
      AND json_type(birth.payload_json, '$.startedAt') = 'null'
      AND json_type(birth.payload_json, '$.completedAt') = 'null'
      AND receipt.aggregate_kind = 'thread' AND receipt.aggregate_id = ${run.threadId}
      AND receipt.status = 'accepted' AND receipt.command_type = 'message.dispatch'
      AND receipt.result_sequence >= birth.sequence
      AND EXISTS (
        SELECT 1 FROM orchestration_events AS first_attempt
        WHERE first_attempt.command_id = birth.command_id
          AND first_attempt.aggregate_kind = 'thread' AND first_attempt.stream_id = birth.stream_id
          AND first_attempt.application_event_version = 2
          AND first_attempt.event_type = 'run-attempt.created'
          AND first_attempt.sequence BETWEEN birth.sequence AND receipt.result_sequence
          AND json_extract(first_attempt.payload_json, '$.id') = ${attempt.id}
          AND json_extract(first_attempt.payload_json, '$.runId') = ${run.id}
          AND json_extract(first_attempt.payload_json, '$.rootNodeId') = ${run.rootNodeId}
          AND json_extract(first_attempt.payload_json, '$.attemptOrdinal') = 1
          AND json_extract(first_attempt.payload_json, '$.reason') = 'initial'
          AND json_extract(first_attempt.payload_json, '$.status') = 'pending'
          AND json_type(first_attempt.payload_json, '$.startedAt') = 'null'
      )
      AND NOT EXISTS (
        SELECT 1 FROM orchestration_events AS change
        WHERE change.aggregate_kind = 'thread' AND change.stream_id = birth.stream_id
          AND change.application_event_version = 2 AND change.sequence > birth.sequence
          AND change.event_type IN ('run.created', 'run.updated')
          AND json_extract(change.payload_json, '$.id') = ${run.id}
          AND (change.event_type = 'run.created'
            OR json_extract(change.payload_json, '$.status') IS NOT ${run.status}
            OR json_extract(change.payload_json, '$.activeAttemptId') IS NOT ${attempt.id}
            OR json_type(change.payload_json, '$.startedAt') IS NOT 'null'
            OR json_type(change.payload_json, '$.completedAt') IS NOT 'null')
      )
    LIMIT 2
  `.pipe(Effect.map((rows) => rows.length === 1));
}

export function makeUnreleasedStartProof(ports: {
  readonly outbox: Pick<Outbox.EffectOutboxV2Shape, "listByThreadId">;
  readonly refusals: Refusals.StartRefusals["Service"];
  readonly readBirth: (
    run: OrchestrationV2Run,
    attempt: OrchestrationV2RunAttempt,
  ) => Effect.Effect<boolean, Sink.EventSinkV2Error>;
}) {
  return (run: OrchestrationV2Run, attempts: ReadonlyArray<OrchestrationV2RunAttempt>) =>
    Effect.gen(function* () {
      if (
        (run.status !== "preparing" && run.status !== "queued") ||
        run.startedAt !== null ||
        run.completedAt !== null ||
        run.manualContinuationOfRunId !== undefined ||
        run.restartContinuationOfRunId !== undefined ||
        run.usageLimitContinuationOfRunId !== undefined
      )
        return false;
      const runAttempts = attempts.filter((attempt) => attempt.runId === run.id);
      const attempt = runAttempts[0];
      if (
        runAttempts.length !== 1 ||
        attempt === undefined ||
        attempt.id !== run.activeAttemptId ||
        attempt.status !== "pending" ||
        attempt.reason !== "initial" ||
        attempt.attemptOrdinal !== 1 ||
        attempt.startedAt !== null ||
        attempt.completedAt !== null
      )
        return false;
      const prefix = yield* ports.refusals.readPrefix({
        threadId: run.threadId,
        runId: run.id,
        attemptId: attempt.id,
      });
      if (prefix.state !== "unknown") return false;
      if (!(yield* ports.readBirth(run, attempt))) return false;
      const rows = yield* ports.outbox.listByThreadId(run.threadId);
      return !rows.some(
        ({ request }) =>
          (request.type === "provider-turn.start" || request.type === "provider-turn.restart") &&
          request.runId === run.id,
      );
    }).pipe(Effect.catchCause(() => Effect.succeed(false)));
}
