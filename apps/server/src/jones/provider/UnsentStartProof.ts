import { sameOperationRequest } from "../workspaceLifecycle/NativeOperation.ts";
import {
  CommandId,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import type * as SqlClient from "effect/sql/SqlClient";
import {
  EVENT_LOG_PRUNED_THROUGH_PROJECTION,
  readEventLogPrunedThrough,
} from "../persistence/EventLogPruneBoundary.ts";
import type * as Outbox from "../../orchestration-v2/EffectOutbox.ts";
import type * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import type * as Sink from "../../orchestration-v2/EventSink.ts";
import type * as Refusals from "../startRefusal/Store.ts";
import type { CheckedPreparedCheckout } from "../workspaceLifecycle/PreparedStartCheckout.ts";
import { preparedStartBirth } from "./PreparedStartBirth.ts";

interface UnsentStartPorts {
  readonly outbox: Pick<Outbox.EffectOutboxV2Shape, "listByThreadId" | "get">;
  readonly receipts: Pick<Receipts.CommandReceiptStoreV2Shape, "getByCommandId">;
  readonly sink: Pick<Sink.EventSinkV2Shape, "readByCommandId">;
  readonly refusals: Refusals.StartRefusals["Service"];
}

const targetsRunStart = (row: Outbox.OrchestrationEffectV2, run: OrchestrationV2Run) =>
  (row.request.type === "provider-turn.start" || row.request.type === "provider-turn.restart") &&
  row.request.runId === run.id;

// A cancelled original row with a monotonic zero claim count is positive custody
// evidence. A missing turn, null startedAt, retry row or held control is not.
export function makeUnsentStartProof(ports: UnsentStartPorts) {
  return (run: OrchestrationV2Run, attempts: ReadonlyArray<OrchestrationV2RunAttempt>) =>
    Effect.gen(function* () {
      const attempt = attempts.find(
        (row) => row.id === run.activeAttemptId && row.runId === run.id,
      );
      if (attempt === undefined || run.manualContinuationOfRunId !== undefined) return false;
      const rows = (yield* ports.outbox.listByThreadId(run.threadId)).filter((row) =>
        targetsRunStart(row, run),
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

const resendCommandId = (run: OrchestrationV2Run) =>
  CommandId.make(`command:restart-resend:${run.id}`);
const resendEffectPrefix = (run: OrchestrationV2Run) => `effect:restart-resend:${run.id}:`;
const decodePayloadIdentity = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Unknown));

function hasExactPlainRequest(
  row: Outbox.OrchestrationEffectV2,
  request: Outbox.PendingOrchestrationEffectV2["request"],
) {
  // Typed outbox decoding can omit unknown keys. Check the stored JSON as well.
  const payload = decodePayloadIdentity(row.payloadIdentity);
  return (
    row.nativeCreationExecutionReference === undefined &&
    Option.isSome(payload) &&
    sameOperationRequest(payload.value, request) &&
    sameOperationRequest(row.request, request)
  );
}

export function recoveryResendEffect(
  run: OrchestrationV2Run,
  commandId: CommandId,
): Outbox.PendingOrchestrationEffectV2 {
  return {
    id: `${resendEffectPrefix(run)}${commandId}`,
    commandId: resendCommandId(run),
    threadId: run.threadId,
    request: { type: "provider-turn.start", runId: run.id },
  };
}

class RecoveryResendCommitError extends Schema.TaggedError<RecoveryResendCommitError>()(
  "RecoveryResendCommitError",
  {
    reason: Schema.Literals(["acceptance_missing", "outbox_row_mismatch"]),
  },
) {}

export const commitRecoveryResends = Effect.fn("UnsentStartProof.commitRecoveryResends")(
  (
    sink: Pick<Sink.EventSinkV2Shape, "commitCommand" | "writeWithEffects" | "withProviderBatch">,
    outbox: Pick<Outbox.EffectOutboxV2Shape, "get">,
    input: Parameters<Sink.EventSinkV2Shape["commitCommand"]>[0],
    effects: ReadonlyArray<Outbox.PendingOrchestrationEffectV2>,
  ) =>
    sink.withProviderBatch(
      Effect.gen(function* () {
        const result = yield* sink.commitCommand(input);
        if (!result.committed || result.receipt.status !== "accepted")
          return yield* new RecoveryResendCommitError({ reason: "acceptance_missing" });
        // The command cancels old starts. Enqueue only after cancellation, before the same commit.
        yield* sink.writeWithEffects({ events: [], effects });
        for (const effect of effects) {
          const row = yield* outbox.get(effect.id);
          if (
            Option.isNone(row) ||
            row.value.commandId !== effect.commandId ||
            row.value.threadId !== effect.threadId ||
            row.value.status !== "pending" ||
            row.value.attemptCount !== 0 ||
            row.value.leaseOwner !== null ||
            row.value.completedAt !== null ||
            !hasExactPlainRequest(row.value, effect.request)
          )
            return yield* new RecoveryResendCommitError({ reason: "outbox_row_mismatch" });
        }
        return result;
      }),
    ),
);

// Recovery commits the replacement under its own receipt. Require a one-to-one
// match even for cancelled predecessors so a missing outbox row cannot hide a claim.
export function makeRecoveryUnsentStartProof(sql: SqlClient.SqlClient, ports: UnsentStartPorts) {
  return Effect.fn("UnsentStartProof.recovery")(
    function* (
      run: OrchestrationV2Run,
      attempts: ReadonlyArray<OrchestrationV2RunAttempt>,
      message: OrchestrationV2ConversationMessage,
      checkout?: CheckedPreparedCheckout,
    ) {
      const [attempt, anotherAttempt] = attempts.filter((row) => row.runId === run.id);
      if (
        run.status !== "starting" ||
        run.startedAt !== null ||
        run.completedAt !== null ||
        run.manualContinuationOfRunId !== undefined ||
        run.restartContinuationOfRunId !== undefined ||
        run.usageLimitContinuationOfRunId !== undefined ||
        attempt === undefined ||
        anotherAttempt !== undefined ||
        attempt.id !== run.activeAttemptId ||
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
        message.id !== run.userMessageId ||
        message.threadId !== run.threadId ||
        message.runId !== run.id ||
        message.role !== "user"
      )
        return false;
      const prefix = yield* ports.refusals.readPrefix({
        threadId: run.threadId,
        runId: run.id,
        attemptId: attempt.id,
      });
      if (prefix.state !== "unknown") return false;

      const threadRows = yield* ports.outbox.listByThreadId(run.threadId);
      if (
        threadRows.some(
          ({ request }) =>
            (request.type === "provider-turn.steer" && request.messageId === message.id) ||
            (request.type === "provider-turn.steer-batch" &&
              (request.messageId === message.id || request.messageIds.includes(message.id))),
        )
      )
        return false;
      const commandId = resendCommandId(run);
      const effectPrefix = resendEffectPrefix(run);
      const rows = threadRows.filter(
        (row) =>
          targetsRunStart(row, run) ||
          row.commandId === commandId ||
          row.id.startsWith(effectPrefix),
      );
      const [original, anotherOriginal] = rows.filter((row) => row.commandId !== commandId);
      const request = { type: "provider-turn.start", runId: run.id } as const;
      if (
        original === undefined ||
        anotherOriginal !== undefined ||
        original.id !== `effect:${original.commandId}:provider-turn.start:${run.id}` ||
        rows.some(
          (row) =>
            row.threadId !== run.threadId ||
            row.status !== "cancelled" ||
            row.attemptCount !== 0 ||
            row.leaseOwner !== null ||
            row.completedAt === null ||
            !hasExactPlainRequest(row, request),
        )
      )
        return false;

      const prunedThrough = yield* readEventLogPrunedThrough(sql);
      // Both the original admission and each replacement need a retained receipt and event batch.
      const readRetainedAcceptance = Effect.fnUntraced(function* (
        commandId: CommandId,
        parentId?: string,
      ) {
        const result = yield* ports.receipts.getByCommandId(commandId);
        if (Option.isNone(result)) return;
        const receipt = result.value;
        if (
          receipt.status !== "accepted" ||
          receipt.error !== null ||
          receipt.commandId !== commandId ||
          receipt.threadId !== (parentId ?? run.threadId) ||
          (parentId !== undefined && receipt.commandType !== "delegated_task.request")
        )
          return;
        const events = yield* ports.sink.readByCommandId({ commandId }).pipe(Stream.runCollect);
        const first = events[0];
        if (
          first === undefined ||
          first.sequence <= prunedThrough ||
          events.at(-1)?.sequence !== receipt.resultSequence ||
          events.some(
            (row, index) =>
              row.sequence !== first.sequence + index ||
              row.commandId !== commandId ||
              (row.event.threadId !== run.threadId && row.event.threadId !== parentId),
          )
        )
          return;
        return {
          receipt,
          events:
            parentId === undefined
              ? events
              : events.filter(({ event }) => event.threadId === run.threadId),
          firstSequence: first.sequence,
        };
      });
      const sameRun = (event: OrchestrationV2DomainEvent, expected = run) =>
        event.runId === run.id &&
        event.providerInstanceId === run.providerInstanceId &&
        sameOperationRequest(event.payload, expected);

      const accepted = yield* readRetainedAcceptance(original.commandId);
      if (accepted === undefined) return false;
      const [witness, anotherRunEvent] = accepted.events.filter(
        ({ event }) =>
          (event.type === "run.created" || event.type === "run.updated") &&
          (event.runId === run.id || event.payload.id === run.id),
      );
      if (
        witness === undefined ||
        anotherRunEvent !== undefined ||
        !sameRun(witness.event) ||
        accepted.events.some(
          ({ event }) =>
            event.type === "provider-turn.updated" &&
            (event.runId === run.id || event.payload.runAttemptId === attempt.id),
        )
      )
        return false;
      const { receipt } = accepted;
      const prepared = receipt.commandType === "prepared-run.release";
      const scopeEvent = prepared ? accepted.events[0]?.event : undefined;
      const scopeId =
        scopeEvent?.type === "checkpoint-scope.created" ? scopeEvent.payload.id : null;
      const preparedActivity = sql`
        json_extract(metadata_json, '$.runId') = ${run.id}
        OR json_extract(metadata_json, '$.nodeId') = ${run.rootNodeId}
        OR json_extract(payload_json, '$.runId') = ${run.id}
        OR json_extract(payload_json, '$.targetRunId') = ${run.id}
        OR json_extract(payload_json, '$.runAttemptId') = ${attempt.id}
        OR json_extract(payload_json, '$.messageId') = ${message.id}
        OR json_extract(payload_json, '$.id') IN
          (${run.id}, ${attempt.id}, ${run.rootNodeId}, ${message.id}, ${run.providerThreadId})
        OR json_extract(payload_json, '$.nodeId') = ${run.rootNodeId}
        OR json_extract(payload_json, '$.scopeId') = ${scopeId}
      `;
      if (receipt.commandType === "queue.start") {
        const inputs = accepted.events.flatMap(({ event }) =>
          event.type === "turn-item.updated" &&
          event.payload.type === "user_message" &&
          event.payload.messageId === message.id
            ? [event.payload]
            : [],
        );
        const input = inputs[0];
        if (
          witness.event.type !== "run.updated" ||
          witness.sequence !== receipt.resultSequence ||
          inputs.length !== 1 ||
          input === undefined ||
          input.threadId !== run.threadId ||
          input.runId !== run.id ||
          input.createdBy !== message.createdBy ||
          input.creationSource !== message.creationSource ||
          input.scheduledTaskId !== message.scheduledTaskId ||
          input.text !== message.text ||
          !sameOperationRequest(input.attachments, message.attachments) ||
          !sameOperationRequest(input.context, message.context) ||
          !(yield* readUnreleasedStartBirth(
            sql,
            { ...run, status: "queued" },
            attempt,
            accepted.firstSequence,
          ))
        )
          return false;
      } else {
        const birth = prepared
          ? yield* Effect.gen(function* () {
              if (checkout === undefined || !(yield* checkout.matches(original.commandId))) return;
              const candidates = yield* preparedStartBirth(sql, {
                threadId: sql`${run.threadId}`,
                runId: sql`${run.id}`,
                releaseCommandId: sql`${original.commandId}`,
              });
              const candidate = candidates[0];
              if (candidates.length === 1 && candidate?.release_sequence === receipt.resultSequence)
                return yield* readRetainedAcceptance(
                  CommandId.make(candidate.command_id),
                  candidate.parent_id ?? undefined,
                );
            })
          : accepted;
        if (
          birth === undefined ||
          (birth.receipt.commandType !== "message.dispatch" &&
            birth.receipt.commandType !== "thread.create" &&
            !(prepared && birth.receipt.commandType === "delegated_task.request")) ||
          (prepared && birth.receipt.resultSequence >= accepted.firstSequence)
        )
          return false;
        const [born, anotherBirth] = birth.events.filter(
          ({ event }) =>
            (event.type === "run.created" || event.type === "run.updated") &&
            (event.runId === run.id || event.payload.id === run.id),
        );
        const births = birth.events.filter(
          ({ event }) =>
            (event.type === "run-attempt.created" || event.type === "run-attempt.updated") &&
            (event.runId === run.id ||
              event.payload.runId === run.id ||
              event.payload.id === attempt.id),
        );
        const messages = birth.events.flatMap(({ event }) =>
          event.type === "message.updated" && event.payload.id === message.id
            ? [event.payload]
            : [],
        );
        if (
          anotherBirth !== undefined ||
          born?.event.type !== "run.created" ||
          !sameRun(born.event, prepared ? { ...run, status: "preparing" } : run) ||
          births.length !== 1 ||
          births[0]?.event.type !== "run-attempt.created" ||
          births[0].event.runId !== run.id ||
          !sameOperationRequest(births[0].event.payload, attempt) ||
          messages.length !== 1 ||
          !sameOperationRequest(messages[0], message)
        )
          return false;

        if (prepared) {
          // The release resolves workspace preparation. It does not accept new
          // input and must never act as a later birth for a parent Stop fence.
          const strategy = run.workspacePreparation;
          const [, root, ready, start] = accepted.events.map(({ event }) => event);
          if (
            // An auto conversation can resolve its cwd outside the thread's checkout binding.
            strategy === undefined ||
            strategy.type === "auto" ||
            run.checkpointId !== null ||
            message.nodeId !== run.rootNodeId ||
            scopeEvent?.type !== "checkpoint-scope.created" ||
            accepted.events.length !== 4 ||
            start?.type !== "run.updated" ||
            root?.type !== "node.updated" ||
            ready?.type !== "turn-item.updated" ||
            ready.payload.type !== "command_execution" ||
            !accepted.events.every(
              ({ event }) =>
                event.runId === run.id &&
                event.nodeId === run.rootNodeId &&
                event.providerInstanceId === run.providerInstanceId,
            )
          )
            return false;
          const scope = scopeEvent.payload;
          const [preparation, anotherPreparation] = birth.events.flatMap(({ event }) =>
            event.type === "turn-item.updated" &&
            event.payload.type === "command_execution" &&
            event.payload.input === "Preparing workspace"
              ? [event.payload]
              : [],
          );
          const [rootBirth, anotherRoot] = birth.events.flatMap(({ event }) =>
            event.type === "node.updated" && event.payload.runId === run.id ? [event.payload] : [],
          );
          const [provider, anotherProvider] = birth.events.flatMap(({ event }) =>
            event.type === "provider-thread.updated" && event.payload.id === run.providerThreadId
              ? [event.payload]
              : [],
          );
          const [input, anotherInput] = birth.events.flatMap(({ event }) =>
            event.type === "turn-item.updated" &&
            event.payload.type === "user_message" &&
            event.payload.messageId === message.id
              ? [event.payload]
              : [],
          );
          if (
            provider === undefined ||
            anotherProvider !== undefined ||
            provider.appThreadId !== run.threadId ||
            provider.providerInstanceId !== run.providerInstanceId ||
            provider.nativeThreadRef !== null ||
            provider.nativeConversationHeadRef !== null ||
            provider.runtimeIdentity != null ||
            provider.status !== "not_loaded" ||
            birth.events.some(
              ({ event }) =>
                event.type === "provider-turn.updated" ||
                event.type === "checkpoint-scope.created" ||
                event.type === "checkpoint.captured",
            ) ||
            rootBirth?.id !== run.rootNodeId ||
            anotherRoot !== undefined ||
            rootBirth?.status !== "pending" ||
            rootBirth.kind !== "root_turn" ||
            rootBirth.providerThreadId !== run.providerThreadId ||
            rootBirth.checkpointScopeId !== null ||
            rootBirth.providerTurnId !== null ||
            rootBirth.nativeItemRef !== null ||
            rootBirth.runtimeRequestId !== null ||
            rootBirth.startedAt !== null ||
            rootBirth.completedAt !== null ||
            !sameOperationRequest(root.payload, { ...rootBirth, checkpointScopeId: scope.id }) ||
            scope.threadId !== run.threadId ||
            scope.runId !== run.id ||
            scope.nodeId !== run.rootNodeId ||
            scope.providerThreadId !== run.providerThreadId ||
            scope.kind !== "root_run" ||
            scope.parentScopeId !== null ||
            scope.ordinalWithinParent !== 0 ||
            !scope.advancesAppRunCount ||
            input === undefined ||
            anotherInput !== undefined ||
            input.threadId !== run.threadId ||
            input.runId !== run.id ||
            input.nodeId !== run.rootNodeId ||
            input.providerThreadId !== run.providerThreadId ||
            input.providerTurnId !== null ||
            input.nativeItemRef !== null ||
            input.inputIntent !== "turn_start" ||
            input.createdBy !== message.createdBy ||
            input.creationSource !== message.creationSource ||
            input.scheduledTaskId !== message.scheduledTaskId ||
            input.text !== message.text ||
            !sameOperationRequest(input.attachments, message.attachments) ||
            !sameOperationRequest(input.context, message.context) ||
            preparation === undefined ||
            anotherPreparation !== undefined ||
            preparation.threadId !== run.threadId ||
            preparation.runId !== run.id ||
            preparation.nodeId !== run.rootNodeId ||
            preparation.providerThreadId !== run.providerThreadId ||
            preparation.providerTurnId !== null ||
            preparation.nativeItemRef !== null ||
            preparation.status !== "running" ||
            preparation.completedAt !== null ||
            !sameOperationRequest(ready.payload, {
              ...preparation,
              status: "completed",
              title: "Workspace ready",
              output: "Workspace preparation completed.",
              exitCode: 0,
              completedAt: ready.occurredAt,
              updatedAt: ready.occurredAt,
            }) ||
            threadRows.some(
              ({ request }) =>
                request.type !== "provider-turn.start" &&
                "runId" in request &&
                request.runId === run.id,
            )
          )
            return false;

          // The ordinary launch may record worktree and setup progress, both
          // updates to the same local preparation item. Any other run activity
          // (including a retry of a failed preparation) breaks the proof.
          const progress = yield* sql<{
            readonly sequence: number;
            readonly command_id: string | null;
          }>`
            SELECT sequence, command_id FROM orchestration_events
            WHERE application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ${run.threadId}
              AND sequence > ${birth.receipt.resultSequence} AND sequence < ${accepted.firstSequence}
              AND (${preparedActivity})
            ORDER BY sequence ASC LIMIT 3
          `;
          if (progress.length > 2) return false;
          const phases = new Set(["Preparing worktree", "Starting setup script"]);
          for (const { command_id, sequence } of progress) {
            if (command_id === null) return false;
            const recorded = yield* readRetainedAcceptance(CommandId.make(command_id));
            const update = recorded?.events[0]?.event;
            if (
              recorded?.receipt.commandType !== "prepared-run.progress" ||
              recorded.events.length !== 1 ||
              recorded.firstSequence !== sequence ||
              update?.type !== "turn-item.updated" ||
              update.runId !== run.id ||
              update.nodeId !== run.rootNodeId ||
              update.providerInstanceId !== run.providerInstanceId ||
              update.payload.type !== "command_execution" ||
              update.payload.title === null ||
              !phases.delete(update.payload.title) ||
              !sameOperationRequest(update.payload, {
                ...preparation,
                title: update.payload.title,
                updatedAt: update.occurredAt,
              })
            )
              return false;
          }
        }
      }

      const history = yield* sql<{
        readonly sequence: number;
        readonly command_id: string | null;
        readonly event_type: string;
      }>`
        SELECT sequence, command_id, event_type FROM orchestration_events
        WHERE application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ${run.threadId}
          AND sequence > ${receipt.resultSequence}
          AND (${
            prepared
              ? preparedActivity
              : sql`
            (event_type IN ('run.created', 'run.updated', 'run-attempt.created', 'run-attempt.updated', 'provider-turn.updated')
              AND (json_extract(metadata_json, '$.runId') = ${run.id}
                OR json_extract(payload_json, '$.runId') = ${run.id}
                OR json_extract(payload_json, '$.id') IN (${run.id}, ${attempt.id})
                OR json_extract(payload_json, '$.runAttemptId') = ${attempt.id}))
            OR (event_type = 'message.updated' AND json_extract(payload_json, '$.id') = ${message.id})
          `
          })
        ORDER BY sequence ASC LIMIT ${rows.length}
      `;
      if (history.length !== rows.length - 1) return false;
      const replacements = new Set(rows.filter((row) => row !== original).map((row) => row.id));
      for (const { sequence, command_id, event_type } of history) {
        if (event_type !== "run.updated" || command_id === null) return false;
        const id = CommandId.make(command_id);
        if (!replacements.delete(`${effectPrefix}${id}`)) return false;
        const recorded = yield* readRetainedAcceptance(id);
        if (recorded?.receipt.commandType !== "provider-runtime.reconcile") return false;
        const suffix = `:${run.threadId}:${DateTime.formatIso(recorded.receipt.acceptedAt)}`;
        if (
          id !== `command:runtime-reconcile:startup${suffix}` &&
          id !== `command:runtime-reconcile:shutdown${suffix}`
        )
          return false;
        const update = recorded.events.find((event) => event.sequence === sequence)?.event;
        if (
          update?.type !== "run.updated" ||
          !sameRun(update) ||
          !DateTime.Equivalence(update.occurredAt, recorded.receipt.acceptedAt)
        )
          return false;
      }
      return true;
    },
    Effect.catchCauseIf(
      (cause) => !Cause.hasInterruptsOnly(cause),
      () => Effect.succeed(false),
    ),
  );
}

// Deferred preparation and queued messages have no start effect until release or
// promotion. When proving a promotion, bound the birth evidence to its prior history.
export function readUnreleasedStartBirth(
  sql: SqlClient.SqlClient,
  run: OrchestrationV2Run,
  attempt: OrchestrationV2RunAttempt,
  beforeSequence?: number,
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
      AND ${
        beforeSequence === undefined
          ? sql`1 = 1`
          : sql`receipt.result_sequence < ${beforeSequence} AND receipt.error IS NULL AND EXISTS (
              SELECT 1 FROM orchestration_events AS tail
              WHERE tail.sequence = receipt.result_sequence AND tail.command_id = birth.command_id
                AND tail.aggregate_kind = 'thread' AND tail.stream_id = birth.stream_id
                AND tail.application_event_version = 2
            )`
      }
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
          AND ${beforeSequence === undefined ? sql`1 = 1` : sql`change.sequence < ${beforeSequence}`}
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
