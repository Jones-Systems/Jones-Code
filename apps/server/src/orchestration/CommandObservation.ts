import * as NativeCreationRepositoryLayer from "../persistence/Layers/NativeCreationRepository.ts";
import type { NativeCreationObservation } from "@t3tools/contracts";
import {
  CommandId,
  MessageId,
  NonNegativeInt,
  OrchestrationCommandObservation,
  OrchestrationObservedTurn,
  ThreadId,
  type OrchestrationDispatchTarget,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { toPersistenceDecodeError, toPersistenceSqlError } from "../persistence/Errors.ts";
import { OrchestrationCommandReceipt } from "../persistence/Services/OrchestrationCommandReceipts.ts";
import { ProjectionSnapshotQuery } from "./Services/ProjectionSnapshotQuery.ts";

const queryError = (operation: string) => (cause: unknown) =>
  Schema.isSchemaError(cause)
    ? toPersistenceDecodeError(`${operation}:decode`)(cause)
    : toPersistenceSqlError(`${operation}:query`)(cause);
const TargetActivity = Schema.Struct({
  pending: NonNegativeInt,
  running: NonNegativeInt,
  lastEventSequence: NonNegativeInt,
});
const CommandEvent = Schema.Struct({
  sequence: NonNegativeInt,
  aggregateKind: Schema.String,
  aggregateId: Schema.String,
  eventType: Schema.String,
  messageId: Schema.NullOr(MessageId),
  threadId: Schema.NullOr(ThreadId),
});

export const makeCommandObservationQuery = Effect.fn("makeCommandObservationQuery")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const nativeCreationRepository = yield* NativeCreationRepositoryLayer.make;
  const snapshots = yield* ProjectionSnapshotQuery;
  const readTarget = Effect.fn("CommandObservation.readTarget")(function* (threadId: ThreadId) {
    const { snapshotSequence } = yield* snapshots.getSnapshotSequence();
    const shell = yield* snapshots.getThreadShellById(threadId);
    const rows = yield* sql`
      SELECT
        (SELECT COUNT(*) FROM projection_turns WHERE thread_id = ${threadId} AND state = 'pending') AS pending,
        (SELECT COUNT(*) FROM projection_turns WHERE thread_id = ${threadId} AND state = 'running') AS running,
        COALESCE((SELECT MAX(sequence) FROM orchestration_events
          WHERE aggregate_kind = 'thread' AND stream_id = ${threadId}), 0) AS "lastEventSequence"
    `;
    const activity = yield* Schema.decodeUnknownEffect(TargetActivity)(rows[0]);
    if (Option.isNone(shell))
      return { snapshotSequence, lastEventSequence: activity.lastEventSequence, target: null };
    const thread = shell.value;
    const blockers: Array<OrchestrationDispatchTarget["blockers"][number]> = [];
    if (thread.archivedAt !== null) blockers.push("archived");
    if (thread.settledOverride === "settled") blockers.push("settled");
    if (activity.pending > 0) blockers.push("pending_turn");
    if (activity.running > 0) blockers.push("running_turn");
    if (thread.session?.status === "starting") blockers.push("session_starting");
    if (thread.session?.status === "running") blockers.push("session_running");
    if (thread.session?.activeTurnId != null) blockers.push("active_turn");
    if (thread.hasPendingApprovals) blockers.push("pending_approval");
    if (thread.hasPendingUserInput) blockers.push("pending_user_input");
    if (thread.hasActionableProposedPlan) blockers.push("actionable_plan");
    if (thread.backgroundLiveness != null) blockers.push("background_work");
    const target: OrchestrationDispatchTarget = {
      modelSelection: thread.modelSelection,
      sessionStatus: thread.session?.status ?? null,
      activeTurnId: thread.session?.activeTurnId ?? null,
      latestTurnId: thread.latestTurn?.turnId ?? null,
      requireIdle: true,
      idle: blockers.length === 0,
      blockers,
    };
    return { snapshotSequence, lastEventSequence: activity.lastEventSequence, target };
  });
  const getTarget = Effect.fn("CommandObservation.getTarget")(function* (threadId: ThreadId) {
    return yield* sql
      .withTransaction(readTarget(threadId))
      .pipe(Effect.mapError(queryError("CommandObservation.target")));
  });
  const observe = Effect.fn("CommandObservation.observe")(function* (input: {
    readonly threadId: ThreadId;
    readonly commandId: CommandId;
    readonly messageId: MessageId;
  }) {
    return yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const { snapshotSequence, target } = yield* readTarget(input.threadId);
          const receipts = yield* sql`
        SELECT command_id AS "commandId", aggregate_kind AS "aggregateKind", aggregate_id AS "aggregateId",
          accepted_at AS "acceptedAt", result_sequence AS "resultSequence", status, error
        FROM orchestration_command_receipts WHERE command_id = ${input.commandId}
      `.pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(OrchestrationCommandReceipt))));
          const events = yield* sql`
        SELECT sequence, aggregate_kind AS "aggregateKind", stream_id AS "aggregateId", event_type AS "eventType",
          json_extract(payload_json, '$.messageId') AS "messageId", json_extract(payload_json, '$.threadId') AS "threadId"
        FROM orchestration_events WHERE command_id = ${input.commandId} ORDER BY sequence
      `.pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(CommandEvent))));
          const turns = yield* sql`
        SELECT turn_id AS "turnId", state, requested_at AS "requestedAt", started_at AS "startedAt",
          completed_at AS "completedAt", assistant_message_id AS "assistantMessageId"
        FROM projection_turns WHERE thread_id = ${input.threadId} AND pending_message_id = ${input.messageId}
        LIMIT 2
      `.pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(OrchestrationObservedTurn))));
          const receipt = receipts[0];
          const starts = events.filter(
            (event) => event.eventType === "thread.turn-start-requested",
          );
          let correlation: OrchestrationCommandObservation["correlation"] = "missing";
          let turn: OrchestrationCommandObservation["turn"] = null;
          if (
            (receipt !== undefined &&
              (receipt.aggregateKind !== "thread" || receipt.aggregateId !== input.threadId)) ||
            events.some(
              (event) => event.aggregateKind !== "thread" || event.aggregateId !== input.threadId,
            ) ||
            starts.some(
              (event) => event.threadId !== input.threadId || event.messageId !== input.messageId,
            )
          ) {
            correlation = "mismatched";
          } else if (starts.length > 1 || turns.length > 1) {
            correlation = "ambiguous";
          } else if (
            receipt?.status === "accepted" &&
            starts.length === 1 &&
            events.some((event) => event.sequence === receipt.resultSequence)
          ) {
            const candidate = turns[0];
            if (receipt.resultSequence <= snapshotSequence && candidate !== undefined) {
              turn = candidate;
              correlation = candidate.turnId === null ? "pending" : "exact";
            } else {
              correlation = "pending";
            }
          }
          const history = yield* nativeCreationRepository.readHistory(input.commandId);
          let creation: NativeCreationObservation | undefined;
          if (Option.isSome(history) && history.value.normalizedCommandDigest !== null) {
            const { intent, effects, normalizedCommandDigest } = history.value;
            const created = effects.find(
              (fact) =>
                fact.kind === "native_command" &&
                fact.phase === "completed" &&
                fact.commandType === "thread.create",
            );
            const incarnation =
              created?.kind === "native_command" && created.phase === "completed"
                ? { eventId: created.eventId, sequence: created.sequence }
                : null;
            const unresolvedEffects = effects
              .filter(
                (fact) =>
                  fact.phase === "started" &&
                  !effects.some(
                    (end) =>
                      end.effectId === fact.effectId &&
                      end.phase === "completed" &&
                      (!("result" in end) || end.result === "succeeded"),
                  ),
              )
              .map((fact) => fact.effectId);
            const finalAccepted = effects.some(
              (fact) =>
                fact.kind === "native_command" &&
                fact.phase === "completed" &&
                fact.commandType === "thread.turn.start" &&
                fact.commandId === intent.commandId,
            );
            const requiredCommandTypes = [
              "thread.create",
              "thread.message.user.append",
              "thread.meta.update",
              "thread.turn.start",
            ] as const;
            const commandChain = requiredCommandTypes.every((type) =>
              effects.some(
                (fact) =>
                  fact.kind === "native_command" &&
                  fact.phase === "completed" &&
                  fact.commandType === type,
              ),
            );
            const requiredLifecycle = [
              "normalization",
              "tracker_registration",
              "bootstrap_detachment",
            ] as const;
            const lifecycleChain = requiredLifecycle.every((action) =>
              effects.some(
                (fact) =>
                  fact.kind === "lifecycle" &&
                  fact.phase === "completed" &&
                  fact.action === action &&
                  fact.result === "succeeded",
              ),
            );
            const checkoutComplete = effects.some(
              (fact) =>
                fact.kind === "worktree" &&
                fact.phase === "completed" &&
                fact.result === "succeeded",
            );
            const cleanupOccurred = effects.some((fact) => fact.kind === "cleanup");
            const setupComplete =
              !intent.binding.runSetupScript ||
              effects.some(
                (fact) =>
                  fact.kind === "setup" &&
                  fact.phase === "completed" &&
                  fact.result === "succeeded",
              );
            const exact =
              intent.threadId === input.threadId && intent.messageId === input.messageId;
            const currentIncarnations = yield* sql<{
              eventId: string;
              sequence: number;
            }>`SELECT event_id AS "eventId", sequence FROM orchestration_events WHERE stream_id = ${input.threadId} AND event_type = 'thread.created' ORDER BY sequence DESC LIMIT 1`;
            const current = currentIncarnations[0];
            let commandFactsConsistent = true;
            for (const fact of effects) {
              if (fact.kind !== "native_command" || fact.phase !== "completed") continue;
              const attested = yield* sql`SELECT e.event_id FROM orchestration_events e
                JOIN orchestration_command_receipts r ON r.command_id = e.command_id
                WHERE e.event_id = ${fact.eventId} AND e.sequence = ${fact.sequence}
                  AND e.command_id = ${fact.commandId} AND e.stream_id = ${fact.threadId}
                  AND r.status = 'accepted' AND r.result_sequence = ${fact.sequence}`;
              if (attested.length !== 1) commandFactsConsistent = false;
            }
            const inconsistent =
              !commandFactsConsistent ||
              !exact ||
              (incarnation !== null &&
                (current?.eventId !== incarnation.eventId ||
                  current.sequence !== incarnation.sequence));
            creation = {
              schema: "t3.native-creation-observation/v1",
              preparationId: intent.preparationId,
              operationId: intent.operationId,
              preparationSha256: intent.preparationSha256,
              bindingDigest: intent.bindingDigest,
              promptDigest: intent.promptDigest,
              commandDigest: intent.commandDigest,
              normalizedCommandDigest,
              claimId: intent.claimId,
              claimedBootId: intent.claimedBootId,
              claimedAt: intent.claimedAt,
              actorSessionId: intent.actorSessionId,
              grantId: intent.grantId,
              grantRevision: intent.grantRevision,
              binding: intent.binding,
              incarnation,
              effects,
              unresolvedEffects,
              outcome:
                inconsistent || unresolvedEffects.length > 0
                  ? "unknown"
                  : incarnation !== null &&
                      finalAccepted &&
                      commandChain &&
                      lifecycleChain &&
                      checkoutComplete &&
                      setupComplete &&
                      !cleanupOccurred
                    ? "complete"
                    : cleanupOccurred
                      ? "incomplete"
                      : "in_progress",
            };
          }
          return {
            ...(creation === undefined ? {} : { creation }),
            ...input,
            snapshotSequence,
            commandStatus: receipt?.status ?? "not_found",
            acceptedSequence: receipt?.status === "accepted" ? receipt.resultSequence : null,
            correlation,
            turn,
            target,
          } satisfies OrchestrationCommandObservation;
        }),
      )
      .pipe(Effect.mapError(queryError("CommandObservation.observe")));
  });
  return { getTarget, observe };
});
