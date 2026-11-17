import type { CommandId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import type { CommandReceiptV2 } from "../../orchestration-v2/CommandReceiptStore.ts";
import type { PendingOrchestrationEffectV2 } from "../../orchestration-v2/EffectOutbox.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
import { importedApplicationAttachmentSha256V1 } from "./ImportedApplicationAttachmentInventory.ts";
import * as Choice from "./ImportedHistoryChoice.ts";

interface Commit {
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly commandType: string;
  readonly importedHistory?: Choice.ImportedHistoryContext;
}
type Reserved = Effect.Success<ReturnType<typeof Choice.reserveImportedHistoryChoice>>;
const deny = (code: string) => Effect.fail(new Choice.ImportedHistoryChoiceError({ code }));

// These hooks run in the sink's captured transaction, alongside ordinary acceptance.
export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const tables = makeJonesTablePresence(sql);
  return {
    reserve: Effect.fn("ImportedHistoryCommit.reserve")(function* (input: Commit) {
      const imported = input.importedHistory;
      if (imported === undefined) {
        if (input.commandType === "thread.imported-history.start")
          return yield* deny("private_imported_lineage_required");
        yield* Choice.assertOrdinaryCommandUnreserved(input.commandId, tables).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        );
        return undefined;
      }
      if (
        imported.command.commandId !== input.commandId ||
        imported.command.threadId !== input.threadId ||
        imported.command.type !== input.commandType
      )
        return yield* deny("choice_command_conflict");
      return yield* Choice.reserveImportedHistoryChoice(imported).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
      );
    }),
    replay: Effect.fn("ImportedHistoryCommit.replay")(function* (
      input: Commit,
      choice: Reserved | undefined,
      receipt: CommandReceiptV2,
    ) {
      const imported = input.importedHistory;
      if (imported === undefined) return;
      const outcome = yield* Choice.readImportedHistoryChoice({
        threadId: input.threadId,
        commandId: input.commandId,
        principal: imported.principal,
      }).pipe(Effect.provideService(SqlClient.SqlClient, sql));
      if (
        choice?.duplicate !== true ||
        outcome === null ||
        receipt.commandType !== imported.command.type ||
        receipt.threadId !== input.threadId
      )
        return yield* deny("unknown_receipt");
    }),
    reject: Effect.fn("ImportedHistoryCommit.reject")(function* (input: Commit, choice: Reserved) {
      const imported = input.importedHistory;
      if (imported === undefined || choice.rejectionReason === null)
        return yield* deny("choice_command_conflict");
      yield* Choice.recordImportedHistoryOutcome({
        commandId: input.commandId,
        threadId: input.threadId,
        actorSessionId: imported.principal.sessionId,
        commandDigest: choice.identity.commandDigest,
        deliveryDigest: choice.identity.deliveryDigest,
        reviewedBasis: imported.command.reviewedBasis,
        status: "rejected",
        runId: null,
        effectId: null,
        reason: choice.rejectionReason,
      }).pipe(Effect.provideService(SqlClient.SqlClient, sql));
    }),
    accept: Effect.fn("ImportedHistoryCommit.accept")(function* (
      input: Commit & { readonly effects: readonly PendingOrchestrationEffectV2[] },
      choice: Reserved | undefined,
    ) {
      const imported = input.importedHistory;
      if (imported === undefined) return;
      if (choice === undefined || choice.duplicate) return yield* deny("unbound_choice");
      const starts = input.effects.filter(
        (effect) => effect.request.type === "provider-turn.start",
      );
      const start = starts[0];
      if (
        starts.length !== 1 ||
        start === undefined ||
        input.effects.some((effect) => effect.nativeCreationExecutionReference !== undefined) ||
        start.commandId !== input.commandId ||
        start.threadId !== input.threadId ||
        start.request.type !== "provider-turn.start"
      )
        return yield* deny("start_effect_conflict");
      const runId = start.request.runId;
      const records = yield* projections.getThreadRecords(input.threadId, [
        "runs",
        "messages",
        "providerThreads",
      ]);
      const run = records.runs.find((run) => run.id === runId);
      const delivery = imported.command.delivery;
      const messageId =
        delivery.type === "message" && delivery.command.type === "message.dispatch"
          ? delivery.command.messageId
          : delivery.type === "queued_run"
            ? delivery.messageId
            : null;
      const provider = records.providerThreads.find(
        (provider) => provider.id === run?.providerThreadId,
      );
      if (
        run === undefined ||
        provider === undefined ||
        run.status !== "starting" ||
        run.activeAttemptId === null ||
        run.userMessageId !== messageId ||
        provider.nativeThreadRef !== null ||
        provider.nativeConversationHeadRef !== null ||
        (delivery.type === "queued_run" && delivery.runId !== run.id)
      )
        return yield* deny("fresh_context_binding_required");
      const source = yield* sql`SELECT * FROM orchestration_v2_projection_turn_items
        WHERE thread_id = ${input.threadId} AND run_id IS NULL ORDER BY ordinal, turn_item_id`;
      yield* sql`INSERT INTO jones_imported_history_start_reservations
        (effect_id, command_id, thread_id, run_id, run_attempt_id, provider_thread_id, source_digest)
        VALUES (${start.id}, ${input.commandId}, ${input.threadId}, ${run.id},
          ${run.activeAttemptId}, ${provider.id}, ${importedApplicationAttachmentSha256V1(source)})`;
      yield* Choice.recordImportedHistoryOutcome({
        commandId: input.commandId,
        threadId: input.threadId,
        actorSessionId: imported.principal.sessionId,
        commandDigest: choice.identity.commandDigest,
        deliveryDigest: choice.identity.deliveryDigest,
        reviewedBasis: imported.command.reviewedBasis,
        status: "accepted",
        runId,
        effectId: start.id,
        reason: null,
      }).pipe(Effect.provideService(SqlClient.SqlClient, sql));
    }),
  };
});
