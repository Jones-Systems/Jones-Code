import * as Schema from "effect/Schema";
import { AuthSessionId, CommandId, MessageId, RunId, ThreadId } from "../baseSchemas.ts";
import { OrchestrationV2Command } from "../orchestrationV2.ts";

const reviewedMessage = OrchestrationV2Command.check(
  Schema.makeFilter(
    (command) =>
      command.type === "message.dispatch" && command.dispatchMode.type === "start_immediately",
  ),
);
export const ImportedHistoryDelivery = Schema.Union([
  Schema.Struct({ type: Schema.Literal("message"), command: reviewedMessage }),
  Schema.Struct({ type: Schema.Literal("queued_run"), runId: RunId, messageId: MessageId }),
]);
export type ImportedHistoryDelivery = typeof ImportedHistoryDelivery.Type;
export const ImportedHistoryStart = Schema.Struct({
  type: Schema.Literal("thread.imported-history.start"),
  commandId: CommandId,
  threadId: ThreadId,
  reviewedBasis: Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/)),
  delivery: ImportedHistoryDelivery,
});
export type ImportedHistoryStart = typeof ImportedHistoryStart.Type;
export const ImportedHistoryReview = Schema.Struct({
  status: Schema.Literals(["available", "unavailable"]),
  reviewedBasis: Schema.NullOr(Schema.String),
  reason: Schema.NullOr(Schema.String),
});
export type ImportedHistoryReview = typeof ImportedHistoryReview.Type;
export const ImportedHistoryOutcome = Schema.Struct({
  commandId: CommandId,
  threadId: ThreadId,
  actorSessionId: AuthSessionId,
  commandDigest: Schema.String,
  deliveryDigest: Schema.String,
  reviewedBasis: Schema.String,
  status: Schema.Literals(["accepted", "rejected", "unknown"]),
  runId: Schema.NullOr(RunId),
  effectId: Schema.NullOr(Schema.String),
  reason: Schema.NullOr(Schema.String),
});
export type ImportedHistoryOutcome = typeof ImportedHistoryOutcome.Type;

export class ImportedHistoryUnavailable extends Schema.TaggedError<ImportedHistoryUnavailable>()(
  "ImportedHistoryUnavailable",
  { reason: Schema.String },
) {}
export const ImportedHistoryReviewInput = Schema.Struct({
  threadId: ThreadId,
  delivery: ImportedHistoryDelivery,
});
export const ImportedHistoryObserveInput = Schema.Struct({
  threadId: ThreadId,
  commandId: CommandId,
});
