import * as Schema from "effect/Schema";
import {
  CommandId,
  EnvironmentId,
  IsoDateTime,
  NonNegativeInt,
  ThreadId,
  TrimmedNonEmptyString,
} from "../baseSchemas.ts";

/** Advertised only by servers whose database records settlement restore rows. */
export const SETTLEMENT_RESTORE_CAPABILITY = {
  schemaVersion: "jones.settlement-restore/v1",
} as const;
export const SettlementRestoreCapability = Schema.Struct({
  schemaVersion: Schema.Literal(SETTLEMENT_RESTORE_CAPABILITY.schemaVersion),
});
export type SettlementRestoreCapability = typeof SettlementRestoreCapability.Type;

/**
 * Restores the placement a thread had before the exact settle the caller observed.
 * It emits the existing `thread.unsettled` event, so older clients decode the result.
 */
export const SettlementRestoreCommand = Schema.Struct({
  type: Schema.Literal("thread.settlement.restore"),
  commandId: CommandId,
  threadId: ThreadId,
  settleCommandId: CommandId,
});
export type SettlementRestoreCommand = typeof SettlementRestoreCommand.Type;

export const SettlementRestorePlacement = Schema.Struct({
  settledOverride: Schema.NullOr(Schema.Literals(["settled", "active"])),
  unsettledAt: Schema.NullOr(IsoDateTime),
  pinnedAt: Schema.NullOr(IsoDateTime),
  pinOrderKey: Schema.NullOr(TrimmedNonEmptyString),
  activeOrderKey: Schema.NullOr(TrimmedNonEmptyString),
});
export type SettlementRestorePlacement = typeof SettlementRestorePlacement.Type;

/** Work a settle ended that restore never revives. */
export const SettlementRestoreNotRestored = Schema.Literals([
  "pull_request_watches",
  "runtime_requests",
  "queued_runs",
  "provider_sessions",
]);
export type SettlementRestoreNotRestored = typeof SettlementRestoreNotRestored.Type;

export const SettlementRestoreRecord = Schema.Struct({
  settleCommandId: CommandId,
  settledSequence: NonNegativeInt,
  settledAt: IsoDateTime,
  capturedAt: IsoDateTime,
  prior: SettlementRestorePlacement,
  snoozedUntilAtSettle: Schema.NullOr(IsoDateTime),
  clearedWatchCount: NonNegativeInt,
  restore: Schema.NullOr(
    Schema.Struct({ commandId: CommandId, restoredAt: IsoDateTime, sequence: NonNegativeInt }),
  ),
  notRestored: Schema.Array(SettlementRestoreNotRestored),
});
export type SettlementRestoreRecord = typeof SettlementRestoreRecord.Type;

export const SettlementRestoreStatus = Schema.Literals([
  "restorable",
  "restored",
  "superseded",
  "none",
]);
export type SettlementRestoreStatus = typeof SettlementRestoreStatus.Type;

/** Why a superseded record cannot be restored. */
export const SettlementRestoreBlocker = Schema.Literals([
  "stale_settle_command",
  "thread_missing",
  "state_changed",
  "invalid_projected_state",
]);
export type SettlementRestoreBlocker = typeof SettlementRestoreBlocker.Type;

export const SettlementRestoreInspectQuery = Schema.Struct({
  threadId: ThreadId,
  settleCommandId: Schema.optional(CommandId),
});
export type SettlementRestoreInspectQuery = typeof SettlementRestoreInspectQuery.Type;

export const SettlementRestoreInspectRequest = Schema.Struct({
  items: Schema.Array(SettlementRestoreInspectQuery).check(
    Schema.isMinLength(1),
    Schema.isMaxLength(100),
  ),
});
export type SettlementRestoreInspectRequest = typeof SettlementRestoreInspectRequest.Type;

export const SettlementRestoreInspection = Schema.Struct({
  threadId: ThreadId,
  status: SettlementRestoreStatus,
  blocker: Schema.optional(SettlementRestoreBlocker),
  requestedSettleCommandId: Schema.optional(CommandId),
  /** The requested settle was replaced by a newer one; its outcome is no longer kept. */
  historicalOutcome: Schema.optional(Schema.Literal("unavailable")),
  /** The thread's current record, which can belong to a newer settle than the one requested. */
  record: Schema.NullOr(SettlementRestoreRecord),
});
export type SettlementRestoreInspection = typeof SettlementRestoreInspection.Type;

export const SettlementRestoreInspectResult = Schema.Struct({
  environmentId: EnvironmentId,
  items: Schema.Array(SettlementRestoreInspection).check(Schema.isMaxLength(100)),
});
export type SettlementRestoreInspectResult = typeof SettlementRestoreInspectResult.Type;

export const SettlementRestoreRejectionCode = Schema.Literals([
  "none",
  "superseded",
  "already_restored",
  "thread_missing",
  "stale_settle_command",
  "invalid_projected_state",
  "command_id_reused",
]);
export type SettlementRestoreRejectionCode = typeof SettlementRestoreRejectionCode.Type;

export const SettlementRestoreOutcome = Schema.Literals([
  "restored",
  ...SettlementRestoreRejectionCode.literals,
]);
export type SettlementRestoreOutcome = typeof SettlementRestoreOutcome.Type;

export const SettlementRestoreResult = Schema.Struct({
  outcome: SettlementRestoreOutcome,
  environmentId: EnvironmentId,
  threadId: ThreadId,
  settleCommandId: CommandId,
  restoreCommandId: CommandId,
  restoredAt: Schema.optional(IsoDateTime),
  sequence: Schema.optional(NonNegativeInt),
  historicalOutcome: Schema.optional(Schema.Literal("unavailable")),
});
export type SettlementRestoreResult = typeof SettlementRestoreResult.Type;

export const settlementRestoreMcpCommandId = (clientRequestId: string) =>
  CommandId.make(`mcp:settlement-restore:${clientRequestId}`);

export const SettlementRestoreMcpInput = Schema.Struct({
  action: Schema.Literals(["inspect", "restore"]).annotate({
    description:
      "inspect reads the thread's undo record; restore undoes the exact settle named by settleCommandId.",
  }),
  threadId: ThreadId,
  settleCommandId: Schema.optional(CommandId).annotate({
    description:
      "The settle command ID you observed. Required for restore; for inspect it reports whether that settle is still the current one.",
  }),
  clientRequestId: Schema.optional(
    TrimmedNonEmptyString.check(Schema.isMaxLength(256)).annotate({
      description: "Required for restore. Reuse it when retrying the same restore.",
    }),
  ),
}).check(
  Schema.makeFilter(
    (input) =>
      input.action === "inspect" ||
      (input.settleCommandId !== undefined && input.clientRequestId !== undefined) ||
      "restore requires settleCommandId and clientRequestId",
  ),
);
export type SettlementRestoreMcpInput = typeof SettlementRestoreMcpInput.Type;

export const SettlementRestoreMcpResult = Schema.Union([
  Schema.Struct({
    action: Schema.Literal("inspect"),
    environmentId: EnvironmentId,
    ...SettlementRestoreInspection.fields,
  }),
  Schema.Struct({ action: Schema.Literal("restore"), ...SettlementRestoreResult.fields }),
]);
export type SettlementRestoreMcpResult = typeof SettlementRestoreMcpResult.Type;
