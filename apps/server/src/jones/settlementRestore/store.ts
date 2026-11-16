import {
  CommandId,
  type OrchestrationV2AppThread,
  type SettlementRestoreRecord,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";
import type { JonesTablePresence } from "../persistence/JonesTablePresence.ts";

const SETTLEMENT_RESTORE_TABLE = "jones_settlement_restores";

export class SettlementRestoreMarkerError extends Schema.TaggedError<SettlementRestoreMarkerError>()(
  "SettlementRestoreMarkerError",
  { reason: Schema.Literals(["storage_unavailable", "record_changed"]) },
) {
  override get message(): string {
    return this.reason === "storage_unavailable"
      ? "Settlement restore storage is unavailable."
      : "The settlement restore record changed before the restore committed.";
  }
}

interface Row {
  readonly thread_id: string;
  readonly settle_command_id: string;
  readonly settled_sequence: number;
  readonly settled_at: string;
  readonly captured_at: string;
  readonly prior_settled_override: string | null;
  readonly prior_unsettled_at: string | null;
  readonly prior_pinned_at: string | null;
  readonly prior_pin_order_key: string | null;
  readonly prior_active_order_key: string | null;
  readonly snoozed_until_at_settle: string | null;
  readonly cleared_watch_count: number;
  readonly restore_command_id: string | null;
  readonly restored_at: string | null;
  readonly restored_sequence: number | null;
}

export interface StoredSettlementRestore {
  readonly threadId: ThreadId;
  readonly record: SettlementRestoreRecord;
}

const NOT_RESTORED = [
  "pull_request_watches",
  "runtime_requests",
  "queued_runs",
  "provider_sessions",
] as const;

const toRecord = (row: Row): StoredSettlementRestore => ({
  threadId: ThreadId.make(row.thread_id),
  record: {
    settleCommandId: CommandId.make(row.settle_command_id),
    settledSequence: row.settled_sequence,
    settledAt: row.settled_at,
    capturedAt: row.captured_at,
    prior: {
      settledOverride:
        row.prior_settled_override === "settled" || row.prior_settled_override === "active"
          ? row.prior_settled_override
          : null,
      unsettledAt: row.prior_unsettled_at,
      pinnedAt: row.prior_pinned_at,
      pinOrderKey: row.prior_pin_order_key,
      activeOrderKey: row.prior_active_order_key,
    },
    snoozedUntilAtSettle: row.snoozed_until_at_settle,
    clearedWatchCount: row.cleared_watch_count,
    restore:
      row.restore_command_id === null || row.restored_at === null || row.restored_sequence === null
        ? null
        : {
            commandId: CommandId.make(row.restore_command_id),
            restoredAt: row.restored_at,
            sequence: row.restored_sequence,
          },
    notRestored: NOT_RESTORED,
  },
});

const iso = (value: DateTime.Utc | null | undefined) =>
  value == null ? null : DateTime.formatIso(value);
const epoch = (value: DateTime.Utc | null | undefined) =>
  value == null ? null : DateTime.toEpochMillis(value);

/** Whether a `thread.settled` payload changed placement, as opposed to a no-op re-settle. */
const settleChangedPlacement = (
  before: OrchestrationV2AppThread,
  after: OrchestrationV2AppThread,
) =>
  before.settledOverride !== after.settledOverride ||
  epoch(before.settledAt) !== epoch(after.settledAt) ||
  epoch(before.unsettledAt) !== epoch(after.unsettledAt) ||
  epoch(before.pinnedAt) !== epoch(after.pinnedAt) ||
  (before.pinOrderKey ?? null) !== (after.pinOrderKey ?? null) ||
  (before.activeOrderKey ?? null) !== (after.activeOrderKey ?? null);

const isThreadPayload = (payload: unknown): payload is OrchestrationV2AppThread =>
  typeof payload === "object" && payload !== null && "settledOverride" in payload;

interface StoredEvent {
  readonly sequence: number;
  readonly commandId: CommandId | null;
  readonly event: { readonly type: string; readonly payload: unknown };
}

export const makeSettlementRestoreStore = (
  sql: SqlClient.SqlClient,
  tablePresent: JonesTablePresence,
) => {
  const present = tablePresent(SETTLEMENT_RESTORE_TABLE);

  const read = Effect.fn("SettlementRestoreStore.read")(function* (threadId: ThreadId) {
    if (!(yield* present)) return null;
    const rows = yield* sql<Row>`SELECT * FROM jones_settlement_restores
        WHERE thread_id = ${threadId}`;
    return rows[0] === undefined ? null : toRecord(rows[0]);
  });

  const findRestoreCommand = Effect.fn("SettlementRestoreStore.findRestoreCommand")(function* (
    commandId: CommandId,
  ) {
    if (!(yield* present)) return null;
    const rows = yield* sql<Row>`SELECT * FROM jones_settlement_restores
        WHERE restore_command_id = ${commandId}`;
    return rows[0] === undefined ? null : toRecord(rows[0]);
  });

  /**
   * Runs inside the EventSink transaction before projections are replaced, so
   * the stored preimage is the target's own row. A failure aborts the settle.
   * Earlier full-thread events in the same batch stand in for the projection.
   */
  const capture = Effect.fn("SettlementRestoreStore.capture")(function* <E>(
    storedEvents: ReadonlyArray<StoredEvent>,
    readThread: (threadId: ThreadId) => Effect.Effect<OrchestrationV2AppThread, E>,
  ) {
    if (!storedEvents.some((stored) => stored.event.type === "thread.settled")) return;
    if (!(yield* present)) return;
    const capturedAt = DateTime.formatIso(yield* DateTime.now);
    for (const [index, stored] of storedEvents.entries()) {
      if (stored.event.type !== "thread.settled" || !isThreadPayload(stored.event.payload))
        continue;
      const after = stored.event.payload;
      const earlier = storedEvents
        .slice(0, index)
        .findLast(
          (candidate) =>
            isThreadPayload(candidate.event.payload) && candidate.event.payload.id === after.id,
        )?.event.payload as OrchestrationV2AppThread | undefined;
      const before = earlier ?? (yield* readThread(after.id));
      if (!settleChangedPlacement(before, after) || after.settledAt === null) continue;
      if (stored.commandId === null) {
        yield* Effect.logWarning("Settlement restore skipped a settle without a command ID.");
        continue;
      }
      const watched = (thread: OrchestrationV2AppThread) =>
        (thread.pullRequests ?? []).filter((link) => link.watch !== undefined).length;
      const clearedWatchCount = Math.max(0, watched(before) - watched(after));
      yield* sql`INSERT INTO jones_settlement_restores (
            thread_id, settle_command_id, settled_sequence, settled_at, captured_at,
            prior_settled_override, prior_unsettled_at, prior_pinned_at, prior_pin_order_key,
            prior_active_order_key, snoozed_until_at_settle, cleared_watch_count,
            restore_command_id, restored_at, restored_sequence
          ) VALUES (
            ${after.id}, ${stored.commandId}, ${stored.sequence}, ${DateTime.formatIso(after.settledAt)},
            ${capturedAt}, ${before.settledOverride}, ${iso(before.unsettledAt)}, ${iso(before.pinnedAt)},
            ${before.pinOrderKey ?? null}, ${before.activeOrderKey ?? null}, ${iso(after.snoozedUntil)},
            ${clearedWatchCount}, NULL, NULL, NULL
          )
          ON CONFLICT(thread_id) DO UPDATE SET
            settle_command_id = excluded.settle_command_id,
            settled_sequence = excluded.settled_sequence,
            settled_at = excluded.settled_at,
            captured_at = excluded.captured_at,
            prior_settled_override = excluded.prior_settled_override,
            prior_unsettled_at = excluded.prior_unsettled_at,
            prior_pinned_at = excluded.prior_pinned_at,
            prior_pin_order_key = excluded.prior_pin_order_key,
            prior_active_order_key = excluded.prior_active_order_key,
            snoozed_until_at_settle = excluded.snoozed_until_at_settle,
            cleared_watch_count = excluded.cleared_watch_count,
            restore_command_id = NULL,
            restored_at = NULL,
            restored_sequence = NULL`;
    }
  });

  /** Compare-and-set inside the restore's commit transaction; a miss aborts the restore. */
  const recordRestore = Effect.fn("SettlementRestoreStore.recordRestore")(function* (input: {
    readonly threadId: ThreadId;
    readonly settleCommandId: CommandId;
    readonly restoreCommandId: CommandId;
    readonly restoredAt: DateTime.Utc;
    readonly sequence: number;
  }) {
    if (!(yield* present))
      return yield* new SettlementRestoreMarkerError({ reason: "storage_unavailable" });
    const updated = yield* sql<{ readonly thread_id: string }>`UPDATE jones_settlement_restores
        SET restore_command_id = ${input.restoreCommandId},
          restored_at = ${DateTime.formatIso(input.restoredAt)},
          restored_sequence = ${input.sequence}
        WHERE thread_id = ${input.threadId}
          AND settle_command_id = ${input.settleCommandId}
          AND restore_command_id IS NULL
        RETURNING thread_id`;
    if (updated.length !== 1)
      return yield* new SettlementRestoreMarkerError({ reason: "record_changed" });
  });

  return { present, read, findRestoreCommand, capture, recordRestore };
};

export type SettlementRestoreStore = ReturnType<typeof makeSettlementRestoreStore>;
