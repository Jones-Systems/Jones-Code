import {
  SettlementRestoreRejectionCode,
  type OrchestrationV2AppThread,
  type SettlementRestoreBlocker,
  type SettlementRestoreCommand,
  type SettlementRestoreRecord,
  type SettlementRestoreStatus,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type { StoredSettlementRestore } from "./store.ts";

type Valid = { readonly type: "valid"; readonly thread: OrchestrationV2AppThread };
type Projected = Valid | { readonly type: "invalid" };

const parse = (value: string | null): Option.Option<DateTime.Utc | null> =>
  value === null ? Option.some(null) : DateTime.make(value);

const validKey = (key: string | null) => key === null || (key.length > 0 && key.trim() === key);

/**
 * The thread as restore would leave it: explicit active intent, so automatic
 * settlement does not immediately re-settle it, and the target's own prior
 * pin, order keys and wake time, including nulls. Snooze, PR links and every
 * operational field stay as they are now.
 */
const projectRestoredThread = (
  thread: OrchestrationV2AppThread,
  record: SettlementRestoreRecord,
  now: DateTime.Utc,
): Projected => {
  const unsettledAt = parse(record.prior.unsettledAt);
  const pinnedAt = parse(record.prior.pinnedAt);
  if (Option.isNone(unsettledAt) || Option.isNone(pinnedAt)) return { type: "invalid" };
  const { pinOrderKey, activeOrderKey } = record.prior;
  // Only pinned threads carry a pin order key; the orchestrator never leaves one behind.
  if (
    !validKey(pinOrderKey) ||
    !validKey(activeOrderKey) ||
    (pinOrderKey !== null && pinnedAt.value === null)
  )
    return { type: "invalid" };
  return {
    type: "valid",
    thread: {
      ...thread,
      settledOverride: "active",
      settledAt: null,
      unsettledAt: unsettledAt.value,
      pinnedAt: pinnedAt.value,
      pinOrderKey,
      activeOrderKey,
      updatedAt: now,
    },
  };
};

/** The thread still shows exactly the settle this record captured, with no newer owner intent. */
const stillSettledByRecord = (
  thread: OrchestrationV2AppThread,
  record: SettlementRestoreRecord,
  now: DateTime.Utc,
) => {
  const snoozed = thread.snoozedUntil ?? null;
  const conflictingSnooze =
    snoozed !== null &&
    DateTime.toEpochMillis(snoozed) > DateTime.toEpochMillis(now) &&
    DateTime.formatIso(snoozed) !== record.snoozedUntilAtSettle;
  return (
    thread.archivedAt === null &&
    thread.settledOverride === "settled" &&
    thread.settledAt !== null &&
    DateTime.toEpochMillis(thread.settledAt) === Date.parse(record.settledAt) &&
    thread.pinnedAt == null &&
    thread.pinOrderKey == null &&
    thread.activeOrderKey == null &&
    !conflictingSnooze
  );
};

type Blocked = {
  readonly blocker: SettlementRestoreBlocker;
  readonly historicalOutcome?: "unavailable";
};

const blockerFor = (
  record: SettlementRestoreRecord,
  thread: OrchestrationV2AppThread | null,
  now: DateTime.Utc,
): Blocked | Valid => {
  if (thread === null || thread.deletedAt !== null) return { blocker: "thread_missing" };
  if (!stillSettledByRecord(thread, record, now)) return { blocker: "state_changed" };
  const projected = projectRestoredThread(thread, record, now);
  return projected.type === "invalid" ? { blocker: "invalid_projected_state" } : projected;
};

export const inspectSettlementRestore = (input: {
  readonly stored: StoredSettlementRestore | null;
  readonly requestedSettleCommandId: SettlementRestoreRecord["settleCommandId"] | undefined;
  readonly thread: OrchestrationV2AppThread | null;
  readonly now: DateTime.Utc;
}): {
  readonly status: SettlementRestoreStatus;
  readonly blocker?: SettlementRestoreBlocker;
  readonly historicalOutcome?: "unavailable";
} => {
  const record = input.stored?.record;
  if (record === undefined)
    return input.requestedSettleCommandId === undefined
      ? { status: "none" }
      : { status: "none", historicalOutcome: "unavailable" };
  if (
    input.requestedSettleCommandId !== undefined &&
    input.requestedSettleCommandId !== record.settleCommandId
  )
    return {
      status: "superseded",
      blocker: "stale_settle_command",
      historicalOutcome: "unavailable",
    };
  if (record.restore !== null) return { status: "restored" };
  const blocked = blockerFor(record, input.thread, input.now);
  return "blocker" in blocked ? { status: "superseded", ...blocked } : { status: "restorable" };
};

type SettlementRestoreDecision =
  | { readonly type: "restore"; readonly thread: OrchestrationV2AppThread }
  | { readonly type: "duplicate"; readonly record: SettlementRestoreRecord }
  | {
      readonly type: "reject";
      readonly code: SettlementRestoreRejectionCode;
      readonly historicalOutcome?: "unavailable";
    };

/** Decided under the thread's dispatch lock after a command-receipt miss. */
export const planSettlementRestore = (input: {
  readonly command: SettlementRestoreCommand;
  readonly stored: StoredSettlementRestore | null;
  readonly sameRestoreCommand: StoredSettlementRestore | null;
  readonly thread: OrchestrationV2AppThread | null;
  readonly now: DateTime.Utc;
}): SettlementRestoreDecision => {
  const { command } = input;
  if (input.sameRestoreCommand !== null && input.sameRestoreCommand.threadId !== command.threadId)
    return { type: "reject", code: "command_id_reused" };
  const record = input.stored?.record;
  if (record === undefined) return { type: "reject", code: "none" };
  if (record.settleCommandId !== command.settleCommandId)
    return { type: "reject", code: "stale_settle_command", historicalOutcome: "unavailable" };
  if (record.restore?.commandId === command.commandId) return { type: "duplicate", record };
  if (record.restore !== null) return { type: "reject", code: "already_restored" };
  const blocked = blockerFor(record, input.thread, input.now);
  if (!("blocker" in blocked)) return { type: "restore", thread: blocked.thread };
  return {
    type: "reject",
    code: blocked.blocker === "state_changed" ? "superseded" : blocked.blocker,
  };
};

const rejectionMessages: Record<SettlementRestoreRejectionCode, string> = {
  none: "this thread has no settlement to undo.",
  superseded: "the thread changed after it was settled.",
  already_restored: "this settlement was already undone.",
  thread_missing: "the thread no longer exists.",
  stale_settle_command: "a newer settlement replaced the one being undone.",
  invalid_projected_state: "the saved placement is no longer valid.",
  command_id_reused: "this command ID belongs to another restore.",
};

/**
 * A restore the orchestrator refused for a domain reason. It is the cause of the
 * dispatch error, so the service reads `code` directly. Clients see only the
 * message, and match its `Settlement restore rejected: <code>:` wording.
 */
export class SettlementRestoreRejectedError extends Schema.TaggedError<SettlementRestoreRejectedError>()(
  "SettlementRestoreRejectedError",
  { code: SettlementRestoreRejectionCode },
) {
  override get message(): string {
    return `Settlement restore rejected: ${this.code}: ${rejectionMessages[this.code]}`;
  }
}

const isRejection = Schema.is(SettlementRestoreRejectedError);

/** The dispatch error cause for a rejected restore. */
export const settlementRestoreRejection = (code: SettlementRestoreRejectionCode) =>
  new SettlementRestoreRejectedError({ code });

/** The rejection a dispatch error's cause carries, or null for any other cause. */
export const readSettlementRestoreRejection = (
  cause: unknown,
): SettlementRestoreRejectionCode | null => (isRejection(cause) ? cause.code : null);
