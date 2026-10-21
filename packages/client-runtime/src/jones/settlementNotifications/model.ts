import {
  EnvironmentId,
  ThreadId,
  SettlementProjectRemaining,
  SettlementOrigin as SettlementOriginSchema,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

export const BASELINE_DEBOUNCE_MS = 1_000;
export const SEQUENCE_RESET_MS = 60_000;

const FiniteNumber = Schema.Number.check(Schema.isFinite());

export const SettlementEntry = Schema.Struct({
  id: Schema.String,
  environmentId: EnvironmentId,
  threadId: ThreadId,
  title: Schema.String,
  settledAtMs: FiniteNumber,
  detectedAtMs: FiniteNumber,
  threadGone: Schema.Boolean,
  projectRemaining: Schema.optional(SettlementProjectRemaining),
});
export type SettlementEntry = typeof SettlementEntry.Type;

export const SettlementInboxState = Schema.Struct({
  version: Schema.Literal(1),
  entries: Schema.mutable(Schema.Array(SettlementEntry)),
  acknowledged: Schema.mutable(Schema.Array(Schema.String)),
});
export type SettlementInboxState = typeof SettlementInboxState.Type;

export const EnvironmentSettlementBaseline = Schema.Struct({
  version: Schema.Literal(1),
  environmentId: EnvironmentId,
  snapshotSequence: FiniteNumber.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)),
  watermarkMs: Schema.NullOr(FiniteNumber),
  threads: Schema.Record(Schema.String, Schema.NullOr(FiniteNumber)),
  confirmedAtMs: FiniteNumber,
});
export type EnvironmentSettlementBaseline = typeof EnvironmentSettlementBaseline.Type;

export type SettlementArrival = Omit<SettlementEntry, "id" | "detectedAtMs" | "threadGone">;

export type SettlementOrigin = "client" | "other" | "unknown";
export const SettlementOriginDetails = Schema.Struct({
  origin: SettlementOriginSchema,
  projectRemaining: Schema.optional(SettlementProjectRemaining),
});
export type SettlementOriginDetails = typeof SettlementOriginDetails.Type;
export type SettlementOriginResolver = (
  arrivals: readonly SettlementArrival[],
) => Promise<readonly (SettlementOrigin | SettlementOriginDetails)[] | null>;

export function settlementEntryId(
  environmentId: EnvironmentId,
  threadId: ThreadId,
  settledAtMs: number,
): string {
  return `${environmentId}:${threadId}:${settledAtMs}`;
}

export function emptyInbox(): SettlementInboxState {
  return { version: 1, entries: [], acknowledged: [] };
}

const decodeInbox = Schema.decodeUnknownSync(Schema.fromJsonString(SettlementInboxState));
export function decodeSettlementInbox(json: string): SettlementInboxState {
  const inbox = decodeInbox(json);
  if (
    inbox.entries.some(
      (entry) =>
        entry.id !== settlementEntryId(entry.environmentId, entry.threadId, entry.settledAtMs),
    )
  ) {
    throw new Error("Settlement entry identity does not match its event.");
  }
  return inbox;
}
export const encodeSettlementInbox = Schema.encodeSync(Schema.fromJsonString(SettlementInboxState));
export const decodeSettlementBaseline = Schema.decodeUnknownSync(
  Schema.fromJsonString(EnvironmentSettlementBaseline),
);
export const encodeSettlementBaseline = Schema.encodeSync(
  Schema.fromJsonString(EnvironmentSettlementBaseline),
);
