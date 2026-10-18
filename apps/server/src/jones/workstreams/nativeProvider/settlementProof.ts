import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import type { NativeProviderCommandReceipt, NativeProviderEventMetadata } from "./service.ts";

const Metadata = Schema.Struct({
  eventId: Schema.String,
  commandId: Schema.NullOr(Schema.String),
  aggregateKind: Schema.Literals(["thread", "project"]),
  aggregateId: Schema.String,
  sequence: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
  type: Schema.String,
  occurredAt: Schema.String,
  applicationEventVersion: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
});
const Receipt = Schema.Struct({
  commandId: Schema.String,
  aggregateKind: Schema.Literals(["thread", "project"]),
  aggregateId: Schema.String,
  commandType: Schema.String,
  acceptedAt: Schema.String,
  resultSequence: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  status: Schema.Literals(["accepted", "rejected"]),
});
const SettlementProof = Schema.Struct({
  receipt: Receipt,
  events: Schema.Array(Metadata),
});
export const SettlementProofJson = Schema.fromJsonString(SettlementProof);
type Proof = typeof SettlementProof.Type;

export const matchesSettlementProof = (
  proof: Proof,
  commandId: string,
  threadId: string,
  action: "settle" | "unsettle",
): boolean => {
  const { receipt, events } = proof;
  if (
    receipt.commandId !== commandId ||
    receipt.aggregateKind !== "thread" ||
    receipt.aggregateId !== threadId ||
    receipt.commandType !== `thread.${action}` ||
    events.length > 256 ||
    events.some(
      (event) =>
        event.applicationEventVersion !== 2 ||
        event.commandId !== commandId ||
        event.aggregateKind !== "thread" ||
        event.aggregateId !== threadId,
    )
  )
    return false;
  if (receipt.status === "rejected") return events.length === 0;
  const settlements = events.filter(
    (event) => event.type === "thread.settled" || event.type === "thread.unsettled",
  );
  const last = events.at(-1);
  return (
    settlements.length === 1 &&
    settlements[0]!.type === (action === "settle" ? "thread.settled" : "thread.unsettled") &&
    last !== undefined &&
    last.sequence === receipt.resultSequence &&
    last.occurredAt === receipt.acceptedAt &&
    events.every(
      (event, index) =>
        event.sequence > 0 &&
        event.sequence <= receipt.resultSequence &&
        (index === 0 || event.sequence === events[index - 1]!.sequence + 1),
    )
  );
};

export const sameNativeProviderReceipt = (
  left: NativeProviderCommandReceipt,
  right: NativeProviderCommandReceipt,
) =>
  left.commandId === right.commandId &&
  left.aggregateKind === right.aggregateKind &&
  left.aggregateId === right.aggregateId &&
  left.commandType === right.commandType &&
  left.acceptedAt === right.acceptedAt &&
  left.resultSequence === right.resultSequence &&
  left.status === right.status;

export const sameNativeProviderEvent = (
  left: NativeProviderEventMetadata,
  right: NativeProviderEventMetadata,
) =>
  left.eventId === right.eventId &&
  left.commandId === right.commandId &&
  left.aggregateKind === right.aggregateKind &&
  left.aggregateId === right.aggregateId &&
  left.sequence === right.sequence &&
  left.type === right.type &&
  left.occurredAt === right.occurredAt &&
  left.applicationEventVersion === right.applicationEventVersion;

export const readNativeProviderCommandMetadata = Effect.fn("NativeProvider.readCommandMetadata")(
  function* (commandId: string) {
    const sql = yield* SqlClient.SqlClient;
    const receipts = yield* sql`SELECT command_id AS "commandId", aggregate_kind AS "aggregateKind",
    aggregate_id AS "aggregateId", command_type AS "commandType", accepted_at AS "acceptedAt",
    result_sequence AS "resultSequence", status FROM orchestration_command_receipts WHERE command_id = ${commandId}`;
    // Retain wrong-domain rows and the 257th overflow sentinel for consumer refusal.
    const events = yield* sql`SELECT event_id AS "eventId", command_id AS "commandId",
    aggregate_kind AS "aggregateKind", stream_id AS "aggregateId", sequence,
    event_type AS "type", occurred_at AS "occurredAt", application_event_version AS "applicationEventVersion"
    FROM orchestration_events WHERE command_id = ${commandId} ORDER BY sequence ASC LIMIT 257`;
    return { receipts, events };
  },
);

export const NativeProviderMetadataSnapshot = Schema.Struct({
  receipts: Schema.Array(Receipt),
  events: Schema.Array(Metadata),
});

export class NativeProviderSettlementProofError extends Schema.TaggedError<NativeProviderSettlementProofError>()(
  "NativeProviderSettlementProofError",
  { reason: Schema.Literals(["invalid_proof", "persistence_failed"]) },
) {
  override get message(): string {
    return "Native settlement proof could not be persisted.";
  }
}

// EventSink calls this inside its receipt/event transaction; a failure must roll back that commit.
export const captureNativeProviderSettlementProof = Effect.fn(
  "NativeProvider.captureSettlementProof",
)(
  function* (input: {
    readonly receipt: NativeProviderCommandReceipt;
    readonly events: ReadonlyArray<NativeProviderEventMetadata>;
  }) {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<{
      thread_id: string;
      native_action: "settle" | "unsettle";
      proof_json: string | null;
    }>`
    SELECT thread_id, native_action, proof_json FROM jones_workstreams_settlement_proofs
    WHERE native_command_id = ${input.receipt.commandId}`;
    const reservation = rows[0];
    if (reservation === undefined) return;
    const decoded = Schema.decodeUnknownOption(SettlementProof)(input);
    if (
      Option.isNone(decoded) ||
      !matchesSettlementProof(
        decoded.value,
        input.receipt.commandId,
        reservation.thread_id,
        reservation.native_action,
      )
    )
      return yield* new NativeProviderSettlementProofError({ reason: "invalid_proof" });
    const encoded = yield* Schema.encodeEffect(SettlementProofJson)(decoded.value);
    if (reservation.proof_json !== null) {
      if (reservation.proof_json !== encoded)
        return yield* new NativeProviderSettlementProofError({ reason: "invalid_proof" });
      return;
    }
    yield* sql`UPDATE jones_workstreams_settlement_proofs SET proof_json = ${encoded}
    WHERE native_command_id = ${input.receipt.commandId} AND proof_json IS NULL`;
  },
  Effect.mapError((error) =>
    Schema.is(NativeProviderSettlementProofError)(error)
      ? error
      : new NativeProviderSettlementProofError({ reason: "persistence_failed" }),
  ),
);
