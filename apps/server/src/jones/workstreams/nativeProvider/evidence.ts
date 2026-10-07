import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import type {
  NativeProviderEvidence as NativeProviderEvidencePort,
} from "./service.ts";

export class NativeProviderEvidenceReadError extends Schema.TaggedError<NativeProviderEvidenceReadError>()(
  "NativeProviderEvidenceReadError",
  { reason: Schema.Literals(["reader_unavailable", "invalid_snapshot"]) },
) {
  override get message(): string {
    return "Native command evidence could not be read.";
  }
}

export class NativeProviderEvidence extends Context.Service<
  NativeProviderEvidence,
  NativeProviderEvidencePort
>()("t3/jones/workstreams/nativeProvider/evidence/NativeProviderEvidence") {}

const MetadataRow = Schema.Struct({
  eventId: Schema.String,
  commandId: Schema.NullOr(Schema.String),
  aggregateKind: Schema.Literals(["thread", "project"]),
  aggregateId: Schema.String,
  sequence: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
  type: Schema.String,
  occurredAt: Schema.String,
  applicationEventVersion: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
});
const ReceiptRow = Schema.Struct({
  commandId: Schema.String,
  aggregateKind: Schema.Literals(["thread", "project"]),
  aggregateId: Schema.String,
  commandType: Schema.String,
  acceptedAt: Schema.String,
  resultSequence: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  status: Schema.Literals(["accepted", "rejected"]),
});

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const readSnapshotByCommandId: NativeProviderEvidencePort["readSnapshotByCommandId"] = (
    commandId,
    _threadId,
  ) =>
    sql.withTransaction(
      Effect.gen(function* () {
        const receiptRows = yield* sql`SELECT command_id AS "commandId",
          aggregate_kind AS "aggregateKind", aggregate_id AS "aggregateId",
          command_type AS "commandType", accepted_at AS "acceptedAt",
          result_sequence AS "resultSequence", status
          FROM orchestration_command_receipts WHERE command_id = ${commandId}`.pipe(
          Effect.mapError(() => new NativeProviderEvidenceReadError({ reason: "reader_unavailable" })),
        );
        const receipts = yield* Schema.decodeUnknownEffect(Schema.Array(ReceiptRow))(receiptRows).pipe(
          Effect.mapError(() => new NativeProviderEvidenceReadError({ reason: "invalid_snapshot" })),
        );
        if (receipts.length > 1)
          return yield* new NativeProviderEvidenceReadError({ reason: "invalid_snapshot" });
        // Retain wrong-domain rows and the 257th overflow sentinel for consumer refusal.
        const metadataRows = yield* sql`SELECT event_id AS "eventId", command_id AS "commandId",
          aggregate_kind AS "aggregateKind", stream_id AS "aggregateId", sequence,
          event_type AS "type", occurred_at AS "occurredAt",
          application_event_version AS "applicationEventVersion"
          FROM orchestration_events WHERE command_id = ${commandId}
          ORDER BY sequence ASC LIMIT 257`.pipe(
          Effect.mapError(() => new NativeProviderEvidenceReadError({ reason: "reader_unavailable" })),
        );
        const events = yield* Schema.decodeUnknownEffect(Schema.Array(MetadataRow))(metadataRows).pipe(
          Effect.mapError(() => new NativeProviderEvidenceReadError({ reason: "invalid_snapshot" })),
        );
        return { receipt: Option.fromUndefinedOr(receipts[0]), events };
      }),
    ).pipe(
      Effect.mapError((error) =>
        Schema.is(NativeProviderEvidenceReadError)(error)
          ? error
          : new NativeProviderEvidenceReadError({ reason: "reader_unavailable" }),
      ),
    );
  return NativeProviderEvidence.of({ readSnapshotByCommandId });
});

export const makeNativeProviderEvidenceLayer = (reader?: NativeProviderEvidencePort) =>
  reader === undefined
    ? Layer.effect(NativeProviderEvidence, make)
    : Layer.succeed(NativeProviderEvidence, reader);
