import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";

import * as NativeCommandEventMetadata from "../Services/NativeCommandEventMetadata.ts";

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
  const readMetadata = SqlSchema.findAll({
    Request: Schema.String,
    Result: MetadataRow,
    execute: (commandId) => sql`
      SELECT sequence, event_id AS "eventId", event_type AS "type",
        aggregate_kind AS "aggregateKind", stream_id AS "aggregateId",
        occurred_at AS "occurredAt", command_id AS "commandId",
        application_event_version AS "applicationEventVersion"
      FROM orchestration_events
      WHERE command_id = ${commandId}
      ORDER BY sequence ASC LIMIT 257
    `,
  });
  const readReceipt = SqlSchema.findOneOption({
    Request: Schema.String,
    Result: ReceiptRow,
    execute: (commandId) => sql`
      SELECT command_id AS "commandId", aggregate_kind AS "aggregateKind",
        aggregate_id AS "aggregateId", command_type AS "commandType",
        accepted_at AS "acceptedAt", result_sequence AS "resultSequence", status
      FROM orchestration_command_receipts WHERE command_id = ${commandId}
    `,
  });

  const readMetadataByCommandId: NativeCommandEventMetadata.NativeCommandEventMetadata["Service"]["readMetadataByCommandId"] =
    (commandId) =>
      readMetadata(commandId).pipe(
        Effect.mapError(
          () =>
            new NativeCommandEventMetadata.NativeCommandEventMetadataError({
              operation: "readMetadataByCommandId",
            }),
        ),
      );
  const readSnapshotByCommandId: NativeCommandEventMetadata.NativeCommandEventMetadata["Service"]["readSnapshotByCommandId"] =
    (commandId) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            const receipt = yield* readReceipt(commandId);
            const events = yield* readMetadata(commandId);
            return { receipt, events };
          }),
        )
        .pipe(
          Effect.mapError(
            () =>
              new NativeCommandEventMetadata.NativeCommandEventMetadataError({
                operation: "readSnapshotByCommandId",
              }),
          ),
        );

  return NativeCommandEventMetadata.NativeCommandEventMetadata.of({
    readMetadataByCommandId,
    readSnapshotByCommandId,
  });
});

export const layer = Layer.effect(NativeCommandEventMetadata.NativeCommandEventMetadata, make);
