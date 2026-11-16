import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION } from "../../orchestration-v2/ProjectionStore.ts";
import { EVENT_LOG_PRUNED_THROUGH_PROJECTION } from "./EventLogPruneBoundary.ts";

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

/**
 * Applies upstream's `pruneEventStore` keep rule through `throughSequence`: v1 events,
 * `thread.created`, running `run.updated` and each stream's newest event stay; prompt anchors are
 * not modeled. Publishes the pruned-through boundary first, leaves receipts, returns deleted sequences.
 */
export const pruneEventLogLikeUpstream = Effect.fn("prunedEventLogTestkit.prune")(function* (
  throughSequence: number,
) {
  const sql = yield* SqlClient.SqlClient;
  const at = DateTime.formatIso(yield* DateTime.now);
  yield* sql`INSERT INTO orchestration_v2_projection_metadata (
      projection_name, schema_version, last_sequence, updated_at
    ) VALUES (
      ${EVENT_LOG_PRUNED_THROUGH_PROJECTION}, ${ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION},
      ${throughSequence}, ${at}
    ) ON CONFLICT(projection_name) DO UPDATE SET
      last_sequence = MAX(last_sequence, excluded.last_sequence), updated_at = excluded.updated_at`;
  const deleted = yield* sql<{ readonly sequence: number }>`
    DELETE FROM orchestration_events WHERE sequence IN (
      SELECT event.sequence FROM orchestration_events AS event
      WHERE event.sequence <= ${throughSequence}
        AND event.application_event_version != 1
        AND event.event_type != 'thread.created'
        AND NOT (event.event_type = 'run.updated'
          AND json_extract(event.payload_json, '$.status') = 'running')
        AND event.sequence != (
          SELECT MAX(newer.sequence) FROM orchestration_events AS newer
          WHERE newer.aggregate_kind = event.aggregate_kind AND newer.stream_id = event.stream_id
        )
    ) RETURNING sequence`;
  return deleted.map((row) => row.sequence).sort((left, right) => left - right);
});

/** One v2 application event row with an explicit sequence, for fixtures that need exact history. */
export const insertTestEvent = Effect.fn("prunedEventLogTestkit.insertEvent")(function* (input: {
  readonly sequence: number;
  readonly eventId: string;
  readonly streamId: string;
  readonly type: string;
  readonly aggregateKind?: "thread" | "project";
  readonly commandId?: string | null;
  readonly occurredAt?: string;
  readonly payload?: unknown;
}) {
  const sql = yield* SqlClient.SqlClient;
  const kind = input.aggregateKind ?? "thread";
  const versions = yield* sql<{ readonly version: number }>`
    SELECT COALESCE(MAX(stream_version), 0) + 1 AS version FROM orchestration_events
    WHERE aggregate_kind = ${kind} AND stream_id = ${input.streamId}`;
  yield* sql`INSERT INTO orchestration_events (
      sequence, event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
      command_id, actor_kind, payload_json, metadata_json, application_event_version
    ) VALUES (
      ${input.sequence}, ${input.eventId}, ${kind}, ${input.streamId}, ${versions[0]!.version},
      ${input.type}, ${input.occurredAt ?? "2026-11-01T00:00:00.000Z"}, ${input.commandId ?? null},
      'client', ${encodeJson(input.payload ?? {})}, '{}', 2
    )`;
});
