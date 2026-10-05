import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_orch_events_thread_creation_lookup
    ON orchestration_events (stream_id, sequence DESC, event_id)
    WHERE aggregate_kind = 'thread' AND event_type = 'thread.created'`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_orch_v2_events_thread_creation_lookup
    ON orchestration_events (stream_id, sequence DESC, event_id, application_event_version, aggregate_kind, event_type)
    WHERE aggregate_kind = 'thread' AND application_event_version = 2 AND event_type = 'thread.created'`;
});
