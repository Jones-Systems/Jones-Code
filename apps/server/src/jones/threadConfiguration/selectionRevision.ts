import type { ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";
import { readEventLogPrunedThrough } from "../persistence/EventLogPruneBoundary.ts";

/**
 * A thread's newest selection-event sequence, floored at the pruned-through sequence: pruning a
 * newer selection event must never make an older revision current again.
 */
export const readSelectionEventRevision = Effect.fn(
  "ThreadSelfConfiguration.readSelectionEventRevision",
)(function* (sql: SqlClient.SqlClient, threadId: ThreadId) {
  const rows = yield* sql<{ readonly revision: number }>`
    SELECT COALESCE(MAX(sequence), 0) AS revision FROM orchestration_events
    WHERE application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ${threadId}
      AND event_type IN ('thread.created', 'thread.model-selection-updated', 'thread.provider-switched')
  `;
  return Math.max(rows[0]?.revision ?? 0, yield* readEventLogPrunedThrough(sql));
});
