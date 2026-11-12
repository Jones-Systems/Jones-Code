import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";

/**
 * Upstream's pruner publishes this metadata row before it deletes events: sequences at or below
 * it may be gone. The row is absent, reading as 0, until a pruning pass has run.
 */
export const EVENT_LOG_PRUNED_THROUGH_PROJECTION = "event-log-pruned-through";

/** Primary-key lookup on the upstream metadata row; 0 when the log has never been pruned. */
export const readEventLogPrunedThrough = (
  sql: SqlClient.SqlClient,
): Effect.Effect<number, SqlError> =>
  sql<{ readonly last_sequence: number }>`SELECT last_sequence
    FROM orchestration_v2_projection_metadata
    WHERE projection_name = ${EVENT_LOG_PRUNED_THROUGH_PROJECTION}`.pipe(
    Effect.map((rows) => rows[0]?.last_sequence ?? 0),
  );
