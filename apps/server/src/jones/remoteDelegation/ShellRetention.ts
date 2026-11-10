import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";

/**
 * Keep active owners (neither archived nor deleted) of bound remote tasks so linked delegates
 * can find their parent. The set follows the live thread list, not every thread that delegated.
 */
export const readRemoteTaskOwnerIds = Effect.fn("ShellRetention.readRemoteTaskOwnerIds")(function* (
  sql: SqlClient.SqlClient,
) {
  const rows = yield* sql<{ readonly threadId: string }>`
      SELECT DISTINCT task.thread_id AS "threadId"
      FROM orchestration_v2_projection_subagents AS task
      JOIN orchestration_v2_projection_threads AS owner ON owner.thread_id = task.thread_id
      WHERE task.driver = 'remote'
        AND owner.archived_at IS NULL
        AND owner.deleted_at IS NULL
        AND CASE WHEN json_valid(task.payload_json)
          THEN json_extract(task.payload_json, '$.remoteChild.threadId') END IS NOT NULL
    `;
  return new Set(rows.map((row) => row.threadId));
});
