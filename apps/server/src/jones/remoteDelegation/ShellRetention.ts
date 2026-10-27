import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";

/** Keep active owners of bound remote tasks so linked delegates can find their parent. */
export const readRemoteTaskOwnerIds = Effect.fn("ShellRetention.readRemoteTaskOwnerIds")(function* (
  sql: SqlClient.SqlClient,
) {
  const rows = yield* sql<{ readonly threadId: string }>`
      SELECT DISTINCT thread_id AS "threadId"
      FROM orchestration_v2_projection_subagents
      WHERE driver = 'remote'
        AND CASE WHEN json_valid(payload_json)
          THEN json_extract(payload_json, '$.remoteChild.threadId') END IS NOT NULL
    `;
  return new Set(rows.map((row) => row.threadId));
});
