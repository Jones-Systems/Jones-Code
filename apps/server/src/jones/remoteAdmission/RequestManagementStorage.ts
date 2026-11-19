import type * as SqlClient from "effect/sql/SqlClient";

export const requestManagementTable = "jones_request_management_edits";

/** Inbox lists alias the admission row as "a". Exact reads retain dismissed evidence. */
export const inboxRequestPredicate = (sql: SqlClient.SqlClient) =>
  sql`(SELECT m.dismissed_at FROM jones_request_management_edits m
    WHERE m.admission_id=a.id ORDER BY m.revision DESC LIMIT 1) IS NULL`;
