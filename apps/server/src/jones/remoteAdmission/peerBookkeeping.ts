import type * as SqlClient from "effect/sql/SqlClient";

// These calls maintain peer protocol state. Their handlers authorize the exact linked session.
const operations: readonly string[] = [
  "t3_remote_admission_ack",
  "t3_remote_admission_attempt_register",
  "t3_remote_admission_capabilities_register",
  "t3_remote_admission_notice_ack",
  "t3_remote_admission_notice_register",
];

export const isPeerBookkeeping = (operation: string) => operations.includes(operation);

// Owner list queries alias the admissions table as "a". Exact lookups keep historical rows.
export const ownerRequestPredicate = (sql: SqlClient.SqlClient) =>
  sql`COALESCE(json_extract(a.envelope_json, '$.operation'), '') NOT IN ${sql.in(operations)}`;
