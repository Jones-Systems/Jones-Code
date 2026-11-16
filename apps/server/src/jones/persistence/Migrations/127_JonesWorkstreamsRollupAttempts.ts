import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`ALTER TABLE jones_workstreams_rollup_queue ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0)`;
  // Existing nonpublished work has no independent counter; preserve its conservative retry quarantine.
  yield* sql`UPDATE jones_workstreams_rollup_queue SET attempt_count=CASE WHEN failure_code='published' THEN 0 ELSE fence END`;
});
