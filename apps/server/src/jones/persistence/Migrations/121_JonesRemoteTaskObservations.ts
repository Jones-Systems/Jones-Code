import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // Upstream IDs identify this sparse extension; foreign keys would obstruct upstream pruning.
  yield* sql`CREATE TABLE jones_remote_task_observations (
    parent_thread_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    observation_json TEXT NOT NULL,
    notices_json TEXT NOT NULL DEFAULT '[]',
    signal_pending INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (parent_thread_id, task_id)
  )`;
});
