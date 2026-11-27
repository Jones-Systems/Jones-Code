import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_workflow_agent_journal (
    run_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('step', 'write', 'turn', 'unconfirmed_stop')),
    name TEXT NOT NULL,
    binding TEXT,
    PRIMARY KEY (run_id, kind, name)
  ) WITHOUT ROWID`;
});
