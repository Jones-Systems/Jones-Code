import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

// Provider starts look up imported choices and runtime-stop lineage by thread.
// Both tables are keyed by command, so those lookups scanned every row.
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE INDEX jones_imported_history_choices_by_thread
    ON jones_imported_history_choices(thread_id)`;
  yield* sql`CREATE INDEX jones_runtime_stop_intents_by_thread
    ON jones_runtime_stop_intents(thread_id)`;
});
