import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_conversation_bindings(thread_id TEXT PRIMARY KEY, binding TEXT NOT NULL CHECK(json_valid(binding)), active_run_id TEXT)`;
  yield* sql`CREATE INDEX jones_conversation_binding_path ON jones_conversation_bindings(json_extract(binding, '$.path'), json_extract(binding, '$.state'))`;
  yield* sql`CREATE TABLE jones_conversation_turns(run_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, snapshot TEXT NOT NULL CHECK(json_valid(snapshot)))`;
  yield* sql`CREATE TABLE jones_conversation_fallbacks(thread_id TEXT PRIMARY KEY, driver TEXT NOT NULL, reason TEXT NOT NULL)`;
  yield* sql`CREATE TABLE jones_conversation_handoffs(thread_id TEXT PRIMARY KEY, record TEXT NOT NULL CHECK(json_valid(record)))`;
  yield* sql`CREATE TABLE jones_conversation_obligations(path TEXT PRIMARY KEY, thread_id TEXT NOT NULL, reason TEXT NOT NULL)`;
});
