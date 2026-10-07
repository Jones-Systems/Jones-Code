import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_runtime_stop_intents (command_id TEXT PRIMARY KEY NOT NULL REFERENCES orchestration_command_receipts(command_id),thread_id TEXT NOT NULL,identity_json TEXT NOT NULL CHECK(json_valid(identity_json)))`;
  yield* sql`CREATE TABLE jones_runtime_stop_fences (command_id TEXT NOT NULL REFERENCES jones_runtime_stop_intents(command_id),thread_id TEXT NOT NULL,run_id TEXT NOT NULL,provider_thread_id TEXT NOT NULL,runtime_generation TEXT NOT NULL,PRIMARY KEY(command_id,run_id))`;
  yield* sql`CREATE INDEX jones_runtime_stop_fences_by_run ON jones_runtime_stop_fences(thread_id,run_id)`;
  yield* sql`CREATE TABLE jones_runtime_stop_observations (command_id TEXT NOT NULL REFERENCES jones_runtime_stop_intents(command_id),phase TEXT NOT NULL CHECK(phase IN ('started','completed')),result TEXT NOT NULL CHECK(result IN ('unknown','stopped')),PRIMARY KEY(command_id,phase))`;
  for (const table of [
    "jones_runtime_stop_intents",
    "jones_runtime_stop_fences",
    "jones_runtime_stop_observations",
  ]) {
    yield* sql`CREATE TRIGGER ${sql(table + "_no_update")} BEFORE UPDATE ON ${sql(table)} BEGIN SELECT RAISE(ABORT,'captured runtime stop identity is immutable'); END`;
    yield* sql`CREATE TRIGGER ${sql(table + "_no_delete")} BEFORE DELETE ON ${sql(table)} BEGIN SELECT RAISE(ABORT,'captured runtime stop fence is permanent'); END`;
  }
  yield* sql`CREATE TRIGGER jones_runtime_stop_intents_no_replace BEFORE INSERT ON jones_runtime_stop_intents WHEN EXISTS(SELECT 1 FROM jones_runtime_stop_intents WHERE command_id=NEW.command_id) BEGIN SELECT RAISE(ABORT,'runtime stop intent cannot be replaced'); END`;
  yield* sql`CREATE TRIGGER jones_runtime_stop_fences_no_replace BEFORE INSERT ON jones_runtime_stop_fences WHEN EXISTS(SELECT 1 FROM jones_runtime_stop_fences WHERE command_id=NEW.command_id AND run_id=NEW.run_id) BEGIN SELECT RAISE(ABORT,'runtime stop fence cannot be replaced'); END`;
  yield* sql`CREATE TRIGGER jones_runtime_stop_observations_no_replace BEFORE INSERT ON jones_runtime_stop_observations WHEN EXISTS(SELECT 1 FROM jones_runtime_stop_observations WHERE command_id=NEW.command_id AND phase=NEW.phase) BEGIN SELECT RAISE(ABORT,'runtime stop observation cannot be replaced'); END`;
});
