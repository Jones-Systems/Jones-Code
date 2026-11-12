import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_planned_update_continuity (
    operation_id TEXT PRIMARY KEY NOT NULL,
    binding_json TEXT NOT NULL CHECK(json_valid(binding_json)),
    activated INTEGER NOT NULL DEFAULT 0 CHECK(activated IN (0,1))
  )`;
  yield* sql`CREATE TABLE jones_planned_update_threads (
    operation_id TEXT NOT NULL REFERENCES jones_planned_update_continuity(operation_id),
    thread_id TEXT NOT NULL,
    snapshot_json TEXT NOT NULL CHECK(json_valid(snapshot_json)),
    queue_done INTEGER NOT NULL DEFAULT 0 CHECK(queue_done IN (0,1)),
    work_done INTEGER NOT NULL DEFAULT 0 CHECK(work_done IN (0,1)),
    PRIMARY KEY(operation_id,thread_id)
  )`;
  yield* sql`CREATE INDEX jones_planned_update_threads_by_thread ON jones_planned_update_threads(thread_id,operation_id)`;
  yield* sql`CREATE TRIGGER jones_planned_update_identity_immutable BEFORE UPDATE OF operation_id,binding_json ON jones_planned_update_continuity
    BEGIN SELECT RAISE(ABORT,'planned update binding is immutable'); END`;
  yield* sql`CREATE TRIGGER jones_planned_update_activation_once BEFORE UPDATE OF activated ON jones_planned_update_continuity
    WHEN OLD.activated=1 AND NEW.activated<>1
    BEGIN SELECT RAISE(ABORT,'planned update activation is one-shot'); END`;
  yield* sql`CREATE TRIGGER jones_planned_update_snapshot_immutable BEFORE UPDATE OF operation_id,thread_id,snapshot_json ON jones_planned_update_threads
    BEGIN SELECT RAISE(ABORT,'planned update eligibility is immutable'); END`;
});
