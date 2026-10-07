import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_deletion_worktree_admissions (
    effect_id TEXT PRIMARY KEY NOT NULL,
    worktree_path TEXT UNIQUE NOT NULL,
    binding_json TEXT NOT NULL CHECK(json_valid(binding_json)),
    start_json TEXT NOT NULL CHECK(json_valid(start_json)),
    state TEXT NOT NULL CHECK(state IN ('started','unknown','completed'))
  )`;
  yield* sql`CREATE TABLE jones_deletion_worktree_observations (
    effect_id TEXT NOT NULL REFERENCES jones_deletion_worktree_admissions(effect_id),
    ordinal INTEGER NOT NULL CHECK(ordinal > 0),
    observation_json TEXT NOT NULL CHECK(json_valid(observation_json)),
    result TEXT NOT NULL CHECK(result IN ('confirmed','absent','unknown')),
    PRIMARY KEY(effect_id,ordinal)
  )`;
  yield* sql`CREATE TRIGGER jones_deletion_admission_identity BEFORE UPDATE ON jones_deletion_worktree_admissions
    WHEN NEW.effect_id != OLD.effect_id OR NEW.worktree_path != OLD.worktree_path OR NEW.binding_json != OLD.binding_json OR NEW.start_json != OLD.start_json
    BEGIN SELECT RAISE(ABORT,'deletion original ownership is immutable'); END`;
  yield* sql`CREATE TRIGGER jones_deletion_admission_permanent BEFORE DELETE ON jones_deletion_worktree_admissions
    BEGIN SELECT RAISE(ABORT,'deletion original ownership is permanent'); END`;
  yield* sql`CREATE TRIGGER jones_deletion_admission_no_replace BEFORE INSERT ON jones_deletion_worktree_admissions
    WHEN EXISTS(SELECT 1 FROM jones_deletion_worktree_admissions WHERE effect_id=NEW.effect_id OR worktree_path=NEW.worktree_path)
    BEGIN SELECT RAISE(ABORT,'deletion original ownership cannot be replaced'); END`;
  yield* sql`CREATE TRIGGER jones_deletion_observation_no_update BEFORE UPDATE ON jones_deletion_worktree_observations
    BEGIN SELECT RAISE(ABORT,'deletion observations are immutable'); END`;
  yield* sql`CREATE TRIGGER jones_deletion_observation_no_delete BEFORE DELETE ON jones_deletion_worktree_observations
    BEGIN SELECT RAISE(ABORT,'deletion observations are immutable'); END`;
});
