import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

// This schema belongs to the Jones migration loader. Merely constructing the store never creates or migrates it.
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_workspace_lifecycle_transitions (
    transition_id TEXT PRIMARY KEY NOT NULL,
    worktree TEXT NOT NULL,
    source_json TEXT NOT NULL,
    binding_json TEXT NOT NULL CHECK(json_valid(binding_json)),
    expected_generation TEXT,
    confirmed_generation TEXT,
    state TEXT NOT NULL CHECK(state IN ('prepared','invoking','confirmed')),
    CHECK((state='confirmed')=(confirmed_generation IS NOT NULL))
  )`;
  yield* sql`CREATE UNIQUE INDEX jones_workspace_lifecycle_pending ON jones_workspace_lifecycle_transitions(worktree) WHERE state!='confirmed'`;
  yield* sql`CREATE TABLE jones_workspace_lifecycle_bindings (
    worktree TEXT PRIMARY KEY NOT NULL,
    transition_id TEXT NOT NULL REFERENCES jones_workspace_lifecycle_transitions(transition_id),
    thread_id TEXT NOT NULL
  )`;
  yield* sql`CREATE INDEX jones_workspace_lifecycle_binding_thread ON jones_workspace_lifecycle_bindings(thread_id)`;
  yield* sql`CREATE TRIGGER jones_workspace_lifecycle_identity BEFORE UPDATE ON jones_workspace_lifecycle_transitions
    WHEN NEW.transition_id IS NOT OLD.transition_id OR NEW.worktree IS NOT OLD.worktree OR
      NEW.source_json IS NOT OLD.source_json OR NEW.binding_json IS NOT OLD.binding_json OR
      NEW.expected_generation IS NOT OLD.expected_generation OR
      (OLD.state='confirmed' AND (NEW.state IS NOT OLD.state OR NEW.confirmed_generation IS NOT OLD.confirmed_generation)) OR
      (OLD.state='invoking' AND NEW.state='prepared')
    BEGIN SELECT RAISE(ABORT,'Workspace lifecycle transition identity is immutable'); END`;
  yield* sql`CREATE TRIGGER jones_workspace_lifecycle_no_delete BEFORE DELETE ON jones_workspace_lifecycle_transitions
    BEGIN SELECT RAISE(ABORT,'Workspace lifecycle transitions must be retained'); END`;
  yield* sql`CREATE TRIGGER jones_workspace_lifecycle_no_replace BEFORE INSERT ON jones_workspace_lifecycle_transitions
    WHEN EXISTS(SELECT 1 FROM jones_workspace_lifecycle_transitions WHERE transition_id=NEW.transition_id)
    BEGIN SELECT RAISE(IGNORE); END`;
});
