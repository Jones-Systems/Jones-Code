import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_native_workspace_admissions (
    claim_id TEXT PRIMARY KEY NOT NULL REFERENCES native_creation_intents(claim_id),
    worktree_path TEXT UNIQUE NOT NULL,
    basis_json TEXT NOT NULL CHECK(json_valid(basis_json))
  )`;
  yield* sql`CREATE TABLE jones_native_workspace_verified (
    claim_id TEXT PRIMARY KEY NOT NULL REFERENCES jones_native_workspace_admissions(claim_id),
    receipt_json TEXT NOT NULL CHECK(json_valid(receipt_json))
  )`;
  for (const table of ["jones_native_workspace_admissions", "jones_native_workspace_verified"]) {
    yield* sql`CREATE TRIGGER ${sql(`${table}_no_update`)} BEFORE UPDATE ON ${sql(table)} BEGIN SELECT RAISE(ABORT, 'native workspace ownership is immutable'); END`;
    yield* sql`CREATE TRIGGER ${sql(`${table}_no_delete`)} BEFORE DELETE ON ${sql(table)} BEGIN SELECT RAISE(ABORT, 'native workspace ownership is permanent'); END`;
    yield* sql`CREATE TRIGGER ${sql(`${table}_no_replace`)} BEFORE INSERT ON ${sql(table)} WHEN EXISTS (SELECT 1 FROM ${sql(table)} WHERE claim_id = NEW.claim_id) BEGIN SELECT RAISE(ABORT, 'native workspace ownership cannot be replaced'); END`;
  }
});
