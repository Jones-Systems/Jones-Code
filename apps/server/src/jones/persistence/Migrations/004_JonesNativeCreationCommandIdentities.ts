import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS native_creation_reserved_command_identities (
    command_id TEXT PRIMARY KEY NOT NULL,
    claim_id TEXT NOT NULL REFERENCES native_creation_intents(claim_id),
    thread_id TEXT NOT NULL
  )`;
  yield* sql`CREATE TRIGGER IF NOT EXISTS native_creation_reserved_command_identities_no_update
    BEFORE UPDATE ON native_creation_reserved_command_identities
    BEGIN SELECT RAISE(ABORT, 'native creation command identities are immutable'); END`;
  yield* sql`CREATE TRIGGER IF NOT EXISTS native_creation_reserved_command_identities_no_delete
    BEFORE DELETE ON native_creation_reserved_command_identities
    BEGIN SELECT RAISE(ABORT, 'native creation command identities are immutable'); END`;
  yield* sql`CREATE TRIGGER IF NOT EXISTS native_creation_reserved_command_identities_no_replace
    BEFORE INSERT ON native_creation_reserved_command_identities
    WHEN EXISTS (SELECT 1 FROM native_creation_reserved_command_identities WHERE command_id = NEW.command_id)
    BEGIN SELECT RAISE(ABORT, 'native creation command identity ownership is permanent'); END`;
});
