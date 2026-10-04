import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TABLE IF NOT EXISTS native_creation_automation_enrollments (
      session_id TEXT PRIMARY KEY NOT NULL,
      enrolled_at TEXT NOT NULL
    )
  `;
  yield* sql`
    CREATE TABLE IF NOT EXISTS native_creation_intents (
      claim_id TEXT PRIMARY KEY,
      operation_id TEXT NOT NULL UNIQUE,
      preparation_id TEXT NOT NULL UNIQUE,
      command_id TEXT NOT NULL UNIQUE,
      thread_id TEXT NOT NULL UNIQUE,
      message_id TEXT NOT NULL UNIQUE,
      project_cwd TEXT NOT NULL,
      branch TEXT NOT NULL,
      worktree_path TEXT NOT NULL UNIQUE,
      canonical_preparation TEXT NOT NULL,
      intent_json TEXT NOT NULL,
      UNIQUE(project_cwd, branch)
    )
  `;
  yield* sql`
    CREATE TABLE IF NOT EXISTS native_creation_normalized_commands (
      claim_id TEXT PRIMARY KEY REFERENCES native_creation_intents(claim_id),
      command_digest TEXT NOT NULL,
      canonical_command TEXT NOT NULL
    )
  `;
  yield* sql`
    CREATE TABLE IF NOT EXISTS native_creation_reserved_commands (
      command_id TEXT PRIMARY KEY,
      claim_id TEXT NOT NULL REFERENCES native_creation_intents(claim_id),
      thread_id TEXT NOT NULL,
      command_type TEXT NOT NULL,
      command_digest TEXT NOT NULL,
      canonical_command TEXT NOT NULL
    )
  `;
  yield* sql`
    CREATE TABLE IF NOT EXISTS native_creation_effect_facts (
      claim_id TEXT NOT NULL REFERENCES native_creation_intents(claim_id),
      effect_id TEXT NOT NULL,
      phase TEXT NOT NULL CHECK(phase IN ('started', 'completed')),
      ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
      fact_json TEXT NOT NULL,
      PRIMARY KEY(claim_id, effect_id, phase),
      UNIQUE(claim_id, ordinal)
    )
  `;
  for (const table of [
    "native_creation_intents",
    "native_creation_automation_enrollments",
    "native_creation_normalized_commands",
    "native_creation_reserved_commands",
    "native_creation_effect_facts",
  ]) {
    // Native creation records never expire, transfer ownership or become replacement rows.
    yield* sql.unsafe(
      `CREATE TRIGGER IF NOT EXISTS ${table}_no_update BEFORE UPDATE ON ${table} BEGIN SELECT RAISE(ABORT, 'native creation rows are immutable'); END`,
    );
    yield* sql.unsafe(
      `CREATE TRIGGER IF NOT EXISTS ${table}_no_delete BEFORE DELETE ON ${table} BEGIN SELECT RAISE(ABORT, 'native creation rows are immutable'); END`,
    );
  }
  yield* sql`CREATE TRIGGER IF NOT EXISTS native_creation_automation_enrollments_no_replace BEFORE INSERT ON native_creation_automation_enrollments
    WHEN EXISTS (SELECT 1 FROM native_creation_automation_enrollments WHERE session_id = NEW.session_id)
    BEGIN SELECT RAISE(ABORT, 'native creation enrollment is permanent'); END`;
  yield* sql`CREATE TRIGGER IF NOT EXISTS native_creation_intents_no_replace BEFORE INSERT ON native_creation_intents
    WHEN EXISTS (SELECT 1 FROM native_creation_intents WHERE claim_id = NEW.claim_id OR operation_id = NEW.operation_id
      OR preparation_id = NEW.preparation_id OR command_id = NEW.command_id OR thread_id = NEW.thread_id OR message_id = NEW.message_id
      OR worktree_path = NEW.worktree_path OR (project_cwd = NEW.project_cwd AND branch = NEW.branch))
    BEGIN SELECT RAISE(ABORT, 'native creation claim is immutable'); END`;
  yield* sql`CREATE TRIGGER IF NOT EXISTS native_creation_normalized_commands_no_replace BEFORE INSERT ON native_creation_normalized_commands
    WHEN EXISTS (SELECT 1 FROM native_creation_normalized_commands WHERE claim_id = NEW.claim_id)
    BEGIN SELECT RAISE(ABORT, 'normalized creation command is immutable'); END`;
  yield* sql`CREATE TRIGGER IF NOT EXISTS native_creation_reserved_commands_no_replace BEFORE INSERT ON native_creation_reserved_commands
    WHEN EXISTS (SELECT 1 FROM native_creation_reserved_commands WHERE command_id = NEW.command_id)
    BEGIN SELECT RAISE(ABORT, 'reserved creation command is immutable'); END`;
  yield* sql`CREATE TRIGGER IF NOT EXISTS native_creation_effect_facts_no_replace BEFORE INSERT ON native_creation_effect_facts
    WHEN EXISTS (SELECT 1 FROM native_creation_effect_facts WHERE claim_id = NEW.claim_id
      AND ((effect_id = NEW.effect_id AND phase = NEW.phase) OR ordinal = NEW.ordinal))
    BEGIN SELECT RAISE(ABORT, 'creation effect fact is immutable'); END`;
});
