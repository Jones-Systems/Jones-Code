import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_thread_links (
    link_id TEXT PRIMARY KEY NOT NULL CHECK (length(link_id) = 68 AND substr(link_id,1,4) = 'tl1:'),
    kind TEXT NOT NULL CHECK (kind IN ('launch','continuation','peer')),
    from_environment_id TEXT NOT NULL, from_thread_id TEXT NOT NULL,
    to_environment_id TEXT NOT NULL, to_thread_id TEXT NOT NULL,
    basis TEXT NOT NULL CHECK (basis IN ('launch','handoff','manual','recovered')),
    asserted_by TEXT NOT NULL, launch_command_id TEXT,
    from_title TEXT CHECK (from_title IS NULL OR length(from_title) <= 512),
    to_title TEXT CHECK (to_title IS NULL OR length(to_title) <= 512),
    remote_environment_label TEXT CHECK (remote_environment_label IS NULL OR length(remote_environment_label) <= 256),
    reverse_state TEXT NOT NULL CHECK (reverse_state IN ('local','recorded','unavailable','unknown')),
    acceptance TEXT CHECK (acceptance IS NULL OR acceptance IN ('offered','accepted')),
    handoff_json TEXT CHECK (handoff_json IS NULL OR (json_valid(handoff_json) AND length(handoff_json) <= 65536)),
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL, accepted_at TEXT, removed_at TEXT,
    CHECK (NOT (from_environment_id = to_environment_id AND from_thread_id = to_thread_id)),
    CHECK ((kind = 'launch') = (basis IN ('launch','recovered'))),
    CHECK (kind = 'continuation' OR acceptance IS NULL),
    CHECK (acceptance IS 'accepted' OR accepted_at IS NULL),
    CHECK ((reverse_state = 'local') = (from_environment_id = to_environment_id)),
    CHECK (kind = 'launch' OR launch_command_id IS NULL)
  )`;
  yield* sql`CREATE INDEX jones_thread_links_from ON jones_thread_links (from_environment_id, from_thread_id)`;
  yield* sql`CREATE INDEX jones_thread_links_to ON jones_thread_links (to_environment_id, to_thread_id)`;
  yield* sql`CREATE TRIGGER jones_thread_links_identity BEFORE UPDATE ON jones_thread_links
    WHEN NEW.link_id IS NOT OLD.link_id OR NEW.kind IS NOT OLD.kind OR
      NEW.from_environment_id IS NOT OLD.from_environment_id OR NEW.from_thread_id IS NOT OLD.from_thread_id OR
      NEW.to_environment_id IS NOT OLD.to_environment_id OR NEW.to_thread_id IS NOT OLD.to_thread_id OR
      NEW.basis IS NOT OLD.basis OR NEW.asserted_by IS NOT OLD.asserted_by OR
      NEW.launch_command_id IS NOT OLD.launch_command_id OR NEW.created_at IS NOT OLD.created_at
    BEGIN SELECT RAISE(ABORT, 'Thread link identity is immutable'); END`;
  yield* sql`CREATE TRIGGER jones_thread_links_acceptance BEFORE UPDATE ON jones_thread_links
    WHEN OLD.acceptance = 'accepted' AND NEW.acceptance IS NOT 'accepted'
    BEGIN SELECT RAISE(ABORT, 'Accepted thread ownership cannot revert'); END`;
  // A tombstone must survive retries, recovery and REPLACE even with recursive triggers disabled.
  yield* sql`CREATE TRIGGER jones_thread_links_no_delete BEFORE DELETE ON jones_thread_links
    BEGIN SELECT RAISE(ABORT, 'Thread links must be soft removed'); END`;
  yield* sql`CREATE TRIGGER jones_thread_links_no_replace BEFORE INSERT ON jones_thread_links
    WHEN EXISTS (SELECT 1 FROM jones_thread_links WHERE link_id = NEW.link_id)
    BEGIN SELECT RAISE(IGNORE); END`;
});
