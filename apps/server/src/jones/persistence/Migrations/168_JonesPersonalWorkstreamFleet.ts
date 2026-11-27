import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_personal_workstream_fleet (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    authority_environment_id TEXT NOT NULL,
    authority_session_id TEXT,
    roster_revision INTEGER NOT NULL DEFAULT 0 CHECK (roster_revision >= 0)
  )`;
  yield* sql`CREATE TABLE jones_personal_workstream_fleet_members (
    environment_id TEXT PRIMARY KEY NOT NULL,
    link_generation TEXT NOT NULL,
    discovered INTEGER NOT NULL DEFAULT 0 CHECK (discovered IN (0, 1))
  ) WITHOUT ROWID`;
  yield* sql`CREATE TABLE jones_personal_workstream_fleet_proposals (
    session_id TEXT PRIMARY KEY NOT NULL,
    authority_environment_id TEXT NOT NULL,
    label TEXT NOT NULL
  ) WITHOUT ROWID`;
  yield* sql`CREATE TABLE jones_personal_workstream_fleet_received (
    authority_environment_id TEXT NOT NULL,
    workstream_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    request_id TEXT NOT NULL,
    PRIMARY KEY (authority_environment_id, workstream_id)
  ) WITHOUT ROWID`;
  yield* sql`CREATE TABLE jones_personal_workstream_operations (
    ordinal INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL UNIQUE,
    workstream_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('put', 'close', 'delete', 'recovery')),
    input_json TEXT NOT NULL CHECK (json_valid(input_json)),
    base_json TEXT CHECK (base_json IS NULL OR json_valid(base_json)),
    candidate_json TEXT NOT NULL CHECK (json_valid(candidate_json)),
    roster_revision INTEGER NOT NULL CHECK (roster_revision >= 0),
    replaces_request_id TEXT,
    reason TEXT,
    status TEXT NOT NULL CHECK (status IN ('pending', 'discovered', 'applied', 'stale', 'cancelled', 'superseded'))
  )`;
  yield* sql`CREATE INDEX jones_personal_workstream_operations_by_workstream
    ON jones_personal_workstream_operations (workstream_id, status, ordinal)`;
  yield* sql`CREATE TABLE jones_personal_workstream_operation_peers (
    request_id TEXT NOT NULL REFERENCES jones_personal_workstream_operations(request_id),
    environment_id TEXT NOT NULL,
    link_generation TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'delivered', 'guard_rejected', 'conflict', 'blocked', 'unknown')),
    reason TEXT,
    observed_json TEXT CHECK (observed_json IS NULL OR json_valid(observed_json)),
    approved_base_json TEXT CHECK (approved_base_json IS NULL OR json_valid(approved_base_json)),
    attempted INTEGER NOT NULL DEFAULT 0 CHECK (attempted IN (0, 1)),
    PRIMARY KEY (request_id, environment_id)
  ) WITHOUT ROWID`;
});
