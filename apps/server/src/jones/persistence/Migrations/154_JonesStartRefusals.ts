import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_start_refusal_attempts (
    thread_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    scope TEXT NOT NULL CHECK (scope IN ('delegated_completion', 'dependency_wake')),
    state TEXT NOT NULL CHECK (state IN ('eligible', 'crossed', 'refused')),
    created_sequence INTEGER NOT NULL,
    reason TEXT CHECK (reason IN ('restart_admission', 'continuity_unavailable', 'continuity_held', 'control_held')),
    settlement_event_id TEXT,
    PRIMARY KEY (thread_id, run_id, attempt_id),
    CHECK ((state = 'refused' AND reason IS NOT NULL AND settlement_event_id IS NOT NULL)
      OR (state <> 'refused' AND reason IS NULL AND settlement_event_id IS NULL))
  )`;
  yield* sql`CREATE TABLE jones_start_refusal_holds (
    thread_id TEXT NOT NULL,
    parent_run_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    message_id TEXT NOT NULL,
    source_generation INTEGER NOT NULL,
    refused_run_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('held', 'armed', 'consumed', 'void')),
    created_sequence INTEGER NOT NULL,
    release_event_id TEXT,
    release_sequence INTEGER,
    PRIMARY KEY (thread_id, parent_run_id, generation, message_id),
    UNIQUE (thread_id, parent_run_id, source_generation),
    CHECK ((release_event_id IS NULL) = (release_sequence IS NULL))
  )`;
  yield* sql`CREATE UNIQUE INDEX jones_start_refusal_release_event
    ON jones_start_refusal_holds(release_event_id) WHERE release_event_id IS NOT NULL`;
});
