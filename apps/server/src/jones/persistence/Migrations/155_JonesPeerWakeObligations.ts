import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_peer_wake_bindings (
    dependency_id TEXT NOT NULL, generation INTEGER NOT NULL,
    thread_id TEXT NOT NULL, message_id TEXT NOT NULL, run_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL, waiter_epoch INTEGER NOT NULL, admitted_sequence INTEGER NOT NULL,
    PRIMARY KEY (dependency_id, generation), UNIQUE (thread_id, run_id, attempt_id)
  )`;
  yield* sql`CREATE TABLE jones_peer_wake_holds (
    dependency_id TEXT NOT NULL, generation INTEGER NOT NULL,
    refused_run_id TEXT NOT NULL, waiter_epoch INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('held','armed','consumed','void')),
    created_sequence INTEGER NOT NULL, release_event_id TEXT, release_sequence INTEGER,
    PRIMARY KEY (dependency_id, generation),
    CHECK ((release_event_id IS NULL) = (release_sequence IS NULL))
  )`;
  yield* sql`CREATE UNIQUE INDEX jones_peer_wake_release_event ON jones_peer_wake_holds(release_event_id)
    WHERE release_event_id IS NOT NULL`;
});
