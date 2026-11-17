import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

// Receiver-side accepted SEND facts only. Authentication, strict result decoding and
// insertion in the accepted-event transaction belong to the core receipt store.
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_remote_send_receipts (
    command_id TEXT PRIMARY KEY NOT NULL CHECK(length(command_id) > 0),
    origin_namespace TEXT NOT NULL CHECK(length(origin_namespace) > 0),
    client_request_id TEXT NOT NULL CHECK(length(client_request_id) > 0),
    request_digest TEXT NOT NULL CHECK(
      length(request_digest) = 64 AND length(CAST(request_digest AS BLOB)) = 64
      AND request_digest NOT GLOB '*[^0-9a-f]*'
    ),
    thread_id TEXT NOT NULL CHECK(length(thread_id) > 0),
    expected_created_at TEXT CHECK(expected_created_at IS NULL OR length(expected_created_at) > 0),
    result_json TEXT NOT NULL CHECK(json_valid(result_json)),
    recorded_at TEXT NOT NULL CHECK(length(recorded_at) > 0)
  )`;
  yield* sql`CREATE INDEX jones_remote_send_receipts_lookup
    ON jones_remote_send_receipts(origin_namespace, thread_id, client_request_id)`;
  yield* sql`CREATE INDEX jones_remote_send_receipts_recorded_at
    ON jones_remote_send_receipts(recorded_at)`;
  yield* sql`CREATE TRIGGER jones_remote_send_receipts_no_update
    BEFORE UPDATE ON jones_remote_send_receipts
    BEGIN SELECT RAISE(ABORT, 'Remote SEND receipts are immutable'); END`;
  // REPLACE does not fire UPDATE triggers. Replay must look up/compare the original
  // in the owning transaction before attempting a plain insert.
  yield* sql`CREATE TRIGGER jones_remote_send_receipts_no_replace
    BEFORE INSERT ON jones_remote_send_receipts
    WHEN EXISTS (SELECT 1 FROM jones_remote_send_receipts WHERE command_id = NEW.command_id)
    BEGIN SELECT RAISE(ABORT, 'Remote SEND receipt command already recorded'); END`;
});
