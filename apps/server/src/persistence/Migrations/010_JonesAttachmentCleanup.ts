import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`CREATE TABLE orchestration_v2_attachment_cleanup_observations (
    effect_id TEXT NOT NULL REFERENCES orchestration_v2_effect_outbox(effect_id),
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
    binding_sha256 TEXT NOT NULL CHECK(length(binding_sha256) = 64 AND binding_sha256 NOT GLOB '*[^0-9a-f]*'),
    canonical_task_json TEXT NOT NULL CHECK(json_valid(canonical_task_json) AND json_type(canonical_task_json) = 'object'),
    observation_json TEXT NOT NULL CHECK(json_valid(observation_json) AND json_type(observation_json) = 'object'),
    correlation_json TEXT NOT NULL CHECK(json_valid(correlation_json) AND json_type(correlation_json) = 'object'),
    recorded_at TEXT NOT NULL CHECK(length(recorded_at) > 0),
    PRIMARY KEY(effect_id, ordinal)
  )`;
  yield* sql`CREATE TRIGGER orchestration_v2_attachment_cleanup_observations_no_update
    BEFORE UPDATE ON orchestration_v2_attachment_cleanup_observations
    BEGIN SELECT RAISE(ABORT, 'attachment cleanup observations are immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_attachment_cleanup_observations_no_delete
    BEFORE DELETE ON orchestration_v2_attachment_cleanup_observations
    BEGIN SELECT RAISE(ABORT, 'attachment cleanup observations are permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_attachment_cleanup_observations_no_replace
    BEFORE INSERT ON orchestration_v2_attachment_cleanup_observations
    WHEN EXISTS (SELECT 1 FROM orchestration_v2_attachment_cleanup_observations
      WHERE effect_id = NEW.effect_id AND ordinal = NEW.ordinal)
    BEGIN SELECT RAISE(ABORT, 'attachment cleanup observation ownership is permanent'); END`;
});
