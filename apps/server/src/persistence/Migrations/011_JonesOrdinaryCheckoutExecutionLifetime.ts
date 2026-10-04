import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`CREATE TABLE orchestration_v2_ordinary_checkout_execution_associations (
    operation_id TEXT NOT NULL REFERENCES orchestration_v2_worktree_path_admissions(operation_id),
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
    predecessor_ordinal INTEGER,
    association_id TEXT NOT NULL CHECK(length(association_id) = 64 AND association_id NOT GLOB '*[^0-9a-f]*'),
    admission_id TEXT NOT NULL REFERENCES orchestration_v2_ordinary_checkout_admissions(admission_id),
    executor_kind TEXT NOT NULL CHECK(executor_kind IN ('actual_outbox_claim', 'captured_managed_run', 'actual_prepared_producer')),
    effect_id TEXT REFERENCES orchestration_v2_effect_outbox(effect_id),
    event_kind TEXT NOT NULL CHECK(event_kind IN ('bind', 'activate', 'join', 'renew', 'retire', 'unknown')),
    association_json TEXT NOT NULL CHECK(json_valid(association_json) AND json_type(association_json) = 'object'
      AND json_type(association_json, '$.version') IS 'integer' AND json_extract(association_json, '$.version') = 1),
    evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json) AND json_type(evidence_json) = 'object'),
    recorded_at TEXT NOT NULL CHECK(length(recorded_at) > 0),
    PRIMARY KEY(operation_id, ordinal),
    FOREIGN KEY(operation_id, predecessor_ordinal)
      REFERENCES orchestration_v2_ordinary_checkout_execution_associations(operation_id, ordinal),
    CHECK((ordinal = 0 AND predecessor_ordinal IS NULL)
      OR (ordinal > 0 AND predecessor_ordinal IS NOT NULL AND predecessor_ordinal = ordinal - 1))
  )`;
  yield* sql`CREATE INDEX orchestration_v2_ordinary_checkout_execution_associations_participant_idx
    ON orchestration_v2_ordinary_checkout_execution_associations(operation_id, association_id, ordinal)`;
  yield* sql`CREATE TRIGGER orchestration_v2_ordinary_checkout_execution_associations_no_update
    BEFORE UPDATE ON orchestration_v2_ordinary_checkout_execution_associations
    BEGIN SELECT RAISE(ABORT, 'ordinary checkout execution associations are immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_ordinary_checkout_execution_associations_no_delete
    BEFORE DELETE ON orchestration_v2_ordinary_checkout_execution_associations
    BEGIN SELECT RAISE(ABORT, 'ordinary checkout execution associations are permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_ordinary_checkout_execution_associations_no_replace
    BEFORE INSERT ON orchestration_v2_ordinary_checkout_execution_associations
    WHEN EXISTS (SELECT 1 FROM orchestration_v2_ordinary_checkout_execution_associations
      WHERE operation_id = NEW.operation_id AND ordinal = NEW.ordinal)
    BEGIN SELECT RAISE(ABORT, 'ordinary checkout execution succession is permanent'); END`;
});
