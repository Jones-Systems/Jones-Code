import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_remote_admission_grants (
    grant_id TEXT PRIMARY KEY, admission_id TEXT NOT NULL UNIQUE, binding_json TEXT NOT NULL CHECK(json_valid(binding_json)),
 managed_scope_id TEXT, destination_thread_id TEXT, session_id TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('pending','active','revoked')), generation INTEGER NOT NULL CHECK(generation > 0),
 expires_at TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, revoked_at TEXT, terminal_at TEXT
  )`;
  yield* sql`CREATE TRIGGER jones_remote_grant_binding_immutable BEFORE UPDATE ON jones_remote_admission_grants
    WHEN NEW.grant_id != OLD.grant_id OR NEW.admission_id != OLD.admission_id
      OR NEW.session_id != OLD.session_id OR NEW.created_at != OLD.created_at
      OR json_extract(NEW.binding_json,'$.receiverEnvironmentId') IS NOT json_extract(OLD.binding_json,'$.receiverEnvironmentId')
      OR json_extract(NEW.binding_json,'$.originEnvironmentId') IS NOT json_extract(OLD.binding_json,'$.originEnvironmentId')
      OR json_extract(NEW.binding_json,'$.originThreadId') IS NOT json_extract(OLD.binding_json,'$.originThreadId')
      OR json_extract(NEW.binding_json,'$.sessionId') IS NOT json_extract(OLD.binding_json,'$.sessionId')
      OR json_extract(NEW.binding_json,'$.operation') IS NOT json_extract(OLD.binding_json,'$.operation')
      OR json_extract(NEW.binding_json,'$.runtimeMode') IS NOT json_extract(OLD.binding_json,'$.runtimeMode')
      OR json_extract(NEW.binding_json,'$.interactionMode') IS NOT json_extract(OLD.binding_json,'$.interactionMode')
      OR (OLD.destination_thread_id IS NOT NULL AND NEW.destination_thread_id IS NOT OLD.destination_thread_id)
      OR (OLD.managed_scope_id IS NOT NULL AND NEW.managed_scope_id IS NOT OLD.managed_scope_id)
      OR (OLD.state='revoked' AND NEW.state!='revoked')
    BEGIN SELECT RAISE(ABORT,'Remote access binding is immutable'); END`;
  yield* sql`CREATE TABLE jones_remote_admission_result_acks (
    admission_id TEXT PRIMARY KEY, session_id TEXT NOT NULL,
    disposition TEXT NOT NULL CHECK(disposition IN ('delivered','suppressed')), acknowledged_at TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE jones_remote_admission_observation_acks (
    session_id TEXT NOT NULL, observation_key TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('launch','retry')), admission_id TEXT,
    disposition TEXT NOT NULL CHECK(disposition IN ('delivered','suppressed')), acknowledged_at TEXT NOT NULL,
    PRIMARY KEY(session_id,observation_key,kind)
  )`;
  yield* sql`CREATE TABLE jones_remote_managed_scopes (
    managed_scope_id TEXT PRIMARY KEY, receiver_environment_id TEXT NOT NULL,
 origin_environment_id TEXT NOT NULL, origin_thread_id TEXT NOT NULL, destination_thread_id TEXT NOT NULL,
 operation_class TEXT NOT NULL CHECK(operation_class = 'messages'), revision INTEGER NOT NULL CHECK(revision > 0),
 provenance_session_id TEXT NOT NULL, establishing_admission_id TEXT NOT NULL,
 UNIQUE(receiver_environment_id,origin_environment_id,origin_thread_id,destination_thread_id,operation_class)
  )`;
  yield* sql`CREATE TABLE jones_remote_admission_access_basis (
    admission_id TEXT PRIMARY KEY, managed_scope_id TEXT NOT NULL, kind TEXT NOT NULL CHECK(kind IN ('lease','permit')),
 grant_id TEXT, generation INTEGER, binding_digest TEXT, scope_revision INTEGER,
 CHECK((kind='lease' AND grant_id IS NOT NULL AND generation IS NOT NULL AND binding_digest IS NULL AND scope_revision IS NULL)
 OR (kind='permit' AND grant_id IS NULL AND generation IS NULL AND binding_digest IS NOT NULL AND scope_revision IS NOT NULL))
  )`;
  yield* sql`CREATE TABLE jones_remote_admission_blocked_marks (
    admission_id TEXT PRIMARY KEY, managed_scope_id TEXT NOT NULL, reason TEXT NOT NULL, created_at TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE jones_remote_admission_blocked_events (
    event_id TEXT PRIMARY KEY, admission_id TEXT NOT NULL, managed_scope_id TEXT NOT NULL,
 reason TEXT NOT NULL, first_at TEXT NOT NULL, last_at TEXT NOT NULL, repeat_count INTEGER NOT NULL DEFAULT 0 CHECK(repeat_count >= 0), UNIQUE(admission_id,reason)
  )`;
  yield* sql`CREATE TABLE jones_remote_admission_blocked_event_keys (
    admission_id TEXT NOT NULL, reason TEXT NOT NULL, managed_scope_id TEXT NOT NULL,
    folded INTEGER NOT NULL DEFAULT 0, last_at TEXT NOT NULL, PRIMARY KEY(admission_id,reason)
  )`;
  yield* sql`CREATE TABLE jones_remote_admission_blocked_summaries (
    managed_scope_id TEXT NOT NULL, reason TEXT NOT NULL, logical_events INTEGER NOT NULL,
 repeat_arrivals INTEGER NOT NULL, first_at TEXT NOT NULL, last_at TEXT NOT NULL, PRIMARY KEY(managed_scope_id,reason)
  )`;
  yield* sql`CREATE TABLE jones_remote_admission_grant_restores (
    predecessor_grant_id TEXT PRIMARY KEY, predecessor_generation INTEGER NOT NULL,
 successor_grant_id TEXT NOT NULL UNIQUE, command_key TEXT NOT NULL, fingerprint TEXT NOT NULL, accepted_at TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE jones_remote_admission_extensions (
    admission_id TEXT PRIMARY KEY, binding_json TEXT, evaluator_json TEXT, outcome TEXT CHECK(outcome IN ('success','failure','unknown')),
 terminal_at TEXT, attempt_id TEXT
  )`;
  yield* sql`CREATE TABLE jones_remote_admission_review_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1), revision INTEGER NOT NULL, prune_revision INTEGER NOT NULL
  )`;
  yield* sql`CREATE TABLE jones_remote_admission_changes (
    revision INTEGER PRIMARY KEY, kind TEXT NOT NULL, entity_id TEXT NOT NULL, created_at TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE jones_remote_admission_fences (
    client_id TEXT NOT NULL, idempotency_key TEXT NOT NULL, predecessor_admission_id TEXT NOT NULL,
 successor_admission_id TEXT NOT NULL, marker TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(client_id,idempotency_key)
  )`;
  yield* sql`CREATE TABLE jones_remote_admission_attempts (
    attempt_id TEXT PRIMARY KEY, root_admission_id TEXT NOT NULL, predecessor_admission_id TEXT NOT NULL UNIQUE, child_admission_id TEXT NOT NULL UNIQUE,
 session_id TEXT NOT NULL, command_key TEXT NOT NULL, fingerprint TEXT NOT NULL, locator TEXT NOT NULL UNIQUE,
 state TEXT NOT NULL CHECK(state IN ('prepared','origin_registered','claimed','completed','refused','expired')),
 root_observation_key TEXT NOT NULL, observation_key TEXT, grant_intent_json TEXT, proof_digest TEXT NOT NULL,
 created_at TEXT NOT NULL, expires_at TEXT NOT NULL, terminal_at TEXT
  )`;
  yield* sql`CREATE TABLE jones_remote_admission_recovered_results (
    admission_id TEXT NOT NULL, revision INTEGER NOT NULL, evidence_digest TEXT NOT NULL,
 result_json TEXT NOT NULL, evidence_json TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(admission_id,revision), UNIQUE(admission_id,evidence_digest)
  )`;
  yield* sql`CREATE TABLE jones_remote_admission_recovered_notices (
    receiver_environment_id TEXT NOT NULL, admission_id TEXT NOT NULL, recovered_revision INTEGER NOT NULL,
 session_id TEXT NOT NULL, root_admission_id TEXT NOT NULL, root_observation_key TEXT NOT NULL, return_binding_json TEXT NOT NULL,
 evidence_digest TEXT NOT NULL, result_json TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('awaiting_support','unregistered','registered','delivered','suppressed','undeliverable','closed_unknown')),
 observation_key TEXT, created_at TEXT NOT NULL, acknowledged_at TEXT,
 PRIMARY KEY(receiver_environment_id,admission_id,recovered_revision)
  )`;
  yield* sql`CREATE TABLE jones_remote_admission_notice_tombstones (
    receiver_environment_id TEXT NOT NULL, admission_id TEXT NOT NULL, recovered_revision INTEGER NOT NULL,
 session_id TEXT NOT NULL, evidence_digest TEXT NOT NULL, disposition TEXT NOT NULL,
    PRIMARY KEY(receiver_environment_id,admission_id,recovered_revision)
  )`;
  yield* sql`CREATE TABLE jones_remote_peer_capabilities (
    session_id TEXT NOT NULL, coordinator_generation TEXT NOT NULL, versions_json TEXT NOT NULL,
 registered_at TEXT NOT NULL, PRIMARY KEY(session_id,coordinator_generation)
  )`;
  yield* sql`CREATE TABLE jones_remote_admission_peer_revisions (
    session_id TEXT PRIMARY KEY, revision INTEGER NOT NULL
  )`;
  yield* sql`CREATE TABLE jones_remote_origin_observations (
    observation_key TEXT PRIMARY KEY, peer_environment_id TEXT NOT NULL, link_generation TEXT NOT NULL,
 session_id TEXT NOT NULL, client_request_id TEXT NOT NULL, receiver_namespace TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('launch','retry','recovered_notice')), root_observation_key TEXT NOT NULL,
 predecessor_observation_key TEXT, admission_id TEXT, attempt_id TEXT, recovered_revision INTEGER,
 return_binding_json TEXT NOT NULL, intent_json TEXT NOT NULL, result_json TEXT,
 state TEXT NOT NULL CHECK(state IN ('intent','pending','resolved','delivered','released','suppressed','closed_unknown')),
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL, terminal_at TEXT,
 UNIQUE(peer_environment_id,session_id,kind,client_request_id)
  )`;
  yield* sql`CREATE TABLE jones_remote_origin_return_anchors (
    root_observation_key TEXT PRIMARY KEY, peer_environment_id TEXT NOT NULL, session_id TEXT NOT NULL,
 return_binding_json TEXT NOT NULL, stopped_at TEXT, resolved_at TEXT, closed_unknown_at TEXT
  )`;
  yield* sql`CREATE TABLE jones_remote_grant_status_cache (
    peer_environment_id TEXT NOT NULL, link_generation TEXT NOT NULL, grant_id TEXT NOT NULL,
 ordinal INTEGER NOT NULL, status_json TEXT NOT NULL, observed_at TEXT NOT NULL,
 PRIMARY KEY(peer_environment_id,link_generation,grant_id)
  )`;
  yield* sql`INSERT INTO jones_remote_admission_review_state VALUES (1,0,0)`;
  yield* sql`CREATE INDEX jones_remote_managed_destination ON jones_remote_managed_scopes(destination_thread_id)`;
  yield* sql`CREATE INDEX jones_remote_grants_scope ON jones_remote_admission_grants(managed_scope_id,session_id,state)`;
  yield* sql`CREATE INDEX jones_remote_blocked_scope ON jones_remote_admission_blocked_events(managed_scope_id,last_at,event_id)`;
  yield* sql`CREATE INDEX jones_remote_attempt_session ON jones_remote_admission_attempts(session_id,state)`;
  yield* sql`CREATE INDEX jones_remote_notice_session ON jones_remote_admission_recovered_notices(session_id,state)`;
});
