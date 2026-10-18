import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

// Empty extension storage only. Foreign 009/011 history is deliberately not adopted.
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_ordinary_checkout_owners (
    checkout_key TEXT PRIMARY KEY NOT NULL CHECK(length(checkout_key)>0),
    project_root TEXT NOT NULL CHECK(length(project_root)>0),
    directory_identity_json TEXT NOT NULL CHECK(json_valid(directory_identity_json)),
    git_common_directory TEXT CHECK(git_common_directory IS NULL OR length(git_common_directory)>0),
    lease_id TEXT UNIQUE NOT NULL CHECK(length(lease_id)>0),
    owner_thread_id TEXT NOT NULL CHECK(length(owner_thread_id)>0),
    birth_event_id TEXT NOT NULL CHECK(length(birth_event_id)>0),
    birth_sequence INTEGER NOT NULL CHECK(typeof(birth_sequence)='integer' AND birth_sequence>=0),
    branch TEXT CHECK(branch IS NULL OR length(branch)>0),
    acquired_at TEXT NOT NULL, renewed_at TEXT NOT NULL, expires_at TEXT NOT NULL,
    owner_epoch INTEGER NOT NULL CHECK(typeof(owner_epoch)='integer' AND owner_epoch>=0),
    target_revision INTEGER NOT NULL CHECK(typeof(target_revision)='integer' AND target_revision>=0),
    revision INTEGER NOT NULL CHECK(typeof(revision)='integer' AND revision>=0),
    state TEXT NOT NULL CHECK(state IN ('owned','unknown','released')),
    CHECK(renewed_at>=acquired_at AND expires_at>=renewed_at),
    CHECK(strftime('%Y-%m-%dT%H:%M:%fZ',acquired_at) IS acquired_at),
    CHECK(strftime('%Y-%m-%dT%H:%M:%fZ',renewed_at) IS renewed_at),
    CHECK(strftime('%Y-%m-%dT%H:%M:%fZ',expires_at) IS expires_at)
  )`;
  yield* sql`CREATE INDEX jones_ordinary_checkout_owner_state ON jones_ordinary_checkout_owners(state,checkout_key)`;
  yield* sql`CREATE TABLE jones_ordinary_checkout_admissions (
    admission_id TEXT PRIMARY KEY NOT NULL CHECK(length(admission_id)>0),
    command_id TEXT NOT NULL CHECK(length(command_id)>0),
    receipt_thread_id TEXT NOT NULL CHECK(length(receipt_thread_id)>0),
    subject_thread_id TEXT NOT NULL CHECK(length(subject_thread_id)>0),
    command_type TEXT NOT NULL CHECK(length(command_type)>0),
    command_digest TEXT NOT NULL CHECK(length(command_digest)=64 AND command_digest NOT GLOB '*[^a-f0-9]*'),
    origin_kind TEXT NOT NULL CHECK(origin_kind IN ('command','queue','delegated','scheduled','prepared')),
    origin_parent_thread_id TEXT CHECK(origin_parent_thread_id IS NULL OR length(origin_parent_thread_id)>0), request_id TEXT CHECK(request_id IS NULL OR length(request_id)>0),
    checkout_key TEXT NOT NULL REFERENCES jones_ordinary_checkout_owners(checkout_key),
    lease_id TEXT NOT NULL CHECK(length(lease_id)>0),
    owner_epoch INTEGER NOT NULL CHECK(typeof(owner_epoch)='integer' AND owner_epoch>=0), target_revision INTEGER NOT NULL CHECK(typeof(target_revision)='integer' AND target_revision>=0),
    birth_event_id TEXT NOT NULL CHECK(length(birth_event_id)>0), birth_sequence INTEGER NOT NULL CHECK(typeof(birth_sequence)='integer' AND birth_sequence>=0),
    receipt_sequence INTEGER NOT NULL CHECK(typeof(receipt_sequence)='integer' AND receipt_sequence>=0),
    basis_event_id TEXT NOT NULL CHECK(length(basis_event_id)>0), basis_sequence INTEGER NOT NULL CHECK(typeof(basis_sequence)='integer' AND basis_sequence>=0),
    run_id TEXT CHECK(run_id IS NULL OR length(run_id)>0), attempt_id TEXT CHECK(attempt_id IS NULL OR length(attempt_id)>0), node_id TEXT CHECK(node_id IS NULL OR length(node_id)>0), message_id TEXT CHECK(message_id IS NULL OR length(message_id)>0), checkpoint_scope_id TEXT CHECK(checkpoint_scope_id IS NULL OR length(checkpoint_scope_id)>0),
    recorded_at TEXT NOT NULL CHECK(strftime('%Y-%m-%dT%H:%M:%fZ',recorded_at) IS recorded_at),
    UNIQUE(command_id,subject_thread_id,checkout_key)
  )`;
  yield* sql`CREATE INDEX jones_ordinary_checkout_admission_checkout ON jones_ordinary_checkout_admissions(checkout_key,admission_id)`;
  yield* sql`CREATE INDEX jones_ordinary_checkout_admission_run ON jones_ordinary_checkout_admissions(subject_thread_id,run_id)`;
  yield* sql`CREATE TABLE jones_ordinary_checkout_effect_links (
    effect_id TEXT NOT NULL CHECK(length(effect_id)>0),
    admission_id TEXT NOT NULL REFERENCES jones_ordinary_checkout_admissions(admission_id),
    request_digest TEXT NOT NULL CHECK(length(request_digest)=64 AND request_digest NOT GLOB '*[^a-f0-9]*'),
    role TEXT NOT NULL CHECK(role IN ('launch','control','checkpoint','rollback','continuation','preparation')),
    recorded_at TEXT NOT NULL CHECK(strftime('%Y-%m-%dT%H:%M:%fZ',recorded_at) IS recorded_at), PRIMARY KEY(effect_id,admission_id)
  )`;
  yield* sql`CREATE INDEX jones_ordinary_checkout_link_admission ON jones_ordinary_checkout_effect_links(admission_id,effect_id)`;
  yield* sql`CREATE TABLE jones_ordinary_checkout_uses (
    operation_id TEXT PRIMARY KEY NOT NULL CHECK(length(operation_id)>0),
    admission_id TEXT NOT NULL REFERENCES jones_ordinary_checkout_admissions(admission_id),
    checkout_key TEXT NOT NULL REFERENCES jones_ordinary_checkout_owners(checkout_key),
    lease_id TEXT NOT NULL CHECK(length(lease_id)>0), owner_epoch INTEGER NOT NULL CHECK(typeof(owner_epoch)='integer' AND owner_epoch>=0),
    source_kind TEXT NOT NULL CHECK(source_kind IN ('outbox','prepared_run','prepared_launch')),
    effect_id TEXT, worker_id TEXT, attempt_count INTEGER, claim_expires_at TEXT, producer_id TEXT,
    state TEXT NOT NULL CHECK(state IN ('reserved','started','draining','completed','no_effect','unknown')),
    revision INTEGER NOT NULL CHECK(typeof(revision)='integer' AND revision>=0), last_event_ordinal INTEGER NOT NULL CHECK(typeof(last_event_ordinal)='integer' AND last_event_ordinal>=0),
    started_at TEXT CHECK(started_at IS NULL OR strftime('%Y-%m-%dT%H:%M:%fZ',started_at) IS started_at),
    settled_at TEXT CHECK(settled_at IS NULL OR strftime('%Y-%m-%dT%H:%M:%fZ',settled_at) IS settled_at),
    CHECK(claim_expires_at IS NULL OR strftime('%Y-%m-%dT%H:%M:%fZ',claim_expires_at) IS claim_expires_at),
    CHECK((source_kind='outbox' AND length(effect_id)>0 AND effect_id IS NOT NULL
      AND length(worker_id)>0 AND worker_id IS NOT NULL AND attempt_count>=0 AND typeof(attempt_count)='integer'
      AND claim_expires_at IS NOT NULL AND producer_id IS NULL)
      OR (source_kind!='outbox' AND effect_id IS NULL AND worker_id IS NULL AND attempt_count IS NULL
      AND claim_expires_at IS NULL AND producer_id IS NOT NULL AND length(producer_id)>0)),
    CHECK((state IN ('completed','no_effect')) = (settled_at IS NOT NULL))
  )`;
  yield* sql`CREATE UNIQUE INDEX jones_ordinary_checkout_use_outbox ON jones_ordinary_checkout_uses(effect_id,attempt_count,admission_id) WHERE source_kind='outbox'`;
  yield* sql`CREATE UNIQUE INDEX jones_ordinary_checkout_use_prepared ON jones_ordinary_checkout_uses(source_kind,producer_id,admission_id) WHERE source_kind!='outbox'`;
  yield* sql`CREATE INDEX jones_ordinary_checkout_use_state ON jones_ordinary_checkout_uses(checkout_key,state)`;
  yield* sql`CREATE INDEX jones_ordinary_checkout_use_admission ON jones_ordinary_checkout_uses(admission_id,state)`;
  yield* sql`CREATE INDEX jones_ordinary_checkout_use_recovery ON jones_ordinary_checkout_uses(state,operation_id)`;
  yield* sql`CREATE TABLE jones_ordinary_checkout_lifecycle_events (
    operation_id TEXT NOT NULL REFERENCES jones_ordinary_checkout_uses(operation_id),
    ordinal INTEGER NOT NULL CHECK(typeof(ordinal)='integer' AND ordinal>0), predecessor_ordinal INTEGER NOT NULL CHECK(typeof(predecessor_ordinal)='integer' AND predecessor_ordinal=ordinal-1),
    association_id TEXT NOT NULL CHECK(length(association_id)=64 AND association_id NOT GLOB '*[^a-f0-9]*'),
    event_kind TEXT NOT NULL CHECK(event_kind IN ('bind','activate','join','renew','retire','unknown','target_transition','release','handoff')),
    binding_json TEXT NOT NULL CHECK(json_valid(binding_json) AND length(binding_json)<=8192),
    evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json) AND length(evidence_json)<=8192),
    recorded_at TEXT NOT NULL CHECK(strftime('%Y-%m-%dT%H:%M:%fZ',recorded_at) IS recorded_at), PRIMARY KEY(operation_id,ordinal)
  )`;
  yield* sql`CREATE INDEX jones_ordinary_checkout_event_association ON jones_ordinary_checkout_lifecycle_events(operation_id,association_id,ordinal)`;
  yield* sql`CREATE TRIGGER jones_ordinary_checkout_event_predecessor BEFORE INSERT ON jones_ordinary_checkout_lifecycle_events
    WHEN NEW.predecessor_ordinal != COALESCE((SELECT MAX(ordinal) FROM jones_ordinary_checkout_lifecycle_events WHERE operation_id=NEW.operation_id),0)
    BEGIN SELECT RAISE(ABORT,'checkout event predecessor mismatch'); END`;
  for (const [table, key] of [
    ["jones_ordinary_checkout_owners", "checkout_key=NEW.checkout_key OR lease_id=NEW.lease_id"],
    [
      "jones_ordinary_checkout_admissions",
      "admission_id=NEW.admission_id OR (command_id=NEW.command_id AND subject_thread_id=NEW.subject_thread_id AND checkout_key=NEW.checkout_key)",
    ],
    [
      "jones_ordinary_checkout_effect_links",
      "effect_id=NEW.effect_id AND admission_id=NEW.admission_id",
    ],
    [
      "jones_ordinary_checkout_uses",
      "operation_id=NEW.operation_id OR (admission_id=NEW.admission_id AND source_kind=NEW.source_kind AND ((source_kind='outbox' AND effect_id=NEW.effect_id AND attempt_count=NEW.attempt_count) OR (source_kind!='outbox' AND producer_id=NEW.producer_id)))",
    ],
    [
      "jones_ordinary_checkout_lifecycle_events",
      "operation_id=NEW.operation_id AND ordinal=NEW.ordinal",
    ],
  ] as const) {
    yield* sql`CREATE TRIGGER ${sql(`${table}_no_delete`)} BEFORE DELETE ON ${sql(table)} BEGIN SELECT RAISE(ABORT,'checkout history is permanent'); END`;
    yield* sql.unsafe(
      `CREATE TRIGGER ${table}_no_replace BEFORE INSERT ON ${table} WHEN EXISTS(SELECT 1 FROM ${table} WHERE ${key}) BEGIN SELECT RAISE(ABORT,'checkout identity cannot be replaced'); END`,
    );
  }
  for (const table of [
    "jones_ordinary_checkout_admissions",
    "jones_ordinary_checkout_effect_links",
    "jones_ordinary_checkout_lifecycle_events",
  ])
    yield* sql`CREATE TRIGGER ${sql(`${table}_immutable`)} BEFORE UPDATE ON ${sql(table)} BEGIN SELECT RAISE(ABORT,'checkout history is immutable'); END`;
  // A different epoch requires a recorded handoff from the exact released owner.
  yield* sql`CREATE TRIGGER jones_ordinary_checkout_owner_identity BEFORE UPDATE ON jones_ordinary_checkout_owners
    WHEN NEW.checkout_key IS NOT OLD.checkout_key
      OR NEW.directory_identity_json IS NOT OLD.directory_identity_json OR NEW.git_common_directory IS NOT OLD.git_common_directory
      OR NEW.revision != OLD.revision+1 OR NEW.target_revision<OLD.target_revision
      OR (OLD.state='released' AND NEW.state!='released' AND NEW.owner_epoch=OLD.owner_epoch)
      OR (NEW.state='released' AND OLD.state!='released' AND NOT EXISTS(
        SELECT 1 FROM jones_ordinary_checkout_lifecycle_events e JOIN jones_ordinary_checkout_uses u ON u.operation_id=e.operation_id
        WHERE u.checkout_key=OLD.checkout_key AND u.owner_epoch=OLD.owner_epoch AND u.state IN ('completed','no_effect')
          AND e.ordinal=u.last_event_ordinal AND e.event_kind='release'))
      OR (NEW.branch IS NOT OLD.branch AND (NEW.target_revision!=OLD.target_revision+1 OR NOT EXISTS(
        SELECT 1 FROM jones_ordinary_checkout_lifecycle_events e JOIN jones_ordinary_checkout_uses u ON u.operation_id=e.operation_id
        WHERE u.checkout_key=OLD.checkout_key AND u.owner_epoch=OLD.owner_epoch AND u.state IN ('completed','no_effect')
          AND e.ordinal=u.last_event_ordinal AND e.event_kind='target_transition'
          AND json_extract(e.evidence_json,'$.transition.beforeBranch') IS OLD.branch
          AND json_extract(e.evidence_json,'$.transition.afterBranch') IS NEW.branch)))
      OR (NEW.owner_epoch=OLD.owner_epoch AND (
        NEW.lease_id IS NOT OLD.lease_id OR NEW.owner_thread_id IS NOT OLD.owner_thread_id
        OR NEW.birth_event_id IS NOT OLD.birth_event_id OR NEW.birth_sequence IS NOT OLD.birth_sequence
        OR NEW.acquired_at IS NOT OLD.acquired_at OR NEW.project_root IS NOT OLD.project_root
        OR NEW.renewed_at<OLD.renewed_at OR NEW.expires_at<OLD.expires_at))
      OR (NEW.owner_epoch!=OLD.owner_epoch AND (NEW.owner_epoch!=OLD.owner_epoch+1 OR OLD.state!='released'
        OR NEW.state!='owned' OR NEW.lease_id=OLD.lease_id OR NOT EXISTS(
          SELECT 1 FROM jones_ordinary_checkout_lifecycle_events e JOIN jones_ordinary_checkout_uses u ON u.operation_id=e.operation_id
          WHERE u.checkout_key=OLD.checkout_key AND u.owner_epoch=OLD.owner_epoch AND e.event_kind='handoff'
            AND json_extract(e.evidence_json,'$.transition.afterEpoch')=NEW.owner_epoch
            AND json_extract(e.evidence_json,'$.transition.afterLease')=NEW.lease_id
            AND json_extract(e.evidence_json,'$.transition.afterThread')=NEW.owner_thread_id
            AND json_extract(e.evidence_json,'$.transition.afterBirth')=NEW.birth_event_id)))
    BEGIN SELECT RAISE(ABORT,'checkout owner identity or revision changed'); END`;
  yield* sql`CREATE TRIGGER jones_ordinary_checkout_use_identity BEFORE UPDATE ON jones_ordinary_checkout_uses
    WHEN NEW.operation_id IS NOT OLD.operation_id OR NEW.admission_id IS NOT OLD.admission_id
      OR NEW.checkout_key IS NOT OLD.checkout_key OR NEW.lease_id IS NOT OLD.lease_id OR NEW.owner_epoch IS NOT OLD.owner_epoch
      OR NEW.source_kind IS NOT OLD.source_kind OR NEW.effect_id IS NOT OLD.effect_id OR NEW.worker_id IS NOT OLD.worker_id
      OR NEW.attempt_count IS NOT OLD.attempt_count OR NEW.claim_expires_at IS NOT OLD.claim_expires_at OR NEW.producer_id IS NOT OLD.producer_id
      OR NEW.revision != OLD.revision+1 OR NEW.last_event_ordinal<OLD.last_event_ordinal
      OR NEW.last_event_ordinal!=COALESCE((SELECT MAX(ordinal) FROM jones_ordinary_checkout_lifecycle_events WHERE operation_id=OLD.operation_id),0)
      OR (OLD.started_at IS NOT NULL AND NEW.started_at IS NOT OLD.started_at)
      OR (OLD.state IN ('completed','no_effect') AND (NEW.state IS NOT OLD.state OR NEW.settled_at IS NOT OLD.settled_at OR NEW.last_event_ordinal!=OLD.last_event_ordinal+1))
    BEGIN SELECT RAISE(ABORT,'checkout use identity or revision changed'); END`;
});
