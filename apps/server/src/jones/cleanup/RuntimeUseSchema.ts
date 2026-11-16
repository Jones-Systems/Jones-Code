import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import { SqlError, UnknownError } from "effect/sql/SqlError";

const deletionPredecessor =
  "CREATE TABLE jones_deletion_worktree_admissions (\n    effect_id TEXT PRIMARY KEY NOT NULL,\n    worktree_path TEXT UNIQUE NOT NULL,\n    binding_json TEXT NOT NULL CHECK(json_valid(binding_json)),\n    start_json TEXT NOT NULL CHECK(json_valid(start_json)),\n    state TEXT NOT NULL CHECK(state IN ('started','unknown','completed'))\n  )";

// Jones-only current state and permanent history. The event insertion and its
// current-row transition are one SQLite statement, including direct writers.
export const createRuntimeUseSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const predecessor = yield* sql<{ readonly sql: string }>`SELECT sql FROM sqlite_master
    WHERE type='table' AND name='jones_deletion_worktree_admissions'`;
  if (predecessor.length !== 1 || predecessor[0]?.sql !== deletionPredecessor)
    return yield* new SqlError({
      reason: new UnknownError({
        cause: "Runtime use journal requires the exact released Jones deletion schema",
      }),
    });
  yield* sql`CREATE TABLE jones_workspace_runtime_uses (
    use_id TEXT PRIMARY KEY NOT NULL CHECK(length(use_id)>0),
    canonical_path TEXT NOT NULL CHECK(substr(canonical_path,1,1)='/'),
    thread_id TEXT NOT NULL CHECK(length(thread_id)>0),
    kind TEXT NOT NULL CHECK(kind IN ('provider_open','provider_invoke','terminal_spawn')),
    provider_session_id TEXT, terminal_session_id TEXT,
    binding_json TEXT NOT NULL CHECK(json_valid(binding_json)),
    binding_sha256 TEXT NOT NULL CHECK(length(binding_sha256)=64 AND binding_sha256 NOT GLOB '*[^a-f0-9]*'),
    state TEXT NOT NULL CHECK(state IN ('pending','retired')),
    last_ordinal INTEGER NOT NULL CHECK(last_ordinal>=0),
    retirement_json TEXT,
    CHECK(json_extract(binding_json,'$.schema') IS 'jones.workspace-runtime-use/v1'
      AND json_extract(binding_json,'$.useId') IS use_id
      AND json_extract(binding_json,'$.canonicalPath') IS canonical_path
      AND json_extract(binding_json,'$.threadId') IS thread_id
      AND json_extract(binding_json,'$.actor.kind') IS kind),
    CHECK((kind IN ('provider_open','provider_invoke') AND provider_session_id IS NOT NULL AND length(provider_session_id)>0
      AND terminal_session_id IS NULL AND json_extract(binding_json,'$.actor.provider.sessionId') IS provider_session_id
      AND json_type(binding_json,'$.actor.provider') IS 'object'
      AND json_type(binding_json,'$.actor.terminalSessionId') IS NULL
      AND json_type(binding_json,'$.actor.provider.instanceId') IS 'text'
      AND length(json_extract(binding_json,'$.actor.provider.instanceId'))>0
      AND json_type(binding_json,'$.actor.provider.driver') IS 'text'
      AND length(json_extract(binding_json,'$.actor.provider.driver'))>0)
      OR (kind='terminal_spawn' AND provider_session_id IS NULL AND terminal_session_id IS NOT NULL AND length(terminal_session_id)>0
      AND json_type(binding_json,'$.actor.provider') IS NULL
      AND json_extract(binding_json,'$.actor.terminalSessionId') IS terminal_session_id)),
    CHECK(json_type(binding_json,'$.directoryIdentity') IS 'object'
      AND json_extract(binding_json,'$.directoryIdentity.version') IS 1
      AND json_type(binding_json,'$.directoryIdentity.device') IS 'text'
      AND json_type(binding_json,'$.directoryIdentity.inode') IS 'text'
      AND json_type(binding_json,'$.directoryIdentity.birth') IS 'text'
      AND json_type(binding_json,'$.original') IS 'object'),
    CHECK((json_type(binding_json,'$.original.claim') IS 'null'
      AND json_type(binding_json,'$.original.outboxReference') IS 'null') OR (
      json_type(binding_json,'$.original.claim') IS 'object'
      AND json_extract(binding_json,'$.original.claim.schema') IS 'jones.workspace-runtime-outbox-claim/v1'
      AND json_type(binding_json,'$.original.claim.workerId') IS 'text'
      AND length(json_extract(binding_json,'$.original.claim.workerId'))>0
      AND json_type(binding_json,'$.original.claim.effectId') IS 'text'
      AND length(json_extract(binding_json,'$.original.claim.effectId'))>0
      AND json_type(binding_json,'$.original.claim.commandId') IS 'text'
      AND json_type(binding_json,'$.original.claim.threadId') IS 'text'
      AND json_extract(binding_json,'$.threadId') IS json_extract(binding_json,'$.original.claim.threadId')
      AND json_type(binding_json,'$.original.claim.claimedAt') IS 'text'
      AND json_type(binding_json,'$.original.claim.requestSha256') IS 'text'
      AND length(json_extract(binding_json,'$.original.claim.requestSha256'))=64
      AND json_extract(binding_json,'$.original.claim.requestSha256') NOT GLOB '*[^a-f0-9]*'
      AND json_type(binding_json,'$.original.claim.attemptCount') IS 'integer'
      AND json_extract(binding_json,'$.original.claim.attemptCount')>0
      AND json_extract(binding_json,'$.original.commandId') IS json_extract(binding_json,'$.original.claim.commandId')
      AND json_extract(binding_json,'$.original.runId') IS json_extract(binding_json,'$.original.claim.runId')
      AND json_extract(binding_json,'$.original.attemptId') IS json_extract(binding_json,'$.original.claim.attemptId')
      AND json_extract(binding_json,'$.original.outboxReference') IS json_extract(binding_json,'$.original.claim.effectId'))),
    CHECK((state='pending' AND retirement_json IS NULL) OR
      (state='retired' AND retirement_json IS NOT NULL AND json_valid(retirement_json)))
  )`;
  yield* sql`CREATE TABLE jones_workspace_runtime_use_events (
    use_id TEXT NOT NULL, ordinal INTEGER NOT NULL CHECK(ordinal>=0),
    action TEXT NOT NULL CHECK(action IN ('reserved','observed','retired')),
    binding_sha256 TEXT NOT NULL,
    evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
    evidence_sha256 TEXT NOT NULL CHECK(length(evidence_sha256)=64 AND evidence_sha256 NOT GLOB '*[^a-f0-9]*'),
    PRIMARY KEY(use_id,ordinal)
  )`;
  yield* sql`CREATE INDEX jones_workspace_runtime_use_pending ON jones_workspace_runtime_uses(state,canonical_path,use_id)`;
  // Reciprocal fences act in the actual writer statement, including a second
  // connection. No application transaction is retained across native/Git work.
  yield* sql`CREATE TRIGGER jones_workspace_runtime_use_deletion_conflict BEFORE INSERT ON jones_workspace_runtime_uses
    WHEN EXISTS(SELECT 1 FROM jones_deletion_worktree_admissions d WHERE d.state!='completed' AND (
      d.worktree_path=NEW.canonical_path
      OR substr(d.worktree_path,1,length(rtrim(NEW.canonical_path,'/'))+1)=rtrim(NEW.canonical_path,'/')||'/'
      OR substr(NEW.canonical_path,1,length(rtrim(d.worktree_path,'/'))+1)=rtrim(d.worktree_path,'/')||'/'))
    BEGIN SELECT RAISE(ABORT,'runtime use conflicts with original deletion'); END`;
  yield* sql`CREATE TRIGGER jones_workspace_runtime_deletion_use_conflict BEFORE INSERT ON jones_deletion_worktree_admissions
    WHEN EXISTS(SELECT 1 FROM jones_workspace_runtime_uses u WHERE u.state='pending' AND (
      u.canonical_path=NEW.worktree_path
      OR substr(u.canonical_path,1,length(rtrim(NEW.worktree_path,'/'))+1)=rtrim(NEW.worktree_path,'/')||'/'
      OR substr(NEW.worktree_path,1,length(rtrim(u.canonical_path,'/'))+1)=rtrim(u.canonical_path,'/')||'/'))
    BEGIN SELECT RAISE(ABORT,'deletion conflicts with unresolved runtime use'); END`;
  for (const table of ["jones_workspace_runtime_uses", "jones_workspace_runtime_use_events"]) {
    yield* sql.unsafe(`CREATE TRIGGER ${table}_no_delete BEFORE DELETE ON ${table}
      BEGIN SELECT RAISE(ABORT,'runtime use history is permanent'); END`);
    yield* sql.unsafe(`CREATE TRIGGER ${table}_no_replace BEFORE INSERT ON ${table}
      WHEN EXISTS(SELECT 1 FROM ${table} WHERE use_id=NEW.use_id${table.endsWith("_events") ? " AND ordinal=NEW.ordinal" : ""})
      BEGIN SELECT RAISE(ABORT,'runtime use identity cannot be replaced'); END`);
  }
  yield* sql`CREATE TRIGGER jones_workspace_runtime_uses_initial BEFORE INSERT ON jones_workspace_runtime_uses
    WHEN NEW.state!='pending' OR NEW.last_ordinal!=0 OR NEW.retirement_json IS NOT NULL
    BEGIN SELECT RAISE(ABORT,'runtime use must begin pending'); END`;
  yield* sql`CREATE TRIGGER jones_workspace_runtime_uses_reserved AFTER INSERT ON jones_workspace_runtime_uses
    BEGIN INSERT INTO jones_workspace_runtime_use_events VALUES
      (NEW.use_id,0,'reserved',NEW.binding_sha256,NEW.binding_json,NEW.binding_sha256); END`;
  yield* sql`CREATE TRIGGER jones_workspace_runtime_events_immutable BEFORE UPDATE ON jones_workspace_runtime_use_events
    BEGIN SELECT RAISE(ABORT,'runtime use events are immutable'); END`;
  yield* sql`CREATE TRIGGER jones_workspace_runtime_event_guard BEFORE INSERT ON jones_workspace_runtime_use_events
    WHEN NOT EXISTS(SELECT 1 FROM jones_workspace_runtime_uses u WHERE u.use_id=NEW.use_id
      AND u.binding_sha256=NEW.binding_sha256 AND u.state='pending'
      AND ((NEW.action='reserved' AND NEW.ordinal=0 AND u.last_ordinal=0
        AND NEW.evidence_json=u.binding_json AND NEW.evidence_sha256=u.binding_sha256)
      OR (NEW.ordinal=u.last_ordinal+1 AND NEW.action IN ('observed','retired')
        AND json_extract(NEW.evidence_json,'$.useId') IS u.use_id
        AND json_extract(NEW.evidence_json,'$.bindingSha256') IS u.binding_sha256
        AND ((NEW.action='observed' AND json_extract(NEW.evidence_json,'$.schema') IN ('jones.workspace-runtime-producer/v1','jones.workspace-runtime-native-binding/v1'))
          OR (NEW.action='retired' AND json_extract(NEW.evidence_json,'$.schema') IS 'jones.workspace-runtime-retirement/v1'
            AND json_extract(NEW.evidence_json,'$.outcome') IN ('not_invoked','producers_exited')))) ))
    BEGIN SELECT RAISE(ABORT,'runtime use event predecessor or identity mismatch'); END`;
  yield* sql`CREATE TRIGGER jones_workspace_runtime_event_transition AFTER INSERT ON jones_workspace_runtime_use_events
    WHEN NEW.action!='reserved'
    BEGIN UPDATE jones_workspace_runtime_uses SET last_ordinal=NEW.ordinal,
      state=CASE WHEN NEW.action='retired' THEN 'retired' ELSE 'pending' END,
      retirement_json=CASE WHEN NEW.action='retired' THEN NEW.evidence_json ELSE NULL END
      WHERE use_id=NEW.use_id; END`;
  yield* sql`CREATE TRIGGER jones_workspace_runtime_producer_identity BEFORE INSERT ON jones_workspace_runtime_use_events
    WHEN NEW.action='observed' AND json_extract(NEW.evidence_json,'$.schema')='jones.workspace-runtime-producer/v1' AND (
      json_type(NEW.evidence_json,'$.producerId') IS NOT 'text'
      OR length(json_extract(NEW.evidence_json,'$.producerId'))=0
      OR json_type(NEW.evidence_json,'$.generation') IS NOT 'text'
      OR length(json_extract(NEW.evidence_json,'$.generation'))=0
      OR json_type(NEW.evidence_json,'$.threadId') IS NOT 'text'
      OR length(json_extract(NEW.evidence_json,'$.threadId'))=0
      OR COALESCE(json_extract(NEW.evidence_json,'$.coverage') IN ('captured_process_only','logical_thread','complete'),0)=0
      OR EXISTS(SELECT 1 FROM jones_workspace_runtime_use_events e
        WHERE e.use_id=NEW.use_id AND e.action='observed' AND json_extract(e.evidence_json,'$.schema')='jones.workspace-runtime-producer/v1' AND
          (json_extract(e.evidence_json,'$.producerId') IS json_extract(NEW.evidence_json,'$.producerId')
          OR json_extract(e.evidence_json,'$.generation') IS json_extract(NEW.evidence_json,'$.generation'))))
    BEGIN SELECT RAISE(ABORT,'runtime producer identity cannot be rebound'); END`;
  yield* sql`CREATE TRIGGER jones_workspace_runtime_native_binding_identity BEFORE INSERT ON jones_workspace_runtime_use_events
    WHEN NEW.action='observed' AND json_extract(NEW.evidence_json,'$.schema')='jones.workspace-runtime-native-binding/v1' AND (
      json_extract(NEW.evidence_json,'$.binding.runtimeGeneration') IS NOT json_extract(NEW.evidence_json,'$.generation')
      OR NOT EXISTS(SELECT 1 FROM jones_workspace_runtime_use_events e WHERE e.use_id=NEW.use_id AND e.action='observed'
        AND json_extract(e.evidence_json,'$.schema')='jones.workspace-runtime-producer/v1'
        AND json_extract(e.evidence_json,'$.producerId') IS json_extract(NEW.evidence_json,'$.producerId')
        AND json_extract(e.evidence_json,'$.generation') IS json_extract(NEW.evidence_json,'$.generation'))
      OR NOT EXISTS(SELECT 1 FROM jones_workspace_runtime_uses u WHERE u.use_id=NEW.use_id AND u.kind IN ('provider_open','provider_invoke')
        AND json_extract(u.binding_json,'$.actor.provider.sessionId') IS json_extract(NEW.evidence_json,'$.binding.providerSessionId')
        AND json_extract(u.binding_json,'$.actor.provider.instanceId') IS json_extract(NEW.evidence_json,'$.binding.providerInstanceId')
        AND json_extract(u.binding_json,'$.actor.provider.driver') IS json_extract(NEW.evidence_json,'$.binding.driver'))
      OR EXISTS(SELECT 1 FROM jones_workspace_runtime_use_events e WHERE e.use_id=NEW.use_id AND e.action='observed'
        AND json_extract(e.evidence_json,'$.schema')='jones.workspace-runtime-native-binding/v1'
        AND json_extract(e.evidence_json,'$.producerId') IS json_extract(NEW.evidence_json,'$.producerId') AND (
          json_extract(e.evidence_json,'$.binding.providerThreadId') IS json_extract(NEW.evidence_json,'$.binding.providerThreadId')
          OR json_extract(e.evidence_json,'$.binding.nativeThreadId') IS json_extract(NEW.evidence_json,'$.binding.nativeThreadId'))))
    BEGIN SELECT RAISE(ABORT,'runtime native binding cannot change producer or owner'); END`;
  yield* sql`CREATE TRIGGER jones_workspace_runtime_retirement_coverage BEFORE INSERT ON jones_workspace_runtime_use_events
    WHEN NEW.action='retired' AND (
      json_extract(NEW.evidence_json,'$.observationCount') IS NOT
        (SELECT count(*) FROM jones_workspace_runtime_use_events WHERE use_id=NEW.use_id AND action='observed')
      OR (json_extract(NEW.evidence_json,'$.outcome')='not_invoked' AND NEW.ordinal!=1)
      OR (json_extract(NEW.evidence_json,'$.outcome')='producers_exited' AND (
        NEW.ordinal<=1 OR json_type(NEW.evidence_json,'$.producers') IS NOT 'array'
        OR json_array_length(NEW.evidence_json,'$.producers')=0
        OR json_array_length(NEW.evidence_json,'$.producers')!=(SELECT count(*) FROM jones_workspace_runtime_use_events
          WHERE use_id=NEW.use_id AND action='observed' AND json_extract(evidence_json,'$.schema')='jones.workspace-runtime-producer/v1')
        OR json_type(NEW.evidence_json,'$.nativeBindings') IS NOT 'array'
        OR json_array_length(NEW.evidence_json,'$.nativeBindings')!=(SELECT count(*) FROM jones_workspace_runtime_use_events
          WHERE use_id=NEW.use_id AND action='observed' AND json_extract(evidence_json,'$.schema')='jones.workspace-runtime-native-binding/v1')
        OR EXISTS(SELECT 1 FROM jones_workspace_runtime_use_events e JOIN jones_workspace_runtime_uses u ON u.use_id=e.use_id
          WHERE e.use_id=NEW.use_id AND e.action='observed' AND json_extract(e.evidence_json,'$.schema')='jones.workspace-runtime-producer/v1'
          AND NOT EXISTS(SELECT 1 FROM json_each(NEW.evidence_json,'$.producers') p WHERE
            p.key=(SELECT count(*) FROM jones_workspace_runtime_use_events prior WHERE prior.use_id=e.use_id AND prior.ordinal<e.ordinal
              AND prior.action='observed' AND json_extract(prior.evidence_json,'$.schema')='jones.workspace-runtime-producer/v1')
            AND json_extract(p.value,'$.observationSha256') IS e.evidence_sha256
            AND json_extract(p.value,'$.producerId') IS json_extract(e.evidence_json,'$.producerId')
            AND json_extract(p.value,'$.generation') IS json_extract(e.evidence_json,'$.generation')
            AND json_extract(p.value,'$.threadId') IS json_extract(e.evidence_json,'$.threadId')
            AND json_extract(p.value,'$.driver') IS 'codex'
            AND json_extract(p.value,'$.driver') IS json_extract(u.binding_json,'$.actor.provider.driver')
            AND json_extract(p.value,'$.providerSessionId') IS u.provider_session_id
            AND json_extract(p.value,'$.instanceId') IS json_extract(u.binding_json,'$.actor.provider.instanceId')
            AND json_type(p.value,'$.pid') IS 'integer' AND json_extract(p.value,'$.pid')>0
            AND json_type(p.value,'$.scopeUnit') IS 'text' AND length(json_extract(p.value,'$.scopeUnit'))>0))
        OR EXISTS(SELECT 1 FROM jones_workspace_runtime_use_events e WHERE e.use_id=NEW.use_id AND e.action='observed'
          AND json_extract(e.evidence_json,'$.schema')='jones.workspace-runtime-native-binding/v1'
          AND NOT EXISTS(SELECT 1 FROM json_each(NEW.evidence_json,'$.nativeBindings') n WHERE
            n.key=(SELECT count(*) FROM jones_workspace_runtime_use_events prior WHERE prior.use_id=e.use_id AND prior.ordinal<e.ordinal
              AND prior.action='observed' AND json_extract(prior.evidence_json,'$.schema')='jones.workspace-runtime-native-binding/v1')
            AND json(n.value)=json(e.evidence_json))))))
    BEGIN SELECT RAISE(ABORT,'runtime retirement coverage is incomplete'); END`;
  yield* sql`CREATE TRIGGER jones_workspace_runtime_use_transition BEFORE UPDATE ON jones_workspace_runtime_uses
    WHEN NEW.use_id IS NOT OLD.use_id OR NEW.canonical_path IS NOT OLD.canonical_path
      OR NEW.thread_id IS NOT OLD.thread_id OR NEW.kind IS NOT OLD.kind
      OR NEW.provider_session_id IS NOT OLD.provider_session_id OR NEW.terminal_session_id IS NOT OLD.terminal_session_id
      OR NEW.binding_json IS NOT OLD.binding_json OR NEW.binding_sha256 IS NOT OLD.binding_sha256
      OR OLD.state!='pending' OR NEW.last_ordinal!=OLD.last_ordinal+1
      OR NOT EXISTS(SELECT 1 FROM jones_workspace_runtime_use_events e WHERE e.use_id=OLD.use_id
        AND e.ordinal=NEW.last_ordinal AND e.binding_sha256=OLD.binding_sha256
        AND ((e.action='observed' AND NEW.state='pending' AND NEW.retirement_json IS NULL)
          OR (e.action='retired' AND NEW.state='retired' AND NEW.retirement_json IS e.evidence_json)))
    BEGIN SELECT RAISE(ABORT,'runtime use transition has no matching event'); END`;
});
