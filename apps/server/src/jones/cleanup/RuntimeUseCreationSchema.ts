import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export const runtimeUseCreationObjectNames = [
  "jones_workspace_runtime_creation_facts",
  "jones_workspace_runtime_creation_facts_no_update",
  "jones_workspace_runtime_creation_facts_no_delete",
  "jones_workspace_runtime_creation_facts_no_replace",
  "jones_workspace_runtime_creation_facts_original_source",
];

// Two bounded immutable facts per original claim: before material creation,
// and after the original verified creation. No native rows/FKs are copied.
export const createRuntimeUseCreationSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_workspace_runtime_creation_facts (
    claim_id TEXT NOT NULL CHECK(length(claim_id)>0),
    phase TEXT NOT NULL CHECK(phase IN ('admitted','material')),
    fact_json TEXT NOT NULL CHECK(json_valid(fact_json)),
    fact_sha256 TEXT NOT NULL CHECK(length(fact_sha256)=64 AND fact_sha256 NOT GLOB '*[^a-f0-9]*'),
    PRIMARY KEY(claim_id,phase),
    CHECK(json_extract(fact_json,'$.claimId') IS claim_id
      AND ((phase='admitted'
        AND json_extract(fact_json,'$.schema') IS 'jones.workspace-runtime-creation-admission/v1'
        AND json_type(fact_json,'$.installationSha256') IS 'text'
        AND json_type(fact_json,'$.nativeIntentSha256') IS 'text'
        AND json_type(fact_json,'$.workspaceBasisSha256') IS 'text'
        AND json_type(fact_json,'$.worktreeEffectId') IS 'text'
        AND json_type(fact_json,'$.commandId') IS 'text'
        AND json_type(fact_json,'$.rootThreadId') IS 'text'
        AND json_type(fact_json,'$.canonicalPath') IS 'text')
      OR (phase='material'
        AND json_extract(fact_json,'$.schema') IS 'jones.workspace-runtime-creation-material/v1'
        AND json_type(fact_json,'$.admissionSha256') IS 'text'
        AND json_type(fact_json,'$.workspaceVerifiedSha256') IS 'text'
        AND json_type(fact_json,'$.worktreeStartSha256') IS 'text'
        AND json_type(fact_json,'$.worktreeCompletionSha256') IS 'text')))
  )`;
  yield* sql`CREATE TRIGGER jones_workspace_runtime_creation_facts_no_update
    BEFORE UPDATE ON jones_workspace_runtime_creation_facts
    BEGIN SELECT RAISE(ABORT,'original creation coverage is immutable'); END`;
  yield* sql`CREATE TRIGGER jones_workspace_runtime_creation_facts_no_delete
    BEFORE DELETE ON jones_workspace_runtime_creation_facts
    BEGIN SELECT RAISE(ABORT,'original creation coverage is permanent'); END`;
  yield* sql`CREATE TRIGGER jones_workspace_runtime_creation_facts_no_replace
    BEFORE INSERT ON jones_workspace_runtime_creation_facts
    WHEN EXISTS(SELECT 1 FROM jones_workspace_runtime_creation_facts
      WHERE claim_id=NEW.claim_id AND phase=NEW.phase)
    BEGIN SELECT RAISE(ABORT,'original creation coverage cannot be replaced'); END`;
  // SQL is an additional exact-source guard. The real repository transaction
  // must also decode/hash each original binding/fact and require the private
  // live pre-effect/material observation. An ID or JSON alone is no authority;
  // this schema does not install a positive writer or a cleanup capability.
  yield* sql`CREATE TRIGGER jones_workspace_runtime_creation_facts_original_source
    BEFORE INSERT ON jones_workspace_runtime_creation_facts
    WHEN (NEW.phase='admitted' AND (
      NOT EXISTS(SELECT 1 FROM jones_workspace_runtime_installation i
        WHERE i.id=1 AND i.basis_sha256=json_extract(NEW.fact_json,'$.installationSha256'))
      OR NOT EXISTS(SELECT 1 FROM native_creation_intents n
        JOIN jones_native_workspace_admissions a ON a.claim_id=n.claim_id
        WHERE n.claim_id=NEW.claim_id
          AND n.command_id=json_extract(NEW.fact_json,'$.commandId')
          AND n.thread_id=json_extract(NEW.fact_json,'$.rootThreadId')
          AND n.worktree_path=json_extract(NEW.fact_json,'$.canonicalPath')
          AND a.worktree_path=n.worktree_path)
      OR EXISTS(SELECT 1 FROM native_creation_effect_facts e WHERE e.claim_id=NEW.claim_id
        AND json_extract(e.fact_json,'$.kind') IN ('fetch','worktree','setup','cleanup'))
      OR EXISTS(SELECT 1 FROM jones_native_workspace_verified v WHERE v.claim_id=NEW.claim_id)))
    OR (NEW.phase='material' AND (
      NOT EXISTS(SELECT 1 FROM jones_workspace_runtime_creation_facts a
        WHERE a.claim_id=NEW.claim_id AND a.phase='admitted'
          AND a.fact_sha256=json_extract(NEW.fact_json,'$.admissionSha256')
          AND json_extract(a.fact_json,'$.canonicalPath')=json_extract(NEW.fact_json,'$.target.canonicalPath'))
      OR NOT EXISTS(SELECT 1 FROM jones_native_workspace_verified v WHERE v.claim_id=NEW.claim_id)))
    BEGIN SELECT RAISE(ABORT,'creation coverage lacks its original native source'); END`;
});
