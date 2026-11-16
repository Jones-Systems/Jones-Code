import { NativeCreationEffect } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { NativeCreationStoredIntent } from "../nativeCreation/NativeCreationRepository.ts";
import {
  NativeWorkspaceBasis,
  NativeWorkspaceVerified,
} from "../nativeCreation/NativeCreationWorkspaceTypes.ts";
import {
  decodeRuntimeUseCreationFact,
  RuntimeUseCreationAdmission,
  RuntimeUseCreationMaterial,
} from "./RuntimeUseCreationProtocol.ts";
import { readRuntimeUseInstallation } from "./RuntimeUseInstallation.ts";
import {
  withCapturedRuntimeUseFreshCreation,
  withCapturedRuntimeUseCreationMaterial,
} from "./RuntimeUseCreationCapture.ts";
import { canonical, digest, RuntimeUseError, runtimeUseError } from "./RuntimeUseProtocol.ts";

const parse = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const decodeIntent = Schema.decodeUnknownEffect(NativeCreationStoredIntent, {
  onExcessProperty: "error",
});
const decodeBasis = Schema.decodeUnknownEffect(NativeWorkspaceBasis, { onExcessProperty: "error" });
const decodeVerified = Schema.decodeUnknownEffect(NativeWorkspaceVerified, {
  onExcessProperty: "error",
});
const decodeEffect = Schema.decodeUnknownEffect(NativeCreationEffect, {
  onExcessProperty: "error",
});
const isAdmission = Schema.is(RuntimeUseCreationAdmission);
const isMaterial = Schema.is(RuntimeUseCreationMaterial);
const decodeMaterial = Schema.decodeUnknownEffect(RuntimeUseCreationMaterial, {
  onExcessProperty: "error",
});
const fail = (reason: string) => Effect.fail(new RuntimeUseError({ reason }));

/** Read-only original-source validation. No absent admission/material is ever
 * inserted, enriched or reconstructed by this reader. Ordinary checkout facts
 * cannot supply any of the native creation witnesses selected here. */
const readCreationHistory = Effect.fn("RuntimeUseCreationStore.readSnapshot")(function* (
  claimId: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const table = yield* sql`SELECT name FROM sqlite_master WHERE type='table'
    AND name='jones_workspace_runtime_creation_facts'`;
  if (table.length === 0) return null;
  const rows = yield* sql<{
    readonly claim_id: string;
    readonly phase: string;
    readonly fact_json: string;
    readonly fact_sha256: string;
  }>`SELECT claim_id,phase,fact_json,fact_sha256 FROM jones_workspace_runtime_creation_facts
    WHERE claim_id=${claimId} ORDER BY phase`;
  if (rows.length === 0) return null;
  if (rows.length > 2) return yield* fail("runtime_creation_history_ambiguous");
  let admission: RuntimeUseCreationAdmission | null = null;
  let material: RuntimeUseCreationMaterial | null = null;
  for (const row of rows) {
    const value = yield* decodeRuntimeUseCreationFact(yield* parse(row.fact_json));
    if (
      row.claim_id !== claimId ||
      value.claimId !== claimId ||
      canonical(value) !== row.fact_json ||
      digest(value) !== row.fact_sha256
    )
      return yield* fail("runtime_creation_fact_identity");
    if (row.phase === "admitted" && isAdmission(value) && admission === null) admission = value;
    else if (row.phase === "material" && isMaterial(value) && material === null) material = value;
    else return yield* fail("runtime_creation_fact_phase");
  }
  if (admission === null) return yield* fail("runtime_creation_original_admission_missing");
  const installation = yield* readRuntimeUseInstallation();
  if (installation === null || admission.installationSha256 !== digest(installation))
    return yield* fail("runtime_creation_installation_changed");
  const intents = yield* sql<{
    readonly command_id: string;
    readonly thread_id: string;
    readonly worktree_path: string;
    readonly intent_json: string;
  }>`
    SELECT command_id,thread_id,worktree_path,intent_json FROM native_creation_intents
    WHERE claim_id=${claimId}`;
  const admissions = yield* sql<{ readonly worktree_path: string; readonly basis_json: string }>`
    SELECT worktree_path,basis_json FROM jones_native_workspace_admissions WHERE claim_id=${claimId}`;
  if (intents.length !== 1 || admissions.length !== 1)
    return yield* fail("runtime_creation_native_source_missing");
  const intent = yield* decodeIntent(yield* parse(intents[0]!.intent_json));
  const basis = yield* decodeBasis(yield* parse(admissions[0]!.basis_json));
  if (
    intent.claimId !== claimId ||
    intent.commandId !== admission.commandId ||
    intent.threadId !== admission.rootThreadId ||
    intent.resources.worktreePath !== admission.canonicalPath ||
    basis.bootId !== intent.claimedBootId ||
    basis.projectId !== intent.binding.projectId ||
    basis.projectCwd !== intent.resources.projectCwd ||
    basis.baseRef !== intent.binding.baseBranch ||
    intents[0]!.command_id !== intent.commandId ||
    intents[0]!.thread_id !== intent.threadId ||
    intents[0]!.worktree_path !== intent.resources.worktreePath ||
    admissions[0]!.worktree_path !== intent.resources.worktreePath ||
    basis.worktreePath !== admission.canonicalPath ||
    canonical(intent) !== intents[0]!.intent_json ||
    canonical(basis) !== admissions[0]!.basis_json ||
    digest(intent) !== admission.nativeIntentSha256 ||
    digest(basis) !== admission.workspaceBasisSha256 ||
    admission.worktreeEffectId !== digest({ claimId, stage: "worktree", basis }) ||
    admission.project.canonicalPath !== basis.projectCwd ||
    admission.canonicalPath === "/" ||
    admission.parent.canonicalPath !==
      (admission.canonicalPath.slice(0, admission.canonicalPath.lastIndexOf("/")) || "/") ||
    admission.gitCommon.canonicalPath !== basis.gitCommonDirectory
  )
    return yield* fail("runtime_creation_native_source_changed");
  const effects = yield* sql<{
    readonly effect_id: string;
    readonly phase: string;
    readonly ordinal: number;
    readonly fact_json: string;
  }>`
    SELECT effect_id,phase,ordinal,fact_json FROM native_creation_effect_facts
    WHERE claim_id=${claimId} ORDER BY ordinal`;
  const facts = yield* Effect.forEach(effects, (row) =>
    parse(row.fact_json).pipe(Effect.flatMap(decodeEffect)),
  );
  // Decode independently; an SQL ID cannot replace the actual owning fact.
  for (const [index, row] of effects.entries()) {
    const fact = facts[index]!;
    if (
      row.ordinal !== index ||
      canonical(fact) !== row.fact_json ||
      fact.effectId !== row.effect_id ||
      fact.phase !== row.phase ||
      fact.ordinal !== row.ordinal
    )
      return yield* fail("runtime_creation_effect_identity");
  }
  if (material === null) return { installation, admission, material, intent, basis, facts };
  const verifiedRows = yield* sql<{ readonly receipt_json: string }>`SELECT receipt_json
    FROM jones_native_workspace_verified WHERE claim_id=${claimId}`;
  if (verifiedRows.length !== 1) return yield* fail("runtime_creation_verified_source_missing");
  const verified = yield* decodeVerified(yield* parse(verifiedRows[0]!.receipt_json));
  const worktree = facts.filter((fact) => fact.kind === "worktree");
  const start = worktree.find((fact) => fact.phase === "started");
  const completion = worktree.find((fact) => fact.phase === "completed");
  if (
    verified.claimId !== claimId ||
    canonical(verified.basis) !== canonical(basis) ||
    canonical(verified) !== verifiedRows[0]!.receipt_json ||
    material.admissionSha256 !== digest(admission) ||
    material.workspaceVerifiedSha256 !== digest(verified) ||
    material.target.canonicalPath !== admission.canonicalPath ||
    material.dotGit.canonicalPath !== admission.canonicalPath + "/.git" ||
    material.gitCommon.canonicalPath !== basis.gitCommonDirectory ||
    canonical(material.gitCommon) !== canonical(admission.gitCommon) ||
    worktree.length !== 2 ||
    start === undefined ||
    completion === undefined ||
    start.projectCwd !== basis.projectCwd ||
    completion.projectCwd !== basis.projectCwd ||
    start.worktreePath !== admission.canonicalPath ||
    completion.worktreePath !== admission.canonicalPath ||
    start.branch !== intent.resources.branch ||
    completion.branch !== intent.resources.branch ||
    start.baseRef !== basis.baseRef ||
    completion.baseRef !== basis.baseRef ||
    start.effectId !== admission.worktreeEffectId ||
    completion.effectId !== admission.worktreeEffectId ||
    completion.phase !== "completed" ||
    completion.result !== "succeeded" ||
    completion.ownership !== "created" ||
    material.worktreeStartSha256 !== digest(start) ||
    material.worktreeCompletionSha256 !== digest(completion)
  )
    return yield* fail("runtime_creation_material_source_changed");
  return { installation, admission, material, intent, basis, verified, facts };
}, Effect.mapError(runtimeUseError));

export const recordCapturedRuntimeUseCreationMaterial = Effect.fn(
  "RuntimeUseCreationStore.recordMaterial",
)(function* (
  verified: NativeWorkspaceVerified,
  effects: readonly (typeof NativeCreationEffect.Type)[],
) {
  const sql = yield* SqlClient.SqlClient;
  return yield* withCapturedRuntimeUseCreationMaterial(verified, (captured) =>
    Effect.gen(function* () {
      const worktree = effects.filter((fact) => fact.kind === "worktree");
      const start = worktree.find((fact) => fact.phase === "started");
      const completion = worktree.find((fact) => fact.phase === "completed");
      if (worktree.length !== 2 || start === undefined || completion === undefined)
        return yield* fail("runtime_creation_material_history_incomplete");
      const fact = yield* decodeMaterial({
        ...captured,
        worktreeStartSha256: digest(start),
        worktreeCompletionSha256: digest(completion),
      });
      yield* sql`INSERT INTO jones_workspace_runtime_creation_facts
          (claim_id,phase,fact_json,fact_sha256)
          VALUES (${verified.claimId},'material',${canonical(fact)},${digest(fact)})`;
      const current = yield* readRuntimeUseCreationHistory(verified.claimId);
      if (current === null || canonical(current.material) !== canonical(fact))
        return yield* fail("runtime_creation_material_readback_changed");
    }),
  );
}, Effect.mapError(runtimeUseError));

export const readRuntimeUseCreationHistory = Effect.fn("RuntimeUseCreationStore.read")(function* (
  claimId: string,
) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql.withTransaction(readCreationHistory(claimId));
});

/** Additional write in the SAME real transaction that inserts a new native
 * workspace admission. Existing admissions never call this or gain a fact. */
export const recordCapturedRuntimeUseCreationAdmission = Effect.fn(
  "RuntimeUseCreationStore.recordAdmission",
)(function* (intent: NativeCreationStoredIntent, basis: NativeWorkspaceBasis) {
  const sql = yield* SqlClient.SqlClient;
  return yield* withCapturedRuntimeUseFreshCreation(intent, basis, (fact) =>
    sql`INSERT INTO jones_workspace_runtime_creation_facts
      (claim_id,phase,fact_json,fact_sha256)
      VALUES (${intent.claimId},'admitted',${canonical(fact)},${digest(fact)})`.pipe(
      Effect.andThen(readRuntimeUseCreationHistory(intent.claimId)),
      Effect.filterOrFail(
        (current) => current !== null && canonical(current.admission) === canonical(fact),
        () => new RuntimeUseError({ reason: "runtime_creation_admission_readback_changed" }),
      ),
      Effect.asVoid,
    ),
  );
}, Effect.mapError(runtimeUseError));
