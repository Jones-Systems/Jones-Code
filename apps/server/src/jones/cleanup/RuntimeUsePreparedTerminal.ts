import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { NativeSetupControl } from "../../terminal/NativeSetupControl.ts";
import { readConsumedSetupRuntimeUse } from "../nativeCreation/NativeWorkspaceSetupCustody.ts";
import { NativeCreationStoredIntent } from "../nativeCreation/NativeCreationRepository.ts";
import { NativeWorkspaceBasis } from "../nativeCreation/NativeCreationWorkspaceTypes.ts";
import { nativeCreationSha256 as nativeSha } from "../nativeCreation/NativeCreationPreparation.ts";
import { NativeCreationEffect } from "@t3tools/contracts";
import { canonical, digest, RuntimeUseError, runtimeUseError } from "./RuntimeUseProtocol.ts";

const decodeIntent = Schema.decodeUnknownEffect(Schema.fromJsonString(NativeCreationStoredIntent), {
  onExcessProperty: "error",
});
const decodeBasis = Schema.decodeUnknownEffect(Schema.fromJsonString(NativeWorkspaceBasis), {
  onExcessProperty: "error",
});
const decodeFact = Schema.decodeUnknownEffect(Schema.fromJsonString(NativeCreationEffect), {
  onExcessProperty: "error",
});
const fail = (reason: string) => Effect.fail(new RuntimeUseError({ reason }));

/** A genuinely prepared native setup precedes its application thread birth.
 * Only the actual consumed original-start capability may take this path. SQL
 * independently checks that exact still-open native owner; a control JSON,
 * callback or missing projection cannot authorize a raw terminal start. */
export const readRuntimeUsePreparedTerminal = Effect.fn("RuntimeUsePreparedTerminal.read")(
  function* (input: {
    readonly threadId: string;
    readonly terminalId: string;
    readonly workspacePath: string;
    readonly cwd: string;
    readonly control: NativeSetupControl | undefined;
  }) {
    const original = yield* readConsumedSetupRuntimeUse(input.control);
    if (original === null) return yield* fail("runtime_use_terminal_thread_missing");
    const { claim, basis, start, control } = original;
    if (
      input.threadId !== claim.threadId ||
      input.terminalId !== control.terminalId ||
      input.workspacePath !== basis.worktreePath ||
      input.cwd !== basis.worktreePath ||
      !claim.binding.runSetupScript ||
      basis.setupDefinition === null ||
      control.claimId !== claim.claimId ||
      control.threadId !== claim.threadId ||
      control.worktreePath !== basis.worktreePath ||
      control.projectCwd !== basis.projectCwd ||
      control.bootId !== basis.bootId ||
      control.producerId !== basis.producerId ||
      control.definitionDigest !== nativeSha(basis.setupDefinition) ||
      control.effectId !== digest({ claimId: claim.claimId, stage: "setup", basis }) ||
      control.generation !==
        digest({ claimId: claim.claimId, effectId: control.effectId, bootId: basis.bootId }) ||
      control.terminalId !== `native-setup-${nativeSha(control.effectId)}` ||
      start.effectId !== control.effectId ||
      start.worktreePath !== basis.worktreePath ||
      start.terminalId !== control.terminalId
    )
      return yield* fail("runtime_use_prepared_terminal_scope_changed");
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<{
      readonly intent_json: string;
      readonly basis_json: string;
      readonly workspace_root: string;
      readonly deleted_at: string | null;
      readonly command_id: string;
      readonly thread_id: string;
      readonly worktree_path: string;
    }>`SELECT n.intent_json,a.basis_json,p.workspace_root,p.deleted_at,n.command_id,n.thread_id,n.worktree_path FROM native_creation_intents n JOIN jones_native_workspace_admissions a ON a.claim_id=n.claim_id JOIN projection_projects p ON p.project_id=json_extract(n.intent_json,'$.binding.projectId') WHERE n.claim_id=${claim.claimId} AND a.worktree_path=n.worktree_path`;
    if (rows.length !== 1) return yield* fail("runtime_use_prepared_terminal_source_missing");
    const row = rows[0]!;
    if (
      row.deleted_at !== null ||
      row.workspace_root !== basis.projectCwd ||
      row.command_id !== claim.commandId ||
      row.thread_id !== input.threadId ||
      row.worktree_path !== basis.worktreePath ||
      canonical(yield* decodeIntent(row.intent_json)) !== canonical(claim) ||
      canonical(yield* decodeBasis(row.basis_json)) !== canonical(basis)
    )
      return yield* fail("runtime_use_prepared_terminal_source_changed");
    const effects = yield* sql<{
      readonly ordinal: number;
      readonly effect_id: string;
      readonly phase: string;
      readonly fact_json: string;
    }>`SELECT ordinal,effect_id,phase,fact_json FROM native_creation_effect_facts WHERE claim_id=${claim.claimId} ORDER BY ordinal`;
    const facts = yield* Effect.forEach(effects, (row) => decodeFact(row.fact_json));
    for (const [index, row] of effects.entries()) {
      const fact = facts[index]!;
      if (
        row.ordinal !== index ||
        fact.ordinal !== index ||
        fact.effectId !== row.effect_id ||
        fact.phase !== row.phase ||
        canonical(fact) !== row.fact_json
      )
        return yield* fail("runtime_use_prepared_terminal_history_changed");
    }
    const setups = facts.filter((fact) => fact.kind === "setup"),
      worktrees = facts.filter((fact) => fact.kind === "worktree");
    const worktreeStart = worktrees.find((fact) => fact.phase === "started"),
      worktreeComplete = worktrees.find((fact) => fact.phase === "completed");
    if (
      setups.length !== 1 ||
      canonical(setups[0]) !== canonical(start) ||
      facts.at(-1)?.ordinal !== start.ordinal ||
      facts.some((fact) => fact.kind === "cleanup") ||
      worktrees.length !== 2 ||
      worktreeStart === undefined ||
      worktreeComplete === undefined ||
      worktreeComplete.phase !== "completed" ||
      worktreeComplete.result !== "succeeded" ||
      worktreeComplete.ownership !== "created" ||
      worktreeStart.effectId !== digest({ claimId: claim.claimId, stage: "worktree", basis }) ||
      worktreeStart.effectId !== worktreeComplete.effectId ||
      worktreeStart.branch !== claim.resources.branch ||
      worktreeComplete.branch !== claim.resources.branch ||
      worktreeStart.worktreePath !== basis.worktreePath ||
      worktreeComplete.worktreePath !== basis.worktreePath ||
      worktreeStart.projectCwd !== basis.projectCwd ||
      worktreeComplete.projectCwd !== basis.projectCwd ||
      worktreeStart.baseRef !== basis.baseRef ||
      worktreeComplete.baseRef !== basis.baseRef ||
      worktreeComplete.ordinal >= start.ordinal
    )
      return yield* fail("runtime_use_prepared_terminal_start_changed");
    return {
      kind: "prepared_native_setup" as const,
      projectRoot: row.workspace_root,
      nativeReference: claim.claimId,
      claimSha256: digest(claim),
      basisSha256: digest(basis),
      setupStartSha256: digest(start),
      historySha256: digest(effects),
    };
  },
  Effect.mapError(runtimeUseError),
);
