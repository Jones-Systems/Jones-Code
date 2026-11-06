import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";
import type { Receipts } from "./Receipts.ts";

export interface PersistedDuty {claim_id:string;worktree:string;kind:string;owner:string;holder_json:string}
export class ClaimAbsence extends Context.Reference<{
  readonly observe: (claim:PersistedDuty)=>Effect.Effect<{holder:string;status:"verified_absent"|"present"|"unknown"}>;
}>("jones/workspaceRetirement/ClaimAbsence",{defaultValue:()=>({observe:claim=>Effect.succeed({holder:claim.holder_json,status:"unknown" as const})})}) {}
export const reconcile = Effect.fn("WorkspaceRetirement.reconcileClaims")(function* (sql:SqlClient.SqlClient["Service"],store:Receipts["Service"]) {
  const owner=yield* ClaimAbsence;
  const claims=yield* sql<PersistedDuty>`SELECT claim_id,worktree,kind,owner,holder_json FROM jones_workspace_duty_claims WHERE released_at IS NULL LIMIT 10001`;
  if(claims.length>10000) return {retained:claims.length,released:0,status:"bounded" as const};
  let released=0;
  for(const claim of claims) {
    const observed=yield* owner.observe(claim);
    if(observed.status!=="verified_absent" || observed.holder!==claim.holder_json) continue;
    yield* store.reconcileClaim(claim.claim_id,claim.holder_json,true);released++;
  }
  return {retained:claims.length-released,released,status:"observed" as const};
});
