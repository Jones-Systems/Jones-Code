import * as Schema from "effect/Schema";
import { NativeDutyOwnerJson } from "./DurableCodecs.ts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";
import type { Receipts } from "./Receipts.ts";
import { observeNativeOwner } from "./NativeDutyOwner.ts";

export interface PersistedDuty {
  claim_id: string;
  worktree: string;
  kind: string;
  owner: string;
  holder_json: string;
  native_owner_json: string | null;
}
export class ClaimAbsence extends Context.Reference<{
  readonly observe: (
    claim: PersistedDuty,
  ) => Effect.Effect<{ holder: string; status: "verified_absent" | "present" | "unknown" }>;
}>("jones/workspaceRetirement/ClaimAbsence", {
  defaultValue: () => ({
    observe: Effect.fn("WorkspaceRetirement.observeClaimAbsence")(function* (claim: PersistedDuty) {
      if (claim.native_owner_json === null)
        return { holder: claim.holder_json, status: "unknown" as const };
      const owner = yield* Schema.decodeEffect(NativeDutyOwnerJson)(claim.native_owner_json).pipe(
        Effect.result,
      );
      // Invalid durable facts remain unknown; a decode failure never proves absence.
      if (owner._tag === "Failure")
        return { holder: claim.holder_json, status: "unknown" as const };
      const status = yield* Effect.promise(() => observeNativeOwner(owner.success));
      return { holder: claim.holder_json, status };
    }),
  }),
}) {}
export const reconcile = Effect.fn("WorkspaceRetirement.reconcileClaims")(function* (
  sql: SqlClient.SqlClient,
  store: Receipts["Service"],
) {
  const owner = yield* ClaimAbsence;
  const claims =
    yield* sql<PersistedDuty>`SELECT c.claim_id,c.worktree,c.kind,c.owner,c.holder_json,i.native_owner_json FROM jones_workspace_duty_claims c LEFT JOIN jones_workspace_duty_identity i ON i.claim_id=c.claim_id WHERE c.released_at IS NULL LIMIT 10001`;
  if (claims.length > 10000)
    return { retained: claims.length, released: 0, status: "bounded" as const };
  let released = 0;
  for (const claim of claims) {
    // Settlement recovery atomically couples this guard's release to its durable script state.
    if (claim.kind === "settlement-intent") continue;
    if (claim.kind === "queued-turn") {
      // The durable run owner, rather than a vanished server, owns queued intent.
      const runs = yield* sql<{
        status: string;
        completed_at: string | null;
      }>`SELECT status,completed_at FROM orchestration_v2_projection_runs WHERE run_id=${claim.claim_id.slice("queued-turn:".length)} AND thread_id=${claim.owner}`;
      if (
        runs[0]?.completed_at &&
        ["completed", "failed", "interrupted", "cancelled", "rolled_back"].includes(runs[0].status)
      ) {
        if (yield* store.reconcileClaim(claim.claim_id, claim.holder_json, true)) released++;
      }
      continue;
    }
    const observed = yield* owner.observe(claim);
    if (observed.status !== "verified_absent" || observed.holder !== claim.holder_json) continue;
    if (yield* store.reconcileClaim(claim.claim_id, claim.holder_json, true)) released++;
  }
  return { retained: claims.length - released, released, status: "observed" as const };
});
