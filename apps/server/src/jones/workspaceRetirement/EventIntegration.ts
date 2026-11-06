import type { OrchestrationV2DomainEvent, OrchestrationV2ThreadShell, ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";
import { RetirementError } from "./Receipts.ts";

// Called within EventSink's transaction, before projections change. No filesystem or common-store lock is acquired here.
export const events = (sql:SqlClient.SqlClient["Service"], values:readonly OrchestrationV2DomainEvent[], getThread:(id:ThreadId)=>Effect.Effect<OrchestrationV2ThreadShell,unknown>) => Effect.gen(function* () {
  for(const event of values) {
    const prior=yield* getThread(event.threadId).pipe(Effect.orElseSucceed(()=>null));
    const payload=event.payload as unknown as {worktreePath?:string|null;id?:string;status?:string;projectId?:string};
    const paths=[...new Set([prior?.worktreePath,payload.worktreePath].filter((p):p is string=>typeof p==="string"))];
    // Binding changes, queue delivery, restore and owner metadata writes all conflict in this same transaction.
    for(const worktree of paths) {
      const retiring=yield* sql`SELECT 1 FROM jones_workspace_retirement_state WHERE worktree=${worktree} AND state='retiring'`;
      if(retiring.length) return yield* new RetirementError({reason:"retiring",target:worktree});
    }
    if(event.type==="thread.settled" && prior?.worktreePath) {
      if(event.payload.settledAt===null) return yield* new RetirementError({reason:"settlement_time_missing",target:prior.worktreePath});
      yield* sql`INSERT INTO jones_workspace_retirement_intents (event_id,thread_id,worktree,settled_at) VALUES (${event.id},${event.threadId},${prior.worktreePath},${DateTime.formatIso(event.payload.settledAt)}) ON CONFLICT(event_id) DO NOTHING`;
    }
    if(event.type==="run.created" && prior?.worktreePath) {
      const known=yield* sql<{incarnation_id:string}>`SELECT incarnation_id FROM jones_workspace_provenance WHERE worktree=${prior.worktreePath} ORDER BY rowid DESC LIMIT 1`;
      yield* sql`INSERT INTO jones_workspace_duty_claims VALUES (${`queued-turn:${event.payload.id}`},${prior.worktreePath},${known[0]?.incarnation_id ?? null},'queued-turn',${event.threadId},${JSON.stringify({runId:event.payload.id,threadId:event.threadId})},${DateTime.formatIso(event.occurredAt)},NULL) ON CONFLICT(claim_id) DO NOTHING`;
    }
    if(event.type==="run.updated" && ["completed","failed","interrupted","cancelled","rolled_back"].includes(event.payload.status))
      yield* sql`UPDATE jones_workspace_duty_claims SET released_at=${DateTime.formatIso(event.occurredAt)} WHERE claim_id=${`queued-turn:${event.payload.id}`} AND released_at IS NULL`;
  }
});
