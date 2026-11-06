import { reconcile } from "./ClaimReconciliation.ts";
// @effect-diagnostics nodeBuiltinImport:off -- A run-local nonce distinguishes server holders; restart alone never proves a consumer absent.
import { randomUUID } from "node:crypto";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import { Receipts, layer } from "./Receipts.ts";

// No filesystem, registry CLI or provenance enrichment occurs on ordinary start paths.
export const hooks = Effect.gen(function* () {
  const sql=yield* Effect.serviceOption(SqlClient.SqlClient);
  const store=Option.isSome(sql) ? yield* Receipts.pipe(Effect.provide(layer),Effect.provideService(SqlClient.SqlClient,sql.value)) : null;
  return {
    hold: Effect.fn("WorkspaceRetirement.holdDuty")(function* (worktree:string|null, kind:string, owner:string) {
      if(worktree===null || store===null) return Effect.void;
      const id=randomUUID();
      for(let attempt=0;attempt<=20;attempt++) {
        const result=yield* store.claim({id,worktree,kind,owner,holder:JSON.stringify({serverPid:process.pid,nonce:id,kind,owner})}).pipe(Effect.result);
        if(result._tag==="Success") return store.releaseClaim(id);
        if(result.failure.reason!=="retiring" || attempt===20) return yield* Effect.fail(result.failure);
        yield* Effect.sleep("250 millis");
      }
      return Effect.void;
    }),
  };
});

export const withDuty = Effect.fn("WorkspaceRetirement.withDuty")(function* <A,E,R>(worktree:string|null,kind:string,owner:string,effect:Effect.Effect<A,E,R>) {
  const duty=yield* hooks;
  const release=yield* duty.hold(worktree,kind,owner);
  return yield* effect.pipe(Effect.ensuring(release.pipe(Effect.orDie)));
});

export const startup = Effect.gen(function* () {
  const sql=yield* Effect.serviceOption(SqlClient.SqlClient);
  if(Option.isNone(sql)) return;
  const store=yield* Receipts.pipe(Effect.provide(layer),Effect.provideService(SqlClient.SqlClient,sql.value));
  yield* reconcile(sql.value,store).pipe(Effect.catch(cause=>Effect.logDebug("Workspace claim reconciliation unavailable",{cause})));
});
