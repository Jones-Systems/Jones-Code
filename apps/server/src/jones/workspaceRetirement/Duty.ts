import { nativePid } from "./Reservation.ts";
import { reconcile } from "./ClaimReconciliation.ts";
// @effect-diagnostics nodeBuiltinImport:off -- A run-local nonce distinguishes server holders; restart alone never proves a consumer absent.
import * as NodeCrypto from "node:crypto";
import * as Effect from "effect/Effect";
import * as Context from "effect/Context";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import { Receipts, layer } from "./Receipts.ts";
import { captureNativeOwner, type NativeDutyOwner } from "./NativeDutyOwner.ts";
import type { DutyIdentity } from "./DutyIdentity.ts";

export class NativeOwnerCapture extends Context.Reference<{
  readonly capture:(pid:number,expected?:{bootId:string;startTime:string})=>Promise<NativeDutyOwner>;
}>("jones/workspaceRetirement/NativeOwnerCapture",{defaultValue:()=>({capture:captureNativeOwner})}) {}

// Only actual duty identity is bound here; optional provenance/registry enrichment never gates ordinary work.
export const hooks = Effect.gen(function* () {
  const sql=yield* Effect.serviceOption(SqlClient.SqlClient);
  const store=Option.isSome(sql) ? yield* Receipts.pipe(Effect.provide(layer),Effect.provideService(SqlClient.SqlClient,sql.value)) : null;
  const nativeBindings=new WeakMap<object,(pid:number,expected?:{bootId:string;startTime:string})=>Effect.Effect<void,unknown>>();
  const sessions=new Map<string,Effect.Effect<void,unknown>>();
  const identities=new WeakMap<object,DutyIdentity>();
  const nativeOwner=yield* NativeOwnerCapture;
  return {
    hold: Effect.fn("WorkspaceRetirement.holdDuty")(function* (worktree:string|null, kind:string, owner:string) {
      if(worktree===null || store===null) return Effect.void;
      const id=NodeCrypto.randomUUID();
      for(let attempt=0;attempt<=20;attempt++) {
        const holder=JSON.stringify({serverPid:nativePid,nonce:id,kind,owner});
        const result=yield* store.claim({id,worktree,kind,owner,holder}).pipe(Effect.result);
        if(result._tag==="Success") {
          const release=store.releaseClaim(id);
          identities.set(release,result.value);
          nativeBindings.set(release,(pid,expected)=>Effect.promise(()=>nativeOwner.capture(pid,expected)).pipe(Effect.flatMap(native=>store.bindNativeOwner(id,holder,native))));
          if(kind==="session") sessions.set(owner,release);
          return release;
        }
        if(result.failure.reason!=="retiring" || attempt===20) return yield* Effect.fail(result.failure);
        yield* Effect.sleep("250 millis");
      }
      return Effect.void;
    }),
    bindNative:(release:Effect.Effect<void,unknown>,pid:number)=>nativeBindings.get(release)?.(pid) ?? Effect.void,
    identity:(release:Effect.Effect<void,unknown>)=>identities.get(release),
    // Consume the observation already made by the provider's native lifecycle. No
    // extra diagnostics request, capture worker, provenance or start-path retry.
    bindSession:Effect.fn("WorkspaceRetirement.bindSessionOwner")(function* (owner:string,providerThreadId:string,generation:string) {
      if(Option.isNone(sql)) return;
      const release=sessions.get(owner);if(!release) return;
      const rows=yield* sql.value<{observation_json:string}>`SELECT observation_json FROM jones_physical_process_evidence WHERE provider_thread_id=${providerThreadId} AND runtime_generation=${generation} ORDER BY rowid DESC LIMIT 1`;
      if(!rows[0]) return;
      let observation:{status:string;responder?:{pid:number;bootId:string;startTime:string}};
      try {observation=JSON.parse(rows[0].observation_json);} catch {return;}
      if(observation.status!=="observed" || !observation.responder) return;
      yield* nativeBindings.get(release)?.(observation.responder.pid,{bootId:observation.responder.bootId,startTime:observation.responder.startTime}) ?? Effect.void;
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
