// @effect-diagnostics nodeBuiltinImport:off -- C4 readback observes both Git registration and the filesystem after unforced removal.
import * as FS from "node:fs/promises";
import { randomUUID } from "node:crypto";
import * as Effect from "effect/Effect";
import { canonical, exists, git } from "./Filesystem.ts";
import { incarnation, type Binding } from "./OutputAttestation.ts";
import { finalCheck, qualify, type QualificationInput } from "./Qualification.ts";
import { Receipts, RetirementError, type RetirementReceipt } from "./Receipts.ts";
import { acquireReservation, holderIdentity, releaseReservation, type ReservationRecord } from "./Reservation.ts";
import { enumerateDisposal, disposePath, verifyClean } from "./Disposal.ts";
import { CONSUMER_LIMITS } from "./ConsumerProbe.ts";
import { withWorkspaceLease } from "../../workspace/workspaceLease.ts";

export interface RetirementRequest {
  cause: RetirementReceipt["cause"]; projectRoot: string; worktree: string;
  maxLevel: "contained" | "branch-retained";
  // Existing native admission and automatic assessment run BEFORE acquiring either reservation.
  admit: Effect.Effect<void, RetirementError>;
  observe: Effect.Effect<QualificationInput, RetirementError>;
  // Read-only revalidation, including durable settlement completion and checkpoint state.
  revalidate: Effect.Effect<void, RetirementError>;
  finalizeOutputs: (receipt:RetirementReceipt)=>Effect.Effect<void, RetirementError>;
}
const io = <A>(target:string, action:()=>Promise<A>) => Effect.tryPromise({try:action,catch:cause=>new RetirementError({target,reason:"observation_failed",cause})});
export const retire = Effect.fn("WorkspaceRetirement.retire")(function* (request:RetirementRequest) {
  const store=yield* Receipts;
  yield* request.admit;
  const input=yield* request.observe;
  const previous=yield* io(request.worktree,()=>qualify(input));
  if(previous.outcome!=="retirable" || (previous.level==="branch-retained" && request.maxLevel==="contained"))
    return {state:"not_retired" as const,reasons:previous.reasons.length ? previous.reasons : ["retention_scope"],qualification:previous};
  return yield* withWorkspaceLease(request.worktree,Effect.uninterruptibleMask(restore=>Effect.gen(function* () {
    yield* request.revalidate;
    const bound=input.expected, id=randomUUID();
    const record:ReservationRecord={schema:"workspace-reservation/v1",admin_id:bound.admin_id,incarnation:incarnation(bound),executor:"jones",receipt:id,...(yield* io(request.worktree,holderIdentity))};
    yield* io(request.worktree,()=>acquireReservation(bound,record));
    let began=false, effected=false, terminal=false;
    const receipt:RetirementReceipt={schema:"worktree-retirement/v1",id,cause:request.cause,state:"intent",binding:bound,incarnation:record.incarnation,qualification:previous,coverage:input.coverage ?? {},preservationRefs:[],journal:id,disposed:[],reasons:[],limits:CONSUMER_LIMITS,bytes:{logical:null,allocated:null,statvfs_delta:null}};
    const finish=Effect.gen(function* () {
      yield* store.finish(receipt);
      terminal=true;
    });
    const execution=Effect.gen(function* () {
      yield* store.begin(receipt); began=true;
      yield* request.revalidate;
      const freshInput=yield* request.observe;
      const final=yield* io(request.worktree,()=>finalCheck(freshInput,previous));
      if(final.conflict || canonical(freshInput.expected)!==canonical(bound)) return yield* new RetirementError({reason:"final_check_conflict",target:request.worktree});
      receipt.qualification=final.result;
      const journal=yield* io(request.worktree,()=>enumerateDisposal(bound,input.coverage?.keepMode ?? "uncommitted-changes"));
      // The journal is durable before preservation/disposal starts; its entries are the only deletion selections.
      yield* store.journal(id,journal);
      for(const [n,head] of (previous.preservationTips ?? []).entries()) {
        const ref=`refs/retired-worktrees/${record.incarnation}/${n}`;
        receipt.preservationRefs.push({ref,head});
      }
      // Persist the complete restore plan before any ref or filesystem mutation.
      yield* store.finish({...receipt,state:"intent"});
      for(const ref of receipt.preservationRefs) {
        effected=true;
        yield* io(request.worktree,()=>git(request.projectRoot,["update-ref",ref.ref,ref.head,"0".repeat(ref.head.length)]));
      }
      for(const [n,entry] of journal.entries()) if(entry.kind==="untracked") {
        yield* store.completePath(id,n,"invoking");
        effected=true;
        const result=yield* io(request.worktree,()=>disposePath(bound,entry)).pipe(Effect.result);
        if(result._tag==="Failure") {
          // A lost unlink reply is reconciled by exact path readback, never another unlink.
          if(!(yield* io(request.worktree,()=>exists(entry.path)))) {receipt.disposed.push(entry.path);yield* store.completePath(id,n,"completed");}
          return yield* result.failure;
        }
        receipt.disposed.push(entry.path);yield* store.completePath(id,n,"completed");
      }
      yield* io(request.worktree,()=>verifyClean(bound,journal));
      yield* request.revalidate;
      // The common-store row and Jones retiring state still exclude every participant here.
      effected=true;
      const removed=yield* restore(io(request.worktree,()=>git(request.projectRoot,["worktree","remove","--",bound.path]))).pipe(Effect.result);
      const readback=yield* io(request.worktree,()=>removalReadback(request.projectRoot,receipt));
      if(readback!=="retired") {
        receipt.state=readback==="present" && removed._tag==="Failure" && !receipt.disposed.length && !receipt.preservationRefs.length ? "not_retired" : "partial";
        receipt.reasons=[readback==="present" ? "git_removal_refused" : "removal_readback_unavailable"];
      } else {receipt.state="retired";receipt.reasons=[];}
      yield* finish;
      if(receipt.state==="retired") yield* request.finalizeOutputs(receipt).pipe(Effect.catch(()=>{
        receipt.reasons.push("output_finalization_pending");
        return store.finish(receipt);
      }));
      return receipt;
    });
    return yield* execution.pipe(
      Effect.catchCause(cause=>Effect.gen(function* () {
        if(!began) return yield* Effect.failCause(cause);
        if(terminal) return receipt;
        receipt.state=effected ? "partial" : "not_retired";
        receipt.reasons=[effected ? "partial_effect_reconcile_required" : "conflict"];
        yield* finish;
        return receipt;
      })),
      Effect.ensuring(Effect.suspend(()=>!began || terminal ? io(request.worktree,()=>releaseReservation(bound,record)).pipe(Effect.orDie) : Effect.void)),
    );
  })));
});
export async function removalReadback(projectRoot:string, receipt:RetirementReceipt):Promise<"retired"|"present"|"unknown"> {
  try {
    const listed=(await git(projectRoot,["worktree","list","--porcelain","-z"])).toString().split("\0");
    const present=await exists(receipt.binding.path), registered=listed.includes("worktree "+receipt.binding.path);
    if(present || registered) return present && registered ? "present" : "unknown";
    for(const ref of receipt.preservationRefs) if((await git(projectRoot,["rev-parse",ref.ref])).toString().trim()!==ref.head) return "unknown";
    if(receipt.qualification.branch && (await git(projectRoot,["rev-parse",receipt.qualification.branch])).toString().trim()!==receipt.qualification.head) return "unknown";
    for(const ref of receipt.qualification.containingRefs ?? []) {
      await git(projectRoot,["merge-base","--is-ancestor",receipt.qualification.head!,ref]);
    }
    // No restore input is read from the removed admin directory.
    await FS.stat(receipt.binding.git_common_dir);
    return "retired";
  } catch {return "unknown";}
}
