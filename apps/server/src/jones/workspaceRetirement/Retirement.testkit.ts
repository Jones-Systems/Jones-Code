// @effect-diagnostics nodeBuiltinImport:off -- Exact disposable fixtures execute production storage, qualification, reservation and unlink/removal paths.
import assert from "node:assert/strict";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import { DatabaseSync } from "node:sqlite";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import { fixture, command } from "./Conformance.testkit.ts";
import { Receipts, layer, RetirementError } from "./Receipts.ts";
import { retire } from "./RetirementExecutor.ts";
import { acquireReservation, holderIdentity, releaseReservation, type ReservationRecord } from "./Reservation.ts";
import { incarnation } from "./OutputAttestation.ts";
import { participantGuard } from "./ParticipantGuard.ts";
import { enumerateDisposal, disposePath } from "./Disposal.ts";
import { settlementAdmission } from "./SettlementAdmission.ts";

async function fixtureSql(path:string, after?:(query:string,values:unknown[])=>Promise<void>) {
  const db=new DatabaseSync(path);
  for(const name of ["136_JonesWorkspaceProvenance.ts","137_JonesWorkspaceRetirement.ts"]) {
    const source=await FS.readFile(new URL("../persistence/Migrations/"+name,import.meta.url),"utf8");
    for(const statement of source.matchAll(/yield\* sql`([^`]+)`/g)) db.exec(statement[1]!);
  }
  let depth=0;
  const sql=Object.assign((parts:TemplateStringsArray,...values:unknown[])=>Effect.tryPromise({try:async()=>{const query=parts.join("?");const rows=db.prepare(query).all(...values as (string|number|null)[]);await after?.(query,values);return rows;},catch:e=>e}),{
    withTransaction:<A,E,R>(body:Effect.Effect<A,E,R>)=>Effect.suspend(()=>{
      const name="test"+depth++, outer=depth===1;db.exec(outer ? "BEGIN IMMEDIATE" : "SAVEPOINT "+name);
      return body.pipe(Effect.tap(()=>Effect.sync(()=>db.exec(outer ? "COMMIT" : "RELEASE "+name))),Effect.catchCause(cause=>Effect.sync(()=>db.exec(outer ? "ROLLBACK" : "ROLLBACK TO "+name)).pipe(Effect.andThen(Effect.failCause(cause)))),Effect.ensuring(Effect.sync(()=>{depth--;})));
    }),
  }) as unknown as SqlClient.SqlClient["Service"];
  return {sql,db,close:()=>db.close()};
}
export const retirementCases:Record<string,()=>Promise<void>>={
  "durable settlement refuses unfinished scripts at ten minutes and missing final checkpoints":async()=>{
    assert.equal(settlementAdmission({script:"running",startedAt:0,now:600000,checkpoint:"ready",enabled:true}).reason,"settle_script_unfinished");
    assert.equal(settlementAdmission({script:"completed",startedAt:0,now:0,checkpoint:"pending",enabled:true}).reason,"final_checkpoint_unfinished");
    assert.equal(settlementAdmission({script:"completed",startedAt:0,now:0,checkpoint:"ready",enabled:false}).decision,"propose");
  },
  "common-store reservation excludes any participant until exact terminal release":()=>fixture(async({input})=>{
    const bound=input.expected,record:ReservationRecord={schema:"workspace-reservation/v1",admin_id:bound.admin_id,incarnation:incarnation(bound),executor:"jones-fixture",receipt:"fixture-receipt",...await holderIdentity()};
    await acquireReservation(bound,record);
    let writes=0;
    try {
      await assert.rejects(participantGuard(bound,async()=>{writes++;}),/retiring/);
      await assert.rejects(acquireReservation(bound,{...record,receipt:"second"}),/retiring/);
      await assert.rejects(acquireReservation(bound,{...record,receipt:"third"},{holderAbsent:async()=>true,receiptReadback:async()=>false}),/reservation_recovery_required/);
      assert.equal(writes,0);
    } finally {await releaseReservation(bound,record);}
    await participantGuard(bound,async()=>{writes++;});assert.equal(writes,1);
  }),
  "receipt and retiring transaction reject claims; verified absence is required for reconciliation":()=>fixture(async({base,worktree})=>{
    const f=await fixtureSql(Path.join(base,"jones.sqlite3"));
    try {
      await Effect.runPromise(Effect.gen(function*(){
        const store=yield* Receipts;
        yield* store.claim({id:"session",worktree,kind:"session",owner:"fixture",holder:'{"native":"fixture"}'});
        assert.equal((yield* store.claims(worktree)).length,1);
        const rejected=yield* store.reconcileClaim("session",'{"native":"fixture"}',false).pipe(Effect.result);
        assert.equal(rejected._tag,"Failure");
        yield* store.reconcileClaim("session",'{"native":"different"}',true);
        assert.equal((yield* store.claims(worktree)).length,1);
        yield* store.reconcileClaim("session",'{"native":"fixture"}',true);
        assert.equal((yield* store.claims(worktree)).length,0);
      }).pipe(Effect.provide(layer),Effect.provideService(SqlClient.SqlClient,f.sql)));
    } finally {f.close();}
  }),
  "production executor retires a clean fixture with durable receipt and retains branch":()=>fixture(async({base,repo,worktree,input})=>{
    const f=await fixtureSql(Path.join(base,"jones.sqlite3"));
    try {
      const result=await Effect.runPromise(retire({cause:"sweep",projectRoot:repo,worktree,maxLevel:"branch-retained",admit:Effect.void,observe:Effect.succeed(input),revalidate:Effect.void,finalizeOutputs:()=>Effect.void}).pipe(Effect.provide(layer),Effect.provideService(SqlClient.SqlClient,f.sql)));
      assert.equal(result.state,"retired");assert.equal(await FS.lstat(worktree).then(()=>true,()=>false),false);
      assert.equal(f.db.prepare("SELECT state FROM jones_workspace_retirement_receipts").get()?.state,"retired");
      assert.equal((await command(repo,["show-ref","--verify","refs/heads/task"])).length>0,true);
      assert.equal(f.db.prepare("SELECT state FROM jones_workspace_retirement_state").get()?.state,"retired");
    } finally {f.close();}
  }),
  "tracked changes journal precedes no-follow symlink disposal and keeps the external target":()=>fixture(async({base,repo,worktree,input})=>{
    const external=Path.join(base,"outside");await FS.writeFile(external,"retained");await FS.symlink(external,Path.join(worktree,"link"));
    await FS.writeFile(Path.join(worktree,"untracked"),"covered");
    input.coverage={keepMode:"tracked-changes",exactRoots:[Path.join(worktree,"link"),Path.join(worktree,"untracked")]};
    const f=await fixtureSql(Path.join(base,"jones.sqlite3"));
    try {
      const result=await Effect.runPromise(retire({cause:"owner_action",projectRoot:repo,worktree,maxLevel:"branch-retained",admit:Effect.void,observe:Effect.succeed(input),revalidate:Effect.void,finalizeOutputs:()=>Effect.void}).pipe(Effect.provide(layer),Effect.provideService(SqlClient.SqlClient,f.sql)));
      assert.equal(result.state,"retired");assert.equal(await FS.readFile(external,"utf8"),"retained");
      assert.deepEqual(f.db.prepare("SELECT state FROM jones_workspace_disposal_journal ORDER BY ordinal").all().map(r=>r.state),["completed","completed"]);
    } finally {f.close();}
  }),
  "partial disposal records actual paths and a second invocation cannot retry it":()=>fixture(async({base,repo,worktree,input})=>{
    const a=Path.join(worktree,"a"),b=Path.join(worktree,"b");await FS.writeFile(a,"a");await FS.writeFile(b,"b");
    input.coverage={keepMode:"tracked-changes",exactRoots:[a,b]};
    const f=await fixtureSql(Path.join(base,"jones.sqlite3"),async(query,values)=>{if(query.includes("UPDATE jones_workspace_disposal_journal") && values[0]==="completed" && values[2]===0) await FS.writeFile(b,"changed after the first disposal");});
    const request={cause:"sweep" as const,projectRoot:repo,worktree,maxLevel:"branch-retained" as const,admit:Effect.void,observe:Effect.succeed(input),revalidate:Effect.void,finalizeOutputs:()=>Effect.void};
    try {
      const run=()=>Effect.runPromise(retire(request).pipe(Effect.provide(layer),Effect.provideService(SqlClient.SqlClient,f.sql)));
      const result=await run();assert.equal(result.state,"partial");
      assert.equal(await FS.lstat(a).then(()=>true,()=>false),false);assert.equal(await FS.readFile(b,"utf8"),"changed after the first disposal");
      assert.equal(f.db.prepare("SELECT state FROM jones_workspace_retirement_receipts").get()?.state,"partial");
      assert.deepEqual(f.db.prepare("SELECT state FROM jones_workspace_disposal_journal ORDER BY ordinal").all().map(r=>r.state),["completed","invoking"]);
      await assert.rejects(run(),/retiring/);assert.equal(await FS.readFile(b,"utf8"),"changed after the first disposal");
    } finally {f.close();}
  }),
  "a change before final checks aborts with no disposal and releases its reservation":()=>fixture(async({base,repo,worktree,input})=>{
    const f=await fixtureSql(Path.join(base,"jones.sqlite3"));let observations=0;
    try {
      const result=await Effect.runPromise(retire({cause:"sweep",projectRoot:repo,worktree,maxLevel:"branch-retained",admit:Effect.void,observe:Effect.tryPromise({try:async()=>{if(++observations===2) await FS.writeFile(Path.join(worktree,"new"),"must remain");return input;},catch:e=>new RetirementError({target:worktree,reason:"fixture_observation_failed",cause:e})}),revalidate:Effect.void,finalizeOutputs:()=>Effect.void}).pipe(Effect.provide(layer),Effect.provideService(SqlClient.SqlClient,f.sql)));
      assert.equal(result.state,"not_retired");assert.equal(await FS.readFile(Path.join(worktree,"new"),"utf8"),"must remain");
      assert.equal(f.db.prepare("SELECT COUNT(*) AS n FROM jones_workspace_disposal_journal").get()?.n,0);
      assert.equal(f.db.prepare("SELECT COUNT(*) AS n FROM jones_workspace_retirement_state").get()?.n,0);
      await participantGuard(input.expected,async()=>{});
    } finally {f.close();}
  }),
  "settlement replay never starts another script and completion is durable":()=>fixture(async({base,worktree})=>{
    const f=await fixtureSql(Path.join(base,"jones.sqlite3"));
    f.db.prepare("INSERT INTO jones_workspace_retirement_intents(event_id,thread_id,worktree,settled_at) VALUES(?,?,?,?)").run("event","thread",worktree,"2026-11-06T00:00:00Z");
    try {await Effect.runPromise(Effect.gen(function*(){
      const store=yield* Receipts;
      assert.equal(yield* store.startScript("event"),true);assert.equal(yield* store.startScript("event"),false);
      yield* store.finishScript("event","completed",0);
      assert.equal((yield* store.settlement("thread",worktree,"2026-11-06T00:00:00Z"))?.script_state,"completed");
      assert.equal(yield* store.startScript("event"),false);
    }).pipe(Effect.provide(layer),Effect.provideService(SqlClient.SqlClient,f.sql)));} finally {f.close();}
  }),
  "replacement parent symlinks cannot redirect a journaled unlink":()=>fixture(async({base,worktree,input})=>{
    await FS.mkdir(Path.join(worktree,"dir"));await FS.writeFile(Path.join(worktree,"dir/a"),"local");
    const journal=await enumerateDisposal(input.expected,"tracked-changes");
    const outside=Path.join(base,"outside");await FS.mkdir(outside);await FS.writeFile(Path.join(outside,"a"),"protected");
    await FS.rename(Path.join(worktree,"dir"),Path.join(worktree,"old"));await FS.symlink(outside,Path.join(worktree,"dir"));
    await assert.rejects(disposePath(input.expected,journal[0]!));assert.equal(await FS.readFile(Path.join(outside,"a"),"utf8"),"protected");
  }),
};
