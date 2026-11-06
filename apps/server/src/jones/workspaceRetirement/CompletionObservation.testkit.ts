// @effect-diagnostics nodeBuiltinImport:off -- Bounded child fixtures prove deadline/reaping and no default content walk.
import assert from "node:assert/strict";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { fixture } from "./Conformance.testkit.ts";
import { completionObservation, recordCompletion, availableLineage } from "./CompletionObservation.ts";
import { outputObligations } from "./ProducerFinalizer.ts";
import { participantGuard } from "./ParticipantGuard.ts";
import {qualify} from "./Qualification.ts";

export const completionCases:Record<string,()=>Promise<void>>={
  "default completion records outcomes and lineage without walking large output descriptors":async()=>fixture(async({base,worktree,input})=>{
    const invalidRoot=Path.join(base,"not-a-directory");await FS.writeFile(invalidRoot,"large-tree descriptor; not traversable");
    for(const status of ["succeeded","failed","cancelled"] as const) {
      const observation=completionObservation({producer:"fixture",owner:"thread",worktree,status,roots:[Path.join(invalidRoot,"500000-entries")],lineage:{threadId:"thread",taskId:"task",pr:"https://github.com/example/project/pull/1"}});
      const result=recordCompletion(observation);
      assert.equal(result.status,"recorded",result.reason ?? undefined);
      const stored=JSON.parse(await FS.readFile(result.reference!,"utf8"));
      assert.equal(stored.status,status);assert.equal(stored.proofStatus,"deferred");
      assert.equal(stored.lineage.threadId,"thread");assert.equal(stored.lineage.taskId,"task");assert.equal(stored.lineage.branch,"refs/heads/task");
      for(const prohibited of ["manifest","content_manifest_sha256","entries","logical_bytes","reconstructible"]) assert.equal(prohibited in stored,false);
      assert.equal(stored.schema,"workspace-output-observation/v1");
    }
    assert.deepEqual(await outputObligations(worktree),[]);
    assert.equal(await FS.stat(Path.join(input.expected.git_dir,"workspace-outputs")).catch(()=>null),null);
    await FS.mkdir(Path.join(worktree,"build"));await FS.writeFile(Path.join(worktree,"build/note"),"unique note");
    const qualified=await qualify(input),detail=qualified.details?.find(d=>d.reason==="unattested_ignored");
    assert.equal(detail?.root,Path.join(worktree,"build"));assert.ok(detail?.missingFact);
    assert.ok(detail?.lineage.some(row=>row.threadId==="thread" && row.pr));
    assert.ok(detail?.nextAction.includes("must not manufacture"));
  }),
  "oversize capture starts no worker and stalled capture has no live work after its deadline":async()=>fixture(async({base,worktree})=>{
    const observation=completionObservation({producer:"fixture",owner:"thread",worktree,status:"succeeded",roots:[],lineage:{}});
    const marker=Path.join(base,"must-not-run");
    const oversize=recordCompletion({...observation,lineage:{oversize:"a".repeat(65536)}},`import{writeFileSync}from'node:fs';writeFileSync(${JSON.stringify(marker)},'bad');`);
    assert.equal(oversize.reason,"metadata_bound");assert.equal(await FS.stat(marker).catch(()=>null),null);
    const pidFile=Path.join(base,"worker.pid");
    const started=performance.now();
    const stalled=recordCompletion(observation,`import{writeFileSync}from'node:fs';writeFileSync(${JSON.stringify(pidFile)},String(process.pid));while(true){}`);
    assert.equal(stalled.reason,"metadata_deadline");
    assert.ok(performance.now()-started<1500,"deadline plus OS process close must remain bounded");
    const pid=Number(await FS.readFile(pidFile,"utf8"));
    assert.throws(()=>process.kill(pid,0),{code:"ESRCH"});
    assert.deepEqual(await outputObligations(worktree),[]);
  }),
  "unavailable and reserved sinks never invent allocation duties":async()=>fixture(async({worktree,input})=>{
    const observation=completionObservation({producer:"fixture",owner:"thread",worktree,status:"failed",roots:[],lineage:{}});
    assert.equal(recordCompletion(observation,"process.exitCode=1;").status,"unavailable");
    await participantGuard(input.expected,async()=>{});
    const store=Path.join(input.expected.git_common_dir,"workspace-lifecycle/reservations.sqlite3");
    const db=new DatabaseSync(store);
    try {db.exec("BEGIN IMMEDIATE");db.prepare("INSERT INTO reservations VALUES(?,?,?)").run(input.expected.admin_id,"other","row");db.exec("COMMIT");}finally{db.close();}
    assert.equal(recordCompletion(observation).status,"unavailable");
    assert.deepEqual(await outputObligations(worktree),[]);
    assert.equal(await FS.stat(Path.join(input.expected.git_dir,"workspace-completions")).catch(()=>null),null);
  }),
  "lineage capture selects explicit known identifiers without copying other environment data":async()=>{
    assert.deepEqual(availableLineage({T3CODE_THREAD_ID:"thread",T3CODE_TASK_ID:"task",T3CODE_RUN_ID:"run",T3CODE_ENVIRONMENT_ID:"environment",T3CODE_PROJECT_ID:"project",T3CODE_WORKSPACE_INCARNATION_ID:"incarnation",T3CODE_BRANCH:"branch",T3CODE_HEAD:"head",T3CODE_BASE_REF:"main",T3CODE_BASE_HEAD:"base",T3CODE_PREDECESSOR:"previous",T3CODE_PULL_REQUEST_URL:"pr",T3CODE_NATIVE_EVENT_ID:"event",PRIVATE_SECRET:"excluded"}),{threadId:"thread",taskId:"task",runId:"run",environmentId:"environment",projectId:"project",incarnationId:"incarnation",branch:"branch",head:"head",baseRef:"main",baseHead:"base",predecessor:"previous",pr:"pr",nativeEventId:"event"});
  },
};
