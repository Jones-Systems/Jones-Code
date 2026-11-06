import type { PtyExitEvent } from "@t3tools/shared/PtyAdapter";
// @effect-diagnostics nodeBuiltinImport:off -- Controlled native-terminal seams execute the real optional observer; no live terminals or private state.
import * as assert from "node:assert/strict";
import * as childProcess from "node:child_process";
import { makeProjectScriptObserver } from "./ProjectScriptObservation.ts";
import { completionSentinel, wrapCommandForCompletion } from "./ScriptCompletionProtocol.ts";
import type { CompletionObservation } from "./CompletionObservation.ts";
import type { DutyIdentity } from "./DutyIdentity.ts";

const nonce = "a".repeat(32);
function fixture() {
  const dataListeners = new Set<(data: string) => void>();
  const exitListeners = new Set<(event: PtyExitEvent) => void>();
  const writes: string[] = [], captures: CompletionObservation[] = [], diagnostics: string[] = [];
  const process = {
    pid: 12345,
    onData: (listener: (data: string) => void) => { dataListeners.add(listener); return () => { dataListeners.delete(listener); }; },
    onExit: (listener: (event: PtyExitEvent) => void) => { exitListeners.add(listener); return () => { exitListeners.delete(listener); }; },
  };
  const physical: DutyIdentity = {status:"bound",incarnation:"controlled-native-incarnation",binding:{path:"/fixture/worktree",dev:"1",ino:"2",gitDirectory:"/fixture/repo/.git/worktrees/worktree",gitDevice:"1",gitInode:"3",commonDirectory:"/fixture/repo/.git"}};
  let current = true;
  const terminal: {process:typeof process;threadId:string;terminalId:string;launchCwd:string;observedCwd:string|null;shell:string;physical:DutyIdentity;isCurrent:()=>boolean} = { process, threadId:"thread", terminalId:"terminal", launchCwd:"/fixture/worktree", observedCwd:"/fixture/worktree", shell:"zsh", physical, isCurrent:()=>current };
  const observer = makeProjectScriptObserver({nonce:()=>nonce,capture:observation=>{captures.push(observation);return {status:"recorded",reason:null};},diagnostic:reason=>diagnostics.push(reason)});
  const input = {threadId:"thread",terminalId:"terminal",data:" echo fixture\r",observation:{invocationId:"4a792973-8d01-4eb5-a710-e34c74f57c6b",projectId:"requested-project",scriptId:"requested-script"}};
  const submit = (data:string) => {writes.push(data);};
  const emit = (data:string) => {for(const listener of dataListeners) listener(data);};
  const exit = () => {for(const listener of exitListeners) listener({exitCode:0,signal:null});};
  return {observer,terminal,input,submit,writes,captures,diagnostics,emit,exit,dataListeners,exitListeners,setCurrent:(value:boolean)=>{current=value;}};
}
export const scriptObservationCases: Record<string,()=>Promise<void>> = {
  "success is observed only from the exact native exit marker; echo and write acceptance are insufficient":async()=>{
    const f=fixture();f.observer.write(f.input,f.terminal,f.submit);
    assert.equal(f.writes.length,1);assert.equal(f.captures.length,0);
    assert.equal(f.dataListeners.size,1);assert.equal(f.exitListeners.size,1);
    assert.ok(f.writes[0]!.startsWith(" { "));
    f.emit(f.writes[0]!);assert.equal(f.captures.length,0);
    const marker=completionSentinel(nonce)+"0\n";
    f.emit(marker.slice(0,20));f.emit(marker.slice(20));
    assert.equal(f.captures.length,1);assert.equal(f.captures[0]!.status,"succeeded");
    assert.equal(f.captures[0]!.proofStatus,"deferred");assert.deepEqual(f.captures[0]!.roots,[]);
    assert.equal(f.captures[0]!.worktree,"/fixture/worktree");assert.equal(f.captures[0]!.lineage.rootInode,"2");
    assert.equal(f.dataListeners.size,0);assert.equal(f.exitListeners.size,0);
  },
  "nonzero completion remains failure and a duplicate invocation never sends another wrapper":async()=>{
    const f=fixture();f.observer.write(f.input,f.terminal,f.submit);
    f.emit(completionSentinel(nonce)+"23\n");
    assert.equal(f.captures[0]!.status,"failed");assert.equal(f.captures[0]!.lineage.exitCode,"23");
    f.observer.write(f.input,f.terminal,f.submit);assert.equal(f.writes.length,1);
    assert.throws(()=>f.observer.write({...f.input,data:"different\r"},f.terminal,f.submit),/invocation_changed/);
    assert.equal(f.writes.length,1);
  },
  "lost write response retains late native completion without replay or fallback":async()=>{
    const f=fixture(),lost=Error("bytes accepted; response lost");
    const submit=(data:string)=>{f.writes.push(data);throw lost;};
    assert.throws(()=>f.observer.write(f.input,f.terminal,submit),error=>error===lost);
    assert.equal(f.writes.length,1);assert.equal(f.captures.length,0);
    assert.throws(()=>f.observer.write(f.input,f.terminal,submit),error=>error===lost);assert.equal(f.writes.length,1);
    f.emit(completionSentinel(nonce)+"0\n");assert.equal(f.captures.length,1);
    assert.throws(()=>f.observer.write(f.input,f.terminal,submit),error=>error===lost);assert.equal(f.writes.length,1);
  },
  "terminal exit, close and replaced lifecycles remain unknown and detach listeners":async()=>{
    for(const mode of ["exit","close","replaced"]){
      const f=fixture();f.observer.write(f.input,f.terminal,f.submit);
      if(mode==="exit") f.exit();
      else if(mode==="close") f.observer.close(f.terminal.process);
      else {f.setCurrent(false);f.emit(completionSentinel(nonce)+"0\n");}
      assert.equal(f.captures.length,0);assert.equal(f.dataListeners.size,0);assert.equal(f.exitListeners.size,0);
      assert.ok(f.diagnostics.length>0);
    }
  },
  "prewrite observer failure and wrapper bounds preserve exactly one original native write":async()=>{
    for(const mode of ["unsupported","unbound","cwd-unavailable","oversized","subscribe-failed"]){
      const f=fixture();
      if(mode==="unsupported") f.terminal.shell="unknown-shell";
      if(mode==="unbound") f.terminal.physical={status:"unbound",reason:"native_worktree_identity_unavailable",nextAction:"controlled native readback"};
      if(mode==="cwd-unavailable") f.terminal.observedCwd=null;
      if(mode==="oversized") f.input.data="x".repeat(65535)+"\r";
      if(mode==="subscribe-failed") f.terminal.process.onExit=()=>{throw Error("observation unavailable");};
      f.observer.write(f.input,f.terminal,f.submit);
      assert.deepEqual(f.writes,[f.input.data]);assert.equal(f.captures.length,0);assert.equal(f.dataListeners.size,0);
    }
  },
  "metadata capture failure and bounded output never change native issuance":async()=>{
    const f=fixture();let captures=0;
    const observer=makeProjectScriptObserver({nonce:()=>nonce,capture:()=>{captures++;throw Error("controlled capture unavailable");},diagnostic:reason=>f.diagnostics.push(reason)});
    observer.write(f.input,f.terminal,f.submit);f.emit(completionSentinel(nonce)+"0\n");
    assert.equal(captures,1);assert.equal(f.writes.length,1);assert.ok(f.diagnostics.includes("completion_capture_unavailable"));
    const g=fixture();g.observer.write(g.input,g.terminal,g.submit);g.emit("x".repeat(65537));
    assert.equal(g.captures.length,0);assert.equal(g.dataListeners.size,0);assert.equal(g.exitListeners.size,0);
  },
  "a busy observer creates no backlog or synthetic duty and preserves a second ordinary command":async()=>{
    const f=fixture();f.observer.write(f.input,f.terminal,f.submit);
    const second={...f.input,observation:{...f.input.observation,invocationId:"5a792973-8d01-4eb5-a710-e34c74f57c6b"},data:"second\r"};
    f.observer.write(second,f.terminal,f.submit);assert.equal(f.writes.length,2);assert.equal(f.writes[1],second.data);
    assert.equal(f.dataListeners.size,1);f.observer.close(f.terminal.process);assert.equal(f.dataListeners.size,0);
  },
  "the actual POSIX wrapper preserves shell state and reports command exit":async()=>{
    const marker=completionSentinel(nonce);
    const wrapper=wrapCommandForCompletion("export JONES_SCRIPT_OBSERVER_FIXTURE=preserved\nfalse", "posix",marker,true).replaceAll("\r","\n");
    const output=await new Promise<string>((resolve,reject)=>childProcess.execFile("/bin/sh",["-c",wrapper+'\nprintf "state:%s\\n" "$JONES_SCRIPT_OBSERVER_FIXTURE"'],{timeout:2000,maxBuffer:65536,encoding:"utf8"},(error,out)=>error ? reject(error) : resolve(out)));
    assert.ok(output.includes(marker+"1"));assert.ok(output.includes("state:preserved"));
  },
};
