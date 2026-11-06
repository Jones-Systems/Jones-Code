// @effect-diagnostics nodeBuiltinImport:off -- Owner absence uses native process identity/session metadata, never PID existence alone or process arguments.
import * as NodeProcess from "node:process";
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import { holderIdentity, nativePid } from "./Reservation.ts";
import { nativePlatform } from "./Filesystem.ts";

type Host = Awaited<ReturnType<typeof holderIdentity>> & {process_namespace:string};
export type NativeDutyOwner =
  | {schema:"workspace-duty-owner/v1";type:"in-process";host:Host;nonce:string}
  | {schema:"workspace-duty-owner/v1";type:"native-session";host:Host;pid:number;start:string;session:string}
  | {schema:"workspace-duty-owner/v1";type:"unresolved";reason:string;nextAction:string};
export interface NativeOwnerIO {
  host:()=>Promise<Host>;
  processes:()=>Promise<string>;
  linuxIdentity:(pid:number)=>Promise<{bootId:string;startTime:string}>;
}
const nativeIO:NativeOwnerIO={
  host:async()=>({...await holderIdentity(),process_namespace:nativePlatform === "linux" ? await NodeFSP.readlink("/proc/self/ns/pid") : nativePlatform === "darwin" ? "darwin-host" : ""}),
  processes:()=>new Promise((resolve,reject)=>NodeChildProcess.execFile("/bin/ps",["-axo","pid=,sess=,lstart="],{timeout:1500,maxBuffer:2*1024*1024,encoding:"utf8",env:{...NodeProcess.env,LC_ALL:"C"}},(error,out)=>error ? reject(error) : resolve(out))),
  linuxIdentity:async pid=>{
    const bootId=(await NodeFSP.readFile("/proc/sys/kernel/random/boot_id","utf8")).trim();
    const stat=await NodeFSP.readFile(`/proc/${pid}/stat`,"utf8");
    return {bootId,startTime:stat.slice(stat.lastIndexOf(")")+2).split(/\s+/)[19]!};
  },
};
function processes(value:string) {
  const lines=value.trim().split("\n");
  if(lines.length>20000) throw new Error("process_bound_exceeded");
  return lines.map(line=>{
    const match=/^\s*(\d+)\s+(\S+)\s+(.+?)\s*$/.exec(line);
    if(!match) throw new Error("native_process_snapshot_unavailable");
    return {pid:Number(match[1]),session:match[2]!,start:match[3]!};
  });
}
export const unresolvedOwner=(reason:string):NativeDutyOwner=>({schema:"workspace-duty-owner/v1",type:"unresolved",reason,nextAction:"Use the named native session/terminal owner to observe its process incarnation and isolated session, or record actual completion/drain. Server PID absence does not resolve this child duty."});
export async function inProcessOwner(nonce:string):Promise<NativeDutyOwner> {
  try {return {schema:"workspace-duty-owner/v1",type:"in-process",host:await nativeIO.host(),nonce};}
  catch {return unresolvedOwner("server_process_incarnation_unavailable");}
}
export async function captureNativeOwner(pid:number,expected?:{bootId:string;startTime:string},io:NativeOwnerIO=nativeIO):Promise<NativeDutyOwner> {
  try {
    if(!Number.isSafeInteger(pid) || pid<=0) throw new Error("native_pid_unavailable");
    const host=await io.host();
    if(expected) {
      if(nativePlatform!=="linux") throw new Error("provider_native_identity_adapter_unavailable");
      const actual=await io.linuxIdentity(pid);
      if(actual.bootId!==expected.bootId || actual.startTime!==expected.startTime) throw new Error("provider_process_incarnation_changed");
    }
    const rows=processes(await io.processes()), native=rows.find(p=>p.pid===pid), server=rows.find(p=>p.pid===nativePid);
    // A shared session cannot establish child absence. Never adopt the server's session.
    if(!native || !server || native.session===server.session || ["0","-","?"].includes(native.session)) throw new Error("isolated_native_session_unverified");
    return {schema:"workspace-duty-owner/v1",type:"native-session",host,pid,start:native.start,session:native.session};
  } catch(cause) {return unresolvedOwner(cause instanceof Error ? cause.message : "native_owner_observation_unavailable");}
}
export async function observeNativeOwner(owner:NativeDutyOwner,io:NativeOwnerIO=nativeIO):Promise<"verified_absent"|"present"|"unknown"> {
  try {
    if(owner.schema!=="workspace-duty-owner/v1" || owner.type==="unresolved") return "unknown";
    if(![owner.host.host,owner.host.boot_id,owner.host.process_start].every(v=>typeof v==="string" && v.length>0) || !Number.isSafeInteger(owner.host.pid) || owner.host.pid<=0) return "unknown";
    if(owner.type==="native-session" && (!owner.session || !owner.start || !Number.isSafeInteger(owner.pid) || owner.pid<=0)) return "unknown";
    const host=await io.host();
    if (![host.host,host.boot_id,host.process_start,host.process_namespace].every(value => typeof value === "string" && value.length > 0)) return "unknown";
    if(host.host!==owner.host.host || !owner.host.process_namespace || host.process_namespace!==owner.host.process_namespace) return "unknown";
    // A verified different boot on the same host excludes all prior-boot children.
    if(host.boot_id!==owner.host.boot_id) return "verified_absent";
    const rows=processes(await io.processes());
    // An in-process job may be awaiting a spawned Git/setup child. Its server's
    // disappearance cannot establish child drain. Actual completion releases it;
    // only a verified new boot establishes restart absence without that owner API.
    if(owner.type==="in-process") return rows.some(p=>p.pid===owner.host.pid && p.start===owner.host.process_start) ? "present" : "unknown";
    // The terminal/provider's entire observed native session must be gone. A dead
    // launcher with children in that session remains present after server restart.
    if(rows.some(p=>p.session===owner.session)) return "present";
    return "verified_absent";
  } catch {return "unknown";}
}
