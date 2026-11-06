// @effect-diagnostics nodeBuiltinImport:off -- unlinkat with no-follow parent descriptors prevents disposal from traversing symlinks.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeChildProcess from "node:child_process";
import { canonical, git, inside } from "./Filesystem.ts";
import type { Binding } from "./OutputAttestation.ts";
import { classifyIgnored } from "./IgnoredClassifier.ts";
import type { JournalEntry } from "./Receipts.ts";

async function pathIdentity(path:string) {
  const s=await NodeFSP.lstat(path,{bigint:true});
  return canonical({dev:String(s.dev),ino:String(s.ino),mode:String(s.mode),size:String(s.size),mtime_ns:String(s.mtimeNs),ctime_ns:String(s.ctimeNs)});
}
export async function enumerateDisposal(bound:Binding, keepMode:string) {
  const bytes=await git(bound.path,["ls-files","--others","--exclude-standard","-z"]);
  if(bytes.toString().includes("\ufffd")) throw new Error("observation_failed");
  const untracked=bytes.toString().split("\0").filter(Boolean);
  if(untracked.length>10000) throw new Error("untracked_paths_bound");
  const ignored=(await git(bound.path,["ls-files","--others","--ignored","--exclude-standard","--directory","-z"])).toString().split("\0").filter(Boolean).map(p=>p.replace(/\/$/,""));
  const entries:JournalEntry[]=[];
  for(const [kind,paths] of [["untracked",untracked],["ignored",ignored]] as const) for(const relative of paths) {
    const absolute=NodePath.resolve(bound.path,relative);
    if(!inside(absolute,bound.path) || absolute===bound.path || relative.split("/").some(p=>p===".git" || p==="..")) throw new Error("identity_mismatch");
    // Every untracked selection gets the same sensitive/nested-store protections as ignored roots.
    if(kind==="untracked") {
      if(keepMode!=="tracked-changes") throw new Error("untracked");
      const result=await classifyIgnored({worktree:bound.path,root:absolute,common:bound.git_common_dir,coverage:{keepMode:"tracked-changes",exactRoots:[absolute]}});
      if(result.outcome!=="disposable") throw new Error(result.reason ?? "untracked");
      const stat=await NodeFSP.lstat(absolute);
      if(stat.isDirectory()) throw new Error("untracked_directory_unverifiable");
    }
    entries.push({path:absolute,kind,identity:await pathIdentity(absolute),state:"planned"});
  }
  return entries;
}
const unlinkScript = String.raw`
import os,sys,json,stat
v=json.loads(sys.argv[1]); b=v['binding']; p=v['path']; expected=json.loads(v['identity'])
parts=os.path.relpath(p,b['path']).split(os.sep)
if any(x in ('','.', '..','.git') for x in parts): raise RuntimeError('identity_mismatch')
fds=[]
try:
 fd=os.open('/',os.O_RDONLY|os.O_DIRECTORY); fds.append(fd)
 for part in b['path'].strip('/').split('/'):
  fd=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=fd); fds.append(fd)
 root=os.fstat(fd)
 if root.st_dev!=b['dev'] or root.st_ino!=b['ino']: raise RuntimeError('identity_mismatch')
 for part in parts[:-1]:
  fd=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=fd); fds.append(fd)
 s=os.stat(parts[-1],dir_fd=fd,follow_symlinks=False)
 actual=dict(dev=str(s.st_dev),ino=str(s.st_ino),mode=str(s.st_mode),size=str(s.st_size),mtime_ns=str(s.st_mtime_ns),ctime_ns=str(s.st_ctime_ns))
 if actual!=expected or not(stat.S_ISREG(s.st_mode) or stat.S_ISLNK(s.st_mode)): raise RuntimeError('disposal_path_changed')
 os.unlink(parts[-1],dir_fd=fd); os.fsync(fd)
finally:
 for fd in reversed(fds): os.close(fd)
`;
export async function disposePath(bound:Binding,entry:JournalEntry) {
  if(entry.kind!=="untracked") throw new Error("invalid_disposal_kind");
  return new Promise<void>((resolve,reject)=>NodeChildProcess.execFile("/usr/bin/python3",["-I","-S","-B","-c",unlinkScript,JSON.stringify({binding:bound,path:entry.path,identity:entry.identity})],{timeout:5000,maxBuffer:4096},error=>error ? reject(error) : resolve()));
}
export async function verifyClean(bound:Binding, journal:readonly JournalEntry[]) {
  const status=await git(bound.path,["status","--porcelain=v1","-z","--untracked-files=all","--ignore-submodules=none"]);
  if(status.length) throw new Error("remaining_local_changes");
  const remaining=(await git(bound.path,["ls-files","--others","--ignored","--exclude-standard","--directory","-z"])).toString().split("\0").filter(Boolean);
  const roots=journal.filter(e=>e.kind==="ignored").map(e=>e.path);
  if(remaining.some(p=>!roots.some(root=>inside(NodePath.resolve(bound.path,p),root)))) throw new Error("remaining_ignored_changed");
}
