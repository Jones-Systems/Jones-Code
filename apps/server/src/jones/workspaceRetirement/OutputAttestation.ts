// @effect-diagnostics nodeBuiltinImport:off -- C2 native descriptor/publication adapter shared with the dependency-free setup entrypoint.
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import { constants } from "node:fs";
import { randomBytes, createHash } from "node:crypto";
import { canonical, exists, fdPath, git, inside, openAbsolute, openRelative, readBounded, sensitive, sha256, snapshot } from "./Filesystem.ts";
import { darwinFilesystem } from "./DarwinFilesystem.ts";

export const LIMITS = { entries: 500_000, bytes: 16 * 1024 ** 3, seconds: 300 } as const;
export interface Entry { relpath: string; type: string; size: number; mode: number; mtime_ns?: string | number; sha256?: string; target?: string }
export interface Binding { path: string; dev: number; ino: number; git_dir: string; git_dir_dev: number; git_dir_ino: number; git_common_dir: string; admin_id: string; breadcrumb_id: string | null }
export const incarnation = (b: Omit<Binding,"admin_id"> | Binding) => sha256(canonical(Object.fromEntries(
  ["path","dev","ino","git_dir","git_dir_dev","git_dir_ino","git_common_dir","breadcrumb_id"].map(key => [key,b[key as keyof typeof b] ?? null]))));
const pathOrder = (a:Entry,b:Entry) => Buffer.compare(Buffer.from(a.relpath),Buffer.from(b.relpath));
export function manifestBytes(entries: readonly Entry[]) { return Buffer.from([...entries].sort(pathOrder).map(({mtime_ns: _mtime, ...e}) => canonical(e)+"\n").join("")); }
export function listingDigest(entries: readonly Entry[]) { return sha256([...entries].sort(pathOrder).map(e => [e.relpath,e.type,e.size,e.mtime_ns,e.mode,...(e.type === "symlink" ? [e.target] : [])].join("\0")+"\n").join("")); }
const safeNumber = (n: bigint) => { const v = Number(n); if (!Number.isSafeInteger(v)) throw new Error("identity_mismatch"); return v; };
export async function binding(worktree: string): Promise<Binding> {
  const path = await FS.realpath(worktree);
  const git_dir = await FS.realpath((await git(path,["rev-parse","--absolute-git-dir"])).toString().trim());
  const git_common_dir = await FS.realpath((await git(path,["rev-parse","--path-format=absolute","--git-common-dir"])).toString().trim());
  const root = await openAbsolute(path,true), admin = await openAbsolute(git_dir,true);
  try {
    const rs = await root.stat({bigint:true}), gs = await admin.stat({bigint:true});
    let breadcrumb_id: string | null = null;
    const breadcrumb = Path.join(git_dir,"workspace-provenance.json");
    if (await exists(breadcrumb)) { const value = JSON.parse((await readBounded(breadcrumb,65536)).toString()); breadcrumb_id = value.breadcrumb_id ?? value.incarnation_id; if (typeof breadcrumb_id !== "string" || !breadcrumb_id) throw new Error("identity_mismatch"); }
    return {path, dev:safeNumber(rs.dev), ino:safeNumber(rs.ino), git_dir,git_dir_dev:safeNumber(gs.dev),git_dir_ino:safeNumber(gs.ino),git_common_dir,admin_id:Path.basename(git_dir),breadcrumb_id};
  } finally { await root.close(); await admin.close(); }
}
interface Metadata { entries: Entry[]; snapshots: Map<string,string>; reasons: string[]; logical: number; allocated: number; nested: string[]; sensitivePaths: string[] }
async function darwinMetadata(path:string,operation:"metadata"|"content",limits:{entries:number;bytes:number;seconds:number}):Promise<Metadata> {
  const data = await darwinFilesystem({path,operation,limits}) as Omit<Metadata,"snapshots"> & {snapshots:Record<string,string>};
  return {...data,snapshots:new Map(Object.entries(data.snapshots))};
}
export async function metadata(root: FS.FileHandle, deadline: number, limits: { entries: number; bytes: number }): Promise<Metadata> {
  if(process.platform === "darwin") return darwinMetadata(fdPath(root.fd),"metadata",{...limits,seconds:Math.max(0.001,(deadline-performance.now())/1000)});
  const entries: Entry[] = [], snapshots = new Map<string,string>(), reasons = new Set<string>(), nested: string[] = [], sensitivePaths: string[] = [];
  const device = (await root.stat({bigint:true})).dev;
  let logical = 0, allocated = 0;
  const pending = [""];
  while (pending.length) {
    const relative = pending.pop()!;
    const dir = await openRelative(root,relative,true);
    try {
      const iterator = await FS.opendir(fdPath(dir.fd));
      try {
        for await (const child of iterator) {
          if (performance.now() > deadline || entries.length >= limits.entries) throw new Error("bound_exceeded");
          if (child.name.includes("\ufffd")) throw new Error("observation_failed");
          const relpath = relative ? relative+"/"+child.name : child.name;
          const path = `${fdPath(dir.fd)}/${child.name}`;
          const s = await FS.lstat(path,{bigint:true});
          if (s.dev !== device) throw new Error("mount_boundary");
          const type = s.isFile() ? "file" : s.isDirectory() ? "directory" : s.isSymbolicLink() ? "symlink" : "special";
          const e: Entry = {relpath,type,size:safeNumber(s.size),mode:Number(s.mode & 0o7777n),mtime_ns:s.mtimeNs.toString()};
          if (sensitive(child.name)) { reasons.add("sensitive_ignored"); sensitivePaths.push(relpath); }
          if (child.name === ".git") { reasons.add("nested_repository"); nested.push(relpath); }
          if (type === "symlink") e.target = new TextDecoder("utf-8",{fatal:true}).decode(await FS.readlink(path,{encoding:"buffer"}));
          else if (type === "directory" && child.name !== ".git" && !sensitive(child.name)) pending.push(relpath);
          else if (type === "file") logical += e.size;
          else if (type === "special") reasons.add("special_file");
          allocated += safeNumber(s.blocks) * 512;
          entries.push(e); snapshots.set(relpath,snapshot(s));
          if (logical > limits.bytes) throw new Error("bound_exceeded");
        }
      } finally { /* for-await closes the bounded directory stream, including errors. */ }
    } finally { await dir.close(); }
  }
  entries.sort(pathOrder);
  const directories=new Set(entries.filter(e=>e.type==="directory").map(e=>e.relpath));
  for(const entry of entries) if(entry.type==="file" && Path.basename(entry.relpath)==="HEAD") {
    const parent=Path.posix.dirname(entry.relpath), prefix=parent==="." ? "" : parent+"/";
    if(directories.has(prefix+"objects") && directories.has(prefix+"refs")) {reasons.add("nested_repository");nested.push(parent==="." ? "" : parent);}
  }
  return {entries,snapshots,reasons:[...reasons].sort(),logical,allocated,nested,sensitivePaths};
}
export interface Observation { entries: number; logical_bytes: number; allocated_bytes: number; listing_digest: string | null; verification: "content" | "unverifiable"; content_manifest_sha256: string | null; reasons: string[] }
export async function contentManifest(path: string, shortened: Partial<typeof LIMITS> | { entries?: number; bytes?: number; seconds?: number } = {}): Promise<{observation: Observation; payload: Buffer}> {
  const limits = {...LIMITS,...shortened};
  if (limits.entries <= 0 || limits.entries > LIMITS.entries || limits.bytes < 0 || limits.bytes > LIMITS.bytes || limits.seconds <= 0 || limits.seconds > LIMITS.seconds) throw new Error("invalid_bounds");
  const observation: Observation = {entries:0,logical_bytes:0,allocated_bytes:0,listing_digest:null,verification:"unverifiable",content_manifest_sha256:null,reasons:[]};
  let root: FS.FileHandle | undefined;
  try {
    if (path.split(Path.sep).some(sensitive)) throw new Error("sensitive_ignored");
    if (path.split(Path.sep).includes(".git")) throw new Error("nested_repository");
    const deadline = performance.now() + limits.seconds*1000;
    if(process.platform === "darwin") {
      const first=await darwinMetadata(path,"content",limits);
      Object.assign(observation,{entries:first.entries.length,logical_bytes:first.logical,allocated_bytes:first.allocated,listing_digest:listingDigest(first.entries),reasons:first.reasons});
      if(first.reasons.length) return {observation,payload:Buffer.alloc(0)};
      const payload=manifestBytes(first.entries);
      if(payload.length>256*1024*1024 || performance.now()>deadline) throw new Error("bound_exceeded");
      observation.verification="content";observation.content_manifest_sha256=sha256(payload);
      return {observation,payload};
    }
    root = await openAbsolute(path,true);
    const initial = snapshot(await root.stat({bigint:true}));
    const first = await metadata(root,deadline,limits);
    Object.assign(observation,{entries:first.entries.length,logical_bytes:first.logical,allocated_bytes:first.allocated,listing_digest:listingDigest(first.entries),reasons:first.reasons});
    if (first.reasons.length) return {observation,payload:Buffer.alloc(0)};
    for (const entry of first.entries) if (entry.type === "file") {
      const file = await openRelative(root,entry.relpath);
      try {
        if (snapshot(await file.stat({bigint:true})) !== first.snapshots.get(entry.relpath)) throw new Error("attestation_mismatch");
        const digest = createHash("sha256"), block = Buffer.alloc(1024*1024);
        let count = 0;
        while (true) {
          if (performance.now() > deadline) throw new Error("bound_exceeded");
          const read = await file.read(block,0,Math.min(block.length,entry.size-count+1),null);
          if (!read.bytesRead) break;
          count += read.bytesRead;
          if (count > entry.size) throw new Error("attestation_mismatch");
          digest.update(block.subarray(0,read.bytesRead));
        }
        if (count !== entry.size || snapshot(await file.stat({bigint:true})) !== first.snapshots.get(entry.relpath)) throw new Error("attestation_mismatch");
        entry.sha256 = digest.digest("hex");
      } finally { await file.close(); }
    }
    const second = await metadata(root,deadline,limits);
    if (canonical([...second.snapshots].sort()) !== canonical([...first.snapshots].sort()) || second.reasons.length || snapshot(await root.stat({bigint:true})) !== initial || snapshot(await FS.lstat(path,{bigint:true})) !== initial) throw new Error("attestation_mismatch");
    const payload = manifestBytes(first.entries);
    if (payload.length > 256*1024*1024 || performance.now() > deadline) throw new Error("bound_exceeded");
    observation.verification = "content"; observation.content_manifest_sha256 = sha256(payload);
    return {observation,payload};
  } catch(cause) { observation.reasons = [...new Set([...observation.reasons, cause instanceof Error && ["bound_exceeded","sensitive_ignored","nested_repository","mount_boundary","attestation_mismatch","darwin_descriptor_adapter_unavailable"].includes(cause.message) ? cause.message : "observation_failed"])].sort(); return {observation,payload:Buffer.alloc(0)}; }
  finally { await root?.close(); }
}
export interface Attestation { schema: "output-attestation/v1"; producer: string; producer_version: string; id: string; incarnation: Binding & {id:string}; roots: (Observation & {path:string})[]; status: "succeeded"|"failed"|"cancelled"; started_at: string; finished_at: string; manifest: string | null }
async function publish(directory: FS.FileHandle, name: string, payload: Buffer) {
  const temporary = `.${name}.${randomBytes(8).toString("hex")}`;
  const base = fdPath(directory.fd);
  const output = await FS.open(`${base}/${temporary}`,constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|constants.O_NOFOLLOW,0o600);
  try { await output.writeFile(payload); await output.sync(); await FS.link(`${base}/${temporary}`,`${base}/${name}`); }
  finally { await output.close(); await FS.unlink(`${base}/${temporary}`); }
}
export async function attest(input: {worktree:string; root:string; producer:string; status:Attestation["status"]; producerVersion?:string; expected?:Binding; participantGuard: <T>(bound:Binding, action:()=>Promise<T>)=>Promise<T>}): Promise<Attestation & {sidecar:string}> {
  if (!/^[a-z0-9][a-z0-9._-]{0,23}$/.test(input.producer) || !["succeeded","failed","cancelled"].includes(input.status)) throw new Error("invalid_input");
  const bound = await binding(input.worktree);
  if (input.expected && canonical(input.expected) !== canonical(bound)) throw new Error("identity_mismatch");
  const path = Path.resolve(input.root);
  if (!inside(path,bound.path) || path === bound.path || inside(path,bound.git_dir)) throw new Error("outside_worktree");
  return input.participantGuard(bound,async () => {
    const started_at = new Date().toISOString();
    const {observation,payload} = await contentManifest(path);
    if (canonical(await binding(bound.path)) !== canonical(bound)) throw new Error("identity_mismatch");
    const parent = Path.join(bound.git_dir,"workspace-outputs");
    await FS.mkdir(parent,{mode:0o700}).catch(cause=>{if(cause.code !== "EEXIST") throw cause;});
    const dir = await openAbsolute(parent,true);
    try {
      const id = input.producer+"-"+randomBytes(16).toString("hex");
      const manifest = observation.verification === "content" ? id+".manifest.jsonl" : null;
      if (manifest) await publish(dir,manifest,payload);
      const record: Attestation = {schema:"output-attestation/v1",producer:input.producer,producer_version:input.producerVersion ?? "1",id,incarnation:{...bound,id:incarnation(bound)},roots:[{path,...observation}],status:input.status,started_at,finished_at:new Date().toISOString(),manifest};
      await publish(dir,id+".json",Buffer.from(canonical(record)+"\n")); await dir.sync();
      return {...record,sidecar:Path.join(parent,id+".json")};
    } finally {await dir.close();}
  });
}
export async function verify(sidecar:string,worktree:string): Promise<{verification:"content"|"mismatch"|"unverifiable"; reason:string|null; root?:string; content_manifest_sha256?:string}> {
  try {
    const bound = await binding(worktree);
    if (Path.dirname(sidecar) !== Path.join(bound.git_dir,"workspace-outputs") || !/^[a-z0-9][a-z0-9._-]{0,23}-[a-f0-9]{32}\.json$/.test(Path.basename(sidecar))) throw new Error("location");
    const record: Attestation = JSON.parse((await readBounded(sidecar,65536)).toString());
    if(record.schema !== "output-attestation/v1" || canonical(record.incarnation) !== canonical({...bound,id:incarnation(bound)})) return {verification:"unverifiable",reason:"identity_mismatch"};
    if (record.id+".json" !== Path.basename(sidecar) || !["succeeded","failed","cancelled"].includes(record.status) || typeof record.producer_version !== "string" || typeof record.started_at !== "string" || typeof record.finished_at !== "string" || record.roots.length !== 1) throw new Error("record");
    const root = record.roots[0]!;
    if (Path.resolve(root.path) !== root.path || !inside(root.path,bound.path) || root.path === bound.path || inside(root.path,bound.git_dir)) throw new Error("root");
    if(root.verification !== "content" || record.manifest !== record.id+".manifest.jsonl") throw new Error("baseline");
    const current = await contentManifest(root.path);
    if (current.observation.verification !== "content") throw new Error("content");
    const baseline = await readBounded(Path.join(Path.dirname(sidecar),record.manifest),256*1024*1024);
    if (sha256(baseline) !== root.content_manifest_sha256 || !baseline.equals(current.payload)) return {verification:"mismatch",reason:"attestation_mismatch",root:root.path};
    return {verification:"content",reason:null,root:root.path,content_manifest_sha256:root.content_manifest_sha256!};
  } catch {return {verification:"unverifiable",reason:"attestation_unverifiable"};}
}
