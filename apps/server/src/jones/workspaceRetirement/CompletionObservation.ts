// @effect-diagnostics nodeBuiltinImport:off -- A killable, bounded metadata sink; never traverses output roots or invokes a content-proof primitive.
import * as NodeChildProcess from "node:child_process";
import * as NodePath from "node:path";
import { RESERVATION_DDL } from "./ParticipantGuard.ts";
import type { Binding } from "./OutputAttestation.ts";

const CAPTURE_LIMITS = { bytes: 64 * 1024, milliseconds: 100 } as const;
export interface CompletionObservation {
  schema: "workspace-output-observation/v1";
  producer: string;
  owner: string;
  worktree: string;
  status: "succeeded" | "failed" | "cancelled";
  proofStatus: "deferred";
  roots: readonly string[];
  lineage: Readonly<Record<string, string>>;
  omittedEnrichment: readonly string[];
}
export interface CaptureResult {
  status: "recorded" | "unavailable";
  reason: string | null;
  reference?: string;
}

// This child owns the entire attempt, including SQLite and file descriptors.
// spawnSync's deadline kills and reaps it: there is no timed-out Promise that
// can continue writing, no queue, and no retry. A lost publication acknowledgement
// is unavailable evidence; the operation is never replayed to obtain metadata.
const PROGRAM = String.raw`
import * as fs from 'node:fs';
import * as path from 'node:path';
import {randomBytes} from 'node:crypto';
import {DatabaseSync} from 'node:sqlite';
let db;
function read(file) {
  const fd=fs.openSync(file,fs.constants.O_RDONLY|fs.constants.O_NOFOLLOW|fs.constants.O_NONBLOCK);
  try {
    const before=fs.fstatSync(fd);if(!before.isFile() || before.size>65536) throw Error('metadata_bound');
    const bytes=Buffer.alloc(65537);let count=0;
    while(count<bytes.length) {const length=fs.readSync(fd,bytes,count,bytes.length-count,null);if(!length) break;count+=length;}
    const after=fs.fstatSync(fd);
    if(count>65536 || count!==before.size || before.ino!==after.ino || before.mtimeMs!==after.mtimeMs || before.ctimeMs!==after.ctimeMs) throw Error('metadata_bound');
    return bytes.subarray(0,count).toString('utf8');
  }
  finally {fs.closeSync(fd);}
}
function directory(file) {const s=fs.lstatSync(file);if(!s.isDirectory() || s.isSymbolicLink()) throw Error('unsafe_metadata_directory');}
try {
  const observation=JSON.parse(fs.readFileSync(0,'utf8'));
  const root=fs.realpathSync(observation.worktree), dot=path.join(root,'.git'), info=fs.lstatSync(dot);
  let admin;
  if(info.isDirectory()) admin=dot;
  else {const match=read(dot).match(/^gitdir: ([^\r\n]+)\r?\n?$/);if(!match) throw Error('git_admin_unavailable');admin=path.resolve(root,match[1]);}
  directory(admin);
  const commonFile=path.join(admin,'commondir');
  const common=fs.existsSync(commonFile) ? path.resolve(admin,read(commonFile).trim()) : admin;
  directory(common);
  const head=read(path.join(admin,'HEAD')).trim();
  if(!/^ref: refs\/[a-zA-Z0-9._/-]+$/.test(head) && !/^[a-f0-9]{40,64}$/.test(head)) throw Error('git_admin_unavailable');
  if(admin!==dot && path.resolve(admin,read(path.join(admin,'gitdir')).trim())!==dot) throw Error('git_admin_unavailable');
  const guard=path.join(common,'workspace-lifecycle');
  fs.mkdirSync(guard,{recursive:true,mode:0o700});directory(guard);
  const store=path.join(guard,'reservations.sqlite3');
  if(fs.existsSync(store) && !fs.lstatSync(store).isFile()) throw Error('reservation_unverifiable');
  db=new DatabaseSync(store);db.exec('PRAGMA busy_timeout=0; BEGIN IMMEDIATE');
  const ddl=${JSON.stringify(RESERVATION_DDL)};
  const version=db.prepare('PRAGMA user_version').get().user_version;
  if(version===0 && !db.prepare("SELECT 1 FROM sqlite_master").get()) {db.exec(ddl);db.exec('PRAGMA user_version=1');}
  else if(version!==1 || db.prepare("SELECT sql FROM sqlite_master WHERE name='reservations'").get()?.sql!==ddl) throw Error('reservation_unverifiable');
  if(db.prepare('SELECT 1 FROM reservations WHERE admin_id=?').get(path.basename(admin))) throw Error('retiring');
  const extra={gitDirectory:admin,gitCommonDirectory:common,adminId:path.basename(admin)};
  const rootInfo=fs.statSync(root);extra.rootDevice=String(rootInfo.dev);extra.rootInode=String(rootInfo.ino);
  if(head.startsWith('ref: ')) {
    extra.branch=head.slice(5);
    if(/^refs\/[a-zA-Z0-9._/-]+$/.test(extra.branch) && !extra.branch.split('/').includes('..')) {
      try {const tip=read(path.join(common,extra.branch)).trim();if(/^[a-f0-9]{40,64}$/.test(tip)) extra.head=tip;} catch {}
    }
  }
  else if(/^[a-f0-9]{40,64}$/.test(head)) extra.head=head;
  try {const birth=JSON.parse(read(path.join(admin,'workspace-provenance.json')));if(typeof birth.incarnation_id==='string') extra.incarnationId=birth.incarnation_id;} catch {}
  const record={...observation,worktree:root,lineage:{...extra,...observation.lineage}};
  const bytes=JSON.stringify(record)+'\n';if(Buffer.byteLength(bytes)>65536) throw Error('metadata_bound');
  const output=path.join(admin,'workspace-completions');fs.mkdirSync(output,{mode:0o700,recursive:true});directory(output);
  const name=randomBytes(16).toString('hex')+'.json', destination=path.join(output,name);
  // Create-only publication needs no disposable named staging file. An interrupted
  // partial observation is never a proof or obligation and is ignored on read.
  const fd=fs.openSync(destination,fs.constants.O_WRONLY|fs.constants.O_CREAT|fs.constants.O_EXCL|fs.constants.O_NOFOLLOW,0o600);
  try {fs.writeFileSync(fd,bytes);} finally {fs.closeSync(fd);}
  db.exec('COMMIT');process.stdout.write(JSON.stringify({reference:destination}));
} catch {process.exitCode=1;}
finally {if(db) db.close();}
`;

export function completionObservation(
  input: Omit<CompletionObservation, "schema" | "proofStatus" | "omittedEnrichment"> & {
    omittedEnrichment?: readonly string[];
  },
): CompletionObservation {
  return {
    schema: "workspace-output-observation/v1",
    ...input,
    proofStatus: "deferred",
    omittedEnrichment: input.omittedEnrichment ?? [],
  };
}
export function recordCompletion(
  observation: CompletionObservation,
  workerProgram = PROGRAM,
): CaptureResult {
  const bytes = JSON.stringify(observation);
  if (Buffer.byteLength(bytes) > CAPTURE_LIMITS.bytes)
    return { status: "unavailable", reason: "metadata_bound" };
  if (!NodePath.isAbsolute(observation.worktree))
    return { status: "unavailable", reason: "git_admin_unavailable" };
  const result = NodeChildProcess.spawnSync(
    process.execPath,
    ["--input-type=module", "-e", workerProgram],
    {
      input: bytes,
      encoding: "utf8",
      timeout: CAPTURE_LIMITS.milliseconds,
      killSignal: "SIGKILL",
      maxBuffer: CAPTURE_LIMITS.bytes,
      env: { PATH: process.env.PATH, NODE_NO_WARNINGS: "1" },
    },
  );
  if (result.status !== 0 || result.error)
    return {
      status: "unavailable",
      reason:
        result.error && "code" in result.error && result.error.code === "ETIMEDOUT"
          ? "metadata_deadline"
          : "metadata_sink_unavailable",
    };
  try {
    const value = JSON.parse(result.stdout);
    return typeof value.reference === "string"
      ? { status: "recorded", reason: null, reference: value.reference }
      : { status: "unavailable", reason: "metadata_sink_unavailable" };
  } catch {
    return { status: "unavailable", reason: "metadata_sink_unavailable" };
  }
}

export function availableLineage(env: NodeJS.ProcessEnv): Record<string, string> {
  const result: Record<string, string> = {};
  for (const [field, key] of Object.entries({
    threadId: "T3CODE_THREAD_ID",
    taskId: "T3CODE_TASK_ID",
    runId: "T3CODE_RUN_ID",
    environmentId: "T3CODE_ENVIRONMENT_ID",
    projectId: "T3CODE_PROJECT_ID",
    incarnationId: "T3CODE_WORKSPACE_INCARNATION_ID",
    branch: "T3CODE_BRANCH",
    head: "T3CODE_HEAD",
    baseRef: "T3CODE_BASE_REF",
    baseHead: "T3CODE_BASE_HEAD",
    predecessor: "T3CODE_PREDECESSOR",
    pr: "T3CODE_PULL_REQUEST_URL",
    nativeEventId: "T3CODE_NATIVE_EVENT_ID",
  })) {
    const value = env[key];
    if (value) result[field] = value;
  }
  return result;
}

// Optional presentation enrichment only. No missing/malformed observation becomes
// an allocation, claim, proof, or work gate. Reads are target-local and bounded.
export async function completionLineage(
  admin: string,
  bound?: Binding,
): Promise<{ status: "available" | "partial" | "unavailable"; lineage: Record<string, string>[] }> {
  const { readdir } = await import("node:fs/promises");
  const { readBounded } = await import("./Filesystem.ts");
  try {
    const directory = NodePath.join(admin, "workspace-completions");
    const names = await readdir(directory);
    let budget = CAPTURE_LIMITS.bytes,
      partial = names.length > 20;
    const lineage: Record<string, string>[] = [];
    for (const name of names.slice(0, 20)) {
      if (!/^[a-f0-9]{32}\.json$/.test(name)) {
        partial = true;
        continue;
      }
      try {
        const bytes = await readBounded(NodePath.join(directory, name), budget);
        budget -= bytes.length;
        const value = JSON.parse(bytes.toString());
        if (
          value.schema !== "workspace-output-observation/v1" ||
          value.proofStatus !== "deferred" ||
          typeof value.lineage !== "object" ||
          value.lineage === null
        ) {
          partial = true;
          continue;
        }
        const fields = Object.fromEntries(
          Object.entries(value.lineage).filter(
            (entry): entry is [string, string] => typeof entry[1] === "string",
          ),
        );
        if (
          fields.gitDirectory !== admin ||
          (bound &&
            (fields.rootDevice !== String(bound.dev) || fields.rootInode !== String(bound.ino)))
        ) {
          partial = true;
          continue;
        }
        lineage.push({
          ...fields,
          producer: typeof value.producer === "string" ? value.producer : "unknown",
        });
      } catch {
        partial = true;
      }
      if (budget <= 0) {
        partial = true;
        break;
      }
    }
    return { status: partial ? "partial" : "available", lineage };
  } catch {
    return { status: "unavailable", lineage: [] };
  }
}
