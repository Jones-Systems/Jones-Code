// @effect-diagnostics nodeBuiltinImport:off -- Producer identities belong to the actual server host, independently of application platform injection.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodeChildProcess from "node:child_process";
import { nativePlatform } from "./Filesystem.ts";

export interface ProducerIdentity {
  host: string;
  boot: string;
  pid: number;
  start: string;
  namespace: string;
}
export type HolderObservation = {
  state: "present" | "absent" | "unknown";
  reason: string;
};

const command = (file: string, args: string[]) =>
  new Promise<string>((resolve, reject) => {
    NodeChildProcess.execFile(
      file,
      args,
      { timeout: 1000, maxBuffer: 4 * 1024 * 1024, env: { PATH: "/usr/bin:/bin", LC_ALL: "C" } },
      (error, stdout) => (error ? reject(error) : resolve(stdout.trim())),
    );
  });

// proc_pidinfo supplies the start timeval rather than ps's second-rounded display.
const DARWIN_START = String.raw`
import ctypes, sys
class Info(ctypes.Structure):
    _fields_ = [(n,ctypes.c_uint32) for n in ('flags','status','xstatus','pid','ppid','uid','gid','ruid','rgid','svuid','svgid','rfu')]+[('comm',ctypes.c_char*16),('name',ctypes.c_char*32)]+[(n,ctypes.c_uint32) for n in ('nfiles','pgid','pjobc','tdev','tpgid','nice')]+[('sec',ctypes.c_uint64),('usec',ctypes.c_uint64)]
lib = ctypes.CDLL('/usr/lib/libproc.dylib',use_errno=True)
info = Info()
size = lib.proc_pidinfo(int(sys.argv[1]),3,0,ctypes.byref(info),ctypes.sizeof(info))
if size != ctypes.sizeof(info) or info.pid != int(sys.argv[1]) or info.status == 5 or not info.sec: sys.exit(1)
print(str(info.sec)+':'+str(info.usec))
`;

function linuxStat(value: string) {
  const end = value.lastIndexOf(")");
  const fields = value.slice(end + 2).trim().split(/\s+/);
  if (end < 0 || fields.length < 20 || !/^\d+$/.test(fields[19]!) || !/^\d+$/.test(fields[3]!))
    throw new Error("process_identity_unavailable");
  if (fields[0] === "Z" || fields[0] === "X") throw new Error("process_identity_unavailable");
  const sid = Number(fields[3]);
  if (!Number.isSafeInteger(sid) || sid < 0) throw new Error("process_identity_unavailable");
  const pid = Number(/^([0-9]+) \(/.exec(value)?.[1]);
  if (!Number.isSafeInteger(pid) || pid <= 0) throw new Error("process_identity_unavailable");
  return { pid, start: fields[19]!, sid };
}

async function forkEpoch() {
  // procfs reports size zero. A fresh open/read is required for each bracket;
  // neither a reused descriptor nor an assumed arithmetic fork count suffices.
  const file = await NodeFSP.open("/proc/stat", "r");
  try {
    const bytes = Buffer.alloc(1024 * 1024);
    const read = await file.read(bytes, 0, bytes.length, null);
    if (read.bytesRead === bytes.length) throw new Error("session_observation_unavailable");
    const count = /^processes ([0-9]+)$/m.exec(bytes.subarray(0, read.bytesRead).toString())?.[1];
    if (!count || BigInt(count) > 0xffffffffffffffffn) throw new Error("session_observation_unavailable");
    return count;
  } finally { await file.close(); }
}

async function linuxSessionSnapshot(identity: ProducerIdentity, sid: number): Promise<HolderObservation> {
  const deadline = performance.now() + 250;
  const first = await forkEpoch();
  let count = 0;
  const entries = await NodeFSP.opendir("/proc");
  for await (const entry of entries) {
    if (++count > 65536 || performance.now() > deadline) throw new Error("session_observation_unavailable");
    if (!/^[0-9]+$/.test(entry.name)) continue;
    const namespace = await NodeFSP.readlink(`/proc/${entry.name}/ns/pid`);
    if (namespace !== identity.namespace) throw new Error("host_or_namespace_mismatch");
    const info = linuxStat(await NodeFSP.readFile(`/proc/${entry.name}/stat`, "utf8"));
    // Zero is only a nonmatch to a positive SID in the verified same namespace.
    if (info.sid === sid && sid > 0) return { state: "present", reason: "holder_present" };
  }
  const second = await forkEpoch();
  if (first !== second || performance.now() > deadline) throw new Error("session_observation_unavailable");
  // Even a quiet snapshot cannot establish the installed kernel's publication
  // ordering, complete visibility/no-hidepid and exact 64-bit build binding.
  // Reviewed source: Linux v6.12.107 f717995c / Debian 6.12.107-1.
  // Those host gates have no qualified entry in this slice. Never expose an
  // assertion flag or injectable gate which promotes this snapshot to absence.
  return { state: "unknown", reason: "linux_kernel_visibility_unqualified" };
}

async function hostIdentity() {
  if (nativePlatform === "linux") {
    const boot = (await NodeFSP.readFile("/proc/sys/kernel/random/boot_id", "utf8")).trim();
    const namespace = await NodeFSP.readlink("/proc/self/ns/pid");
    if (!boot || !/^pid:\[\d+\]$/.test(namespace) ||
      linuxStat(await NodeFSP.readFile("/proc/self/stat", "utf8")).pid !== process.pid) throw new Error("holder_identity_unavailable");
    return { host: NodeOS.hostname(), boot, namespace };
  }
  if (nativePlatform === "darwin") {
    const boot = await command("/usr/sbin/sysctl", ["-n", "kern.boottime"]);
    if (!boot) throw new Error("holder_identity_unavailable");
    return { host: NodeOS.hostname(), boot, namespace: "darwin:global" };
  }
  throw new Error("holder_identity_unavailable");
}

export async function producerIdentity(): Promise<ProducerIdentity> {
  const host = await hostIdentity();
  const start = nativePlatform === "linux"
    ? linuxStat(await NodeFSP.readFile(`/proc/${process.pid}/stat`, "utf8")).start
    : await command("/usr/bin/python3", ["-B", "-c", DARWIN_START, String(process.pid)]);
  return { ...host, pid: process.pid, start };
}

export async function observeProducerSelf(identity: ProducerIdentity): Promise<HolderObservation> {
  try {
    const current = await hostIdentity();
    if (current.host !== identity.host || current.namespace !== identity.namespace)
      return { state: "unknown", reason: "host_or_namespace_mismatch" };
    if (current.boot !== identity.boot) return { state: "absent", reason: "different_boot" };
    let start: string;
    if (nativePlatform === "linux") {
      // ENOENT may also reflect visibility or a racing lookup. It is unknown,
      // while a successful lookup of a different incarnation proves the old self ended.
      const info = linuxStat(await NodeFSP.readFile(`/proc/${identity.pid}/stat`, "utf8"));
      if (info.pid !== identity.pid) throw new Error("process_identity_unavailable");
      start = info.start;
    } else {
      const rows = (await command("/bin/ps", ["-axo", "pid=,stat="])).split("\n");
      if (!rows.length || rows.some((row) => !/^\s*\d+\s+\S+\s*$/.test(row)))
        throw new Error("process_identity_unavailable");
      const row = rows.find((entry) => Number(entry.trim().split(/\s+/)[0]) === identity.pid);
      if (!row) return { state: "absent", reason: "self_absent" };
      if (row.includes("Z")) throw new Error("process_identity_unavailable");
      start = await command("/usr/bin/python3", ["-B", "-c", DARWIN_START, String(identity.pid)]);
    }
    return start === identity.start
      ? { state: "present", reason: "holder_present" }
      : { state: "absent", reason: "self_incarnation_absent" };
  } catch {
    return { state: "unknown", reason: "session_observation_unavailable" };
  }
}

export async function observeProducerSession(identity: ProducerIdentity, sid: number): Promise<HolderObservation> {
  try {
    if (!Number.isSafeInteger(sid) || sid <= 0)
      return { state: "unknown", reason: "session_observation_unavailable" };
    const current = await hostIdentity();
    if (current.host !== identity.host || current.namespace !== identity.namespace)
      return { state: "unknown", reason: "host_or_namespace_mismatch" };
    if (current.boot !== identity.boot) return { state: "absent", reason: "different_boot" };
    if (nativePlatform === "darwin")
      return { state: "unknown", reason: "darwin_unpublished_allocation_unproved" };
    // A positive observation needs no quiet-census premise. Never adopt this PID
    // as a new session: the recorded SID came only from the original detached spawn.
    return await linuxSessionSnapshot(identity, sid);
  } catch {
    return { state: "unknown", reason: "session_observation_unavailable" };
  }
}
