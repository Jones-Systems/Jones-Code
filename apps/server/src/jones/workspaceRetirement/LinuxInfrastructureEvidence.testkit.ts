// @effect-diagnostics nodeBuiltinImport:off -- Synthetic Python kernel/bus/filesystem projections; no host process or manager queries.
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import { linuxInfrastructureHelper } from "./LinuxInfrastructureHelper.ts";
import {
  consumerDecision,
  processProbe,
  type ProcessProbeIO,
  type Probe,
} from "./ConsumerProbe.ts";
import { infrastructureBasis } from "./LinuxInfrastructureEvidence.ts";

const pythonFixture = String.raw`
import copy, types
real_proc = proc
real_trust = trust
real_socket = socket.socket
real_stat = os.stat
real_lstat = os.lstat
real_readlink = os.readlink
real_fstat = os.fstat
real_getpid = os.getpid
real_getuid = os.getuid

class Fixture(Observation):
    def __init__(self, fault=None):
        super().__init__(1500)
        self.fault = fault
        self.pids = {3: 41}
        self.reads = {}
        self.calls = []
        self.closed = []
        self.unit_reads = 0
        self.owner_reads = 0
        self.trust_reads = {}
        self.unit = dict(Id='user@501.service', LoadState='loaded', ActiveState='active', SubState='running', InvocationID=[1]*16,
                         FragmentPath='/usr/lib/systemd/system/user@.service', DropInPaths=['/etc/systemd/system/user@.service.d/10-native.conf'], SourcePath='', Transient=False, NeedDaemonReload=False,
                         MainPID=41, ExecMainPID=41, User='501', ControlGroup='/user.slice/user-501.slice/user@501.service')
    def anchor(self, pid):
        fd = 20 + len(self.pids)
        self.pids[fd] = pid
        self.handles.append(fd)
        return fd
    def close(self):
        os.close = lambda fd: self.closed.append(fd)
        super().close()
    def bus(self, args):
        if self.fault == 'timeout': raise Unknown('timeout')
        self.calls.append(args)
        if args[0] == 'call' and args[1] == BUS:
            method, name = args[4], args[6]
            if method == 'GetNameOwner':
                self.owner_reads += 1
                return [dict(type='s', data=[':1.8' if not (self.fault == 'owner_change' and self.owner_reads > 1) else ':1.9'])]
            if method == 'GetConnectionUnixProcessID':
                pid = 8 if name == BUS else 1
                if self.fault == 'broker_mismatch' and name == BUS: pid = 9
                if self.fault == 'manager_wrong_pid' and name != BUS: pid = 2
                return [dict(type='u', data=[pid])]
            if method == 'GetConnectionUnixUser':
                return [dict(type='u', data=[501 if self.fault == 'manager_wrong_uid' and name != BUS else 0])]
            raise AssertionError(args)
        if args[0] == 'call':
            assert args[4] == 'GetUnitByPID'
            return [dict(type='o', data=['/org/freedesktop/systemd1/unit/user_40501_2eservice'])]
        assert args[0] == 'get-property'
        unit = copy.deepcopy(self.unit)
        self.unit_reads += 1
        if self.fault in ('credential', 'pam', 'descendant', 'spoof'):
            unit['MainPID'] = 42
        changes = {
            'transient': ('Transient', True), 'reload': ('NeedDaemonReload', True),
            'generated': ('SourcePath', '/run/generated'), 'wrong_template': ('FragmentPath', '/etc/user@.service'),
            'wrong_uid': ('User', '502'), 'inactive': ('ActiveState', 'inactive'), 'old_main': ('ExecMainPID', 42),
            'zero_invocation': ('InvocationID', [0]*16), 'wrong_id': ('Id', 'credential.service'),
            'cgroup_prefix': ('ControlGroup', '/user.slice/user-501.slice/user@501.servic'),
            'file_bound': ('DropInPaths', ['/etc/systemd/system/'+str(i)+'.conf' for i in range(32)]),
        }
        if self.fault in changes:
            k,v = changes[self.fault]; unit[k] = v
        if self.fault == 'invocation_change' and self.unit_reads > 2:
            unit['InvocationID'] = [2]*16
        types_by_name = dict(zip(['Id','LoadState','ActiveState','SubState','InvocationID','FragmentPath','DropInPaths','SourcePath','Transient','NeedDaemonReload','MainPID','ExecMainPID','User','ControlGroup'], ['s','s','s','s','ay','s','as','s','b','b','u','u','s','s']))
        rows = [dict(type=types_by_name[n], data=unit[n]) for n in args[4:]]
        if self.fault == 'projection_missing': rows.pop()
        return rows
    def read(self, fd, name, cap=65536):
        if name.startswith('../sys/'):
            return '00000000-0000-0000-0000-000000000001\n'
        pid = self.pids[fd]
        key = (pid,name)
        self.reads[key] = self.reads.get(key,0)+1
        n = self.reads[key]
        uid = 501 if pid in (41,77) else 0
        if name == 'status':
            ns = str(pid)
            if self.fault in ('missing_nspid','multiple_nspid','wrong_nspid') and pid == 77:
                ns = {'missing_nspid':'','multiple_nspid':str(pid)+' 1','wrong_nspid':'2'}[self.fault]
            return 'Pid:\t'+str(pid)+'\nNSpid:\t'+ns+'\nUid:\t'+'\t'.join([str(uid)]*4)+'\n'
        if name == 'stat':
            start = 100
            if self.fault == 'candidate_reuse' and pid == 41 and n > 1: start = 101
            if self.fault == 'broker_reuse' and pid == 8 and n > 1: start = 101
            if self.fault == 'manager_reuse' and pid == 1 and n > 1: start = 101
            parent = '2' if self.fault == 'parent_change' and pid == 41 and n > 1 else '1'
            return str(pid)+' (fixed) S '+parent+' '+'0 '*17+str(start)+' 0'
        if name == 'cgroup':
            return '0::/user.slice/user-501.slice/user@501.service/app.slice\n'
        raise AssertionError(name)
    def metadata(self, path):
        self.trust_reads[path] = self.trust_reads.get(path,0)+1
        file = path in ('/usr/bin/python3','/usr/bin/busctl',self.unit['FragmentPath']) or path.endswith('.conf')
        socket_mode = path == SOCKET
        mode = (stat.S_IFSOCK | 0o666) if socket_mode else (stat.S_IFREG | 0o644) if file else (stat.S_IFDIR | 0o755)
        if self.fault == 'writable_override' and path.endswith('.conf'): mode |= 0o020
        if self.fault == 'writable_tool' and path == '/usr/bin': mode |= 0o020
        if self.fault == 'writable_parent' and path == '/etc/systemd': mode |= 0o002
        if self.fault == 'missing_override' and path.endswith('.conf'): raise FileNotFoundError()
        if self.fault == 'missing_tool' and path == '/usr/bin/busctl': raise FileNotFoundError()
        if self.fault == 'symlink_bound' and path == '/usr/bin/busctl': mode = stat.S_IFLNK | 0o777
        inode = len(path)
        if self.fault == 'changed_override' and path.endswith('.conf') and self.trust_reads[path] > 1: inode += 1
        return types.SimpleNamespace(st_uid=0, st_mode=mode, st_dev=1, st_ino=inode, st_ctime_ns=10, st_mtime_ns=10, st_size=0)

class Connection:
    def __init__(self, fixture): self.fixture=fixture; self.closed=False
    def settimeout(self, timeout): assert 0 < timeout <= 1.5
    def setsockopt(self, level, key, value): assert key == socket.SO_PASSCRED and value == 1
    def connect(self, path): assert path == SOCKET
    def sendall(self, payload): assert payload == b'\0AUTH EXTERNAL 353031\r\n'
    def recvmsg(self, size, control):
        fault = self.fixture.fault
        credentials = [] if fault == 'missing_scm' else [(socket.SOL_SOCKET, socket.SCM_CREDENTIALS, struct.pack('3i', 8, 0, 0))]
        flags = socket.MSG_CTRUNC if fault == 'truncated_scm' else socket.MSG_TRUNC if fault == 'truncated_data' else 0
        return b'OK '+b'a'*32+b'\r\n', credentials, flags, None
    def close(self): self.closed=True

faults = [None, 'missing_nspid','multiple_nspid','wrong_nspid','missing_scm','truncated_scm','truncated_data','broker_mismatch','manager_wrong_pid','manager_wrong_uid','owner_change','credential','pam','descendant','spoof','transient','reload','generated','wrong_template','wrong_uid','inactive','old_main','zero_invocation','wrong_id','cgroup_prefix','file_bound','invocation_change','projection_missing','candidate_reuse','broker_reuse','manager_reuse','parent_change','writable_override','writable_parent','missing_override','changed_override','writable_tool','timeout','missing_tool','symlink_bound']
for fault in faults:
    f = Fixture(fault)
    os.getpid = lambda: 77
    os.getuid = lambda: 501
    os.fstat = lambda fd: types.SimpleNamespace(st_dev=1, st_ino=f.pids[fd])
    os.stat = lambda path: types.SimpleNamespace(st_dev=1, st_ino=1)
    os.lstat = f.metadata
    os.readlink = lambda path: path
    read_at = f.read
    socket.socket = lambda *args: Connection(f)
    # Initial snapshot is captured before the normal surfaces, exactly as in the collector.
    expected = digest(proc(3,41))
    f.reads.clear()
    try:
        result = verify(f,41,501,expected)
        assert fault is None, 'accepted '+str(fault)
        assert len(result) == 64
        # Same stable facts at a later observation produce the same basis, including a different observer PID.
        assert verify(f,41,501,expected) == result
    except (Unknown, FileNotFoundError) as error:
        assert fault is not None, repr(error)
    finally:
        f.close()
        assert f.closed == f.handles and all(s.closed for s in f.sockets)
print('synthetic proof scenarios',len(faults))
# Exercise the real subprocess bounding/disposal code with finite injected transport output.
class Selector:
    def __enter__(self): return self
    def __exit__(self,*args): pass
    def register(self,*args): pass
    def select(self,timeout): return [] if transport_fault == 'timeout' else [True]
class Pipe:
    closed = False
    def fileno(self): return 99
    def close(self): self.closed=True
class Child:
    def __init__(self,*args,**kwargs):
        self.stdout=Pipe(); self.killed=False; self.done=False
        assert args[0][0] == '/usr/bin/busctl'
        assert '--address=unix:path=/run/dbus/system_bus_socket' in args[0]
        assert '--auto-start=no' in args[0] and '--allow-interactive-authorization=no' in args[0]
        children.append(self)
    def wait(self,**kwargs): self.done=True; return 0
    def poll(self): return 0 if self.done else None
    def kill(self): self.killed=True
for transport_fault in ('none','bounds','timeout'):
    children=[]
    parts = [b'x'*8192]*9 if transport_fault == 'bounds' else [b'{"type":"u","data":[8]}\n',b'']
    subprocess.Popen=Child
    selectors.DefaultSelector=Selector
    os.read=lambda *args: parts.pop(0)
    observation=Observation(1500)
    try:
        rows=observation.bus(['call',BUS,DBUS_PATH,BUS,'GetConnectionUnixProcessID','s',BUS])
        assert transport_fault == 'none' and rows == [{'type':'u','data':[8]}]
    except Unknown as error:
        assert str(error) == transport_fault
    finally:
        assert children[0].done and children[0].stdout.closed
        assert children[0].killed == (transport_fault != 'none')
print('synthetic transport scenarios 3')
`;
export function policyIO(
  basis = "a".repeat(64),
  overrides: Partial<ProcessProbeIO> = {},
): ProcessProbeIO {
  return {
    list: async () => ["41"],
    uid: () => 501,
    statUid: async () => 501,
    command: async () => {
      throw Error("unexpected command");
    },
    capture: async (pid) => ({
      base: "/proc/" + pid,
      fd: 3,
      snapshot: "fixture",
      close: async () => {},
    }),
    verifyInfrastructure: async () => ({ kind: "verified", basisDigest: basis }),
    read: async () => "",
    readlink: async () => {
      throw Object.assign(Error("private"), { code: "EACCES" });
    },
    ...overrides,
  };
}
export async function policyProbe(basis?: string) {
  return processProbe("/fixture", "linux", policyIO(basis));
}
function decision(
  probe: Probe,
  others: Partial<
    Record<"native" | "registry" | "services" | "workloads" | "obligations", Probe>
  > = {},
) {
  const clear: Probe = { status: "complete", observedAt: probe.observedAt, paths: [], limits: [] };
  return consumerDecision(
    "/fixture",
    {
      native: clear,
      registry: clear,
      services: clear,
      workloads: clear,
      obligations: clear,
      ...others,
      process: probe,
    },
    probe.observedAt,
  );
}
export const infrastructureCases: Record<string, () => Promise<void>> = {
  "fixed helper proves exact main-process metadata and rejects incomplete, spoofed or changed evidence":
    async () => {
      const source =
        linuxInfrastructureHelper.replace("if __name__ == '__main__':\n    main()", "") +
        pythonFixture;
      const stdout = await new Promise<string>((resolve, reject) =>
        NodeChildProcess.execFile(
          "/usr/bin/python3",
          ["-I", "-B", "-c", source],
          { timeout: 10000, maxBuffer: 65536 },
          (error, stdout, stderr) => (error ? reject(Error(stderr)) : resolve(stdout)),
        ),
      );
      NodeAssert.match(stdout, /synthetic proof scenarios 40/);
    },
  "only fresh accounted process gaps can qualify; raw observations, independent holds and paths survive":
    async () => {
      const probe = await policyProbe();
      NodeAssert.equal(probe.status, "partial");
      NodeAssert.equal(probe.processPolicy?.effectiveStatus, "complete");
      NodeAssert.deepEqual(decision(probe).unknown, []);
      NodeAssert.deepEqual(decision({ ...probe }).unknown, ["consumer_evidence_unavailable"]);
      const old = await processProbe(
        "/fixture",
        "linux",
        (() => {
          const io = policyIO();
          delete io.verifyInfrastructure;
          return io;
        })(),
      );
      NodeAssert.deepEqual(decision(old).unknown, ["consumer_evidence_unavailable"]);
      for (const name of ["native", "registry", "services", "workloads", "obligations"] as const) {
        NodeAssert.deepEqual(decision(probe, { [name]: { ...probe, status: "partial" } }).unknown, [
          "consumer_evidence_unavailable",
        ]);
        const held = decision(probe, {
          [name]: { ...probe, status: "complete", paths: ["/fixture/actual"] },
        });
        NodeAssert.ok(held.holds.length > 0);
      }
      const actual = await processProbe(
        "/fixture",
        "linux",
        policyIO(undefined, {
          readlink: async (path) => {
            if (path.endsWith("/cwd")) return "/fixture/actual";
            throw Object.assign(Error(), { code: "EACCES" });
          },
        }),
      );
      NodeAssert.deepEqual(decision(actual).holds, ["active_consumer"]);
      const maps = await processProbe(
        "/fixture",
        "linux",
        policyIO(undefined, {
          read: async (path) => {
            if (path.endsWith("cmdline")) throw Object.assign(Error(), { code: "EACCES" });
            return "100-200 r-xp 0 0 0 /fixture/library";
          },
        }),
      );
      NodeAssert.deepEqual(maps.paths, ["/fixture/library"]);
      NodeAssert.equal(maps.processPolicy?.deniedInstances, 1);
    },
  "unclassified denial, nonpermission error, bounds, deadline and disposal failure retain holds":
    async () => {
      for (const overrides of [
        {
          capture: async () => {
            throw Error("anchor unavailable");
          },
        },
        {
          verifyInfrastructure: async () => ({
            kind: "unclassified" as const,
            reason: "identity" as const,
          }),
        },
        {
          verifyInfrastructure: async () => {
            throw Error("private");
          },
        },
        {
          read: async () => {
            throw Object.assign(Error(), { code: "EIO" });
          },
        },
        {
          capture: async () => ({
            base: "/proc/41",
            fd: 3,
            snapshot: "fixture",
            close: async () => {
              throw Error("close");
            },
          }),
        },
      ] satisfies Partial<ProcessProbeIO>[]) {
        const probe = await processProbe("/fixture", "linux", policyIO(undefined, overrides));
        NodeAssert.deepEqual(decision(probe).unknown, ["consumer_evidence_unavailable"]);
      }
      let closed = 0,
        verified = 0;
      const bound = await processProbe(
        "/fixture",
        "linux",
        policyIO(undefined, {
          list: async () => Array.from({ length: 17 }, (_, i) => String(i + 1)),
          capture: async (pid) => ({
            base: "/proc/" + pid,
            fd: 3,
            snapshot: "fixture",
            close: async () => {
              closed++;
            },
          }),
          verifyInfrastructure: async () => {
            verified++;
            return { kind: "verified", basisDigest: "a".repeat(64) };
          },
        }),
      );
      NodeAssert.equal(closed, 17);
      NodeAssert.equal(verified, 16);
      NodeAssert.equal(bound.processPolicy?.unclassifiedInstances, 1);
      NodeAssert.deepEqual(decision(bound).unknown, ["consumer_evidence_unavailable"]);
      let time = 0;
      const deadline = await processProbe(
        "/fixture",
        "linux",
        policyIO(undefined, {
          now: () => time,
          verifyInfrastructure: async () => {
            time = 5001;
            return { kind: "verified", basisDigest: "a".repeat(64) };
          },
        }),
      );
      NodeAssert.deepEqual(decision(deadline).unknown, ["consumer_evidence_unavailable"]);
      const mixed = await processProbe(
        "/fixture",
        "linux",
        policyIO(undefined, {
          list: async () => ["41", "42"],
          verifyInfrastructure: async (_a, pid) =>
            pid === "41"
              ? { kind: "verified", basisDigest: "a".repeat(64) }
              : { kind: "unclassified", reason: "identity" },
        }),
      );
      NodeAssert.equal(mixed.processPolicy?.verifiedExclusions, 1);
      NodeAssert.deepEqual(decision(mixed).unknown, ["consumer_evidence_unavailable"]);
      NodeAssert.equal(infrastructureBasis([]), infrastructureBasis([]));
      NodeAssert.notEqual(infrastructureBasis([]), infrastructureBasis(["a".repeat(64)]));
    },
  "Darwin never captures Linux anchors or invokes its proof helper": async () => {
    const probe = await processProbe(
      "/fixture",
      "darwin",
      policyIO(undefined, {
        capture: async () => {
          throw Error("Linux call");
        },
        verifyInfrastructure: async () => {
          throw Error("Linux call");
        },
        command: async (_file, args) => (args[0] === "-axo" ? "41 501" : ""),
      }),
    );
    NodeAssert.equal(probe.status, "complete");
    NodeAssert.equal(probe.processPolicy, undefined);
  },
};
