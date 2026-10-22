"""Opt-in physical-host relay with explicit SSH, source and owned-storage bindings."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import math
import errno
import hashlib
import json
import os
from pathlib import Path
import select
import shlex
import signal
import socket
import subprocess
import time
from urllib.parse import urlsplit


WORKER = "apps/server/integration/jones-e2e/physicalHost.worker.ts"
PROTOCOL = "jones-je08h-control/v1"
MAX_FRAME = 2 * 1024 * 1024
OPS = {"pairing", "seed", "snapshot", "stop-server", "closed-snapshot", "close", "cancel"}
SSH_OPTIONS = ["-T", "-o", "BatchMode=yes", "-o", "ForwardAgent=no",
       "-o", "ControlMaster=no", "-o", "ControlPath=none", "-o", "StrictHostKeyChecking=yes",
       "-o", "UpdateHostKeys=no", "-o", "ExitOnForwardFailure=yes",
       "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=2", "-o", "LogLevel=ERROR"]


class Refusal(Exception):
    """Only fixed, nonsecret error codes may reach the operator receipt."""


def require(condition, code):
    if not condition:
        raise Refusal(code)


def keys(value, expected):
    require(isinstance(value, dict) and set(value) == set(expected), "closed-schema-mismatch")


def identifier(value):
    return isinstance(value, str) and 1 <= len(value) <= 80 and all(
        char.isascii() and (char.isalnum() or char == "-") for char in value)


def port(value):
    require(type(value) is int and 1024 <= value <= 65535, "invalid-loopback-port")
    return value


def absolute_path(value):
    require(isinstance(value, str) and Path(value).is_absolute() and str(Path(value)) == value
            and ".." not in Path(value).parts and not any(c in value for c in "\r\n\0")
            and value != "/", "invalid-absolute-path")
    return value


def remote_parent(value, root):
    absolute_path(value)
    absolute_path(root)
    require(value.startswith(root + "/"), "unowned-remote-parent")


def validate_hosts(hosts):
    keys(hosts, ["driver", "worker"])
    for host in hosts.values():
        keys(host, ["alias", "python", "admissionCollector", "taskRoot"])
        alias = host["alias"]
        require(isinstance(alias, str) and 1 <= len(alias) <= 100
                and alias[0].isalnum() and all(c.isascii() and (c.isalnum() or c in "-_.") for c in alias),
                "invalid-ssh-alias")
        for name in ["python", "admissionCollector", "taskRoot"]:
            absolute_path(host[name])
    require(hosts["driver"]["alias"] != hosts["worker"]["alias"], "duplicate-host-alias")


def validate_source(source):
    keys(source, ["repository", "node", "nodeVersion", "identity", "hashes"])
    absolute_path(source["repository"])
    absolute_path(source["node"])
    require(isinstance(source["nodeVersion"], str) and source["nodeVersion"].startswith("24.")
            and all(part.isdigit() for part in source["nodeVersion"].split("."))
            and len(source["nodeVersion"].split(".")) == 3, "node-version")
    require(isinstance(source["identity"], str) and 1 <= len(source["identity"]) <= 200
            and "REPLACE" not in source["identity"], "unbound-source-identity")
    hashes = source["hashes"]
    require(isinstance(hashes, dict) and 5 <= len(hashes) <= 10000, "source-hash-count")
    for name, digest in hashes.items():
        require(isinstance(name, str) and name and not Path(name).is_absolute()
                and str(Path(name)) == name and not any(c in name for c in "\r\n\0")
                and not any(part in {"..", ".git", ".env", ".ssh"} for part in Path(name).parts)
                and isinstance(digest, str) and len(digest) == 64
                and all(c in "0123456789abcdef" for c in digest), "invalid-source-hash")
    for name in [WORKER, WORKER.replace(".worker.ts", ".ts"),
                 "apps/server/integration/jones-e2e/serverFixture.ts",
                 "apps/server/integration/jones-e2e/cross-server-message.integration.test.ts", "pnpm-lock.yaml"]:
        require(name in hashes, "required-source-hash-missing")


def validate_request(value):
    keys(value, ["schema", "relayBindingId", "controlPort", "peerPort", "source",
                 "scratchParent", "evidenceParent", "sessions", "operatorRoot", "ssh", "hosts"])
    require(value["schema"] == "jones-je08h-relay/v1", "relay-schema")
    require(identifier(value["relayBindingId"]), "relay-binding-id")
    require(port(value["controlPort"]) != port(value["peerPort"]), "overlapping-forward-ports")
    absolute_path(value["operatorRoot"])
    absolute_path(value["ssh"])
    validate_hosts(value["hosts"])
    validate_source(value["source"])
    remote_parent(value["scratchParent"], value["hosts"]["worker"]["taskRoot"])
    remote_parent(value["evidenceParent"], value["hosts"]["worker"]["taskRoot"])
    require(value["scratchParent"] != value["evidenceParent"], "scratch-evidence-overlap")
    require(isinstance(value["sessions"], list) and 1 <= len(value["sessions"]) <= 8,
            "session-count")
    seen = set()
    for session in value["sessions"]:
        keys(session, ["runId", "stage", "fault", "expectedEnd"])
        require(identifier(session["runId"]) and session["runId"] not in seen, "duplicate-run-id")
        seen.add(session["runId"])
        require(session["stage"] in {"import-smoke", "lifecycle-smoke", "qualify"}, "stage")
        require(session["fault"] in {"none", "after-listening"}, "fault")
        require(session["expectedEnd"] in {"close", "cancel", "driver-eof", "after-listening"}, "expected-end")
        require((session["fault"] == "after-listening") ==
                (session["expectedEnd"] == "after-listening"), "fault-end-mismatch")
    return value


def process_identity(value, source):
    keys(value, ["pid", "ppid", "pgid", "startTicks", "executable", "nodeVersion", "cwd", "bootIdSha256"])
    require(all(type(value[key]) is int and value[key] > 0 for key in ["pid", "ppid", "pgid"]), "worker-pid")
    require(isinstance(value["startTicks"], str) and value["startTicks"].isdigit(), "worker-start-ticks")
    require(value["executable"] == source["node"] and value["cwd"] == source["repository"]
            and value["nodeVersion"] == source["nodeVersion"], "worker-runtime-identity")
    require(isinstance(value["bootIdSha256"], str) and len(value["bootIdSha256"]) == 64
            and all(c in "0123456789abcdef" for c in value["bootIdSha256"]), "worker-boot-id")
    return value


def owned_path(value, parent, run_id):
    require(isinstance(value, str) and str(Path(value).parent) == parent
            and Path(value).name.startswith("je08h-" + run_id + "-")
            and not any(c in value for c in "\r\n\0"), "unowned-worker-root")


class Frames:
    def __init__(self, stream):
        self.stream, self.pending, self.queue = stream, bytearray(), []

    def consume(self):
        chunk = os.read(self.stream.fileno(), 65536)
        if not chunk:
            require(not self.pending, "partial-control-frame-at-eof")
            return False
        self.pending.extend(chunk)
        require(len(self.pending) <= MAX_FRAME, "control-frame-limit")
        while b"\n" in self.pending:
            line, _, rest = self.pending.partition(b"\n")
            self.pending = bytearray(rest)
            try:
                value = json.loads(line)
            except (ValueError, UnicodeError):
                raise Refusal("invalid-control-json") from None
            self.queue.append((bytes(line) + b"\n", value))
            require(len(self.queue) <= 4, "control-queue-limit")
        return True

    def read(self, deadline):
        while not self.queue:
            timeout = deadline - time.monotonic()
            require(timeout > 0 and select.select([self.stream], [], [], timeout)[0], "control-deadline")
            require(self.consume(), "control-eof-before-receipt")
        return self.queue.pop(0)


def write_worker_frame(stream, raw, deadline):
    # A partial pipe write stays within this one request; failure never resends it.
    remaining = memoryview(raw)
    while remaining:
        timeout = deadline - time.monotonic()
        require(timeout > 0 and select.select([], [stream], [], timeout)[1], "worker-write-deadline")
        try:
            count = os.write(stream.fileno(), remaining)
        except BlockingIOError:
            continue
        require(count > 0, "worker-pipe-closed")
        remaining = remaining[count:]


# Fixed helpers print only bounded identity/readback fields. They never forward worker traffic.
HOLD = '''import os,sys,json,hashlib,urllib.request
s=open('/proc/self/stat').read().rsplit(') ',1)[1].split()
r={'kind':'forward-ready','pid':os.getpid(),'startTicks':s[19]}
if len(sys.argv)>1:
 u='http://127.0.0.1:'+sys.argv[1]+'/.well-known/t3/environment'
 opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
 with opener.open(u,timeout=10) as response:
  data=response.read(1048577)
  assert len(data)<=1048576
  descriptor=json.loads(data)
 assert descriptor['environmentId']==sys.argv[2]
 r['descriptorMatched']=True
print(json.dumps(r),flush=True)
sys.stdin.buffer.read()
'''
READBACK = '''import os,sys,json,hashlib,socket
pid,ticks,owned,mode=sys.argv[1:]
p='/proc/'+pid
exists=os.path.exists(p+'/stat')
same=exists and open(p+'/stat').read().rsplit(') ',1)[1].split()[19]==ticks
r={'sameProcessPresent':same,'ownedAbsent':not os.path.lexists(owned) if owned else None}
if same and mode=='live':
 r.update(executable=os.readlink(p+'/exe'),cwd=os.readlink(p+'/cwd'),
          bootIdSha256=hashlib.sha256(open('/proc/sys/kernel/random/boot_id','rb').read()).hexdigest())
print(json.dumps(r))
'''
PORT_CLOSED = '''import socket,sys,json,errno
s=socket.socket();s.settimeout(3)
try: result=s.connect_ex(('127.0.0.1',int(sys.argv[1])))
finally:s.close()
print(json.dumps({'portClosed':result==errno.ECONNREFUSED}))
'''


def admission_barrier(next_run_id, collect, release, receipt, deadline):
    receipt.update(nextRunId=next_run_id, released=False, laptopAdmission="stage-initial-only")
    started = time.time()
    samples = collect(deadline)
    receipt["hosts"] = samples
    require(set(samples) == {"driver", "worker"}, "admission-hosts-missing")
    for sample in samples.values():
        require(isinstance(sample, dict) and sample.get("schema") == "work-capacity-admission/v1"
                and sample.get("status") == "ok", "admission-unavailable")
        admission = sample.get("admission", {})
        measured = admission.get("measurements", {})
        cpu, ram = measured.get("cpu_used_percent"), measured.get("ram_available_gib")
        require(all(type(n) in {int, float} and math.isfinite(n) for n in (cpu, ram))
                and 0 <= cpu < 85 and ram > 0
                and type(admission.get("serial_test_process_ceiling")) is int
                and admission["serial_test_process_ceiling"] >= 1, "admission-held")
        try:
            stamp = datetime.fromisoformat(sample["collected_at_utc"].replace("Z", "+00:00"))
            require(stamp.utcoffset() == timezone.utc.utcoffset(stamp), "admission-timezone")
            sampled = stamp.timestamp()
        except (KeyError, ValueError, TypeError, AttributeError):
            raise Refusal("admission-timestamp-unavailable") from None
        require(started - 1 <= sampled <= time.time() + 1 and time.time() - sampled <= 120,
                "admission-stale")
    require(time.monotonic() < deadline, "admission-deadline")
    release()
    receipt["released"] = True


class Relay:
    def __init__(self, request):
        self.request, self.children = request, []
        self.record = {"schema": "jones-je08h-relay-receipt/v1",
                       "relayBindingId": request["relayBindingId"], "sessions": [], "children": [],
                       "sourceSha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                       "cleanupErrors": [], "transportComplete": False}

    def spawn(self, host, command, options=(), role="probe"):
        require(host in {"driver", "worker"}, "host-not-allowed")
        child = subprocess.Popen([self.request["ssh"], *SSH_OPTIONS, *options, self.request["hosts"][host]["alias"], command], stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                 start_new_session=True, bufsize=0)
        item = {"role": role, "host": host, "localPid": child.pid, "closed": False}
        self.children.append((child, item))
        self.record["children"].append(item)
        return child, item

    def stop(self, child, item, timeout=12):
        if child.stdin and not child.stdin.closed:
            child.stdin.close()
        try:
            child.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            # Terminate only this captured local SSH object; remote closure still needs readback.
            item["terminationRequested"] = True
            child.terminate()
            try:
                child.wait(timeout=8)
            except subprocess.TimeoutExpired:
                raise Refusal("captured-ssh-close-unknown") from None
        item.update(closed=True, exitCode=child.returncode)
        if child.stdout and not child.stdout.closed:
            child.stdout.close()

    def helper(self, host, code, argv=(), options=(), hold=False, role="probe"):
        command = shlex.join([self.request["hosts"][host]["python"], "-c", code, *map(str, argv)])
        child, item = self.spawn(host, command, options, role)
        try:
            _, value = Frames(child.stdout).read(time.monotonic() + 15)
            require(isinstance(value, dict), "helper-receipt-type")
            if hold:
                keys(value, ["kind", "pid", "startTicks"] + (["descriptorMatched"] if argv else []))
                require(value["kind"] == "forward-ready" and type(value["pid"]) is int
                        and value["pid"] > 0 and str(value["startTicks"]).isdigit(), "forward-receipt")
                item["remote"] = {"pid": value["pid"], "startTicks": value["startTicks"]}
                if argv:
                    require(value["descriptorMatched"] is True, "forward-descriptor-mismatch")
            else:
                self.stop(child, item)
                require(child.returncode == 0, "helper-exit-failed")
            return child, item, value
        except BaseException:
            self.stop(child, item)
            raise

    def readback(self, identity, owned="", live=False):
        _, _, result = self.helper("worker", READBACK,
                                   [identity["pid"], identity["startTicks"], owned, "live" if live else "closed"])
        if live:
            require(result.get("sameProcessPresent") is True
                    and all(result.get(key) == identity[key] for key in ["executable", "cwd", "bootIdSha256"]),
                    "worker-live-readback-mismatch")
        else:
            require(result.get("sameProcessPresent") is False
                    and (not owned or result.get("ownedAbsent") is True), "worker-cleanup-readback-failed")
        return result

    def close_forward(self, child, item):
        self.stop(child, item)
        require(child.returncode == 0, "forward-exit-failed")
        identity = item.get("remote")
        require(identity is not None, "forward-remote-identity-unavailable")
        _, _, result = self.helper(item["host"], READBACK,
                                   [identity["pid"], identity["startTicks"], "", "closed"])
        require(result.get("sameProcessPresent") is False, "forward-helper-still-present")
        item["remoteProcessAbsent"] = True

    def peer_forwards(self, ready, run_id):
        value = ready["value"]
        keys(value, ["process", "source", "owned", "home", "evidence", "dbPath", "dbStat",
                     "environmentId", "url", "authorityStateDir"])
        require(value["source"] == self.request["source"], "ready-source-mismatch")
        owned_path(value["owned"], self.request["scratchParent"], run_id)
        owned_path(value["evidence"], self.request["evidenceParent"], run_id)
        endpoint = urlsplit(value["url"])
        require(endpoint.scheme == "http" and endpoint.hostname == "127.0.0.1"
                and not endpoint.username and not endpoint.password and not endpoint.query
                and not endpoint.fragment and endpoint.path in {"", "/"}, "worker-url-not-loopback")
        remote_port = port(endpoint.port)
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            local_port = reservation.getsockname()[1]
        self.helper("worker", HOLD, options=["-L", f"127.0.0.1:{local_port}:127.0.0.1:{remote_port}"],
                    hold=True, role="peer-local-forward")
        self.helper("driver", HOLD, [self.request["peerPort"], value["environmentId"]],
                    options=["-R", f"127.0.0.1:{self.request['peerPort']}:127.0.0.1:{local_port}"],
                    hold=True, role="peer-reverse-forward")
        return local_port

    def collect_transition_admission(self, deadline):
        hosts = self.request["hosts"]
        def collect(host):
            command = shlex.join([hosts[host]["python"], hosts[host]["admissionCollector"], "--format", "admission", "--sample-seconds", "1"])
            child, item = self.spawn(host, command, role="transition-admission")
            try:
                _, sample = Frames(child.stdout).read(min(deadline, time.monotonic() + 15))
                self.stop(child, item)
                require(child.returncode == 0, "admission-collector-exit")
                return sample
            finally:
                if not item["closed"]:
                    self.stop(child, item)
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = {host: pool.submit(collect, host) for host in hosts}
            return {host: future.result() for host, future in futures.items()}

    def session(self, connection, binding, next_binding=None):
        record = {"runId": binding["runId"], "stage": binding["stage"], "fault": binding["fault"],
                  "expectedEnd": binding["expectedEnd"], "requestCount": 0, "responseCount": 0}
        self.record["sessions"].append(record)
        start_index = len(self.children)
        worker = worker_item = boot_identity = final = final_raw = None
        worker_frames = None
        driver_live, local_peer_port = True, None
        deadline = time.monotonic() + 180
        pending, next_id, phase_deadline = None, 0, time.monotonic() + 15
        error = None
        try:
            source = self.request["source"]
            command = "cd " + shlex.quote(source["repository"]) + " && exec " + shlex.join([source["node"], WORKER])
            worker, worker_item = self.spawn("worker", command, role="worker-control")
            os.set_blocking(worker.stdin.fileno(), False)
            worker_frames = Frames(worker.stdout)
            raw, boot = worker_frames.read(phase_deadline)
            keys(boot, ["schema", "kind", "process"])
            require(boot["schema"] == PROTOCOL and boot["kind"] == "boot", "worker-boot-schema")
            boot_identity = process_identity(boot["process"], self.request["source"])
            record["worker"] = boot_identity
            record["liveReadback"] = self.readback(boot_identity, live=True)
            connection.sendall(raw)
            driver_frames = Frames(connection)
            while final is None:
                due = min(deadline, phase_deadline)
                streams = [worker.stdout] + ([connection] if driver_live else [])
                available = [stream for stream, frames in [(worker.stdout, worker_frames),
                             (connection, driver_frames)] if frames.queue and stream in streams]
                if not available:
                    remaining = due - time.monotonic()
                    require(remaining > 0, "session-deadline")
                    available = select.select(streams, [], [], remaining)[0]
                    require(available, "session-deadline")
                for stream in available:
                    frames = driver_frames if stream is connection else worker_frames
                    if not frames.queue and not frames.consume():
                        if stream is connection:
                            driver_live = False
                            worker.stdin.close()
                            phase_deadline = time.monotonic() + 25
                            record["driverEof"] = True
                            continue
                        raise Refusal("worker-eof-without-final")
                    while frames.queue:
                        raw, frame = frames.queue.pop(0)
                        require(isinstance(frame, dict) and frame.get("schema") == PROTOCOL, "control-schema")
                        require(frame.get("runId") == binding["runId"], "control-run-id")
                        if stream is connection:
                            require(pending is None and type(frame.get("id")) is int
                                    and frame["id"] == next_id, "control-request-order")
                            if next_id == 0:
                                keys(frame, ["schema", "runId", "id", "op", "source", "scratchParent",
                                             "evidenceParent", "stage", "fault"])
                                require(frame["op"] == "init" and all(
                                    frame[key] == self.request[key] for key in ["source", "scratchParent", "evidenceParent"])
                                    and all(frame[key] == binding[key] for key in ["stage", "fault"]), "init-binding-mismatch")
                            else:
                                keys(frame, ["schema", "runId", "id", "op"])
                                require(frame["op"] in OPS, "unknown-worker-operation")
                            pending = next_id
                            next_id += 1
                            phase_deadline = min(deadline, time.monotonic() +
                                                 (60 if pending == 0 else 25 if frame["op"] in {"close", "cancel"} else 15))
                            write_worker_frame(worker.stdin, raw, phase_deadline)
                            record["requestCount"] += 1
                        elif frame.get("kind") == "closed":
                            keys(frame, ["schema", "runId", "kind", "clean", "owned", "evidence",
                                         "cancelled", "failed", "process"])
                            require(frame["process"] == boot_identity, "final-worker-identity-mismatch")
                            final, final_raw = frame, raw
                            break
                        else:
                            keys(frame, ["schema", "runId", "id", "ok", "value"])
                            require(pending is not None and type(frame["id"]) is int
                                    and frame["id"] == pending and frame["ok"] is True,
                                    "worker-response-order-or-error")
                            if pending == 0:
                                if binding["stage"] == "import-smoke":
                                    keys(frame["value"], ["imported", "evidence"])
                                    require(frame["value"]["imported"] is True, "import-not-ready")
                                else:
                                    require(frame["value"].get("process") == boot_identity, "ready-process-mismatch")
                                    local_peer_port = self.peer_forwards(frame, binding["runId"])
                                    record["forwardDescriptorMatched"] = True
                                require(time.monotonic() < min(deadline, phase_deadline), "ready-forward-deadline")
                            pending = None
                            phase_deadline = min(deadline, time.monotonic() + 15)
                            record["responseCount"] += 1
                            if driver_live:
                                connection.sendall(raw)
        except BaseException as exc:
            error = exc
            record["error"] = str(exc) if isinstance(exc, Refusal) else type(exc).__name__
        finally:
            # EOF asks the remote worker's own finalizer to close its server and synthetic root.
            if worker is not None:
                if not worker.stdin.closed:
                    worker.stdin.close()
                if final is None and worker_frames is not None:
                    try:
                        drain_deadline = time.monotonic() + 25
                        while final is None:
                            _, frame = worker_frames.read(drain_deadline)
                            if isinstance(frame, dict) and frame.get("kind") == "closed":
                                require(frame.get("schema") == PROTOCOL and frame.get("runId") == binding["runId"]
                                        and frame.get("process") == boot_identity, "drain-receipt-mismatch")
                                final = frame
                    except BaseException:
                        record["finalReceiptUnavailable"] = True
                try:
                    self.stop(worker, worker_item, timeout=25)
                    require(final is not None and boot_identity is not None, "remote-cleanup-unknown")
                    owned = final.get("owned")
                    if owned is not None:
                        owned_path(owned, self.request["scratchParent"], binding["runId"])
                    require(final.get("clean") is True, "worker-final-cleanup-failed")
                    record["cleanupReadback"] = self.readback(boot_identity, owned or "")
                    record["final"] = {key: final[key] for key in ["clean", "cancelled", "failed"]}
                    expected = binding["expectedEnd"]
                    require(bool(final["failed"]) == (expected == "after-listening")
                            and bool(final["cancelled"]) == (expected in {"cancel", "driver-eof"})
                            and worker.returncode == (1 if expected == "after-listening" else 0), "worker-end-mismatch")
                except BaseException as exc:
                    record["cleanupError"] = str(exc) if isinstance(exc, Refusal) else type(exc).__name__
                    error = error or Refusal("session-cleanup-failed")
            for child, item in reversed(self.children[start_index:]):
                if item["closed"]:
                    continue
                try:
                    self.close_forward(child, item)
                except BaseException:
                    item["cleanupUnknown"] = True
                    error = error or Refusal("owned-forward-cleanup-failed")
            if local_peer_port is not None:
                with socket.socket() as probe:
                    probe.settimeout(3)
                    record["localPeerClosed"] = probe.connect_ex(("127.0.0.1", local_peer_port)) == errno.ECONNREFUSED
                try:
                    _, _, result = self.helper("driver", PORT_CLOSED, [self.request["peerPort"]])
                    require(record["localPeerClosed"] and result.get("portClosed") is True, "forward-port-still-open")
                    record["vpsPeerClosed"] = True
                except BaseException:
                    error = error or Refusal("forward-port-readback-failed")
            # SSH can translate a TCP reset into EOF. Withhold the unchanged closed frame
            # until cleanup is proved so EOF alone cannot certify a failed cleanup.
            try:
                if error:
                    record["eofBarrier"] = "closed-receipt-withheld"
                else:
                    def release():
                        if driver_live and final_raw is not None:
                            connection.sendall(final_raw)
                    if binding["stage"] == "qualify" and next_binding is not None:
                        record["nextRunAdmission"] = {}
                        try:
                            admission_barrier(next_binding["runId"], self.collect_transition_admission,
                                              release, record["nextRunAdmission"], min(deadline, phase_deadline))
                        except BaseException as exc:
                            record["eofBarrier"] = "closed-receipt-withheld"
                            record["nextRunAdmission"]["error"] = str(exc) if isinstance(exc, Refusal) else type(exc).__name__
                            raise
                    else:
                        release()
                    record["eofBarrier"] = "released"
            finally:
                connection.close()
        if error:
            raise error

    def run(self):
        listener = socket.socket()
        try:
            listener.bind(("127.0.0.1", 0))
            listener.listen(1)
            listener.settimeout(120)
            local_port = listener.getsockname()[1]
            self.helper("driver", HOLD,
                        options=["-R", f"127.0.0.1:{self.request['controlPort']}:127.0.0.1:{local_port}"],
                        hold=True, role="control-reverse-forward")
            print(json.dumps({"event": "relay-ready", "transport": {"kind": "relay",
                              "controlPort": self.request["controlPort"], "peerPort": self.request["peerPort"],
                              "relayBindingId": self.request["relayBindingId"]}}), flush=True)
            for index, binding in enumerate(self.request["sessions"]):
                connection, address = listener.accept()
                require(address[0] == "127.0.0.1", "nonloopback-driver")
                connection.settimeout(15)
                next_binding = self.request["sessions"][index + 1] if index + 1 < len(self.request["sessions"]) else None
                self.session(connection, binding, next_binding)
            self.record["transportComplete"] = True
        finally:
            listener.close()
            for child, item in reversed(self.children):
                if not item["closed"]:
                    try:
                        if item["role"].endswith("forward"):
                            self.close_forward(child, item)
                        else:
                            self.stop(child, item)
                    except BaseException:
                        self.record["cleanupErrors"].append("captured-ssh-close-unknown")
            try:
                _, _, result = self.helper("driver", PORT_CLOSED, [self.request["controlPort"]])
                require(result.get("portClosed") is True, "control-forward-still-open")
                self.record["controlForwardClosed"] = True
            except BaseException:
                self.record["cleanupErrors"].append("control-forward-readback-unavailable")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", required=True)
    args = parser.parse_args()
    request_path = Path(args.request)
    require(not request_path.is_symlink(), "symlinked-request")
    raw = request_path.read_bytes()
    require(len(raw) <= MAX_FRAME, "request-size")
    request = validate_request(json.loads(raw))
    root = Path(request["operatorRoot"])
    require(root.resolve() == root and request_path.parent.resolve() == root, "request-outside-owner-root")
    receipts = root / "receipts"
    receipts.mkdir(mode=0o700, exist_ok=True)
    require(not receipts.is_symlink(), "redirected-receipts-directory")
    destination = receipts / (request["relayBindingId"] + ".json")
    relay = Relay(request)
    relay.record["requestSha256"] = hashlib.sha256(raw).hexdigest()
    with destination.open("x", encoding="utf8") as receipt:
        os.chmod(destination, 0o600)
        def cancelled(_number, _frame):
            # Ignore further signals while exact captured-child finalizers run.
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            raise Refusal("operator-cancelled")
        signal.signal(signal.SIGINT, cancelled)
        signal.signal(signal.SIGTERM, cancelled)
        try:
            relay.run()
        except BaseException as error:
            relay.record["error"] = str(error) if isinstance(error, Refusal) else type(error).__name__
        finally:
            relay.record["operatorCleanupComplete"] = all(
                item["closed"] and not item.get("cleanupUnknown", False)
                and (not item["role"].endswith("forward") or item.get("remoteProcessAbsent") is True)
                for item in relay.record["children"]
            ) and all(session.get("cleanupReadback") and not session.get("cleanupError")
                      for session in relay.record["sessions"]) \
                and not relay.record["cleanupErrors"] and relay.record.get("controlForwardClosed", False)
            json.dump(relay.record, receipt, indent=2)
            receipt.write("\n")
            receipt.flush()
            os.fsync(receipt.fileno())
    print(json.dumps({"event": "relay-finished", "receipt": str(destination),
                      "sessions": len(relay.record["sessions"]),
                      "operatorCleanupComplete": relay.record["operatorCleanupComplete"]}), flush=True)
    return 0 if relay.record["transportComplete"] and relay.record["operatorCleanupComplete"] \
        and "error" not in relay.record else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (Refusal, ValueError, OSError) as error:
        print(json.dumps({"event": "relay-refused", "error": str(error) if isinstance(error, Refusal)
                          else type(error).__name__}), flush=True)
        raise SystemExit(2)
