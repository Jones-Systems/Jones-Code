"""One explicitly admitted stage. Never retries or advances to the next stage."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import select
import shlex
import signal
import time
import subprocess
import sys

import importlib.util
from datetime import datetime, timezone

RELAY = Path(__file__).with_name("physicalHost.relay.py")
sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location("physical_host_relay", RELAY)
relay_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(relay_module)
require, keys = relay_module.require, relay_module.keys


def validate_operator_request(request):
    keys(request, ["schema", "operatorRoot", "ssh", "hosts", "runId", "stage", "local", "remote",
                   "controlPort", "peerPort", "transportSources"])
    require(request["schema"] == "jones-je08h-operator/v1", "operator-schema")
    for name in ["operatorRoot", "ssh"]:
        relay_module.absolute_path(request[name])
    relay_module.validate_hosts(request["hosts"])
    for name in ["local", "remote"]:
        relay_module.validate_source(request[name])
    require(relay_module.identifier(request["runId"]) and len(request["runId"]) <= 64, "run-id")
    require(request["stage"] in {"import-smoke", "lifecycle-smoke", "qualify"}, "stage")
    require(relay_module.port(request["controlPort"]) != relay_module.port(request["peerPort"]), "overlapping-ports")
    keys(request["transportSources"], ["relaySha256", "operatorSha256"])
    for digest in request["transportSources"].values():
        require(isinstance(digest, str) and len(digest) == 64 and all(c in "0123456789abcdef" for c in digest),
                "transport-source-hash")
    return request


def requests_for(request):
    run_id, stage = request["runId"], request["stage"]
    transport = {"kind": "relay", "controlPort": request["controlPort"], "peerPort": request["peerPort"],
                 "relayBindingId": run_id + "-relay"}
    parents = {name: host["taskRoot"] + "/" + run_id for name, host in request["hosts"].items()}
    remote = {"source": request["remote"], "scratchParent": parents["worker"] + "/scratch",
              "evidenceParent": parents["worker"] + "/evidence"}
    driver = {"schema": "jones-je08h-request/v1", "runId": run_id, "stage": stage,
              "runs": 4 if stage == "qualify" else 1, "local": request["local"], "remote": remote,
              "scratchParent": parents["driver"] + "/scratch", "evidenceParent": parents["driver"] + "/evidence",
              "transport": transport}
    sessions = [{"runId": run_id + "-" + str(i + 1), "stage": stage, "fault": "none",
                 "expectedEnd": "cancel" if stage == "lifecycle-smoke" else "close"}
                for i in range(driver["runs"])]
    if stage == "lifecycle-smoke":
        sessions.append({"runId": run_id + "-failure", "stage": stage,
                         "fault": "after-listening", "expectedEnd": "after-listening"})
    relay = {"schema": "jones-je08h-relay/v1", **{k: v for k, v in transport.items() if k != "kind"},
             **remote, "sessions": sessions,
             **{key: request[key] for key in ["operatorRoot", "ssh", "hosts"]}}
    relay_module.validate_request(relay)
    return driver, relay


def validate_admission(admitted, run_id):
    keys(admitted, ["schema", "runId", "bothHostSlotsGranted", "collectedAtUtc", "cpuPercent",
                    "localBootIdSha256", "remoteBootIdSha256", "serialTestProcessCeiling",
                    "remoteRamAvailableGiB", "operatorMemoryDecision"])
    require(admitted["schema"] == "jones-je08h-admission/v1" and admitted["runId"] == run_id
            and admitted["bothHostSlotsGranted"] is True, "admission-binding")
    keys(admitted["cpuPercent"], ["operator", "driver", "worker"])
    keys(admitted["serialTestProcessCeiling"], ["operator", "driver", "worker"])
    keys(admitted["remoteRamAvailableGiB"], ["driver", "worker"])
    require(all(type(n) is int and n >= 1 for n in admitted["serialTestProcessCeiling"].values())
            and all(type(n) in {int, float} and 0 < n < float("inf") for n in admitted["remoteRamAvailableGiB"].values())
            and admitted["operatorMemoryDecision"] == "continue", "admission-memory-or-serial-hold")
    require(all(type(n) in {int, float} and 0 <= n < 85 for n in admitted["cpuPercent"].values()), "admission-cpu")
    for name in ["localBootIdSha256", "remoteBootIdSha256"]:
        digest = admitted[name]
        require(isinstance(digest, str) and len(digest) == 64 and all(c in "0123456789abcdef" for c in digest), "boot-hash")
    require(admitted["localBootIdSha256"] != admitted["remoteBootIdSha256"], "same-host")
    stamp = datetime.fromisoformat(admitted["collectedAtUtc"].replace("Z", "+00:00"))
    require(stamp.utcoffset() == timezone.utc.utcoffset(stamp) and -1 <= time.time() - stamp.timestamp() <= 120,
            "stale-admission")


EVIDENCE_READBACK = """import json,pathlib,sys
root=pathlib.Path(sys.argv[1]); count=int(sys.argv[2]); stage=sys.argv[3]
files=['summary.json']+[f'run-{i}/{name}.json' for i in range(1,count+1) for name in ['binding','result','b-cleanup']]
if stage=='lifecycle-smoke': files+=['setup-failure/b-cleanup.json','setup-failure/result.json']
print(json.dumps({name:json.loads((root/name).read_text()) for name in files}))
"""
PROCESS_READBACK = """import pathlib,json,sys,os,hashlib
rows=[]
for entry in json.loads(sys.argv[1]):
 identity=entry['identity']; path=pathlib.Path('/proc')/str(identity['pid'])/'stat'
 same=path.exists() and path.read_text().rsplit(') ',1)[1].split()[19]==identity['startTicks']
 rows.append({'pid':identity['pid'],'startTicks':identity['startTicks'],'sameProcessPresent':same,
              'ownedAbsent':not os.path.lexists(entry['owned']),
              'bootIdSha256':hashlib.sha256(pathlib.Path('/proc/sys/kernel/random/boot_id').read_bytes()).hexdigest()})
print(json.dumps(rows))
"""


def postcondition_entries(evidence, request, admitted):
    entries = {"driver": [], "worker": []}
    summary = evidence["summary.json"]
    require(summary["passed"] == request["runs"], "driver-run-count")
    for index in range(1, request["runs"] + 1):
        prefix = "run-" + str(index) + "/"
        binding, result = evidence[prefix + "binding.json"], evidence[prefix + "result.json"]
        require(result["outcome"] == "passed" and result["localCleanup"] is True
                and result["remoteCleanup"] is True and result["cleanupFailures"] == [], "driver-result-failed")
        entries["driver"].append({"identity": binding["process"], "owned": binding["root"]})
        closed = evidence[prefix + "b-cleanup.json"]
        require(closed["clean"] is True and closed["failed"] is False, "worker-result-failed")
        entries["worker"].append({"identity": closed["process"], "owned": closed["owned"]})
    if request["stage"] == "lifecycle-smoke":
        closed = evidence["setup-failure/b-cleanup.json"]
        require(closed["clean"] is True and closed["failed"] is True, "expected-failure-not-cleaned")
        entries["worker"].append({"identity": closed["process"], "owned": closed["owned"]})
    for host, values in entries.items():
        parent = request["scratchParent"] if host == "driver" else request["remote"]["scratchParent"]
        boot = admitted["localBootIdSha256" if host == "driver" else "remoteBootIdSha256"]
        source = request["local"] if host == "driver" else request["remote"]["source"]
        for entry in values:
            relay_module.process_identity(entry["identity"], source)
            require(entry["identity"]["bootIdSha256"] == boot, "process-host-binding")
            require(str(Path(entry["owned"]).parent) == parent, "cleanup-root-outside-parent")
    return entries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", required=True)
    parser.add_argument("--admission-receipt", required=True)
    args = parser.parse_args()
    request_path = Path(args.request)
    require(not request_path.is_symlink(), "symlinked-request")
    raw = request_path.read_bytes()
    require(len(raw) <= relay_module.MAX_FRAME, "operator-request-size")
    operator = validate_operator_request(json.loads(raw))
    root = Path(operator["operatorRoot"])
    require(root.resolve() == root and request_path.parent.resolve() == root, "operator-root")
    admission = Path(args.admission_receipt)
    require(admission.parent.resolve() == root and not admission.is_symlink(), "admission-path")
    admitted = json.loads(admission.read_text())
    validate_admission(admitted, operator["runId"])
    for name, path in [("relaySha256", RELAY), ("operatorSha256", Path(__file__))]:
        require(hashlib.sha256(path.read_bytes()).hexdigest() == operator["transportSources"][name], "transport-source-mismatch")
    request, relay_request = requests_for(operator)
    relay_raw = json.dumps(relay_request).encode()
    driver_raw = json.dumps(request).encode()
    top_request = root / (operator["runId"] + ".relay-request.json")
    record_path = root / (operator["runId"] + ".operator.json")
    record = {"schema": "jones-je08h-operator-receipt/v1", "runId": operator["runId"], "stage": operator["stage"],
              "admissionSha256": hashlib.sha256(admission.read_bytes()).hexdigest(),
              "operatorRequestSha256": hashlib.sha256(raw).hexdigest(),
              "driverRequestSha256": hashlib.sha256(driver_raw).hexdigest(),
              "relayRequestSha256": hashlib.sha256(relay_raw).hexdigest(), "complete": False}
    def ssh(host, command):
        return [operator["ssh"], *relay_module.SSH_OPTIONS, operator["hosts"][host]["alias"], command]
    children = []
    def probe(host, code, arguments):
        command = shlex.join([operator["hosts"][host]["python"], "-c", code, *map(str, arguments)])
        child = subprocess.Popen(ssh(host, command), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        children.append(child)
        _, value = relay_module.Frames(child.stdout).read(time.monotonic() + 15)
        child.wait(timeout=12)
        require(child.returncode == 0, "readback-probe-exit")
        return value
    def cancel(_sig, _frame):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        raise RuntimeError("operator-cancelled")
    signal.signal(signal.SIGINT, cancel)
    signal.signal(signal.SIGTERM, cancel)
    with record_path.open("x") as receipt:
        try:
            with top_request.open("xb") as out:
                out.write(relay_raw)
            remote_path = operator["hosts"]["driver"]["taskRoot"] + "/" + operator["runId"] + ".driver.json"
            # The operator creates only this task's request and parent directories.
            staging = "import json,pathlib,sys; p=pathlib.Path(sys.argv[1]); b=sys.stdin.buffer.read(2097153); assert len(b)<=2097152; r=json.loads(b); p.parent.mkdir(parents=True,exist_ok=True); [pathlib.Path(r[k]).mkdir(parents=True,exist_ok=True) for k in ('scratchParent','evidenceParent')]; f=p.open('xb'); f.write(b); f.close()"
            remote_command = shlex.join([operator["hosts"]["driver"]["python"], "-c", staging, remote_path])
            child = subprocess.Popen(ssh("driver", remote_command), stdin=subprocess.PIPE,
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            children.append(child)
            child.communicate(driver_raw, timeout=20)
            assert child.returncode == 0
            parents = request["remote"]
            setup = "import pathlib,sys; [pathlib.Path(p).mkdir(parents=True,exist_ok=True) for p in sys.argv[1:]]"
            child = subprocess.Popen(ssh("worker", shlex.join([operator["hosts"]["worker"]["python"], "-c", setup,
                                     parents["scratchParent"], parents["evidenceParent"]])),
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            children.append(child)
            child.wait(timeout=20)
            assert child.returncode == 0
            relay = subprocess.Popen([sys.executable, str(RELAY), "--request", str(top_request)],
                                     stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                     env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "HOME": str(Path.home()), "PYTHONDONTWRITEBYTECODE": "1"})
            children.append(relay)
            assert select.select([relay.stdout], [], [], 20)[0]
            ready = json.loads(relay.stdout.readline(4096))
            assert ready == {"event": "relay-ready", "transport": request["transport"]}
            command = "cd " + shlex.quote(request["local"]["repository"]) + " && exec " + shlex.join([
                "/usr/bin/env", "-i", "PATH=/usr/bin:/bin", "LANG=C.UTF-8", request["local"]["node"],
                "apps/server/integration/jones-e2e/physicalHost.ts", "--request", remote_path])
            driver = subprocess.Popen(ssh("driver", command), stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE)
            children.append(driver)
            record["driverSshPid"] = driver.pid
            record["relayPid"] = relay.pid
            print(json.dumps({"event": "stage-started", "driverSshPid": driver.pid, "relayPid": relay.pid}), flush=True)
            streams = {driver.stdout: "stdout", driver.stderr: "stderr"}
            captures = {}
            limit = time.monotonic() + (750 if operator["stage"] == "qualify" else 270)
            try:
                for label in streams.values():
                    path = root / (operator["runId"] + ".driver." + label)
                    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                    captures[label] = [os.fdopen(fd, "wb"), 0, hashlib.sha256()]
                    record["driver" + label.title() + "Path"] = str(path)
                while streams:
                    remaining = limit - time.monotonic()
                    assert remaining > 0
                    ready, _, _ = select.select(list(streams), [], [], remaining)
                    assert ready
                    for stream in ready:
                        block = os.read(stream.fileno(), 65536)
                        if not block:
                            del streams[stream]
                            continue
                        handle, size, digest = captures[streams[stream]]
                        assert size + len(block) <= 1048576, "driver diagnostic limit"
                        handle.write(block)
                        digest.update(block)
                        captures[streams[stream]][1] += len(block)
                driver.wait(timeout=max(0.1, limit - time.monotonic()))
            finally:
                for label, (handle, size, digest) in captures.items():
                    handle.close()
                    record["driver" + label.title() + "Bytes"] = size
                    record["driver" + label.title() + "Sha256"] = digest.hexdigest()
            record["driverExitCode"] = driver.returncode
            assert driver.returncode == 0
            relay.wait(timeout=60)
            record["relayExitCode"] = relay.returncode
            assert relay.returncode == 0
            relay_receipt = json.loads((root / "receipts" / (relay_request["relayBindingId"] + ".json")).read_text())
            assert relay_receipt["operatorCleanupComplete"] and relay_receipt["transportComplete"]
            summary = json.loads(Path(record["driverStdoutPath"]).read_text())
            relay_module.owned_path(summary["evidence"], request["evidenceParent"], request["runId"])
            evidence = probe("driver", EVIDENCE_READBACK, [summary["evidence"], request["runs"], request["stage"]])
            with (root / (operator["runId"] + ".driver-evidence.json")).open("x") as output:
                json.dump(evidence, output, indent=2)
            entries = postcondition_entries(evidence, request, admitted)
            record["postconditions"] = {}
            for host, values in entries.items():
                checks = probe(host, PROCESS_READBACK, [json.dumps(values)])
                require(isinstance(checks, list) and len(checks) == len(values), "postcondition-count")
                for expected, actual in zip(values, checks):
                    require(actual.get("pid") == expected["identity"]["pid"]
                            and actual.get("startTicks") == expected["identity"]["startTicks"]
                            and actual.get("bootIdSha256") == expected["identity"]["bootIdSha256"]
                            and actual.get("sameProcessPresent") is False and actual.get("ownedAbsent") is True,
                            "process-or-root-cleanup-unproved")
                record["postconditions"][host] = checks
            record["complete"] = True
        except BaseException as exc:
            record["errorType"] = type(exc).__name__
        finally:
            # Captured local processes only; remote cleanup remains a separate receipt gate.
            for child in reversed(children):
                if child.poll() is None:
                    child.terminate()
                    try:
                        child.wait(timeout=60)
                    except subprocess.TimeoutExpired:
                        record["capturedChildClosureUnknown"] = True
            record["children"] = [{"pid": p.pid, "exitCode": p.poll()} for p in children]
            json.dump(record, receipt, indent=2)
            receipt.write("\n")
        return 0 if record["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
