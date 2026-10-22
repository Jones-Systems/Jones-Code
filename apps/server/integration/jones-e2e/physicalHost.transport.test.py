"""Pure transport checks. Native qualification remains an explicit operator action."""
import copy
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import socket
import sys
import time
import unittest
from unittest.mock import Mock, patch

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("physical_host_operator", HERE / "physicalHost.operator.py")
operator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(operator)
relay = operator.relay_module


def request_fixture(stage="qualify"):
    value = json.loads((HERE / "physicalHost.request.example.json").read_text())
    value["runId"], value["stage"] = "transport-test", stage
    value["hosts"]["driver"]["alias"] = "driver-test"
    value["hosts"]["worker"]["alias"] = "worker-test"
    for source in [value["local"], value["remote"]]:
        source["identity"] = "synthetic-source"
        source["hashes"] = {name: "a" * 64 for name in source["hashes"]}
    value["transportSources"] = {name: "b" * 64 for name in value["transportSources"]}
    return value


def admission_fixture():
    return {"schema": "jones-je08h-admission/v1", "runId": "transport-test", "bothHostSlotsGranted": True,
            "collectedAtUtc": datetime.now(timezone.utc).isoformat(),
            "cpuPercent": {host: 10 for host in ["operator", "driver", "worker"]},
            "serialTestProcessCeiling": {host: 1 for host in ["operator", "driver", "worker"]},
            "remoteRamAvailableGiB": {host: 12 for host in ["driver", "worker"]},
            "operatorMemoryDecision": "continue", "localBootIdSha256": "a" * 64,
            "remoteBootIdSha256": "b" * 64}


def samples_fixture():
    sample = {"schema": "work-capacity-admission/v1", "status": "ok",
              "collected_at_utc": datetime.now(timezone.utc).isoformat(),
              "admission": {"serial_test_process_ceiling": 1,
                            "measurements": {"cpu_used_percent": 20, "ram_available_gib": 12}}}
    return {host: copy.deepcopy(sample) for host in ["driver", "worker"]}


class TransportTests(unittest.TestCase):
    def test_example_requires_explicit_binding(self):
        with self.assertRaises(relay.Refusal):
            operator.validate_operator_request(json.loads((HERE / "physicalHost.request.example.json").read_text()))

    def test_closed_schema_and_routes(self):
        value = request_fixture()
        operator.validate_operator_request(value)
        for change in [lambda r: r.update(unexpected=True),
                       lambda r: r["hosts"]["driver"].update(alias="-oProxyCommand=bad"),
                       lambda r: r["remote"].update(node="../node"),
                       lambda r: r["remote"]["hashes"].pop("pnpm-lock.yaml"),
                       lambda r: r.update(peerPort=r["controlPort"])]:
            bad = copy.deepcopy(value)
            change(bad)
            with self.assertRaises(relay.Refusal):
                operator.validate_operator_request(bad)

    def test_session_derivation_matches_driver(self):
        for stage, count in [("import-smoke", 1), ("lifecycle-smoke", 2), ("qualify", 4)]:
            driver, transport = operator.requests_for(request_fixture(stage))
            self.assertEqual(driver["runs"], 4 if stage == "qualify" else 1)
            self.assertEqual(len(transport["sessions"]), count)
            self.assertEqual(transport["sessions"][0]["runId"], "transport-test-1")
            self.assertEqual(transport["source"], driver["remote"]["source"])
            if stage == "lifecycle-smoke":
                self.assertEqual(transport["sessions"][0]["expectedEnd"], "cancel")
                self.assertEqual(transport["sessions"][1], {
                    "runId": "transport-test-failure", "stage": stage,
                    "fault": "after-listening", "expectedEnd": "after-listening"})

    def test_initial_admission_refuses_hold_and_wrong_host(self):
        operator.validate_admission(admission_fixture(), "transport-test")
        changes = [lambda a: a["cpuPercent"].update(worker=85),
                   lambda a: a["serialTestProcessCeiling"].update(driver=0),
                   lambda a: a["remoteRamAvailableGiB"].update(worker=0),
                   lambda a: a.update(operatorMemoryDecision="hold"),
                   lambda a: a.update(collectedAtUtc="2000-01-01T00:00:00Z"),
                   lambda a: a.update(remoteBootIdSha256=a["localBootIdSha256"])]
        for change in changes:
            admitted = admission_fixture()
            change(admitted)
            with self.assertRaises(relay.Refusal):
                operator.validate_admission(admitted, "transport-test")

    def test_admission_failure_prevents_release_and_next_run(self):
        for name in ["cpu85", "missing-host", "unavailable", "missing-ram", "stale", "deadline"]:
            with self.subTest(name=name):
                samples = samples_fixture()
                if name == "cpu85": samples["worker"]["admission"]["measurements"]["cpu_used_percent"] = 85
                if name == "missing-host": del samples["driver"]
                if name == "unavailable": samples["driver"]["status"] = "unavailable"
                if name == "missing-ram": del samples["worker"]["admission"]["measurements"]["ram_available_gib"]
                if name == "stale": samples["driver"]["collected_at_utc"] = "2000-01-01T00:00:00Z"
                events, receipt = [], {}
                with self.assertRaises(relay.Refusal):
                    relay.admission_barrier("next-run", lambda deadline: samples,
                                            lambda: events.append("release"), receipt,
                                            time.monotonic() + (-1 if name == "deadline" else 5))
                    events.append("next-run")
                self.assertEqual(events, [])
                self.assertFalse(receipt["released"])

    def test_admission_pass_releases_once_after_collection(self):
        events, receipt = [], {}
        def collect(deadline):
            events.append("collected")
            return samples_fixture()
        relay.admission_barrier("next-run", collect, lambda: events.append("released"), receipt,
                                time.monotonic() + 5)
        self.assertEqual(events, ["collected", "released"])
        self.assertTrue(receipt["released"])
        self.assertEqual(receipt["nextRunId"], "next-run")

    def test_frames_preserve_exact_bytes_and_reject_partial_eof(self):
        left, right = socket.socketpair()
        try:
            raw = b'{ "schema" : "test", "value": 1 }\n'
            left.sendall(raw)
            frames = relay.Frames(right)
            received, value = frames.read(time.monotonic() + 1)
            self.assertEqual(received, raw)
            self.assertEqual(value, {"schema": "test", "value": 1})
            left.sendall(b'{"unfinished":')
            left.shutdown(socket.SHUT_WR)
            self.assertTrue(frames.consume())
            with self.assertRaisesRegex(relay.Refusal, "partial-control-frame"):
                frames.consume()
        finally:
            left.close()
            right.close()

    def test_forward_close_requires_remote_identity_absence(self):
        _, request = operator.requests_for(request_fixture())
        instance = relay.Relay(request)
        child = Mock(returncode=0)
        item = {"host": "worker", "remote": {"pid": 123, "startTicks": "456"}}
        with patch.object(instance, "stop"), patch.object(instance, "helper", return_value=(None, None, {"sameProcessPresent": True})):
            with self.assertRaisesRegex(relay.Refusal, "forward-helper-still-present"):
                instance.close_forward(child, item)
        self.assertNotIn("remoteProcessAbsent", item)
        with patch.object(instance, "stop"), patch.object(instance, "helper", return_value=(None, None, {"sameProcessPresent": False})):
            instance.close_forward(child, item)
        self.assertTrue(item["remoteProcessAbsent"])

    def test_postconditions_reject_wrong_host_or_cleanup_result(self):
        request, _ = operator.requests_for(request_fixture("import-smoke"))
        admitted = admission_fixture()
        def identity(source, boot):
            return {"pid": 123, "ppid": 122, "pgid": 123, "startTicks": "456",
                    "executable": source["node"], "nodeVersion": source["nodeVersion"],
                    "cwd": source["repository"], "bootIdSha256": boot}
        evidence = {
            "summary.json": {"passed": 1},
            "run-1/binding.json": {"process": identity(request["local"], admitted["localBootIdSha256"]),
                                   "root": request["scratchParent"] + "/je08h-transport-test-1-owned"},
            "run-1/result.json": {"outcome": "passed", "localCleanup": True,
                                  "remoteCleanup": True, "cleanupFailures": []},
            "run-1/b-cleanup.json": {"clean": True, "failed": False,
                                     "process": identity(request["remote"]["source"], admitted["remoteBootIdSha256"]),
                                     "owned": request["remote"]["scratchParent"] + "/je08h-transport-test-1-owned"},
        }
        entries = operator.postcondition_entries(evidence, request, admitted)
        self.assertEqual(set(entries), {"driver", "worker"})
        for change in [lambda e: e["run-1/result.json"].update(localCleanup=False),
                       lambda e: e["run-1/b-cleanup.json"]["process"].update(bootIdSha256="c" * 64),
                       lambda e: e["run-1/binding.json"].update(root="/outside/owned")]:
            bad = copy.deepcopy(evidence)
            change(bad)
            with self.assertRaises(relay.Refusal):
                operator.postcondition_entries(bad, request, admitted)

    def test_process_identity_is_bound_to_source(self):
        source = request_fixture()["remote"]
        identity = {"pid": 123, "ppid": 122, "pgid": 123, "startTicks": "456", "executable": source["node"],
                    "nodeVersion": source["nodeVersion"], "cwd": source["repository"], "bootIdSha256": "a" * 64}
        relay.process_identity(identity, source)
        identity["cwd"] = "/other/checkout"
        with self.assertRaises(relay.Refusal):
            relay.process_identity(identity, source)


if __name__ == "__main__":
    unittest.main()
