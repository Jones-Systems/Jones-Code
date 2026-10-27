"""Disposable connected acceptance: real Fleet owners, synthetic native qualification.

The parent owns this fixture process, its short scratch and its shutdown. No
installed binding, authentication state or live service is read or changed.
"""
from dataclasses import asdict
import json
from pathlib import Path
import sys
from types import SimpleNamespace

from host_tooling.fleet_ops.bc_client import ControllerClient
from host_tooling.fleet_ops.connections.bindings import PeerBinding
from host_tooling.fleet_ops.controller import cli
from host_tooling.fleet_ops.durable import SubmissionBinding
import test_fleet_connections as nf
from test_fleet_bc_e2e import controller_server
from test_fleet_placement_integration import PlacementIntegrationTests


if sys.argv[1] == "client":
    binding = Path(sys.argv[sys.argv.index("--binding") + 1])
    config = json.loads(binding.read_text())
    peer = PeerBinding(**config["peer"])
    client = ControllerClient(controller=peer, port=config["port"], verifier=nf.verifier(peer), timeout_ms=10000)
    raise SystemExit(cli.main(sys.argv[2:], composition=SimpleNamespace(client=lambda _: client)))

case = PlacementIntegrationTests()
try:
    case.setUp()
    with controller_server(case.controller):
        case.step()
        request = case.request().to_mapping()
        binding = case.root / "jones-client.json"
        binding.write_text(json.dumps({"peer": asdict(case.controller_peer), "port": case.port}))
        print(json.dumps({"request": request, "binding": str(binding)}), flush=True)
        for line in sys.stdin:
            command = json.loads(line)
            if command["action"] == "stop":
                break
            if command["action"] == "execute":
                selected = SubmissionBinding.from_mapping(command["binding"])
                case.step()
                local = case.service.run_one(selected)
                case.step()
                case.step()
                print(json.dumps({"state": local.state.value}), flush=True)
            elif command["action"] == "collect":
                case.step()
                print(json.dumps({"collected": True}), flush=True)
finally:
    case.doCleanups()
