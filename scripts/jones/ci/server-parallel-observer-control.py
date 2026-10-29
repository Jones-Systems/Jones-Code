"""Admit the inspected full artifact; never fetch or execute a workload."""

import datetime
import hashlib
import json
import os
import pathlib
import re
import stat
import sys
import zipfile

REPOSITORY = "Jones-Systems/Jones-Code"
REPOSITORY_ID = 1394424242
RUN = 40828828769
ATTEMPT = 1
WORKFLOW_ID = 400014551
WORKFLOW_PATH = ".github/workflows/server-parallel-qualification.yml"
JOB_ID = 123298044279
ARTIFACT_ID = 12877931910
ARTIFACT_NAME = f"server-parallel-linux-{RUN}-{ATTEMPT}"
REPORT_NAME = f"jones-server-parallel-reports-{RUN}-{ATTEMPT}"
SOURCE = "8e20d00f7df1c9f86bc48fcc296f0521a9f6e343"
TREE = "39e18d9e7eba8b51695ae053e35259d7300ca00f"
BUILD = "8cafed1d21a6fa361000b59ce444ab08ecf9af4bfcbdeb46fac58820f0e8b347"
WORKSPACE = "/home/runner/work/Jones-Code/Jones-Code"
NODE = "/home/runner/.vite-plus/js_runtime/node/26.12.1/bin/node"
HELPER_PATH = "scripts/jones/ci/server-parallel-observer-control.py"
INPUTS = (
    "package.json",
    "pnpm-lock.yaml",
    "patches/@ff-labs__fff-node@0.11.0.patch",
    "apps/server/src/jones/serverParallelism/diagnosticObservation.ts",
    "apps/server/src/jones/serverParallelism/resourceSamples.ts",
    "apps/server/src/jones/serverParallelism/nativeQualification.ts",
    "apps/server/src/jones/serverParallelism/combinedQualification.ts",
    "apps/server/src/jones/serverCompute/processSlot.ts",
    "scripts/jones/serverParallelism/observer-diagnostic.mjs",
)
SIDECARS = ("raw", "trace", "summary", "error", "cleanup")
MAX_ARCHIVE = 20_000_000
MAX_EXPANDED = 180_000_000
MAX_ENTRIES = 256


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def sha(value, length=64):
    return isinstance(value, str) and re.fullmatch(f"[0-9a-f]{{{length}}}", value) is not None


def no_duplicates(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "Duplicate JSON field")
        result[key] = value
    return result


def load(path):
    return json.loads(read(path), object_pairs_hook=no_duplicates)


def real(path, directory=True):
    path = pathlib.Path(path)
    require(path.is_absolute() and str(path) == str(path.resolve(strict=True)), "Noncanonical or symlink path")
    for component in (path, *path.parents):
        require(not component.is_symlink(), "Symlink component")
    mode = path.stat().st_mode
    require(stat.S_ISDIR(mode) if directory else stat.S_ISREG(mode), "Unexpected path type")
    require(path.stat().st_uid == os.getuid(), "Path is not owned by this job")
    return path


def read(path):
    return real(path, directory=False).read_bytes()


def write_new(path, value):
    real(path.parent)
    with path.open("x", encoding="utf-8") as output:
        json.dump(value, output, indent=2)
        output.write("\n")


def timestamp(value):
    parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(parsed.utcoffset() == datetime.timedelta(0), "Expected UTC metadata timestamp")
    return parsed


def context(root):
    root = real(root)
    evidence = real(root / "evidence")
    value = load(evidence / "context.json")
    require(value["repository"] == REPOSITORY, "Wrong control repository")
    require(value["event"] == "workflow_dispatch" and value["platform"] == "linux", "Manual Linux control only")
    require(value["scope"] == "observer-control", "Control scope required")
    require(value["source"] == SOURCE and sha(value["source"], 40), "Wrong explicit source pin")
    require(value["artifactName"] == ARTIFACT_NAME, "Wrong explicit full artifact")
    require(sha(value["archiveSha256"]) and sha(value["receiptSha256"]), "Explicit lowercase digests required")
    require(sha(value["orchestrationSha"], 40), "Explicit orchestration revision required")
    require(type(value["runId"]) is int and value["runId"] > 0 and value["runId"] != RUN, "Full/control run collision")
    require(value["runAttempt"] == 1, "Control retries require reconciliation; reruns are closed")
    require(value["workspace"] == WORKSPACE, "Relocated workload checkout")
    require(not root.is_relative_to(pathlib.Path(WORKSPACE)), "Orchestration must be outside measured checkout")
    require(read(root / "OWNER") == f"{value['runId']}.{value['runAttempt']}\n".encode(), "Control scratch ownership mismatch")
    return root, evidence, value


def metadata(root):
    root, evidence, ctx = context(root)
    names = ("run", "jobs", "artifacts", "workflow")
    values = {name: load(evidence / f"{name}.json") for name in names}
    run, jobs, artifacts, workflow = (values[name] for name in names)
    for key in ("repository", "head_repository"):
        require(run[key]["id"] == REPOSITORY_ID and run[key]["full_name"] == REPOSITORY, "Wrong source/head repository")
    require(run["id"] == RUN and run["run_attempt"] == ATTEMPT, "Wrong full run or attempt")
    require(run["head_sha"] == ctx["source"] and run["head_commit"]["id"] == SOURCE, "Run/source disagreement")
    require(run["head_commit"]["tree_id"] == TREE, "Wrong full source tree")
    require(run["workflow_id"] == WORKFLOW_ID and run["path"] == WORKFLOW_PATH, "Wrong full workflow")
    require(run["name"] == "Server parallel qualification" and run["event"] == "workflow_dispatch", "Wrong full workflow event/name")
    require(run["status"] == "completed" and run["conclusion"] == "success", "Full workflow did not succeed")
    require(workflow["id"] == WORKFLOW_ID and workflow["path"] == WORKFLOW_PATH, "Workflow metadata disagreement")
    require(jobs["total_count"] == len(jobs["jobs"]) <= 100, "Incomplete jobs metadata")
    selected_jobs = [job for job in jobs["jobs"] if job["id"] == JOB_ID]
    require(len(selected_jobs) == 1, "Missing/duplicate full Linux job")
    job = selected_jobs[0]
    require(job["run_id"] == RUN and job["run_attempt"] == ATTEMPT and job["head_sha"] == SOURCE, "Job/run/source disagreement")
    require(job["name"] == "Server parallel (linux)" and "ubuntu-24.04" in job["labels"], "Wrong full Linux job")
    require(job["status"] == "completed" and job["conclusion"] == "success", "Full Linux job did not succeed")
    require(artifacts["total_count"] == len(artifacts["artifacts"]) <= 100, "Incomplete artifact metadata")
    selected = [item for item in artifacts["artifacts"] if item["name"] == ctx["artifactName"]]
    require(len(selected) == 1, "Missing/duplicate named full artifact")
    artifact = selected[0]
    require(artifact["id"] == ARTIFACT_ID and artifact["expired"] is False, "Wrong/expired full artifact")
    require(artifact["digest"] == "sha256:" + ctx["archiveSha256"], "Metadata/archive digest disagreement")
    require(type(artifact["size_in_bytes"]) is int and 0 < artifact["size_in_bytes"] <= MAX_ARCHIVE, "Archive size bound")
    bound = artifact["workflow_run"]
    require(bound["id"] == RUN and bound["repository_id"] == REPOSITORY_ID and bound["head_repository_id"] == REPOSITORY_ID, "Artifact run/repository disagreement")
    require(bound["head_sha"] == SOURCE, "Artifact/source disagreement")
    require(timestamp(job["started_at"]) <= timestamp(artifact["created_at"]) <= timestamp(job["completed_at"]), "Artifact is outside successful job attempt")
    require(timestamp(artifact["expires_at"]) > datetime.datetime.now(datetime.timezone.utc), "Full artifact retention expired")
    orchestration = load(evidence / "orchestration-identity.json")
    helper_root = real(root / "orchestration")
    require(orchestration["head"] == ctx["orchestrationSha"] and sha(orchestration["tree"], 40), "Wrong orchestration revision")
    require(orchestration["directory"] == str(helper_root) and orchestration["trackedStatus"] == "", "Dirty/relocated orchestration")
    helper_hashes = {name: digest(read(helper_root / name)) for name in (WORKFLOW_PATH, HELPER_PATH)}
    require(pathlib.Path(__file__).resolve() == helper_root / HELPER_PATH, "Validator is outside pinned orchestration")
    hashes = {name: digest(read(evidence / f"{name}.json")) for name in names}
    return {
        "repository": REPOSITORY, "runId": RUN, "runAttempt": ATTEMPT,
        "workflowId": WORKFLOW_ID, "jobId": JOB_ID, "artifactId": ARTIFACT_ID,
        "artifactName": ARTIFACT_NAME, "metadataHashes": hashes,
        "metadataSha256": digest(json.dumps(hashes, sort_keys=True).encode()),
        "archiveSha256": ctx["archiveSha256"], "receiptSha256": ctx["receiptSha256"],
        "archiveSize": artifact["size_in_bytes"],
        "orchestration": {**orchestration, "helperHashes": helper_hashes},
    }


def archive(root, extract=False):
    root, evidence, ctx = context(root)
    admitted = metadata(root)
    archive_path = real(evidence / "full.zip", directory=False)
    require(archive_path.stat().st_size == admitted["archiveSize"] <= MAX_ARCHIVE, "Archive byte size mismatch")
    require(digest(archive_path.read_bytes()) == ctx["archiveSha256"], "Archive byte digest mismatch")
    destination = root / "observer-prior"
    if extract:
        require(not destination.exists() and not destination.is_symlink(), "Preexisting extraction destination")
    entries = {}
    with zipfile.ZipFile(archive_path) as zipped:
        infos = zipped.infolist()
        require(0 < len(infos) <= MAX_ENTRIES, "Archive entry bound")
        require(sum(item.file_size for item in infos) <= MAX_EXPANDED, "Archive expansion bound")
        for info in infos:
            name = info.filename.rstrip("/")
            parts = name.split("/")
            require(info.orig_filename == info.filename and "\\" not in name, "Unsafe ZIP filename")
            require(info.filename == name + ("/" if info.is_dir() else ""), "Noncanonical ZIP filename")
            require(all(part not in ("", ".", "..") for part in parts), "Unsafe ZIP path")
            require(parts[0] == REPORT_NAME and not name.startswith("/"), "Wrong ZIP report root")
            require(all(ord(char) >= 32 and ord(char) != 127 for char in name), "Control character in ZIP path")
            require(name not in entries, "Duplicate/overlapping ZIP entry")
            kind = stat.S_IFMT(info.external_attr >> 16)
            require(kind in ((0, stat.S_IFDIR) if info.is_dir() else (0, stat.S_IFREG)), "ZIP special file/symlink")
            require(not info.flag_bits & 1 and info.compress_type in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED), "Unsupported ZIP encoding")
            entries[name] = info
        for name, info in entries.items():
            for parent in pathlib.PurePosixPath(name).parents:
                if str(parent) in entries:
                    require(entries[str(parent)].is_dir(), "ZIP file/directory overlap")
            require(info.is_dir() or len(pathlib.PurePosixPath(name).parts) > 1, "Report root is a file")
        required = ["observer-full.json", "baseline-laptop.json", "source-command.json", "SOURCE_COMMIT", "OWNER", "source-build.patch"]
        required += [f"observer-full.json.{kind}.json" for kind in SIDECARS]
        require(all(f"{REPORT_NAME}/{name}" in entries and not entries[f"{REPORT_NAME}/{name}"].is_dir() for name in required), "Missing exact full evidence")
        if extract:
            destination.mkdir(mode=0o700)
            expanded = 0
            for name, info in entries.items():
                target = destination / name
                target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                real(target.parent)
                if info.is_dir():
                    target.mkdir(mode=0o700, exist_ok=True)
                    continue
                with zipped.open(info) as source, target.open("xb") as output:
                    written = 0
                    while chunk := source.read(1024 * 1024):
                        written += len(chunk)
                        expanded += len(chunk)
                        require(written <= info.file_size and expanded <= MAX_EXPANDED, "ZIP expanded beyond bound")
                        output.write(chunk)
                    require(written == info.file_size, "ZIP length mismatch")
    return admitted


def full_evidence(root):
    root, evidence, ctx = context(root)
    prior = real(real(root / "observer-prior") / REPORT_NAME)
    receipt_bytes = read(prior / "observer-full.json")
    require(digest(receipt_bytes) == ctx["receiptSha256"], "Full receipt digest mismatch")
    receipt = load(prior / "observer-full.json")
    expected = {
        "schema": "jones.server-parallelism.observer-diagnostic/v1",
        "source": SOURCE, "sourceTree": TREE, "build": BUILD, "mode": "full",
        "runtimeForm": "source", "profile": "laptop", "outcome": "recorded",
        "eligible": False, "passed": False, "complete": True, "cleanupProved": True,
        "aborted": False, "priorFullHash": None,
    }
    require(all(type(receipt.get(key)) is type(value) and receipt.get(key) == value for key, value in expected.items()), "Incomplete or mismatched full receipt")
    bounds = {"count": 2, "windows": 3, "acquisitions": 500, "files": 512, "fullRuns": 1, "controlRunsMaximum": 1}
    require(all(receipt["bounds"].get(key) == value for key, value in bounds.items()), "Changed diagnostic bounds")
    require(read(prior / "OWNER") == f"{RUN}.{ATTEMPT}\n".encode(), "Original OWNER changed")
    require(read(prior / "SOURCE_COMMIT") == (SOURCE + "\n").encode(), "Original SOURCE_COMMIT changed")
    require(read(prior / "source-build.patch") == b"", "Original source patch is not empty")
    for kind in SIDECARS:
        require(digest(read(prior / f"observer-full.json.{kind}.json")) == receipt["artifacts"][kind]["sha256"], "Full sidecar changed: " + kind)
    for name, field in (("baseline-laptop.json", "baselineHash"), ("source-command.json", "manifestHash")):
        require(digest(read(prior / name)) == receipt[field], "Original bytes changed: " + name)
    require(set(receipt["inputHashes"]) == set(INPUTS) and all(sha(value) for value in receipt["inputHashes"].values()), "Wrong diagnostic input set")
    return prior, receipt


def preflight(root):
    root, evidence, ctx = context(root)
    admitted = archive(root)
    prior, receipt = full_evidence(root)
    identities = {name: load(evidence / f"{name}.json") for name in ("before-setup", "before-admission")}
    for identity in identities.values():
        require(identity["directory"] == WORKSPACE and identity["head"] == SOURCE and identity["tree"] == TREE, "Workload checkout identity changed")
        require(identity["trackedStatus"] == "", "Measured tree is dirty")
    current = identities["before-admission"]
    require(current["node"] == NODE and current["nodeVersion"] == "26.12.1", "Node path/version changed")
    require(current["inputHashes"] == receipt["inputHashes"], "Measured diagnostic input bytes changed")
    generated = read(evidence / "regenerated-source-command.json")
    require(digest(generated) == receipt["manifestHash"], "Regenerated manifest bytes changed")
    manifest = load(evidence / "regenerated-source-command.json")
    require(manifest == {
        "schema": "jones.server-parallelism.command/v1", "runtimeForm": "source",
        "file": NODE, "args": ["--max-old-space-size=256", WORKSPACE + "/apps/server/src/bin.ts", "__jones-server-compute"],
        "build": BUILD, "identityResolver": WORKSPACE + "/apps/server/src/jones/serverCompute/workerCommand.ts",
    }, "Unbound regenerated source command")
    reports = real(ctx["reports"])
    scratch = real(ctx["scratch"])
    require(reports != scratch and not reports.is_relative_to(pathlib.Path(WORKSPACE)) and not scratch.is_relative_to(pathlib.Path(WORKSPACE)), "Workload outputs must stay outside source")
    owner = f"{ctx['runId']}.{ctx['runAttempt']}\n".encode()
    require(read(reports / "OWNER") == owner and read(scratch / "OWNER") == owner, "Workload output ownership mismatch")
    command = {
        "cwd": WORKSPACE, "file": NODE,
        "args": [WORKSPACE + "/scripts/jones/serverParallelism/ci-run.mjs", str(reports), str(scratch / "installed"), "observer-control", str(prior)],
    }
    return {
        "schema": "jones.server-parallelism.observer-control-route/v1",
        "eligible": False, "preflight": "passed", **admitted,
        "measured": {"source": SOURCE, "sourceTree": TREE, "build": BUILD, "manifestSha256": digest(generated), "inputHashes": current["inputHashes"], "checkouts": identities},
        "priorDirectory": str(prior), "command": command,
    }


def main():
    require(len(sys.argv) == 3, "Usage: server-parallel-observer-control.py metadata|extract|preflight|gate <owned root>")
    action, root = sys.argv[1], pathlib.Path(sys.argv[2])
    root, evidence, _ = context(root)
    if action == "metadata":
        result = metadata(root)
        write_new(evidence / "metadata-admission.json", result)
    elif action == "extract":
        archive(root, extract=True)
        full_evidence(root)
        result = {"priorDirectory": str(root / "observer-prior" / REPORT_NAME)}
    elif action in ("preflight", "gate"):
        result = preflight(root)
        if action == "preflight":
            write_new(evidence / "control-route.json", result)
        else:
            require(load(evidence / "control-route.json") == result, "Preflight evidence changed")
            # Claim before the shell command: a lost/partial effect never reopens admission.
            write_new(evidence / "control-command-started.json", {"effect": "unknown-until-command-receipts", "command": result["command"]})
    else:
        raise ValueError("Unknown validator operation")
    print(json.dumps(result))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError, zipfile.BadZipFile) as error:
        print(f"Observer control admission refused: {error}", file=sys.stderr)
        sys.exit(1)
