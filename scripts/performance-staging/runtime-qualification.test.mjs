import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import * as NodeURL from "node:url";

import { createOwnedRoot, disposeOwnedRoot } from "./guard.mjs";
import { runOwnedChild } from "./lifecycle.mjs";

const directory = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const worktree = NodePath.resolve(directory, "../..");
const runtimeRoot = "/home/malcolmjones/Projects/Jones-Code-performance-runtime-20261002";
const acquisitionPath = NodePath.join(runtimeRoot, "acquisition.json");
const workerPath = NodePath.join(directory, "runtime-qualification-worker.mjs");
const executablePath = NodePath.join(runtimeRoot, "bin/node");
const sourceRevision = "da5f4aee0035beec471b38598eaa2857d1e5155c";
const enabled = process.env.JONES_RUNTIME_SOURCE_QUALIFICATION === "1";

function sha256(path, maximum = 4 * 1024 * 1024) {
  const info = NodeFS.lstatSync(path);
  NodeAssert.ok(
    info.isFile() && !info.isSymbolicLink() && info.size <= maximum,
    `unavailable: unsupported or oversized input ${path}`,
  );
  const digest = NodeCrypto.createHash("sha256");
  const fd = NodeFS.openSync(path, "r");
  try {
    const opened = NodeFS.fstatSync(fd);
    NodeAssert.equal(opened.dev, info.dev);
    NodeAssert.equal(opened.ino, info.ino);
    const buffer = Buffer.alloc(64 * 1024);
    let bytes = 0;
    for (;;) {
      const count = NodeFS.readSync(fd, buffer, 0, buffer.length, null);
      if (!count) return digest.digest("hex");
      bytes += count;
      NodeAssert.ok(bytes <= maximum, `unavailable: input grew beyond its limit ${path}`);
      digest.update(buffer.subarray(0, count));
    }
  } finally {
    NodeFS.closeSync(fd);
  }
}

function checkedAcquisition() {
  NodeAssert.equal(
    sha256(acquisitionPath),
    "abca9a13ab7d32cbf99bda2d075de4e0061c7a546ee594afc520704483a4151d",
    "unavailable: root acquisition receipt changed",
  );
  const metadata = JSON.parse(NodeFS.readFileSync(acquisitionPath, "utf8"));
  NodeAssert.equal(metadata.schema, "jones-performance-owned-node-acquisition/v1");
  NodeAssert.equal(metadata.root, runtimeRoot);
  NodeAssert.equal(metadata.status, "binary_extracted_not_executed");
  NodeAssert.equal(metadata.node_version, "26.8.2");
  NodeAssert.equal(metadata.executable_path, executablePath);
  NodeAssert.equal(
    metadata.archive_sha256,
    "40e1d3225c1c9ae9a2671c98ecb9857e4d5555026394f348645676798840d5c5",
  );
  NodeAssert.equal(metadata.archive_sha256, metadata.expected_archive_sha256);
  NodeAssert.equal(
    metadata.checksum_metadata_sha256,
    "c31cbd53707d1e82ed2094d4554eb13562a8be9521433f8bc4447776a7e7dad3",
  );
  NodeAssert.equal(NodeFS.realpathSync(executablePath), executablePath);
  NodeAssert.equal(NodeFS.lstatSync(executablePath).size, 150434304);
  NodeAssert.equal(sha256(executablePath, 150434304), metadata.executable_sha256);
  NodeAssert.equal(
    metadata.executable_sha256,
    "8a22a371fd85aecf5411636574309f6380fbc42694aaf0651a089a8ef9c44e52",
  );
  return metadata;
}

NodeTest.test(
  "Node 26.8.2 source qualification uses the seven unchanged A behavior cases",
  {
    skip: enabled
      ? false
      : "unavailable: JONES_RUNTIME_SOURCE_QUALIFICATION=1 and exact runtime grant required",
  },
  async (t) => {
    const metadata = checkedAcquisition();
    const runId = NodeCrypto.randomUUID();
    const parentPath = NodePath.join(runtimeRoot, "evidence");
    NodeAssert.equal(NodeFS.realpathSync(parentPath), parentPath);
    const sourceIdentity = process.env.JONES_RUNTIME_HARNESS_REVISION;
    NodeAssert.match(
      sourceIdentity ?? "",
      /^[a-f0-9]{40}$/,
      "unavailable: executor must bind the exact harness candidate revision",
    );
    const owner = createOwnedRoot({
      parentPath,
      childName: `source-qualification-${runId}`,
      binding: {
        repository: "Jones-Systems/Jones-Code",
        sourceRevision: sourceIdentity,
        taskRef: "spec.jones-performance-portfolio#task.e-migrate.001",
        runId,
      },
      policy: {
        homePath: runtimeRoot,
        worktreePaths: [worktree],
        protectedPaths: [
          "/home/malcolmjones/.t3",
          "/home/malcolmjones/.codex",
          "/home/malcolmjones/Projects/Jones-Code-performance-worktrees-20261002/lease",
          "/home/malcolmjones/Projects/Jones-Code-performance-worktrees-20261002/baseline",
          "/home/malcolmjones/Projects/Jones-Code-performance-worktrees-20261002/live-baseline",
        ],
        maxFiles: 2048,
        maxFileBytes: 32 * 1024 * 1024,
        maxTotalBytes: 128 * 1024 * 1024,
        maxReceiptBytes: 24 * 1024,
      },
    });
    const root = owner.creationReceipt.canonicalRootPath;
    const cancellation = new AbortController();
    const abort = () => cancellation.abort();
    process.once("SIGINT", abort);
    process.once("SIGTERM", abort);
    let child;
    let report;
    let failure;
    let cleanup;
    let evidencePreserved = false;
    const evidencePath = NodePath.join(parentPath, `source-qualification-${runId}.jsonl`);
    try {
      for (const relative of ["home", "home/config", "home/data", "tmp", "cache", "node_modules"]) {
        NodeFS.mkdirSync(NodePath.join(root, relative), { mode: 0o700, recursive: true });
      }
      const requestPath = NodePath.join(root, "request.json");
      NodeFS.writeFileSync(
        requestPath,
        `${JSON.stringify({
          schema: "jones-performance-source-runtime-request/v1",
          ownedRootPath: root,
          sourceRevision,
          executablePath,
          nodeVersion: "26.8.2",
        })}\n`,
        { flag: "wx", mode: 0o600 },
      );
      child = await runOwnedChild({
        owner,
        executable: executablePath,
        args: ["--no-warnings", workerPath],
        env: {
          HOME: NodePath.join(root, "home"),
          TMPDIR: NodePath.join(root, "tmp"),
          TMP: NodePath.join(root, "tmp"),
          TEMP: NodePath.join(root, "tmp"),
          XDG_CACHE_HOME: NodePath.join(root, "cache"),
          XDG_CONFIG_HOME: NodePath.join(root, "home/config"),
          XDG_DATA_HOME: NodePath.join(root, "home/data"),
          LANG: "C.UTF-8",
          TZ: "UTC",
          NODE_ENV: "test",
          NODE_NO_WARNINGS: "1",
          JONES_PERFORMANCE_RUNTIME_REQUEST: requestPath,
        },
        timeoutMs: 120_000,
        terminateGraceMs: 10_000,
        reapTimeoutMs: 5_000,
        maxOutputBytes: 56 * 1024,
        signal: AbortSignal.any([cancellation.signal, t.signal]),
      });
      if (!child.truncated && child.stdout.trim()) report = JSON.parse(child.stdout);
      NodeAssert.equal(child.closed, true, "unknown: qualification leaf did not close");
      NodeAssert.equal(child.reaped, true, "unknown: qualification leaf was not reaped");
      NodeAssert.equal(child.outcome, "success", child.stderr || child.stopReason || report?.error);
      NodeAssert.equal(report?.schema, "jones-performance-source-runtime/v1");
      NodeAssert.equal(report?.outcome, "passed", report?.error);
      NodeAssert.equal(report?.runtime.execPath, executablePath);
      NodeAssert.equal(report?.runtime.nodeVersion, "26.8.2");
      NodeAssert.equal(report?.source.boundRevision, sourceRevision);
      NodeAssert.equal(report?.selectedCases.length, 7);
      NodeAssert.equal(report?.cases.length, 7);
      NodeAssert.ok(report.cases.every((entry) => entry.state === "passed"));
      NodeAssert.equal(report.runnerClosed, true, "unknown: runner closure was not established");
      NodeAssert.equal(report.closeErrors, false, "runner logged a closure error");
      NodeAssert.equal(report.compiledQualification, "unknown/held");
      NodeAssert.equal(report.productionSQLiteQualification, "unverified");
    } catch (error) {
      failure = error;
    } finally {
      cancellation.abort();
      process.removeListener("SIGINT", abort);
      process.removeListener("SIGTERM", abort);
      const childEvidence = child
        ? {
            ...child,
            stdout: report ? "" : child.stdout,
            stdoutSha256: NodeCrypto.createHash("sha256").update(child.stdout).digest("hex"),
          }
        : null;
      let harnessFileSha256;
      let identityFailure;
      try {
        harnessFileSha256 = {
          "runtime-qualification.test.mjs": sha256(NodeURL.fileURLToPath(import.meta.url)),
          "runtime-qualification-worker.mjs": sha256(workerPath),
          "guard.mjs": sha256(NodePath.join(directory, "guard.mjs")),
          "lifecycle.mjs": sha256(NodePath.join(directory, "lifecycle.mjs")),
        };
      } catch (error) {
        failure ??= error;
        identityFailure = String(error.stack ?? error).slice(0, 4096);
      }
      const evidence = {
        schema: "jones-performance-source-runtime-evidence/v1",
        phase: "before-cleanup",
        runId,
        taskRef: owner.creationReceipt.binding.taskRef,
        sourceRevision,
        declaredHarnessRevision: sourceIdentity,
        harnessFileSha256,
        identityFailure: identityFailure ?? null,
        acquisition: metadata,
        child: childEvidence,
        report,
        originalError: failure ? String(failure.stack ?? failure).slice(0, 4096) : null,
      };
      try {
        const bytes = `${JSON.stringify(evidence)}\n`;
        NodeAssert.ok(Buffer.byteLength(bytes) <= 64 * 1024, "evidence output limit");
        NodeFS.writeFileSync(evidencePath, bytes, { flag: "wx", mode: 0o600 });
        evidencePreserved = !identityFailure;
      } catch (error) {
        failure ??= error;
      }
      // A fulfilled runner promise cannot release this root; its captured leaf and close diagnostics must agree.
      const knownClosure =
        !child ||
        (child.closed &&
          child.reaped &&
          child.outcome !== "unknown" &&
          (child.pid === null || (report?.runnerClosed === true && !report?.closeErrors)));
      cleanup =
        knownClosure && evidencePreserved
          ? disposeOwnedRoot(owner, { childReceipts: child ? [child] : [] })
          : {
              schema: "jones-performance-cleanup/v1",
              outcome: "retained",
              absent: false,
              reason: evidencePreserved
                ? "unknown_runner_or_leaf_closure"
                : "evidence_preservation_failed",
              creationReceipt: owner.creationReceipt,
            };
      evidence.cleanup = cleanup;
      const cleanupReadback = {
        schema: cleanup.schema,
        outcome: cleanup.outcome,
        absent: cleanup.absent,
        reason: cleanup.reason,
        rootPath: root,
      };
      if (evidencePreserved) {
        try {
          NodeFS.appendFileSync(
            evidencePath,
            `${JSON.stringify({ phase: "cleanup", runId, cleanup: cleanupReadback })}\n`,
          );
        } catch (error) {
          failure ??= error;
        }
      }
      t.diagnostic(
        JSON.stringify({
          evidencePath: evidencePreserved ? evidencePath : null,
          runId,
          outcome: report?.outcome ?? child?.outcome ?? "unavailable",
          cleanup: cleanupReadback,
        }),
      );
      if (cleanup.outcome !== "complete" || !cleanup.absent) {
        failure ??= new Error(`qualification cleanup retained ${root}: ${cleanup.reason}`);
      }
      if (failure) failure.evidence = evidence;
    }
    if (failure) throw failure;
  },
);
