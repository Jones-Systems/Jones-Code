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
const diagnostic = process.env.JONES_RUNTIME_NODE24_DIAGNOSTIC === "1";
const diagnosticBindingPath = NodePath.join(runtimeRoot, "evidence/node24-diagnostic-binding.json");
const diagnosticExecutablePath =
  "/home/malcolmjones/.local/lib/nodejs/node-v24.19.0-linux-x64/bin/node";
const phaseJournalLimit = 8 * 1024;

function capturePhaseJournal(root) {
  let complete = true;
  const journals = Object.fromEntries(
    ["main", "setup"].map((role) => {
      const path = NodePath.join(root, `phases-${role}.jsonl`);
      const info = NodeFS.lstatSync(path);
      NodeAssert.ok(
        info.isFile() &&
          !info.isSymbolicLink() &&
          info.nlink === 1 &&
          info.size <= phaseJournalLimit,
        "unavailable: unsupported or oversized phase journal",
      );
      const bytes = NodeFS.readFileSync(path);
      NodeAssert.ok(bytes.length <= phaseJournalLimit, "phase journal exceeded its bound");
      const lines = bytes.toString("utf8").split("\n");
      const tail = lines.pop();
      if (tail) complete = false;
      const records = lines.map((line) => {
        const record = JSON.parse(line);
        NodeAssert.deepEqual(Object.keys(record).sort(), ["phase", "pid", "threadId"]);
        NodeAssert.match(record.phase, /^[a-z][a-z-]{0,63}$/);
        NodeAssert.ok(
          Number.isSafeInteger(record.pid) &&
            record.pid > 0 &&
            Number.isSafeInteger(record.threadId) &&
            record.threadId >= 0,
          "invalid phase journal identity",
        );
        return record;
      });
      return [
        role,
        {
          byteLength: bytes.length,
          sha256: NodeCrypto.createHash("sha256").update(bytes).digest("hex"),
          records,
          partialTrailingBytes: Buffer.byteLength(tail ?? ""),
        },
      ];
    }),
  );
  return {
    schema: "jones-performance-runtime-phase-journal/v1",
    outcome: complete ? "captured" : "partial",
    journals,
  };
}

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

function checkedDiagnosticBinding() {
  NodeAssert.equal(
    sha256(diagnosticBindingPath, 1024),
    "0e658b24c1f26fc29b18bbdc312362fe2abc8b9789544c7e4f52a1d0d965e4f9",
    "unavailable: root Node 24 diagnostic binding changed",
  );
  const binding = JSON.parse(NodeFS.readFileSync(diagnosticBindingPath, "utf8"));
  NodeAssert.equal(binding.schema, "jones-performance-node24-diagnostic-binding/v1");
  NodeAssert.equal(binding.executable_path, diagnosticExecutablePath);
  NodeAssert.equal(binding.executable_bytes, 125989464);
  NodeAssert.equal(binding.expected_node_version, "24.19.0");
  NodeAssert.equal(
    binding.executable_sha256,
    "bc17c508ffeed0ec622934f9b7fa72f8e78da65350e63c3eceb56fa688aa5e12",
  );
  NodeAssert.equal(NodeFS.realpathSync(diagnosticExecutablePath), diagnosticExecutablePath);
  NodeAssert.equal(NodeFS.lstatSync(diagnosticExecutablePath).size, binding.executable_bytes);
  NodeAssert.equal(
    sha256(diagnosticExecutablePath, binding.executable_bytes),
    binding.executable_sha256,
  );
  return binding;
}

NodeTest.test(
  diagnostic
    ? "Node 24 diagnostic comparator uses the seven unchanged A behavior cases"
    : "Node 26.8.2 source qualification uses the seven unchanged A behavior cases",
  {
    skip:
      enabled || diagnostic
        ? false
        : "unavailable: JONES_RUNTIME_SOURCE_QUALIFICATION=1 and exact runtime grant required",
  },
  async (t) => {
    NodeAssert.ok(!(enabled && diagnostic), "unavailable: choose exactly one runtime mode");
    const metadata = diagnostic ? checkedDiagnosticBinding() : checkedAcquisition();
    const actualInvoker = diagnostic
      ? { execPath: process.execPath, nodeVersion: process.versions.node }
      : null;
    if (diagnostic) {
      t.diagnostic(
        JSON.stringify({
          mode: "node24-diagnostic",
          expectedNodeVersion: metadata.expected_node_version,
          actualInvoker,
        }),
      );
      NodeAssert.equal(actualInvoker.execPath, diagnosticExecutablePath);
      NodeAssert.equal(actualInvoker.nodeVersion, metadata.expected_node_version);
    }
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
    const evidencePath = NodePath.join(
      parentPath,
      diagnostic ? `node24-diagnostic-${runId}.jsonl` : `source-qualification-${runId}.jsonl`,
    );
    try {
      for (const relative of ["home", "home/config", "home/data", "tmp", "cache", "node_modules"]) {
        NodeFS.mkdirSync(NodePath.join(root, relative), { mode: 0o700, recursive: true });
      }
      const requestPath = NodePath.join(root, "request.json");
      NodeFS.writeFileSync(
        requestPath,
        `${JSON.stringify({
          schema: diagnostic
            ? "jones-performance-node24-diagnostic-request/v1"
            : "jones-performance-source-runtime-request/v1",
          ownedRootPath: root,
          sourceRevision,
          executablePath: diagnostic ? diagnosticExecutablePath : executablePath,
          nodeVersion: diagnostic ? metadata.expected_node_version : "26.8.2",
        })}\n`,
        { flag: "wx", mode: 0o600 },
      );
      NodeFS.writeFileSync(
        NodePath.join(root, "phases-main.jsonl"),
        `${JSON.stringify({ phase: "leaf-start-requested", pid: process.pid, threadId: 0 })}\n`,
        { flag: "wx", mode: 0o600 },
      );
      NodeFS.writeFileSync(NodePath.join(root, "phases-setup.jsonl"), "", {
        flag: "wx",
        mode: 0o600,
      });
      child = await runOwnedChild({
        owner,
        executable: diagnostic ? diagnosticExecutablePath : executablePath,
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
          ...(diagnostic ? { JONES_RUNTIME_NODE24_DIAGNOSTIC: "1" } : {}),
        },
        timeoutMs: 120_000,
        terminateGraceMs: 10_000,
        reapTimeoutMs: 5_000,
        maxOutputBytes: 56 * 1024,
        signal: AbortSignal.any([cancellation.signal, t.signal]),
      });
      let reportParseFailure;
      try {
        if (!child.truncated && child.stdout.trim()) report = JSON.parse(child.stdout);
      } catch (error) {
        reportParseFailure = error;
      }
      NodeAssert.equal(child.closed, true, "unknown: qualification leaf did not close");
      NodeAssert.equal(child.reaped, true, "unknown: qualification leaf was not reaped");
      NodeAssert.equal(
        child.outcome,
        "success",
        `qualification leaf outcome=${child.outcome}; exitCode=${child.exitCode}; signal=${child.signal ?? "none"}; stopReason=${child.stopReason ?? "none"}; ${report?.error ?? child.stderr ?? ""}`.slice(
          0,
          2048,
        ),
      );
      if (reportParseFailure) throw reportParseFailure;
      NodeAssert.equal(
        report?.schema,
        diagnostic
          ? "jones-performance-node24-diagnostic-runtime/v1"
          : "jones-performance-source-runtime/v1",
      );
      NodeAssert.equal(
        report?.outcome,
        "passed",
        String(report?.error ?? "qualification report is missing or did not pass").slice(0, 2048),
      );
      if (diagnostic) {
        NodeAssert.equal(report?.runtime.execPath, diagnosticExecutablePath);
        NodeAssert.equal(report?.runtime.nodeVersion, metadata.expected_node_version);
        NodeAssert.equal(report?.node26Qualification, "unverified");
      } else {
        NodeAssert.equal(report?.runtime.execPath, executablePath);
        NodeAssert.equal(report?.runtime.nodeVersion, "26.8.2");
      }
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
      let phaseJournal = {
        schema: "jones-performance-runtime-phase-journal/v1",
        outcome: "unknown",
        reason: "captured_leaf_closure_unknown",
      };
      if (!child || (child.closed && child.reaped)) {
        try {
          phaseJournal = capturePhaseJournal(root);
          if (phaseJournal.outcome !== "captured") {
            failure ??= new Error("phase journal has an incomplete trailing record");
          }
        } catch (error) {
          failure ??= error;
          phaseJournal.reason = "phase_journal_capture_failed";
        }
      }
      const evidence = {
        schema: diagnostic
          ? "jones-performance-node24-diagnostic-evidence/v1"
          : "jones-performance-source-runtime-evidence/v1",
        phase: "before-cleanup",
        runId,
        taskRef: owner.creationReceipt.binding.taskRef,
        sourceRevision,
        declaredHarnessRevision: sourceIdentity,
        harnessFileSha256,
        identityFailure: identityFailure ?? null,
        ...(diagnostic
          ? { diagnosticBinding: metadata, actualInvoker, node26Qualification: "unverified" }
          : { acquisition: metadata }),
        child: childEvidence,
        report,
        phaseJournal,
        originalError: failure ? String(failure.stack ?? failure).slice(0, 4096) : null,
      };
      try {
        const bytes = `${JSON.stringify(evidence)}\n`;
        NodeAssert.ok(Buffer.byteLength(bytes) <= 64 * 1024, "evidence output limit");
        NodeFS.writeFileSync(evidencePath, bytes, { flag: "wx", mode: 0o600 });
        evidencePreserved = !identityFailure && phaseJournal.outcome === "captured";
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
