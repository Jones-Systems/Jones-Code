import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { readRuntimeBinding } from "../../../../scripts/jones/performance/runtime-binding.mjs";
import { assertCurrentCandidate } from "../../../../scripts/jones/performance/current-qualification.mjs";
import { createOwnedRoot, disposeOwnedRoot } from "../../../../scripts/jones/performance/guard.mjs";
import { runOwnedChild } from "../../../../scripts/jones/performance/lifecycle.mjs";

const scriptPath = NodeURL.fileURLToPath(import.meta.url);
const worktreePath = NodePath.resolve(NodePath.dirname(scriptPath), "../../../..");

export function v2BenchmarkMetadata() {
  return {
    schema: "jones-sqlite-v2-benchmark-metadata/v1",
    outcome: "unavailable",
    reason: "explicit candidate/runtime binding required",
    execution: "explicit --request <JSON-file>",
    caps: { commands: 64, payloadBytes: 4096, intervalMs: 100 },
    workload: "single-thread-metadata-acceptance",
    sqlTiming: "unavailable",
  };
}

export function v2BenchmarkClosureKnown(child, report, evidencePreserved) {
  return (
    evidencePreserved &&
    (!child ||
      (child.closed === true &&
        child.reaped === true &&
        ["success", "failed", "timed_out", "cancelled", "output_limited", "spawn_refused"].includes(
          child.outcome,
        ) &&
        report?.databaseClosed === true))
  );
}

export function validateV2BenchmarkRequest(request) {
  NodeAssert.deepEqual(Object.keys(request).sort(), ["candidate", "parentPath", "workload"]);
  NodeAssert.deepEqual(Object.keys(request.workload).sort(), [
    "commands",
    "intervalMs",
    "payloadBytes",
  ]);
  for (const [key, minimum, maximum] of [
    ["commands", 1, 64],
    ["payloadBytes", 1, 4096],
    ["intervalMs", 0, 100],
  ]) {
    NodeAssert.ok(
      Number.isInteger(request.workload[key]) &&
        request.workload[key] >= minimum &&
        request.workload[key] <= maximum,
      `invalid ${key}`,
    );
  }
  NodeAssert.equal(
    request.candidate.worktreePath,
    worktreePath,
    "run the harness from the bound candidate checkout",
  );
  NodeAssert.equal(NodePath.resolve(request.parentPath), request.parentPath);
  return request;
}

export async function runV2Benchmark(input, signal) {
  const request = validateV2BenchmarkRequest(input);
  assertCurrentCandidate(request.candidate);
  const runtime = readRuntimeBinding();
  const owner = createOwnedRoot({
    parentPath: request.parentPath,
    childName: `v2-benchmark-${NodeCrypto.randomUUID()}`,
    binding: {
      repository: request.candidate.repository,
      sourceRevision: request.candidate.sourceRevision,
      taskRef: "jones-sqlite-v2-benchmark",
      runId: NodeCrypto.randomUUID(),
    },
    policy: {
      homePath: NodeOS.homedir(),
      worktreePaths: [worktreePath],
      protectedPaths: [".t3", ".codex", ".ssh", ".config"].map((name) =>
        NodePath.join(NodeOS.homedir(), name),
      ),
    },
  });
  const root = owner.creationReceipt.canonicalRootPath;
  const evidencePath = `${root}.jsonl`;
  let child,
    report,
    error,
    preserved = false;
  try {
    for (const name of ["home", "tmp", "cache"])
      NodeFS.mkdirSync(NodePath.join(root, name), { mode: 0o700 });
    NodeFS.writeFileSync(
      NodePath.join(root, "request.json"),
      JSON.stringify({ ...request, root }),
      { flag: "wx", mode: 0o600 },
    );
    NodeFS.writeFileSync(NodePath.join(root, "phases.jsonl"), "", { flag: "wx", mode: 0o600 });
    child = await runOwnedChild({
      owner,
      executable: runtime.executablePath,
      args: [NodePath.join(NodePath.dirname(scriptPath), "v2Benchmark.worker.mjs")],
      env: {
        HOME: NodePath.join(root, "home"),
        TMPDIR: NodePath.join(root, "tmp"),
        TMP: NodePath.join(root, "tmp"),
        TEMP: NodePath.join(root, "tmp"),
        XDG_CACHE_HOME: NodePath.join(root, "cache"),
        NODE_ENV: "test",
        JONES_RUNTIME_BINDING: process.env.JONES_RUNTIME_BINDING,
      },
      timeoutMs: 120000,
      terminateGraceMs: 10000,
      reapTimeoutMs: 5000,
      maxOutputBytes: 64 * 1024,
      signal,
    });
    NodeAssert.ok(
      child.closed && child.reaped && !child.truncated,
      "unknown child closure or truncated output",
    );
    report = JSON.parse(child.stdout);
    NodeAssert.equal(report.schema, "jones-sqlite-v2-benchmark/v1");
    NodeAssert.equal(child.outcome, "success", report.error);
    NodeAssert.equal(report.outcome, "passed");
    NodeAssert.equal(report.databaseClosed, true);
    NodeAssert.deepEqual(report.candidate, request.candidate);
    NodeAssert.deepEqual(report.runtime, runtime);
    NodeAssert.deepEqual(report.measurements.options, request.workload);
    assertCurrentCandidate(request.candidate);
  } catch (cause) {
    error = String(cause?.stack ?? cause).slice(0, 8192);
  }
  try {
    const phases = NodeFS.readFileSync(NodePath.join(root, "phases.jsonl"));
    NodeAssert.ok(phases.length <= 8192);
    NodeFS.writeFileSync(
      evidencePath,
      `${JSON.stringify({ candidate: request.candidate, runtime, child, report, error, phases: phases.toString("utf8") })}\n`,
      { flag: "wx", mode: 0o600 },
    );
    preserved = true;
  } catch (cause) {
    error ??= String(cause);
  }
  // Closure must be acknowledged by the database scope and the captured child before disposal.
  const cleanup = v2BenchmarkClosureKnown(child, report, preserved)
    ? disposeOwnedRoot(owner, { childReceipts: child ? [child] : [] })
    : { outcome: "retained", absent: false, reason: "database_or_child_closure_unknown" };
  if (preserved) NodeFS.appendFileSync(evidencePath, `${JSON.stringify({ cleanup })}\n`);
  return {
    outcome: !error && cleanup.absent ? "passed" : "failed",
    evidencePath: preserved ? evidencePath : null,
    report,
    error,
    cleanup,
  };
}

if (process.argv[1] && NodePath.resolve(process.argv[1]) === scriptPath) {
  if (process.argv.length === 2 || (process.argv.length === 3 && process.argv[2] === "--help")) {
    process.stdout.write(`${JSON.stringify(v2BenchmarkMetadata())}\n`);
  } else {
    NodeAssert.ok(process.argv.length === 4 && process.argv[2] === "--request");
    const path = process.argv[3];
    const info = NodeFS.lstatSync(path);
    NodeAssert.ok(info.isFile() && !info.isSymbolicLink() && info.size <= 16384);
    const abort = new AbortController();
    const cancel = () => abort.abort();
    process.once("SIGINT", cancel);
    process.once("SIGTERM", cancel);
    try {
      const result = await runV2Benchmark(
        JSON.parse(NodeFS.readFileSync(path, "utf8")),
        abort.signal,
      );
      process.stdout.write(`${JSON.stringify(result)}\n`);
      process.exitCode = result.outcome === "passed" ? 0 : 1;
    } finally {
      process.removeListener("SIGINT", cancel);
      process.removeListener("SIGTERM", cancel);
    }
  }
}
