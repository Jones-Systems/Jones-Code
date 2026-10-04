import * as Assert from "node:assert/strict";
import * as Crypto from "node:crypto";
import * as FS from "node:fs";
import * as OS from "node:os";
import * as Path from "node:path";
import * as URL from "node:url";
import { readRuntimeBinding } from "../../../scripts/performance-staging/runtime-binding.mjs";
import { assertCurrentCandidate } from "../../../scripts/performance-staging/current-qualification.mjs";
import { createOwnedRoot, disposeOwnedRoot } from "../../../scripts/performance-staging/guard.mjs";
import { runOwnedChild } from "../../../scripts/performance-staging/lifecycle.mjs";

const scriptPath = URL.fileURLToPath(import.meta.url);
const worktreePath = Path.resolve(Path.dirname(scriptPath), "../../..");

export function validateV2BenchmarkRequest(request) {
  Assert.deepEqual(Object.keys(request).sort(), ["candidate", "parentPath", "workload"]);
  Assert.deepEqual(Object.keys(request.workload).sort(), ["commands", "intervalMs", "payloadBytes"]);
  for (const [key, minimum, maximum] of [["commands", 1, 64], ["payloadBytes", 1, 4096], ["intervalMs", 0, 100]]) {
    Assert.ok(Number.isInteger(request.workload[key]) && request.workload[key] >= minimum && request.workload[key] <= maximum, `invalid ${key}`);
  }
  Assert.equal(request.candidate.worktreePath, worktreePath, "run the harness from the bound candidate checkout");
  Assert.equal(Path.resolve(request.parentPath), request.parentPath);
  return request;
}

export async function runV2Benchmark(input, signal) {
  const request = validateV2BenchmarkRequest(input);
  assertCurrentCandidate(request.candidate);
  const runtime = readRuntimeBinding();
  const owner = createOwnedRoot({ parentPath: request.parentPath, childName: `v2-benchmark-${Crypto.randomUUID()}`,
    binding: { repository: request.candidate.repository, sourceRevision: request.candidate.sourceRevision, taskRef: "jones-sqlite-v2-benchmark", runId: Crypto.randomUUID() },
    policy: { homePath: OS.homedir(), worktreePaths: [worktreePath], protectedPaths: [".t3", ".codex", ".ssh", ".config"].map((name) => Path.join(OS.homedir(), name)) } });
  const root = owner.creationReceipt.canonicalRootPath;
  const evidencePath = `${root}.jsonl`;
  let child, report, error, preserved = false;
  try {
    for (const name of ["home", "tmp", "cache"]) FS.mkdirSync(Path.join(root, name), { mode: 0o700 });
    FS.writeFileSync(Path.join(root, "request.json"), JSON.stringify({ ...request, root }), { flag: "wx", mode: 0o600 });
    FS.writeFileSync(Path.join(root, "phases.jsonl"), "", { flag: "wx", mode: 0o600 });
    child = await runOwnedChild({ owner, executable: runtime.executablePath,
      args: [Path.join(Path.dirname(scriptPath), "jones-sqlite-v2-benchmark-worker.mjs")],
      env: { HOME: Path.join(root, "home"), TMPDIR: Path.join(root, "tmp"), TMP: Path.join(root, "tmp"), TEMP: Path.join(root, "tmp"), XDG_CACHE_HOME: Path.join(root, "cache"), NODE_ENV: "test", JONES_RUNTIME_BINDING: process.env.JONES_RUNTIME_BINDING },
      timeoutMs: 120000, terminateGraceMs: 10000, reapTimeoutMs: 5000, maxOutputBytes: 64 * 1024, signal });
    Assert.ok(child.closed && child.reaped && !child.truncated, "unknown child closure or truncated output");
    report = JSON.parse(child.stdout);
    Assert.equal(report.schema, "jones-sqlite-v2-benchmark/v1");
    Assert.equal(child.outcome, "success", report.error);
    Assert.equal(report.outcome, "passed");
    Assert.equal(report.databaseClosed, true);
    Assert.deepEqual(report.candidate, request.candidate);
    Assert.deepEqual(report.runtime, runtime);
    Assert.deepEqual(report.measurements.options, request.workload);
    assertCurrentCandidate(request.candidate);
  } catch (cause) { error = String(cause?.stack ?? cause).slice(0, 8192); }
  try {
    const phases = FS.readFileSync(Path.join(root, "phases.jsonl"));
    Assert.ok(phases.length <= 8192);
    FS.writeFileSync(evidencePath, `${JSON.stringify({ candidate: request.candidate, runtime, child, report, error, phases: phases.toString("utf8") })}\n`, { flag: "wx", mode: 0o600 });
    preserved = true;
  } catch (cause) { error ??= String(cause); }
  // Closure must be acknowledged by the database scope and the captured child before disposal.
  const cleanup = preserved && (!child || (child.closed && child.reaped && child.outcome !== "unknown" && report?.databaseClosed === true))
    ? disposeOwnedRoot(owner, { childReceipts: child ? [child] : [] })
    : { outcome: "retained", absent: false, reason: "database_or_child_closure_unknown" };
  if (preserved) FS.appendFileSync(evidencePath, `${JSON.stringify({ cleanup })}\n`);
  return { outcome: !error && cleanup.absent ? "passed" : "failed", evidencePath: preserved ? evidencePath : null, report, error, cleanup };
}

if (process.argv[1] && Path.resolve(process.argv[1]) === scriptPath) {
  if (process.argv.length === 2 || (process.argv.length === 3 && process.argv[2] === "--help")) {
    process.stdout.write(`${JSON.stringify({ schema: "jones-sqlite-v2-benchmark-metadata/v1", execution: "explicit --request <JSON-file>", caps: { commands: 64, payloadBytes: 4096, intervalMs: 100 }, workload: "single-thread-metadata-acceptance", sqlTiming: "unavailable" })}\n`);
  } else {
    Assert.ok(process.argv.length === 4 && process.argv[2] === "--request");
    const path = process.argv[3];
    const info = FS.lstatSync(path);
    Assert.ok(info.isFile() && !info.isSymbolicLink() && info.size <= 16384);
    const abort = new AbortController();
    const cancel = () => abort.abort();
    process.once("SIGINT", cancel);
    process.once("SIGTERM", cancel);
    try {
      const result = await runV2Benchmark(JSON.parse(FS.readFileSync(path, "utf8")), abort.signal);
      process.stdout.write(`${JSON.stringify(result)}\n`);
      process.exitCode = result.outcome === "passed" ? 0 : 1;
    } finally {
      process.removeListener("SIGINT", cancel);
      process.removeListener("SIGTERM", cancel);
    }
  }
}
