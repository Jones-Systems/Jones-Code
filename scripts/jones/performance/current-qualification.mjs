import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { assertCurrentDatabaseSource } from "./sources.mjs";
import { readRuntimeBinding } from "./runtime-binding.mjs";
import { createOwnedRoot, disposeOwnedRoot } from "./guard.mjs";
import { runOwnedChild } from "./lifecycle.mjs";

export const currentQualificationModules = Object.freeze([
  "apps/server/src/persistence/initializeV2Database.test.ts",
  "apps/server/src/persistence/reconcileV2PreviewMigration.test.ts",
  "apps/server/src/orchestration-v2/ProjectionStore.test.ts",
]);

const scriptPath = NodeURL.fileURLToPath(import.meta.url);
const worktreePath = NodePath.resolve(NodePath.dirname(scriptPath), "../../..");
export const currentQualificationUnavailable = Object.freeze([
  "StoreNativeAcceptance.integration.test.ts: unavailable until #148 source is bound",
  "installed runtime qualification",
  "historical source qualification",
]);

export function currentQualificationMetadata() {
  return {
    schema: "jones-current-v2-qualification/v1",
    outcome: "unavailable",
    reason: "explicit candidate/runtime binding required",
    execution: "explicit --request <JSON-file>",
    modules: currentQualificationModules,
    unavailable: currentQualificationUnavailable,
  };
}

export function validateCurrentQualificationRequest(request) {
  NodeAssert.deepEqual(Object.keys(request).sort(), ["candidate", "parentPath"]);
  NodeAssert.equal(request.candidate.worktreePath, worktreePath, "run the harness from the bound candidate checkout");
  NodeAssert.equal(NodePath.resolve(request.parentPath), request.parentPath);
  return request;
}

export function currentQualificationClosureKnown(child, report, evidencePreserved) {
  return evidencePreserved && (!child || (child.closed === true && child.reaped === true &&
    ["success", "failed", "timed_out", "cancelled", "output_limited", "spawn_refused"].includes(child.outcome) && report?.runnerClosed === true));
}

export function assertCurrentCandidate(candidate) {
  assertCurrentDatabaseSource(candidate);
  NodeAssert.equal(candidate.worktreePath, worktreePath, "run the harness from the bound candidate checkout");
  for (const file of currentQualificationModules) NodeAssert.ok(NodeFS.statSync(NodePath.join(candidate.worktreePath, file)).isFile());
  return candidate;
}

export async function runCurrentQualification(input, signal) {
  const { candidate, parentPath } = validateCurrentQualificationRequest(input);
  assertCurrentCandidate(candidate);
  const binding = { repository: candidate.repository, sourceRevision: candidate.sourceRevision,
    taskRef: "jones-current-v2-qualification", runId: NodeCrypto.randomUUID() };
  const policy = { homePath: NodeOS.homedir(), worktreePaths: [worktreePath],
    protectedPaths: [".t3", ".codex", ".ssh", ".config"].map((name) => NodePath.join(NodeOS.homedir(), name)) };
  const runtime = readRuntimeBinding();
  const owner = createOwnedRoot({ parentPath, childName: `v2-qualification-${NodeCrypto.randomUUID()}`, binding, policy });
  const root = owner.creationReceipt.canonicalRootPath;
  const evidencePath = `${root}.json`;
  let child, report, failure, preserved = false;
  try {
    for (const relative of ["home", "tmp", "cache"]) NodeFS.mkdirSync(NodePath.join(root, relative), { mode: 0o700 });
    NodeFS.writeFileSync(NodePath.join(root, "phases.jsonl"), "", { flag: "wx", mode: 0o600 });
    NodeFS.writeFileSync(NodePath.join(root, "request.json"), JSON.stringify({ candidate, root }), { flag: "wx", mode: 0o600 });
    child = await runOwnedChild({ owner, executable: runtime.executablePath,
      args: [NodeURL.fileURLToPath(new NodeURL.URL("./current-qualification-worker.mjs", import.meta.url))],
      env: { HOME: NodePath.join(root, "home"), TMPDIR: NodePath.join(root, "tmp"), TMP: NodePath.join(root, "tmp"), TEMP: NodePath.join(root, "tmp"), XDG_CACHE_HOME: NodePath.join(root, "cache"), NODE_ENV: "test", JONES_RUNTIME_BINDING: process.env.JONES_RUNTIME_BINDING },
      timeoutMs: 120000, terminateGraceMs: 10000, reapTimeoutMs: 5000, maxOutputBytes: 128 * 1024, signal });
    NodeAssert.ok(child.closed && child.reaped && !child.truncated, "unknown child closure or truncated report");
    report = JSON.parse(child.stdout);
    NodeAssert.equal(report.schema, "jones-current-v2-qualification/v1");
    NodeAssert.equal(report.outcome, "passed", report.error);
    NodeAssert.equal(report.runnerClosed, true);
    NodeAssert.deepEqual(report.unavailable, currentQualificationUnavailable);
    NodeAssert.deepEqual(report.candidate, candidate);
    NodeAssert.equal(report.runtime.executablePath, runtime.executablePath);
    NodeAssert.equal(report.runtime.nodeVersion, runtime.nodeVersion);
    NodeAssert.equal(child.outcome, "success");
    assertCurrentCandidate(candidate);
  } catch (error) { failure = error; }
  const evidence = { candidate, runtime, child, report, error: failure?.message, productionQualification: "unverified", benchmarkQualification: "unverified" };
  try {
    const journal = NodeFS.readFileSync(NodePath.join(root, "phases.jsonl"));
    NodeAssert.ok(journal.length <= 8192);
    evidence.phases = journal.toString("utf8");
    NodeFS.writeFileSync(evidencePath, JSON.stringify(evidence), { flag: "wx", mode: 0o600 });
    preserved = true;
  } catch (error) { failure ??= error; }
  // A crashed runner may retain workers; a captured leaf alone cannot acknowledge runner closure.
  const cleanup = currentQualificationClosureKnown(child, report, preserved)
    ? disposeOwnedRoot(owner, { childReceipts: child ? [child] : [] })
    : { outcome: "retained", absent: false, reason: "unknown_runner_close_or_evidence" };
  if (preserved) NodeFS.appendFileSync(evidencePath, `\n${JSON.stringify({ cleanup })}\n`);
  return { outcome: !failure && cleanup.absent ? "passed" : "failed", evidencePath: preserved ? evidencePath : null, cleanup, report, error: failure?.message };
}

if (process.argv[1] && NodePath.resolve(process.argv[1]) === scriptPath) {
  if (process.argv.length === 2 || (process.argv.length === 3 && process.argv[2] === "--help")) {
    process.stdout.write(`${JSON.stringify(currentQualificationMetadata())}\n`);
  } else {
    NodeAssert.ok(process.argv.length === 4 && process.argv[2] === "--request");
    const requestPath = process.argv[3];
    const info = NodeFS.lstatSync(requestPath);
    NodeAssert.ok(info.isFile() && !info.isSymbolicLink() && info.size <= 16384);
    const abort = new AbortController();
    const cancel = () => abort.abort();
    process.once("SIGINT", cancel);
    process.once("SIGTERM", cancel);
    try {
      const result = await runCurrentQualification(JSON.parse(NodeFS.readFileSync(requestPath, "utf8")), abort.signal);
      process.stdout.write(`${JSON.stringify(result)}\n`);
      process.exitCode = result.outcome === "passed" ? 0 : 1;
    } finally {
      process.removeListener("SIGINT", cancel);
      process.removeListener("SIGTERM", cancel);
    }
  }
}
