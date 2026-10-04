import * as Assert from "node:assert/strict";
import * as Child from "node:child_process";
import * as Crypto from "node:crypto";
import * as FS from "node:fs";
import * as Path from "node:path";
import * as URL from "node:url";
import { readRuntimeBinding } from "./runtime-binding.mjs";
import { createOwnedRoot, disposeOwnedRoot } from "./guard.mjs";
import { runOwnedChild } from "./lifecycle.mjs";

export const currentQualificationModules = Object.freeze([
  "apps/server/src/persistence/initializeV2Database.test.ts",
  "apps/server/src/persistence/reconcileV2PreviewMigration.test.ts",
  "apps/server/src/orchestration-v2/ProjectionStore.test.ts",
  "apps/server/src/orchestration-v2/StoreNativeAcceptance.integration.test.ts",
]);

export function assertCurrentCandidate(candidate) {
  Assert.equal(candidate.repository, "Jones-Systems/Jones-Code");
  Assert.match(candidate.sourceRevision, /^[a-f0-9]{40}$/);
  Assert.match(candidate.tree, /^[a-f0-9]{40}$/);
  Assert.match(candidate.lockSha256, /^[a-f0-9]{64}$/);
  Assert.equal(Path.resolve(candidate.worktreePath), candidate.worktreePath);
  Assert.equal(FS.realpathSync(candidate.worktreePath), candidate.worktreePath);
  const git = (...args) => Child.execFileSync("git", ["-c", "core.fsmonitor=false", "-C", candidate.worktreePath, ...args], {
    encoding: "utf8", maxBuffer: 256 * 1024,
    env: { PATH: process.env.PATH ?? "/usr/bin:/bin", GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null", GIT_OPTIONAL_LOCKS: "0" },
  }).trim();
  Assert.equal(git("rev-parse", "--show-toplevel"), candidate.worktreePath);
  Assert.equal(git("rev-parse", "HEAD"), candidate.sourceRevision);
  Assert.equal(git("rev-parse", "HEAD^{tree}"), candidate.tree);
  Assert.equal(git("status", "--porcelain", "--untracked-files=all"), "", "candidate must be clean");
  Assert.equal(Crypto.createHash("sha256").update(FS.readFileSync(Path.join(candidate.worktreePath, "pnpm-lock.yaml"))).digest("hex"), candidate.lockSha256);
  for (const file of currentQualificationModules) Assert.ok(FS.statSync(Path.join(candidate.worktreePath, file)).isFile());
  return candidate;
}

export async function runCurrentQualification({ candidate, parentPath, binding, policy, signal }) {
  assertCurrentCandidate(candidate);
  const runtime = readRuntimeBinding();
  const owner = createOwnedRoot({ parentPath, childName: `v2-qualification-${Crypto.randomUUID()}`, binding, policy });
  const root = owner.creationReceipt.canonicalRootPath;
  const evidencePath = `${root}.json`;
  let child, report, failure, preserved = false;
  try {
    for (const relative of ["home", "tmp", "cache"]) FS.mkdirSync(Path.join(root, relative), { mode: 0o700 });
    FS.writeFileSync(Path.join(root, "phases.jsonl"), "", { flag: "wx", mode: 0o600 });
    FS.writeFileSync(Path.join(root, "request.json"), JSON.stringify({ candidate, root }), { flag: "wx", mode: 0o600 });
    child = await runOwnedChild({ owner, executable: runtime.executablePath,
      args: [URL.fileURLToPath(new URL("./current-qualification-worker.mjs", import.meta.url))],
      env: { HOME: Path.join(root, "home"), TMPDIR: Path.join(root, "tmp"), TMP: Path.join(root, "tmp"), TEMP: Path.join(root, "tmp"), XDG_CACHE_HOME: Path.join(root, "cache"), NODE_ENV: "test", JONES_RUNTIME_BINDING: process.env.JONES_RUNTIME_BINDING },
      timeoutMs: 120000, terminateGraceMs: 10000, reapTimeoutMs: 5000, maxOutputBytes: 128 * 1024, signal });
    Assert.ok(child.closed && child.reaped && !child.truncated, "unknown child closure or truncated report");
    report = JSON.parse(child.stdout);
    Assert.equal(report.schema, "jones-current-v2-qualification/v1");
    Assert.equal(report.outcome, "passed", report.error);
    Assert.deepEqual(report.candidate, candidate);
    Assert.equal(report.runtime.executablePath, runtime.executablePath);
    Assert.equal(report.runtime.nodeVersion, runtime.nodeVersion);
    Assert.equal(child.outcome, "success");
    assertCurrentCandidate(candidate);
  } catch (error) { failure = error; }
  const evidence = { candidate, runtime, child, report, error: failure?.message, productionQualification: "unverified", benchmarkQualification: "unverified" };
  try {
    const journal = FS.readFileSync(Path.join(root, "phases.jsonl"));
    Assert.ok(journal.length <= 8192);
    evidence.phases = journal.toString("utf8");
    FS.writeFileSync(evidencePath, JSON.stringify(evidence), { flag: "wx", mode: 0o600 });
    preserved = true;
  } catch (error) { failure ??= error; }
  // A crashed runner may retain workers; a captured leaf alone cannot acknowledge runner closure.
  const cleanup = preserved && (!child || (child.closed && child.reaped && child.outcome !== "unknown" && report?.runnerClosed === true))
    ? disposeOwnedRoot(owner, { childReceipts: child ? [child] : [] })
    : { outcome: "retained", absent: false, reason: "unknown_runner_close_or_evidence" };
  if (preserved) FS.appendFileSync(evidencePath, `\n${JSON.stringify({ cleanup })}\n`);
  return { outcome: !failure && cleanup.absent ? "passed" : "failed", evidencePath: preserved ? evidencePath : null, cleanup, report, error: failure?.message };
}
