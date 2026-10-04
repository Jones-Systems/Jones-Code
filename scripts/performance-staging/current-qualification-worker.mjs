import * as Assert from "node:assert/strict";
import * as FS from "node:fs";
import * as Crypto from "node:crypto";
import * as Path from "node:path";
import * as Stream from "node:stream";
import * as URL from "node:url";
import { assertCurrentCandidate, currentQualificationModules } from "./current-qualification.mjs";
import { readRuntimeBinding } from "./runtime-binding.mjs";

const root = FS.realpathSync(process.cwd());
const phase = (label) => FS.appendFileSync(Path.join(root, "phases.jsonl"), `${JSON.stringify({ phase: label, pid: process.pid })}\n`);
const results = [];
let runner, runnerClosed = false, failure, reason, candidate, runnerSha256, unhandled = [];
try {
  phase("entered");
  const runtime = readRuntimeBinding();
  Assert.equal(process.execPath, runtime.executablePath);
  Assert.equal(process.versions.node, runtime.nodeVersion);
  const request = JSON.parse(FS.readFileSync(Path.join(root, "request.json"), "utf8"));
  Assert.equal(request.root, root);
  for (const key of ["HOME", "TMPDIR", "TMP", "TEMP", "XDG_CACHE_HOME"]) Assert.ok(process.env[key]?.startsWith(`${root}${Path.sep}`));
  candidate = assertCurrentCandidate(request.candidate);
  phase("source-validated");
  const api = Path.join(candidate.worktreePath, "node_modules/vite-plus/dist/test/node.js");
  runnerSha256 = Crypto.createHash("sha256").update(FS.readFileSync(api)).digest("hex");
  const { startVitest } = await import(URL.pathToFileURL(api).href);
  let logBytes = 0;
  const output = new Stream.Writable({ write(chunk, _encoding, done) { logBytes += chunk.length; done(logBytes > 65536 ? new Error("runner log bound exceeded") : undefined); } });
  phase("runner-start");
  runner = await startVitest("test", [...currentQualificationModules], {
    root: candidate.worktreePath, config: false, run: true, watch: false, api: false, ui: false,
    environment: "node", pool: "threads", maxWorkers: 1, fileParallelism: false, maxConcurrency: 1,
    isolate: true, allowOnly: false, passWithNoTests: false, include: [...currentQualificationModules],
    setupFiles: [Path.join(candidate.worktreePath, "packages/shared/src/testing/longTempDir.ts"), Path.join(candidate.worktreePath, "apps/server/src/testUtils/gitConfig.setup.ts")],
    testTimeout: 60000, hookTimeout: 60000,
    reporters: [{ onInit(context) { runner = context; }, onTestCaseResult(test) { results.push({ module: Path.relative(candidate.worktreePath, test.module.moduleId), name: test.name, state: test.result().state }); }, onTestRunEnd(_modules, errors, runReason) { reason = runReason; unhandled = errors.map(String); } }],
  }, { cacheDir: Path.join(root, "cache"), envDir: false, publicDir: false, server: { middlewareMode: true, ws: false, hmr: false, watch: null } }, { stdout: output, stderr: output });
  Assert.ok(runner);
  Assert.equal(reason, "passed");
  Assert.equal(unhandled.length, 0);
  Assert.ok(results.length > 0 && results.every((test) => test.state === "passed"));
  for (const file of currentQualificationModules) Assert.ok(results.some((test) => test.module === file), `missing module ${file}`);
  assertCurrentCandidate(candidate);
} catch (error) { failure = error; }
finally {
  if (runner) { try { await runner.close(); runnerClosed = true; phase("runner-closed"); } catch (error) { failure ??= error; } }
}
const report = { schema: "jones-current-v2-qualification/v1", outcome: failure ? "failed" : "passed", runnerClosed, candidate, runnerSha256, runtime: { executablePath: process.execPath, nodeVersion: process.versions.node }, results, error: failure?.message, productionQualification: "unverified", benchmarkQualification: "unverified" };
process.stdout.write(`${JSON.stringify(report)}\n`);
process.exitCode = failure ? 1 : 0;
