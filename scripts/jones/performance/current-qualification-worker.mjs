import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeCrypto from "node:crypto";
import * as NodePath from "node:path";
import * as NodeStream from "node:stream";
import * as NodeURL from "node:url";
import {
  assertCurrentCandidate,
  currentQualificationModules,
  currentQualificationUnavailable,
} from "./current-qualification.mjs";
import { readRuntimeBinding } from "./runtime-binding.mjs";

const root = NodeFS.realpathSync(process.cwd());
const phase = (label) =>
  NodeFS.appendFileSync(
    NodePath.join(root, "phases.jsonl"),
    `${JSON.stringify({ phase: label, pid: process.pid })}\n`,
  );
const results = [];
let runner,
  runnerClosed = false,
  failure,
  reason,
  candidate,
  runnerSha256,
  unhandled = [];
try {
  NodeAssert.match(NodePath.basename(root), /^v2-qualification-[a-f0-9-]{36}$/);
  phase("entered");
  const runtime = readRuntimeBinding();
  NodeAssert.equal(process.execPath, runtime.executablePath);
  NodeAssert.equal(process.versions.node, runtime.nodeVersion);
  const requestPath = NodePath.join(root, "request.json");
  const requestInfo = NodeFS.lstatSync(requestPath);
  NodeAssert.ok(requestInfo.isFile() && !requestInfo.isSymbolicLink() && requestInfo.size <= 16384);
  const request = JSON.parse(NodeFS.readFileSync(requestPath, "utf8"));
  NodeAssert.equal(request.root, root);
  for (const key of ["HOME", "TMPDIR", "TMP", "TEMP", "XDG_CACHE_HOME"])
    NodeAssert.ok(process.env[key]?.startsWith(`${root}${NodePath.sep}`));
  candidate = assertCurrentCandidate(request.candidate);
  NodeAssert.equal(
    candidate.worktreePath,
    NodePath.resolve(NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)), "../../.."),
  );
  phase("source-validated");
  const api = NodePath.join(candidate.worktreePath, "node_modules/vite-plus/dist/test/node.js");
  runnerSha256 = NodeCrypto.createHash("sha256").update(NodeFS.readFileSync(api)).digest("hex");
  const { startVitest } = await import(NodeURL.pathToFileURL(api).href);
  let logBytes = 0;
  const output = new NodeStream.Writable({
    write(chunk, _encoding, done) {
      logBytes += chunk.length;
      done(logBytes > 65536 ? new Error("runner log bound exceeded") : undefined);
    },
  });
  phase("runner-start");
  runner = await startVitest(
    "test",
    [...currentQualificationModules],
    {
      root: candidate.worktreePath,
      config: false,
      run: true,
      watch: false,
      api: false,
      ui: false,
      environment: "node",
      pool: "threads",
      maxWorkers: 1,
      fileParallelism: false,
      maxConcurrency: 1,
      isolate: true,
      allowOnly: false,
      passWithNoTests: false,
      include: [...currentQualificationModules],
      setupFiles: [
        NodePath.join(candidate.worktreePath, "packages/shared/src/testing/longTempDir.ts"),
        NodePath.join(candidate.worktreePath, "apps/server/src/testUtils/gitConfig.setup.ts"),
      ],
      testTimeout: 60000,
      hookTimeout: 60000,
      reporters: [
        {
          onInit(context) {
            runner = context;
          },
          onTestCaseResult(test) {
            results.push({
              module: NodePath.relative(candidate.worktreePath, test.module.moduleId),
              name: test.name,
              state: test.result().state,
            });
          },
          onTestRunEnd(_modules, errors, runReason) {
            reason = runReason;
            unhandled = errors.map(String);
          },
        },
      ],
    },
    {
      cacheDir: NodePath.join(root, "cache"),
      envDir: false,
      publicDir: false,
      server: { middlewareMode: true, ws: false, hmr: false, watch: null },
    },
    { stdout: output, stderr: output },
  );
  NodeAssert.ok(runner);
  NodeAssert.equal(reason, "passed");
  NodeAssert.equal(unhandled.length, 0);
  NodeAssert.ok(results.length > 0 && results.every((test) => test.state === "passed"));
  for (const file of currentQualificationModules)
    NodeAssert.ok(
      results.some((test) => test.module === file),
      `missing module ${file}`,
    );
  assertCurrentCandidate(candidate);
} catch (error) {
  failure = error;
} finally {
  if (runner) {
    try {
      await runner.close();
      runnerClosed = true;
      phase("runner-closed");
    } catch (error) {
      failure ??= error;
    }
  }
}
const report = {
  schema: "jones-current-v2-qualification/v1",
  outcome: failure ? "failed" : "passed",
  runnerClosed,
  candidate,
  runnerSha256,
  runtime: { executablePath: process.execPath, nodeVersion: process.versions.node },
  results,
  unavailable: currentQualificationUnavailable,
  error: failure?.message,
  productionQualification: "unverified",
  benchmarkQualification: "unverified",
};
process.stdout.write(`${JSON.stringify(report)}\n`);
process.exitCode = failure ? 1 : 0;
