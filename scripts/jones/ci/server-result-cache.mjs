import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import {
  cachedTestReportPath,
  cachedTestTaskName,
  cachedTestTasks,
  readRunSummary,
  testResultDirectory,
} from "../../lib/cached-tests.ts";

const root = NodePath.resolve(import.meta.dirname, "../../..");
const server = NodePath.join(root, "apps/server");
const transferTest = "integration/transferBudgetV2.integration.test.ts";
const schema = "jones.server-result-cache-pilot/v1";
const digest = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");
const readJson = (file) => JSON.parse(NodeFS.readFileSync(file, "utf8"));

export function caseResults(report, projectRoot) {
  if (
    report.success !== true ||
    report.numFailedTests !== 0 ||
    !Array.isArray(report.testResults)
  ) {
    throw new Error("Expected a passing Vitest JSON report");
  }
  const files = new Map();
  for (const module of report.testResults) {
    const file = NodePath.relative(projectRoot, NodePath.resolve(projectRoot, module.name));
    if (
      !file ||
      file.startsWith("../") ||
      NodePath.isAbsolute(file) ||
      files.has(file) ||
      !Array.isArray(module.assertionResults)
    ) {
      throw new Error("Invalid or duplicate test module " + module.name);
    }
    const cases = module.assertionResults.map((test) => {
      if (
        !Array.isArray(test.ancestorTitles) ||
        !test.ancestorTitles.every((title) => typeof title === "string") ||
        typeof test.title !== "string" ||
        !["passed", "pending", "skipped", "todo", "disabled"].includes(test.status)
      ) {
        throw new Error("Invalid test case result in " + file);
      }
      return JSON.stringify([test.ancestorTitles, test.title, test.status]);
    });
    files.set(file, { cases: cases.length, caseDigest: digest(JSON.stringify(cases.sort())) });
  }
  if ([...files.values()].reduce((total, file) => total + file.cases, 0) !== report.numTotalTests) {
    throw new Error("Vitest report case totals do not reconcile");
  }
  return files;
}

function sameCases(left, right) {
  return left?.cases === right?.cases && left?.caseDigest === right?.caseDigest && left != null;
}

export function verifyCases(expected, results) {
  if (expected.size !== results.size) throw new Error("Test file counts do not match");
  for (const [file, cases] of expected) {
    if (!sameCases(cases, results.get(file))) throw new Error("Test cases differ in " + file);
  }
}

function identity(shard, env) {
  return {
    repository: env.GITHUB_REPOSITORY,
    commit: env.GITHUB_SHA,
    run: env.GITHUB_RUN_ID,
    attempt: env.GITHUB_RUN_ATTEMPT,
    shard,
    weights: env.JONES_SERVER_SHARD_WEIGHTS_DIGEST ?? "checked-in",
    image: env.ImageVersion,
    node: process.version,
  };
}

async function select(env) {
  const { shardedTestFiles } = await import("../../lib/sharded-tests.ts");
  const count = Number(env.JONES_RESULT_CACHE_SHARDS);
  if (!Number.isSafeInteger(count) || count < 1) throw new Error("Missing server shard count");
  for (let index = 1; index <= count; index += 1) {
    const shard = index + "/" + count;
    const files = await shardedTestFiles(server, shard);
    if (!files.includes(NodePath.join(server, transferTest))) continue;
    const state = identity(shard, env);
    const key = "jones-server-result-pilot-v1-" + digest(JSON.stringify(state));
    NodeFS.writeFileSync(
      NodePath.join(env.JONES_RESULT_CACHE_DIR, "selection.json"),
      JSON.stringify({ state, files: files.map((file) => NodePath.relative(server, file)).sort() }),
    );
    NodeFS.appendFileSync(
      env.GITHUB_OUTPUT,
      ["key=" + key, "shard=" + index, "index=" + (index - 1), ""].join("\n"),
    );
    return;
  }
  throw new Error("Transfer budget test was not assigned to a server shard");
}

function runTasks(args, env) {
  const result = NodeChildProcess.spawnSync(
    NodePath.join(root, "node_modules/.bin/vp"),
    ["run", ...args],
    {
      cwd: root,
      env,
      stdio: "inherit",
    },
  );
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error("Test task failed: " + (result.signal ?? result.status));
}

function reportFor(task, file) {
  const bytes = NodeFS.readFileSync(NodePath.join(server, cachedTestReportPath(task)));
  const cases = caseResults(JSON.parse(bytes), server);
  if (cases.size !== 1 || !cases.has(file)) throw new Error("Wrong test module for " + file);
  return { ...cases.get(file), reportSha256: digest(bytes) };
}

function measure(phase, env) {
  const work = env.JONES_RESULT_CACHE_DIR;
  const { state, files } = readJson(NodePath.join(work, "selection.json"));
  const producer = phase === "warm" ? readJson(NodePath.join(work, "cold/pilot.json")) : null;
  if (
    producer &&
    (producer.schema !== schema ||
      producer.phase !== "cold" ||
      JSON.stringify(producer.state) !== JSON.stringify(state))
  ) {
    throw new Error("Cold result does not belong to this pilot");
  }

  const reportDir = NodePath.join(server, testResultDirectory);
  NodeFS.mkdirSync(NodePath.dirname(reportDir), { recursive: true });
  NodeFS.mkdirSync(reportDir);
  try {
    const request = {
      run: NodeCrypto.randomUUID(),
      files: files.map((file) => "apps/server/" + file),
      groupSize: 1,
      uncached: ["apps/server/" + transferTest],
      reportResults: true,
    };
    const taskEnv = {
      ...env,
      T3_CACHED_TESTS: JSON.stringify(request),
      T3CODE_TRANSFER_BUDGET_REPORT_PATH: NodePath.join(work, "transfer.md"),
      T3CODE_TRANSFER_BUDGET_RESULT_PATH: NodePath.join(work, "transfer.json"),
    };
    const config = cachedTestTasks(taskEnv.T3_CACHED_TESTS, root);
    runTasks(["--concurrency-limit", "1", "test-cache"], taskEnv);
    const summary = readRunSummary(env.VITE_CACHE_PATH, request.run);
    if (!summary) throw new Error("Missing task summary for the current run");
    const summaries = new Map(summary.tasks.map((task) => [task.task_name, task]));
    if (summary.tasks.length !== summaries.size || summaries.size !== Object.keys(config).length) {
      throw new Error("Task summary does not cover the planned tasks exactly once");
    }

    const records = new Map();
    const hits = [];
    for (const file of files) {
      const task = cachedTestTaskName(["apps/server/" + file]);
      const observed = summaries.get(task);
      if (!observed || observed.command !== config[task]?.command) {
        throw new Error("Task summary does not match the plan for " + file);
      }
      const record = reportFor(task, file);
      const hit = observed.result.CacheHit !== undefined;
      if (hit) {
        if (
          file === transferTest ||
          observed.result.CacheHit.source !== "Local" ||
          producer?.files[file]?.reportSha256 !== record.reportSha256
        ) {
          throw new Error("Unverified result reused for " + file);
        }
        hits.push({ task, file });
      }
      records.set(file, { ...record, execution: hit ? "cached" : "fresh" });
    }
    readJson(taskEnv.T3CODE_TRANSFER_BUDGET_RESULT_PATH);
    NodeFS.readFileSync(taskEnv.T3CODE_TRANSFER_BUDGET_REPORT_PATH);

    let audit = null;
    if (producer) {
      verifyCases(new Map(Object.entries(producer.files)), records);
      const hit = hits[0];
      if (!hit) throw new Error("The restored cache produced no reusable results");
      NodeFS.rmSync(NodePath.join(server, cachedTestReportPath(hit.task)));
      runTasks(["--no-cache", "--remote-cache", "off", hit.task], taskEnv);
      if (!sameCases(records.get(hit.file), reportFor(hit.task, hit.file))) {
        throw new Error("Fresh audit disagreed with cache for " + hit.file);
      }
      audit = hit.file;
    }

    const elapsedMs = Date.now() - Number(env.JONES_RESULT_CACHE_STARTED_MS);
    if (!Number.isFinite(elapsedMs) || elapsedMs <= 0) throw new Error("Invalid pilot start time");
    const result = {
      schema,
      phase,
      state,
      elapsedMs,
      archiveRestored: producer !== null,
      cacheHits: hits.length,
      audit,
      files: Object.fromEntries(records),
    };
    NodeFS.writeFileSync(NodePath.join(work, "pilot.json"), JSON.stringify(result, null, 2) + "\n");
    NodeFS.appendFileSync(
      env.GITHUB_STEP_SUMMARY,
      "### Server result cache " +
        phase +
        " (" +
        state.shard +
        ")\n\n" +
        files.length +
        " files, " +
        hits.length +
        " verified cache hits, " +
        (elapsedMs / 1000).toFixed(1) +
        " s from before cache restore through evidence.\n\n" +
        (audit ? "Fresh hit audit passed for `" + audit + "`.\n" : ""),
    );
  } finally {
    NodeFS.rmSync(reportDir, { recursive: true, force: true });
  }
}

function compare(env) {
  const work = env.JONES_RESULT_CACHE_DIR;
  const warm = readJson(NodePath.join(work, "pilot.json"));
  const cold = readJson(NodePath.join(work, "cold/pilot.json"));
  const receipt = readJson(NodePath.join(work, "control/test-receipt.json"));
  const report = NodeFS.readFileSync(NodePath.join(work, "control/test-results.json"));
  const state = warm.state;
  if (
    receipt.schema !== "jones.ci-test-receipt/v1" ||
    receipt.job !== "test_server" ||
    receipt.outcome !== "success" ||
    receipt.commit !== state.commit ||
    receipt.repository !== state.repository ||
    receipt.run !== state.run ||
    receipt.attempt !== state.attempt ||
    receipt.node !== state.node ||
    (receipt.weightsDigest ?? "checked-in") !== state.weights ||
    !receipt.command.includes("--shard " + state.shard + " ") ||
    receipt.reportSha256 !== digest(report)
  ) {
    throw new Error("Fresh control does not belong to this pilot");
  }
  verifyCases(caseResults(JSON.parse(report), server), new Map(Object.entries(warm.files)));
  const [header, ...timings] = NodeFS.readFileSync(
    NodePath.join(work, "control/server-timings.ndjson"),
    "utf8",
  )
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line));
  const end = timings.at(-1);
  if (
    header.checkoutSha !== state.commit ||
    header.runId !== state.run ||
    header.runAttempt !== Number(state.attempt) ||
    [header.shardIndex, header.shardCount].join("/") !== state.shard ||
    end?.type !== "terminal" ||
    end.unhandledErrorCount !== 0 ||
    !Number.isFinite(end.elapsedMs) ||
    end.elapsedMs <= 0
  ) {
    throw new Error("Missing fresh control timings");
  }
  NodeFS.writeFileSync(
    NodePath.join(work, "comparison.json"),
    JSON.stringify({
      schema: "jones.server-result-cache-comparison/v1",
      state,
      controlReportSha256: receipt.reportSha256,
      files: Object.keys(warm.files).length,
      cacheHits: warm.cacheHits,
      freshAudit: warm.audit,
      controlMs: end.elapsedMs,
      coldMs: cold.elapsedMs,
      warmMs: warm.elapsedMs,
    }) + "\n",
  );
  NodeFS.appendFileSync(
    env.GITHUB_STEP_SUMMARY,
    "### Weighted shard comparison\n\n" +
      "The fresh control, cold cache run and warm cache run agree on every file and case outcome.\n\n" +
      "| Run | Seconds |\n| --- | ---: |\n" +
      "| Fresh control (Vitest reporter) | " +
      (end.elapsedMs / 1000).toFixed(1) +
      " |\n" +
      "| Cold cache (through evidence) | " +
      (cold.elapsedMs / 1000).toFixed(1) +
      " |\n" +
      "| Warm cache (restore through evidence) | " +
      (warm.elapsedMs / 1000).toFixed(1) +
      " |\n",
  );
}

async function main() {
  const mode = process.argv[2];
  const env = process.env;
  if (mode === "select") await select(env);
  else if (mode === "cold" || mode === "warm") measure(mode, env);
  else if (mode === "compare") compare(env);
  else throw new Error("Expected select, cold, warm or compare");
}

if (process.argv[1] && NodeURL.pathToFileURL(process.argv[1]).href === import.meta.url)
  await main();
