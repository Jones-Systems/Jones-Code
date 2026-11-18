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
const alwaysFresh = [
  transferTest,
  // Automatic input tracing injects variables into the worker environment.
  "src/jones/hostService/systemServiceControl.test.ts",
];
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

export function pilotCacheIdentity(shard, env) {
  return {
    repository: env.GITHUB_REPOSITORY,
    commit: env.GITHUB_SHA,
    run: env.GITHUB_RUN_ID,
    attempt: env.GITHUB_RUN_ATTEMPT,
    shard,
    weights: env.JONES_SERVER_SHARD_WEIGHTS_DIGEST ?? "checked-in",
    // Hosted runners can roll image revisions during the same run. The fresh
    // control and audit compare outcomes across matching platforms.
    os: env.RUNNER_OS,
    architecture: env.RUNNER_ARCH,
    imageFamily: env.ImageOS,
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
    const state = pilotCacheIdentity(shard, env);
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
  const report = JSON.parse(bytes);
  const cases = caseResults(report, server);
  if (cases.size !== 1 || !cases.has(file)) throw new Error("Wrong test module for " + file);
  return { ...cases.get(file), reportSha256: digest(bytes), report };
}

function taskPlan(files, env, work) {
  const request = {
    run: NodeCrypto.randomUUID(),
    files: files.map((file) => "apps/server/" + file),
    groupSize: 1,
    uncached: alwaysFresh.map((file) => "apps/server/" + file),
    reportResults: true,
  };
  const taskEnv = {
    ...env,
    T3_CACHED_TESTS: JSON.stringify(request),
    T3CODE_TRANSFER_BUDGET_REPORT_PATH: NodePath.join(work, "transfer.md"),
    T3CODE_TRANSFER_BUDGET_RESULT_PATH: NodePath.join(work, "transfer.json"),
  };
  return { request, taskEnv };
}

function runWithReports(operation) {
  const reportDir = NodePath.join(server, testResultDirectory);
  NodeFS.mkdirSync(NodePath.dirname(reportDir), { recursive: true });
  NodeFS.mkdirSync(reportDir);
  try {
    operation();
  } finally {
    NodeFS.rmSync(reportDir, { recursive: true, force: true });
  }
}

function collectResults(files, request, taskEnv, verifyHit, onReport = () => {}) {
  const config = cachedTestTasks(taskEnv.T3_CACHED_TESTS, root);
  runTasks(["--concurrency-limit", "1", "test-cache"], taskEnv);
  const summary = readRunSummary(taskEnv.VITE_CACHE_PATH, request.run);
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
    const { report, ...record } = reportFor(task, file);
    const hit = observed.result.CacheHit !== undefined;
    if (hit) {
      if (
        request.uncached.includes("apps/server/" + file) ||
        observed.result.CacheHit.source !== "Local"
      ) {
        throw new Error("Unexpected result reused for " + file);
      }
      verifyHit(file, record);
      hits.push({ task, file });
    }
    records.set(file, { ...record, execution: hit ? "cached" : "fresh" });
    onReport(report);
  }
  return { records, hits };
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

  const { request, taskEnv } = taskPlan(files, env, work);
  runWithReports(() => {
    const { records, hits } = collectResults(files, request, taskEnv, (file, record) => {
      if (producer?.files[file]?.reportSha256 !== record.reportSha256) {
        throw new Error("Unverified result reused for " + file);
      }
    });
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
      runnerImageVersion: env.ImageVersion ?? null,
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
  });
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
      runnerImageVersions: {
        control: header.runner.imageVersion,
        cold: cold.runnerImageVersion,
        warm: warm.runnerImageVersion,
      },
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

const rolloutSchema = "jones.server-result-cache/v1";
const sha256 = /^[a-f0-9]{64}$/;
const commitSha = /^[a-f0-9]{40}$/;
const positiveInteger = /^[1-9][0-9]*$/;

export function rolloutCacheKeys(shard, fingerprint, env) {
  const {
    GITHUB_REPOSITORY,
    GITHUB_REF,
    GITHUB_RUN_ID,
    GITHUB_RUN_ATTEMPT,
    RUNNER_OS,
    RUNNER_ARCH,
    ImageVersion,
  } = env;
  const pr = /^refs\/pull\/([1-9][0-9]*)\/merge$/.exec(GITHUB_REF)?.[1];
  if (
    GITHUB_REPOSITORY !== "Jones-Systems/Jones-Code" ||
    !pr ||
    RUNNER_OS !== "Linux" ||
    !RUNNER_ARCH ||
    !ImageVersion ||
    !positiveInteger.test(GITHUB_RUN_ID ?? "") ||
    !positiveInteger.test(GITHUB_RUN_ATTEMPT ?? "") ||
    !/^\d+\/\d+$/.test(shard) ||
    !sha256.test(fingerprint)
  ) {
    throw new Error("Missing pull request cache identity");
  }
  const prefix =
    "jones-server-results-v1-pr-" +
    pr +
    "-" +
    digest(
      JSON.stringify([shard, fingerprint, RUNNER_OS, RUNNER_ARCH, ImageVersion, process.version]),
    ) +
    "-";
  return { prefix, key: prefix + GITHUB_RUN_ID + "-" + GITHUB_RUN_ATTEMPT };
}

function fingerprintInputs() {
  const files = [
    "pnpm-lock.yaml",
    "pnpm-workspace.yaml",
    ".github/workflows/ci.yml",
    "vite.config.ts",
    "apps/server/vite.config.ts",
    "scripts/lib/cached-tests.ts",
    "scripts/lib/sharded-tests.ts",
    "scripts/jones/ci/server-result-cache.mjs",
  ];
  return digest(
    JSON.stringify(files.map((file) => digest(NodeFS.readFileSync(NodePath.join(root, file))))),
  );
}

async function selectRollout(env) {
  const shard = env.JONES_RESULT_CACHE_SHARD;
  const { shardedTestFiles } = await import("../../lib/sharded-tests.ts");
  const files = (await shardedTestFiles(server, shard))
    .map((file) => NodePath.relative(server, file))
    .sort();
  if (files.length === 0 || new Set(files).size !== files.length) {
    throw new Error("Expected a nonempty shard without duplicate files");
  }
  const keys = rolloutCacheKeys(shard, fingerprintInputs(), env);
  NodeFS.writeFileSync(
    NodePath.join(env.JONES_RESULT_CACHE_DIR, "selection.json"),
    JSON.stringify({ ...keys, shard, files }),
  );
  NodeFS.appendFileSync(
    env.GITHUB_OUTPUT,
    `prefix=${keys.prefix}
key=${keys.key}
`,
  );
}

export function validateArchive(archive, matchedKey, prefix) {
  if (
    archive?.schema !== rolloutSchema ||
    archive.key !== matchedKey ||
    !matchedKey?.startsWith(prefix) ||
    !archive.files ||
    typeof archive.files !== "object" ||
    Array.isArray(archive.files)
  ) {
    throw new Error("Restored archive has no matching receipt");
  }
  for (const record of Object.values(archive.files)) {
    if (
      !sha256.test(record?.reportSha256 ?? "") ||
      !commitSha.test(record?.producer?.commit ?? "") ||
      !positiveInteger.test(record?.producer?.run ?? "") ||
      !positiveInteger.test(record?.producer?.attempt ?? "")
    )
      throw new Error("Malformed result producer");
  }
  return new Map(Object.entries(archive.files));
}

export function combineCaseReports(reports) {
  const totals = Object.fromEntries(
    [
      "numTotalTestSuites",
      "numPassedTestSuites",
      "numFailedTestSuites",
      "numPendingTestSuites",
      "numTotalTests",
      "numPassedTests",
      "numFailedTests",
      "numPendingTests",
      "numTodoTests",
    ].map((key) => [key, reports.reduce((total, report) => total + report[key], 0)]),
  );
  if (!Object.values(totals).every(Number.isSafeInteger)) {
    throw new Error("Incomplete Vitest totals");
  }
  return { ...totals, success: true, testResults: reports.flatMap((report) => report.testResults) };
}

function executeRollout(env) {
  const started = Date.now();
  const work = env.JONES_RESULT_CACHE_DIR;
  const { key, prefix, shard, files } = readJson(NodePath.join(work, "selection.json"));
  const archivePath = NodePath.join(env.VITE_CACHE_PATH, "server-result-receipt.json");
  const restoredKey = env.JONES_RESULT_CACHE_RESTORED_KEY;
  const restored = restoredKey
    ? validateArchive(readJson(archivePath), restoredKey, prefix)
    : new Map();
  const producer = {
    commit: env.GITHUB_SHA,
    run: env.GITHUB_RUN_ID,
    attempt: env.GITHUB_RUN_ATTEMPT,
  };
  const { request, taskEnv } = taskPlan(files, env, work);
  runWithReports(() => {
    const reports = [];
    const { records, hits } = collectResults(
      files,
      request,
      taskEnv,
      (file, record) => {
        if (restored.get(file)?.reportSha256 !== record.reportSha256) {
          throw new Error("Unverified result reused for " + file);
        }
      },
      (report) => reports.push(report),
    );
    // Rotate the audit as the PR changes while keeping selection reproducible.
    const audit = hits[Number.parseInt(producer.commit.slice(0, 8), 16) % hits.length];
    if (audit) {
      NodeFS.rmSync(NodePath.join(server, cachedTestReportPath(audit.task)));
      runTasks(["--no-cache", "--remote-cache", "off", audit.task], taskEnv);
      if (!sameCases(records.get(audit.file), reportFor(audit.task, audit.file))) {
        throw new Error("Fresh audit disagreed with cache for " + audit.file);
      }
    }
    if (records.has(transferTest)) {
      readJson(taskEnv.T3CODE_TRANSFER_BUDGET_RESULT_PATH);
      NodeFS.copyFileSync(
        taskEnv.T3CODE_TRANSFER_BUDGET_RESULT_PATH,
        env.T3CODE_TRANSFER_BUDGET_RESULT_PATH,
      );
      NodeFS.copyFileSync(
        taskEnv.T3CODE_TRANSFER_BUDGET_REPORT_PATH,
        env.T3CODE_TRANSFER_BUDGET_REPORT_PATH,
      );
    }
    const report = combineCaseReports(reports);
    verifyCases(records, caseResults(report, server));
    const evidence =
      JSON.stringify({
        schema: rolloutSchema,
        key,
        restoredKey: restoredKey || null,
        ...producer,
        weightsDigest: env.JONES_SERVER_SHARD_WEIGHTS_DIGEST ?? null,
        shard,
        elapsedMs: Date.now() - started,
        audit: audit?.file ?? null,
        cacheHits: hits.length,
        files: Object.fromEntries(
          Array.from(records, ([file, record]) => [
            file,
            {
              ...record,
              producer: record.execution === "cached" ? restored.get(file).producer : producer,
            },
          ]),
        ),
      }) + "\n";
    NodeFS.writeFileSync(
      NodePath.join(env.RUNNER_TEMP, "test-results.json"),
      JSON.stringify(report) + "\n",
    );
    NodeFS.writeFileSync(NodePath.join(env.RUNNER_TEMP, "server-result-cache.json"), evidence);
    const hasNewResults = [...records].some(
      ([file, record]) => record.execution === "fresh" && !alwaysFresh.includes(file),
    );
    if (hasNewResults) {
      NodeFS.writeFileSync(archivePath, evidence);
      NodeFS.appendFileSync(env.GITHUB_OUTPUT, "save_cache=true\n");
    }
    NodeFS.appendFileSync(
      env.GITHUB_STEP_SUMMARY,
      `### Server result cache (${shard})

${files.length} files, ${hits.length} reused, ${report.numTotalTests} cases.

` +
        (audit
          ? `Fresh hit audit passed for \`${audit.file}\`.
`
          : ""),
    );
  });
}

async function main() {
  const mode = process.argv[2];
  const env = process.env;
  if (mode === "select") await select(env);
  else if (mode === "cold" || mode === "warm") measure(mode, env);
  else if (mode === "compare") compare(env);
  else if (mode === "select-rollout") await selectRollout(env);
  else if (mode === "run-rollout") executeRollout(env);
  else throw new Error("Expected select, cold, warm, compare, select-rollout or run-rollout");
}

if (process.argv[1] && NodeURL.pathToFileURL(process.argv[1]).href === import.meta.url)
  await main();
