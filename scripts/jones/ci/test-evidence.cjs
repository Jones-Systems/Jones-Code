const fs = require("node:fs");
const path = require("node:path");
const { createHash } = require("node:crypto");
const { execFileSync } = require("node:child_process");

function receipt(report, context) {
  if (context.outcome === "success" && !report)
    throw new Error("Passing tests must produce a case report");
  const parsed = report ? JSON.parse(report) : null;
  return {
    schema: "jones.ci-test-receipt/v1",
    ...context,
    reportSha256: report ? createHash("sha256").update(report).digest("hex") : null,
    totalTests: parsed?.numTotalTests ?? null,
    passedTests: parsed?.numPassedTests ?? null,
    failedTests: parsed?.numFailedTests ?? null,
    pendingTests: parsed?.numPendingTests ?? null,
  };
}

function main() {
  const reportPath = path.join(process.env.RUNNER_TEMP, "test-results.json");
  const receiptPath = path.join(process.env.RUNNER_TEMP, "test-receipt.json");
  if (process.argv[2] === "cleanup") {
    fs.rmSync(reportPath, { force: true });
    fs.rmSync(receiptPath, { force: true });
    return;
  }
  const report = fs.existsSync(reportPath) ? fs.readFileSync(reportPath, "utf8") : null;
  const context = {
    repository: process.env.GITHUB_REPOSITORY,
    commit: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
    eventCommit: process.env.GITHUB_SHA,
    run: process.env.GITHUB_RUN_ID,
    attempt: process.env.GITHUB_RUN_ATTEMPT,
    job: process.env.GITHUB_JOB,
    command: process.env.TEST_COMMAND,
    outcome: process.env.TEST_OUTCOME,
    node: process.version,
    // oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone CI receipt records the executing runner's actual platform without loading application services.
    platform: process.platform,
    // oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone CI receipt records the executing Node process architecture without loading application services.
    architecture: process.arch,
    weightsSourceRun: process.env.JONES_SERVER_SHARD_WEIGHTS_SOURCE_RUN_ID ?? null,
    weightsDigest: process.env.JONES_SERVER_SHARD_WEIGHTS_DIGEST ?? null,
  };
  const result = receipt(report, context);
  fs.writeFileSync(receiptPath, JSON.stringify(result, null, 2) + "\n");
  console.log(JSON.stringify(result));
}

module.exports = { receipt };
if (require.main === module) main();
