const assert = require("node:assert/strict");
const { readFileSync } = require("node:fs");
const { test } = require("node:test");
const { failures, LINUX_GATES, NATIVE_GATES, REQUIRED_JOBS } = require("./check-results.cjs");
const { receipt } = require("./test-evidence.cjs");

const green = () => {
  const results = Object.fromEntries(
    REQUIRED_JOBS.map((job) => [job, { result: "success", outputs: {} }]),
  );
  results.linux_changes.outputs = Object.fromEntries(
    Object.values(LINUX_GATES).map((key) => [key, "true"]),
  );
  results.native_changes.outputs = Object.fromEntries(
    Object.values(NATIVE_GATES).map((key) => [key, "true"]),
  );
  return results;
};

test("Check accepts green runs and only classifier-authorized skips across the entire server chain", () => {
  const results = green();
  assert.deepEqual(failures(results), []);
  for (const [job, output] of Object.entries(LINUX_GATES)) {
    results[job].result = "skipped";
    results.linux_changes.outputs[output] = "false";
  }
  for (const [job, output] of Object.entries(NATIVE_GATES)) {
    results[job].result = "skipped";
    results.native_changes.outputs[output] = "false";
  }
  assert.deepEqual(failures(results), []);
});

test("Check rejects every failed, cancelled, unknown or unexplained skipped job", () => {
  for (const name of REQUIRED_JOBS) {
    for (const result of ["failure", "cancelled", "skipped", "unknown", undefined]) {
      const results = green();
      results[name].result = result;
      assert.ok(failures(results).includes(name), `${name}: ${result}`);
    }
    const results = green();
    delete results[name];
    assert.ok(failures(results).includes(name), `missing ${name}`);
  }
  assert.ok(failures({ ...green(), unexpected: { result: "skipped" } }).includes("unexpected"));
});

test("an explicit false is insufficient when its classifier fails or is absent", () => {
  for (const [classifier, gates] of [
    ["linux_changes", LINUX_GATES],
    ["native_changes", NATIVE_GATES],
  ]) {
    for (const [job, output] of Object.entries(gates)) {
      for (const result of ["failure", "cancelled", "skipped"]) {
        const results = green();
        results[job].result = "skipped";
        results[classifier] = { result, outputs: { [output]: "false" } };
        assert.ok(failures(results).includes(job));
      }
    }
  }
});

test("missing or malformed classifier decisions fail Check even when fallback jobs succeeded", () => {
  for (const [classifier, gates] of [
    ["linux_changes", LINUX_GATES],
    ["native_changes", NATIVE_GATES],
  ]) {
    for (const output of new Set(Object.values(gates))) {
      for (const value of [undefined, "", true, false, "unknown"]) {
        const results = green();
        results[classifier].outputs[output] = value;
        assert.ok(failures(results).includes(classifier), `${classifier}.${output}: ${value}`);
      }
    }
  }
});

test("workflow enrolls every job in Check and preserves conditional and always-run boundaries", () => {
  const workflow = readFileSync(
    new URL("../../../.github/workflows/ci.yml", `file://${__filename}`),
    "utf8",
  );
  const jobs = Object.fromEntries(
    [...workflow.matchAll(/^  ([\w-]+):\n([\s\S]*?)(?=^  [\w-]+:\n|$(?![\s\S]))/gm)]
      .filter(
        ([, name]) => name !== "pull_request" && name !== "push" && name !== "workflow_dispatch",
      )
      .map(([, name, body]) => [name, body]),
  );
  assert.deepEqual(
    Object.keys(jobs)
      .filter((name) => name !== "check")
      .sort(),
    [...REQUIRED_JOBS].sort(),
  );
  const needed = jobs.check
    .match(/needs:\s*\[([\s\S]*?)\]/)[1]
    .split(",")
    .map((name) => name.trim())
    .filter(Boolean);
  assert.deepEqual(needed.sort(), [...REQUIRED_JOBS].sort());
  for (const [job, output] of Object.entries(LINUX_GATES))
    assert.ok(jobs[job].includes(`needs.linux_changes.outputs.${output} != 'false'`), job);
  for (const [job, output] of Object.entries(NATIVE_GATES))
    assert.ok(jobs[job].includes(`needs.native_changes.outputs.${output} != 'false'`), job);
  assert.doesNotMatch(jobs.lint, /needs:|\n    if:/);
  assert.doesNotMatch(jobs.native_enrollment_qualification, /needs:|\n    if:/);
  assert.match(jobs.test, /if: \$\{\{ !cancelled\(\) \}\}/);
  assert.match(jobs.test, /node --test --test-concurrency=1 scripts\/jones\/ci\/\*\.node-test\.\*/);
  for (const [job, packageName] of [
    ["test_desktop", "desktop"],
    ["test_mobile", "mobile"],
    ["test_client_runtime", "client-runtime"],
    ["test_web", "web"],
  ])
    assert.ok(jobs[job].includes(`vp run --filter @t3tools/${packageName} test`));
  assert.match(jobs.test_web, /shard: \[1, 2\]/);
  assert.match(jobs.test_web, /--shard \$\{\{ matrix.shard \}\}\/\$\{\{ strategy.job-total \}\}/);
  for (const node of ["24.22.1", "26.8.2"]) {
    assert.ok(jobs.native_enrollment_qualification.includes(node));
    assert.ok(jobs.native_enrollment_qualification_macos.includes(node));
  }
});

test("test evidence binds complete case counts and a digest to the run, and refuses unreported success", () => {
  const report = JSON.stringify({
    numTotalTests: 5,
    numPassedTests: 3,
    numFailedTests: 1,
    numPendingTests: 1,
  });
  const result = receipt(report, {
    outcome: "failure",
    run: "123",
    command: "vp run --filter @t3tools/web test --shard 1/2",
  });
  assert.equal(result.totalTests, 5);
  assert.equal(result.passedTests, 3);
  assert.equal(result.failedTests, 1);
  assert.equal(result.pendingTests, 1);
  assert.equal(result.run, "123");
  assert.match(result.reportSha256, /^[a-f0-9]{64}$/);
  assert.throws(() => receipt(null, { outcome: "success" }), /case report/);
  assert.equal(receipt(null, { outcome: "failure" }).reportSha256, null);
});
