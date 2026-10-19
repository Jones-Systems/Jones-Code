"use strict";

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const helper = require("./focused-precheck.cjs");

const SHA = "a".repeat(40);
const SCRIPT = "vp test run --config ../../vite.config.ts --dir .";
const FILE = "packages/shared/src/example.test.ts";
const ENV = {
  GITHUB_EVENT_NAME: "workflow_dispatch", DEFAULT_BRANCH: "main", GITHUB_REF: "refs/heads/main",
  GITHUB_REPOSITORY: "Jones-Systems/Jones-Code",
  GITHUB_WORKFLOW_REF: "Jones-Systems/Jones-Code/.github/workflows/focused-precheck.yml@refs/heads/main",
  GITHUB_SHA: SHA, GITHUB_WORKFLOW_SHA: "b".repeat(40),
  INPUT_CANDIDATE_SHA: SHA, INPUT_CANDIDATE_BRANCH: "work/example", INPUT_PROFILE: "tests",
  INPUT_TEST_PATHS: FILE, INPUT_REQUEST_ID: "request-123", GITHUB_RUN_ID: "123", GITHUB_RUN_ATTEMPT: "2",
};

const activeFixtures = new Set();
function removeFixture(root) {
  fs.rmSync(root, { recursive: true, force: true });
  activeFixtures.delete(root);
}
for (const [signal, code] of [["SIGINT", 130], ["SIGTERM", 143]]) {
  process.once(signal, () => {
    for (const root of activeFixtures) removeFixture(root);
    process.exit(code);
  });
}
process.once("exit", () => {
  for (const root of activeFixtures) removeFixture(root);
});

function fixture(t) {
  // Tiny fixtures live beside the checkout, never inside its index or shared /tmp.
  const root = fs.mkdtempSync(path.join(path.dirname(process.cwd()), ".focused-precheck-test-"));
  activeFixtures.add(root);
  t.after(() => removeFixture(root));
  const tracked = new Map();
  function add(file, content = "", mode = "100644") {
    const target = path.join(root, file);
    fs.mkdirSync(path.dirname(target), { recursive: true });
    fs.writeFileSync(target, content);
    tracked.set(file, mode);
    return target;
  }
  add("packages/shared/package.json", JSON.stringify({ scripts: { test: SCRIPT } }));
  add(FILE);
  return { root, tracked, add };
}

function report(file, statuses = ["passed"]) {
  return { success: true, testResults: [{ name: file, status: "passed", assertionResults: statuses.map((status) => ({ status })) }] };
}

function runCLI(root, command, env = {}) {
  return spawnSync(process.execPath, [path.join(__dirname, "focused-precheck.cjs"), command], {
    encoding: "utf8", env: { ...process.env, ...ENV, RUNNER_TEMP: root, GITHUB_WORKSPACE: root, ...env },
  });
}

function receipt(root) {
  return JSON.parse(fs.readFileSync(path.join(root, "focused-precheck/receipt.json"), "utf8"));
}

test("normalizes, deduplicates and sorts exact paths", () => {
  const other = "apps/server/src/example.test.ts";
  const input = helper.validateRequest({ ...ENV, INPUT_TEST_PATHS: ` ${FILE}\n${other}\t${FILE} ` });
  assert.deepEqual(input.test_paths, [other, FILE]);
  assert.equal(input.candidate_branch, "work/example");
});

for (const [key, values] of Object.entries({
  GITHUB_EVENT_NAME: ["push", "pull_request"], GITHUB_REF: ["refs/heads/work", "refs/tags/main"],
  GITHUB_WORKFLOW_REF: ["Jones-Systems/Jones-Code/.github/workflows/other.yml@refs/heads/main", "other/repo/.github/workflows/focused-precheck.yml@refs/heads/main"],
  GITHUB_WORKFLOW_SHA: ["", "not-a-sha"], INPUT_CANDIDATE_SHA: ["main", "A".repeat(40), "a".repeat(39)],
  INPUT_CANDIDATE_BRANCH: ["-work", "/work", ".work", "work/", "work.", "a..b", "a//b", "a@{b", "work.lock", "a b", "x".repeat(201)],
  INPUT_PROFILE: ["full", ""], INPUT_REQUEST_ID: ["short", "Uppercase", "../request", "x".repeat(65)],
  INPUT_TEST_PATHS: ["", " ", FILE + "\u00a0", "x".repeat(4097), "packages/shared/../x.test.ts", "packages/shared/./x.test.ts", "packages/shared//x.test.ts", "packages/shared/src/a:test.ts", "scripts/a.test.ts", ".github/a.test.ts", "native/a.test.ts", "infra/a.test.ts", "apps/marketing/a.test.ts", "packages/source-control-testing/a.test.ts", "packages/oxlint-plugin/a.test.ts", "apps/web/src/a.browser.test.tsx", "packages/shared/src/a.spec.ts", "packages/shared/src/*.test.ts"],
})) {
  test(`rejects invalid ${key}`, () => {
    for (const value of values) assert.throws(() => helper.validateRequest({ ...ENV, [key]: value }), undefined, value);
  });
}

test("static requires no test files; other profiles require bounded selection", () => {
  assert.throws(() => helper.validateRequest({ ...ENV, INPUT_PROFILE: "static" }));
  assert.deepEqual(helper.validateRequest({ ...ENV, INPUT_PROFILE: "static", INPUT_TEST_PATHS: "" }).test_paths, []);
  assert.throws(() => helper.validateRequest({ ...ENV, INPUT_TEST_PATHS: Array.from({ length: 21 }, (_, i) => `packages/shared/${i}.test.ts`).join(" ") }));
  assert.equal(helper.validateRequest({ ...ENV, INPUT_PROFILE: "static-and-tests" }).profile, "static-and-tests");
});

test("requires tracked regular files and rejects a symlink file", (t) => {
  const { root, tracked, add } = fixture(t);
  assert.equal(helper.regularFile(root, FILE, tracked), path.join(root, FILE));
  tracked.delete(FILE);
  assert.throws(() => helper.buildGroups(root, [FILE], tracked), /untracked/);
  tracked.set(FILE, "120000");
  assert.throws(() => helper.buildGroups(root, [FILE], tracked), /non-regular/);
  tracked.set(FILE, "100644");
  fs.unlinkSync(path.join(root, FILE));
  const target = add("target");
  fs.symlinkSync(target, path.join(root, FILE));
  assert.throws(() => helper.buildGroups(root, [FILE], tracked), /symlink/);
});

test("rejects symlink ancestors even when the target is inside the root", (t) => {
  const { root, tracked, add } = fixture(t);
  add("elsewhere/file.test.ts");
  fs.symlinkSync(path.join(root, "elsewhere"), path.join(root, "packages/shared/link"));
  const file = "packages/shared/link/file.test.ts";
  tracked.set(file, "100644");
  assert.throws(() => helper.buildGroups(root, [file], tracked), /symlink/);
});

test("rejects missing files, unsupported scripts and changed test config", (t) => {
  const f = fixture(t);
  assert.throws(() => helper.buildGroups(f.root, [FILE], f.tracked, ["vite.config.ts"]), /configuration/);
  assert.throws(() => helper.buildGroups(f.root, [FILE], f.tracked, ["packages/shared/vitest.config.mts"]), /configuration/);
  f.add("packages/shared/package.json", JSON.stringify({ scripts: { test: "vp test run || true" } }));
  assert.throws(() => helper.buildGroups(f.root, [FILE], f.tracked), /script/);
  fs.unlinkSync(path.join(f.root, FILE));
  assert.throws(() => helper.buildGroups(f.root, [FILE], f.tracked), /ENOENT/);
});

test("rejects substring and case-insensitive selection collisions", (t) => {
  const f = fixture(t);
  const extra = "packages/shared/other/src/EXAMPLE.test.ts";
  f.add(extra);
  assert.throws(() => helper.buildGroups(f.root, [FILE], f.tracked), /ambiguous/);
  f.tracked.delete(extra);
  f.add("packages/shared/src/example.test.tsx");
  assert.throws(() => helper.buildGroups(f.root, [FILE], f.tracked), /ambiguous/);
});

test("binds supported package commands and removes allow-empty flags", (t) => {
  const f = fixture(t);
  const scripts = {
    "apps/server": "vp test run", "packages/client-runtime": "vp test run",
    "apps/desktop": "vp test run --passWithNoTests", "apps/mobile": SCRIPT,
    "apps/web": "vp test run --passWithNoTests --project unit --project browser",
  };
  const files = [FILE];
  for (const [pkg, script] of Object.entries(scripts)) {
    f.add(`${pkg}/package.json`, JSON.stringify({ scripts: { test: script } }));
    const file = `${pkg}/src/a.test.ts`; f.add(file); files.push(file);
  }
  const groups = helper.buildGroups(f.root, files, f.tracked);
  assert.equal(groups.length, 6);
  assert.deepEqual(groups.find((g) => g.cwd === "apps/web").argv, ["vp", "test", "run", "--project", "unit", "src/a.test.ts"]);
  assert.deepEqual(groups.find((g) => g.cwd === "apps/desktop").argv, ["vp", "test", "run", "src/a.test.ts"]);
  const mobile = groups.find((g) => g.cwd === "apps/mobile");
  assert.deepEqual(mobile.argv.slice(0, 7), ["vp", "test", "run", "--config", "../../vite.config.ts", "--dir", "."]);
  const argv = helper.testArgv(mobile, "/runner/report.json");
  assert.deepEqual(argv.slice(-4), ["--passWithNoTests=false", "--reporter=default", "--reporter=json", "--outputFile.json=/runner/report.json"]);
});

test("same-invocation report requires exact files and actual execution", (t) => {
  const { root } = fixture(t);
  const file = path.join(root, FILE);
  const assess = (r, code = 0) => helper.assessReport(r, [file], root, code);
  assert.equal(assess(report(file)).result, "pass");
  for (const r of [undefined, {}, { testResults: [] }, report(file, []), report(file, ["skipped", "todo", "pending"])]) assert.equal(assess(r).result, "unknown");
  assert.equal(assess(report(file), 1).result, "fail");
  assert.equal(assess(undefined, 1).result, "fail");
  assert.equal(assess(report(file), null).result, "unknown");
  assert.equal(assess({ ...report(file), success: false }).result, "fail");
  assert.equal(assess(report(file, ["failed"])).result, "fail");
  assert.equal(assess(report(file, ["skipped", "passed"])).result, "pass");
});

test("missing, extra, duplicate or wrong report filenames cannot pass", (t) => {
  const { root, add } = fixture(t);
  const file = path.join(root, FILE), other = add("other.test.ts");
  for (const names of [[other], [file, other], [file, file], [path.join(root, "missing.test.ts")]]) {
    const r = { success: true, testResults: names.map((name) => report(name).testResults[0]) };
    assert.deepEqual(helper.assessReport(r, [file], root, 0).result, "unknown");
    const failed = helper.assessReport(r, [file], root, 1);
    assert.equal(failed.result, "fail"); assert.equal(failed.selection_mismatch, true);
  }
});

test("installed runner versions must match the bound upstream sources", (t) => {
  const f = fixture(t);
  f.add("package.json", "{}");
  f.add("node_modules/vite-plus/package.json", JSON.stringify({ name: "vite-plus", version: "1.0.0" }));
  f.add("node_modules/vitest/package.json", JSON.stringify({ name: "vitest", version: "5.0.1" }));
  assert.deepEqual(helper.verifyRunner(f.root), { vite_plus: "1.0.0", vitest: "5.0.1" });
  f.add("node_modules/vitest/package.json", JSON.stringify({ name: "vitest", version: "6.0.0" }));
  assert.throws(() => helper.verifyRunner(f.root), /unsupported installed/);
});

test("final gate requires valid plan, setup success and every expected check", () => {
  const base = helper.baseline(ENV);
  const plan = { ...helper.validateRequest({ ...ENV, INPUT_PROFILE: "static", INPUT_TEST_PATHS: "" }), request: "valid", groups: [] };
  const checks = helper.expectedChecks(plan).map((c) => ({ ...c, result: "pass", seconds: 1, setup_seconds: 3 }));
  assert.equal(checks.length, 4);
  assert.equal(helper.finalize(base, plan, checks, true).result, "pass");
  assert.equal(helper.finalize(base, plan, checks, true).setup_seconds, 3);
  assert.equal(helper.finalize(base, plan, checks, false).result, "unavailable");
  assert.equal(helper.finalize(base, plan, [], true).result, "unknown");
  assert.equal(helper.finalize(base, plan, [], false).checks[0].result, "unavailable");
  assert.equal(helper.finalize(base, plan, checks.slice(1), true).result, "unknown");
  assert.equal(helper.finalize(base, plan, [...checks, checks[0]], true).result, "unknown");
  assert.equal(helper.finalize(base, plan, [{ ...checks[0], result: "fail" }, ...checks.slice(1)], true).result, "fail");
  assert.deepEqual(helper.finalize(base, undefined, checks, true).checks, []);
  assert.equal(helper.finalize(base, { ...plan, profile: "tests" }, [], true).result, "unknown");
});

test("receipt separates workflow revision and dispatch default head", () => {
  const base = helper.baseline(ENV);
  assert.equal(base.workflow_sha, ENV.GITHUB_WORKFLOW_SHA);
  assert.equal(base.default_head_sha, ENV.GITHUB_SHA);
  assert.equal(base.run_url, "https://github.com/Jones-Systems/Jones-Code/actions/runs/123");
  assert.match(base.helper_sha256, /^[0-9a-f]{64}$/);
  assert.equal(base.not_covered, "candidate tree, not the PR merge commit; not a required check; not merge approval");
  assert.match(base.advisory, /not a security attestation/);
});

test("invalid CLI input creates a fixed-path receipt without running checks", (t) => {
  const { root } = fixture(t);
  const output = path.join(root, "outputs");
  assert.equal(runCLI(root, "validate", { INPUT_REQUEST_ID: "../../escape", GITHUB_OUTPUT: output }).status, 1);
  assert.equal(receipt(root).request, "unknown");
  assert.deepEqual(receipt(root).checks, []);
  assert.match(fs.readFileSync(output, "utf8"), /artifact=focused-precheck-invalid-request-a2/);
  assert.equal(runCLI(root, "finish", { PRECHECK_SETUP_OK: "true" }).status, 1);
  assert.equal(receipt(root).result, "unknown");
});

test("checkout mismatch stops planning before setup and keeps empty checks", (t) => {
  const { root } = fixture(t);
  assert.equal(runCLI(root, "validate").status, 0);
  const result = runCLI(root, "plan", { GITHUB_WORKSPACE: path.resolve(__dirname, "../..") });
  assert.equal(result.status, 1);
  assert.match(receipt(root).error, /checkout mismatch/);
  assert.equal(runCLI(root, "finish", { PRECHECK_SETUP_OK: "true" }).status, 1);
  assert.deepEqual(receipt(root).checks, []);
});

test("finish recovers from missing or partial results as unknown, never green", (t) => {
  const { root } = fixture(t);
  const plan = { ...helper.validateRequest({ ...ENV, INPUT_PROFILE: "static", INPUT_TEST_PATHS: "" }), request: "valid", groups: [] };
  assert.equal(runCLI(root, "validate").status, 0);
  fs.writeFileSync(path.join(root, "focused-precheck/plan.json"), JSON.stringify(plan));
  fs.writeFileSync(path.join(root, "focused-precheck/results.jsonl"), '{"partial":');
  const summary = path.join(root, "summary");
  assert.equal(runCLI(root, "finish", { PRECHECK_SETUP_OK: "true", GITHUB_STEP_SUMMARY: summary }).status, 1);
  assert.equal(receipt(root).result, "unknown");
  assert.equal(receipt(root).checks.length, 4);
  assert.match(fs.readFileSync(summary, "utf8"), /not merge approval/);
  assert.equal(runCLI(root, "finish", { PRECHECK_SETUP_OK: "false" }).status, 1);
  assert.equal(receipt(root).result, "unavailable");
});

test("fixture cleanup owns only its captured root on success and failure", (t) => {
  const first = fixture(t), second = fixture(t);
  removeFixture(first.root);
  assert.equal(fs.existsSync(first.root), false);
  assert.equal(fs.existsSync(second.root), true);
  assert.throws(() => {
    try { throw new Error("original failure"); }
    finally { removeFixture(second.root); }
  }, /original failure/);
  assert.equal(fs.existsSync(second.root), false);
});

test("test CLI runs serial groups with exact argv and retains each result", (t) => {
  const f = fixture(t);
  f.add("package.json", "{}");
  f.add("node_modules/vite-plus/package.json", '{"version":"1.0.0"}');
  f.add("node_modules/vitest/package.json", '{"version":"5.0.1"}');
  const other = "apps/server/src/second.test.ts";
  f.add(other);
  f.add("apps/server/package.json", '{"scripts":{"test":"vp test run"}}');
  const executable = f.add("bin/vp", `#!${process.execPath}
const fs = require('node:fs');
const path = require('node:path');
const argv = process.argv.slice(2);
const output = argv.find((arg) => arg.startsWith('--outputFile.json=')).slice('--outputFile.json='.length);
const tests = argv.filter((arg) => arg.endsWith('.test.ts'));
fs.appendFileSync(process.env.CALLS, JSON.stringify({cwd:process.cwd(),argv})+'\\n');
const mismatch = process.env.MISMATCH === 'true';
const result = {success:true, testResults:tests.map((name) => ({name:path.resolve(name),status:'passed',assertionResults:[{status: 'passed'}]}))};
if (mismatch) result.testResults=[];
fs.writeFileSync(output, JSON.stringify(result));
`);
  fs.chmodSync(executable, 0o755);
  const request = helper.validateRequest({ ...ENV, INPUT_TEST_PATHS: `${FILE} ${other}` });
  assert.equal(runCLI(f.root, "validate").status, 0);
  const plan = { ...request, request: "valid", groups: helper.buildGroups(f.root, request.test_paths, f.tracked), setup_started_at: Date.now() };
  fs.writeFileSync(path.join(f.root, "focused-precheck/plan.json"), JSON.stringify(plan));
  const calls = path.join(f.root, "calls");
  const env = { PATH: `${path.join(f.root, "bin")}${path.delimiter}${process.env.PATH}`, CALLS: calls };
  assert.equal(runCLI(f.root, "run-tests", env).status, 0);
  assert.equal(runCLI(f.root, "finish", { PRECHECK_SETUP_OK: "true" }).status, 0);
  assert.equal(receipt(f.root).checks.length, 2);
  const invoked = fs.readFileSync(calls, "utf8").trim().split("\n").map(JSON.parse);
  assert.deepEqual(invoked.map((c) => c.cwd), [path.join(f.root, "apps/server"), path.join(f.root, "packages/shared")]);
  assert.ok(invoked.every((c) => c.argv.includes("--passWithNoTests=false") && c.argv.includes("--reporter=json")));
  // A stale successful report cannot make an empty later invocation pass.
  fs.rmSync(path.join(f.root, "focused-precheck/results.jsonl"));
  assert.equal(runCLI(f.root, "run-tests", { ...env, MISMATCH: "true" }).status, 1);
  assert.equal(runCLI(f.root, "finish", { PRECHECK_SETUP_OK: "true" }).status, 1);
  assert.ok(receipt(f.root).checks.every((c) => c.result === "unknown"));
});

test("static CLI accepts only literal check IDs and records a failed command", (t) => {
  const f = fixture(t);
  const executable = f.add("bin/bash", `#!${process.execPath}
require('node:fs').writeFileSync(process.env.CALLS, JSON.stringify(process.argv.slice(2)));
process.exit(9);
`);
  fs.chmodSync(executable, 0o755);
  assert.equal(runCLI(f.root, "validate").status, 0);
  const plan = { ...helper.validateRequest({ ...ENV, INPUT_PROFILE: "static", INPUT_TEST_PATHS: "" }), request: "valid", groups: [], setup_started_at: Date.now() };
  fs.writeFileSync(path.join(f.root, "focused-precheck/plan.json"), JSON.stringify(plan));
  const calls = path.join(f.root, "calls");
  const env = { ...process.env, ...ENV, RUNNER_TEMP: f.root, GITHUB_WORKSPACE: f.root, CALLS: calls, PATH: `${path.join(f.root, "bin")}${path.delimiter}${process.env.PATH}` };
  const invoke = (id) => spawnSync(process.execPath, [path.join(__dirname, "focused-precheck.cjs"), "run-check", id], { env, encoding: "utf8" });
  assert.equal(invoke("knip; echo unexpected").status, 1);
  assert.equal(fs.existsSync(calls), false);
  assert.equal(invoke("knip").status, 1);
  assert.deepEqual(JSON.parse(fs.readFileSync(calls, "utf8")), ["-euo", "pipefail", "-c", "vp run knip:check"]);
  assert.equal(JSON.parse(fs.readFileSync(path.join(f.root, "focused-precheck/results.jsonl"), "utf8")).result, "fail");
});

test("candidate planning binds actual HEAD/tree and distinguishes unknown ancestry", () => {
  const root = path.resolve(__dirname, "../..");
  const head = spawnSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).stdout.trim();
  const request = helper.validateRequest({ ...ENV, INPUT_CANDIDATE_SHA: head, INPUT_PROFILE: "static", INPUT_TEST_PATHS: "" });
  const plan = helper.inspectCandidate(root, request, head);
  assert.equal(plan.candidate_sha_checked_out, head);
  assert.match(plan.candidate_tree, /^[0-9a-f]{40}$/);
  assert.equal(plan.contains_default_head, true);
  assert.equal(plan.candidate_changes_precheck, false);
  const missing = helper.inspectCandidate(root, request, SHA);
  assert.equal(missing.contains_default_head, "unknown");
  assert.equal(missing.candidate_changes_precheck, "unknown");
});
